/**
 * AURA STREAM — Cloud SFTP Upload Gateway (Single Port, Memory-Based)
 *
 * Deploy on Railway.app (free tier).
 * Camera → SFTP (single TCP port) → memory buffer → Supabase → live guest stream.
 *
 * Required env vars (set in Railway dashboard):
 *   NEXT_PUBLIC_SUPABASE_URL
 *   SUPABASE_SERVICE_ROLE_KEY
 *   UPLOAD_SECRET        — camera password
 *   PORT                 — Railway sets this automatically
 */

'use strict';

const { Server, utils: { sftp: { STATUS_CODE, flagsToString } } } = require('ssh2');
const { generateKeyPairSync } = require('crypto');
const { createClient } = require('@supabase/supabase-js');

// ── 1. Config ──────────────────────────────────────────────────────────────────
const SUPABASE_URL  = process.env.NEXT_PUBLIC_SUPABASE_URL;
const SUPABASE_KEY  = process.env.SUPABASE_SERVICE_ROLE_KEY;
const UPLOAD_SECRET = process.env.UPLOAD_SECRET;
const PORT          = parseInt(process.env.PORT || '2222', 10);

const VALID_EXTS = new Set(['jpg', 'jpeg', 'png', 'heic', 'heif', 'cr2', 'cr3', 'nef', 'arw', 'dng']);

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error('❌  Missing NEXT_PUBLIC_SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY');
  process.exit(1);
}

// ── 2. Supabase client ────────────────────────────────────────────────────────
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY, {
  auth: { persistSession: false },
});

// ── 3. Ephemeral RSA host key (generated fresh on each start — no file needed) ─
console.log('[STARTUP] Generating ephemeral host key …');
const { privateKey: hostKey } = generateKeyPairSync('rsa', {
  modulusLength: 2048,
  publicKeyEncoding:  { type: 'pkcs1', format: 'pem' },
  privateKeyEncoding: { type: 'pkcs1', format: 'pem' },
});

// ── 4. Upload a buffer directly to Supabase storage ──────────────────────────
async function uploadBufferToSupabase(buffer, filename, eventId) {
  const ext = (filename.split('.').pop() || '').toLowerCase();

  if (!VALID_EXTS.has(ext)) {
    console.log(`[UPLOAD] ⚠️  Skipping unsupported extension: ${filename}`);
    return;
  }

  const storagePath = `events/${eventId}/${Date.now()}-${Math.random().toString(36).slice(2, 8)}.${ext}`;
  const mime        = ext === 'png' ? 'image/png' : 'image/jpeg';

  console.log(`[UPLOAD] 📤 Uploading ${filename} (${(buffer.length / 1024).toFixed(0)} KB) to Supabase…`);

  try {
    const { error: upErr } = await supabase.storage
      .from('wedding-photos')
      .upload(storagePath, buffer, { contentType: mime, cacheControl: '3600', upsert: false });
    if (upErr) throw upErr;

    const { data: urlData } = supabase.storage
      .from('wedding-photos')
      .getPublicUrl(storagePath);

    const { error: dbErr } = await supabase
      .from('event_images')
      .insert({ event_id: eventId, storage_path: storagePath, public_url: urlData.publicUrl });
    if (dbErr) throw dbErr;

    console.log(`[UPLOAD] ✅ Live on guest stream: ${urlData.publicUrl}`);
  } catch (err) {
    console.error(`[UPLOAD] ❌ Failed — ${filename}:`, err.message);
  }
}

// ── 5. SFTP Server ────────────────────────────────────────────────────────────
const sftpServer = new Server({ hostKeys: [hostKey] }, (client) => {
  let clientEventId = null;

  // Per-connection file handle registry
  // key: uint32 handle ID, value: { chunks[], filename, isDir }
  const handles  = new Map();
  let   nextId   = 1;

  function makeHandle(id) {
    const buf = Buffer.alloc(4);
    buf.writeUInt32BE(id, 0);
    return buf;
  }
  function readHandle(buf) {
    return buf.readUInt32BE(0);
  }

  // ── Auth ───────────────────────────────────────────────────────────────────
  client.on('authentication', async (ctx) => {
    if (ctx.method !== 'password') return ctx.reject(['password']);

    const username     = ctx.username.trim();
    const password     = ctx.password;
    const parsedId     = username.startsWith('event_') ? username.slice(6) : username;

    console.log(`[AUTH] Attempt — user: "${username}", eventId: "${parsedId}"`);

    // Password check
    if (UPLOAD_SECRET && password !== UPLOAD_SECRET) {
      console.warn(`[AUTH] ❌ Wrong password for "${username}"`);
      return ctx.reject();
    }

    // Verify event in DB
    try {
      const { data: event, error } = await supabase
        .from('events')
        .select('id, event_name')
        .eq('id', parsedId)
        .single();

      if (error) { console.warn('[AUTH] ❌ Supabase error:', error.message); return ctx.reject(); }
      if (!event) { console.warn(`[AUTH] ❌ Event "${parsedId}" not found`); return ctx.reject(); }

      clientEventId = event.id;
      console.log(`[AUTH] ✅ OK — event: "${event.event_name}" (${clientEventId})`);
      ctx.accept();
    } catch (e) {
      console.error('[AUTH] Exception:', e.message);
      ctx.reject();
    }
  });

  client.on('ready', () => {
    console.log('[SSH]  Client ready');

    client.on('session', (accept) => {
      const session = accept();

      session.on('sftp', (accept) => {
        const sftp = accept();
        console.log('[SFTP] Session started');

        // helper: safely read a uint32 handle, return null on error
        function safeId(handle) {
          try { return handle.readUInt32BE(0); }
          catch (e) { console.error('[SFTP] Bad handle buffer:', handle, e.message); return null; }
        }

        // pending renames: oldName → { buffer, filename }
        const pendingBuffers = new Map(); // filename → Buffer (for rename support)

        // ── REALPATH ─────────────────────────────────────────────────────
        sftp.on('REALPATH', (reqid, p) => {
          console.log(`[SFTP] REALPATH "${p}"`);
          sftp.name(reqid, [{ filename: '/', longname: 'drwxr-xr-x 1 user group 0 Jan 1 00:00 /', attrs: {} }]);
        });

        // ── STAT / LSTAT ──────────────────────────────────────────────────
        const dirAttrs = { mode: 0o40755, uid: 0, gid: 0, size: 0, atime: 0, mtime: 0 };
        sftp.on('STAT',  (reqid, p) => { console.log(`[SFTP] STAT "${p}"`);  sftp.attrs(reqid, dirAttrs); });
        sftp.on('LSTAT', (reqid, p) => { console.log(`[SFTP] LSTAT "${p}"`); sftp.attrs(reqid, dirAttrs); });

        // ── OPENDIR ───────────────────────────────────────────────────────
        sftp.on('OPENDIR', (reqid, p) => {
          const id = nextId++;
          console.log(`[SFTP] OPENDIR "${p}" handle=${id}`);
          handles.set(id, { isDir: true });
          sftp.handle(reqid, makeHandle(id));
        });

        // ── READDIR ───────────────────────────────────────────────────────
        sftp.on('READDIR', (reqid) => {
          console.log('[SFTP] READDIR → EOF');
          sftp.status(reqid, STATUS_CODE.EOF);
        });

        // ── OPEN ──────────────────────────────────────────────────────────
        sftp.on('OPEN', (reqid, filename, flags) => {
          const mode     = flagsToString(flags) || 'w';
          const basename = filename.split('/').filter(Boolean).pop() || filename;
          const id       = nextId++;
          console.log(`[SFTP] OPEN "${basename}" mode=${mode} handle=${id}`);
          handles.set(id, { isDir: false, filename: basename, chunks: [] });
          sftp.handle(reqid, makeHandle(id));
        });

        // ── WRITE ─────────────────────────────────────────────────────────
        sftp.on('WRITE', (reqid, handle, offset, data) => {
          const id = safeId(handle);
          if (id === null) return sftp.status(reqid, STATUS_CODE.FAILURE);

          const obj = handles.get(id);
          if (!obj || obj.isDir) {
            console.warn(`[SFTP] WRITE — no handle for id=${id}`);
            return sftp.status(reqid, STATUS_CODE.FAILURE);
          }

          obj.chunks.push({ offset, data: Buffer.from(data) });
          const total = obj.chunks.reduce((s, c) => s + c.data.length, 0);
          console.log(`[SFTP] WRITE handle=${id} offset=${offset} chunkSize=${data.length} totalSoFar=${total}`);
          sftp.status(reqid, STATUS_CODE.OK);
        });

        // ── CLOSE ─────────────────────────────────────────────────────────
        sftp.on('CLOSE', (reqid, handle) => {
          const id = safeId(handle);
          if (id === null) return sftp.status(reqid, STATUS_CODE.FAILURE);

          const obj = handles.get(id);
          if (!obj) {
            console.warn(`[SFTP] CLOSE — unknown handle id=${id}`);
            return sftp.status(reqid, STATUS_CODE.FAILURE);
          }

          handles.delete(id);
          sftp.status(reqid, STATUS_CODE.OK); // ack immediately

          if (obj.isDir) { console.log(`[SFTP] CLOSE dir handle=${id}`); return; }

          const totalBytes = obj.chunks.reduce((s, c) => s + c.data.length, 0);
          console.log(`[SFTP] CLOSE file="${obj.filename}" handle=${id} chunks=${obj.chunks.length} totalBytes=${totalBytes}`);

          if (obj.chunks.length === 0) {
            console.log('[SFTP] No data in this handle — skipping upload');
            return;
          }

          obj.chunks.sort((a, b) => a.offset - b.offset);
          const fileBuffer = Buffer.concat(obj.chunks.map((c) => c.data));
          console.log(`[SFTP] Assembled ${(fileBuffer.length / 1024).toFixed(0)} KB from ${obj.chunks.length} chunks`);

          // Cache under filename in case a RENAME comes next
          pendingBuffers.set(obj.filename, fileBuffer);

          uploadBufferToSupabase(fileBuffer, obj.filename, clientEventId);
        });

        // ── RENAME ────────────────────────────────────────────────────────
        // Some phone apps upload to a temp file then rename to the real name.
        // If we have the buffer cached under oldName, re-upload under newName.
        sftp.on('RENAME', (reqid, oldPath, newPath) => {
          const oldName = oldPath.split('/').filter(Boolean).pop() || oldPath;
          const newName = newPath.split('/').filter(Boolean).pop() || newPath;
          console.log(`[SFTP] RENAME "${oldName}" → "${newName}"`);

          const buf = pendingBuffers.get(oldName);
          if (buf) {
            pendingBuffers.delete(oldName);
            console.log(`[SFTP] Re-uploading ${(buf.length / 1024).toFixed(0)} KB under new name "${newName}"`);
            uploadBufferToSupabase(buf, newName, clientEventId);
          }

          sftp.status(reqid, STATUS_CODE.OK);
        });

        // ── MKDIR / REMOVE / RMDIR ─────────────────────────────────────────
        sftp.on('MKDIR',  (reqid, p)    => { console.log(`[SFTP] MKDIR "${p}"`);  sftp.status(reqid, STATUS_CODE.OK); });
        sftp.on('REMOVE', (reqid, p)    => { console.log(`[SFTP] REMOVE "${p}"`); sftp.status(reqid, STATUS_CODE.OK); });
        sftp.on('RMDIR',  (reqid, p)    => { console.log(`[SFTP] RMDIR "${p}"`);  sftp.status(reqid, STATUS_CODE.OK); });
      });
    });
  });

  client.on('close', () => {
    console.log('[SSH]  Client disconnected — flushing any unclosed handles…');
    // Phone sends all WRITE data but disconnects without sending CLOSE.
    // Upload whatever data we accumulated in open handles.
    for (const [id, obj] of handles) {
      if (!obj.isDir && obj.chunks && obj.chunks.length > 0) {
        const totalBytes = obj.chunks.reduce((s, c) => s + c.data.length, 0);
        console.log(`[SFTP] Flushing unclosed handle=${id} file="${obj.filename}" chunks=${obj.chunks.length} totalBytes=${totalBytes}`);
        obj.chunks.sort((a, b) => a.offset - b.offset);
        const fileBuffer = Buffer.concat(obj.chunks.map((c) => c.data));
        uploadBufferToSupabase(fileBuffer, obj.filename, clientEventId);
      }
    }
    handles.clear();
  });
  client.on('error', (e) => console.error('[SSH]  Error:', e.message));
});

// ── 6. Start listening ────────────────────────────────────────────────────────
sftpServer.listen(PORT, '0.0.0.0', () => {
  console.log('\n╔══════════════════════════════════════════════════╗');
  console.log('║     AURA STREAM — Cloud SFTP Gateway (Ready)     ║');
  console.log('╠══════════════════════════════════════════════════╣');
  console.log(`║  Port       : ${PORT}`);
  console.log(`║  Auth       : ${UPLOAD_SECRET ? '✅ Password enabled' : '⚠️  Open (no password)'}`);
  console.log('╠══════════════════════════════════════════════════╣');
  console.log('║  Camera setup:                                   ║');
  console.log('║   Protocol → SFTP                                ║');
  console.log('║   Username → event_<eventId>                     ║');
  console.log(`║   Password → ${UPLOAD_SECRET || '(none)'}`);
  console.log('╚══════════════════════════════════════════════════╝\n');
});
