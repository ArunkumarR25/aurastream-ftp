/**
 * AURA STREAM — Cloud SFTP Upload Gateway (Single Port)
 *
 * Deploy this on Railway.app (free).
 * Cameras connect via SFTP on a single port → photos auto-upload to Supabase.
 */

const fs = require('fs');
const path = require('path');
const os = require('os');
const { Server, SFTP_STATUS_CODE: STATUS_CODE } = require('ssh2');
const { generateKeyPairSync } = require('crypto');
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');

// ── 1. Environment Variables ─────────────────────────────────────────────────
const supabaseUrl  = process.env.NEXT_PUBLIC_SUPABASE_URL;
const supabaseKey  = process.env.SUPABASE_SERVICE_ROLE_KEY;
const uploadSecret = process.env.UPLOAD_SECRET;
const PORT         = parseInt(process.env.PORT || '2222', 10);

if (!supabaseUrl || !supabaseKey) {
  console.error('❌ Missing NEXT_PUBLIC_SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY');
  process.exit(1);
}

// ── 2. Supabase Admin Client ─────────────────────────────────────────────────
const supabase = createClient(supabaseUrl, supabaseKey, {
  auth: { persistSession: false },
  realtime: { transport: ws },
});

// ── 3. Temp directory for incoming SFTP uploads ──────────────────────────────
const TEMP_DIR = path.join(os.tmpdir(), 'aura-sftp-temp');
fs.mkdirSync(TEMP_DIR, { recursive: true });

// ── 4. Generate Host Keys (Ephemeral - No Setup Needed) ──────────────────────
console.log('Generating ephemeral host key...');
const hostKey = generateKeyPairSync('rsa', {
  modulusLength: 2048,
  publicKeyEncoding: { type: 'pkcs1', format: 'pem' },
  privateKeyEncoding: { type: 'pkcs1', format: 'pem' }
}).privateKey;

// ── 5. Helper: Upload file to Supabase ───────────────────────────────────────
async function uploadToSupabase(filePath, filename, eventId) {
  const ext = filename.split('.').pop()?.toLowerCase();
  const validExts = ['jpg', 'jpeg', 'png', 'heic', 'heif', 'cr2', 'cr3', 'nef', 'arw', 'dng'];
  
  if (!validExts.includes(ext)) {
    console.log(`[UPLOAD] ⚠️ Ignored invalid extension: ${filename}`);
    fs.unlinkSync(filePath);
    return;
  }

  try {
    const stat = fs.statSync(filePath);
    console.log(`[UPLOAD] 📷 Starting upload: ${filename} (${eventId}) — ${(stat.size / 1024).toFixed(0)} KB`);
    
    const buffer = fs.readFileSync(filePath);
    const storagePath = `events/${eventId}/${Date.now()}-${Math.random().toString(36).slice(2, 8)}.${ext}`;
    const mime = ext === 'png' ? 'image/png' : 'image/jpeg';

    const { error: uploadErr } = await supabase.storage
      .from('wedding-photos')
      .upload(storagePath, buffer, { contentType: mime, cacheControl: '3600', upsert: false });
    if (uploadErr) throw uploadErr;

    const { data: urlData } = supabase.storage.from('wedding-photos').getPublicUrl(storagePath);

    const { error: dbErr } = await supabase
      .from('event_images')
      .insert({ event_id: eventId, storage_path: storagePath, public_url: urlData.publicUrl });
    if (dbErr) throw dbErr;

    console.log(`[UPLOAD] ✅ Successfully uploaded to guest stream: ${filename}`);
  } catch (err) {
    console.error(`[UPLOAD] ❌ Failed to upload ${filename}:`, err.message);
  } finally {
    if (fs.existsSync(filePath)) {
      try { fs.unlinkSync(filePath); } catch (_) {}
    }
  }
}

// ── 6. SSH/SFTP Server Initialization ────────────────────────────────────────
const sftpServer = new Server({
  hostKeys: [hostKey]
}, (client) => {
  let eventId = '';
  let userSandboxDir = '';
  const handles = new Map();
  let handleCounter = 0;

  client.on('authentication', async (ctx) => {
    // Authenticate by username and password
    if (ctx.method !== 'password') {
      return ctx.reject(['password']);
    }

    const username = ctx.username;
    const password = ctx.password;
    console.log(`[AUTH] Login attempt for user: "${username}"`);

    const parsedEventId = username.startsWith('event_') ? username.slice(6) : username;

    if (uploadSecret && password !== uploadSecret) {
      console.log(`[AUTH] ❌ Incorrect password for "${username}"`);
      return ctx.reject();
    }

    try {
      // Validate event exists in database
      const { data: event, error } = await supabase
        .from('events')
        .select('id, event_name')
        .eq('id', parsedEventId)
        .single();

      if (error || !event) {
        console.log(`[AUTH] ❌ Event "${parsedEventId}" not found in database.`);
        return ctx.reject();
      }

      eventId = event.id;
      userSandboxDir = path.join(TEMP_DIR, eventId);
      fs.mkdirSync(userSandboxDir, { recursive: true });

      console.log(`[AUTH] ✅ Authenticated successfully — Event: "${event.event_name}" (${eventId})`);
      ctx.accept();
    } catch (err) {
      console.error('[AUTH] Database error:', err.message);
      ctx.reject();
    }
  }).on('ready', () => {
    console.log('[SSH] Connection established and client authenticated.');

    client.on('session', (accept, reject) => {
      const session = accept();

      session.on('sftp', (accept, reject) => {
        const sftpStream = accept();
        console.log('[SFTP] Subsystem session started.');

        sftpStream.on('REALPATH', (reqid, clientPath) => {
          // Send back root "/" path to client
          sftpStream.name(reqid, [{ filename: '/' }]);
        });

        sftpStream.on('STAT', (reqid, clientPath) => {
          // Simulate directory attributes or stats
          const resolvedPath = path.join(userSandboxDir, clientPath.replace(/^\/+/, ''));
          try {
            const stats = fs.statSync(resolvedPath);
            sftpStream.attrs(reqid, {
              mode: stats.mode,
              size: stats.size,
              mtime: Math.floor(stats.mtimeMs / 1000),
              atime: Math.floor(stats.atimeMs / 1000)
            });
          } catch (err) {
            // If checking root directory path, return directory attributes
            if (clientPath === '/' || clientPath === '.') {
              sftpStream.attrs(reqid, {
                mode: 0o40000 | 0o777, // Directory
                size: 0,
                mtime: Math.floor(Date.now() / 1000),
                atime: Math.floor(Date.now() / 1000)
              });
            } else {
              sftpStream.status(reqid, STATUS_CODE.NO_SUCH_FILE);
            }
          }
        });

        sftpStream.on('LSTAT', (reqid, clientPath) => {
          // Redirect LSTAT to STAT logic
          sftpStream.emit('STAT', reqid, clientPath);
        });

        sftpStream.on('OPENDIR', (reqid, clientPath) => {
          handleCounter++;
          const handleStr = `d${handleCounter}`;
          handles.set(handleStr, { isDir: true, path: clientPath });
          sftpStream.handle(reqid, Buffer.from(handleStr));
        });

        sftpStream.on('READDIR', (reqid, handle) => {
          // Just return EOF to represent an empty directory listing
          sftpStream.status(reqid, STATUS_CODE.EOF);
        });

        sftpStream.on('OPEN', (reqid, filename, flags, attrs) => {
          const sanitizedFilename = filename.replace(/^\/+/, '');
          const localPath = path.join(userSandboxDir, sanitizedFilename);
          
          console.log(`[SFTP] Client opening file for writing: ${sanitizedFilename}`);
          
          try {
            // Ensure target folder exists
            fs.mkdirSync(path.dirname(localPath), { recursive: true });
            
            // Open file for writing
            const fd = fs.openSync(localPath, 'w');
            
            handleCounter++;
            const handleStr = `f${handleCounter}`;
            handles.set(handleStr, { fd, localPath, filename: sanitizedFilename });
            
            sftpStream.handle(reqid, Buffer.from(handleStr));
          } catch (err) {
            console.error(`[SFTP] Open error:`, err.message);
            sftpStream.status(reqid, STATUS_CODE.FAILURE);
          }
        });

        sftpStream.on('WRITE', (reqid, handle, offset, data) => {
          const handleStr = handle.toString();
          const handleObj = handles.get(handleStr);
          if (!handleObj) {
            return sftpStream.status(reqid, STATUS_CODE.FAILURE);
          }

          try {
            fs.writeSync(handleObj.fd, data, 0, data.length, offset);
            sftpStream.status(reqid, STATUS_CODE.OK);
          } catch (err) {
            console.error(`[SFTP] Write error:`, err.message);
            sftpStream.status(reqid, STATUS_CODE.FAILURE);
          }
        });

        sftpStream.on('CLOSE', (reqid, handle) => {
          const handleStr = handle.toString();
          const handleObj = handles.get(handleStr);
          if (!handleObj) {
            return sftpStream.status(reqid, STATUS_CODE.FAILURE);
          }

          try {
            if (handleObj.fd) {
              fs.closeSync(handleObj.fd);
              handles.delete(handleStr);
              sftpStream.status(reqid, STATUS_CODE.OK);

              console.log(`[SFTP] Finished transfer: ${handleObj.filename}`);
              // Queue upload directly to Supabase
              uploadToSupabase(handleObj.localPath, handleObj.filename, eventId);
            } else {
              handles.delete(handleStr);
              sftpStream.status(reqid, STATUS_CODE.OK);
            }
          } catch (err) {
            console.error(`[SFTP] Close error:`, err.message);
            sftpStream.status(reqid, STATUS_CODE.FAILURE);
          }
        });

        sftpStream.on('MKDIR', (reqid, clientPath, attrs) => {
          const localPath = path.join(userSandboxDir, clientPath.replace(/^\/+/, ''));
          try {
            fs.mkdirSync(localPath, { recursive: true });
            sftpStream.status(reqid, STATUS_CODE.OK);
          } catch (err) {
            sftpStream.status(reqid, STATUS_CODE.FAILURE);
          }
        });

        sftpStream.on('REMOVE', (reqid, clientPath) => {
          sftpStream.status(reqid, STATUS_CODE.OK);
        });

        sftpStream.on('RMDIR', (reqid, clientPath) => {
          sftpStream.status(reqid, STATUS_CODE.OK);
        });

        sftpStream.on('RENAME', (reqid, oldPath, newPath) => {
          sftpStream.status(reqid, STATUS_CODE.OK);
        });
      });
    });
  }).on('close', () => {
    console.log('[SSH] Client disconnected.');
  }).on('error', (err) => {
    console.error('[SSH] Connection error:', err.message);
  });
});

sftpServer.listen(PORT, '0.0.0.0', () => {
  console.log('\n╔══════════════════════════════════════════════════╗');
  console.log('║       AURA STREAM — SFTP Gateway (Cloud)         ║');
  console.log('╠══════════════════════════════════════════════════╣');
  console.log(`║  SFTP Port   : ${PORT}`);
  console.log(`║  Protocol    : SSH / SFTP (Single Port)          ║`);
  console.log(`║  Auth Secret : ${uploadSecret ? '✅ Set' : '⚠️ NOT SET'}`);
  console.log('╠══════════════════════════════════════════════════╣');
  console.log('║  Camera Settings:                                ║');
  console.log('║   Protocol → SFTP (SSH File Transfer)            ║');
  console.log('║   Port     → (Your Railway Public Port)          ║');
  console.log('║   Username → event_<eventId>                     ║');
  console.log(`║   Password → ${uploadSecret || '(none)'}`);
  console.log('╚══════════════════════════════════════════════════╝\n');
});
