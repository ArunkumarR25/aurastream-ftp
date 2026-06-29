/**
 * AURA STREAM — Cloud FTP Upload Gateway
 *
 * Deploy this on Railway.app (free).
 * Cameras connect via FTP → photos auto-upload to Supabase → guest stream live.
 *
 * Required environment variables (set in Railway dashboard):
 *   NEXT_PUBLIC_SUPABASE_URL     — your Supabase project URL
 *   SUPABASE_SERVICE_ROLE_KEY    — Supabase service role key (never expose publicly)
 *   UPLOAD_SECRET                — password cameras use to authenticate
 *   PASV_URL                     — public IP/hostname of this server (Railway assigns this)
 *   FTP_PORT                     — port to listen on (default 2121, Railway may override)
 */

const fs = require('fs');
const path = require('path');
const os = require('os');
const FtpSrv = require('ftp-srv');
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');

// ── 1. Environment Variables ─────────────────────────────────────────────────
const supabaseUrl  = process.env.NEXT_PUBLIC_SUPABASE_URL;
const supabaseKey  = process.env.SUPABASE_SERVICE_ROLE_KEY;
const uploadSecret = process.env.UPLOAD_SECRET;

// Railway assigns a hostname; fall back to local IP for local dev
const PASV_URL = process.env.PASV_URL || getLocalIp();
const FTP_PORT = parseInt(process.env.PORT || process.env.FTP_PORT || '2121', 10);

if (!supabaseUrl || !supabaseKey) {
  console.error('❌  Missing NEXT_PUBLIC_SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY');
  process.exit(1);
}

// ── 2. Supabase Admin Client ─────────────────────────────────────────────────
const supabase = createClient(supabaseUrl, supabaseKey, {
  auth: { persistSession: false },
  realtime: { transport: ws },
});

// ── 3. Temp directory for incoming FTP files ─────────────────────────────────
const TEMP_DIR = path.join(os.tmpdir(), 'aura-ftp-temp');
fs.mkdirSync(TEMP_DIR, { recursive: true });

function getLocalIp() {
  const nets = os.networkInterfaces();
  for (const ifaces of Object.values(nets)) {
    for (const iface of ifaces) {
      if (iface.family === 'IPv4' && !iface.internal) return iface.address;
    }
  }
  return '127.0.0.1';
}

// ── 4. FTP Server ────────────────────────────────────────────────────────────
const ftpServer = new FtpSrv({
  url:      `ftp://0.0.0.0:${FTP_PORT}`,
  pasv_url: PASV_URL,
  pasv_min: 10000,
  pasv_max: 10100,
  anonymous: false,
  greeting:  'AuraStream FTP Gateway — ready',
});

// Authenticate incoming camera connections
ftpServer.on('login', async ({ username, password }, resolve, reject) => {
  console.log(`[AUTH] Attempt — user: "${username}"`);

  // Username format: event_<eventId>  or just <eventId>
  const eventId = username.startsWith('event_') ? username.slice(6) : username;

  // Check upload secret (password)
  if (uploadSecret && password !== uploadSecret) {
    console.log(`[AUTH] ❌ Wrong password for "${username}"`);
    return reject(new Error('Invalid password'));
  }

  // Verify event exists in Supabase
  const { data: event, error } = await supabase
    .from('events')
    .select('id, event_name')
    .eq('id', eventId)
    .single();

  if (error || !event) {
    console.log(`[AUTH] ❌ Event "${eventId}" not found`);
    return reject(new Error('Event not found'));
  }

  console.log(`[AUTH] ✅ Authenticated — Event: "${event.event_name}" (${eventId})`);

  // Give camera its own sandbox folder
  const eventDir = path.join(TEMP_DIR, eventId);
  fs.mkdirSync(eventDir, { recursive: true });
  resolve({ root: eventDir });
});

// ── 5. Background Watcher — upload finished files to Supabase ────────────────
const processing = new Set();
const VALID_EXTS  = ['jpg', 'jpeg', 'png', 'heic', 'heif', 'cr2', 'cr3', 'nef', 'arw', 'dng'];

setInterval(async () => {
  if (!fs.existsSync(TEMP_DIR)) return;

  for (const eventId of fs.readdirSync(TEMP_DIR)) {
    const eventPath = path.join(TEMP_DIR, eventId);
    if (!fs.statSync(eventPath).isDirectory()) continue;

    for (const filename of fs.readdirSync(eventPath)) {
      if (filename.startsWith('.')) continue;

      const filePath = path.join(eventPath, filename);
      if (processing.has(filePath)) continue;

      const ext = filename.split('.').pop()?.toLowerCase();
      if (!VALID_EXTS.includes(ext)) continue;

      const stat1 = fs.statSync(filePath);
      processing.add(filePath);

      // Wait 800ms and re-check size — ensures camera finished writing
      await new Promise(r => setTimeout(r, 800));
      if (!fs.existsSync(filePath)) { processing.delete(filePath); continue; }

      const stat2 = fs.statSync(filePath);
      if (stat2.size !== stat1.size || stat1.size === 0) {
        processing.delete(filePath); // still writing
        continue;
      }

      // ── Upload to Supabase ────────────────────────────────────────────────
      console.log(`[UPLOAD] 📷 ${filename} (${eventId}) — ${(stat2.size / 1024).toFixed(0)} KB`);
      try {
        const buffer      = fs.readFileSync(filePath);
        const storagePath = `events/${eventId}/${Date.now()}-${Math.random().toString(36).slice(2, 8)}.${ext}`;
        const mime        = ext === 'png' ? 'image/png' : 'image/jpeg';

        const { error: uploadErr } = await supabase.storage
          .from('wedding-photos')
          .upload(storagePath, buffer, { contentType: mime, cacheControl: '3600', upsert: false });
        if (uploadErr) throw uploadErr;

        const { data: urlData } = supabase.storage.from('wedding-photos').getPublicUrl(storagePath);

        const { error: dbErr } = await supabase
          .from('event_images')
          .insert({ event_id: eventId, storage_path: storagePath, public_url: urlData.publicUrl });
        if (dbErr) throw dbErr;

        console.log(`[UPLOAD] ✅ Done — ${filename}`);
        fs.unlinkSync(filePath);
      } catch (err) {
        console.error(`[UPLOAD] ❌ Failed — ${filename}:`, err.message);
      } finally {
        processing.delete(filePath);
      }
    }
  }
}, 1500);

// ── 6. Start ─────────────────────────────────────────────────────────────────
ftpServer.listen().then(() => {
  console.log('\n╔══════════════════════════════════════════════════╗');
  console.log('║       AURA STREAM — FTP Gateway  (Cloud)         ║');
  console.log('╠══════════════════════════════════════════════════╣');
  console.log(`║  FTP Port    : ${FTP_PORT}`);
  console.log(`║  Passive IP  : ${PASV_URL}`);
  console.log(`║  Auth Secret : ${uploadSecret ? '✅ set' : '⚠️  NOT SET (open access!)'}`);
  console.log('╠══════════════════════════════════════════════════╣');
  console.log('║  Camera settings:                                ║');
  console.log(`║   Host     → ${PASV_URL}`);
  console.log(`║   Port     → ${FTP_PORT}`);
  console.log('║   Username → event_<eventId>                     ║');
  console.log(`║   Password → ${uploadSecret || '(none)'}`);
  console.log('╚══════════════════════════════════════════════════╝\n');
});
