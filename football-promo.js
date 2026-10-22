'use strict';
const { createReadStream, statSync } = require('node:fs');
const { join } = require('node:path');

// Only these public files are served. File metadata is read once at startup;
// video bytes are streamed with backpressure, never buffered on the game loop.
function createPromoHandler(root = join(__dirname, 'public', 'galaxy-football')) {
  const types = {
    'index.html': 'text/html; charset=utf-8',
    'style.css': 'text/css; charset=utf-8',
    'app.js': 'text/javascript; charset=utf-8',
    'assets/logo.png': 'image/png',
    'assets/icon.png': 'image/png',
    'assets/cover.png': 'image/png',
    'assets/poster.jpg': 'image/jpeg',
    'assets/gameplay.mp4': 'video/mp4',
  };
  const files = new Map(Object.entries(types).map(([name, type]) => {
    const path = join(root, name.replace(/^assets\//, '')), stat = statSync(path);
    return [name, { path, type, size: stat.size,
      etag: `W/"${stat.size.toString(16)}-${Math.trunc(stat.mtimeMs).toString(16)}"` }];
  }));
  return function handlePromo(req, res) {
    const [pathname, query = ''] = req.url.split('?');
    if (pathname !== '/galaxy-football' && !pathname.startsWith('/galaxy-football/')) return false;
    const end = (status, headers = {}) => { res.writeHead(status, headers); res.end(); return true; };
    if (!['GET', 'HEAD'].includes(req.method)) return end(405, { Allow: 'GET, HEAD' });
    if (pathname === '/galaxy-football') return end(308, { Location: '/galaxy-football/' + (query ? '?' + query : '') });
    const name = pathname.slice('/galaxy-football/'.length) || 'index.html';
    const file = files.get(name);
    if (!file) return end(404);
    const headers = {
      'Content-Type': file.type,
      'Cache-Control': `public, max-age=${name === 'index.html' ? 60 : 3600}`,
      ETag: file.etag,
      'X-Content-Type-Options': 'nosniff',
      'Referrer-Policy': 'strict-origin-when-cross-origin',
      'Accept-Ranges': 'bytes',
    };
    if (req.headers['if-none-match']?.split(',').map(value => value.trim()).some(value => value === file.etag || value === '*')) return end(304, headers);
    let start = 0, endByte = file.size - 1, status = 200;
    // Weak ETags cannot satisfy If-Range; send the full representation then.
    if (req.headers.range && !req.headers['if-range'] && req.method === 'GET') {
      const range = /^bytes=(\d*)-(\d*)$/.exec(req.headers.range);
      if (!range || (!range[1] && !range[2])) return end(416, { ...headers, 'Content-Range': `bytes */${file.size}` });
      if (!range[1]) start = Math.max(0, file.size - Number(range[2]));
      else { start = Number(range[1]); if (range[2]) endByte = Math.min(endByte, Number(range[2])); }
      if (!Number.isSafeInteger(start) || !Number.isSafeInteger(endByte) || start < 0 || start >= file.size || endByte < start) return end(416, { ...headers, 'Content-Range': `bytes */${file.size}` });
      status = 206;
      headers['Content-Range'] = `bytes ${start}-${endByte}/${file.size}`;
    }
    headers['Content-Length'] = endByte - start + 1;
    if (req.method === 'HEAD') return end(200, headers);
    const stream = createReadStream(file.path, { start, end: endByte });
    stream.once('open', () => { if (!res.destroyed) { res.writeHead(status, headers); stream.pipe(res); } });
    stream.once('error', () => { if (!res.headersSent) end(503, { 'Cache-Control': 'no-store' }); else res.destroy(); });
    res.once('close', () => stream.destroy());
    return true;
  };
}
module.exports = { createPromoHandler };
