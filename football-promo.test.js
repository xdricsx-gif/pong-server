'use strict';
const { test, before, after } = require('node:test');
const assert = require('node:assert/strict');
const { createServer } = require('node:http');
const { readFileSync } = require('node:fs');
const { join } = require('node:path');
const { createPromoHandler } = require('./football-promo');
let server, origin;
before(async () => {
  const handle = createPromoHandler();
  server = createServer((req, res) => {
    if (handle(req, res)) return;
    res.writeHead(200); res.end('existing-route');
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  origin = `http://127.0.0.1:${server.address().port}`;
});
after(() => new Promise(resolve => { server.closeAllConnections(); server.close(resolve); }));
test('landing HTML is public and has correct server and store links', async () => {
  const response = await fetch(origin + '/galaxy-football/?utm_source=meta');
  assert.equal(response.status, 200);
  assert.match(response.headers.get('content-type'), /text\/html/);
  const html = await response.text();
  assert.match(html, /https:\/\/pong-server-production-9f1f.up.railway.app\/galaxy-football\//);
  assert.match(html, /com.galaxyfootball.app/);
  assert.match(html, /preload="none"/);
  assert.doesNotMatch(html, /chatgpt.site|http-equiv="refresh"/);
});
test('trailing slash redirect preserves campaign query', async () => {
  const r = await fetch(origin + '/galaxy-football?utm_source=meta', { redirect: 'manual' });
  assert.equal(r.status, 308);
  assert.equal(r.headers.get('location'), '/galaxy-football/?utm_source=meta');
});
test('static assets support cache revalidation and HEAD', async () => {
  const r = await fetch(origin + '/galaxy-football/style.css', { method: 'HEAD' });
  assert.equal(r.status, 200); assert.ok(Number(r.headers.get('content-length')) > 0);
  assert.equal(await r.text(), '');
  const cached = await fetch(origin + '/galaxy-football/style.css', { headers: { 'If-None-Match': r.headers.get('etag') } });
  assert.equal(cached.status, 304); assert.equal(await cached.text(), '');
});
test('video supports precise byte ranges and suffix ranges', async () => {
  const bytes = readFileSync(join(__dirname, 'public/galaxy-football/gameplay.mp4'));
  for (const [range, expected] of [['bytes=0-1023', bytes.subarray(0, 1024)], ['bytes=-128', bytes.subarray(-128)]]) {
    const r = await fetch(origin + '/galaxy-football/assets/gameplay.mp4', { headers: { Range: range } });
    assert.equal(r.status, 206); assert.equal(r.headers.get('content-type'), 'video/mp4');
    assert.deepEqual(Buffer.from(await r.arrayBuffer()), expected);
  }
});
test('unsatisfiable or malformed ranges do not stream the full video', async () => {
  for (const range of ['bytes=999999999-', 'bytes=5-3', 'bytes=0-5,8-12', 'bytes=-0', 'bytes=-']) {
    const r = await fetch(origin + '/galaxy-football/assets/gameplay.mp4', { headers: { Range: range } });
    assert.equal(r.status, 416); assert.equal(await r.text(), '');
  }
});
test('unknown files and encoded traversal cannot expose server files', async () => {
  for (const path of ['.env', 'server.js', '%2e%2e%2fserver.js', 'assets/unknown.png']) {
    const r = await fetch(origin + '/galaxy-football/' + path); assert.equal(r.status, 404);
  }
  assert.equal((await fetch(origin + '/galaxy-football/', { method: 'POST' })).status, 405);
});
test('game, dashboard and privacy routes remain outside the promo handler', async () => {
  for (const path of ['/health', '/config.json', '/dashboard/', '/socket.io/?EIO=4', '/privacy']) {
    assert.equal(await (await fetch(origin + path)).text(), 'existing-route');
  }
});
