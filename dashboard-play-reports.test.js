'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const {createPlayReports} = require('./dashboard-play-reports');
const {PACKAGE, aggregateInstalls} = require('./dashboard-data');
const BUCKET = 'pubsite_prod_6971140736608519774';
const BASE = Date.parse('2026-10-24T12:00:00Z');
const DAY = 86400000;
const row = (date, installs = 4, country = 'UA') => ({date, country, installs, uninstalls: 1, installed: 12, activeInstalled: 12, newUsers: null, lostUsers: null, totalUsers: null});
const csv = rows => Buffer.from('Date,Package name,Country,Install events,Uninstall events,Active Device Installs\n' + rows.map(r => `${r.date},${PACKAGE},${r.country},${r.installs},${r.uninstalls},${r.installed}`).join('\n'));
function httpError(status, reason = 'forbidden') { return Object.assign(new Error('private response must not escape'), {response: {status}, reportReason: reason}); }
function deferred() { let resolve; const promise = new Promise(r => {resolve = r;}); return {promise, resolve}; }
function storage(initial = {}) {
  const data = new Map(Object.entries(initial));
  const counters = {reads: 0, commits: 0};
  const controls = {failRead: false, failWrite: false};
  const db = {
    collection(name) {
      const q = {
        orderBy() {return q;}, limit() {return q;},
        async get() {
          counters.reads++; if (controls.failRead) throw new Error('storage unavailable');
          return {docs: [...data].filter(([key]) => key.startsWith(name + '/')).sort().map(([key, value]) => ({id: key.slice(name.length + 1), data: () => structuredClone(value)}))};
        },
        doc(id) {return {name, id, async get() {counters.reads++; if (controls.failRead) throw new Error('storage unavailable'); return {data: () => structuredClone(data.get(name + '/' + id))};}};},
      }; return q;
    },
    batch() {
      const changes = [];
      return {set(ref, value) {changes.push([ref.name + '/' + ref.id, structuredClone(value)]);}, async commit() {counters.commits++; if (controls.failWrite) throw new Error('storage unavailable'); for (const [key, value] of changes) data.set(key, value);}};
    },
  };
  return {db, data, counters, controls};
}
function create(store, download, options = {}) {
  return createPlayReports({db: store.db, download, bucket: BUCKET, now: () => BASE, firstMonth: '2026-10', ...options});
}
function importFile(rows) {
  return {name: `installs_${PACKAGE}_${rows[0].date.slice(0, 7).replace('-', '')}_country.csv`, base64: csv(rows).toString('base64')};
}

test('get returns persisted data while Google is pending and concurrent callers share one read/download', async () => {
  const old = {rows: [row('2026-10-05')], importedAt: '2026-10-10T12:00:00Z'};
  const store = storage({'dashboard_play_installs/2026-10-05': old}), wait = deferred();
  let calls = 0;
  const reports = create(store, async ({bucket, name, signal}) => {calls++; assert.equal(bucket, BUCKET); assert.equal(name, `stats/installs/installs_${PACKAGE}_202610_country.csv`); assert.ok(signal); return wait.promise;});
  const results = await Promise.all(Array.from({length: 20}, () => reports.get()));
  assert.equal(calls, 1); assert.equal(store.counters.reads, 2);
  assert.ok(results.every(r => r.refreshing && r.dataThrough === '2026-10-05'));
  wait.resolve(csv([row('2026-10-20')])); await reports.refresh();
  assert.equal(reports.status().dataThrough, '2026-10-20');
});

test('403 preserves imported history and reports its age instead of fresh zeroes', async () => {
  const store = storage({'dashboard_play_installs/2026-10-05': {rows: [row('2026-10-05')], importedAt: '2026-10-10T12:00:00Z'}});
  const reports = create(store, async () => {throw httpError(403);});
  const result = await reports.refresh();
  assert.equal(result.ok, true); assert.equal(result.mode, 'imported'); assert.equal(result.automaticError, 'access_denied');
  assert.equal(result.dataThrough, '2026-10-05'); assert.equal(result.ageDays, 19); assert.equal(result.stale, true);
  assert.equal(result.syncedAt, '2026-10-10T12:00:00Z'); assert.equal(result.lastSuccessAt, null);
  assert.equal(JSON.stringify(result).includes('private response'), false);
});

test('403 with no stored reports remains unavailable; unsafe reason is not exposed', async () => {
  const reports = create(storage(), async () => {throw httpError(403, 'secret user@example.com');});
  const result = await reports.refresh();
  assert.equal(result.ok, false); assert.equal(result.error, 'access_denied'); assert.equal(result.reason, 'unknown');
  assert.equal(aggregateInstalls(result.rows, '2026-10-01', '2026-10-24').installs, null);
});

test('successful reports survive restart and a later Google outage', async () => {
  const store = storage(); let time = BASE;
  const reports = create(store, async () => csv([row('2026-10-20')]), {now: () => time});
  const initial = await reports.refresh();
  assert.equal(initial.mode, 'automatic'); assert.equal(initial.lastSuccessAt, new Date(BASE).toISOString());
  assert.equal(store.data.get('dashboard_play_installs/2026-10-20').source, 'automatic');
  time += 7 * 3600000;
  const restarted = create(store, async () => {throw httpError(403);}, {now: () => time});
  const fallback = await restarted.refresh();
  assert.equal(fallback.mode, 'automatic'); assert.equal(fallback.dataThrough, '2026-10-20');
  assert.equal(fallback.automaticError, 'access_denied'); assert.equal(fallback.lastSuccessAt, initial.lastSuccessAt);
});

test('one failing month does not discard a successful current month or stored history', async () => {
  const store = storage({'dashboard_play_installs/2026-09-30': {rows: [row('2026-09-30')], importedAt: '2026-10-05T12:00:00Z'}});
  const reports = create(store, async ({name}) => {if (name.includes('_202609_')) throw httpError(500); return csv([row('2026-10-20')]);}, {firstMonth: '2026-09'});
  const result = await reports.refresh();
  assert.equal(result.ok, true); assert.equal(result.partial, true); assert.equal(result.automaticError, 'sync_failed');
  assert.equal(result.files, 2); assert.equal(result.rows.length, 2);
  assert.equal(store.data.get('dashboard_play_installs/2026-10-20').rows[0].installs, 4);
});

test('all 404 responses are not interpreted as successful empty reports', async () => {
  const result = await create(storage(), async () => {throw httpError(404);}).refresh();
  assert.equal(result.ok, false); assert.equal(result.error, 'no_reports'); assert.equal(result.lastSuccessAt, null);
});

test('historical files are fetched once, persisted, and skipped after process restart', async () => {
  const store = storage(); let time = BASE; const calls = [];
  const download = async ({name}) => {calls.push(name); const m = name.match(/_(\d{4})(\d{2})_/); return csv([row(`${m[1]}-${m[2]}-01`)]);};
  const options = {firstMonth: '2026-04', now: () => time};
  await create(store, download, options).refresh(); assert.equal(calls.length, 7);
  const restarted = create(store, download, options);
  await restarted.refresh(); assert.equal(calls.length, 7);
  time += 6 * 3600000;
  await restarted.refresh(); assert.equal(calls.length, 9);
  assert.deepEqual(calls.slice(-2).map(n => n.match(/_(\d{6})_/)[1]), ['202610', '202609']);
});

test('forced refresh is rate limited and still coalesces requests', async () => {
  const store = storage(); let time = BASE, calls = 0;
  const reports = create(store, async () => {calls++; return csv([row('2026-10-20')]);}, {now: () => time});
  await reports.refresh();
  await Promise.all(Array.from({length: 20}, () => reports.refresh({force: true}))); assert.equal(calls, 1);
  time += 60000;
  await Promise.all(Array.from({length: 20}, () => reports.refresh({force: true}))); assert.equal(calls, 2);
});

test('normal Google delay is explicit and stale threshold uses report date, not download time', async () => {
  const store = storage(); let time = BASE;
  const reports = create(store, async () => csv([row('2026-10-20')]), {now: () => time});
  const result = await reports.refresh();
  assert.equal(result.ageDays, 4); assert.equal(result.stale, false); assert.deepEqual(result.expectedDelayDays, {min: 3, max: 7});
  time += 4 * DAY;
  assert.equal(reports.status().ageDays, 8); assert.equal(reports.status().stale, true);
});

test('repeated downloads dedupe by country/date and corrected days replace old country rows', async () => {
  const store = storage(); let time = BASE, revised = false;
  const reports = create(store, async () => csv(revised ? [row('2026-10-20', 5)] : [row('2026-10-20'), row('2026-10-20'), row('2026-10-20', 2, 'PL')]), {now: () => time});
  assert.equal((await reports.refresh()).rows.length, 2);
  revised = true; time += 6 * 3600000;
  const result = await reports.refresh();
  assert.equal(result.rows.length, 1); assert.equal(result.rows[0].installs, 5);
  assert.equal(store.data.get('dashboard_play_installs/2026-10-20').rows.length, 1);
});

test('manual imports remain compatible and repeated imports cannot double totals', async () => {
  const store = storage(), reports = create(store, async () => {throw httpError(403);});
  const files = [importFile([row('2026-10-20')])];
  assert.equal((await reports.importFiles(files)).days, 1);
  await reports.importFiles(files);
  assert.equal(store.data.size, 1); assert.equal(reports.status().rows.length, 1);
  assert.equal(reports.status().mode, 'imported'); assert.equal(reports.status().lastSuccessAt, null);
});

test('failed persistence retries all pending historical days without redownloading old months', async () => {
  const store = storage(); store.controls.failWrite = true;
  let time = BASE; const calls = [];
  const reports = create(store, async ({name}) => {calls.push(name); const m = name.match(/_(\d{4})(\d{2})_/); return csv([row(`${m[1]}-${m[2]}-01`)]);}, {firstMonth: '2026-04', now: () => time});
  assert.equal((await reports.refresh()).storageError, 'write_failed'); assert.equal(store.data.size, 0);
  store.controls.failWrite = false; time += 5 * 60000;
  const result = await reports.refresh();
  assert.equal(result.storageError, null); assert.equal(calls.length, 9);
  assert.equal([...store.data.keys()].filter(k => k.startsWith('dashboard_play_installs/')).length, 7);
});

test('storage read failure remains explicit and does not prevent remote recovery', async () => {
  const store = storage(); store.controls.failRead = true;
  const reports = create(store, async () => csv([row('2026-10-20')]));
  const result = await reports.refresh();
  assert.equal(result.ok, true); assert.equal(result.storageError, 'read_failed'); assert.equal(result.dataThrough, '2026-10-20');
});

test('month rollover fetches new and previous month while older files remain cached', async () => {
  const store = storage(); let time = BASE; const calls = [];
  const reports = create(store, async ({name}) => {calls.push(name); const m = name.match(/_(\d{4})(\d{2})_/); return csv([row(`${m[1]}-${m[2]}-01`)]);}, {firstMonth: '2026-09', now: () => time});
  await reports.refresh(); time = Date.parse('2026-11-01T12:00:00Z'); await reports.refresh();
  assert.deepEqual(calls.slice(-2).map(n => n.match(/_(\d{6})_/)[1]), ['202611', '202610']);
});

test('invalid bucket does not issue requests and remains explicitly unconfigured', async () => {
  let calls = 0; const reports = create(storage(), async () => {calls++;}, {bucket: ''});
  const result = await reports.refresh(); assert.equal(result.error, 'not_configured'); assert.equal(calls, 0);
});

test('invalid CSV cannot overwrite last valid daily report', async () => {
  const store = storage({'dashboard_play_installs/2026-10-20': {rows: [row('2026-10-20')], syncedAt: '2026-10-23T12:00:00Z', source: 'automatic'}});
  const result = await create(store, async () => csv([row('2026-10-20', -1)])).refresh();
  assert.equal(result.automaticError, 'sync_failed'); assert.equal(result.rows[0].installs, 4);
});

test('manual upload wins over an already running Google refresh', async () => {
  const store = storage(), wait = deferred();
  const reports = create(store, async () => wait.promise);
  await reports.get();
  const imported = reports.importFiles([importFile([row('2026-10-20', 9)])]);
  wait.resolve(csv([row('2026-10-20', 4)])); await imported;
  assert.equal(reports.status().rows[0].installs, 9);
  assert.equal(store.data.get('dashboard_play_installs/2026-10-20').source, 'imported');
});

test('pending failed automatic write cannot undo a later successful manual upload', async () => {
  const store = storage(); store.controls.failWrite = true;
  let time = BASE, failing = false;
  const reports = create(store, async () => {if (failing) throw httpError(403); return csv([row('2026-10-20', 4)]);}, {now: () => time});
  await reports.refresh(); store.controls.failWrite = false; time += 60000;
  await reports.importFiles([importFile([row('2026-10-20', 9)])]);
  time += 5 * 60000; failing = true; await reports.refresh();
  assert.equal(store.data.get('dashboard_play_installs/2026-10-20').rows[0].installs, 9);
  assert.equal(reports.status().rows[0].installs, 9);
});

test('invalid upload exposes the existing HTTP 400 contract before any storage access', async () => {
  const store = storage(), reports = create(store, async () => {throw new Error('must not download');});
  for (const files of [[], null, [{name: 'secret.csv', base64: ''}]]) {
    await assert.rejects(reports.importFiles(files), error => error.message === 'invalid_reports' && error.status === 400);
  }
  assert.equal(store.counters.reads, 0); assert.equal(store.counters.commits, 0);
});
