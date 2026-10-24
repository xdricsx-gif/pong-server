'use strict';
const {PACKAGE, day, parseInstalls, validateReportUpload} = require('./dashboard-data');
const DAY = 86400000, SIX_HOURS = 6 * 3600000, RETRY = 5 * 60000, FORCE_INTERVAL = 60000;
const DAILY_COLLECTION = 'dashboard_play_installs', META_COLLECTION = 'dashboard_meta', META_DOCUMENT = 'play_reports';
function monthBefore(month) {
  const d = new Date(`${month}-01T00:00:00Z`);
  d.setUTCMonth(d.getUTCMonth() - 1);
  return d.toISOString().slice(0, 7);
}
function errorKind(error) {
  const status = Number(error?.response?.status || error?.status);
  if (status === 403 || status === 401) return 'access_denied';
  if (status === 404) return 'not_found';
  return 'sync_failed';
}
// Only safe reason codes reach the dashboard, never a transport response body.
function safeReason(error) {
  const value = error?.reportReason;
  return typeof value === 'string' && /^[A-Za-z_]{1,60}$/.test(value) ? value : 'unknown';
}
/**
 * download({bucket, name, signal}) returns original CSV bytes and should honor
 * signal/its own timeout. get() waits for persistence only, never Google.
 * All records stay in the existing private admin collections.
 */
function createPlayReports({db, download, bucket, now = Date.now, firstMonth = '2026-04'}) {
  if (!/^\d{4}-(0[1-9]|1[0-2])$/.test(firstMonth)) throw new Error('invalid_first_month');
  const daily = new Map(), pendingDocuments = new Map();
  let flatRows = null, hydrated = false, hydration = null, hydrationRetryAt = 0;
  let flight = null, nextAttemptAt = 0, storageError = null, importing = 0;
  let meta = {checkedMonths: [], lastAttemptAt: null, lastSuccessAt: null, error: null, reason: null, partial: false};
  const clock = () => +now();
  const iso = () => new Date(clock()).toISOString();
  const timestamp = document => document.syncedAt || document.importedAt || '';
  function applyDocuments(documents) {
    for (const document of documents) {
      if (!Array.isArray(document.rows)) continue;
      const date = document.rows[0]?.date;
      if (!date || day(date) !== date) continue;
      const old = daily.get(date);
      if (old && timestamp(old) > timestamp(document)) continue;
      daily.set(date, {...document, rows: [...new Map(document.rows.filter(r => r.date === date).map(r => [r.country, r])).values()]});
    }
    flatRows = null;
  }
  function status() {
    if (!flatRows) flatRows = [...daily.values()].flatMap(d => d.rows).sort((a, b) => a.date.localeCompare(b.date) || a.country.localeCompare(b.country));
    const latest = flatRows.at(-1)?.date || null;
    const latestDocument = latest ? daily.get(latest) : null;
    const syncedAt = [...daily.values()].map(timestamp).filter(Boolean).sort().at(-1) || null;
    const ageDays = latest ? Math.floor((Date.parse(day(new Date(clock()))) - Date.parse(latest)) / DAY) : null;
    const error = meta.error || (!flatRows.length ? 'not_yet_synced' : null);
    return {
      ok: flatRows.length > 0, rows: flatRows,
      files: new Set(flatRows.map(r => r.date.slice(0, 7))).size,
      syncedAt, mode: latestDocument?.source === 'automatic' ? 'automatic' : 'imported',
      ...(flatRows.length ? (error ? {automaticError: error} : {}) : {error}),
      reason: meta.reason, lastAttemptAt: meta.lastAttemptAt,
      lastSuccessAt: meta.lastSuccessAt, dataThrough: latest,
      ageDays, stale: ageDays === null || ageDays > 7,
      refreshing: Boolean(flight), partial: Boolean(meta.partial), storageError,
      expectedDelayDays: {min: 3, max: 7},
    };
  }
  async function hydrate() {
    if (hydration) return hydration;
    if (hydrated || clock() < hydrationRetryAt) return;
    hydration = (async () => {
      const results = await Promise.allSettled([
        Promise.resolve().then(() => db.collection(DAILY_COLLECTION).orderBy('__name__').limit(5000).get()),
        Promise.resolve().then(() => db.collection(META_COLLECTION).doc(META_DOCUMENT).get()),
      ]);
      if (results[0].status === 'fulfilled') applyDocuments(results[0].value.docs.map(d => d.data()));
      if (results[1].status === 'fulfilled') {
        const saved = results[1].value.data();
        // Do not overwrite a refresh completed while a storage read retried.
        if (saved && (!meta.lastAttemptAt || saved.lastAttemptAt > meta.lastAttemptAt)) {
          meta = {...meta, ...saved, checkedMonths: Array.isArray(saved.checkedMonths) ? saved.checkedMonths : []};
          nextAttemptAt = (Date.parse(meta.lastAttemptAt) || 0) + (meta.error || meta.partial ? RETRY : SIX_HOURS);
        }
      }
      hydrated = results.every(r => r.status === 'fulfilled');
      storageError = hydrated ? null : 'read_failed';
      if (!hydrated) hydrationRetryAt = clock() + RETRY;
    })();
    try { await hydration; } finally { hydration = null; }
  }
  async function persist(documents, saveMeta = true) {
    for (let offset = 0; offset < documents.length; offset += 400) {
      const batch = db.batch();
      for (const document of documents.slice(offset, offset + 400)) batch.set(db.collection(DAILY_COLLECTION).doc(document.rows[0].date), document);
      await batch.commit();
    }
    if (saveMeta) {
      const batch = db.batch();
      batch.set(db.collection(META_COLLECTION).doc(META_DOCUMENT), meta);
      await batch.commit();
    }
  }
  function monthsToFetch() {
    const current = day(new Date(clock())).slice(0, 7), previous = monthBefore(current);
    const recent = [current, previous].filter(m => m >= firstMonth);
    const known = new Set(meta.checkedMonths), history = [];
    // At most 12 historical files per attempt; persist checked old months so a
    // restart does not download the entire history again.
    for (let month = monthBefore(previous); month >= firstMonth && history.length < 12; month = monthBefore(month)) {
      if (!known.has(month)) history.push(month);
    }
    return [...recent, ...history];
  }
  async function readMonth(month) {
    const name = `stats/installs/installs_${PACKAGE}_${month.replace('-', '')}_country.csv`;
    const controller = new AbortController();
    let timer;
    const timeout = new Promise((resolve, reject) => {
      timer = setTimeout(() => { controller.abort(); reject(new Error('download_timeout')); }, 25000);
      timer.unref?.();
    });
    try {
      const bytes = await Promise.race([Promise.resolve().then(() => download({bucket, name, signal: controller.signal})), timeout]);
      const buffer = Buffer.from(bytes);
      if (buffer.length > 10000000) throw new Error('report_too_large');
      const rows = parseInstalls(buffer);
      if (!rows.length || rows.length > 10000 || rows.some(r => r.country.length > 80 || r.installs === null || r.uninstalls === null || r.installed === null || day(r.date) !== r.date || !r.date.startsWith(month) || r.date > day(new Date(clock())) ||
        Object.entries(r).some(([key, value]) => !['date', 'country'].includes(key) && value !== null && (!Number.isSafeInteger(value) || value < 0)))) throw new Error('invalid_report');
      return rows;
    } finally { clearTimeout(timer); }
  }
  async function sync() {
    meta.lastAttemptAt = iso(); meta.error = null; meta.reason = null; meta.partial = false;
    if (!bucket || !/^pubsite_prod_(rev_)?\d+$/.test(bucket)) {
      meta.error = 'not_configured'; nextAttemptAt = clock() + RETRY; return;
    }
    const months = monthsToFetch(), rows = [], checked = new Set(meta.checkedMonths), failures = [];
    let cursor = 0, found = 0, denied = false;
    async function worker() {
      while (cursor < months.length && !denied) {
        const month = months[cursor++];
        try { rows.push(...await readMonth(month)); found++; checked.add(month); }
        catch (error) {
          const kind = errorKind(error);
          if (kind === 'not_found') checked.add(month);
          else { failures.push({error: kind, reason: safeReason(error)}); if (kind === 'access_denied') denied = true; }
        }
      }
    }
    await Promise.all([worker(), worker()]);
    const syncedAt = iso(), groups = new Map();
    for (const row of rows) {
      if (!groups.has(row.date)) groups.set(row.date, new Map());
      groups.get(row.date).set(row.country, row);
    }
    const documents = [...groups.values()].map(group => ({rows: [...group.values()], syncedAt, source: 'automatic'}));
    applyDocuments(documents); meta.checkedMonths = [...checked].sort();
    if (found) meta.lastSuccessAt = syncedAt;
    const failure = failures.find(f => f.error === 'access_denied') || failures[0];
    meta.error = failure?.error || (!found ? 'no_reports' : null);
    meta.reason = failure?.reason || null; meta.partial = Boolean(failure && found);
    nextAttemptAt = clock() + (meta.error ? RETRY : SIX_HOURS);
    try {
      for (const document of documents) pendingDocuments.set(document.rows[0].date, document);
      const pending = [...pendingDocuments.values()];
      await persist(pending);
      for (const document of pending) if (pendingDocuments.get(document.rows[0].date) === document) pendingDocuments.delete(document.rows[0].date);
      storageError = hydrated ? null : 'read_failed';
    }
    catch { storageError = 'write_failed'; nextAttemptAt = clock() + RETRY; }
  }
  function start(force) {
    if (importing) return Promise.resolve(status());
    if (flight) return flight;
    const elapsed = clock() - (Date.parse(meta.lastAttemptAt) || 0);
    if (force ? elapsed < FORCE_INTERVAL : clock() < nextAttemptAt) return Promise.resolve(status());
    flight = Promise.resolve().then(sync).catch(() => {
      meta.error = 'sync_failed'; meta.reason = 'unknown'; nextAttemptAt = clock() + RETRY;
    }).finally(() => { flight = null; });
    return flight;
  }
  async function get({refresh = false} = {}) {
    await hydrate();
    // start consumes failures so background refresh cannot reject unhandled.
    start(refresh);
    return status();
  }
  async function refresh({force = false} = {}) {
    await hydrate(); await start(force); return status();
  }
  async function importFiles(files) {
    let rows;
    try { rows = validateReportUpload(files); }
    catch { const error = new Error('invalid_reports'); error.status = 400; throw error; }
    importing++;
    try {
      // A late background download must not overwrite the newer manual upload.
      if (flight) await flight;
      const importedAt = iso(), groups = new Map();
      for (const row of rows) { if (!groups.has(row.date)) groups.set(row.date, []); groups.get(row.date).push(row); }
      const documents = [...groups.values()].map(dayRows => ({rows: dayRows, importedAt, source: 'imported'}));
      // A manual import does not imply a successful automatic synchronization.
      await persist(documents, false); applyDocuments(documents);
      for (const document of documents) pendingDocuments.delete(document.rows[0].date);
      return {ok: true, days: documents.length, importedAt};
    } finally { importing--; }
  }
  return {get, refresh, importFiles, status};
}
module.exports = {createPlayReports};



