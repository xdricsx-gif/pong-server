// SERVER FINAL — Authoritative 60 tick
const { createServer } = require('http');
const { Server } = require('socket.io');
const { Pool } = require('pg');

const httpServer = createServer();
const io = new Server(httpServer, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  pingInterval: 5000,
  pingTimeout: 10000,
});

const PORT = process.env.PORT || 3000;

// ══════════════════════════════════════════════════════
// POSTGRESQL ANALYTICS
// ══════════════════════════════════════════════════════
let pgPool = null;

async function initPostgres() {
  if (!process.env.DATABASE_URL) {
    console.log('[Analytics] DATABASE_URL not set — analytics disabled');
    return;
  }
  try {
    pgPool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS players (
        uid TEXT PRIMARY KEY,
        first_seen TIMESTAMPTZ DEFAULT NOW(),
        last_seen TIMESTAMPTZ DEFAULT NOW(),
        total_games INT DEFAULT 0,
        total_wins INT DEFAULT 0,
        peak_rating INT DEFAULT 500,
        total_playtime_sec INT DEFAULT 0
      );
      CREATE TABLE IF NOT EXISTS game_events (
        id SERIAL PRIMARY KEY,
        ts TIMESTAMPTZ DEFAULT NOW(),
        event_type TEXT NOT NULL,
        uid TEXT,
        room_id TEXT,
        mode TEXT,
        place INT,
        duration_sec INT,
        rating_delta INT,
        bots_count INT,
        players_count INT,
        goals_conceded INT
      );
      CREATE TABLE IF NOT EXISTS daily_stats (
        date DATE PRIMARY KEY,
        dau INT DEFAULT 0,
        games_played INT DEFAULT 0,
        avg_game_duration_sec INT DEFAULT 0,
        new_registrations INT DEFAULT 0,
        total_playtime_sec BIGINT DEFAULT 0
      );
    `);
    // Idempotent migrations — для кейсу, якщо таблиці вже створені без цих полів
    await pgPool.query(`
      ALTER TABLE players    ADD COLUMN IF NOT EXISTS peak_rating INT DEFAULT 500;
      ALTER TABLE game_events ADD COLUMN IF NOT EXISTS rating_delta INT;
    `);
    console.log('[Analytics] PostgreSQL connected and tables ready');
  } catch(e) {
    console.error('[Analytics] PostgreSQL init error:', e.message);
    pgPool = null;
  }
}

async function trackEvent(eventType, data = {}) {
  if (!pgPool) return;
  try {
    await pgPool.query(
      `INSERT INTO game_events (event_type, uid, room_id, mode, place, duration_sec, rating_delta, bots_count, players_count, goals_conceded)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
      [eventType, data.uid||null, data.roomId||null, data.mode||null,
       data.place??null, data.durationSec??null, data.ratingDelta??null,
       data.botsCount??null, data.playersCount??null, data.goalsConceded??null]
    );
  } catch(e) { console.error('[Analytics] trackEvent error:', e.message); }
}

// Автоматично визначає, чи це перше входження UID.
// RETURNING xmax — якщо 0, рядок щойно вставлений (новий гравець);
// якщо ненульове — upsert (гравець існував).
async function trackPlayerSeen(uid, platform = null) {
  if (!pgPool || !uid) return;
  try {
    const res = await pgPool.query(
      `INSERT INTO players (uid) VALUES ($1)
       ON CONFLICT (uid) DO UPDATE SET last_seen=NOW()
       RETURNING (xmax = 0) AS is_new`,
      [uid]
    );
    const isNew = res.rows[0] && res.rows[0].is_new === true;

    // Daily active users + (якщо новий) daily new registrations.
    if (isNew) {
      await pgPool.query(
        `INSERT INTO daily_stats (date, new_registrations) VALUES (CURRENT_DATE, 1)
         ON CONFLICT (date) DO UPDATE SET new_registrations = daily_stats.new_registrations + 1`
      );
    }
    await pgPool.query(
      `INSERT INTO daily_stats (date, dau) VALUES (CURRENT_DATE, 1)
       ON CONFLICT (date) DO UPDATE
       SET dau = (SELECT COUNT(DISTINCT uid) FROM game_events
                  WHERE ts::date = CURRENT_DATE AND uid IS NOT NULL)`,
    );

    // Зберігаємо подію реєстрації з платформою (для ретеншн-аналізу).
    if (isNew) {
      await trackEvent('player_registered', { uid, platform });
    }
  } catch(e) { console.error('[Analytics] trackPlayerSeen error:', e.message); }
}

async function trackGameEnd(room, winnerSlot, places, isTraining, ratingDeltas = {}) {
  if (!pgPool) return;
  try {
    const durationSec = room.startedAt ? Math.round((Date.now() - room.startedAt) / 1000) : null;
    const botsCount = Object.keys(room.bots || {}).filter(s => room.bots[s]).length;
    const playersCount = Object.keys(room.players).length;
    const mode = isTraining ? 'training' : (botsCount > 0 ? 'ranked_bots' : 'ranked');
    const gs = room.game;

    // Збираємо учасників: активні + ті, хто вийшов/відключився (uid у _disconnected)
    const participants = []; // {uid, slot, rating}
    const seenSlots = new Set();
    for (const [sid, player] of Object.entries(room.players)) {
      if (!player.uid) continue;
      participants.push({ uid: player.uid, slot: player.slot, rating: player.rating });
      seenSlots.add(player.slot);
    }
    if (room._disconnected) {
      for (const [slotStr, info] of Object.entries(room._disconnected)) {
        const slot = parseInt(slotStr);
        if (seenSlots.has(slot) || !info || !info.uid) continue;
        participants.push({ uid: info.uid, slot, rating: info.rating });
        seenSlots.add(slot);
      }
    }

    const playerOps = [];
    for (const p of participants) {
      const placeIdx = places.indexOf(p.slot);
      const goalsConceded = gs ? (ML - (gs.lives?.[p.slot] ?? ML)) : null;
      const ratingDelta = ratingDeltas[p.slot] != null ? ratingDeltas[p.slot] : null;
      const newRating = (typeof p.rating === 'number' && ratingDelta != null)
        ? Math.max(0, p.rating + ratingDelta)
        : null;

      playerOps.push(trackEvent('game_end', {
        uid: p.uid, roomId: room.id, mode,
        place: placeIdx + 1, durationSec,
        botsCount, playersCount, goalsConceded,
        ratingDelta,
      }));

      playerOps.push(pgPool.query(
        `UPDATE players SET
          total_games = total_games + 1,
          total_wins = total_wins + $2,
          total_playtime_sec = total_playtime_sec + $3,
          peak_rating = GREATEST(peak_rating, COALESCE($4, peak_rating)),
          last_seen = NOW()
         WHERE uid = $1`,
        [p.uid, placeIdx === 0 ? 1 : 0, durationSec || 0, newRating]
      ));
    }

    await Promise.all(playerOps);

    if (durationSec) {
      await pgPool.query(
        `INSERT INTO daily_stats (date, games_played, total_playtime_sec)
         VALUES (CURRENT_DATE, 1, $1)
         ON CONFLICT (date) DO UPDATE
         SET games_played = daily_stats.games_played + 1,
             total_playtime_sec = daily_stats.total_playtime_sec + $1`,
        [durationSec]
      );
    }
  } catch(e) { console.error('[Analytics] trackGameEnd error:', e.message); }
}

initPostgres();
const TICK_RATE = 60;
const TICK_MS = 1000 / TICK_RATE;

const W = 520, H = 520, BR = 8, SMAX = 4.875, C = 130;
const PL = 54, PLV = 54, PTH = 16, PTV = 16;
const ML = 10, EPU = 1 / 3, ECR = 1 / 10000;
const FDR = 380, BMULT = 2.32, FR = 54, RD = 2000;
const PS = 3.375;

const SLOTS = [0, 1, 2, 3];
const SLOT_VIEW = ['bottom', 'top', 'left', 'right'];
const BOT_NAMES = ['ZEPHYR', 'GLITCH', 'NOVA', 'STORM', 'BLAZE', 'PIXEL'];
// Ranked боти — виглядають як живі гравці
const RANKED_BOT_NICKS = [
  'xXxSNIPERxXx','DARK_KNIGHT','PRO_GAMER_1','SHADOW_WOLF','KILLER_INSTINCT',
  'NOOB_DESTROYER','LEGENDARY_X','GHOST_RECON','TOXIC_BEAST','RAGE_QUIT_LOL',
  'HEADSHOT_KING','CYBER_NINJA','STEALTH_MODE','BLOOD_MOON','VOID_WALKER',
  'NIGHT_CRAWLER','DEMON_SLAYER','EPIC_FAIL','GOD_OF_WAR','ZERO_DEATHS',
  'NEON_VIPER','IRON_FIST','BLAZE_RUNNER','STORM_CHASER','DARK_MATTER',
  'PIXEL_REAPER','TURBO_SHARK','LASER_WOLF','HYPER_X','MATRIX_BREAKER',
  'ALPHA_WOLF','OMEGA_PRIME','THUNDER_GOD','DEATH_BRINGER','CHAOS_LORD',
  'SILENT_KILLER','RAPID_FIRE','COLD_BLOODED','NOVA_STRIKE','CYBER_WOLF',
  'SHADOW_HUNTER','GHOST_BLADE','DARK_PHOENIX','TOXIC_SNIPER','VENOM_X',
  'DRAGON_SLAYER','NIGHT_HAWK','IRON_MAIDEN','SPEED_DEMON','ACID_RAIN',
  'PRO_HUNTER','SKULL_KING','DEATH_STAR','TITAN_FALL','BLACK_OPS',
  'REAPER_X','GRIM_SHADOW','STORM_RIDER','BLADE_MASTER','FIRE_WOLF',
  'ELECTRIC_X','PLASMA_GUN','NUCLEAR_X','DARK_FORCE','QUANTUM_ACE',
  'HYPER_BEAST','LASER_HAWK','TURBO_KING','VOID_REAPER','ALPHA_PRIME',
  'TOXIC_LORD','CYBER_BEAST','NEON_GHOST','IRON_WOLF','SHADOW_PRIME',
  'DEATH_HAWK','STORM_WOLF','BLADE_X','FIRE_HAWK','DARK_WOLF',
  'ELECTRIC_WOLF','PLASMA_X','NUCLEAR_WOLF','DARK_HAWK','QUANTUM_WOLF',
  'HYPER_WOLF','LASER_X','TURBO_WOLF','VOID_WOLF','ALPHA_HAWK',
  'TOXIC_WOLF','CYBER_HAWK','NEON_WOLF','IRON_HAWK','SHADOW_WOLF99',
  'DEATH_WOLF','STORM_HAWK','BLADE_WOLF','FIRE_X','DARK_PRIME',
];
function makeRankedBot(playerRating) {
  // Рейтинг бота близько до рейтингу гравця (±150)
  const delta = Math.floor(Math.random() * 300) - 150;
  const rating = Math.max(100, playerRating + delta);
  const nick = RANKED_BOT_NICKS[Math.floor(Math.random() * RANKED_BOT_NICKS.length)];
  const avatarId = Math.floor(Math.random() * 5);
  // Ракетка залежно від рейтингу
  const pid = Math.min(19, Math.floor(rating / 120));
  const avgUpgrade = Math.floor(Math.random() * 80) + 10;
  return { nick, rating, avatarId, isRankedBot: true, paddleId: pid, avgUpgrade };
}

// ── SHOP CONSTANTS (server-authoritative) ──
const HANGAR_COSTS_SRV = [
  null,
  {cur:'silver', price:5000},
  {cur:'silver', price:10000},
  {cur:'silver', price:20000},
  {cur:'silver', price:35000},
  {cur:'silver', price:55000},
  {cur:'silver', price:80000},
  {cur:'gold',   price:150},
  {cur:'gold',   price:250},
  {cur:'gold',   price:400},
];

// Швидкості ракеток для розрахунку швидкості ботів
const PADDLE_SPD_SRV = [
  3.375,3.375,4.5,3.375,3.375,3.375,3.375,3.375,4.5,3.375,
  3.375,3.375,4.5,3.375,3.375,3.375,3.375,3.375,4.5,3.375,
];

const PADDLE_PRICES_SRV = {
  0:{cur:'free',   price:0},
  1:{cur:'silver', price:8000},
  2:{cur:'silver', price:12000},
  3:{cur:'silver', price:15000},
  4:{cur:'silver', price:18000},
  5:{cur:'silver', price:22000},
  6:{cur:'silver', price:28000},
  7:{cur:'silver', price:32000},
  8:{cur:'silver', price:38000},
  9:{cur:'silver', price:45000},
  10:{cur:'silver',price:55000},
  11:{cur:'silver',price:65000},
  12:{cur:'gold',  price:200},
  13:{cur:'gold',  price:300},
  14:{cur:'gold',  price:350},
  15:{cur:'gold',  price:450},
  16:{cur:'gold',  price:550},
  17:{cur:'gold',  price:650},
  18:{cur:'gold',  price:800},
  19:{cur:'gold',  price:1000},
};

const ENERGY_GOLD_COST_SRV = 50;

// ══════════════════════════════════════════════════════
// PADDLE STATS — SERVER-AUTHORITATIVE (дзеркало клієнта)
// ══════════════════════════════════════════════════════
// ВАЖЛИВО: ДОЛЖНО бути синхронізовано з PADDLE_CATALOG у index.html (рядок ~1665)
// Якщо міняєш базові характеристики — міняй в ОБОХ місцях!
const PADDLE_CATALOG_SRV = [
  { spd:3.375, w:54, fr:54, bm:2.32, er:1.0, fd:1.0 }, // 0 Новачок
  { spd:3.375, w:62, fr:54, bm:2.32, er:1.0, fd:1.0 }, // 1 Фронтир
  { spd:4.5,   w:50, fr:54, bm:2.32, er:1.0, fd:1.0 }, // 2 Вектор
  { spd:3.375, w:52, fr:68, bm:2.32, er:1.0, fd:1.0 }, // 3 Орбіта
  { spd:3.375, w:52, fr:54, bm:2.9,  er:1.0, fd:1.0 }, // 4 Імпульс
  { spd:2.5,   w:72, fr:54, bm:2.32, er:1.2, fd:1.0 }, // 5 Авангард
  { spd:5.5,   w:46, fr:50, bm:2.0,  er:1.3, fd:0.8 }, // 6 Спринт
  { spd:3.8,   w:42, fr:72, bm:2.6,  er:1.0, fd:1.4 }, // 7 Сталкер
  { spd:4.0,   w:58, fr:60, bm:2.5,  er:1.2, fd:1.2 }, // 8 Баланс
  { spd:2.8,   w:80, fr:50, bm:2.0,  er:1.5, fd:1.0 }, // 9 Егіда
  { spd:6.0,   w:48, fr:58, bm:2.4,  er:1.5, fd:0.7 }, // 10 Зевс
  { spd:3.2,   w:50, fr:90, bm:2.6,  er:0.8, fd:1.6 }, // 11 Полюс
  { spd:3.6,   w:66, fr:60, bm:2.8,  er:1.3, fd:1.3 }, // 12 Титан
  { spd:5.0,   w:52, fr:72, bm:2.7,  er:1.4, fd:1.2 }, // 13 Темпест
  { spd:3.8,   w:62, fr:70, bm:3.2,  er:1.2, fd:1.5 }, // 14 Кріон
  { spd:5.2,   w:54, fr:66, bm:3.4,  er:1.5, fd:1.2 }, // 15 Рейвен
  { spd:4.0,   w:76, fr:78, bm:3.0,  er:1.3, fd:1.6 }, // 16 Фантом
  { spd:3.5,   w:58, fr:86, bm:3.8,  er:1.2, fd:1.8 }, // 17 Інферно
  { spd:5.0,   w:68, fr:80, bm:3.5,  er:1.6, fd:1.6 }, // 18 Етернал
  { spd:6.0,   w:76, fr:92, bm:4.0,  er:2.0, fd:2.0 }, // 19 Абсолют
];

const HANGAR_PARTS_SRV = ['w', 'spd', 'fr', 'bm', 'er', 'fd'];

// lv1=0.9x, lv10=1.2x (те саме що клієнтський hangarMult)
function hangarMultSrv(level) {
  const lv = Math.max(1, Math.min(10, level|0));
  return 0.90 + (lv - 1) * (0.30 / 9);
}

// Дефолтні stats — для нових гравців або fallback при помилках.
const DEFAULT_PADDLE_STATS = Object.freeze({
  spd:3.375, w:54, fr:54, bm:2.32, er:1.0, fd:1.0,
  paddleId:0, avgUpgrade:0,
});

// Pure функція — розраховує фінальні stats без I/O.
// hangars — { '5': {w:3, spd:7, ...}, '12': {...} }  (ключ — paddleId як string)
function computePaddleStats(paddleId, hangars) {
  const pid = (paddleId|0);
  const base = PADDLE_CATALOG_SRV[pid] || PADDLE_CATALOG_SRV[0];
  const hangarForPid = (hangars && (hangars[pid] || hangars[String(pid)])) || {};
  const result = { paddleId: pid };
  let sumLv = 0;
  for (const part of HANGAR_PARTS_SRV) {
    const lv = hangarForPid[part] || 1;
    const mult = hangarMultSrv(lv);
    result[part] = +(base[part] * mult).toFixed(3);
    sumLv += lv;
  }
  // avgUpgrade — середній прогрес апгрейдів як ДРІБ 0..1
  // (контракт з клієнтом: getPaddleAvgUpgrade() повертає 0..1)
  // MAX_LVL=9 — рівень 1 = 0 апгрейдів, рівень 10 = 9 апгрейдів
  const MAX_LV = 9;
  let totalUpgrades = 0;
  for (const part of HANGAR_PARTS_SRV) {
    totalUpgrades += (hangarForPid[part] || 1) - 1;
  }
  result.avgUpgrade = totalUpgrades / (HANGAR_PARTS_SRV.length * MAX_LV);
  return result;
}

// Async: читає актуальні дані гравця з Firestore, валідує і повертає
// серверно-розраховані paddleStats. Блокує читерство:
//   - paddleId, який гравець не придбав → fallback до 0
//   - вигадані рівні апгрейдів → обрізаємо до 1..10
// Якщо Firestore недоступний (db=null) або uid відсутній — дефолтна ракетка.
async function loadValidatedPaddleStats(uid) {
  if (!db || !uid) return { ...DEFAULT_PADDLE_STATS };
  try {
    const [pubSnap, privSnap] = await Promise.all([
      db.collection('users_public').doc(uid).get(),
      db.collection('users_private').doc(uid).get(),
    ]);
    const pub = pubSnap.exists ? pubSnap.data() : {};
    const priv = privSnap.exists ? privSnap.data() : {};

    let paddleId = pub.paddleId|0;
    const ownedPaddles = Array.isArray(priv.ownedPaddles) ? priv.ownedPaddles : [0];

    // Валідація: чи володіє паддлом? Якщо ні → fallback до першої доступної.
    if (!ownedPaddles.includes(paddleId)) {
      console.log('[paddleStats] uid='+uid+' namesake="'+pub.nickname+'" tried paddleId='+paddleId+' but ownedPaddles='+JSON.stringify(ownedPaddles)+' → fallback to 0');
      paddleId = ownedPaddles.includes(0) ? 0 : (ownedPaddles[0]|0);
    }

    // Валідація рівнів апгрейдів: clamp в [1..10]
    const rawHangars = priv.hangars && typeof priv.hangars === 'object' ? priv.hangars : {};
    const safeHangars = {};
    for (const [pid, parts] of Object.entries(rawHangars)) {
      if (!parts || typeof parts !== 'object') continue;
      safeHangars[pid] = {};
      for (const part of HANGAR_PARTS_SRV) {
        const lv = parts[part]|0;
        safeHangars[pid][part] = Math.max(1, Math.min(10, lv || 1));
      }
    }

    return computePaddleStats(paddleId, safeHangars);
  } catch(e) {
    console.error('[paddleStats] load error for uid='+uid+':', e.message);
    return { ...DEFAULT_PADDLE_STATS };
  }
}

// Серверна валідація rating — читаємо з БД, не довіряємо клієнту
async function loadValidatedRating(uid) {
  if (!db || !uid) return 500;
  try {
    const snap = await db.collection('users_public').doc(uid).get();
    if (!snap.exists) return 500;
    const r = snap.data().rating;
    return (typeof r === 'number' && r >= 0) ? r : 500;
  } catch(e) {
    return 500;
  }
}

// ══════════════════════════════════════════════════════
// FIREBASE ID TOKEN VERIFICATION
// ══════════════════════════════════════════════════════
// Клієнт передає idToken разом з критичними подіями (mm:join, rejoin, shop:auth).
// Сервер верифікує його через Admin SDK і витягує реальний uid.
// Це блокує підміну чужого uid.
//
// Повертає { uid } при успіху, null при помилці.
// Якщо Admin SDK недоступний (db=null) — fallback без верифікації (dev режим).
async function verifyAuthToken(idToken) {
  if (!admin) return null;                // Admin недоступний — legacy режим
  if (!idToken || typeof idToken !== 'string') return null;
  try {
    const decoded = await admin.auth().verifyIdToken(idToken);
    return { uid: decoded.uid };
  } catch(e) {
    console.log('[auth] token verification failed:', e.code || e.message);
    return null;
  }
}

// Helper: розв'язує фінальний UID для handler'а.
// - Якщо Admin SDK доступний → вимагає валідний idToken, повертає verified uid.
// - Якщо Admin SDK недоступний → fallback на клієнтський uid (для dev).
// Повертає string uid або null.
async function resolveUid({ idToken, fallbackUid }) {
  if (!admin) return fallbackUid || null; // legacy fallback
  const verified = await verifyAuthToken(idToken);
  return verified ? verified.uid : null;
}

// ══════════════════════════════════════════════════════
// PUBLIC CONFIG ENDPOINT — /config.json
// ══════════════════════════════════════════════════════
// Єдина точка правди для public констант. Клієнт fetch'ить це
// при старті і перезаписує свої локальні значення.
// Змінюючи константу — мінять ТУТ, клієнт автоматично підхопить.
// CONFIG_VERSION — якщо бампаєш — clients повинні refetch (кеш invalidate).
const CONFIG_VERSION = 1;

// Повний каталог ракеток (дзеркало клієнтського, плюс name/desc/price — для UI)
const PADDLE_CATALOG_FULL = [
  {id:0, name:'Новачок',  desc:'Базова ракетка',                price:0,     cur:'free',   lvl:1,  spd:3.375,w:54,fr:54,bm:2.32,er:1.0, fd:1.0},
  {id:1, name:'Фронтир',  desc:'Ширша площа відбивання',        price:8000,  cur:'silver', lvl:1,  spd:3.375,w:62,fr:54,bm:2.32,er:1.0, fd:1.0},
  {id:2, name:'Вектор',   desc:'Підвищена швидкість руху',      price:12000, cur:'silver', lvl:2,  spd:4.5,  w:50,fr:54,bm:2.32,er:1.0, fd:1.0},
  {id:3, name:'Орбіта',   desc:'Більший радіус силового поля',  price:15000, cur:'silver', lvl:2,  spd:3.375,w:52,fr:68,bm:2.32,er:1.0, fd:1.0},
  {id:4, name:'Імпульс',  desc:'Сильніше відбиття поля',        price:18000, cur:'silver', lvl:3,  spd:3.375,w:52,fr:54,bm:2.9, er:1.0, fd:1.0},
  {id:5, name:'Авангард', desc:'Дуже широка але повільна',      price:22000, cur:'silver', lvl:3,  spd:2.5,  w:72,fr:54,bm:2.32,er:1.2, fd:1.0},
  {id:6, name:'Спринт',   desc:'Максимальна швидкість',         price:28000, cur:'silver', lvl:4,  spd:5.5,  w:46,fr:50,bm:2.0, er:1.3, fd:0.8},
  {id:7, name:'Сталкер',  desc:'Вузька з великим полем',        price:32000, cur:'silver', lvl:4,  spd:3.8,  w:42,fr:72,bm:2.6, er:1.0, fd:1.4},
  {id:8, name:'Баланс',   desc:'Рівні покращені характеристики',price:38000, cur:'silver', lvl:5,  spd:4.0,  w:58,fr:60,bm:2.5, er:1.2, fd:1.2},
  {id:9, name:'Егіда',    desc:'Гігантська ширина',             price:45000, cur:'silver', lvl:6,  spd:2.8,  w:80,fr:50,bm:2.0, er:1.5, fd:1.0},
  {id:10,name:'Зевс',     desc:'Надшвидка з середнім полем',    price:55000, cur:'silver', lvl:7,  spd:6.0,  w:48,fr:58,bm:2.4, er:1.5, fd:0.7},
  {id:11,name:'Полюс',    desc:'Гігантський радіус поля',       price:65000, cur:'silver', lvl:8,  spd:3.2,  w:50,fr:90,bm:2.6, er:0.8, fd:1.6},
  {id:12,name:'Титан',    desc:'Широка і потужна',              price:200,   cur:'gold',   lvl:5,  spd:3.6,  w:66,fr:60,bm:2.8, er:1.3, fd:1.3},
  {id:13,name:'Темпест',  desc:'Швидка з великим полем',        price:300,   cur:'gold',   lvl:6,  spd:5.0,  w:52,fr:72,bm:2.7, er:1.4, fd:1.2},
  {id:14,name:'Кріон',    desc:'Потужне поле з широкою базою',  price:350,   cur:'gold',   lvl:7,  spd:3.8,  w:62,fr:70,bm:3.2, er:1.2, fd:1.5},
  {id:15,name:'Рейвен',   desc:'Висока сила і швидкість',       price:450,   cur:'gold',   lvl:9,  spd:5.2,  w:54,fr:66,bm:3.4, er:1.5, fd:1.2},
  {id:16,name:'Фантом',   desc:'Широка з великим полем',        price:550,   cur:'gold',   lvl:10, spd:4.0,  w:76,fr:78,bm:3.0, er:1.3, fd:1.6},
  {id:17,name:'Інферно',  desc:'Найпотужніше поле в грі',       price:650,   cur:'gold',   lvl:12, spd:3.5,  w:58,fr:86,bm:3.8, er:1.2, fd:1.8},
  {id:18,name:'Етернал',  desc:'Все збалансовано на максимумі', price:800,   cur:'gold',   lvl:15, spd:5.0,  w:68,fr:80,bm:3.5, er:1.6, fd:1.6},
  {id:19,name:'Абсолют',  desc:'Найкраща ракетка в грі',        price:1000,  cur:'gold',   lvl:20, spd:6.0,  w:76,fr:92,bm:4.0, er:2.0, fd:2.0},
];

function getPublicConfig() {
  return {
    version: CONFIG_VERSION,
    paddleCatalog: PADDLE_CATALOG_FULL,
    hangarCosts: HANGAR_COSTS_SRV,       // null, silver/gold ціни за рівень
    energyGoldCost: ENERGY_GOLD_COST_SRV,
    game: {
      W, H, C, BR, SMAX, PL, PLV, PTH, PTV,
      ML, EPU, FR, BMULT, PS,
      FDR, RD,
      tickRate: TICK_RATE,
      matchDurationMs: 3 * 60 * 1000,
    },
    exchangeRates: [
      { goldIn:1,   silverOut:100   },
      { goldIn:5,   silverOut:500   },
      { goldIn:10,  silverOut:1000  },
      { goldIn:50,  silverOut:5000  },
      { goldIn:100, silverOut:10000 },
    ],
  };
}

httpServer.on('request', (req, res) => {
  // CORS preflight
  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET',
      'Access-Control-Max-Age': '86400',
    });
    res.end();
    return;
  }
  if (req.method === 'GET' && (req.url === '/config.json' || req.url.startsWith('/config.json?'))) {
    try {
      const body = JSON.stringify(getPublicConfig());
      res.writeHead(200, {
        'Content-Type': 'application/json; charset=utf-8',
        'Access-Control-Allow-Origin': '*',
        // Клієнт може кешувати 5 хвилин; при зміні CONFIG_VERSION клієнт робить refetch
        'Cache-Control': 'public, max-age=300',
      });
      res.end(body);
    } catch(e) {
      res.writeHead(500); res.end('config error');
    }
    return;
  }
  if (req.method === 'GET' && req.url === '/health') {
    res.writeHead(200, {'Content-Type': 'text/plain'});
    res.end('ok');
    return;
  }
  // Інші маршрути — нехай socket.io сам обробляє (handshake etc).
  // Якщо не socket.io URL — 404.
  if (!req.url.startsWith('/socket.io/')) {
    res.writeHead(404);
    res.end();
  }
});

const CS = [
  { ax: 0,   ay: C,   bx: C,   by: 0,   nx:  1/Math.SQRT2, ny:  1/Math.SQRT2 },
  { ax: W-C, ay: 0,   bx: W,   by: C,   nx: -1/Math.SQRT2, ny:  1/Math.SQRT2 },
  { ax: 0,   ay: H-C, bx: C,   by: H,   nx:  1/Math.SQRT2, ny: -1/Math.SQRT2 },
  { ax: W-C, ay: H,   bx: W,   by: H-C, nx: -1/Math.SQRT2, ny: -1/Math.SQRT2 },
];

function slotToPaddle(slot, cx, gs, room) {
  const view = SLOT_VIEW[slot];
  let pw = PL, pvh = PLV;
  if (room) {
    const player = getSlotPlayer(room, slot);
    if (player && player.paddleStats && player.paddleStats.w) {
      if (view === 'bottom' || view === 'top') pw = player.paddleStats.w;
      else pvh = player.paddleStats.w;
    }
    // ── Також беремо з _disconnected якщо гравець тимчасово відключений ──
    if (!player && room._disconnected && room._disconnected[slot] && room._disconnected[slot].paddleStats) {
      const dps = room._disconnected[slot].paddleStats;
      if (view === 'bottom' || view === 'top') pw = dps.w || PL;
      else pvh = dps.w || PLV;
    }
  }
  if (view==='bottom') return {x:cx-pw/2,  y:H-PTH-2, w:pw,  h:PTH, axis:'x', min:C, max:W-C-pw};
  if (view==='top')    return {x:cx-pw/2,  y:2,        w:pw,  h:PTH, axis:'x', min:C, max:W-C-pw};
  if (view==='left')   return {x:2,         y:cx-pvh/2, w:PTV, h:pvh, axis:'y', min:C, max:H-C-pvh};
  if (view==='right')  return {x:W-PTV-2,   y:cx-pvh/2, w:PTV, h:pvh, axis:'y', min:C, max:H-C-pvh};
}

function cPt(px, py, ax, ay, bx, by) {
  const dx = bx-ax, dy = by-ay, l2 = dx*dx+dy*dy;
  if (!l2) return { cx: ax, cy: ay };
  const t = Math.max(0, Math.min(1, ((px-ax)*dx+(py-ay)*dy)/l2));
  return { cx: ax+t*dx, cy: ay+t*dy };
}

function resolveChamfersBall(ball) {
  for(const s of CS){
    const{cx,cy}=cPt(ball.x,ball.y,s.ax,s.ay,s.bx,s.by);
    const d=Math.hypot(ball.x-cx,ball.y-cy);
    if(d<BR+1){
      let nx=ball.x-cx,ny=ball.y-cy;const l=Math.hypot(nx,ny);
      if(l<0.001){nx=s.nx;ny=s.ny;}else{nx/=l;ny/=l;}
      if(nx*s.nx+ny*s.ny<0){nx=-nx;ny=-ny;}
      const dot=ball.vx*nx+ball.vy*ny;if(dot<0){ball.vx-=2*dot*nx;ball.vy-=2*dot*ny;}
      ball.x+=nx*(BR+1-d);ball.y+=ny*(BR+1-d);
      const spd=Math.hypot(ball.vx,ball.vy);if(spd>SMAX){ball.vx=ball.vx/spd*SMAX;ball.vy=ball.vy/spd*SMAX;}
    }
  }
}

function clampBallObj(ball) {
  for(const s of CS){
    const d=(ball.x-s.ax)*s.nx+(ball.y-s.ay)*s.ny;
    if(d<-BR){
      const dv=ball.vx*s.nx+ball.vy*s.ny;ball.vx-=2*dv*s.nx;ball.vy-=2*dv*s.ny;
      const{cx,cy}=cPt(ball.x,ball.y,s.ax,s.ay,s.bx,s.by);
      ball.x=cx+s.nx*(BR+1);ball.y=cy+s.ny*(BR+1);
    }
  }
}

function predBall(ball, axis, wp) {
  let bx=ball.x,by=ball.y,vx=ball.vx,vy=ball.vy;
  for (let i=0; i<300; i++) {
    bx+=vx; by+=vy;
    if(bx-BR<0){bx=BR;vx=Math.abs(vx);} if(bx+BR>W){bx=W-BR;vx=-Math.abs(vx);}
    if(by-BR<0){by=BR;vy=Math.abs(vy);} if(by+BR>H){by=H-BR;vy=-Math.abs(vy);}
    if(axis==='x'&&Math.abs(by-wp)<Math.abs(vy)+1) return bx;
    if(axis==='y'&&Math.abs(bx-wp)<Math.abs(vx)+1) return by;
  }
  return axis==='x'?bx:by;
}

function getFFRadius(f) {
  const maxR = f.maxR || FR;
  return Math.min(maxR, (f.t / 200) * maxR);
}

// ── FORCE FIELD REFLECTION (remains server-authoritative) ──
// Fіз. модель: V_ball' = galilean_reflect(V_ball, V_paddle, n) + V_paddle·k_drag + n·push
//   1. Переходимо в систему паддла: V_rel = V_ball − V_paddle
//   2. Відбиваємо по нормалі: V_rel' = V_rel − 2(V_rel·n)·n
//   3. Повертаємось у world: V_ball' = V_rel' + V_paddle
//   4. Buff: додаємо часткову швидкість паддла (k_drag) + постійний імпульс (push)
//   5. Clamp до SMAX
const FF_K_DRAG = 0.7;   // скільки швидкості паддла "прилипає" до м'яча (0=ігнор, 1=100%)
const FF_PUSH   = 0.8;   // постійний імпульс назовні (buff активного shield'а)
const FF_MIN_SP = 2.5;   // мінімальна швидкість м'яча після відбиття
function applyFFBall(gs, s, ball) {
  const f = gs.fields[s];
  if (!f || !f.active) return false;

  // ── Cooldown ──
  const _cdKey = 'ff_cd_' + s;
  if (ball[_cdKey] && ball[_cdKey] > 0) { ball[_cdKey]--; return false; }

  const p = slotToPaddle(s, gs.paddles[s], gs, null);
  const fcx = p.x + p.w/2, fcy = p.y + p.h/2;
  const maxR = f.maxR || FR;
  const collideR = maxR + BR;

  // ── Дистанції поточний/попередній тік ──
  const dx = ball.x - fcx, dy = ball.y - fcy;
  const dist = Math.hypot(dx, dy);
  const oldX = ball.x - ball.vx, oldY = ball.y - ball.vy;
  const odx = oldX - fcx, ody = oldY - fcy;
  const oldDist = Math.hypot(odx, ody);

  if (dist > collideR && oldDist > collideR) return false;

  // ── CCD: точка контакту (де саме м'яч перетнув межу) ──
  let hitX, hitY;
  if (oldDist > collideR && dist <= collideR) {
    const a = ball.vx*ball.vx + ball.vy*ball.vy;
    const b = 2 * (odx*ball.vx + ody*ball.vy);
    const c = odx*odx + ody*ody - collideR*collideR;
    const disc = b*b - 4*a*c;
    if (disc >= 0 && a > 1e-6) {
      const tHit = (-b - Math.sqrt(disc)) / (2*a);
      if (tHit >= 0 && tHit <= 1) {
        hitX = oldX + ball.vx * tHit;
        hitY = oldY + ball.vy * tHit;
      }
    }
  }
  if (hitX === undefined) { hitX = ball.x; hitY = ball.y; }

  // ── Radial normal від центру поля до точки контакту ──
  // Це дає "де торкнулось — туди і полетіло" поведінку, природну для круглого поля.
  const hdx = hitX - fcx, hdy = hitY - fcy;
  const hdist = Math.hypot(hdx, hdy);
  let nx, ny;
  if (hdist > 1e-6) {
    nx = hdx / hdist;
    ny = hdy / hdist;
  } else {
    // М'яч точно в центрі — fallback на face normal паддла
    const view = SLOT_VIEW[s];
    if (view === 'bottom')     { nx = 0; ny = -1; }
    else if (view === 'top')   { nx = 0; ny =  1; }
    else if (view === 'left')  { nx = 1; ny =  0; }
    else                       { nx =-1; ny =  0; }
  }

  // ── Швидкість ПАДДЛА цього тіку (pixels per tick) ──
  // Вона тільки вздовж однієї осі, залежно від слоту.
  const prevPos = gs.paddlesPrev ? (gs.paddlesPrev[s] != null ? gs.paddlesPrev[s] : gs.paddles[s]) : gs.paddles[s];
  const paddleMove = gs.paddles[s] - prevPos;
  const view = SLOT_VIEW[s];
  const paddleAxis = (view === 'bottom' || view === 'top') ? 'x' : 'y';
  const paddleVx = paddleAxis === 'x' ? paddleMove : 0;
  const paddleVy = paddleAxis === 'y' ? paddleMove : 0;

  // ── Trigger: м'яч наближається до МЕЖІ поля в lab frame (V_ball · n < 0) ──
  // Це об'єктивний критерій — не залежить від системи відліку паддла.
  // Усуває "крюк" (м'яч вже летить назовні → skip).
  const vDotN = ball.vx * nx + ball.vy * ny;
  if (vDotN >= 0) return false;

  // ── ФОРМУЛА (specification):
  //   V_ball' = reflect(V_ball, n) + V_paddle · k_drag + n · push
  //
  // Частини:
  //   reflect(V_ball, n): чиста pong-рефлексія → м'яч летить у напрямок,
  //     протилежний падінню по нормалі. Гарантує базову передбачуваність.
  //   V_paddle · k_drag: підхоплення імпульсу паддла. Якщо паддл рухається в
  //     напрямок нормалі (лівою стороною при русі вліво) — м'яч ще сильніше туди.
  //     Якщо паддл рухається ВІД напрямку нормалі — м'яч втрачає цю компоненту.
  //   n · push: постійний buff. Поле "бумкає" м'яч назовні незалежно від стану.
  const reflVx = ball.vx - 2 * vDotN * nx;
  const reflVy = ball.vy - 2 * vDotN * ny;
  let newVx = reflVx + paddleVx * FF_K_DRAG + nx * FF_PUSH;
  let newVy = reflVy + paddleVy * FF_K_DRAG + ny * FF_PUSH;

  // ── Масштабування: min/max clamp ──
  // Якщо швидкість занизька — масштабуємо пропорційно до MIN_SP
  // (не пушимо тільки по нормалі — тангенціальна компонента може це компенсувати).
  let sp = Math.hypot(newVx, newVy);
  if (sp < FF_MIN_SP && sp > 0.01) {
    const f = FF_MIN_SP / sp;
    newVx *= f;
    newVy *= f;
    sp = FF_MIN_SP;
  }
  if (sp > SMAX) {
    const f = SMAX / sp;
    newVx *= f;
    newVy *= f;
  }

  ball.vx = newVx;
  ball.vy = newVy;

  // ── Позиція: м'яч залишається в точці контакту + 2px назовні ──
  // НЕ телепортуємо на край поля, НЕ виштовхуємо на центр паддла.
  ball.x = hitX + nx * 2;
  ball.y = hitY + ny * 2;

  // ── Cooldown 10 тіків (~167ms) ──
  ball[_cdKey] = 10;

  // ── Округлення для детермінізму клієнт/сервер ──
  ball.x  = Math.round(ball.x  * 10) / 10;
  ball.y  = Math.round(ball.y  * 10) / 10;
  ball.vx = Math.round(ball.vx * 10) / 10;
  ball.vy = Math.round(ball.vy * 10) / 10;

  return true;
}

function spawnBallQueued(gs) {
  const ang=(Math.random()*0.6+0.2)*Math.PI*(Math.random()<0.5?1:-1)+(Math.random()<0.5?0:Math.PI);
  const spd=2.5+Math.random()*0.5;
  const lastTimer = gs.respawns.length > 0 ? Math.max(...gs.respawns.map(r => r.timer)) : 0;
  const delay = Math.max(2000, lastTimer + 2000);
  gs.respawns.push({ timer: delay, vx: Math.cos(ang)*spd, vy: Math.sin(ang)*spd });
}

const rooms = new Map();

function createRoom(id) {
  return {
    id, players: {}, bots: {}, status: 'waiting',
    countdownTimer: null, tickInterval: null, game: null,
    // ── Кеш slot → player object (hot-path у tick()) ──
    // Підтримуйте через setSlotPlayer/clearSlotPlayer, не мутуйте напряму.
    _slotMap: { 0: null, 1: null, 2: null, 3: null },
  };
}

// Встановлює player в slot + підтримує _slotMap.
// Викликай ЗАМІСТЬ прямого room.players[sid] = player.
function setSlotPlayer(room, sid, player) {
  room.players[sid] = player;
  if (!room._slotMap) room._slotMap = { 0:null, 1:null, 2:null, 3:null };
  room._slotMap[player.slot] = player;
}

// Видаляє player зі slot + очищає кеш.
function clearSlotPlayer(room, sid) {
  const p = room.players[sid];
  if (p && room._slotMap && room._slotMap[p.slot] === p) {
    room._slotMap[p.slot] = null;
  }
  delete room.players[sid];
}

// Повертає player на слоті (O(1)). Якщо кеш пустий — падає на scan (safeguard).
function getSlotPlayer(room, slot) {
  if (room._slotMap && room._slotMap[slot] !== undefined) {
    return room._slotMap[slot] || null;
  }
  // fallback — якщо кімната створена у старому коді (не має _slotMap)
  for (const sid in room.players) {
    if (room.players[sid].slot === slot) return room.players[sid];
  }
  return null;
}

function findOrCreateRoom() {
  for (const [, r] of rooms) {
    if ((r.status === 'waiting' || r.status === 'countdown') && Object.keys(r.players).length < 4) return r;
  }
  const id = 'r_' + Math.random().toString(36).slice(2, 8);
  const room = createRoom(id);
  rooms.set(id, room);
  return room;
}

function getAvailableSlot(room) {
  const taken = Object.values(room.players).map(p => p.slot);
  return SLOTS.find(s => !taken.includes(s));
}

function fillBots(room, isRanked = false) {
  room.bots = {};
  let bi = 0;
  const taken = Object.values(room.players).map(p => p.slot);
  const disconnectedSlots = Object.keys(room._disconnected || {}).map(Number);
  // Середній рейтинг реальних гравців для калібровки ботів
  const realPlayers = Object.values(room.players);
  const avgRating = realPlayers.length
    ? Math.round(realPlayers.reduce((s, p) => s + (p.rating || 500), 0) / realPlayers.length)
    : 500;
  for (const s of SLOTS) {
    if (!taken.includes(s) && !disconnectedSlots.includes(s)) {
      if (isRanked) {
        room.bots[s] = makeRankedBot(avgRating);
      } else {
        room.bots[s] = { nick: BOT_NAMES[bi++ % BOT_NAMES.length], rating: 490 + Math.floor(Math.random()*30) };
      }
    }
  }
}

function buildPlayers(room) {
  const res = {};
  for (const s of SLOTS) {
    const p = getSlotPlayer(room, s);
    if (p) {
      res[s] = { nick: p.nick, rating: p.rating, isBot: false, wins: p.wins||0, games: p.games||0, avatarId: p.avatarId||0 };
    } else if (room._disconnected && room._disconnected[s]) {
      // ── Відключений гравець — показуємо як гравця (не бота) але з позначкою ──
      res[s] = { nick: room._disconnected[s].nick, rating: room._disconnected[s].rating, isBot: false, disconnected: true, wins: 0, games: 0 };
    } else if (room.bots[s]) {
      const bot = room.bots[s];
      if (bot.isRankedBot) {
        // Ranked боти — виглядають як живі гравці
        res[s] = { nick: bot.nick, rating: bot.rating, isBot: false, wins: Math.floor(Math.random()*50), games: Math.floor(Math.random()*100)+50, avatarId: bot.avatarId || 0 };
      } else {
        res[s] = { nick: bot.nick, rating: bot.rating, isBot: true, wins: 0, games: 0 };
      }
    }
  }
  return res;
}

function broadcastLobby(room) {
  // НЕ надсилаємо список гравців — лише кількість підключених
  const count = Object.keys(room.players).length;
  io.to(room.id).emit('lobby:update', { count });
}

function createGameState(room) {
  const gs = {
    balls: [], respawns: [],
    paddles: { 0: W/2, 1: W/2, 2: H/2, 3: H/2 },
    // paddlesPrev зберігає позицію з попереднього тіку — використовується для обчислення
    // швидкості ракетки (V_paddle = paddles − paddlesPrev). Важливо для фізики поля.
    paddlesPrev: { 0: W/2, 1: W/2, 2: H/2, 3: H/2 },
    lives: { 0: ML, 1: ML, 2: ML, 3: ML },
    scores: { 0: 0, 1: 0, 2: 0, 3: 0 },
    energy: { 0: 1, 1: 1, 2: 1, 3: 1 },
    fields: { 0:{active:false,t:0,r:0}, 1:{active:false,t:0,r:0}, 2:{active:false,t:0,r:0}, 3:{active:false,t:0,r:0} },
    eliminated: { 0: false, 1: false, 2: false, 3: false },
    // ── Єдиний хронологічний журнал вибуття (source of truth для ranking) ──
    // Перший вибулий = 4 місце, останній вибулий = 2 місце, ще живий = 1.
    // Заповнюється через markEliminated() — незалежно від причини (lives/leave/timeout).
    eliminationOrder: [],
    botTargets: { 0: W/2, 1: W/2, 2: H/2, 3: H/2 },
    gameOver: false, winner: null, tick: 0,
  };
  const PREGAME_DELAY = 5000;
  for(let i=0;i<4;i++){
    const ang=(Math.random()*0.6+0.2)*Math.PI*(Math.random()<0.5?1:-1)+(Math.random()<0.5?0:Math.PI);
    const spd=2.5+Math.random()*0.5;
    gs.respawns.push({ timer: PREGAME_DELAY + 3000 + i*2000, vx: Math.cos(ang)*spd, vy: Math.sin(ang)*spd });
  }
  return gs;
}

// Єдиний source of truth для вибуття гравця.
// reason: 'lives' | 'leave' | 'timeout' — для логів.
// Повертає true якщо elimination відбулась (false = уже був eliminated).
function markEliminated(gs, slot, reason) {
  if (!gs || gs.eliminated[slot]) return false;
  gs.eliminated[slot] = true;
  gs.lives[slot] = 0;
  if (!gs.eliminationOrder.includes(slot)) {
    gs.eliminationOrder.push(slot);
  }
  console.log(`[elim] slot=${slot} reason=${reason} order=[${gs.eliminationOrder.join(',')}]`);
  return true;
}

function activeSlots(gs) { return SLOTS.filter(s => !gs.eliminated[s]); }

function hitRect(ball, p) {
  return ball.x+BR > p.x && ball.x-BR < p.x+p.w && ball.y+BR > p.y && ball.y-BR < p.y+p.h;
}

function tick(room) {
  const gs = room.game;
  if (!gs || gs.gameOver) return;
  gs.tick++;
  const sendBalls = true; // Lockstep: надсилаємо стан кожен тік
  try {
    for (const s of SLOTS) {
      if (gs.eliminated[s]) continue;
      if (gs.fields[s].active) {
        gs.fields[s].t += TICK_MS;
        const pStats = getSlotPlayer(room, s)?.paddleStats
          || room._disconnected?.[s]?.paddleStats;
        const fd = pStats?.fd || 1.0;
        const maxRf = gs.fields[s].maxR || pStats?.fr || FR;
        gs.fields[s].r = Math.min(maxRf, (gs.fields[s].t / 200) * maxRf);
        if (gs.fields[s].t >= FDR * fd) { gs.fields[s].active = false; gs.fields[s].t = 0; gs.fields[s].r = 0; }
      } else {
        const pStats2 = getSlotPlayer(room, s)?.paddleStats
          || room._disconnected?.[s]?.paddleStats;
        const er = pStats2?.er || 1.0;
        gs.energy[s] = Math.min(1, gs.energy[s] + ECR * TICK_MS * er);
      }
    }

    // ── Рух гравців — тільки підключених ──
    for (const [sid, player] of Object.entries(room.players)) {
      const s = player.slot;
      if (gs.eliminated[s]) continue;
      const inp = player.input || {};
      const view = SLOT_VIEW[s];
      const isHoriz = view === 'top' || view === 'bottom';
      const pStats = player.paddleStats || {};
      const pSpd = pStats.spd || PS;
      const pW = pStats.w || PL;
      const pHalf = isHoriz ? pW/2 : (pStats.w||PLV)/2;
      const mn = C+pHalf, mx = (isHoriz?W:H)-C-pHalf;
      if (inp.left)  gs.paddles[s] = Math.max(mn, gs.paddles[s] - pSpd);
      if (inp.right) gs.paddles[s] = Math.min(mx, gs.paddles[s] + pSpd);
      // ── БАГ 2 FIX: якщо boost натиснутий — обробляємо ЗАВЖДИ, навіть без енергії ──
      // Різниця від старого коду: boost очищається завжди, а не тільки при успішній активації
      if (inp.boost && !gs.fields[s].active) {
        if (gs.energy[s] >= EPU) {
          // Є енергія — активуємо поле
          gs.fields[s].active = true;
          gs.fields[s].r = 0;
          gs.fields[s].maxR = pStats?.fr || FR;
          gs.energy[s] = Math.max(0, gs.energy[s] - EPU);

          // Client-authoritative boost position:
          // Якщо клієнт надіслав позицію при активації — перевіряємо чи надійна
          if (inp.boostPos !== undefined) {
            const viewB = SLOT_VIEW[s];
            const isHorizB = viewB === 'top' || viewB === 'bottom';
            const pWB = pStats?.w || PL;
            const halfB = pWB / 2;
            const mnB = C + halfB;
            const mxB = (isHorizB ? W : H) - C - halfB;
            const clampedPos = Math.max(mnB, Math.min(mxB, inp.boostPos));
            const serverPos = gs.paddles[s];
            const maxSpeed = pStats?.spd || PS;
            // Максимальний можливий рух за час пінгу (~5 тіків)
            const maxDrift = maxSpeed * 8;
            if (Math.abs(clampedPos - serverPos) <= maxDrift) {
              // Надійно — беремо позицію клієнта
              gs.paddles[s] = clampedPos;
              gs.fields[s].t = TICK_MS * 3; // компенсуємо мережеву затримку (~3 тіки)
            } else {
              // Підозріло — серверна позиція, без компенсації
              gs.fields[s].t = 0;
              console.log(`Suspicious boost pos: slot${s} diff=${Math.abs(clampedPos-serverPos).toFixed(0)}px`);
            }
          } else {
            gs.fields[s].t = 0;
          }
        }
        // Очищаємо boost ЗАВЖДИ (і при активації, і при недостачі енергії) —
        // щоб він не "висів" в буфері до появи енергії
        inp.boost = false;
        inp.boostPos = undefined;
      }
    }
    // ── Відключені гравці — ракетка стоїть (input не надходить, нічого не робимо) ──

    // ── Боти ──
    for (const s of SLOTS) {
      if (!room.bots[s] || gs.eliminated[s]) continue;
      const bot = room.bots[s];
      const view = SLOT_VIEW[s];
      const isHoriz = view === 'top' || view === 'bottom';
      const pw = isHoriz ? PL : PLV;
      const mn = C + pw/2;
      const mx = (isHoriz ? W : H) - C - pw/2;

      // Знаходимо найнебезпечніший м'яч
      let bestBall = gs.balls[0] || {x:W/2,y:H/2,vx:0,vy:0};
      let bestThreat = -Infinity;
      for (const b of gs.balls) {
        const flying = (view==='bottom'&&b.vy>0)||(view==='top'&&b.vy<0)||(view==='left'&&b.vx<0)||(view==='right'&&b.vx>0);
        const threat = flying ? 1000 - (isHoriz?Math.abs(b.y-(view==='bottom'?H:0)):Math.abs(b.x-(view==='left'?0:W))) : 0;
        if (threat > bestThreat) { bestThreat = threat; bestBall = b; }
      }

      // Лінійне передбачення
      const wallPos = isHoriz ? (view==='top'?PTH/2:H-PTH/2) : (view==='left'?PTV/2:W-PTV/2);
      const velToWall = view==='bottom'?bestBall.vy:view==='top'?-bestBall.vy:view==='left'?-bestBall.vx:bestBall.vx;
      const distToWall = isHoriz?Math.abs(bestBall.y-wallPos):Math.abs(bestBall.x-wallPos);
      const ttr = velToWall > 0.1 ? distToWall/velToWall : 30;
      const predictedPerp = isHoriz
        ? (bestBall.x + bestBall.vx * Math.min(ttr, 16) * 0.8)
        : (bestBall.y + bestBall.vy * Math.min(ttr, 16) * 0.8);
      const jitter = (Math.random()-0.5) * (bot.isRankedBot ? 5 : 8);
      const target = Math.max(mn, Math.min(mx, predictedPerp + jitter));

      // Швидкість: ranked боти використовують свою ракетку
      const botBaseSpd = PADDLE_SPD_SRV[bot.paddleId] || 3.375;
      const botMult = 0.9 + (bot.avgUpgrade||30)/100*0.3;
      const botSpd = bot.isRankedBot ? Math.min(SMAX, botBaseSpd*botMult) : 3.5;

      gs.botTargets[s] += (target - gs.botTargets[s]) * (bot.isRankedBot ? 0.16 : 0.08);
      const diff = gs.botTargets[s] - gs.paddles[s];
      if (Math.abs(diff) > 1) gs.paddles[s] = Math.max(mn, Math.min(mx, gs.paddles[s] + Math.sign(diff)*Math.min(botSpd, Math.abs(diff))));

      // Силове поле: активуємо коли м'яч реально летить до нас і потрапить в радіус
      if (!gs.fields[s].active && gs.energy[s] >= EPU && velToWall > 0.1 && distToWall > 0) {
        const ticksToArrive = distToWall / velToWall;
        if (ticksToArrive >= 5 && ticksToArrive <= 30) {
          const predictedHit = isHoriz
            ? (bestBall.x + bestBall.vx * ticksToArrive)
            : (bestBall.y + bestBall.vy * ticksToArrive);
          const botFR = bot.isRankedBot ? Math.round(FR * (0.9 + (bot.avgUpgrade||30)/100*0.3)) : FR;
          if (Math.abs(predictedHit - gs.paddles[s]) < botFR + BR + 4) {
            gs.fields[s].active = true; gs.fields[s].t = 0; gs.fields[s].maxR = botFR;
            gs.energy[s] = Math.max(0, gs.energy[s] - EPU);
          }
        }
      }
    }

    for (let i = gs.respawns.length - 1; i >= 0; i--) {
      gs.respawns[i].timer -= TICK_MS;
      if (gs.respawns[i].timer <= 0) {
        gs.balls.push({ x: W/2, y: H/2, vx: gs.respawns[i].vx, vy: gs.respawns[i].vy, id: Date.now()+i });
        gs.respawns.splice(i, 1);
      }
    }

    const goal = (slot) => {
      if (gs.eliminated[slot]) return false;
      gs.scores[slot]++; gs.lives[slot]--;
      if (gs.lives[slot] <= 0) {
        markEliminated(gs, slot, 'lives');
        // Якщо відключений гравець вибув — відміняємо його таймер реконекту
        if (room._disconnected && room._disconnected[slot]) {
          if (room._slotDeleteTimers && room._slotDeleteTimers[slot]) {
            clearTimeout(room._slotDeleteTimers[slot]);
            delete room._slotDeleteTimers[slot];
          }
          delete room._disconnected[slot];
        }
        const active = activeSlots(gs);
        if (active.length === 1) { endGame(room, active[0]); return true; }
      }
      return false;
    };

    for (let bi = gs.balls.length - 1; bi >= 0; bi--) {
      const ball = gs.balls[bi];
      ball.x += ball.vx; ball.y += ball.vy;
    // Округлення до 3 знаків — зменшує float drift між клієнтом і сервером
    ball.x=Math.round(ball.x*1000)/1000;
    ball.y=Math.round(ball.y*1000)/1000;
    ball.vx=Math.round(ball.vx*1000)/1000;
    ball.vy=Math.round(ball.vy*1000)/1000;
      let _ffHit=false;
      for (const s of SLOTS) {
        if (gs.eliminated[s]) continue;
        if (!_ffHit && applyFFBall(gs, s, ball)) _ffHit=true;
      }
      resolveChamfersBall(ball);
      clampBallObj(ball);
      let hit = false;
      for (const s of SLOTS) {
        if (gs.eliminated[s]) continue;
        const p = slotToPaddle(s, gs.paddles[s], gs, room);
        if (hitRect(ball, p)) {
          const view = SLOT_VIEW[s];
          const speed = Math.hypot(ball.vx, ball.vy);
          let sideOffset;
          if (p.axis === 'x') {
            sideOffset = Math.max(-0.85, Math.min(0.85, (ball.x-(p.x+p.w/2))/(p.w/2)));
            ball.y = view==='top' ? p.y+p.h+BR : p.y-BR;
            ball.vy = view==='top' ? Math.abs(speed)*0.85 : -Math.abs(speed)*0.85;
            ball.vx = sideOffset * speed * 0.7;
          } else {
            sideOffset = Math.max(-0.85, Math.min(0.85, (ball.y-(p.y+p.h/2))/(p.h/2)));
            ball.x = view==='left' ? p.x+p.w+BR : p.x-BR;
            ball.vx = view==='left' ? Math.abs(speed)*0.85 : -Math.abs(speed)*0.85;
            ball.vy = sideOffset * speed * 0.7;
          }
          const actual = Math.hypot(ball.vx, ball.vy);
          if (actual > 0.01) { ball.vx = ball.vx/actual*speed; ball.vy = ball.vy/actual*speed; }
          // Округлення після відбиття від ракетки
          ball.x=Math.round(ball.x*10)/10;ball.y=Math.round(ball.y*10)/10;
          ball.vx=Math.round(ball.vx*10)/10;ball.vy=Math.round(ball.vy*10)/10;
          hit = true; break;
        }
      }
      if (hit) continue;
      const by = ball.y, bx2 = ball.x;
      if (by-BR < 0 && bx2 > C && bx2 < W-C) {
        if (gs.eliminated[1]) { ball.vy = Math.abs(ball.vy); }
        else { if (!goal(1)) spawnBallQueued(gs); gs.balls.splice(bi,1); }
      } else if (by+BR > H && bx2 > C && bx2 < W-C) {
        if (gs.eliminated[0]) { ball.vy = -Math.abs(ball.vy); }
        else { if (!goal(0)) spawnBallQueued(gs); gs.balls.splice(bi,1); }
      } else if (bx2-BR < 0 && by > C && by < H-C) {
        if (gs.eliminated[2]) { ball.vx = Math.abs(ball.vx); }
        else { if (!goal(2)) spawnBallQueued(gs); gs.balls.splice(bi,1); }
      } else if (bx2+BR > W && by > C && by < H-C) {
        if (gs.eliminated[3]) { ball.vx = -Math.abs(ball.vx); }
        else { if (!goal(3)) spawnBallQueued(gs); gs.balls.splice(bi,1); }
      }
      if (gs.gameOver) { broadcastState(room); return; }
    }
    broadcastState(room, sendBalls);

    // ── В КІНЦІ тіку зберігаємо поточні позиції як previous ──
    // Наступний тік читатиме V_paddle = paddles − paddlesPrev як зміну за 1 тік
    if (gs.paddlesPrev) {
      gs.paddlesPrev[0] = gs.paddles[0];
      gs.paddlesPrev[1] = gs.paddles[1];
      gs.paddlesPrev[2] = gs.paddles[2];
      gs.paddlesPrev[3] = gs.paddles[3];
    }
  } catch(e) {
    console.error('TICK ERROR:', e.message, e.stack?.split('\n')[1]);
  }
}

// ── Серверна статистика розсинхронів ──
const _srvStats = {};

function broadcastState(room, sendBalls=true) {
  const gs = room.game;
  if (!gs) return;
  io.to(room.id).emit('gs', {
    seq: gs.tick,
    balls: gs.balls.map(b=>({x:Math.round(b.x*10)/10,y:Math.round(b.y*10)/10,vx:Math.round(b.vx*100)/100,vy:Math.round(b.vy*100)/100,id:b.id})),
    respawns: gs.respawns.map(r=>({timer:Math.round(r.timer),vx:r.vx,vy:r.vy})),
    p: [Math.round(gs.paddles[0]),Math.round(gs.paddles[1]),Math.round(gs.paddles[2]),Math.round(gs.paddles[3])],
    e: [Math.round(gs.energy[0]*100),Math.round(gs.energy[1]*100),Math.round(gs.energy[2]*100),Math.round(gs.energy[3]*100)],
    f: [
      gs.fields[0].active ? Math.round(getFFRadius(gs.fields[0])) : 0,
      gs.fields[1].active ? Math.round(getFFRadius(gs.fields[1])) : 0,
      gs.fields[2].active ? Math.round(getFFRadius(gs.fields[2])) : 0,
      gs.fields[3].active ? Math.round(getFFRadius(gs.fields[3])) : 0,
    ],
    ft: [ // field timer — скільки ms поле вже активне
      gs.fields[0].active ? Math.round(gs.fields[0].t) : 0,
      gs.fields[1].active ? Math.round(gs.fields[1].t) : 0,
      gs.fields[2].active ? Math.round(gs.fields[2].t) : 0,
      gs.fields[3].active ? Math.round(gs.fields[3].t) : 0,
    ],
    fmr: [ // field maxR — прокачаний радіус
      gs.fields[0].maxR || FR,
      gs.fields[1].maxR || FR,
      gs.fields[2].maxR || FR,
      gs.fields[3].maxR || FR,
    ],
    el: [gs.eliminated[0]?1:0,gs.eliminated[1]?1:0,gs.eliminated[2]?1:0,gs.eliminated[3]?1:0],
    lv: [gs.lives[0],gs.lives[1],gs.lives[2],gs.lives[3]],
    sc: [gs.scores[0],gs.scores[1],gs.scores[2],gs.scores[3]],
    // ── Передаємо які слоти зараз відключені ──
    dc: SLOTS.map(s => (room._disconnected && room._disconnected[s]) ? 1 : 0),
    pw: SLOTS.map(s=>{
      const p=getSlotPlayer(room, s);
      if (p && p.paddleStats && p.paddleStats.w) return Math.round(p.paddleStats.w);
      const d=room._disconnected?.[s];
      if (d && d.paddleStats && d.paddleStats.w) return Math.round(d.paddleStats.w);
      return PL;
    }),
  });
}

function getTrainingRewards(winnerSlot, mySlot) {
  const place = mySlot === winnerSlot ? 0 : 1;
  const xpMap = [60, 30, 15, 5];
  const silverMap = [80, 40, 20, 5];
  return { xp: xpMap[place]||5, silver: silverMap[place]||5, place };
}

// ── Записуємо нагороди через Admin SDK (сервер авторитетний) ──
async function commitRewards(room, winnerSlot, places, ratingDeltas, isTraining, trainingRewards) {
  if (!db) return; // Admin SDK недоступний — клієнт запише сам (fallback)
  const gs = room.game;
  const today = new Date().toISOString().slice(0, 10);
  const batch = db.batch();

  // ── Збираємо ВСІХ учасників матчу з uid: активних + тих, хто вийшов/відключився ──
  // Після leave()/timeout гравця нема в room.players, але uid + rating у _disconnected[slot].
  // Без цього блоку волонтарно-вийшовший гравець не отримав би -20 рейтингу.
  const participants = []; // {uid, slot, rating}
  const claimedSlots = new Set();

  for (const [sid, player] of Object.entries(room.players)) {
    if (!player.uid || player.isBot) continue;
    participants.push({ uid: player.uid, slot: player.slot, rating: player.rating || 500 });
    claimedSlots.add(player.slot);
  }
  if (room._disconnected) {
    for (const [slotStr, info] of Object.entries(room._disconnected)) {
      const slot = parseInt(slotStr);
      if (claimedSlots.has(slot)) continue; // активний гравець вже взятий
      if (!info || !info.uid) continue;
      participants.push({ uid: info.uid, slot, rating: info.rating || 500 });
      claimedSlots.add(slot);
    }
  }

  for (const p of participants) {
    const s = p.slot;
    const pubRef = db.collection('users_public').doc(p.uid);
    const privRef = db.collection('users_private').doc(p.uid);

    if (isTraining) {
      const tr = trainingRewards;
      if (!tr) continue;
      const place = s === winnerSlot ? 0 : 1;
      const XP_MAP = [150, 75, 30, 0];
      const SIL_MAP = [1500, 700, 200, 0];
      const xp = XP_MAP[place] || 0;
      const silver = SIL_MAP[place] || 0;
      batch.update(privRef, {
        xp: admin.firestore.FieldValue.increment(xp),
        silver: admin.firestore.FieldValue.increment(silver),
      });
      batch.update(pubRef, { gamesPlayed: admin.firestore.FieldValue.increment(1) });
    } else {
      const delta = ratingDeltas[s] != null ? ratingDeltas[s] : -20;
      const placeIdx = places.indexOf(s);
      const XP_MAP = [100, 50, 20, 0];
      const xp = XP_MAP[placeIdx] || 0;
      const newRating = Math.max(0, p.rating + delta);

      const pubUpd = {
        rating: newRating,
        gamesPlayed: admin.firestore.FieldValue.increment(1),
        ratingDate: today,
      };
      if (delta > 0) pubUpd.wins = admin.firestore.FieldValue.increment(1);
      pubUpd.ratingToday = admin.firestore.FieldValue.increment(delta);

      batch.update(pubRef, pubUpd);
      if (xp > 0) batch.update(privRef, { xp: admin.firestore.FieldValue.increment(xp) });
    }
  }

  try {
    await batch.commit();
    console.log(`[rewards] committed for room ${room.id} (${participants.length} participants)`);
  } catch(e) {
    console.error('[rewards] batch error:', e.message);
  }
}

function endGame(room, winnerSlot) {
  const gs = room.game;
  gs.gameOver = true; gs.winner = winnerSlot;
  room.status = 'finished';
  if (room.tickInterval) { clearInterval(room.tickInterval); room.tickInterval = null; }
  if (room.matchTimerInterval) { clearInterval(room.matchTimerInterval); room.matchTimerInterval = null; }
  if (room._slotDeleteTimers) {
    for (const t of Object.values(room._slotDeleteTimers)) clearTimeout(t);
    room._slotDeleteTimers = {};
  }
  const RATING_BY_PLACE = [50, 20, 5, -20];

  // ── SINGLE SOURCE OF TRUTH для ranking ──
  // 1 місце = переможець (ще живий або з найбільше lives якщо по таймеру)
  // 2-4 місце = ОБЕРНЕНИЙ порядок вибуття (останній вибулий = 2, перший вибулий = 4)
  // Якщо матч скінчився по таймеру з кількома живими — живі йдуть за lives desc після переможця.
  const places = [winnerSlot];
  const alive = SLOTS.filter(s => !gs.eliminated[s] && s !== winnerSlot);
  // Живі, що не winner — за lives desc (тай-брейк: хто менше scores = менше пропустив)
  alive.sort((a, b) => (gs.lives[b] - gs.lives[a]) || (gs.scores[a] - gs.scores[b]));
  places.push(...alive);
  // Вибулі у зворотному порядку
  const elimReversed = [...gs.eliminationOrder].reverse();
  for (const s of elimReversed) {
    if (!places.includes(s)) places.push(s);
  }
  // Безпека: добиваємо будь-які залишки (не повинно спрацювати)
  for (const s of SLOTS) {
    if (!places.includes(s)) places.push(s);
  }

  const ratingDeltas = {};
  places.forEach((slot, idx) => { ratingDeltas[slot] = RATING_BY_PLACE[idx] || -20; });
  const isTraining = Object.values(room.players).some(p => p.trainingMode);
  const hasRankedBots = Object.values(room.bots||{}).some(b => b.isRankedBot);
  let trainingRewards = null;
  if(isTraining){
    const realPlayer = Object.values(room.players).find(p=>p.trainingMode);
    if(realPlayer) trainingRewards = getTrainingRewards(winnerSlot, realPlayer.slot);
  }

  // Записуємо нагороди на сервері — клієнт більше не пише рейтинг/XP/срібло після матчу
  commitRewards(room, winnerSlot, places, ratingDeltas, isTraining, trainingRewards);
  // Аналітика
  trackGameEnd(room, winnerSlot, places, isTraining, ratingDeltas);

  io.to(room.id).emit('game:over', {
    winnerSlot, players: buildPlayers(room),
    ratingDeltas: isTraining ? {} : ratingDeltas,
    places, trainingRewards,
    // Сигнал клієнту: сервер взяв запис на себе
    serverCommitted: !!db,
  });
  setTimeout(() => { rooms.delete(room.id); }, 30000);
}

function startGame(room) {
  room.status = 'playing';
  // Боти тільки для тренування — в рейтингових грають тільки реальні гравці
  const isTrainingRoom = Object.values(room.players).some(p => p.trainingMode);
  if (isTrainingRoom) {
    fillBots(room, false);
  } else {
    // Ranked — заповнюємо порожні слоти ranked-ботами
    fillBots(room, true);
  }
  room.game = createGameState(room);
  // Paddle visual data — передається один раз при старті
  const paddleVisuals = SLOTS.map(s => {
    const p = getSlotPlayer(room, s);
    const stats = p?.paddleStats || room._disconnected?.[s]?.paddleStats || {};
    const isBot = !p && !room._disconnected?.[s] && room.bots?.[s];
    if (isBot) {
      const bot = room.bots[s];
      if (bot.isRankedBot) {
        return { paddleId: bot.paddleId || 0, avgUpgrade: bot.avgUpgrade || 30 };
      }
      const botRating = bot.rating || 490;
      const botPaddleId = Math.min(19, Math.floor(botRating / 80));
      return { paddleId: botPaddleId, avgUpgrade: Math.floor(Math.random() * 60) };
    }
    return {
      paddleId: stats.paddleId !== undefined ? stats.paddleId : 0,
      avgUpgrade: stats.avgUpgrade !== undefined ? Math.round(stats.avgUpgrade * 100) : 0,
    };
  });
  io.to(room.id).emit('game:start', { players: buildPlayers(room), mySlot: null, roomId: room.id, paddleVisuals });
  for (const [sid, player] of Object.entries(room.players)) {
    io.to(sid).emit('myslot', { mySlot: player.slot, roomId: room.id });
  }
  room.tickInterval = setInterval(() => tick(room), TICK_MS);
  room.startedAt = Date.now(); // для підрахунку тривалості матчу

  // ── Аналітика: подія старту матчу (для drop-off rate) ──
  try {
    const botsCount = Object.keys(room.bots || {}).filter(s => room.bots[s]).length;
    const realPlayersCount = Object.keys(room.players).length;
    const isTrainingRoom = Object.values(room.players).some(p => p.trainingMode);
    const mode = isTrainingRoom ? 'training' : (botsCount > 0 ? 'ranked_bots' : 'ranked');
    for (const player of Object.values(room.players)) {
      if (!player.uid) continue;
      trackEvent('match_start', {
        uid: player.uid, roomId: room.id, mode,
        botsCount, playersCount: realPlayersCount,
      });
    }
  } catch(e) { /* analytics не повинно ламати gameplay */ }

  // ── Серверний таймер матчу — 3 хвилини ──
  const MATCH_DURATION_MS = 3 * 60 * 1000;
  room.matchTimeLeft = MATCH_DURATION_MS;

  // Надсилаємо клієнтам кожну секунду
  room.matchTimerInterval = setInterval(() => {
    if (room.game?.gameOver) {
      clearInterval(room.matchTimerInterval);
      room.matchTimerInterval = null;
      return;
    }
    room.matchTimeLeft -= 1000;
    io.to(room.id).emit('match:timer', { timeLeft: room.matchTimeLeft });

    if (room.matchTimeLeft <= 0) {
      clearInterval(room.matchTimerInterval);
      room.matchTimerInterval = null;
      // Час вийшов — визначаємо переможця
      try { if (room.game && !room.game.gameOver) {
        const gs = room.game;
        const ML_SRV = 10;
        const active = SLOTS.filter(s => !gs.eliminated?.[s]);
        if (active.length > 0) {
          // Хто менше пропустив — переможець
          // Переможець = хто більше lives залишилось (менше пропустив)
          const livesMap = {};
          active.forEach(s => { livesMap[s] = gs.lives?.[s] ?? 0; });
          const winnerSlot = active.reduce((a, b) =>
            livesMap[a] >= livesMap[b] ? a : b
          );
          console.log('[match:timeout] room='+room.id+' winner='+winnerSlot+' lives='+JSON.stringify(livesMap));
          endGame(room, winnerSlot);
        }
      } } catch(e) { console.error('[match:timeout] error:', e.message, e.stack); }
    }
  }, 1000);

  console.log(`Game started: ${room.id}, ${Object.keys(room.players).length} real players`);
}

// ── БАГ 4 FIX: countdown НЕ рестартиться при новому гравці ──
// Нова логіка: якщо countdown вже йде — не чіпаємо його. countdownTimeLeft
// зберігається в room і передається новачкам одразу після join.
function startCountdown(room) {
  if (room.countdownTimer) return; // вже запущений — не чіпаємо
  room.status = 'countdown';
  room.countdownTimeLeft = 10;
  io.to(room.id).emit('mm:countdown', { timeLeft: room.countdownTimeLeft });
  room.countdownTimer = setInterval(() => {
    room.countdownTimeLeft--;
    io.to(room.id).emit('mm:countdown', { timeLeft: room.countdownTimeLeft });
    if (room.countdownTimeLeft <= 0) {
      clearInterval(room.countdownTimer);
      room.countdownTimer = null;
      startGame(room);
    }
  }, 1000);
}

io.on('connection', (socket) => {
  let myRoom = null, mySlot = null;

  socket.on('paddle:stats',({slot,paddleStats})=>{
    // ── SECURITY: ігноруємо. Сервер рахує paddleStats сам з БД при mm:join.
    // Цей handler залишено для сумісності зі старими клієнтами, але він nop.
  });

  // Реєструємо shop handlers для цього сокета
  registerShopHandlers(socket);

  // Аутентифікація для shop без mm:join (з меню/магазину)
  // SECURITY: верифікуємо idToken і використовуємо тільки verified UID.
  // Якщо Admin SDK недоступний — fallback на клієнтський uid (dev режим).
  socket.on('shop:auth', async ({ uid, idToken }) => {
    const resolvedUid = await resolveUid({ idToken, fallbackUid: uid });
    if (resolvedUid) {
      socket.uid = resolvedUid;
    } else {
      socket.uid = undefined;
      socket.emit('shop:error', { msg: 'auth_required' });
    }
  });

  // ── Обмін золото → срібло ──
  socket.on('shop:exchange', async ({ rateId }) => {
    if (!db) return socket.emit('shop:error', { msg: 'server_unavailable' });
    if (!socket.uid) return socket.emit('shop:error', { msg: 'auth_required' });
    const RATES = [
      { goldIn:1,   silverOut:100   },
      { goldIn:5,   silverOut:500   },
      { goldIn:10,  silverOut:1000  },
      { goldIn:50,  silverOut:5000  },
      { goldIn:100, silverOut:10000 },
    ];
    const rate = RATES[parseInt(rateId)];
    if (!rate) return socket.emit('shop:error', { msg: 'invalid_rate' });

    const privRef = db.collection('users_private').doc(socket.uid);

    try {
      const result = await db.runTransaction(async (tx) => {
        const privSnap = await tx.get(privRef);
        if (!privSnap.exists) throw { code: 'user_not_found' };
        const priv = privSnap.data();

        const gold   = priv.gold   || 0;
        const silver = priv.silver || 0;
        if (gold < rate.goldIn) throw { code: 'not_enough_gold' };

        const newGold   = gold   - rate.goldIn;
        const newSilver = silver + rate.silverOut;
        tx.update(privRef, { gold: newGold, silver: newSilver });
        return { gold: newGold, silver: newSilver };
      });

      socket.emit('shop:exchanged', result);
      trackEvent('purchase', {
        uid: socket.uid, kind: 'exchange', item: `gold_to_silver_${rate.goldIn}`,
        amount: rate.goldIn, cur: 'gold',
      });
    } catch(e) {
      if (e && e.code) { socket.emit('shop:error', { msg: e.code }); return; }
      console.error('shop:exchange', e.message);
      socket.emit('shop:error', { msg: 'server_error' });
    }
  });

  // ── Обробка незакінченого матчу (disconnect) ──
  socket.on('shop:resolve_pending', async ({ type, aliveAtLastUpdate }) => {
    if (!db || !socket.uid) return;
    const RATE = [50, 20, 5, -20];
    const XP   = [100, 50, 20, 0];
    const alive = Math.max(1, Math.min(4, parseInt(aliveAtLastUpdate) || 4));
    const place = Math.max(0, Math.min(3, alive - 1));
    const delta = RATE[place] || -20;
    const today = new Date().toISOString().slice(0, 10);

    const pubRef  = db.collection('users_public').doc(socket.uid);
    const privRef = db.collection('users_private').doc(socket.uid);

    try {
      await db.runTransaction(async (tx) => {
        const pubSnap = await tx.get(pubRef);
        if (!pubSnap.exists) throw { code: 'user_not_found' };
        const pub = pubSnap.data();

        if (type === 'ranked') {
          const newRating = Math.max(0, (pub.rating || 500) + delta);
          const newRatingToday = (pub.ratingToday || 0) + delta;
          const newGamesPlayed = (pub.gamesPlayed || 0) + 1;
          tx.update(pubRef, {
            rating: newRating,
            gamesPlayed: newGamesPlayed,
            ratingDate: today,
            ratingToday: newRatingToday,
          });
          if (XP[place] > 0) {
            // Для privRef треба теж прочитати перед write (Firestore tx rule)
            const privSnap = await tx.get(privRef);
            if (privSnap.exists) {
              const newXp = (privSnap.data().xp || 0) + XP[place];
              tx.update(privRef, { xp: newXp });
            }
          }
        }
      });
      socket.emit('shop:pending_resolved', { type, place, delta });
    } catch(e) {
      if (e && e.code) return; // silent, user_not_found just ignore
      console.error('shop:resolve_pending', e.message);
    }
  });

  socket.on('mm:join', async ({ nick, rating, uid, idToken, wins, games, paddleStats, trainingMode, avatarId, platform }) => {
    // ── SECURITY: верифікуємо idToken, витягуємо реальний uid ──
    const verifiedUid = await resolveUid({ idToken, fallbackUid: uid });
    if (admin && !verifiedUid && !trainingMode) {
      // В проді Admin SDK доступний — без токена не пускаємо
      socket.emit('mm:error', 'Authentication required');
      return;
    }
    // Використовуємо verified uid (або fallback в dev-режимі без Admin)
    const realUid = verifiedUid;
    if (realUid) socket.uid = realUid; // для shop handlers
    if (realUid && !trainingMode) trackPlayerSeen(realUid, platform);

    // ── SECURITY: ігноруємо клієнтський paddleStats і rating ──
    // Завантажуємо з Firestore (server-authoritative).
    // Якщо db відсутній — використовуємо дефолтні значення.
    const serverStats = await loadValidatedPaddleStats(realUid);
    const serverRating = realUid ? await loadValidatedRating(realUid) : (rating|0) || 500;
    // Якщо клієнт встиг disconnect поки ми читали Firestore — нічого не робимо
    if (!socket.connected) return;

    if(trainingMode){
      const tRoom = createRoom('training_'+socket.id);
      rooms.set(tRoom.id, tRoom);
      myRoom = tRoom; mySlot = 0;
      setSlotPlayer(tRoom, socket.id, { slot:0, nick, rating: serverRating, uid: realUid, wins:wins||0, games:games||0, input:{},
        paddleStats: serverStats, trainingMode:true });
      socket.join(tRoom.id);
      socket.emit('mm:joined',{mySlot:0,roomId:tRoom.id});
      socket.emit('myslot',{mySlot:0,roomId:tRoom.id});
      startGame(tRoom);
      return;
    }
    const room = findOrCreateRoom();
    myRoom = room; mySlot = getAvailableSlot(room);
    if (mySlot === undefined) {
      const botSlot = SLOTS.find(s => room.bots[s] && !Object.values(room.players).some(p=>p.slot===s));
      if (botSlot !== undefined) { delete room.bots[botSlot]; mySlot = botSlot; }
      else { socket.emit('mm:error', 'Кімната повна'); return; }
    }
    setSlotPlayer(room, socket.id, { slot: mySlot, nick, rating: serverRating, uid: realUid, wins: wins||0, games: games||0, avatarId: avatarId||0, input: {},
      paddleStats: serverStats });
    socket.join(room.id);
    socket.emit('mm:joined', { mySlot, roomId: room.id });
    broadcastLobby(room);
    const count = Object.keys(room.players).length;
    // Ranked — без ботів, чекаємо реальних гравців
    if (count >= 4) {
      if (room.countdownTimer) { clearInterval(room.countdownTimer); room.countdownTimer = null; }
      startGame(room);
    } else {
      broadcastLobby(room);
      // ── БАГ 4 FIX: countdown не рестартиться, новачку надсилаємо поточний час ──
      if (room.countdownTimer) {
        socket.emit('mm:countdown', { timeLeft: room.countdownTimeLeft });
      } else {
        startCountdown(room);
      }
    }
  });

  socket.on('rejoin', async ({ roomId, slot, nick, rating, uid, idToken, paddleStats }) => {
    const room = rooms.get(roomId);
    if (!room) { socket.emit('rejoin:fail', { reason: 'room_gone' }); return; }

    // ── SECURITY: верифікуємо idToken ──
    const verifiedUid = await resolveUid({ idToken, fallbackUid: uid });
    if (admin && !verifiedUid) {
      socket.emit('rejoin:fail', { reason: 'auth_required' });
      return;
    }
    const realUid = verifiedUid;

    const alreadyTaken = Object.values(room.players).some(p => p.slot === slot);
    if (alreadyTaken) { socket.emit('rejoin:fail', { reason: 'slot_taken' }); return; }

    // ── Додатковий захист: слот, який ми відновлюємо, має належати цьому UID ──
    // (щоб не можна було підхопити чужий відключений слот)
    const savedData = room._disconnected && room._disconnected[slot];
    if (savedData && savedData.uid && realUid && savedData.uid !== realUid) {
      console.log('[rejoin] UID mismatch: slot='+slot+' wants='+realUid+' saved='+savedData.uid);
      socket.emit('rejoin:fail', { reason: 'wrong_uid' });
      return;
    }

    // ── Відміняємо таймер кіку для цього слоту ──
    if (room._slotDeleteTimers && room._slotDeleteTimers[slot]) {
      clearTimeout(room._slotDeleteTimers[slot]);
      delete room._slotDeleteTimers[slot];
    }

    // ── SECURITY: перезавантажуємо paddleStats з БД, не довіряємо клієнту ──
    let restoredPaddleStats;
    if (savedData && savedData.paddleStats) {
      restoredPaddleStats = savedData.paddleStats;
    } else {
      restoredPaddleStats = await loadValidatedPaddleStats(realUid);
      if (!socket.connected) return;
    }
    // Rating також валідуємо з БД
    const serverRating = realUid ? await loadValidatedRating(realUid) : (rating|0) || 500;
    if (!socket.connected) return;

    if (room._disconnected) delete room._disconnected[slot];

    if (realUid) socket.uid = realUid;
    myRoom = room; mySlot = slot;
    delete room.bots[slot];
    setSlotPlayer(room, socket.id, { slot, nick, rating: serverRating, uid: realUid, input: {}, paddleStats: restoredPaddleStats });
    socket.join(room.id);

    // ── Сповіщаємо інших що гравець повернувся ──
    io.to(room.id).emit('player:reconnected', { slot });

    if (room.status === 'playing' && room.game) {
      const gs = room.game;
      // Paddle visuals для всіх слотів — щоб після reconnect відображались правильно
      const rejoinPaddleVisuals = SLOTS.map(s => {
        const p = getSlotPlayer(room, s);
        const stats = p?.paddleStats || room._disconnected?.[s]?.paddleStats || {};
        const isBot = !p && !room._disconnected?.[s] && room.bots?.[s];
        if (isBot) {
          const botRating = room.bots[s].rating || 490;
          return { paddleId: Math.min(19, Math.floor(botRating / 80)), avgUpgrade: Math.floor(Math.random() * 60) };
        }
        return {
          paddleId: stats.paddleId !== undefined ? stats.paddleId : 0,
          avgUpgrade: stats.avgUpgrade !== undefined ? Math.round(stats.avgUpgrade * 100) : 0,
        };
      });
      socket.emit('rejoin:game', {
        mySlot: slot, players: buildPlayers(room),
        gameOver: gs.gameOver, winner: gs.winner,
        lives: gs.lives, scores: gs.scores,
        paddleVisuals: rejoinPaddleVisuals,
      });
      socket.emit('myslot', { mySlot: slot });
    } else if (room.status === 'waiting' || room.status === 'countdown') {
      socket.emit('rejoin:lobby', { mySlot: slot });
      // Якщо countdown вже йде — reconnected клієнту теж надсилаємо поточний час
      if (room.countdownTimer) {
        socket.emit('mm:countdown', { timeLeft: room.countdownTimeLeft });
      }
      broadcastLobby(room);
    } else {
      socket.emit('rejoin:fail', { reason: 'unknown_status' });
    }
  });

  socket.on('input', ({ left, right, boost, hist, pos, boostPos, fieldPos }) => {
    if (!myRoom || !myRoom.players[socket.id]) return;
    const player = myRoom.players[socket.id];
    const gs = myRoom.game;

    // ── Input buffering ──
    const anyBoost = hist && hist.length > 1 ? hist.some(h => h.boost) : false;
    const finalBoost = boost || anyBoost;
    // Якщо поточний pos відсутній але є в hist — беремо останній
    const effectivePos = pos !== undefined ? pos : (hist && hist.length > 0 ? hist[hist.length-1].pos : undefined);
    player.input = { left, right, boost: finalBoost, fieldPos };
    // Зберігаємо позицію при активації поля
    if (finalBoost && !player.input._boostSaved) {
      player.input.boostPos = boostPos;
      player.input._boostSaved = true;
    } else if (!finalBoost) {
      player.input._boostSaved = false;
    }
    player._lastInputSeq = (player._lastInputSeq || 0) + 1;

    // ── Server follows client position ──
    // Приймаємо позицію від клієнта якщо вона в межах допустимого
    if (effectivePos !== undefined && gs && !gs.eliminated[player.slot]) {
      const pos = effectivePos; // використовуємо ефективний pos
      const slot = player.slot;
      const view = SLOT_VIEW[slot];
      const isHoriz = view === 'top' || view === 'bottom';
      const pStats = player.paddleStats || {};
      const pW = pStats.w || PL;
      const half = pW / 2;
      const mn = C + half;
      const mx = (isHoriz ? W : H) - C - half;

      // Клампуємо в межі поля
      const clampedPos = Math.max(mn, Math.min(mx, pos));
      const serverPos = gs.paddles[slot];
      const diff = Math.abs(clampedPos - serverPos);

      // Максимальне допустиме відхилення за тік:
      // швидкість ракетки × кількість тіків між пакетами (~3) + запас
      const maxSpeed = pStats.spd || PS;
      const MAX_DRIFT = maxSpeed * 6 + 20; // ~40px при нормальній грі

      if (diff <= MAX_DRIFT) {
        gs.paddles[slot] = clampedPos;
      } else {
        gs.paddles[slot] += (clampedPos - serverPos) * 0.3;
        console.log(`Paddle anomaly slot${slot}: diff=${diff.toFixed(0)}px`);
      }

      // Якщо поле активне — використовуємо fieldPos для точної синхронізації відбиття
      const _inp = player.input;
      if (_inp && _inp.fieldPos !== undefined && gs.fields[slot] && gs.fields[slot].active) {
        const fpClamped = Math.max(mn, Math.min(mx, _inp.fieldPos));
        const fpDiff = Math.abs(fpClamped - serverPos);
        if (fpDiff <= MAX_DRIFT) {
          gs.paddles[slot] = fpClamped;
        }
        _inp.fieldPos = undefined;
      }
    }
  });

  socket.on('mm:cancel', () => leave({ voluntary: true }));
  // Явний вихід з кнопки — одразу eliminates
  socket.on('leave_game', () => leave({ voluntary: true }));
  // Raw disconnect (transport close, ping timeout, закрив таб) — даємо 30с на rejoin
  socket.on('disconnect', () => leave({ voluntary: false }));



  function leave(opts = {}) {
    const voluntary = opts.voluntary === true;
    if (!myRoom) return;
    const room = myRoom;
    const slot = mySlot;
    const pinfo = room.players[socket.id];
    socket.leave(room.id);

    if (room.status === 'playing' && slot !== null && pinfo && room.game && !room.game.gameOver) {
      // ── СТРАТЕГІЯ ──
      // voluntary (натиснув "Вийти") → одразу markEliminated. Блокує експлойт.
      // non-voluntary (disconnect) → 30с grace на rejoin. Після — markEliminated.
      if (voluntary) {
        markEliminated(room.game, slot, 'leave');
        // Дані для commitRewards (uid потрібен для запису рейтингу)
        room._disconnected = room._disconnected || {};
        room._disconnected[slot] = {
          nick: pinfo.nick, rating: pinfo.rating, uid: pinfo.uid,
          paddleStats: pinfo.paddleStats,
          leftVoluntarily: true,
        };
        clearSlotPlayer(room, socket.id);
        myRoom = null; mySlot = null;
        io.to(room.id).emit('player:left', { slot });

        const active = activeSlots(room.game);
        if (active.length === 1) {
          endGame(room, active[0]);
        } else if (active.length === 0) {
          if (room.tickInterval) clearInterval(room.tickInterval);
          if (room.matchTimerInterval) clearInterval(room.matchTimerInterval);
          rooms.delete(room.id);
        }
        return;
      }

      // ── Raw disconnect — чекаємо 30с на rejoin ──
      room._disconnected = room._disconnected || {};
      room._disconnected[slot] = {
        nick: pinfo.nick, rating: pinfo.rating, uid: pinfo.uid,
        paddleStats: pinfo.paddleStats,
        leftVoluntarily: false,
      };
      clearSlotPlayer(room, socket.id);
      myRoom = null; mySlot = null;
      io.to(room.id).emit('player:disconnected', { slot, reconnectTimeout: 30 });

      // 30с timer: якщо не повернувся — markEliminated
      room._slotDeleteTimers = room._slotDeleteTimers || {};
      room._slotDeleteTimers[slot] = setTimeout(() => {
        // Якщо rejoin відбувся — _disconnected[slot] вже прибрано з rejoin handler
        if (!room._disconnected || !room._disconnected[slot]) return;
        console.log(`Room ${room.id}: slot ${slot} timed out → markEliminated`);
        delete room._slotDeleteTimers[slot];
        // Гравець НЕ повернувся → elim (залишаємо дані в _disconnected для commitRewards!)
        if (room.game && !room.game.gameOver) {
          markEliminated(room.game, slot, 'timeout');
          io.to(room.id).emit('player:left', { slot });
          const active = activeSlots(room.game);
          if (active.length === 1) {
            endGame(room, active[0]);
          } else if (active.length === 0) {
            if (room.tickInterval) clearInterval(room.tickInterval);
            if (room.matchTimerInterval) clearInterval(room.matchTimerInterval);
            rooms.delete(room.id);
          }
        }
      }, 30000);
      return;
    }

    // Не під час playing — звичайне видалення
    clearSlotPlayer(room, socket.id);
    myRoom = null; mySlot = null;

    const count = Object.keys(room.players).length;
    if (count === 0) {
      if (room.tickInterval) clearInterval(room.tickInterval);
      if (room.countdownTimer) clearInterval(room.countdownTimer);
      if (room.matchTimerInterval) clearInterval(room.matchTimerInterval);
      rooms.delete(room.id);
    } else {
      broadcastLobby(room);
      if (room.status === 'countdown' && count === 1) {
        if (room.countdownTimer) { clearInterval(room.countdownTimer); room.countdownTimer = null; }
        room.status = 'waiting';
        io.to(room.id).emit('mm:waiting', {});
      }
    }
  }
});

httpServer.listen(PORT, () => console.log(`Server on port ${PORT}, ${TICK_RATE} ticks/sec`));

// ── Лог активних кімнат кожні 30с ──
setInterval(() => {
  if (rooms.size === 0) return;
  for (const [rid, room] of rooms) {
    if (!room.game || room.game.gameOver) continue;
    const gs = room.game;
    const players = Object.values(room.players).map(p =>
      `slot${p.slot}:${p.nick}(ping?)`
    ).join(', ');
    const balls = gs.balls.map(b =>
      `[${b.x.toFixed(0)},${b.y.toFixed(0)} v:${b.vx.toFixed(2)},${b.vy.toFixed(2)}]`
    ).join(' ');
    console.log(`[ROOM ${rid.slice(-4)}] tick=${gs.tick} | ${players} | balls: ${balls}`);
  }
}, 30000);

// ══════════════════════════════════════════════════════
// DAILY RATING REWARDS — Firebase Admin + cron
// ══════════════════════════════════════════════════════
let admin = null;
let db = null;

// Ініціалізуємо Firebase Admin якщо є змінні середовища
try {
  admin = require('firebase-admin');
  const serviceAccount = process.env.FIREBASE_SERVICE_ACCOUNT
    ? JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT)
    : null;

  if (serviceAccount && !admin.apps.length) {
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
      databaseURL: 'https://crazy-footbal-default-rtdb.europe-west1.firebasedatabase.app',
    });
    db = admin.firestore();
    console.log('Firebase Admin initialized');
  }
} catch(e) {
  console.log('Firebase Admin not available:', e.message);
}

const DAILY_REWARDS = { 1: 100, 2: 50, 3: 30 };

// ── SHOP SOCKET HANDLERS ──
function registerShopHandlers(socket) {
  // ── Купівля ракетки ──
  socket.on('shop:buy_paddle', async ({ paddleId }) => {
    if (!db) return socket.emit('shop:error', { msg: 'server_unavailable' });
    if (!socket.uid) return socket.emit('shop:error', { msg: 'auth_required' });
    const pid = parseInt(paddleId);
    const priceDef = PADDLE_PRICES_SRV[pid];
    if (!priceDef) return socket.emit('shop:error', { msg: 'invalid_paddle' });
    if (priceDef.cur === 'free') return socket.emit('shop:error', { msg: 'already_free' });

    const privRef = db.collection('users_private').doc(socket.uid);
    const pubRef  = db.collection('users_public').doc(socket.uid);

    try {
      // ── ТРАНЗАКЦІЯ: атомарно read → check → write ──
      // Firestore автоматично re-tryить, якщо документ змінився між read і write
      const result = await db.runTransaction(async (tx) => {
        const privSnap = await tx.get(privRef);
        if (!privSnap.exists) throw { code: 'user_not_found' };
        const priv = privSnap.data();

        if ((priv.ownedPaddles || []).includes(pid)) throw { code: 'already_owned' };

        const silver = priv.silver || 0;
        const gold   = priv.gold   || 0;
        if (priceDef.cur === 'silver' && silver < priceDef.price) throw { code: 'not_enough_silver' };
        if (priceDef.cur === 'gold'   && gold   < priceDef.price) throw { code: 'not_enough_gold' };

        const newOwned = [...(priv.ownedPaddles || []), pid];
        const privUpd = { ownedPaddles: newOwned };
        let newSilver = silver, newGold = gold;
        if (priceDef.cur === 'silver') { privUpd.silver = silver - priceDef.price; newSilver = privUpd.silver; }
        if (priceDef.cur === 'gold')   { privUpd.gold   = gold   - priceDef.price; newGold   = privUpd.gold; }

        tx.update(privRef, privUpd);
        tx.update(pubRef, { paddleId: pid });

        return { paddleId: pid, silver: newSilver, gold: newGold };
      });

      socket.emit('shop:bought_paddle', {
        paddleId: result.paddleId,
        ...(priceDef.cur === 'silver' ? { silver: result.silver } : { gold: result.gold }),
      });
      // Аналітика покупки
      trackEvent('purchase', {
        uid: socket.uid, kind: 'paddle', item: pid,
        amount: priceDef.price, cur: priceDef.cur,
      });
    } catch(e) {
      if (e && e.code) { socket.emit('shop:error', { msg: e.code }); return; }
      console.error('shop:buy_paddle', e.message);
      socket.emit('shop:error', { msg: 'server_error' });
    }
  });

  // ── Апгрейд модуля ──
  socket.on('shop:upgrade_hangar', async ({ paddleId, partId }) => {
    if (!db) return socket.emit('shop:error', { msg: 'server_unavailable' });
    if (!socket.uid) return socket.emit('shop:error', { msg: 'auth_required' });
    const pid = parseInt(paddleId);
    const VALID_PARTS = ['w','spd','fr','bm','er','fd'];
    if (!VALID_PARTS.includes(partId)) return socket.emit('shop:error', { msg: 'invalid_part' });

    const privRef = db.collection('users_private').doc(socket.uid);

    try {
      const result = await db.runTransaction(async (tx) => {
        const privSnap = await tx.get(privRef);
        if (!privSnap.exists) throw { code: 'user_not_found' };
        const priv = privSnap.data();

        const hangars = priv.hangars || {};
        const currentLv = ((hangars[pid] || {})[partId]) || 1;
        if (currentLv >= 10) throw { code: 'max_level' };

        const cost = HANGAR_COSTS_SRV[currentLv];
        if (!cost) throw { code: 'invalid_level' };

        const silver = priv.silver || 0;
        const gold   = priv.gold   || 0;
        if (cost.cur === 'silver' && silver < cost.price) throw { code: 'not_enough_silver' };
        if (cost.cur === 'gold'   && gold   < cost.price) throw { code: 'not_enough_gold' };

        const newLv = currentLv + 1;
        const upd = {};
        upd[`hangars.${pid}.${partId}`] = newLv;
        let newSilver = silver, newGold = gold;
        if (cost.cur === 'silver') { upd.silver = silver - cost.price; newSilver = upd.silver; }
        if (cost.cur === 'gold')   { upd.gold   = gold   - cost.price; newGold   = upd.gold; }

        tx.update(privRef, upd);
        return { paddleId: pid, partId, newLevel: newLv, silver: newSilver, gold: newGold, cur: cost.cur };
      });

      socket.emit('shop:upgraded', {
        paddleId: result.paddleId, partId: result.partId, newLevel: result.newLevel,
        ...(result.cur === 'silver' ? { silver: result.silver } : { gold: result.gold }),
      });
      trackEvent('purchase', {
        uid: socket.uid, kind: 'hangar', item: `${result.paddleId}:${result.partId}:${result.newLevel}`,
        amount: HANGAR_COSTS_SRV[result.newLevel - 1]?.price, cur: result.cur,
      });
    } catch(e) {
      if (e && e.code) { socket.emit('shop:error', { msg: e.code }); return; }
      console.error('shop:upgrade_hangar', e.message);
      socket.emit('shop:error', { msg: 'server_error' });
    }
  });

  // ── Купівля енергії ──
  socket.on('shop:buy_energy', async () => {
    if (!db) return socket.emit('shop:error', { msg: 'server_unavailable' });
    if (!socket.uid) return socket.emit('shop:error', { msg: 'auth_required' });
    const privRef = db.collection('users_private').doc(socket.uid);

    try {
      const result = await db.runTransaction(async (tx) => {
        const privSnap = await tx.get(privRef);
        if (!privSnap.exists) throw { code: 'user_not_found' };
        const priv = privSnap.data();

        const gold = priv.gold || 0;
        if (gold < ENERGY_GOLD_COST_SRV) throw { code: 'not_enough_gold' };

        const newGold = gold - ENERGY_GOLD_COST_SRV;
        tx.update(privRef, {
          gold: newGold,
          energy: 100,
          energyLastRegen: Date.now(),
        });
        return { gold: newGold, energy: 100 };
      });

      socket.emit('shop:energy_bought', result);
      trackEvent('purchase', {
        uid: socket.uid, kind: 'energy', item: 'refill_100',
        amount: ENERGY_GOLD_COST_SRV, cur: 'gold',
      });
    } catch(e) {
      if (e && e.code) { socket.emit('shop:error', { msg: e.code }); return; }
      console.error('shop:buy_energy', e.message);
      socket.emit('shop:error', { msg: 'server_error' });
    }
  });
}

async function processDailyRewards() {
  if (!db) { console.log('No Firebase db — skipping daily rewards'); return; }

  const yesterday = new Date(Date.now() - 86400000).toISOString().slice(0, 10);
  console.log('Processing daily rewards for:', yesterday);

  try {
    // Завантажуємо гравців які грали вчора
    const snapshot = await db.collection('users_public')
      .where('ratingDate', '==', yesterday)
      .get();

    if (snapshot.empty) {
      console.log('No players for', yesterday);
      return;
    }

    // Формуємо список з денним приростом
    const players = [];
    snapshot.forEach(doc => {
      const d = doc.data();
      if (d.ratingToday && d.ratingToday > 0) {
        players.push({ uid: doc.id, nick: d.nickname || 'Гравець', ratingToday: d.ratingToday });
      }
    });

    if (!players.length) { console.log('No active players'); return; }

    players.sort((a, b) => b.ratingToday - a.ratingToday);
    const top3 = players.slice(0, 3);
    console.log('Top 3:', top3.map(p => p.nick + ': +' + p.ratingToday));

    // Зберігаємо знімок дня
    await db.collection('dailySnapshots').doc(yesterday).set({
      date: yesterday,
      top3,
      totalPlayers: players.length,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
    });

    // Нараховуємо нагороди
    const batch = db.batch();
    for (let i = 0; i < top3.length; i++) {
      const player = top3[i];
      const place = i + 1;
      const gold = DAILY_REWARDS[place];
      if (!gold) continue;

      // Перевіряємо чи вже отримував
      const existing = await db.collection('users_public').doc(player.uid)
        .collection('notifications')
        .where('type', '==', 'daily_rating_reward')
        .where('rewardDate', '==', yesterday)
        .limit(1).get();
      if (!existing.empty) continue;

      const placeEmojis = { 1: '🥇', 2: '🥈', 3: '🥉' };

      // Нараховуємо золото
      batch.update(db.collection('users_public').doc(player.uid), {
        gold: admin.firestore.FieldValue.increment(gold),
      });

      // Сповіщення
      batch.set(db.collection('users_public').doc(player.uid).collection('notifications').doc(), {
        type: 'daily_rating_reward',
        rewardDate: yesterday,
        place,
        goldAmount: gold,
        text: placeEmojis[place] + ' ' + place + '-е місце денного рейтингу (' + yesterday + ')! +🪙' + gold + ' золота нараховано',
        read: false,
        resolved: true,
        date: admin.firestore.FieldValue.serverTimestamp(),
      });

      console.log('Rewarded:', player.nick, 'place', place, 'gold', gold);
    }

    await batch.commit();
    console.log('Daily rewards done!');

  } catch(e) {
    console.error('processDailyRewards error:', e.message);
  }
}

// ── Cron: запускаємо о 00:01 щодня ──
function scheduleDailyRewards() {
  const now = new Date();
  // Час до наступної 00:01 UTC
  const next = new Date(Date.UTC(
    now.getUTCFullYear(),
    now.getUTCMonth(),
    now.getUTCDate() + 1, // завтра
    0, 1, 0 // 00:01:00
  ));
  const msUntilNext = next.getTime() - now.getTime();
  console.log('Next daily rewards in:', Math.round(msUntilNext / 1000 / 60), 'minutes');

  setTimeout(() => {
    processDailyRewards();
    // Після першого запуску — повторюємо кожні 24 години
    setInterval(processDailyRewards, 24 * 60 * 60 * 1000);
  }, msUntilNext);
}

scheduleDailyRewards();

// ══════════════════════════════════════════════════════
// GRACEFUL SHUTDOWN — SIGTERM/SIGINT
// ══════════════════════════════════════════════════════
// Railway при redeploy надсилає SIGTERM, через ~10с — SIGKILL.
// Наша задача:
//   1. Перестати приймати нові підключення.
//   2. Повідомити активні кімнати про shutdown.
//   3. Зупинити ігрові tick-loops.
//   4. Дочекати pending PostgreSQL записів (аналітика).
//   5. Exit(0) — щоб Railway зарахував shutdown як чистий.
//
// Failsafe: якщо graceful hang'не — форс-вихід через 8с.
let _shuttingDown = false;
async function gracefulShutdown(signal) {
  if (_shuttingDown) return;
  _shuttingDown = true;
  console.log(`[shutdown] Received ${signal}, starting graceful shutdown...`);

  // Failsafe — якщо щось зависне, примусово виходимо через 8с
  const forceExit = setTimeout(() => {
    console.log('[shutdown] Force exit after 8s timeout');
    process.exit(1);
  }, 8000);
  forceExit.unref();

  try {
    // 1. Перестаємо слухати нові HTTP-з'єднання (існуючі WS живуть)
    httpServer.close(() => console.log('[shutdown] HTTP server closed'));

    // 2. Проходимось по всіх кімнатах — зупиняємо timers, повідомляємо клієнтів
    let notifiedRooms = 0;
    for (const [, room] of rooms) {
      if (room.tickInterval)        { clearInterval(room.tickInterval);        room.tickInterval = null; }
      if (room.matchTimerInterval)  { clearInterval(room.matchTimerInterval);  room.matchTimerInterval = null; }
      if (room.countdownTimer)      { clearInterval(room.countdownTimer);      room.countdownTimer = null; }
      if (room._slotDeleteTimers) {
        for (const t of Object.values(room._slotDeleteTimers)) clearTimeout(t);
        room._slotDeleteTimers = {};
      }
      if (room.status === 'playing' || room.status === 'countdown' || room.status === 'waiting') {
        io.to(room.id).emit('server:shutdown', { reason: signal, graceMs: 3000 });
        notifiedRooms++;
      }
    }
    console.log(`[shutdown] Notified ${notifiedRooms} active rooms, waiting 2s for delivery...`);

    // 3. Даємо подіям долетіти до клієнтів (socket.io flush)
    await new Promise(r => setTimeout(r, 2000));

    // 4. Закриваємо Socket.IO (відключає усі сокети)
    await new Promise(resolve => {
      io.close(() => { console.log('[shutdown] Socket.IO closed'); resolve(); });
    });

    // 5. Закриваємо PostgreSQL pool — waits for in-flight queries
    if (pgPool) {
      try {
        await pgPool.end();
        console.log('[shutdown] PostgreSQL pool closed');
      } catch(e) {
        console.error('[shutdown] pgPool.end error:', e.message);
      }
    }

    console.log('[shutdown] Graceful shutdown complete. Exiting 0.');
    clearTimeout(forceExit);
    process.exit(0);
  } catch(e) {
    console.error('[shutdown] Error during shutdown:', e.message);
    process.exit(1);
  }
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT',  () => gracefulShutdown('SIGINT'));

// Fatal errors — логуємо і намагаємось shutdown, не крашимо тихо
process.on('uncaughtException', (err) => {
  console.error('[fatal] uncaughtException:', err.stack || err);
  gracefulShutdown('uncaughtException');
});
process.on('unhandledRejection', (reason) => {
  console.error('[fatal] unhandledRejection:', reason);
  // Не кидаємо shutdown для unhandledRejection — тільки логуємо,
  // бо часто це некритичні Firebase/Firestore помилки.
});
