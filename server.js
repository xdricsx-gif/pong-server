const UfoFleet = require('./galaxy-fleet');
// SERVER FINAL — Authoritative 60 tick
const { createServer } = require('http');
const { readFileSync } = require('fs');
const { join } = require('path');
const { Server } = require('socket.io');
const netcode2 = require('./netcode-server');
const {createMatchService}=require('./match-service');
let matchService;
const { Pool } = require('pg');
const { createBillingService, createPublisher, registerBillingHandlers } = require('./play-billing');
let playBilling = null;
const { createDashboard } = require('./football-dashboard');
const { createPromoHandler } = require('./football-promo');
const handlePromo = createPromoHandler();
let footballDashboard = null;
const {createPushService}=require('./push-service');
const {createPushHttp}=require('./push-http');
let pushService=null;
const {createPlayerActivity}=require('./player-activity');
let playerActivity=null;
const {createWelcome}=require('./onboarding-welcome');
const {createWelcomeGateway}=require('./welcome-socket');
let welcomeService=null, welcomeEnabled=false;
const welcomeGateway=createWelcomeGateway({getService:()=>welcomeService});

const httpServer = createServer();
const io = new Server(httpServer, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  // ── PING/PONG TIMEOUTS ──
  // Збільшено для стійкості до mobile network instability.
  // 3G/4G у транспорті, тунелях, при перемиканні cells — pause до 15-20с нормальний.
  // Раніше: pingInterval=5s, pingTimeout=10s → false-disconnect через ping timeout.
  // Тепер: pingInterval=10s (рідші перевірки), pingTimeout=25s (терпиме до stalls).
  // Загалом max gap клієнт→сервер = 35s до disconnect (10 + 25).
  pingInterval: 10000,
  pingTimeout: 25000,
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
    pgPool = new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: { rejectUnauthorized: false },
      // Connection pool tuning — Railway PG може закривати idle connection
      max: 10,                              // макс concurrent connections
      idleTimeoutMillis: 30000,             // закривати idle через 30с (нормально)
      connectionTimeoutMillis: 5000,        // 5с на встановлення
    });

    // ── КРИТИЧНО: pool.on('error') ──
    // Без цього handler'а будь-яка idle-connection помилка тригерить uncaughtException
    // → графefulShutdown → crash гри. Тепер перехоплюємо тут і пробуємо reconnect.
    pgPool.on('error', (err, client) => {
      console.error('[Analytics] PostgreSQL idle connection error (handled):', err.message);
      // Не падаємо — пул сам спробує перепідключитись на наступному запиті
    });

    // Окремий handler для конкретних client помилок під час query
    pgPool.on('connect', (client) => {
      client.on('error', (err) => {
        console.error('[Analytics] PostgreSQL client error (handled):', err.message);
      });
    });

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
    const mode = room.mode || (isTraining ? 'training' : (botsCount > 0 ? 'ranked_bots' : 'ranked'));
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
      const placeIdx = room.managed ? room.result?.rewards?.[p.slot]?.place ?? -1 : places.indexOf(p.slot);
      const goalsConceded = gs?.scores?.[p.slot] ?? null;
      const ratingDelta = ratingDeltas[p.slot] != null ? ratingDeltas[p.slot] : null;
      const newRating = (mode === 'ranked' && typeof p.rating === 'number' && ratingDelta != null)
        ? Math.max(0, p.rating + ratingDelta)
        : null;

      playerOps.push(trackEvent('game_end', {
        uid: p.uid, roomId: room.id, mode,
        place: placeIdx < 0 ? null : placeIdx + 1, durationSec,
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
const ML = 15, EPU = 1 / 3, ECR = 1 / 10000;
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
  const pid = 0;
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

// ═══════════════════════════════════════════════════════
// ── LINEAR SUBLEVEL SYSTEM (server) ──
// 4 levels × 10 sublevels = 40 послідовних апгрейдів
// HANGAR_SUB_COSTS_SRV[i] = ціна купівлі sub'у i (для переходу sub=i-1 → i)
// Лінійна прогресія цін: дешеві ранні, дорогі пізні (gold для L4)
// HISTORICAL: раніше 5 рівнів × 10 = 50. Найслабший L1=НОВАЧОК видалено,
// залишилось 4 рівні з нумерацією L1..L4 (=stat'и старих L2..L5).
// ═══════════════════════════════════════════════════════
const MAX_SUB_SRV = 40;
const HANGAR_SUB_COSTS_SRV = (function(){
  const arr = [null];
  for(let i=1; i<=40; i++){
    if(i<=10){
      arr.push({cur:'silver', price: 1500 + (i-1)*800});      // 1500..8700 silver (L1=пілот)
    } else if(i<=20){
      arr.push({cur:'silver', price: 12000 + (i-11)*2000});   // 12000..30000 silver (L2=ветеран)
    } else if(i<=30){
      arr.push({cur:'silver', price: 35000 + (i-21)*3500});   // 35000..66500 silver (L3=еліта)
    } else {
      arr.push({cur:'gold', price: 100 + (i-31)*20});         // 100..280 gold (L4=легенда)
    }
  }
  return arr;
})();

// Множник на основі рівня — L1=1.06, L2=1.13, L3=1.21, L4=1.30
// Старий L1=1.00 видалено; кожен рівень тепер відповідає старому L+1.
function getLevelMultSrv(level){
  return [1.00, 1.06, 1.13, 1.21, 1.30][level] || 1.06;
}

// Переводимо sub → {level, sublevel}
// 4-рівнева система: sub 0..40, level 1..4, sublevel 0..10
function subToLevelSrv(sub){return UfoFleet.progress(sub);}

// Отримуємо поточний sub з hangar object (з міграцією з legacy 6-параметрів)
function getHangarSubSrv(hangar){
  if(!hangar) return 0;
  if(typeof hangar.sub === 'number') return Math.max(0, Math.min(MAX_SUB_SRV, hangar.sub));
  // Migration: sum of (part-1) over 6 параметрів, max 54 → clamp to 50
  let total = 0;
  ['w','spd','fr','bm','er','fd'].forEach(p => { total += (hangar[p]||1) - 1; });
  return Math.min(MAX_SUB_SRV, total);
}

// Клас корабля по paddleId (синхронізовано з client getPaddleClass)
//   ids 0-3 → E, 4-7 → D, 8-11 → C, 12-15 → B, 16-19 → A
function getPaddleClassSrv(pid){
  pid = pid|0;
  if(pid<=3)  return 'E';
  if(pid<=7)  return 'D';
  if(pid<=11) return 'C';
  if(pid<=15) return 'B';
  return 'A';
}

// Множник ціни прокачки залежно від класу (A — базова, E у 15× дешевша)
const CLASS_COST_MULT_SRV = {
  A: 1.00,
  B: 0.50,
  C: 0.25,
  D: 0.13,
  E: 1/15,  // ≈ 0.067
};

// Повертає {cur, price} для прокачки конкретного корабля до конкретного sub'у
function getUpgradeCostSrv(pid, nextSub){
  if(nextSub < 1 || nextSub > MAX_SUB_SRV) return null;
  const base = HANGAR_SUB_COSTS_SRV[nextSub];
  if(!base) return null;
  const cls = getPaddleClassSrv(pid);
  const mult = CLASS_COST_MULT_SRV[cls] !== undefined ? CLASS_COST_MULT_SRV[cls] : 1.0;
  let price = base.price * mult;
  if(base.cur === 'silver'){
    price = Math.max(10, Math.round(price/10)*10);
  } else {
    price = Math.max(1, Math.round(price));
  }
  return {cur: base.cur, price: price};
}

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
const PADDLE_CATALOG_SRV = [UfoFleet.base];

const HANGAR_PARTS_SRV = ['w', 'spd', 'fr', 'bm', 'er', 'fd'];

// lv1=0.9x, lv10=1.2x (те саме що клієнтський hangarMult)
function hangarMultSrv(level) {
  const lv = Math.max(1, Math.min(10, level|0));
  return 0.90 + (lv - 1) * (0.30 / 9);
}

// Дефолтні stats — для нових гравців або fallback при помилках.
const DEFAULT_PADDLE_STATS = Object.freeze(UfoFleet.stats(0));

// Pure функція — розраховує фінальні stats без I/O.
// hangars — { '5': {sub:13} or legacy {w:3, spd:7, ...}, '12': {...} }
function computePaddleStats(paddleId, hangars) {return UfoFleet.stats(hangars?.[0]?.sub);}

// Async: читає актуальні дані гравця з Firestore, валідує і повертає
// серверно-розраховані paddleStats. Блокує читерство:
//   - paddleId, який гравець не придбав → fallback до 0
//   - вигадані рівні апгрейдів → обрізаємо до 1..10
// Якщо Firestore недоступний (db=null) або uid відсутній — дефолтна ракетка.
async function loadValidatedPaddleStats(uid, {strict=false}={}) {
  if (!db || !uid) {if(strict)throw Error('server_unavailable');return { ...DEFAULT_PADDLE_STATS };}
  try {
    const [pubSnap, privSnap] = await Promise.all([
      db.collection('users_public').doc(uid).get(),
      db.collection('users_private').doc(uid).get(),
    ]);
    if(strict&&(!pubSnap.exists||!privSnap.exists))throw Error('server_unavailable');
    const pub = pubSnap.exists ? pubSnap.data() : {};
    const priv = privSnap.exists ? privSnap.data() : {};

    // Read the new namespace only. Never overwrite prior purchases or hangars.
    return UfoFleet.fromAccount(priv);
  } catch(e) {
    if(strict)throw Error('server_unavailable');
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
    return { uid: decoded.uid, anonymous: decoded.firebase?.sign_in_provider === 'anonymous', expiresAt: Number(decoded.exp)*1000 };
  } catch(e) {
    console.log('[auth] token verification failed:', e.code || e.message);
    return null;
  }
}

// Helper: розв'язує фінальний UID для handler'а.
// - Якщо Admin SDK доступний → вимагає валідний idToken, повертає verified uid.
// - Якщо Admin SDK недоступний → fallback на клієнтський uid (для dev).
// Повертає string uid або null.
async function resolveUid({ idToken, fallbackUid, onVerified }) {
  if (!admin) return fallbackUid || null; // legacy fallback
  const verified = await verifyAuthToken(idToken);
  if(!verified)return null;
  try{if((await db.collection('dashboard_admin_players').doc(verified.uid).get()).data()?.banned)return null;}catch{return null;}
  onVerified?.(verified);
  return verified.uid;
}

// ══════════════════════════════════════════════════════
// PUBLIC CONFIG ENDPOINT — /config.json
// ══════════════════════════════════════════════════════
// Єдина точка правди для public констант. Клієнт fetch'ить це
// при старті і перезаписує свої локальні значення.
// Змінюючи константу — мінять ТУТ, клієнт автоматично підхопить.
// CONFIG_VERSION — якщо бампаєш — clients повинні refetch (кеш invalidate).
const CONFIG_VERSION = 2;

// Повний каталог ракеток (дзеркало клієнтського, плюс name/desc/price — для UI)
const PADDLE_CATALOG_FULL = [UfoFleet.base];

function getPublicConfig() {
  return {
    version: CONFIG_VERSION,
    paddleCatalog: PADDLE_CATALOG_FULL,
    fleetVersion: UfoFleet.VERSION,
    hangarCosts: HANGAR_COSTS_SRV,       // null, silver/gold ціни за рівень (LEGACY)
    hangarSubCosts: HANGAR_SUB_COSTS_SRV, // нова лінійна прокачка (5×10 = 50)
    energyGoldCost: ENERGY_GOLD_COST_SRV,
    game: {
      W, H, C:54, BR, SMAX, PL:54, PLV:54, PTH:54, PTV:54,
      ML, EPU, FR, BMULT, PS:UfoFleet.base.spd,
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

const handleAuthSession=require('./auth-session').createAuthSession({getAuth:()=>admin?.auth(),getDb:()=>db});
const handlePush=createPushHttp({getService:()=>pushService,verifyIdToken:token=>admin.auth().verifyIdToken(token,true)});
httpServer.on('request', (req, res) => {
  if(req.url.startsWith('/push/')){handlePush(req,res).catch(()=>{if(!res.headersSent)res.writeHead(503,{'Cache-Control':'no-store'});res.end();});return;}
  if(req.url==='/auth/session'){handleAuthSession(req,res).catch(()=>{if(!res.headersSent)res.writeHead(503,{'Cache-Control':'no-store'});res.end();});return;}
  if (handlePromo(req, res)) return;
  if (req.url === '/dashboard' || req.url.startsWith('/dashboard/')) {
    if (!footballDashboard) { res.writeHead(503, {'Content-Type':'text/plain','Cache-Control':'no-store'}); res.end('Dashboard unavailable'); return; }
    footballDashboard.handle(req, res).catch(() => { if (!res.headersSent) res.writeHead(503); res.end(); });
    return;
  }
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
  const publicPages = { '/delete-account': 'delete-account.html', '/privacy': 'privacy.html' };
  const pagePath = req.url.split('?')[0];
  const publicPage = Object.prototype.hasOwnProperty.call(publicPages, pagePath) ? publicPages[pagePath] : null;
  if (['GET', 'HEAD'].includes(req.method) && publicPage) {
    res.writeHead(200, {
      'Content-Type': 'text/html; charset=utf-8',
      'Cache-Control': 'public, max-age=300',
      'X-Content-Type-Options': 'nosniff',
      'Referrer-Policy': 'no-referrer',
    });
    res.end(req.method === 'HEAD' ? undefined : readFileSync(join(__dirname, publicPage)));
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

function clampBallObj(ball, room, gs) {
  for(const s of CS){
    const d=(ball.x-s.ax)*s.nx+(ball.y-s.ay)*s.ny;
    if(d<-BR){
      // ── PRE-EVENT для клієнтської reconciliation ──
      const preX = ball.x, preY = ball.y, preVx = ball.vx, preVy = ball.vy;
      const dv=ball.vx*s.nx+ball.vy*s.ny;ball.vx-=2*dv*s.nx;ball.vy-=2*dv*s.ny;
      const{cx,cy}=cPt(ball.x,ball.y,s.ax,s.ay,s.bx,s.by);
      ball.x=cx+s.nx*(BR+1);ball.y=cy+s.ny*(BR+1);
      // ── AAA EVENT: chamfer (corner) bounce ──
      if (room && gs && typeof io !== 'undefined') {
        io.to(room.id).emit('event:ball', {
          type: 'chamfer_bounce',
          tick: gs.tick,
          ballId: ball.id,
          pre:  { x: preX, y: preY, vx: preVx, vy: preVy },
          post: { x: ball.x, y: ball.y, vx: ball.vx, vy: ball.vy },
        });
      }
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

// ── FORCE FIELD REPULSOR (continuous field) ──
// 
// Поведінка: поле працює як гравітаційний репульсор. Щотіку м'яч, що знаходиться
// в радіусі, отримує імпульс по нормалі (від центра поля назовні). Чим ГЛИБШЕ
// м'яч у полі — тим СИЛЬНІШИЙ push. Додатково, при першому входженні в поле
// м'яч отримує класичне reflection — "удар".
//
// Це гарантує:
// - М'яч ЗАВЖДИ відлітає (не може застрягнути чи пролетіти наскрізь)
// - Більш прокачане поле (великий FR) = довший час впливу = сильніший відскок
// - Немає "крюка" — continuous push тільки виштовхує
const FF_K_DRAG            = 0.7;   // drag на reflection (коли паддл рухається)
const FF_REFLECT_PUSH      = 1.0;   // одноразовий push при вході м'яча у поле (удар)
const FF_PULSE_FORCE       = 0.28;  // базовий континуальний імпульс за тік
const FF_PULSE_CENTER_MULT = 4.5;   // множник у центрі поля (на 50% проникнення ≈ 2× сила)
const FF_MIN_SP            = 2.5;

// ═══════════════════════════════════════════════════════
// ── ELIMINATION WALL PHYSICS ──
// Коли гравець вибуває (lives=0) — його ворота стають постійним
// силовим полем. Відрізняється від звичайного FF:
// - Active завжди (не залежить від energy)
// - Випадкове відхилення нормалі (chaos factor) — м'яч відлітає
//   у непередбачуваному напрямку для більш цікавої гри
// - Boost: підсилює швидкість м'яча (як FF reflect, але без paddle vel)
// - Працює як "стіна-портал" — м'яч відбивається з мінімальною швидкістю
//   щоб не застрягав біля воріт
// ═══════════════════════════════════════════════════════
const ELIM_WALL_BOOST       = 1.30;  // множник speed після відскоку (+30%)
const ELIM_WALL_RANDOM_DEG  = 25;    // ±25° випадкового відхилення
const ELIM_WALL_MIN_SP      = 3.5;   // мінімальна швидкість після відскоку
const ELIM_WALL_MAX_SP      = 6.5;   // максимальна (clamp)

function applyEliminatedWall(gs, slot, ball, room) {
  // ── PRE-EVENT STATE ──
  const preX = ball.x, preY = ball.y, preVx = ball.vx, preVy = ball.vy;
  // slot: 0=bottom, 1=top, 2=left, 3=right
  // Нормаль "у центр поля" від воріт
  let nx, ny;
  const view = SLOT_VIEW[slot];
  if (view === 'bottom')     { nx = 0; ny = -1; }
  else if (view === 'top')   { nx = 0; ny =  1; }
  else if (view === 'left')  { nx = 1; ny =  0; }
  else                       { nx =-1; ny =  0; }

  // Класичне reflection
  const vDotN = ball.vx * nx + ball.vy * ny;
  let reflVx = ball.vx - 2 * vDotN * nx;
  let reflVy = ball.vy - 2 * vDotN * ny;

  // Випадкове відхилення (rotate vector by ±RANDOM_DEG)
  const randAngle = (Math.random() * 2 - 1) * ELIM_WALL_RANDOM_DEG * Math.PI / 180;
  const cosA = Math.cos(randAngle), sinA = Math.sin(randAngle);
  const rotX = reflVx * cosA - reflVy * sinA;
  const rotY = reflVx * sinA + reflVy * cosA;
  reflVx = rotX; reflVy = rotY;

  // Boost speed
  reflVx *= ELIM_WALL_BOOST;
  reflVy *= ELIM_WALL_BOOST;

  // Clamp + ensure outward direction
  let sp = Math.hypot(reflVx, reflVy);
  if (sp < ELIM_WALL_MIN_SP) {
    // Якщо швидкість занадто мала — додаємо push в напрямку нормалі
    const k = ELIM_WALL_MIN_SP / Math.max(0.1, sp);
    reflVx *= k; reflVy *= k;
    sp = ELIM_WALL_MIN_SP;
  }
  if (sp > ELIM_WALL_MAX_SP) {
    const k = ELIM_WALL_MAX_SP / sp;
    reflVx *= k; reflVy *= k;
  }

  // Перевіряємо що швидкість напрямлена ВІД воріт (outward)
  const newVDotN = reflVx * nx + reflVy * ny;
  if (newVDotN < 0.5) {
    // Випадкове відхилення завернуло м'яч назад — додаємо нормальну компоненту
    reflVx += nx * (1.0 - newVDotN);
    reflVy += ny * (1.0 - newVDotN);
  }

  ball.vx = reflVx;
  ball.vy = reflVy;
  // ── AAA EVENT: elim wall bounce (non-deterministic через Math.random) ──
  if (room) {
    io.to(room.id).emit('event:ball', {
      type: 'elim_wall_bounce',
      tick: gs.tick,
      ballId: ball.id,
      slot,
      pre:  { x: preX, y: preY, vx: preVx, vy: preVy },
      post: { x: ball.x, y: ball.y, vx: ball.vx, vy: ball.vy },
    });
  }
}

function applyFFBall(gs, s, ball, room) {
  const f = gs.fields[s];
  if (!f || !f.active) return false;

  const p = slotToPaddle(s, gs.paddles[s], gs, null);
  const fcx = p.x + p.w/2, fcy = p.y + p.h/2;
  const maxR = f.maxR || FR;
  const collideR = maxR + BR;

  const dx = ball.x - fcx, dy = ball.y - fcy;
  const dist = Math.hypot(dx, dy);
  const oldX = ball.x - ball.vx, oldY = ball.y - ball.vy;
  const odx = oldX - fcx, ody = oldY - fcy;
  const oldDist = Math.hypot(odx, ody);

  // ── М'яч поза полем — нічого ──
  if (dist > collideR && oldDist > collideR) return false;

  // ── Визначаємо нормаль ──
  let nx, ny;
  if (dist > 1e-6) {
    nx = dx / dist; ny = dy / dist;
  } else {
    const view = SLOT_VIEW[s];
    if (view === 'bottom')     { nx = 0; ny = -1; }
    else if (view === 'top')   { nx = 0; ny =  1; }
    else if (view === 'left')  { nx = 1; ny =  0; }
    else                       { nx =-1; ny =  0; }
  }

  // ── Швидкість паддла ──
  const prevPos = gs.paddlesPrev ? (gs.paddlesPrev[s] != null ? gs.paddlesPrev[s] : gs.paddles[s]) : gs.paddles[s];
  const paddleMove = gs.paddles[s] - prevPos;
  const view = SLOT_VIEW[s];
  const paddleAxis = (view === 'bottom' || view === 'top') ? 'x' : 'y';
  const paddleVx = paddleAxis === 'x' ? paddleMove : 0;
  const paddleVy = paddleAxis === 'y' ? paddleMove : 0;

  const _hitKey = 'ff_hit_' + s;

  // ── Detect FIRST ENTRY: попередній тік поза полем, цей тік всередині ──
  // Тут застосовуємо класичне reflection — відчуття "удару" об поле.
  const justEntered = oldDist > collideR && dist <= collideR && !ball[_hitKey];

  if (justEntered) {
    // ── PRE-EVENT STATE для клієнтської reconciliation ──
    const preX = ball.x, preY = ball.y, preVx = ball.vx, preVy = ball.vy;
    // CCD — точка перетину межі
    let hitX = ball.x, hitY = ball.y;
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
    // Нормаль у точці контакту (уточнена)
    const hhdx = hitX - fcx, hhdy = hitY - fcy;
    const hhD = Math.hypot(hhdx, hhdy);
    if (hhD > 1e-6) { nx = hhdx / hhD; ny = hhdy / hhD; }

    const vDotN = ball.vx * nx + ball.vy * ny;
    if (vDotN < 0) {
      // Reflection (м'яч летить у поле)
      const reflVx = ball.vx - 2 * vDotN * nx;
      const reflVy = ball.vy - 2 * vDotN * ny;
      ball.vx = reflVx + paddleVx * FF_K_DRAG + nx * FF_REFLECT_PUSH;
      ball.vy = reflVy + paddleVy * FF_K_DRAG + ny * FF_REFLECT_PUSH;
    } else {
      // М'яч зайшов у поле, але летить паралельно/назовні (рідко, наприклад поле розширилось)
      // Даємо потужний push назовні
      ball.vx += nx * FF_REFLECT_PUSH * 1.5 + paddleVx * FF_K_DRAG;
      ball.vy += ny * FF_REFLECT_PUSH * 1.5 + paddleVy * FF_K_DRAG;
    }

    // Позиціонуємо на межі поля
    ball.x = hitX + nx * 2;
    ball.y = hitY + ny * 2;
    ball[_hitKey] = true;
    // ── AAA EVENT: FF reflect (discrete first-contact reflection) ──
    if (room) {
      io.to(room.id).emit('event:ball', {
        type: 'field_reflect',
        tick: gs.tick,
        ballId: ball.id,
        slot: s,
        pre:  { x: preX, y: preY, vx: preVx, vy: preVy },
        post: { x: ball.x, y: ball.y, vx: ball.vx, vy: ball.vy },
      });
    }
  } else {
    // ── CONTINUOUS PULSE: м'яч у полі — щотіку отримує імпульс назовні ──
    // Глибина проникнення (0 на межі, 1 у центрі)
    const penetration = Math.max(0, Math.min(1, 1 - (dist / collideR)));
    // Квадратична крива: швидкий приріст ближче до центру
    // penetration=0.5 → pen²=0.25, але через (1 + pen²·(mult-1)) сила зростає різко
    const penSq = penetration * penetration;
    // Сила: базова + підсилена в центрі
    const forceMag = FF_PULSE_FORCE * (1 + penSq * (FF_PULSE_CENTER_MULT - 1));
    ball.vx += nx * forceMag;
    ball.vy += ny * forceMag;
  }

  // ── Коли м'яч вийшов — скидаємо прапор "вже був ударений" ──
  // (щоб наступний вхід знову дав reflection, якщо поле ще активне)
  // Також: emit event щоб клієнт міг синхронізуватись після continuous pulse drift
  if (dist > collideR + 2) {
    if (ball[_hitKey] && room) {
      // Щойно вийшов з зони collision — clean state для клієнта
      io.to(room.id).emit('event:ball', {
        type: 'field_exit',
        tick: gs.tick,
        ballId: ball.id,
        slot: s,
        post: { x: ball.x, y: ball.y, vx: ball.vx, vy: ball.vy },
      });
    }
    ball[_hitKey] = false;
  }

  // ── Clamp швидкості ──
  let sp = Math.hypot(ball.vx, ball.vy);
  if (sp < FF_MIN_SP && sp > 0.01) {
    const k = FF_MIN_SP / sp; ball.vx *= k; ball.vy *= k; sp = FF_MIN_SP;
  }
  if (sp > SMAX) {
    const k = SMAX / sp; ball.vx *= k; ball.vy *= k;
  }

  ball.x  = Math.round(ball.x  * 10) / 10;
  ball.y  = Math.round(ball.y  * 10) / 10;
  ball.vx = Math.round(ball.vx * 10) / 10;
  ball.vy = Math.round(ball.vy * 10) / 10;

  return true;
}

// ── MAGNETIC FIELD (blockaway magnet) ──
// Напівкругле поле перед ракеткою, тягне і утримує м'ячі.
// Витрата: з 1.0 до 0 за 5000мс → 0.0002 / мс
// Відновлення: з 0 до 1.0 за 10000мс → 0.0001 / мс
const MAG_R            = 75;    // радіус магніту (px)
const MAG_PULL_FORCE   = 0.35;  // сила притягання до центру ракетки (per tick)
const MAG_HOLD_DAMPING = 0.55;  // демпфування швидкості м'яча коли утримується (0..1)
const MAG_DRAIN_PER_MS = 1/5000;    // 0.0002 — витрата енергії при утриманні
const MAG_REGEN_PER_MS = 1/10000;   // 0.0001 — регенерація коли неактивний
const MAG_RELEASE_PUSH = 3.5;   // швидкість вильоту при відпусканні (буде clamped до SMAX)
const MAG_MIN_ENERGY   = 0.10;  // мінімум енергії для активації (щоб не "чіркати"). Нижче — повна блокування.

// Перевірка: чи м'яч у півколі магніту перед ракеткою
function isBallInMagnet(gs, s, ball) {
  const p = slotToPaddle(s, gs.paddles[s], gs, null);
  const fcx = p.x + p.w/2, fcy = p.y + p.h/2;
  const dx = ball.x - fcx, dy = ball.y - fcy;
  const dist = Math.hypot(dx, dy);
  if (dist > MAG_R + BR) return null;
  // Перевіряємо півколо — тільки "перед" ракеткою (у напрямку face normal)
  const view = SLOT_VIEW[s];
  let faceNy = 0, faceNx = 0;
  if (view === 'bottom')     { faceNx = 0; faceNy = -1; }
  else if (view === 'top')   { faceNx = 0; faceNy =  1; }
  else if (view === 'left')  { faceNx = 1; faceNy =  0; }
  else                       { faceNx =-1; faceNy =  0; }
  // Dot product: якщо м'яч з "лицевого" боку паддла
  const dot = dx * faceNx + dy * faceNy;
  if (dot < 0) return null; // м'яч позаду ракетки — не тягнемо
  return { p, fcx, fcy, dist, dx, dy, faceNx, faceNy };
}

// ── Магніт — offset-based holding ──
// Концепція: при першому захопленні зберігаємо offset м'яча відносно центру ракетки.
// Далі м'яч ЖОРСТКО прив'язаний до паддла — рухається разом з ним без фізики.
// При release використовуємо збережений offset для визначення кута вильоту.
const MAG_CAPTURE_RADIUS = 55;  // м'яч захоплюється коли в цьому радіусі
const MAG_ENTRY_DAMPING  = 0.6; // як швидко м'яч "осаджується" у offset після захоплення
function applyMagnetBall(gs, s, ball, room) {
  if (!gs.magnet[s]) return false;

  const p = slotToPaddle(s, gs.paddles[s], gs, null);
  const fcx = p.x + p.w/2, fcy = p.y + p.h/2;
  const view = SLOT_VIEW[s];
  // Face normal
  let nx = 0, ny = 0;
  if (view === 'bottom')     { nx = 0; ny = -1; }
  else if (view === 'top')   { nx = 0; ny =  1; }
  else if (view === 'left')  { nx = 1; ny =  0; }
  else                       { nx =-1; ny =  0; }

  const dx = ball.x - fcx, dy = ball.y - fcy;
  const dist = Math.hypot(dx, dy);

  // Поза зоною дії?
  if (dist > MAG_R + BR) {
    // Якщо м'яч уже був захоплений і випав з зони — випускаємо його через release
    // (щоб м'яч не зависав з vx=vy=0 коли magnet його "випустив")
    if (ball['mag_held_' + s]) {
      console.log(`[MAG-LOSE] slot=${s} ball=${String(ball.id).slice(-4)} dist=${dist.toFixed(0)} (>83) pad=${Math.round(gs.paddles[s])}`);
      releaseMagnetBall(gs, s, ball, 0, room);
    }
    delete ball['_magInZone_' + s];
    return false;
  }

  // Перевірка півкола (тільки перед ракеткою)
  const frontDot = dx * nx + dy * ny;
  if (frontDot < 0) {
    // М'яч позаду — НЕ чіпаємо його швидкість взагалі.
    // Якщо ВЖЕ був схоплений, але потрапив позаду — release (щоб не зависнув)
    if (ball['mag_held_' + s]) {
      console.log(`[MAG-BEHIND] slot=${s} ball=${String(ball.id).slice(-4)} ball=(${ball.x.toFixed(0)},${ball.y.toFixed(0)}) pad=${Math.round(gs.paddles[s])} fc=(${fcx.toFixed(0)},${fcy.toFixed(0)}) frontDot=${frontDot.toFixed(1)}`);
      releaseMagnetBall(gs, s, ball, 0, room);
    }
    return false;
  }

  const offKey_X = 'mag_offX_' + s;
  const offKey_Y = 'mag_offY_' + s;
  const heldKey  = 'mag_held_' + s;

  if (!ball[heldKey]) {
    // ── НЕ ЗАХОПЛЕНИЙ: тягнемо м'яч досередини і захоплюємо коли близько ──
    const tgtX = fcx + nx * MAG_R * 0.40;
    const tgtY = fcy + ny * MAG_R * 0.40;
    const toX = tgtX - ball.x, toY = tgtY - ball.y;
    const toD = Math.hypot(toX, toY);

    if (toD < MAG_CAPTURE_RADIUS) {
      // ── PRE-EVENT STATE ──
      const preX = ball.x, preY = ball.y, preVx = ball.vx, preVy = ball.vy;
      // Захоплюємо
      ball[heldKey] = true;
      ball[offKey_X] = ball.x - fcx;
      ball[offKey_Y] = ball.y - fcy;
      ball.vx = 0;
      ball.vy = 0;
      delete ball['_magInZone_' + s];
      console.log(`[MAG-CAPTURE] slot=${s} ball=${String(ball.id).slice(-4)} off=(${ball[offKey_X].toFixed(1)},${ball[offKey_Y].toFixed(1)}) pad=${Math.round(gs.paddles[s])} fc=(${fcx.toFixed(0)},${fcy.toFixed(0)})`);
      // ── AAA EVENT: magnet capture ──
      if (room) {
        io.to(room.id).emit('event:ball', {
          type: 'magnet_capture',
          tick: gs.tick,
          ballId: ball.id,
          slot: s,
          pre:  { x: preX, y: preY, vx: preVx, vy: preVy },
          post: { x: ball.x, y: ball.y, vx: 0, vy: 0 },
          offX: ball[offKey_X], offY: ball[offKey_Y],
        });
      }
    } else {
      // Тягнемо — тільки додаємо імпульс, БЕЗ damping.
      // Damping збивав швидкість коли м'яч летів повз край поля.
      if (toD > 0.1) {
        const tnx = toX / toD, tny = toY / toD;
        const pull = MAG_PULL_FORCE * 2.0;
        ball.vx += tnx * pull;
        ball.vy += tny * pull;
      }
      // Логуємо м'ячі що проводять багато тіків у зоні без захоплення (можуть "відскочити" назовні)
      ball['_magInZone_' + s] = (ball['_magInZone_' + s] || 0) + 1;
      if (ball['_magInZone_' + s] === 10 || ball['_magInZone_' + s] === 20) {
        console.log(`[MAG-STUCK] slot=${s} ball=${String(ball.id).slice(-4)} ticks=${ball['_magInZone_' + s]} toD=${toD.toFixed(0)} v=(${ball.vx.toFixed(1)},${ball.vy.toFixed(1)}) dist=${dist.toFixed(0)}`);
      }
    }
    return true;
  }

  // ── ЗАХОПЛЕНИЙ: жорстка прив'язка до ракетки ──
  const offX = ball[offKey_X] || 0;
  const offY = ball[offKey_Y] || 0;
  // Safety clamp: offset не повинен перевищувати MAG_R (щоб м'яч не виходив з зони при русі paddle)
  const offDist = Math.hypot(offX, offY);
  if (offDist > MAG_R * 0.85) {
    // Нормалізуємо до 85% від радіуса — лишаємо запас щоб не випадав з зони
    const scale = (MAG_R * 0.85) / offDist;
    ball[offKey_X] = offX * scale;
    ball[offKey_Y] = offY * scale;
  }
  const finalOffX = ball[offKey_X] || 0;
  const finalOffY = ball[offKey_Y] || 0;
  const newX = fcx + finalOffX;
  const newY = fcy + finalOffY;
  // Debug: якщо м'яч різко перестрибнув (ознака розсинхрону)
  const moveDelta = Math.hypot(newX - ball.x, newY - ball.y);
  if (moveDelta > 15) {
    console.log(`[MAG-HOLD-JUMP] slot=${s} ball=${String(ball.id).slice(-4)} delta=${moveDelta.toFixed(0)} old=(${ball.x.toFixed(0)},${ball.y.toFixed(0)}) new=(${newX.toFixed(0)},${newY.toFixed(0)}) pad=${Math.round(gs.paddles[s])} tick=${gs.tick}`);
  }
  ball.vx = newX - ball.x;
  ball.vy = newY - ball.y;
  ball.x = newX;
  ball.y = newY;

  return true;
}

// Release: м'яч вилетів з магніту. Напрямок залежить від збереженого offset'а вздовж ракетки.
function releaseMagnetBall(gs, s, ball, releaseIndex, room) {
  if (!ball['mag_held_' + s]) return;
  // ── PRE-EVENT STATE ──
  const preX = ball.x, preY = ball.y, preVx = ball.vx, preVy = ball.vy;
  // Використовуємо ЗБЕРЕЖЕНИЙ offset — не поточну позицію (вона могла оновитись)
  const offX = ball['mag_offX_' + s] || 0;
  const offY = ball['mag_offY_' + s] || 0;
  // Очищаємо прапори
  delete ball['mag_held_' + s];
  delete ball['mag_offX_' + s];
  delete ball['mag_offY_' + s];

  const p = slotToPaddle(s, gs.paddles[s], gs, null);
  const fcx = p.x + p.w/2, fcy = p.y + p.h/2;
  const view = SLOT_VIEW[s];
  // Face normal (куди «дивиться» ракетка) + tangent (вісь ракетки)
  let nx = 0, ny = 0, tx = 0, ty = 0;
  if (view === 'bottom')     { nx = 0; ny = -1; tx = 1; ty = 0; }
  else if (view === 'top')   { nx = 0; ny =  1; tx = 1; ty = 0; }
  else if (view === 'left')  { nx = 1; ny =  0; tx = 0; ty = 1; }
  else                       { nx =-1; ny =  0; tx = 0; ty = 1; }
  // Offset вздовж тангенти — визначає кут вильоту
  const tangentOffset = offX * tx + offY * ty;
  const half = p.w/2 * (tx !== 0 ? 1 : 0) + p.h/2 * (ty !== 0 ? 1 : 0);
  let k = tangentOffset / Math.max(half, 1);
  if (k > 1) k = 1; else if (k < -1) k = -1;
  // Якщо декілька м'ячів release одночасно — розводимо по куту
  // Кожний наступний отримує зсув кута щоб не накладалися на release position
  const indexOffset = (releaseIndex||0) * 0.20; // ~11° зсув за кожний м'яч
  // Направлення зсуву — у сторону де більше місця (поза clamp-ом)
  k = k + (k >= 0 ? -indexOffset : indexOffset);
  if (k > 1) k = 1; else if (k < -1) k = -1;
  // Вихідний вектор: переважно по нормалі, з кутом до 60° в бік зсуву
  const MAX_ANGLE = Math.PI / 3;
  const angle = k * MAX_ANGLE;
  const cosA = Math.cos(angle), sinA = Math.sin(angle);
  const outX = nx * cosA + tx * sinA;
  const outY = ny * cosA + ty * sinA;
  const targetSp = Math.min(MAG_RELEASE_PUSH, SMAX);
  ball.vx = outX * targetSp;
  ball.vy = outY * targetSp;
  // Виштовхуємо м'яч за межу магніту + додаємо distance для різних м'ячів щоб точно не перекривались
  const pushDist = MAG_R + BR + 2 + (releaseIndex||0) * BR;
  ball.x = fcx + outX * pushDist;
  ball.y = fcy + outY * pushDist;
  ball.x  = Math.round(ball.x  * 10) / 10;
  ball.y  = Math.round(ball.y  * 10) / 10;
  ball.vx = Math.round(ball.vx * 10) / 10;
  ball.vy = Math.round(ball.vy * 10) / 10;
  console.log(`[MAG-RELEASE] slot=${s} ball=${String(ball.id).slice(-4)} idx=${releaseIndex||0} pos=(${ball.x.toFixed(0)},${ball.y.toFixed(0)}) v=(${ball.vx.toFixed(1)},${ball.vy.toFixed(1)}) k=${k.toFixed(2)} angle=${(angle*180/Math.PI).toFixed(0)}deg pad=${Math.round(gs.paddles[s])}`);
  // ── AAA EVENT: magnet release ──
  if (room) {
    io.to(room.id).emit('event:ball', {
      type: 'magnet_release',
      tick: gs.tick,
      ballId: ball.id,
      slot: s,
      pre:  { x: preX, y: preY, vx: preVx, vy: preVy },
      post: { x: ball.x, y: ball.y, vx: ball.vx, vy: ball.vy },
    });
  }
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

function findOrCreateRoom(protocol = 1) {
  for (const [, r] of rooms) {
    if ((r.protocol || 1) === protocol && (r.status === 'waiting' || r.status === 'countdown') && Object.keys(r.players).length < 4) return r;
  }
  const id = 'r_' + Math.random().toString(36).slice(2, 8);
  const room = createRoom(id);
  room.protocol = protocol;
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
    ? Math.round(realPlayers.reduce((s, p) => s + (p.rating ?? 500), 0) / realPlayers.length)
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
    // ── Магнітне поле (другий skill на кнопку блискавки) ──
    // Напівкругле поле перед ракеткою, що притягує й утримує м'ячі. Працює поки натиснуто.
    // Окрема шкала енергії, що витрачається при утриманні.
    magEnergy: { 0: 1, 1: 1, 2: 1, 3: 1 },          // 0..1
    magnet:    { 0:false, 1:false, 2:false, 3:false }, // активний зараз?
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
  // ── Event trigger для client-side animation (V2 Electric Arc) ──
  // Broadcast'иться один раз з force flag, потім очищається в tick
  if (!gs._elimEvents) gs._elimEvents = [];
  gs._elimEvents.push(slot);
  gs._forceNextBroadcast = true;
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
  // ── SNAPSHOT RATE: 30Hz (every 2nd physics tick) ──
  // Physics ticks 60Hz для точної фізики, broadcasts 30Hz для економії трафіку.
  // Клієнт інтерполює між snapshots і робить deterministic ball simulation
  // на 60Hz, тому плавність не страждає, а трафік -50%.
  // Винятки: коли gameOver або important events — broadcast одразу.
  const sendBalls = true; // Lockstep: надсилаємо стан кожен тік
  // shouldBroadcast визначається пізніше при виклику broadcastState
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
        if (gs.fields[s].t >= FDR * fd) {
          gs.fields[s].active = false;
          gs.fields[s].t = 0;
          gs.fields[s].r = 0;
          // Force broadcast — клієнт має одразу дізнатись що поле вимкнулось
          gs._forceNextBroadcast = true;
          // Очистити hit-прапори: наступна активація зможе дати reflection при вході
          const _hk = 'ff_hit_' + s;
          for (const b of gs.balls) { delete b[_hk]; }
          // ── AAA EVENT: FF деактивовано ──
          io.to(room.id).emit('event:field', {
            type: 'deactivate',
            tick: gs.tick,
            slot: s,
          });
        }
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
          // Force broadcast — активація поля має одразу дійти до всіх
          gs._forceNextBroadcast = true;
          // ── AAA EVENT: повідомляємо клієнтів про активацію FF з точним tick'ом ──
          io.to(room.id).emit('event:field', {
            type: 'activate',
            tick: gs.tick,
            slot: s,
            paddleX: gs.paddles[s],
            maxR: gs.fields[s].maxR,
          });

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

      // ── MAGNET HANDLING ──
      // Магніт активний, поки інп.magnet=true І є енергія.
      // Гістерезис: щоб УВІМКНУТИ — потрібно >= MAG_MIN_ENERGY.
      // Після увімкнення працює поки енергія > 0 (щоб не миготіло на 10%).
      const wasMagnet = gs.magnet[s];
      const wantMagnet = !!inp.magnet;
      let nowMagnet = false;
      if (wantMagnet) {
        if (wasMagnet) {
          // Уже активний — працює поки є енергія
          nowMagnet = gs.magEnergy[s] > 0;
        } else {
          // Перехід OFF→ON: потрібен поріг
          nowMagnet = gs.magEnergy[s] >= MAG_MIN_ENERGY;
        }
      }
      gs.magnet[s] = nowMagnet;
      // Force broadcast при зміні magnet state (capture/release має дійти миттєво)
      if (wasMagnet !== nowMagnet) {
        gs._forceNextBroadcast = true;
      }
      if (nowMagnet) {
        gs.magEnergy[s] = Math.max(0, gs.magEnergy[s] - MAG_DRAIN_PER_MS * TICK_MS);
      }
      // Release: був активний → став неактивним
      if (wasMagnet && !gs.magnet[s]) {
        let relIdx = 0;
        for (const b of gs.balls) {
          if (b['mag_held_' + s]) {
            releaseMagnetBall(gs, s, b, relIdx, room);
            relIdx++;
          }
        }
      }
      // Регенерація — тільки коли неактивний
      if (!gs.magnet[s]) {
        gs.magEnergy[s] = Math.min(1, gs.magEnergy[s] + MAG_REGEN_PER_MS * TICK_MS);
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
            // ── AAA EVENT: бот активував FF ──
            io.to(room.id).emit('event:field', {
              type: 'activate',
              tick: gs.tick,
              slot: s,
              paddleX: gs.paddles[s],
              maxR: botFR,
            });
          }
        }
      }
    }

    for (let i = gs.respawns.length - 1; i >= 0; i--) {
      gs.respawns[i].timer -= TICK_MS;
      if (gs.respawns[i].timer <= 0) {
        const newBall = { x: W/2, y: H/2, vx: gs.respawns[i].vx, vy: gs.respawns[i].vy, id: Date.now()+i };
        gs.balls.push(newBall);
        gs.respawns.splice(i, 1);
        // Новий м'яч на полі — клієнт має одразу дізнатись
        gs._forceNextBroadcast = true;
        // ── AAA EVENT: новий м'яч ─── клієнт додає його у свою симуляцію
        io.to(room.id).emit('event:ball', {
          type: 'spawn',
          tick: gs.tick,
          ballId: newBall.id,
          post: { x: newBall.x, y: newBall.y, vx: newBall.vx, vy: newBall.vy },
        });
      }
    }

    const goal = (slot) => {
      if (gs.eliminated[slot]) return false;
      gs.scores[slot]++; gs.lives[slot]--;
      // ── Force broadcast: гол — критична подія, не чекаємо 30Hz tick ──
      gs._forceNextBroadcast = true;
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
      // Якщо м'яч зараз захоплений магнітом — НЕ рухаємо його фізикою.
      // Magnet ставить ball.x/y жорстко на paddle+offset, а рух по інерції тут
      // створював би осцилляцію (v накопичується неправильно між тіками).
      const isHeldAny = ball['mag_held_0']||ball['mag_held_1']||ball['mag_held_2']||ball['mag_held_3'];
      if (!isHeldAny) {
        ball.x += ball.vx; ball.y += ball.vy;
      }
    // Округлення до 3 знаків — зменшує float drift між клієнтом і сервером
    ball.x=Math.round(ball.x*1000)/1000;
    ball.y=Math.round(ball.y*1000)/1000;
    ball.vx=Math.round(ball.vx*1000)/1000;
    ball.vy=Math.round(ball.vy*1000)/1000;
      // Continuous force field: викликаємо для ВСІХ полів, а не зупиняємось на першому
      // Для magnet-held м'ячів пропускаємо FF (magnet все одно перезапише позицію).
      if (!isHeldAny) {
        for (const s of SLOTS) {
          if (gs.eliminated[s]) continue;
          applyFFBall(gs, s, ball, room);
        }
      }
      // Magnetic field: притягання/утримання м'ячів
      for (const s of SLOTS) {
        if (gs.eliminated[s]) continue;
        applyMagnetBall(gs, s, ball, room);
      }
      resolveChamfersBall(ball);
      clampBallObj(ball, room, gs);
      // Paddle collisions — пропускаємо для magnet-held (м'яч вже фіксований на paddle)
      let hit = false;
      if (!isHeldAny) {
      for (const s of SLOTS) {
        if (gs.eliminated[s]) continue;
        const p = slotToPaddle(s, gs.paddles[s], gs, room);
        if (hitRect(ball, p)) {
          // ── PRE-EVENT STATE для клієнтської reconciliation ──
          const preX = ball.x, preY = ball.y, preVx = ball.vx, preVy = ball.vy;
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
          // ── AAA EVENT для клієнтської reconciliation ──
          // Клієнт використовує pre/post щоб точно відтворити подію
          // в своїй симуляції. tick = коли подія сталася на сервері.
          io.to(room.id).emit('event:ball', {
            type: 'paddle_hit',
            tick: gs.tick,
            ballId: ball.id,
            slot: s,
            speed: Math.round(speed*10)/10,
            pre:  { x: preX, y: preY, vx: preVx, vy: preVy },
            post: { x: ball.x, y: ball.y, vx: ball.vx, vy: ball.vy },
          });
          // Backward-compat: legacy paddle:hit event (для звуку — клієнт вже має listener)
          io.to(room.id).emit('paddle:hit', { slot: s, speed: Math.round(speed*10)/10 });
          hit = true; break;
        }
      }
      if (hit) continue;
      const by = ball.y, bx2 = ball.x;
      // ── ELIMINATION WALL: ворота вибулого гравця стали FF-стіною ──
      // Замість простого vy=abs(vy) — застосовуємо boost + random spread,
      // щоб гра з вибуваючими гравцями ставала всё більш chaotic та цікавою
      if (by-BR < 0 && bx2 > C && bx2 < W-C) {
        if (gs.eliminated[1]) {
          // Спершу clamp position — м'яч не повинен пройти за лінію
          ball.y = BR;
          applyEliminatedWall(gs, 1, ball, room);
        }
        else {
          io.to(room.id).emit('event:ball', { type: 'remove', tick: gs.tick, ballId: ball.id, scoredSlot: 1 });
          if (!goal(1)) spawnBallQueued(gs); gs.balls.splice(bi,1);
        }
      } else if (by+BR > H && bx2 > C && bx2 < W-C) {
        if (gs.eliminated[0]) {
          ball.y = H - BR;
          applyEliminatedWall(gs, 0, ball, room);
        }
        else {
          io.to(room.id).emit('event:ball', { type: 'remove', tick: gs.tick, ballId: ball.id, scoredSlot: 0 });
          if (!goal(0)) spawnBallQueued(gs); gs.balls.splice(bi,1);
        }
      } else if (bx2-BR < 0 && by > C && by < H-C) {
        if (gs.eliminated[2]) {
          ball.x = BR;
          applyEliminatedWall(gs, 2, ball, room);
        }
        else {
          io.to(room.id).emit('event:ball', { type: 'remove', tick: gs.tick, ballId: ball.id, scoredSlot: 2 });
          if (!goal(2)) spawnBallQueued(gs); gs.balls.splice(bi,1);
        }
      } else if (bx2+BR > W && by > C && by < H-C) {
        if (gs.eliminated[3]) {
          ball.x = W - BR;
          applyEliminatedWall(gs, 3, ball, room);
        }
        else {
          io.to(room.id).emit('event:ball', { type: 'remove', tick: gs.tick, ballId: ball.id, scoredSlot: 3 });
          if (!goal(3)) spawnBallQueued(gs); gs.balls.splice(bi,1);
        }
      }
      } // end if (!isHeldAny) для paddle/goal блоків
      if (gs.gameOver) { broadcastState(room); return; }
    }
    // ── BROADCAST 30Hz (every 2nd tick) + force broadcast при важливих подіях ──
    // Force broadcast: коли голу щойно забили, поле активувалось/деактивувалось,
    // магніт capture/release, або кулька народилась/зникла.
    const _shouldForce = gs._forceNextBroadcast === true;
    if (gs.tick % 2 === 0 || _shouldForce) {
      broadcastState(room, sendBalls);
      gs._forceNextBroadcast = false;
      // Очищуємо elim events після broadcast — щоб не дублювались
      if (gs._elimEvents && gs._elimEvents.length) gs._elimEvents = [];
    }

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
  // Broadcast id — окремий лічильник (тільки broadcasts) для клієнтського
  // packet loss detection. seq=tick біжить 60Hz (включаючи пропущені), а bid
  // інкрементиться рівно ОДИН раз на кожен реальний broadcast (~30Hz нормально,
  // частіше при force broadcast). Клієнт використовує bid для loss%.
  gs._broadcastId = (gs._broadcastId || 0) + 1;
  io.to(room.id).emit('gs', {
    seq: gs.tick,
    bid: gs._broadcastId, // broadcast index для loss detection
    st: Date.now(), // server time для clock sync на клієнті
    balls: gs.balls.map(b=>{
      const out={x:Math.round(b.x*10)/10,y:Math.round(b.y*10)/10,vx:Math.round(b.vx*100)/100,vy:Math.round(b.vy*100)/100,id:b.id};
      // Magnet flags: для якого slot м'яч схоплений і offset
      for(let s=0;s<4;s++){
        if(b['mag_held_'+s]){
          out.mh=s; // magnet-held slot (0..3)
          out.mox=Math.round((b['mag_offX_'+s]||0)*10)/10;
          out.moy=Math.round((b['mag_offY_'+s]||0)*10)/10;
          break;
        }
      }
      return out;
    }),
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
    // ── Elimination events (трігер анімації на клієнті, очищається після broadcast) ──
    elev: gs._elimEvents && gs._elimEvents.length ? [...gs._elimEvents] : undefined,
    lv: [gs.lives[0],gs.lives[1],gs.lives[2],gs.lives[3]],
    sc: [gs.scores[0],gs.scores[1],gs.scores[2],gs.scores[3]],
    // Magnet state + енергія (для HUD та візуалу півкола)
    mg: [gs.magnet[0]?1:0, gs.magnet[1]?1:0, gs.magnet[2]?1:0, gs.magnet[3]?1:0],
    me: [Math.round(gs.magEnergy[0]*100),Math.round(gs.magEnergy[1]*100),Math.round(gs.magEnergy[2]*100),Math.round(gs.magEnergy[3]*100)],
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

function getTrainingRewards(winnerSlot, mySlot, places) {
  const place = places.indexOf(mySlot);
  return { xp: [150,75,30,0][place]||0, silver: [1500,700,200,0][place]||0, place };
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
    participants.push({ uid: player.uid, slot: player.slot, rating: player.rating ?? 500 });
    claimedSlots.add(player.slot);
  }
  if (room._disconnected) {
    for (const [slotStr, info] of Object.entries(room._disconnected)) {
      const slot = parseInt(slotStr);
      if (claimedSlots.has(slot)) continue; // активний гравець вже взятий
      if (!info || !info.uid) continue;
      participants.push({ uid: info.uid, slot, rating: info.rating ?? 500 });
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
      const place = places.indexOf(s);
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
      if (placeIdx === 0) pubUpd.wins = admin.firestore.FieldValue.increment(1);
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
  // NOTE: щоденна нагорода більше не потребує умов — просто увійти в гру.
  // Прогрес фіксується при кожному відкритті daily quest screen (через `daily:get`).
}

function endGame(room, winnerSlot) {
  if(room.managed){void matchService.finish(room);return;}
  if (room.status === 'finished') return;
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
    if(realPlayer) trainingRewards = getTrainingRewards(winnerSlot, realPlayer.slot, places);
  }

  // Записуємо нагороди на сервері — клієнт більше не пише рейтинг/XP/срібло після матчу
  commitRewards(room, winnerSlot, places, ratingDeltas, isTraining, trainingRewards);
  // Аналітика
  trackGameEnd(room, winnerSlot, places, isTraining, ratingDeltas);
  footballDashboard?.match(room, isTraining);

  io.to(room.id).emit('game:over', {
    roomId:room.id, scores:SLOTS.map(s=>gs.scores[s]), lives:SLOTS.map(s=>gs.lives[s]),
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
  // Ranked timeout restores the original bots; team and private duels stay human-only.
  const isTrainingRoom = room.mode==='training'||Object.values(room.players).some(p => p.trainingMode);
  if (isTrainingRoom) {
    fillBots(room, false);
  } else if(room.mode==='ranked'||!room.managed) {
    // Ranked — заповнюємо порожні слоти ranked-ботами
    fillBots(room, true);
  }
  room.game = createGameState(room);
  if (room.protocol === 2) netcode2.prepare(room, {io, getPlayer:getSlotPlayer, endGame, onAbort:id=>{if(room.managed)void matchService.abort(room,'simulation_unavailable');else rooms.delete(id);}});
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
  room.paddleVisuals=paddleVisuals;
  io.to(room.id).emit('game:start', { players: buildPlayers(room), mySlot: null, roomId: room.id, paddleVisuals, matchInfo:room.managed?matchService.info(room):null, netVersion: room.net2 ? netcode2.VERSION : null });
  for (const [sid, player] of Object.entries(room.players)) {
    io.to(sid).emit('myslot', { mySlot: player.slot, roomId: room.id });
    if (room.net2) netcode2.init(room, io.sockets.sockets.get(sid), player.slot);
  }
  room.tickInterval = room.net2 ? null : setInterval(() => tick(room), TICK_MS);
  room.startedAt = Date.now(); // для підрахунку тривалості матчу
  for(const uid of new Set(Object.values(room.players).map(player=>player.uid).filter(Boolean)))
    void pushService?.recordMatch(uid).catch(()=>{});

  // ── Аналітика: подія старту матчу (для drop-off rate) ──
  try {
    const botsCount = Object.keys(room.bots || {}).filter(s => room.bots[s]).length;
    const realPlayersCount = Object.keys(room.players).length;
    const isTrainingRoom = Object.values(room.players).some(p => p.trainingMode);
    const mode = room.mode || (isTrainingRoom ? 'training' : (botsCount > 0 ? 'ranked_bots' : 'ranked'));
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
  room.matchTimerInterval = room.net2 ? null : setInterval(() => {
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
        const ML_SRV = 15;
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
  socket.on('net:diagnostics', enabled => {socket.data.netDiagnostics=enabled===true;});
  require('./economy-service').attach(socket,db);
  let myRoom = null, mySlot = null;

  socket.on('paddle:stats',({slot,paddleStats})=>{
    // ── SECURITY: ігноруємо. Сервер рахує paddleStats сам з БД при mm:join.
    // Цей handler залишено для сумісності зі старими клієнтами, але він nop.
  });

  // Реєструємо shop handlers для цього сокета
  registerShopHandlers(socket);
  registerBillingHandlers(socket, () => playBilling);

  // Аутентифікація для shop без mm:join (з меню/магазину)
  // SECURITY: верифікуємо idToken і використовуємо тільки verified UID.
  // Якщо Admin SDK недоступний — fallback на клієнтський uid (dev режим).
  let shopAuthGeneration=0;
  let welcomeSession=null;
  const welcomeConnection=welcomeGateway.attach(socket,{getSession:()=>welcomeSession});
  socket.on('disconnect',()=>{welcomeSession=null;});
  socket.on('shop:auth', async (value, ack) => {
    const generation=++shopAuthGeneration;
    welcomeSession=null;
    let identity=null;
    const resolvedUid = await resolveUid({idToken:value?.idToken,fallbackUid:value?.uid,
      onVerified:verified=>{identity=verified;}});
    if(!socket.connected||generation!==shopAuthGeneration)return;
    if (resolvedUid) {
      socket.uid = resolvedUid;
      // Welcome privileges come only from this verified shop login. A UID set by
      // matchmaking or a legacy dev fallback must never authorize a gold gift.
      if(value?.welcomeVersion===1&&identity?.uid===resolvedUid&&!identity.anonymous&&identity.expiresAt>Date.now())
        welcomeSession={uid:resolvedUid,generation,expiresAt:identity.expiresAt,verified:true,anonymous:false};
      footballDashboard?.presence(resolvedUid);
      if(identity?.uid===resolvedUid&&!identity.anonymous)void pushService?.markAuthenticated(resolvedUid).catch(()=>{});
      const activity=playerActivity?.authenticate(socket,resolvedUid,{anonymous:!identity||identity.anonymous})||false;
      if(typeof ack==='function')ack({ok:true,activityVersion:activity?1:null,welcomeVersion:welcomeSession&&welcomeEnabled?1:null});
      if(welcomeEnabled)welcomeConnection.authenticated();
    } else {
      playerActivity?.authenticate(socket,null);
      socket.uid = undefined;
      socket.emit('shop:error', { msg: 'auth_required' });
      if(typeof ack==='function')ack({ok:false,error:'auth_required'});
    }
  });

  socket.on('tutorial:progress', async (value, ack) => {
    const generation=shopAuthGeneration;
    try {
      const uid=value?.idToken?await resolveUid({idToken:value.idToken}):welcomeSession?.uid;
      if(!socket.connected||generation!==shopAuthGeneration||(welcomeSession&&welcomeSession.uid!==uid))throw Error('auth_changed');
      const result=await require('./flight-school').saveFlightSchool(db,uid,value);
      if(!socket.connected||generation!==shopAuthGeneration||(welcomeSession&&welcomeSession.uid!==uid))throw Error('auth_changed');
      if(typeof ack==='function')ack({ok:true,...result});
      if(result.progress.completed&&welcomeEnabled)welcomeConnection.afterTutorial(uid);
    } catch(e){if(socket.connected&&typeof ack==='function')ack({ok:false,error:e.message});}
  });

  // ── Обмін золото → срібло ──
  socket.on('shop:exchange', async ({ rateId }) => {
    if (!db) return socket.emit('shop:error', { operation:'exchange', msg: 'server_unavailable' });
    if (!socket.uid) return socket.emit('shop:error', { operation:'exchange', msg: 'auth_required' });
    const RATES = [
      { goldIn:1,   silverOut:100   },
      { goldIn:5,   silverOut:500   },
      { goldIn:10,  silverOut:1000  },
      { goldIn:50,  silverOut:5000  },
      { goldIn:100, silverOut:10000 },
    ];
    const rate = Number.isInteger(rateId) ? RATES[rateId] : null;
    if (!rate) return socket.emit('shop:error', { operation:'exchange', msg: 'invalid_rate' });

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
        return { gold: newGold, silver: newSilver, rateId, silverGained:rate.silverOut, goldSpent:rate.goldIn };
      });

      socket.emit('shop:exchanged', result);
      trackEvent('purchase', {
        uid: socket.uid, kind: 'exchange', item: `gold_to_silver_${rate.goldIn}`,
        amount: rate.goldIn, cur: 'gold',
      });
    } catch(e) {
      if (e && e.code) { socket.emit('shop:error', { operation:'exchange', msg: e.code }); return; }
      console.error('shop:exchange', e.message);
      socket.emit('shop:error', { operation:'exchange', msg: 'server_error' });
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
          const newRating = Math.max(0, (pub.rating ?? 500) + delta);
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

  socket.on('mm:join', async ({ nick, rating, uid, idToken, wins, games, paddleStats, trainingMode, avatarId, platform, netVersion }) => {
    if (myRoom && myRoom.status === 'playing') return;
    if(!trainingMode&&(netVersion!==netcode2.VERSION||process.env.NETCODE_V2==='0')){
      socket.emit('mm:error','Оновіть Galaxy Football у Google Play до версії 20.18 або новішої.');return;
    }
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
    if (realUid) footballDashboard?.presence(realUid);
    if (realUid && !trainingMode) trackPlayerSeen(realUid, platform);

    // ── SECURITY: ігноруємо клієнтський paddleStats і rating ──
    // Завантажуємо з Firestore (server-authoritative).
    // Якщо db відсутній — використовуємо дефолтні значення.
    const serverStats = await loadValidatedPaddleStats(realUid);
    const serverRating = realUid ? await loadValidatedRating(realUid) : (rating|0) || 500;
    // Якщо клієнт встиг disconnect поки ми читали Firestore — нічого не робимо
    if (!socket.connected) return;

    if(trainingMode){
      // NOTE: training mode у нас НЕ використовує сервер для AI — все локально в браузері.
      // Це гілка для historic/legacy сумісності, не повинна спрацьовувати.
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
    const protocol = netVersion === netcode2.VERSION && process.env.NETCODE_V2 !== '0' ? 2 : 1;
    const room = findOrCreateRoom(protocol);
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

  socket.on('rejoin', async ({ roomId, slot, nick, rating, uid, idToken, paddleStats, netVersion }) => {
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
    if (room.net2 && (netVersion !== netcode2.VERSION || !savedData || savedData.leftVoluntarily || !realUid || savedData.uid !== realUid || room.game.eliminated[slot])) {
      socket.emit('rejoin:fail', {reason:'not_rejoinable'}); return;
    }
    if (savedData && savedData.uid && realUid && savedData.uid !== realUid) {
      console.log('[rejoin] UID mismatch: slot='+slot+' wants='+realUid+' saved='+savedData.uid);
      socket.emit('rejoin:fail', { reason: 'wrong_uid' });
      return;
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

    // Async account reads yield: another connection or a match finish may win the race.
    // Claim a slot only after rechecking; keep its disconnect deadline until then.
    if (rooms.get(roomId)!==room || !['playing','waiting','countdown'].includes(room.status) ||
        Object.values(room.players).some(p=>p.slot===slot) ||
        (room.net2 && (room._disconnected?.[slot]!==savedData || room.game.eliminated[slot]))) {
      socket.emit('rejoin:fail',{reason:'not_rejoinable'});return;
    }
    if (room._slotDeleteTimers?.[slot]) {
      clearTimeout(room._slotDeleteTimers[slot]);delete room._slotDeleteTimers[slot];
    }
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
        mySlot: slot, roomId: room.id, players: buildPlayers(room), netVersion: room.net2 ? netcode2.VERSION : null,
        gameOver: room.net2 ? false : gs.gameOver, winner: room.net2 ? null : gs.winner,
        lives: gs.lives, scores: gs.scores,
        paddleVisuals: rejoinPaddleVisuals,
      });
      socket.emit('myslot', { mySlot: slot });
      if (room.net2) { room.net2.authority.connect(slot); netcode2.init(room, socket, slot); }
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

  // ──────────────────────────────────────────────
  // ── NETCODE: Ping/Pong + Clock Sync ──────────
  // Клієнт шле ('np', clientTime). Сервер відповідає одразу
  // з його timestamp. Клієнт отримує RTT + може обчислити
  // server clock offset.
  // ──────────────────────────────────────────────
  // ──────────────────────────────────────────────
  // PING/PONG
  // Клієнт надсилає 'np' (network ping) кожні 2с.
  // Сервер повертає {c, s} — клієнтський timestamp + серверний.
  // Це дає клієнту RTT і clock offset.
  // ──────────────────────────────────────────────
  socket.on('np', (clientT) => {
    socket.lastPingT = Date.now(); // для серверного моніторингу
    socket.emit('np', { c: clientT, s: Date.now() });
  });

  socket.on('net:input', packet => netcode2.input(myRoom, socket, packet));
  socket.on('net:ping', c => {
    const now=performance.now();if(!Number.isFinite(c)||now-(socket._netPingAt||0)<250)return;
    socket._netPingAt=now;socket.emit('net:pong',{c,s:now});
  });
  socket.on('net:sync', () => netcode2.sync(myRoom, socket, mySlot));
  socket.on('net:resume', () => {
    if (!myRoom?.net2 || !myRoom.players[socket.id]) return;
    const now=Date.now();if(now-(socket._netResumeAt||0)<500)return;socket._netResumeAt=now;
    myRoom.net2.authority.connect(mySlot);netcode2.init(myRoom,socket,mySlot);
  });
  socket.on('net:pause', () => {
    const now=Date.now();if(now-(socket._netPauseAt||0)<250)return;socket._netPauseAt=now;
    if(myRoom?.net2 && myRoom.players[socket.id]) myRoom.net2.authority.disconnect(mySlot,false);
  });
  socket.on('input', ({ left, right, boost, magnet, hist, pos, boostPos, fieldPos }) => {
    if (myRoom?.net2) return;
    if (!myRoom || !myRoom.players[socket.id]) return;
    const player = myRoom.players[socket.id];
    const gs = myRoom.game;

    // ── Input buffering ──
    const anyBoost = hist && hist.length > 1 ? hist.some(h => h.boost) : false;
    const finalBoost = boost || anyBoost;
    // Якщо поточний pos відсутній але є в hist — беремо останній
    const effectivePos = pos !== undefined ? pos : (hist && hist.length > 0 ? hist[hist.length-1].pos : undefined);
    player.input = { left, right, boost: finalBoost, magnet: !!magnet, fieldPos };
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

      // Server-follows-client: беремо клієнтську позицію (prediction вже валідована клампами + швидкістю).
      // АЛЕ: коли магніт активний, обмежуємо максимальний delta за тік щоб м'яч не "стрибав" 
      // через network batching (коли кілька пакетів прийшли разом після паузи).
      if (gs.magnet && gs.magnet[slot] && diff > 8) {
        // Обмежуємо рух paddle до 3×швидкості за тік — достатньо швидко щоб догнати,
        // але не стрибком (бо m'яч жорстко прив'язаний до paddle при magnet)
        const maxDelta = (pStats.spd || PS) * 3;
        const dir = clampedPos > serverPos ? 1 : -1;
        gs.paddles[slot] = serverPos + dir * Math.min(diff, maxDelta);
      } else {
        gs.paddles[slot] = clampedPos;
      }
      if (diff > 80) {
        console.log(`Paddle jump slot${slot}: diff=${diff.toFixed(0)}px (accepted, mag=${!!gs.magnet[slot]})`);
      }

      // Якщо поле активне — використовуємо fieldPos для точної синхронізації відбиття
      const _inp = player.input;
      if (_inp && _inp.fieldPos !== undefined && gs.fields[slot] && gs.fields[slot].active) {
        const fpClamped = Math.max(mn, Math.min(mx, _inp.fieldPos));
        gs.paddles[slot] = fpClamped;
        _inp.fieldPos = undefined;
      }
    }
  });

  socket.on('mm:cancel', () => leave({ voluntary: true }));
  // Явний вихід з кнопки — одразу eliminates
  socket.on('leave_game', () => leave({ voluntary: true }));
  // Raw disconnect (transport close, ping timeout, закрив таб) — даємо 30с на rejoin
  socket.on('disconnect', (reason) => {
    // Логуємо для діагностики чому клієнт disconnect'нувся.
    // Можливі причини:
    //   "transport close"      — мережа пропала / клієнт закрив вкладку
    //   "transport error"      — socket помилка
    //   "ping timeout"         — клієнт не відповів на server ping за heartbeat
    //   "client namespace disconnect" — клієнт викликав .disconnect() (наш код)
    //   "server namespace disconnect" — сервер викликав .disconnect()
    const inGame = !!(myRoom && myRoom.status === 'playing');
    console.log(`[socket] disconnect socket=${socket.id} uid=${socket.uid||'?'} reason=${reason} inGame=${inGame}`);
    leave({ voluntary: false });
  });



  function leave(opts = {}) {
    const voluntary = opts.voluntary === true;
    if (!myRoom) return;
    const room = myRoom;
    const slot = mySlot;
    const pinfo = room.players[socket.id];
    socket.leave(room.id);

    if (room.status === 'playing' && slot !== null && pinfo && room.net2) {
      room._disconnected ||= {};
      room._disconnected[slot] = {nick:pinfo.nick,rating:pinfo.rating,uid:pinfo.uid,paddleStats:pinfo.paddleStats,leftVoluntarily:voluntary};
      netcode2.disconnect(room,socket,slot,voluntary);clearSlotPlayer(room,socket.id);
      myRoom=null;mySlot=null;
      if(!voluntary){
        io.to(room.id).emit('player:disconnected',{slot,reconnectTimeout:30});
        room._slotDeleteTimers ||= {};
        room._slotDeleteTimers[slot]=setTimeout(()=>{
          delete room._slotDeleteTimers[slot];
          if(room._disconnected?.[slot] && room.status==='playing')room.net2.authority.system({type:'eliminate',slot,reason:'timeout'});
        },30000);
      }
      return;
    }
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

matchService=createMatchService({io,rooms,createRoom,setPlayer:setSlotPlayer,clearPlayer:clearSlotPlayer,getPlayer:getSlotPlayer,startGame,net:netcode2,getDb:()=>db,authenticate:resolveUid,loadStats:loadValidatedPaddleStats,onSettled:(room,result)=>{
  if(!result.aborted){const deltas=Object.fromEntries(Object.entries(result.rewards).map(([slot,r])=>[slot,r.ratingDelta]));void trackGameEnd(room,result.winnerSlot,result.places,room.mode==='training',deltas);footballDashboard?.match(room,room.mode==='training');}
}});
io.on('connection',socket=>{matchService.attach(socket);playerActivity?.attach(socket);});
httpServer.listen(PORT, () => console.log(`Server on port ${PORT}, ${TICK_RATE} ticks/sec`));

// ── Лог активних кімнат кожні 30с ──
setInterval(() => {
  if (rooms.size === 0) return;
  for (const [rid, room] of rooms) {
    if (!room.game || room.game.gameOver) continue;
    const gs = room.game;
    const now = Date.now();
    if(room.net2)console.log('[NET2] '+JSON.stringify({room:rid,tick:gs.tick,...room.net2.metrics}));
    const players = Object.values(room.players).map(p => {
      // Отримуємо socket для цього гравця
      const sock = io.sockets.sockets.get(p.id);
      let pingInfo;
      if (!sock || !sock.connected) {
        pingInfo = 'disconnected';
      } else if (sock.lastPingT) {
        const age = ((now - sock.lastPingT) / 1000).toFixed(1);
        pingInfo = `lastPing=${age}s`;
      } else {
        pingInfo = 'no-ping';
      }
      return `slot${p.slot}:${p.nick}(${pingInfo})`;
    }).join(', ');
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
    welcomeEnabled=typeof process.env.WELCOME_SENDER_UID==='string'&&/^[^/\x00-\x1f]{1,128}$/.test(process.env.WELCOME_SENDER_UID);
    welcomeService=createWelcome({db,senderUid:process.env.WELCOME_SENDER_UID,senderNick:'DRICS'});
    pushService=createPushService({db,send:message=>admin.messaging().send(message)});
    void pushService.resume().catch(()=>console.log('[push] recovery unavailable'));
    footballDashboard = createDashboard({db, credentials:serviceAccount,pushService,
      onPlayerChanged: uid => {for(const socket of io.sockets.sockets.values())if(socket.uid===uid)socket.disconnect(true);},
      verifyIdToken: token => admin.auth().verifyIdToken(token, true),
      listAccounts: (limit,pageToken) => admin.auth().listUsers(limit,pageToken),
      live: () => ({online:new Set([...io.sockets.sockets.values()].map(s=>s.uid).filter(Boolean)).size,
        rooms:[...rooms.values()].filter(r=>r.status==='playing').length, connections:io.engine.clientsCount})});
    footballDashboard.initialize();
    playerActivity=createPlayerActivity({db});
    console.log('Firebase Admin initialized');
    const playPublisher = createPublisher(serviceAccount);
    playPublisher.checkAccess()
      .then(() => console.log('Google Play billing API ready'))
      .catch(error => console.log('Google Play billing API not ready:', error.code));
    playBilling = createBillingService({
      db,
      verifyIdToken: token => admin.auth().verifyIdToken(token, true),
      publisher: playPublisher,
      enabled: process.env.GOOGLE_PLAY_BILLING_ENABLED === 'true',
    });
  }
} catch(e) {
  console.log('Firebase Admin not available:', e.message);
}

const DAILY_REWARDS = { 1: 100, 2: 50, 3: 30 };

// ── SHOP SOCKET HANDLERS ──
function registerShopHandlers(socket) {
  // Retired fleet stays in accounts; stale clients must not spend currency on it.
  socket.on('shop:buy_paddle',()=>socket.emit('shop:error',{msg:'fleet_unavailable'}));
  socket.on('shop:upgrade_hangar',()=>socket.emit('shop:error',{msg:'fleet_unavailable'}));

  // ── shop:upgrade_next — нова лінійна прокачка (5×10 sublevels) ──
  // Інкрементує sub на 1. Сервер сам обчислює рівень+sublevel з sub.
  // На відміну від upgrade_hangar — не вимагає вибору параметра.
  // ═══════════════════════════════════════════════════════
  socket.on('shop:upgrade_next',()=>socket.emit('shop:error',{msg:'update_required'}));

  // ── Купівля енергії ──
  socket.on('shop:buy_energy', async () => {
    if (!db) return socket.emit('shop:error', { operation:'energy', msg: 'server_unavailable' });
    if (!socket.uid) return socket.emit('shop:error', { operation:'energy', msg: 'auth_required' });
    const privRef = db.collection('users_private').doc(socket.uid);

    try {
      const result = await db.runTransaction(async (tx) => {
        const privSnap = await tx.get(privRef);
        if (!privSnap.exists) throw { code: 'user_not_found' };
        const priv = privSnap.data();

        // Захист від марної покупки: якщо енергії >=80, не дозволяємо
        // (за оптимальної гри 5 ігор по 20 енергії = 100, при 80+ покупка дає <20 пользы)
        const currentEnergy = (priv.energy != null) ? priv.energy : 100;
        if (currentEnergy >= 80) throw { code: 'energy_already_high' };

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
      if (e && e.code) { socket.emit('shop:error', { operation:'energy', msg: e.code }); return; }
      console.error('shop:buy_energy', e.message);
      socket.emit('shop:error', { operation:'energy', msg: 'server_error' });
    }
  });

  // ═══════════════════════════════════════════════════════
  // DAILY QUEST handlers
  // Reward за день N: silver D1=500, D2=1300, D3=1750, D4=2200, D5=3000;
  //                   gold  D6=35,  D7=60.
  // Цикл 7 днів — після D7 наступний successful claim повертає до D1.
  // Streak: якщо last claim був вчора → продовжуємо, інакше → reset до D1.
  // ═══════════════════════════════════════════════════════
  socket.on('daily:get', async () => {
    if (!db) return socket.emit('daily:error', { msg: 'server_unavailable' });
    // Якщо auth ще не пройшов — мовчки ігноруємо (клієнт перезапитає після shop:auth)
    if (!socket.uid) return;
    try {
      const dailyRef = db.collection('users_private').doc(socket.uid).collection('dailyQuest').doc('current');
      const snap = await dailyRef.get();
      const data = snap.exists ? snap.data() : {};
      const todayUTC = new Date().toISOString().slice(0, 10);
      // Чи був claim сьогодні
      const todayClaimed = (data.lastClaimDate === todayUTC);
      const missionSnap=await db.collection('users_private').doc(socket.uid).get();
      socket.emit('daily:state', {
        missions:missionSnap.data()?.dailyMissions||null,
        date: todayUTC,
        todayClaimed,
        streakDay: data.streakDay || 0, // 0 = ніколи не клеймили
        lastClaimDate: data.lastClaimDate || null,
      });
    } catch(e) {
      console.error('daily:get', e.message);
      socket.emit('daily:error', { msg: 'server_error' });
    }
  });

  socket.on('daily:claim', async () => {
    if (!db) return socket.emit('daily:error', { msg: 'server_unavailable' });
    if (!socket.uid) return socket.emit('daily:error', { msg: 'auth_required' });

    const DAILY_REWARDS = [
      { day: 1, cur: 'silver', amount: 500 },
      { day: 2, cur: 'silver', amount: 1300 },
      { day: 3, cur: 'silver', amount: 1750 },
      { day: 4, cur: 'silver', amount: 2200 },
      { day: 5, cur: 'silver', amount: 3000 },
      { day: 6, cur: 'gold',   amount: 35  },
      { day: 7, cur: 'gold',   amount: 60  },
    ];

    try {
      const dailyRef = db.collection('users_private').doc(socket.uid).collection('dailyQuest').doc('current');
      const privRef  = db.collection('users_private').doc(socket.uid);
      const todayUTC = new Date().toISOString().slice(0, 10);

      const result = await db.runTransaction(async (tx) => {
        const dailySnap = await tx.get(dailyRef);
        const privSnap  = await tx.get(privRef);
        if (!privSnap.exists) throw { code: 'user_not_found' };
        const data = dailySnap.exists ? dailySnap.data() : {};
        const priv = privSnap.data();

        // Єдина умова: ще не отримано сьогодні (вхід = достатньо)
        if (data.lastClaimDate === todayUTC) throw { code: 'already_claimed' };

        // Визначаємо який день у streak (1-7)
        // Якщо lastClaimDate === вчора → продовжуємо streak (день+1, циклічно 1-7)
        // Інакше → починаємо з 1
        const yesterdayUTC = new Date(Date.now() - 86400000).toISOString().slice(0, 10);
        const prevStreakDay = data.streakDay || 0;
        let newStreakDay;
        if (data.lastClaimDate === yesterdayUTC && prevStreakDay >= 1 && prevStreakDay < 7) {
          newStreakDay = prevStreakDay + 1;
        } else if (data.lastClaimDate === yesterdayUTC && prevStreakDay === 7) {
          newStreakDay = 1; // після day 7 цикл починається спочатку
        } else {
          newStreakDay = 1; // streak порушено або вперше
        }

        // Знаходимо нагороду
        const reward = DAILY_REWARDS.find(r => r.day === newStreakDay);
        if (!reward) throw { code: 'invalid_day' };

        // Інкрементуємо валюту
        const cur = reward.cur;
        const updPriv = {};
        if (cur === 'silver') updPriv.silver = (priv.silver || 0) + reward.amount;
        if (cur === 'gold')   updPriv.gold   = (priv.gold   || 0) + reward.amount;
        tx.update(privRef, updPriv);

        // Оновлюємо dailyQuest стан
        tx.set(dailyRef, {
          date: todayUTC,
          todayClaimed: true,
          streakDay: newStreakDay,
          lastClaimDate: todayUTC,
        }, { merge: true });

        return {
          claimedDay: newStreakDay,
          cur, amount: reward.amount,
          newSilver: cur === 'silver' ? updPriv.silver : (priv.silver || 0),
          newGold:   cur === 'gold'   ? updPriv.gold   : (priv.gold   || 0),
        };
      });

      socket.emit('daily:claimed', result);
      trackEvent('purchase', {
        uid: socket.uid, kind: 'daily_reward', item: `day_${result.claimedDay}`,
        amount: -result.amount, cur: result.cur, // негативний бо це gain
      });
    } catch(e) {
      if (e && e.code) { socket.emit('daily:error', { msg: e.code }); return; }
      console.error('daily:claim', e.message);
      socket.emit('daily:error', { msg: 'server_error' });
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

    await playerActivity?.close();
    await pushService?.stop();

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

// Fatal errors — логуємо і намагаємось shutdown, не крашимо тихо.
// АЛЕ: помилки з PostgreSQL (analytics) — НЕ КРИТИЧНІ для гри. Гра має продовжити
// працювати навіть якщо PG відвалиться. Інакше один проблемний idle connection
// в analytics вбиває всіх гравців онлайн.
process.on('uncaughtException', (err) => {
  const msg = (err && err.message) || String(err);
  const stack = (err && err.stack) || '';
  // Перевіряємо чи це PG-related помилка (idle connection terminated, ECONNRESET to PG, etc.)
  const isPgError = (
    msg.includes('Connection terminated') ||
    msg.includes('Connection ended') ||
    stack.includes('node_modules/pg/') ||
    stack.includes('/pg/lib/')
  );
  if (isPgError) {
    console.error('[handled] PostgreSQL connection error (game continues):', msg);
    // Не shutdown — analytics не критичний. Pool сам перевідновиться.
    return;
  }
  // Реальна фатальна помилка — shutdown
  console.error('[fatal] uncaughtException:', stack || err);
  gracefulShutdown('uncaughtException');
});
process.on('unhandledRejection', (reason) => {
  const msg = (reason && reason.message) || String(reason);
  console.error('[handled] unhandledRejection:', msg);
  // Не кидаємо shutdown — часто це некритичні Firebase/Firestore/PG помилки.
});
