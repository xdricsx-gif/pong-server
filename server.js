// SERVER FINAL — Authoritative 60 tick
const { createServer } = require('http');
const { Server } = require('socket.io');

const httpServer = createServer();
const io = new Server(httpServer, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  pingInterval: 5000,
  pingTimeout: 10000,
});

const PORT = process.env.PORT || 3000;
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
    const player = Object.values(room.players).find(p => p.slot === slot);
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

function applyFFBall(gs, s, ball) {
  const f = gs.fields[s];
  if (!f || !f.active) return false;

  // ── Cooldown: запобігає стаку колізій ──
  const _cdKey = 'ff_cd_' + s;
  if (ball[_cdKey] && ball[_cdKey] > 0) { ball[_cdKey]--; return false; }

  const p = slotToPaddle(s, gs.paddles[s], gs, null);
  const fcx = p.x + p.w/2, fcy = p.y + p.h/2;
  const dx = ball.x - fcx, dy = ball.y - fcy;
  const dist = Math.hypot(dx, dy);
  const maxR = f.maxR || FR;

  // ── Зона виявлення: тільки +BR буфер, без швидкості ──
  // Це фіксує баг "занадто рання реакція на швидкі м'ячі"
  const collideR = maxR + BR;
  if (dist > collideR + 4) return false;

  // ── Нормаль від центру поля → м'яч ──
  // Якщо м'яч прямо в центрі — відштовхуємо від ракетки
  let nx, ny;
  if (dist > 0.5) {
    nx = dx / dist;
    ny = dy / dist;
  } else {
    const view = SLOT_VIEW[s];
    nx = view==='left' ? 1 : view==='right' ? -1 : 0;
    ny = view==='bottom' ? -1 : view==='top' ? 1 : 0;
    if (nx===0 && ny===0) ny = -1;
  }

  // ── Dot product: швидкість м'яча вздовж нормалі ──
  const dot = ball.vx*nx + ball.vy*ny;

  // ── М'яч летить назовні (dot > 0) — не чіпаємо ──
  // Це фіксує баг "hooking" коли поле наздоганяє м'яч що вже відлетів
  if (dot > 0) {
    // Але якщо м'яч застряг всередині — виштовхуємо
    if (dist < collideR - BR) {
      ball.x = fcx + nx * (collideR + 2);
      ball.y = fcy + ny * (collideR + 2);
    }
    return false;
  }

  // ── CCD: точна позиція зіткнення ──
  // Перевіряємо попередній тік щоб знайти точний момент перетину межі
  const oldX = ball.x - ball.vx;
  const oldY = ball.y - ball.vy;
  const rx = oldX - fcx, ry = oldY - fcy;
  const a = ball.vx*ball.vx + ball.vy*ball.vy;
  const b = 2*(rx*ball.vx + ry*ball.vy);
  const c = rx*rx + ry*ry - collideR*collideR;
  const disc = b*b - 4*a*c;

  if (disc >= 0 && a > 0.0001) {
    const t = (-b - Math.sqrt(disc)) / (2*a);
    // t в діапазоні 0..1 = перетин відбувся цього тіку
    if (t >= 0 && t <= 1.0) {
      ball.x = oldX + ball.vx * t;
      ball.y = oldY + ball.vy * t;
      const hdx = ball.x - fcx, hdy = ball.y - fcy;
      const hdist = Math.hypot(hdx, hdy);
      if (hdist > 0.5) { nx = hdx/hdist; ny = hdy/hdist; }
    }
  }

  // ── Відбиття: дзеркальне відображення швидкості ──
  const dot2 = ball.vx*nx + ball.vy*ny;
  ball.vx -= 2 * dot2 * nx;
  ball.vy -= 2 * dot2 * ny;

  // ── Нормалізуємо до цільової швидкості ──
  const vspeed = Math.hypot(ball.vx, ball.vy);
  const targetSpeed = Math.min(Math.max(vspeed, 2.5) * BMULT, SMAX);
  if (vspeed > 0.01) {
    ball.vx = (ball.vx / vspeed) * targetSpeed;
    ball.vy = (ball.vy / vspeed) * targetSpeed;
  }

  // ── Мінімальний кут від нормалі ≥30° (0.5 = cos60°) ──
  // Запобігає "ковзанню" вздовж межі поля
  const MIN_NORM = 0.5;
  const normComp = ball.vx*nx + ball.vy*ny;
  if (normComp < MIN_NORM * targetSpeed) {
    const boost = MIN_NORM * targetSpeed - normComp;
    ball.vx += nx * boost;
    ball.vy += ny * boost;
    const s2 = Math.hypot(ball.vx, ball.vy);
    if (s2 > 0.01) { ball.vx = ball.vx/s2*targetSpeed; ball.vy = ball.vy/s2*targetSpeed; }
  }

  // ── Виштовхуємо м'яч назовні межі ──
  ball.x = fcx + nx * (collideR + 4);
  ball.y = fcy + ny * (collideR + 4);

  // ── Cooldown 10 тіків (~167ms) ──
  ball['ff_cd_' + s] = 10;

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
  return { id, players: {}, bots: {}, status: 'waiting', countdownTimer: null, tickInterval: null, game: null };
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
    const p = Object.values(room.players).find(p => p.slot === s);
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
    lives: { 0: ML, 1: ML, 2: ML, 3: ML },
    scores: { 0: 0, 1: 0, 2: 0, 3: 0 },
    energy: { 0: 1, 1: 1, 2: 1, 3: 1 },
    fields: { 0:{active:false,t:0,r:0}, 1:{active:false,t:0,r:0}, 2:{active:false,t:0,r:0}, 3:{active:false,t:0,r:0} },
    eliminated: { 0: false, 1: false, 2: false, 3: false },
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
        const pStats = Object.values(room.players).find(p=>p.slot===s)?.paddleStats
          || room._disconnected?.[s]?.paddleStats;
        const fd = pStats?.fd || 1.0;
        const maxRf = gs.fields[s].maxR || pStats?.fr || FR;
        gs.fields[s].r = Math.min(maxRf, (gs.fields[s].t / 200) * maxRf);
        if (gs.fields[s].t >= FDR * fd) { gs.fields[s].active = false; gs.fields[s].t = 0; gs.fields[s].r = 0; }
      } else {
        const pStats2 = Object.values(room.players).find(p=>p.slot===s)?.paddleStats
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
      if (inp.boost && !gs.fields[s].active && gs.energy[s] >= EPU) {
        gs.fields[s].active = true;
        gs.fields[s].r = 0;
        gs.fields[s].maxR = pStats?.fr || FR;
        gs.energy[s] = Math.max(0, gs.energy[s] - EPU);

        // Client-authoritative boost position:
        // Якщо клієнт надіслав позицію при активації — перевіряємо чи надійна
        if (inp.boostPos !== undefined) {
          const view = SLOT_VIEW[s];
          const isHoriz = view === 'top' || view === 'bottom';
          const pW = pStats?.w || PL;
          const half = pW / 2;
          const mn = C + half;
          const mx = (isHoriz ? W : H) - C - half;
          const clampedPos = Math.max(mn, Math.min(mx, inp.boostPos));
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
        gs.eliminated[slot] = true;
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
      const p=Object.values(room.players).find(p=>p.slot===s);
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

  for (const [sid, player] of Object.entries(room.players)) {
    if (!player.uid || player.isBot) continue;
    const s = player.slot;
    const pubRef = db.collection('users_public').doc(player.uid);
    const privRef = db.collection('users_private').doc(player.uid);

    if (isTraining) {
      // Тренування — XP і срібло
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
      // Рейтингова гра — рейтинг, XP, gamesPlayed, wins
      const delta = ratingDeltas[s] || -20;
      const placeIdx = places.indexOf(s);
      const XP_MAP = [100, 50, 20, 0];
      const xp = XP_MAP[placeIdx] || 0;
      const currentRating = player.rating || 500;
      const newRating = Math.max(0, currentRating + delta);

      const pubUpd = {
        rating: newRating,
        gamesPlayed: admin.firestore.FieldValue.increment(1),
        ratingDate: today,
      };
      if (delta > 0) pubUpd.wins = admin.firestore.FieldValue.increment(1);
      // ratingToday — накопичуємо за день
      pubUpd.ratingToday = admin.firestore.FieldValue.increment(delta);

      batch.update(pubRef, pubUpd);
      if (xp > 0) batch.update(privRef, { xp: admin.firestore.FieldValue.increment(xp) });
    }
  }

  try {
    await batch.commit();
    console.log('[rewards] committed for room', room.id);
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
  const ML_END = 10;
  // Місця визначаються за кількістю залишених lives (більше = менше пропустив = вище місце)
  // Переможець вже визначений, решта сортується за lives (desc)
  const places = [winnerSlot];
  const others = SLOTS.filter(s => s !== winnerSlot)
    .sort((a, b) => (gs.lives[b] ?? 0) - (gs.lives[a] ?? 0));
  places.push(...others);
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
    const p = Object.values(room.players).find(p => p.slot === s);
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
      if (room.game && !room.game.gameOver) {
        const gs = room.game;
        const ML_SRV = 10;
        const active = SLOTS.filter(s => !gs.eliminated?.[s]);
        if (active.length > 0) {
          // Хто менше пропустив — переможець
          // Переможець = хто більше lives залишилось (менше пропустив)
          const winnerSlot = active.reduce((a, b) =>
            (gs.lives?.[a] ?? 0) >= (gs.lives?.[b] ?? 0) ? a : b
          );
          console.log('[match:timeout] room='+room.id+' winner='+winnerSlot+' lost='+JSON.stringify(lostMap));
          endGame(room, winnerSlot);
        }
      }
    }
  }, 1000);

  console.log(`Game started: ${room.id}, ${Object.keys(room.players).length} real players`);
}

function startCountdown(room) {
  if (room.countdownTimer) { clearInterval(room.countdownTimer); room.countdownTimer = null; }
  room.status = 'countdown';
  let tl = 10;
  io.to(room.id).emit('mm:countdown', { timeLeft: tl });
  room.countdownTimer = setInterval(() => {
    tl--;
    io.to(room.id).emit('mm:countdown', { timeLeft: tl });
    if (tl <= 0) { clearInterval(room.countdownTimer); room.countdownTimer = null; startGame(room); }
  }, 1000);
}

io.on('connection', (socket) => {
  let myRoom = null, mySlot = null;

  socket.on('paddle:stats',({slot,paddleStats})=>{
    if(myRoom){
      for(const [sid,p] of Object.entries(myRoom.players)){
        if(p.slot===slot){ p.paddleStats=paddleStats; break; }
      }
    }
  });

  // Реєструємо shop handlers для цього сокета
  registerShopHandlers(socket);

  // Аутентифікація для shop без mm:join (з меню/магазину)
  socket.on('shop:auth', ({ uid }) => {
    if (uid) socket.uid = uid;
  });

  // ── Обмін золото → срібло ──
  socket.on('shop:exchange', async ({ rateId }) => {
    if (!db) return socket.emit('shop:error', { msg: 'server_unavailable' });
    const RATES = [
      { goldIn:1,   silverOut:100   },
      { goldIn:5,   silverOut:500   },
      { goldIn:10,  silverOut:1000  },
      { goldIn:50,  silverOut:5000  },
      { goldIn:100, silverOut:10000 },
    ];
    const rate = RATES[parseInt(rateId)];
    if (!rate) return socket.emit('shop:error', { msg: 'invalid_rate' });
    try {
      const privRef = db.collection('users_private').doc(socket.uid);
      const snap = await privRef.get();
      if (!snap.exists) return socket.emit('shop:error', { msg: 'user_not_found' });
      const priv = snap.data();
      if ((priv.gold || 0) < rate.goldIn)
        return socket.emit('shop:error', { msg: 'not_enough_gold' });
      await privRef.update({
        gold:   admin.firestore.FieldValue.increment(-rate.goldIn),
        silver: admin.firestore.FieldValue.increment(rate.silverOut),
      });
      socket.emit('shop:exchanged', {
        gold:   (priv.gold || 0) - rate.goldIn,
        silver: (priv.silver || 0) + rate.silverOut,
      });
    } catch(e) {
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
    try {
      const pubRef  = db.collection('users_public').doc(socket.uid);
      const privRef = db.collection('users_private').doc(socket.uid);
      const snap = await pubRef.get();
      if (!snap.exists) return;
      const pub = snap.data();
      if (type === 'ranked') {
        const newRating = Math.max(0, (pub.rating || 500) + delta);
        await pubRef.update({
          rating: newRating,
          gamesPlayed: admin.firestore.FieldValue.increment(1),
          ratingDate: today,
          ratingToday: admin.firestore.FieldValue.increment(delta),
        });
        if (XP[place] > 0)
          await privRef.update({ xp: admin.firestore.FieldValue.increment(XP[place]) });
      }
      socket.emit('shop:pending_resolved', { type, place, delta });
    } catch(e) {
      console.error('shop:resolve_pending', e.message);
    }
  });

  socket.on('mm:join', ({ nick, rating, uid, wins, games, paddleStats, trainingMode, avatarId }) => {
    if(uid) socket.uid = uid; // зберігаємо для shop handlers
    if(trainingMode){
      const tRoom = createRoom('training_'+socket.id);
      rooms.set(tRoom.id, tRoom);
      myRoom = tRoom; mySlot = 0;
      tRoom.players[socket.id] = { slot:0, nick, rating, uid, wins:wins||0, games:games||0, input:{},
        paddleStats: paddleStats||{spd:3.375,w:54,fr:54,bm:2.32}, trainingMode:true };
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
    room.players[socket.id] = { slot: mySlot, nick, rating, uid, wins: wins||0, games: games||0, avatarId: avatarId||0, input: {},
      paddleStats: paddleStats || { spd:3.375, w:54, fr:54, bm:2.32, er:1.0, fd:1.0 } };
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
      startCountdown(room); // таймер 10с — якщо не набралось 4, стартуємо з тими хто є
    }
  });

  socket.on('rejoin', ({ roomId, slot, nick, rating, uid, paddleStats }) => {
    const room = rooms.get(roomId);
    if (!room) { socket.emit('rejoin:fail', { reason: 'room_gone' }); return; }

    const alreadyTaken = Object.values(room.players).some(p => p.slot === slot);
    if (alreadyTaken) { socket.emit('rejoin:fail', { reason: 'slot_taken' }); return; }

    // ── Відміняємо таймер кіку для цього слоту ──
    if (room._slotDeleteTimers && room._slotDeleteTimers[slot]) {
      clearTimeout(room._slotDeleteTimers[slot]);
      delete room._slotDeleteTimers[slot];
    }

    // ── Відновлюємо paddleStats: з rejoin запиту або зі збереженого _disconnected ──
    const savedData = room._disconnected && room._disconnected[slot];
    const restoredPaddleStats = paddleStats || savedData?.paddleStats || { spd:3.375, w:54, fr:54, bm:2.32, er:1.0, fd:1.0 };

    if (room._disconnected) delete room._disconnected[slot];

    myRoom = room; mySlot = slot;
    delete room.bots[slot];
    room.players[socket.id] = { slot, nick, rating, uid, input: {}, paddleStats: restoredPaddleStats };
    socket.join(room.id);

    // ── Сповіщаємо інших що гравець повернувся ──
    io.to(room.id).emit('player:reconnected', { slot });

    if (room.status === 'playing' && room.game) {
      const gs = room.game;
      // Paddle visuals для всіх слотів — щоб після reconnect відображались правильно
      const rejoinPaddleVisuals = SLOTS.map(s => {
        const p = Object.values(room.players).find(p => p.slot === s);
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

  socket.on('mm:cancel', () => leave());
  socket.on('disconnect', () => leave());



  function leave() {
    if (!myRoom) return;
    const room = myRoom;
    const slot = mySlot;
    const pinfo = room.players[socket.id];
    delete room.players[socket.id];
    socket.leave(room.id);
    myRoom = null; mySlot = null;

    const count = Object.keys(room.players).length;

    if (room.status === 'playing' && slot !== null && pinfo) {
      // ── НОВА ЛОГІКА: зберігаємо дані гравця і чекаємо 30с ──
      room._disconnected = room._disconnected || {};
      room._disconnected[slot] = {
        nick: pinfo.nick,
        rating: pinfo.rating,
        uid: pinfo.uid,
        paddleStats: pinfo.paddleStats, // ← зберігаємо прокачку!
      };

      // ── Сповіщаємо інших що гравець відключився (але гра продовжується!) ──
      io.to(room.id).emit('player:disconnected', { slot, reconnectTimeout: 30 });

      // ── Таймер 30с для цього конкретного слоту ──
      room._slotDeleteTimers = room._slotDeleteTimers || {};
      room._slotDeleteTimers[slot] = setTimeout(() => {
        if (!room._disconnected || !room._disconnected[slot]) return; // вже повернувся
        console.log(`Room ${room.id}: slot ${slot} timed out`);

        // Визначаємо місце гравця на момент кіку
        const gs = room.game;
        const places = gs ? SLOTS.filter(s=>!gs.eliminated[s])
          .sort((a,b)=>(gs.scores[b]||0)-(gs.scores[a]||0)) : [];
        const placeIdx = places.indexOf(slot);
        const RATING_BY_PLACE = [50, 20, 5, -20];
        const ratingDelta = RATING_BY_PLACE[placeIdx] !== undefined ? RATING_BY_PLACE[placeIdx] : -20;

        delete room._disconnected[slot];
        delete room._slotDeleteTimers[slot];

        // ── Тепер надсилаємо player:left з рейтингом ──
        io.to(room.id).emit('player:left', { slot, ratingDelta });

        // Перевіряємо чи кімната пуста після кіку
        const realPlayers = Object.keys(room.players).length;
        const stillDisconnected = Object.keys(room._disconnected || {}).length;
        if (realPlayers === 0 && stillDisconnected === 0) {
          if (room.tickInterval) clearInterval(room.tickInterval);
          io.to(room.id).emit('room:closed');
          rooms.delete(room.id);
        }
      }, 30000);

    } else if (count === 0) {
      if (room.tickInterval) clearInterval(room.tickInterval);
      if (room.countdownTimer) clearInterval(room.countdownTimer);
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
    const pid = parseInt(paddleId);
    const priceDef = PADDLE_PRICES_SRV[pid];
    if (!priceDef) return socket.emit('shop:error', { msg: 'invalid_paddle' });
    if (priceDef.cur === 'free') return socket.emit('shop:error', { msg: 'already_free' });

    try {
      const privRef = db.collection('users_private').doc(socket.uid);
      const pubRef  = db.collection('users_public').doc(socket.uid);
      const privSnap = await privRef.get();
      if (!privSnap.exists) return socket.emit('shop:error', { msg: 'user_not_found' });
      const priv = privSnap.data();

      // Перевірка: вже куплено?
      if ((priv.ownedPaddles || []).includes(pid))
        return socket.emit('shop:error', { msg: 'already_owned' });

      // Перевірка балансу
      if (priceDef.cur === 'silver' && (priv.silver || 0) < priceDef.price)
        return socket.emit('shop:error', { msg: 'not_enough_silver' });
      if (priceDef.cur === 'gold' && (priv.gold || 0) < priceDef.price)
        return socket.emit('shop:error', { msg: 'not_enough_gold' });

      const privUpd = { ownedPaddles: [...(priv.ownedPaddles||[]), pid] };
      if (priceDef.cur === 'silver') privUpd.silver = admin.firestore.FieldValue.increment(-priceDef.price);
      if (priceDef.cur === 'gold')   privUpd.gold   = admin.firestore.FieldValue.increment(-priceDef.price);

      await Promise.all([
        privRef.update(privUpd),
        pubRef.update({ paddleId: pid }),
      ]);

      const newBalance = priceDef.cur === 'silver'
        ? { silver: (priv.silver || 0) - priceDef.price }
        : { gold: (priv.gold || 0) - priceDef.price };

      socket.emit('shop:bought_paddle', { paddleId: pid, ...newBalance });
    } catch(e) {
      console.error('shop:buy_paddle', e.message);
      socket.emit('shop:error', { msg: 'server_error' });
    }
  });

  // ── Апгрейд модуля ──
  socket.on('shop:upgrade_hangar', async ({ paddleId, partId }) => {
    if (!db) return socket.emit('shop:error', { msg: 'server_unavailable' });
    const pid = parseInt(paddleId);
    const VALID_PARTS = ['w','spd','fr','bm','er','fd'];
    if (!VALID_PARTS.includes(partId)) return socket.emit('shop:error', { msg: 'invalid_part' });

    try {
      const privRef = db.collection('users_private').doc(socket.uid);
      const privSnap = await privRef.get();
      if (!privSnap.exists) return socket.emit('shop:error', { msg: 'user_not_found' });
      const priv = privSnap.data();

      const hangars = priv.hangars || {};
      const currentLv = ((hangars[pid] || {})[partId]) || 1;
      if (currentLv >= 10) return socket.emit('shop:error', { msg: 'max_level' });

      const cost = HANGAR_COSTS_SRV[currentLv];
      if (!cost) return socket.emit('shop:error', { msg: 'invalid_level' });

      // Перевірка балансу
      if (cost.cur === 'silver' && (priv.silver || 0) < cost.price)
        return socket.emit('shop:error', { msg: 'not_enough_silver' });
      if (cost.cur === 'gold' && (priv.gold || 0) < cost.price)
        return socket.emit('shop:error', { msg: 'not_enough_gold' });

      const newLv = currentLv + 1;
      const upd = {};
      upd[`hangars.${pid}.${partId}`] = newLv;
      if (cost.cur === 'silver') upd.silver = admin.firestore.FieldValue.increment(-cost.price);
      if (cost.cur === 'gold')   upd.gold   = admin.firestore.FieldValue.increment(-cost.price);

      await privRef.update(upd);

      const newBalance = cost.cur === 'silver'
        ? { silver: (priv.silver || 0) - cost.price }
        : { gold: (priv.gold || 0) - cost.price };

      socket.emit('shop:upgraded', { paddleId: pid, partId, newLevel: newLv, ...newBalance });
    } catch(e) {
      console.error('shop:upgrade_hangar', e.message);
      socket.emit('shop:error', { msg: 'server_error' });
    }
  });

  // ── Купівля енергії ──
  socket.on('shop:buy_energy', async () => {
    if (!db) return socket.emit('shop:error', { msg: 'server_unavailable' });
    try {
      const privRef = db.collection('users_private').doc(socket.uid);
      const privSnap = await privRef.get();
      if (!privSnap.exists) return socket.emit('shop:error', { msg: 'user_not_found' });
      const priv = privSnap.data();

      if ((priv.gold || 0) < ENERGY_GOLD_COST_SRV)
        return socket.emit('shop:error', { msg: 'not_enough_gold' });

      await privRef.update({
        gold: admin.firestore.FieldValue.increment(-ENERGY_GOLD_COST_SRV),
        energy: 100,
        energyLastRegen: Date.now(),
      });

      socket.emit('shop:energy_bought', {
        gold: (priv.gold || 0) - ENERGY_GOLD_COST_SRV,
        energy: 100,
      });
    } catch(e) {
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
