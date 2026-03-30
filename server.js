// SERVER FINAL — Authoritative 60 tick
// Вся фізика на сервері. Клієнти надсилають тільки input (ліво/право/буст).
// Сервер надсилає повний стан 60 разів/сек.

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

// ── КОНСТАНТИ (мають збігатись з клієнтом) ──
const W = 520, H = 520, BR = 8, SMAX = 4.875, C = 88;
const PL = 54, PLV = 54, PTH = 16, PTV = 16;
const ML = 10, EPU = 1 / 3, ECR = 1 / 10000;
const FDR = 380, BMULT = 1.55, FR = 36, RD = 2000;
const PS = 3.375; // paddle speed px/tick (75% сповільнення)
const FUEL_DRAIN = 0.15 / 60;   // витрата палива за тік руху (15% за секунду)
const FUEL_REGEN = 0.15 / 60;   // відновлення палива за тік без руху

const SLOTS = [0, 1, 2, 3];
const BOT_NAMES = ['ZEPHYR', 'GLITCH', 'NOVA', 'STORM', 'BLAZE', 'PIXEL'];

// Кути — випуклі чверті кіл (опуклі всередину поля)
// Центри кіл ЗОВНІ кутів, м'яч відбивається коли занадто близько до кута
const CORNER_CIRCLES = [
  { cx: C,   cy: C,   nx:  1, ny:  1 }, // верхній лівий
  { cx: W-C, cy: C,   nx: -1, ny:  1 }, // верхній правий
  { cx: C,   cy: H-C, nx:  1, ny: -1 }, // нижній лівий
  { cx: W-C, cy: H-C, nx: -1, ny: -1 }, // нижній правий
];
const CORNER_R = C; // радіус заокруглення

// ── VIEW POSITIONS (slot 0 = bottom, slot 1 = top, slot 2 = left, slot 3 = right) ──
// Це абсолютні позиції на полі — НЕ залежать від перспективи гравця
const SLOT_VIEW = ['bottom', 'top', 'left', 'right'];

function slotToPaddle(slot, x) {
  // x = позиція центру ракетки вздовж стіни
  const view = SLOT_VIEW[slot];
  if (view === 'bottom') return { x: x - PL/2, y: H-PTH-2, w: PL, h: PTH, axis: 'x', min: C, max: W-C-PL };
  if (view === 'top')    return { x: x - PL/2, y: 2,        w: PL, h: PTH, axis: 'x', min: C, max: W-C-PL };
  if (view === 'left')   return { x: 2,         y: x - PLV/2, w: PTV, h: PLV, axis: 'y', min: C, max: H-C-PLV };
  if (view === 'right')  return { x: W-PTV-2,   y: x - PLV/2, w: PTV, h: PLV, axis: 'y', min: C, max: H-C-PLV };
}

// ── ROOM ──
const rooms = new Map();

function createRoom(id) {
  return {
    id,
    players: {},  // socketId -> { slot, nick, rating, uid, input: {left,right,boost} }
    bots: {},     // slot -> { nick, rating }
    status: 'waiting',
    countdownTimer: null,
    tickInterval: null,
    game: null,
  };
}

function findOrCreateRoom() {
  for (const [, r] of rooms) {
    if ((r.status === 'waiting' || r.status === 'countdown') && Object.keys(r.players).length < 4)
      return r;
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

function fillBots(room) {
  room.bots = {};
  let bi = 0;
  const taken = Object.values(room.players).map(p => p.slot);
  for (const s of SLOTS) {
    if (!taken.includes(s)) {
      room.bots[s] = { nick: BOT_NAMES[bi++ % BOT_NAMES.length], rating: 490 + Math.floor(Math.random()*30) };
    }
  }
}

function buildPlayers(room) {
  const res = {};
  for (const s of SLOTS) {
    const p = Object.values(room.players).find(p => p.slot === s);
    if (p) res[s] = { nick: p.nick, rating: p.rating, isBot: false };
    else if (room.bots[s]) res[s] = { nick: room.bots[s].nick, rating: room.bots[s].rating, isBot: true };
  }
  return res;
}

function broadcastLobby(room) {
  io.to(room.id).emit('lobby:update', { players: buildPlayers(room) });
}

// ── PHYSICS ──
function cPt(px, py, ax, ay, bx, by) {
  const dx = bx-ax, dy = by-ay, l2 = dx*dx+dy*dy;
  if (!l2) return { cx: ax, cy: ay };
  const t = Math.max(0, Math.min(1, ((px-ax)*dx+(py-ay)*dy)/l2));
  return { cx: ax+t*dx, cy: ay+t*dy };
}

function resolveChamfers(gs) {
  // Випуклі кути — відбиваємо від чвертей кіл
  // М'яч не може зайти в зону кута (де x<C та y<C одночасно тощо)
  let hit = false;
  for (const corner of CORNER_CIRCLES) {
    const dx = gs.ball.x - corner.cx;
    const dy = gs.ball.y - corner.cy;
    // М'яч в зоні кута якщо він з боку кута (знак збігається)
    if (dx*corner.nx > 0 || dy*corner.ny > 0) continue;
    const dist = Math.hypot(dx, dy);
    // Відбиваємо якщо м'яч ближче ніж CORNER_R до центру кола
    if (dist < CORNER_R - BR) {
      // Нормаль — від центру до м'яча (відштовхуємо всередину поля)
      let nx = dx, ny = dy;
      const l = Math.hypot(nx, ny);
      if (l < 0.001) { nx = corner.nx; ny = corner.ny; }
      else { nx /= l; ny /= l; }
      // Відбиття
      const dot = gs.ball.vx*nx + gs.ball.vy*ny;
      if (dot < 0) {
        gs.ball.vx -= 2*dot*nx;
        gs.ball.vy -= 2*dot*ny;
      }
      // Виштовхуємо м'яч на поверхню кола
      const push = CORNER_R - BR - dist + 1;
      gs.ball.x += nx*push;
      gs.ball.y += ny*push;
      const spd = Math.hypot(gs.ball.vx, gs.ball.vy);
      if (spd > SMAX) { gs.ball.vx = gs.ball.vx/spd*SMAX; gs.ball.vy = gs.ball.vy/spd*SMAX; }
      hit = true;
    }
  }
  return hit;
}

function clampBall(gs) {
  // Додаткова перевірка — м'яч не виходить за межі кутових кіл
  for (const corner of CORNER_CIRCLES) {
    const dx = gs.ball.x - corner.cx;
    const dy = gs.ball.y - corner.cy;
    if (dx*corner.nx > 0 || dy*corner.ny > 0) continue;
    const dist = Math.hypot(dx, dy);
    if (dist < CORNER_R - BR - 2) {
      const l = dist < 0.001 ? 1 : dist;
      gs.ball.x = corner.cx + (dx/l)*(CORNER_R - BR - 1);
      gs.ball.y = corner.cy + (dy/l)*(CORNER_R - BR - 1);
    }
  }
}

function hitRect(ball, p) {
  return ball.x+BR>p.x && ball.x-BR<p.x+p.w && ball.y+BR>p.y && ball.y-BR<p.y+p.h;
}

function addSpin(gs, p) {
  if (p.axis === 'x') { const r = (gs.ball.x-(p.x+p.w/2))/(p.w/2); gs.ball.vx += r*2.5; }
  else { const r = (gs.ball.y-(p.y+p.h/2))/(p.h/2); gs.ball.vy += r*2.5; }
  const sp = Math.hypot(gs.ball.vx, gs.ball.vy);
  const ns = Math.min(sp*1.04, SMAX*0.75);
  gs.ball.vx = gs.ball.vx/sp*ns; gs.ball.vy = gs.ball.vy/sp*ns;
}

function applyFF(gs, slot) {
  const f = gs.fields[slot];
  if (!f.active) return false;
  const p = slotToPaddle(slot, gs.paddles[slot]);
  const fcx = p.x+p.w/2, fcy = p.y+p.h/2;
  const dx = gs.ball.x-fcx, dy = gs.ball.y-fcy;
  const dist = Math.hypot(dx, dy);
  const FF_RADIUS = FR * 1.3; // +30% дистанція
  if (dist > FF_RADIUS+BR) return false;

  const view = SLOT_VIEW[slot];
  const isHoriz = view==='top' || view==='bottom';

  // Визначаємо позицію м'яча відносно центру ракетки вздовж її осі
  const relPos = isHoriz ? (gs.ball.x - fcx) : (gs.ball.y - fcy);
  const halfPaddle = isHoriz ? p.w/2 : p.h/2;
  const relPct = relPos / halfPaddle; // -1 = ліво, 0 = центр, +1 = право

  let outVx = gs.ball.vx;
  let outVy = gs.ball.vy;

  if (dist < FF_RADIUS * 0.5) {
    // М'яч ВСЕРЕДИНІ поля — відштовхуємо вперед + вліво або вправо
    // "Вперед" = від ракетки (нормаль стіни)
    let fwdX = 0, fwdY = 0;
    if (view==='bottom') fwdY = -1;
    else if (view==='top') fwdY = 1;
    else if (view==='left') fwdX = 1;
    else fwdX = -1;

    // Бічна складова залежить від позиції м'яча на ракетці
    const sideForce = relPct * 0.7; // -0.7..+0.7
    let sideX = isHoriz ? sideForce : 0;
    let sideY = isHoriz ? 0 : sideForce;

    // Нормалізуємо напрямок і задаємо швидкість
    const dirX = fwdX + sideX;
    const dirY = fwdY + sideY;
    const len = Math.hypot(dirX, dirY);
    const speed = Math.min(Math.hypot(gs.ball.vx,gs.ball.vy)*BMULT, SMAX);
    outVx = (dirX/len)*speed;
    outVy = (dirY/len)*speed;
  } else {
    // М'яч на краю поля — звичайне відбиття від поверхні поля
    const nx = dist > 0.001 ? dx/dist : 0;
    const ny = dist > 0.001 ? dy/dist : 1;
    const dot = gs.ball.vx*nx+gs.ball.vy*ny;
    outVx = gs.ball.vx - 2*dot*nx;
    outVy = gs.ball.vy - 2*dot*ny;
    const sp = Math.hypot(outVx, outVy);
    if (sp > 0.001) {
      const ns = Math.min(sp*BMULT, SMAX);
      outVx = outVx/sp*ns;
      outVy = outVy/sp*ns;
    }
  }

  gs.ball.vx = outVx;
  gs.ball.vy = outVy;
  // Виштовхуємо м'яч за межу поля
  const nx2 = dist > 0.001 ? dx/dist : (isHoriz?0:1);
  const ny2 = dist > 0.001 ? dy/dist : (isHoriz?1:0);
  gs.ball.x = fcx + nx2*(FF_RADIUS+BR+2);
  gs.ball.y = fcy + ny2*(FF_RADIUS+BR+2);
  f.active = false; f.t = 0;
  return true;
}

function predBall(gs, axis, wp) {
  let bx=gs.ball.x,by=gs.ball.y,vx=gs.ball.vx,vy=gs.ball.vy;
  for (let i=0; i<300; i++) {
    bx+=vx; by+=vy;
    if(bx-BR<0){bx=BR;vx=Math.abs(vx);} if(bx+BR>W){bx=W-BR;vx=-Math.abs(vx);}
    if(by-BR<0){by=BR;vy=Math.abs(vy);} if(by+BR>H){by=H-BR;vy=-Math.abs(vy);}
    if(axis==='x'&&Math.abs(by-wp)<Math.abs(vy)+1) return bx;
    if(axis==='y'&&Math.abs(bx-wp)<Math.abs(vx)+1) return by;
  }
  return axis==='x'?bx:by;
}

function spawnBall(gs) {
  let vx, vy, a = 0;
  do {
    const ang = (Math.random()*0.7+0.15)*Math.PI*(Math.random()<0.5?1:-1)+(Math.random()<0.5?0:Math.PI);
    vx = Math.cos(ang)*(1.3+Math.random()*0.56);
    vy = Math.sin(ang)*(1.3+Math.random()*0.56);
    a++;
  } while ((Math.abs(vx)<1.8||Math.abs(vy)<1.8) && a<30);
  gs.ball = { x: W/2, y: H/2, vx: 0, vy: 0 };
  gs.respawn = { active: true, timer: RD, vx, vy };
}

function createGameState(room) {
  const gs = {
    ball: { x: W/2, y: H/2, vx: 0, vy: 0 },
    respawn: { active: true, timer: RD, vx: 0, vy: 0 },
    paddles: { 0: W/2, 1: W/2, 2: H/2, 3: H/2 }, // центр ракетки
    lives: { 0: ML, 1: ML, 2: ML, 3: ML },
    scores: { 0: 0, 1: 0, 2: 0, 3: 0 },
    energy: { 0: 1, 1: 1, 2: 1, 3: 1 },
    fields: { 0:{active:false,t:0}, 1:{active:false,t:0}, 2:{active:false,t:0}, 3:{active:false,t:0} },
    eliminated: { 0: false, 1: false, 2: false, 3: false },
    botTargets: { 0: W/2, 1: W/2, 2: H/2, 3: H/2 },
    fuel: { 0: 1, 1: 1, 2: 1, 3: 1 },
    gameOver: false,
    winner: null,
    tick: 0,
  };
  const { vx, vy } = (() => {
    let vx, vy, a=0;
    do { const ang=(Math.random()*0.7+0.15)*Math.PI*(Math.random()<0.5?1:-1)+(Math.random()<0.5?0:Math.PI); vx=Math.cos(ang)*4; vy=Math.sin(ang)*4; a++; } while((Math.abs(vx)<1.8||Math.abs(vy)<1.8)&&a<30);
    return {vx,vy};
  })();
  gs.respawn.vx = vx; gs.respawn.vy = vy;
  return gs;
}

function activeSlots(gs) { return SLOTS.filter(s => !gs.eliminated[s]); }

function tick(room) {
  const gs = room.game;
  if (!gs || gs.gameOver) return;
  gs.tick++;

  // ── Енергія і поля ──
  for (const s of SLOTS) {
    if (gs.eliminated[s]) continue;
    if (gs.fields[s].active) {
      gs.fields[s].t += TICK_MS;
      if (gs.fields[s].t >= FDR) { gs.fields[s].active = false; gs.fields[s].t = 0; }
    } else {
      gs.energy[s] = Math.min(1, gs.energy[s] + ECR * TICK_MS);
    }
  }

  // ── Рух гравців (input) ──
  for (const [sid, player] of Object.entries(room.players)) {
    const s = player.slot;
    if (gs.eliminated[s]) continue;
    const inp = player.input || {};
    const view = SLOT_VIEW[s];
    const isHoriz = view === 'top' || view === 'bottom';
    const mn = isHoriz ? C+PL/2 : C+PLV/2;
    const mx = isHoriz ? W-C-PL/2 : H-C-PLV/2;
    const moving = inp.left || inp.right;
    if (moving && gs.fuel[s] > 0) {
      // Рухаємось тільки якщо є паливо
      if (inp.left)  gs.paddles[s] = Math.max(mn, gs.paddles[s] - PS);
      if (inp.right) gs.paddles[s] = Math.min(mx, gs.paddles[s] + PS);
      gs.fuel[s] = Math.max(0, gs.fuel[s] - FUEL_DRAIN);
    } else if (!moving) {
      // Відновлення палива коли не рухається
      gs.fuel[s] = Math.min(1, gs.fuel[s] + FUEL_REGEN);
    }
    if (inp.boost && !gs.fields[s].active && gs.energy[s] >= EPU) {
      gs.fields[s].active = true; gs.fields[s].t = 0;
      gs.energy[s] = Math.max(0, gs.energy[s] - EPU);
    }
  }

  // ── Боти ──
  for (const s of SLOTS) {
    if (!room.bots[s] || gs.eliminated[s]) continue;
    const view = SLOT_VIEW[s];
    const isHoriz = view === 'top' || view === 'bottom';
    const mn = isHoriz ? C+PL/2 : C+PLV/2;
    const mx = isHoriz ? W-C-PL/2 : H-C-PLV/2;
    const wallPos = isHoriz ? (view==='top'?PTH/2:H-PTH/2) : (view==='left'?PTV/2:W-PTV/2);
    const pred = isHoriz ? predBall(gs,'x',wallPos) : predBall(gs,'y',wallPos);
    gs.botTargets[s] += (pred - gs.botTargets[s]) * 0.08;
    const diff = gs.botTargets[s] - gs.paddles[s];
    if (Math.abs(diff) > 2) gs.paddles[s] = Math.max(mn, Math.min(mx, gs.paddles[s] + Math.sign(diff)*Math.min(3.5,Math.abs(diff))));
    // Бот активує поле
    if (!gs.fields[s].active && gs.energy[s] >= EPU) {
      const p = slotToPaddle(s, gs.paddles[s]);
      const dist = isHoriz ? Math.abs(gs.ball.y-(p.y+p.h/2)) : Math.abs(gs.ball.x-(p.x+p.w/2));
      if (dist < 80 && Math.random() < 0.02) {
        gs.fields[s].active = true; gs.fields[s].t = 0;
        gs.energy[s] = Math.max(0, gs.energy[s] - EPU);
      }
    }
  }

  // ── Respawn ──
  if (gs.respawn.active) {
    gs.respawn.timer -= TICK_MS;
    if (gs.respawn.timer <= 0) {
      gs.respawn.active = false;
      gs.ball.vx = gs.respawn.vx;
      gs.ball.vy = gs.respawn.vy;
    }
    broadcastState(room);
    return;
  }

  // ── М'яч ──
  gs.ball.x += gs.ball.vx;
  gs.ball.y += gs.ball.vy;

  // Силові поля
  for (const s of SLOTS) {
    if (gs.eliminated[s]) continue;
    if (applyFF(gs, s)) { broadcastState(room); return; }
  }

  // Кути
  for(let i=0;i<3;i++) if(resolveChamfers(gs)) break;
  clampBall(gs);

  // Ракетки
  for (const s of SLOTS) {
    if (gs.eliminated[s]) continue;
    const p = slotToPaddle(s, gs.paddles[s]);
    if (hitRect(gs.ball, p)) {
      const view = SLOT_VIEW[s];
      if (p.axis === 'x') {
        gs.ball.y = view==='top' ? p.y+p.h+BR : p.y-BR;
        gs.ball.vy = view==='top' ? Math.abs(gs.ball.vy) : -Math.abs(gs.ball.vy);
      } else {
        gs.ball.x = view==='left' ? p.x+p.w+BR : p.x-BR;
        gs.ball.vx = view==='left' ? Math.abs(gs.ball.vx) : -Math.abs(gs.ball.vx);
      }
      addSpin(gs, p);
      broadcastState(room);
      return;
    }
  }

  // ── Голи ──
  const goal = (slot) => {
    if (gs.eliminated[slot]) return false;
    gs.scores[slot]++; gs.lives[slot]--;
    io.to(room.id).emit('goal', { slot, lives: {...gs.lives}, scores: {...gs.scores} });
    if (gs.lives[slot] <= 0) {
      gs.eliminated[slot] = true;
      io.to(room.id).emit('eliminated', { slot });
      const active = activeSlots(gs);
      if (active.length === 1) { endGame(room, active[0]); return true; }
    }
    spawnBall(gs);
    return true;
  };

  const bx=gs.ball.x, by=gs.ball.y;
  // Гол рахується тільки коли м'яч ПОВНІСТЮ зник за межею (центр + радіус)
  if (by+BR<0   && bx>C && bx<W-C) { if (!goal(1)) gs.ball.vy= Math.abs(gs.ball.vy); }
  else if (by-BR>H && bx>C && bx<W-C) { if (!goal(0)) gs.ball.vy=-Math.abs(gs.ball.vy); }
  else if (bx+BR<0 && by>C && by<H-C) { if (!goal(2)) gs.ball.vx= Math.abs(gs.ball.vx); }
  else if (bx-BR>W && by>C && by<H-C) { if (!goal(3)) gs.ball.vx=-Math.abs(gs.ball.vx); }

  broadcastState(room);
}

function broadcastState(room) {
  const gs = room.game;
  if (!gs) return;
  io.to(room.id).emit('gs', {
    bx: Math.round(gs.ball.x*10)/10,
    by: Math.round(gs.ball.y*10)/10,
    bvx: Math.round(gs.ball.vx*100)/100,
    bvy: Math.round(gs.ball.vy*100)/100,
    rs: gs.respawn.active ? Math.round(gs.respawn.timer) : -1,
    rvx: gs.respawn.vx, rvy: gs.respawn.vy,
    p: [ // paddles — центри
      Math.round(gs.paddles[0]),
      Math.round(gs.paddles[1]),
      Math.round(gs.paddles[2]),
      Math.round(gs.paddles[3]),
    ],
    e: [ // energy 0-100
      Math.round(gs.energy[0]*100),
      Math.round(gs.energy[1]*100),
      Math.round(gs.energy[2]*100),
      Math.round(gs.energy[3]*100),
    ],
    fu: [ // fuel 0-100
      Math.round(gs.fuel[0]*100),
      Math.round(gs.fuel[1]*100),
      Math.round(gs.fuel[2]*100),
      Math.round(gs.fuel[3]*100),
    ],
    f: [ // fields active
      gs.fields[0].active?1:0,
      gs.fields[1].active?1:0,
      gs.fields[2].active?1:0,
      gs.fields[3].active?1:0,
    ],
    el: [ // eliminated
      gs.eliminated[0]?1:0,
      gs.eliminated[1]?1:0,
      gs.eliminated[2]?1:0,
      gs.eliminated[3]?1:0,
    ],
  });
}

function endGame(room, winnerSlot) {
  const gs = room.game;
  gs.gameOver = true; gs.winner = winnerSlot;
  room.status = 'finished';
  if (room.tickInterval) { clearInterval(room.tickInterval); room.tickInterval = null; }
  io.to(room.id).emit('game:over', { winnerSlot, players: buildPlayers(room) });
}

function startGame(room) {
  room.status = 'playing';
  fillBots(room);
  room.game = createGameState(room);
  io.to(room.id).emit('game:start', {
    players: buildPlayers(room),
    mySlot: null, // кожен отримає свій через mm:joined
  });
  // Надсилаємо mySlot кожному окремо
  for (const [sid, player] of Object.entries(room.players)) {
    io.to(sid).emit('myslot', { mySlot: player.slot });
  }
  room.tickInterval = setInterval(() => tick(room), TICK_MS);
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

// ── SOCKET ──
io.on('connection', (socket) => {
  let myRoom = null, mySlot = null;

  socket.on('mm:join', ({ nick, rating, uid }) => {
    const room = findOrCreateRoom();
    myRoom = room;
    mySlot = getAvailableSlot(room);
    if (mySlot === undefined) { socket.emit('mm:error', 'Кімната повна'); return; }

    room.players[socket.id] = { slot: mySlot, nick, rating, uid, input: {} };
    socket.join(room.id);
    socket.emit('mm:joined', { mySlot });
    broadcastLobby(room);

    const count = Object.keys(room.players).length;
    if (count === 1) { room.status = 'waiting'; socket.emit('mm:waiting', {}); }
    else if (count >= 4) { if (room.countdownTimer) { clearInterval(room.countdownTimer); room.countdownTimer = null; } startGame(room); }
    else startCountdown(room);
  });

  // Гравець надсилає стан кнопок (не позицію!)
  socket.on('input', ({ left, right, boost }) => {
    if (!myRoom || !myRoom.players[socket.id]) return;
    myRoom.players[socket.id].input = { left, right, boost };
  });

  socket.on('mm:cancel', () => leave());
  socket.on('disconnect', () => leave());

  function leave() {
    if (!myRoom) return;
    delete myRoom.players[socket.id];
    socket.leave(myRoom.id);
    const count = Object.keys(myRoom.players).length;
    if (count === 0) {
      if (myRoom.tickInterval) clearInterval(myRoom.tickInterval);
      if (myRoom.countdownTimer) clearInterval(myRoom.countdownTimer);
      rooms.delete(myRoom.id);
    } else {
      broadcastLobby(myRoom);
      if (myRoom.game?.started && mySlot !== null) {
        myRoom.bots[mySlot] = { nick: BOT_NAMES[0], rating: 500 };
        io.to(myRoom.id).emit('player:left', { slot: mySlot });
      } else if (myRoom.status === 'countdown' && count === 1) {
        if (myRoom.countdownTimer) { clearInterval(myRoom.countdownTimer); myRoom.countdownTimer = null; }
        myRoom.status = 'waiting';
        io.to(myRoom.id).emit('mm:waiting', {});
      }
    }
    myRoom = null; mySlot = null;
  }
});

httpServer.listen(PORT, () => console.log(`Server on port ${PORT}, ${TICK_RATE} ticks/sec`));
