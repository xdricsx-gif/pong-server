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
const FDR = 380, BMULT = 1.55, FR = 36, RD = 1500;
const PS = 3.375; // paddle speed px/tick (75% сповільнення)

const SLOTS = [0, 1, 2, 3];
const SLOT_VIEW = ['bottom', 'top', 'left', 'right'];
const BOT_NAMES = ['ZEPHYR', 'GLITCH', 'NOVA', 'STORM', 'BLAZE', 'PIXEL'];

const CS = [
  { ax: 0,   ay: C,   bx: C,   by: 0,   nx:  1/Math.SQRT2, ny:  1/Math.SQRT2 },
  { ax: W-C, ay: 0,   bx: W,   by: C,   nx: -1/Math.SQRT2, ny:  1/Math.SQRT2 },
  { ax: 0,   ay: H-C, bx: C,   by: H,   nx:  1/Math.SQRT2, ny: -1/Math.SQRT2 },
  { ax: W-C, ay: H,   bx: W,   by: H-C, nx: -1/Math.SQRT2, ny: -1/Math.SQRT2 },
];


function slotToPaddle(slot, cx) {
  const view = SLOT_VIEW[slot];
  if (view==='bottom') return {x:cx-PL/2, y:H-PTH-2, w:PL, h:PTH, axis:'x', min:C, max:W-C-PL};
  if (view==='top')    return {x:cx-PL/2, y:2,        w:PL, h:PTH, axis:'x', min:C, max:W-C-PL};
  if (view==='left')   return {x:2,        y:cx-PLV/2, w:PTV, h:PLV, axis:'y', min:C, max:H-C-PLV};
  if (view==='right')  return {x:W-PTV-2,  y:cx-PLV/2, w:PTV, h:PLV, axis:'y', min:C, max:H-C-PLV};
}

function cPt(px, py, ax, ay, bx, by) {
  const dx = bx-ax, dy = by-ay, l2 = dx*dx+dy*dy;
  if (!l2) return { cx: ax, cy: ay };
  const t = Math.max(0, Math.min(1, ((px-ax)*dx+(py-ay)*dy)/l2));
  return { cx: ax+t*dx, cy: ay+t*dy };
}

function resolveChamfers(gs) {
  let hit=false;
  for(const s of CS){
    const{cx,cy}=cPt(gs.ball.x,gs.ball.y,s.ax,s.ay,s.bx,s.by);
    const d=Math.hypot(gs.ball.x-cx,gs.ball.y-cy);
    if(d<BR+1){
      let nx=gs.ball.x-cx,ny=gs.ball.y-cy;
      const l=Math.hypot(nx,ny);
      if(l<0.001){nx=s.nx;ny=s.ny;}else{nx/=l;ny/=l;}
      if(nx*s.nx+ny*s.ny<0){nx=-nx;ny=-ny;}
      const dot=gs.ball.vx*nx+gs.ball.vy*ny;
      if(dot<0){gs.ball.vx-=2*dot*nx;gs.ball.vy-=2*dot*ny;}
      gs.ball.x+=nx*(BR+1-d);gs.ball.y+=ny*(BR+1-d);
      const spd=Math.hypot(gs.ball.vx,gs.ball.vy);
      if(spd>SMAX){gs.ball.vx=gs.ball.vx/spd*SMAX;gs.ball.vy=gs.ball.vy/spd*SMAX;}
      hit=true;
    }
  }
  return hit;
}

function clampBall(gs) {
  for(const s of CS){
    const d=(gs.ball.x-s.ax)*s.nx+(gs.ball.y-s.ay)*s.ny;
    if(d<-BR){
      const dv=gs.ball.vx*s.nx+gs.ball.vy*s.ny;
      gs.ball.vx-=2*dv*s.nx;gs.ball.vy-=2*dv*s.ny;
      const{cx,cy}=cPt(gs.ball.x,gs.ball.y,s.ax,s.ay,s.bx,s.by);
      gs.ball.x=cx+s.nx*(BR+1);gs.ball.y=cy+s.ny*(BR+1);
    }
  }
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
  // Гарантуємо що обидві складові вектора ненульові
  const ang = (Math.random()*0.6+0.2)*Math.PI*(Math.random()<0.5?1:-1)+(Math.random()<0.5?0:Math.PI);
  const spd = 2.5 + Math.random()*0.5; // фіксована швидкість 2.5-3.0
  const vx = Math.cos(ang)*spd;
  const vy = Math.sin(ang)*spd;
  gs.ball = { x: W/2, y: H/2, vx: 0, vy: 0 };
  gs.respawn = { active: true, timer: RD, vx, vy };
}

const rooms = new Map();

function createRoom(id) {
  return {
    id,
    players: {},
    bots: {},
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
    gameOver: false,
    winner: null,
    tick: 0,
  };
  // Використовуємо ту саму функцію що і для респауну
  const _ang = (Math.random()*0.6+0.2)*Math.PI*(Math.random()<0.5?1:-1)+(Math.random()<0.5?0:Math.PI);
  const _spd = 2.5 + Math.random()*0.5;
  gs.respawn.vx = Math.cos(_ang)*_spd;
  gs.respawn.vy = Math.sin(_ang)*_spd;
  return gs;
}

function activeSlots(gs) { return SLOTS.filter(s => !gs.eliminated[s]); }

function tick(room) {
  const gs = room.game;
  if (!gs || gs.gameOver) return;
  gs.tick++;
  try {

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
    if (inp.left)  gs.paddles[s] = Math.max(mn, gs.paddles[s] - PS);
    if (inp.right) gs.paddles[s] = Math.min(mx, gs.paddles[s] + PS);
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
  } catch(e) {
    console.error('TICK ERROR:', e.message, e.stack?.split('\n')[1]);
  }
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
