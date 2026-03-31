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
const W = 520, H = 520, BR = 8, SMAX = 4.875, C = 130;
const PL = 54, PLV = 54, PTH = 16, PTV = 16;
const ML = 10, EPU = 1 / 3, ECR = 1 / 10000;
const FDR = 380, BMULT = 2.32, FR = 54, RD = 2000;
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

function spawnBallQueued(gs) {
  const ang=(Math.random()*0.6+0.2)*Math.PI*(Math.random()<0.5?1:-1)+(Math.random()<0.5?0:Math.PI);
  const spd=2.5+Math.random()*0.5;
  gs.respawns.push({ timer: RD, vx: Math.cos(ang)*spd, vy: Math.sin(ang)*spd });
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
    if (p) res[s] = { nick: p.nick, rating: p.rating, isBot: false, wins: p.wins||0, games: p.games||0 };
    else if (room.bots[s]) res[s] = { nick: room.bots[s].nick, rating: room.bots[s].rating, isBot: true, wins: 0, games: 0 };
  }
  return res;
}

function broadcastLobby(room) {
  io.to(room.id).emit('lobby:update', { players: buildPlayers(room) });
}

function createGameState(room) {
  const gs = {
    balls: [],       // активні м'ячі
    respawns: [],    // черга респаунів [{timer, vx, vy}]
    paddles: { 0: W/2, 1: W/2, 2: H/2, 3: H/2 }, // центр ракетки
    lives: { 0: ML, 1: ML, 2: ML, 3: ML },
    scores: { 0: 0, 1: 0, 2: 0, 3: 0 },
    energy: { 0: 1, 1: 1, 2: 1, 3: 1 },
    fields: { 0:{active:false,t:0,r:0}, 1:{active:false,t:0,r:0}, 2:{active:false,t:0,r:0}, 3:{active:false,t:0,r:0} },
    eliminated: { 0: false, 1: false, 2: false, 3: false },
    botTargets: { 0: W/2, 1: W/2, 2: H/2, 3: H/2 },
    gameOver: false,
    winner: null,
    tick: 0,
  };
  // Використовуємо ту саму функцію що і для респауну
  // М'ячі стартують після pregame (5 сек) + затримка між ними
  const PREGAME_DELAY = 5000;
  for(let i=0;i<4;i++){
    const ang=(Math.random()*0.6+0.2)*Math.PI*(Math.random()<0.5?1:-1)+(Math.random()<0.5?0:Math.PI);
    const spd=2.5+Math.random()*0.5;
    gs.respawns.push({ timer: PREGAME_DELAY + i*600, vx: Math.cos(ang)*spd, vy: Math.sin(ang)*spd });
  }
  return gs;
}

function activeSlots(gs) { return SLOTS.filter(s => !gs.eliminated[s]); }

function hitRect(ball, p) {
  return ball.x+BR > p.x && ball.x-BR < p.x+p.w &&
         ball.y+BR > p.y && ball.y-BR < p.y+p.h;
}

function getFFRadius(f) {
  // Радіус розширюється від 0 до FR за час FDR
  return Math.min(FR, (f.t / FDR) * FR);
}

function applyFF(gs, s) {
  const f = gs.fields[s];
  if (!f || !f.active) return false;
  const p = slotToPaddle(s, gs.paddles[s]);
  const fcx = p.x + p.w/2, fcy = p.y + p.h/2;
  const dx = gs.ball.x - fcx, dy = gs.ball.y - fcy;
  const dist = Math.hypot(dx, dy);
  const currentR = getFFRadius(f);
  if (dist > currentR + BR) return false;

  const view = SLOT_VIEW[s];
  let nx = 0, ny = 0;
  let sideOffset = 0;
  if (view === 'bottom') { ny = -1; sideOffset = (gs.ball.x - fcx) / (p.w/2); }
  else if (view === 'top')    { ny =  1; sideOffset = (gs.ball.x - fcx) / (p.w/2); }
  else if (view === 'left')   { nx =  1; sideOffset = (gs.ball.y - fcy) / (p.h/2); }
  else if (view === 'right')  { nx = -1; sideOffset = (gs.ball.y - fcy) / (p.h/2); }
  sideOffset = Math.max(-0.8, Math.min(0.8, sideOffset));

  const speed = Math.min(Math.hypot(gs.ball.vx, gs.ball.vy) * BMULT, SMAX);
  if (view === 'bottom' || view === 'top') {
    gs.ball.vy = ny * Math.abs(speed) * 0.85;
    gs.ball.vx = sideOffset * speed * 0.8;
  } else {
    gs.ball.vx = nx * Math.abs(speed) * 0.85;
    gs.ball.vy = sideOffset * speed * 0.8;
  }
  const actual = Math.hypot(gs.ball.vx, gs.ball.vy);
  if (actual > 0.01) { gs.ball.vx = gs.ball.vx/actual*speed; gs.ball.vy = gs.ball.vy/actual*speed; }
  gs.ball.x = fcx + nx*(currentR + BR + 2);
  gs.ball.y = fcy + ny*(currentR + BR + 2);
  f.active = false; f.t = 0;
  return true;
}

// Версії фізичних функцій для окремого ball об'єкта
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

function applyFFBall(gs, s, ball) {
  const f = gs.fields[s];
  if (!f || !f.active) return false;
  const p = slotToPaddle(s, gs.paddles[s]);
  const fcx = p.x+p.w/2, fcy = p.y+p.h/2;
  const dx = ball.x-fcx, dy = ball.y-fcy;
  const dist = Math.hypot(dx,dy);
  const currentR = getFFRadius(f);
  if (currentR < 2 || dist > currentR + BR) return false;
  const view = SLOT_VIEW[s];
  let nx=0,ny=0,sideOffset=0;
  if(view==='bottom'){ny=-1;sideOffset=(ball.x-fcx)/(p.w/2);}
  else if(view==='top'){ny=1;sideOffset=(ball.x-fcx)/(p.w/2);}
  else if(view==='left'){nx=1;sideOffset=(ball.y-fcy)/(p.h/2);}
  else{nx=-1;sideOffset=(ball.y-fcy)/(p.h/2);}
  sideOffset=Math.max(-0.8,Math.min(0.8,sideOffset));
  const speed=Math.min(Math.hypot(ball.vx,ball.vy)*BMULT,SMAX);
  if(view==='bottom'||view==='top'){ball.vy=ny*Math.abs(speed)*0.85;ball.vx=sideOffset*speed*0.8;}
  else{ball.vx=nx*Math.abs(speed)*0.85;ball.vy=sideOffset*speed*0.8;}
  const actual=Math.hypot(ball.vx,ball.vy);
  if(actual>0.01){ball.vx=ball.vx/actual*speed;ball.vy=ball.vy/actual*speed;}
  ball.x=fcx+nx*(currentR+BR+2);ball.y=fcy+ny*(currentR+BR+2);
  return true;
}

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
      // Плавне розширення від 0 до FR за перші 200мс
      const expandTime = 200;
      gs.fields[s].r = Math.min(FR, (gs.fields[s].t / expandTime) * FR);
      if (gs.fields[s].t >= FDR) { gs.fields[s].active = false; gs.fields[s].t = 0; gs.fields[s].r = 0; }
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
      gs.fields[s].active = true; gs.fields[s].t = 0; gs.fields[s].r = 0;
      gs.energy[s] = Math.max(0, gs.energy[s] - EPU);
      inp.boost = false;
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
    const ball0 = gs.balls[0] || {x:W/2,y:H/2,vx:0,vy:0};
    const pred = isHoriz ? predBall(ball0,'x',wallPos) : predBall(ball0,'y',wallPos);
    gs.botTargets[s] += (pred - gs.botTargets[s]) * 0.08;
    const diff = gs.botTargets[s] - gs.paddles[s];
    if (Math.abs(diff) > 2) gs.paddles[s] = Math.max(mn, Math.min(mx, gs.paddles[s] + Math.sign(diff)*Math.min(3.5,Math.abs(diff))));
    // Бот активує поле
    if (!gs.fields[s].active && gs.energy[s] >= EPU) {
      const p = slotToPaddle(s, gs.paddles[s]);
      const _b0 = gs.balls[0] || {x:W/2,y:H/2};
      const dist = isHoriz ? Math.abs(_b0.y-(p.y+p.h/2)) : Math.abs(_b0.x-(p.x+p.w/2));
      if (dist < 80 && Math.random() < 0.02) {
        gs.fields[s].active = true; gs.fields[s].t = 0;
        gs.energy[s] = Math.max(0, gs.energy[s] - EPU);
      }
    }
  }

  // ── Respawns → balls ──
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
      const active = activeSlots(gs);
      if (active.length === 1) { endGame(room, active[0]); return true; }
    }
    return false;
  };

  // Фізика кожного м'яча
  for (let bi = gs.balls.length - 1; bi >= 0; bi--) {
    const ball = gs.balls[bi];
    ball.x += ball.vx; ball.y += ball.vy;

    // Силові поля
    let removed = false;
    for (const s of SLOTS) {
      if (gs.eliminated[s]) continue;
      if (applyFFBall(gs, s, ball)) { removed = false; break; }
    }

    // Кути
    resolveChamfersBall(ball);
    clampBallObj(ball);

    // Ракетки
    let hit = false;
    for (const s of SLOTS) {
      if (gs.eliminated[s]) continue;
      const p = slotToPaddle(s, gs.paddles[s]);
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
        hit = true; break;
      }
    }
    if (hit) continue;

    // Голи
    let scored = false;
    const by = ball.y, bx2 = ball.x;
    if (by-BR < 0 && bx2 > C && bx2 < W-C) {
      if (gs.eliminated[1]) { ball.vy = Math.abs(ball.vy); } // відбиваємо
      else { if (!goal(1)) { spawnBallQueued(gs); } gs.balls.splice(bi,1); scored=true; }
    } else if (by+BR > H && bx2 > C && bx2 < W-C) {
      if (gs.eliminated[0]) { ball.vy = -Math.abs(ball.vy); }
      else { if (!goal(0)) { spawnBallQueued(gs); } gs.balls.splice(bi,1); scored=true; }
    } else if (bx2-BR < 0 && by > C && by < H-C) {
      if (gs.eliminated[2]) { ball.vx = Math.abs(ball.vx); }
      else { if (!goal(2)) { spawnBallQueued(gs); } gs.balls.splice(bi,1); scored=true; }
    } else if (bx2+BR > W && by > C && by < H-C) {
      if (gs.eliminated[3]) { ball.vx = -Math.abs(ball.vx); }
      else { if (!goal(3)) { spawnBallQueued(gs); } gs.balls.splice(bi,1); scored=true; }
    }
    if (scored && gs.gameOver) { broadcastState(room); return; }
  }

  broadcastState(room);
  } catch(e) {
    console.error('TICK ERROR:', e.message, e.stack?.split('\n')[1]);
  }
}

function broadcastState(room) {
  const gs = room.game;
  if (!gs) return;
  io.to(room.id).emit('gs', {
    balls: gs.balls.map(b=>({x:Math.round(b.x*10)/10,y:Math.round(b.y*10)/10,vx:Math.round(b.vx*100)/100,vy:Math.round(b.vy*100)/100,id:b.id})),
    respawns: gs.respawns.map(r=>({timer:Math.round(r.timer),vx:r.vx,vy:r.vy})),
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
    f: [ // fields — поточний радіус (0 = неактивне)
      gs.fields[0].active ? Math.round(getFFRadius(gs.fields[0])) : 0,
      gs.fields[1].active ? Math.round(getFFRadius(gs.fields[1])) : 0,
      gs.fields[2].active ? Math.round(getFFRadius(gs.fields[2])) : 0,
      gs.fields[3].active ? Math.round(getFFRadius(gs.fields[3])) : 0,
    ],
    el: [gs.eliminated[0]?1:0,gs.eliminated[1]?1:0,gs.eliminated[2]?1:0,gs.eliminated[3]?1:0],
    lv: [gs.lives[0],gs.lives[1],gs.lives[2],gs.lives[3]],
    sc: [gs.scores[0],gs.scores[1],gs.scores[2],gs.scores[3]],
  });
}

function endGame(room, winnerSlot) {
  const gs = room.game;
  gs.gameOver = true; gs.winner = winnerSlot;
  room.status = 'finished';
  if (room.tickInterval) { clearInterval(room.tickInterval); room.tickInterval = null; }

  // Визначаємо місця: 1-ше=winner, решта за кількістю очок (scores)
  const RATING_BY_PLACE = [50, 20, 5, -20]; // 1,2,3,4 місця
  const places = [winnerSlot]; // 1-ше місце
  // Решта сортуємо за scores DESC
  const others = SLOTS.filter(s => s !== winnerSlot)
    .sort((a,b) => (gs.scores[b]||0) - (gs.scores[a]||0));
  places.push(...others); // places[0]=1ше, [1]=2ге, [2]=3тє, [3]=4те

  const ratingDeltas = {};
  places.forEach((slot, idx) => {
    ratingDeltas[slot] = RATING_BY_PLACE[idx] || -20;
  });

  io.to(room.id).emit('game:over', {
    winnerSlot,
    players: buildPlayers(room),
    ratingDeltas, // { slot: delta }
    places,       // [slot1st, slot2nd, slot3rd, slot4th]
  });

  // Кімната закривається через 30с після кінця гри
  setTimeout(() => { rooms.delete(room.id); }, 30000);
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
    io.to(sid).emit('myslot', { mySlot: player.slot, roomId: room.id });
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

  socket.on('mm:join', ({ nick, rating, uid, wins, games }) => {
    const room = findOrCreateRoom();
    myRoom = room;
    mySlot = getAvailableSlot(room);
    // Якщо вільних слотів немає але є боти — витісняємо бота
    if (mySlot === undefined) {
      const botSlot = SLOTS.find(s => room.bots[s] && !Object.values(room.players).some(p=>p.slot===s));
      if (botSlot !== undefined) {
        delete room.bots[botSlot];
        mySlot = botSlot;
      } else {
        socket.emit('mm:error', 'Кімната повна'); return;
      }
    }

    room.players[socket.id] = { slot: mySlot, nick, rating, uid, wins: wins||0, games: games||0, input: {} };
    socket.join(room.id);
    socket.emit('mm:joined', { mySlot, roomId: room.id });
    broadcastLobby(room);

    const count = Object.keys(room.players).length;
    if (count === 1) {
      // Одразу заповнюємо вільні слоти ботами і запускаємо таймер
      fillBots(room);
      broadcastLobby(room);
      startCountdown(room);
    } else if (count >= 4) {
      if (room.countdownTimer) { clearInterval(room.countdownTimer); room.countdownTimer = null; }
      startGame(room);
    } else {
      // Ще один реальний гравець — оновлюємо лобі і скидаємо таймер
      fillBots(room); // оновлюємо ботів (деяких могло витіснити)
      broadcastLobby(room);
      startCountdown(room); // скидаємо таймер
    }
  });

  // Реконект — гравець повертається в кімнату
  socket.on('rejoin', ({ roomId, slot, nick, rating, uid }) => {
    const room = rooms.get(roomId);
    if (!room) { socket.emit('rejoin:fail', { reason: 'room_gone' }); return; }

    // Перевіряємо що цей слот справді відключений (не зайнятий іншим гравцем)
    const alreadyTaken = Object.values(room.players).some(p => p.slot === slot);
    if (alreadyTaken) { socket.emit('rejoin:fail', { reason: 'slot_taken' }); return; }

    // Очищаємо _disconnected запис
    if (room._disconnected) delete room._disconnected[slot];

    // Кімната існує — відновлюємо гравця
    myRoom = room;
    mySlot = slot;

    // Скасовуємо таймер видалення кімнати
    if (room._deleteTimer) { clearTimeout(room._deleteTimer); room._deleteTimer = null; }
    if (room._disconnected) delete room._disconnected[slot];
    // Замінюємо бота назад на гравця
    delete room.bots[slot];
    room.players[socket.id] = { slot, nick, rating, uid, input: {} };
    socket.join(room.id);

    if (room.status === 'playing' && room.game) {
      // Гра іде — повертаємо в гру
      const gs = room.game;
      socket.emit('rejoin:game', {
        mySlot: slot,
        players: buildPlayers(room),
        gameOver: gs.gameOver,
        winner: gs.winner,
        lives: gs.lives,
        scores: gs.scores,
      });
      socket.emit('myslot', { mySlot: slot });
    } else if (room.status === 'waiting' || room.status === 'countdown') {
      // Ще в лобі
      socket.emit('rejoin:lobby', { mySlot: slot });
      broadcastLobby(room);
    } else {
      socket.emit('rejoin:fail', { reason: 'unknown_status' });
    }
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
    const room = myRoom;
    const slot = mySlot;
    const pinfo = room.players[socket.id];
    delete room.players[socket.id];
    socket.leave(room.id);
    myRoom = null; mySlot = null;

    const count = Object.keys(room.players).length;

    if (room.status === 'playing' && slot !== null) {
      // Під час гри — замінюємо на бота і чекаємо реконект 30 сек
      // Нараховуємо штраф рейтингу за вихід (як за останнє місце)
      io.to(room.id).emit('player:left', { slot, ratingDelta: -5 });

      // Зберігаємо для реконекту
      room._disconnected = room._disconnected || {};
      room._disconnected[slot] = { nick: pinfo?.nick, rating: pinfo?.rating, uid: pinfo?.uid };

      // Таймер 30с — якщо не повернувся, видаляємо кімнату
      if (room._deleteTimer) clearTimeout(room._deleteTimer);
      room._deleteTimer = setTimeout(() => {
        console.log(`Room ${room.id}: timeout, deleting`);
        if (room.tickInterval) clearInterval(room.tickInterval);
        if (room.countdownTimer) clearInterval(room.countdownTimer);
        // Сповіщаємо всіх хто ще є
        io.to(room.id).emit('room:closed');
        rooms.delete(room.id);
      }, 30000);
    } else if (count === 0) {
      // Не в грі і нікого немає — видаляємо одразу
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
