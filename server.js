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
  const p = slotToPaddle(s, gs.paddles[s], gs, null);
  const fcx = p.x+p.w/2, fcy = p.y+p.h/2;
  const dx = ball.x-fcx, dy = ball.y-fcy;
  const dist = Math.hypot(dx,dy);
  const currentR = getFFRadius(f);
  if (currentR < 2) return false;
  const maxR = f.maxR || FR;
  if (dist > maxR + BR) return false;
  let nx, ny;
  if (dist > 0.5) { nx = dx/dist; ny = dy/dist; }
  else {
    const view = SLOT_VIEW[s];
    nx = view==='bottom'?0:view==='top'?0:view==='left'?1:-1;
    ny = view==='bottom'?-1:view==='top'?1:0;
  }
  const dot = ball.vx*nx + ball.vy*ny;
  if (dot > 0 && dist > currentR*0.5) return false;
  const speed = Math.min(Math.hypot(ball.vx,ball.vy)*BMULT, SMAX);
  ball.vx -= 2*dot*nx; ball.vy -= 2*dot*ny;
  const actual = Math.hypot(ball.vx, ball.vy);
  if (actual > 0.01) { ball.vx = ball.vx/actual*speed; ball.vy = ball.vy/actual*speed; }
  const pushR = Math.max(currentR, dist);
  ball.x = fcx + nx*(pushR + BR + 1);
  ball.y = fcy + ny*(pushR + BR + 1);
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

function fillBots(room) {
  room.bots = {};
  let bi = 0;
  const taken = Object.values(room.players).map(p => p.slot);
  // ── Не ставимо бота на слот відключеного гравця (він може повернутись) ──
  const disconnectedSlots = Object.keys(room._disconnected || {}).map(Number);
  for (const s of SLOTS) {
    if (!taken.includes(s) && !disconnectedSlots.includes(s)) {
      room.bots[s] = { nick: BOT_NAMES[bi++ % BOT_NAMES.length], rating: 490 + Math.floor(Math.random()*30) };
    }
  }
}

function buildPlayers(room) {
  const res = {};
  for (const s of SLOTS) {
    const p = Object.values(room.players).find(p => p.slot === s);
    if (p) {
      res[s] = { nick: p.nick, rating: p.rating, isBot: false, wins: p.wins||0, games: p.games||0 };
    } else if (room._disconnected && room._disconnected[s]) {
      // ── Відключений гравець — показуємо як гравця (не бота) але з позначкою ──
      res[s] = { nick: room._disconnected[s].nick, rating: room._disconnected[s].rating, isBot: false, disconnected: true, wins: 0, games: 0 };
    } else if (room.bots[s]) {
      res[s] = { nick: room.bots[s].nick, rating: room.bots[s].rating, isBot: true, wins: 0, games: 0 };
    }
  }
  return res;
}

function broadcastLobby(room) {
  io.to(room.id).emit('lobby:update', { players: buildPlayers(room) });
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
        gs.fields[s].active = true; gs.fields[s].t = 0; gs.fields[s].r = 0;
        gs.fields[s].maxR = pStats?.fr || FR;
        gs.energy[s] = Math.max(0, gs.energy[s] - EPU);
        inp.boost = false;
      }
    }
    // ── Відключені гравці — ракетка стоїть (input не надходить, нічого не робимо) ──

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
      if (!gs.fields[s].active && gs.energy[s] >= EPU) {
        const p = slotToPaddle(s, gs.paddles[s], gs, room);
        const _b0 = gs.balls[0] || {x:W/2,y:H/2};
        const dist = isHoriz ? Math.abs(_b0.y-(p.y+p.h/2)) : Math.abs(_b0.x-(p.x+p.w/2));
        if (dist < 80 && Math.random() < 0.02) {
          gs.fields[s].active = true; gs.fields[s].t = 0;
          gs.energy[s] = Math.max(0, gs.energy[s] - EPU);
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
      for (const s of SLOTS) {
        if (gs.eliminated[s]) continue;
        applyFFBall(gs, s, ball);
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
    p: [Math.round(gs.paddles[0]),Math.round(gs.paddles[1]),Math.round(gs.paddles[2]),Math.round(gs.paddles[3])],
    e: [Math.round(gs.energy[0]*100),Math.round(gs.energy[1]*100),Math.round(gs.energy[2]*100),Math.round(gs.energy[3]*100)],
    f: [
      gs.fields[0].active ? Math.round(getFFRadius(gs.fields[0])) : 0,
      gs.fields[1].active ? Math.round(getFFRadius(gs.fields[1])) : 0,
      gs.fields[2].active ? Math.round(getFFRadius(gs.fields[2])) : 0,
      gs.fields[3].active ? Math.round(getFFRadius(gs.fields[3])) : 0,
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

function endGame(room, winnerSlot) {
  const gs = room.game;
  gs.gameOver = true; gs.winner = winnerSlot;
  room.status = 'finished';
  if (room.tickInterval) { clearInterval(room.tickInterval); room.tickInterval = null; }
  // Скасовуємо всі таймери реконекту
  if (room._slotDeleteTimers) {
    for (const t of Object.values(room._slotDeleteTimers)) clearTimeout(t);
    room._slotDeleteTimers = {};
  }
  const RATING_BY_PLACE = [50, 20, 5, -20];
  const places = [winnerSlot];
  const others = SLOTS.filter(s => s !== winnerSlot).sort((a,b) => (gs.scores[b]||0) - (gs.scores[a]||0));
  places.push(...others);
  const ratingDeltas = {};
  places.forEach((slot, idx) => { ratingDeltas[slot] = RATING_BY_PLACE[idx] || -20; });
  const isTraining = Object.values(room.players).some(p => p.trainingMode);
  let trainingRewards = null;
  if(isTraining){
    const realPlayer = Object.values(room.players).find(p=>p.trainingMode);
    if(realPlayer) trainingRewards = getTrainingRewards(winnerSlot, realPlayer.slot);
  }
  io.to(room.id).emit('game:over', {
    winnerSlot, players: buildPlayers(room),
    ratingDeltas: isTraining ? {} : ratingDeltas,
    places, trainingRewards,
  });
  setTimeout(() => { rooms.delete(room.id); }, 30000);
}

function startGame(room) {
  room.status = 'playing';
  fillBots(room);
  room.game = createGameState(room);
  io.to(room.id).emit('game:start', { players: buildPlayers(room), mySlot: null });
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

io.on('connection', (socket) => {
  let myRoom = null, mySlot = null;

  socket.on('paddle:stats',({slot,paddleStats})=>{
    if(myRoom){
      for(const [sid,p] of Object.entries(myRoom.players)){
        if(p.slot===slot){ p.paddleStats=paddleStats; break; }
      }
    }
  });

  socket.on('mm:join', ({ nick, rating, uid, wins, games, paddleStats, trainingMode }) => {
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
    room.players[socket.id] = { slot: mySlot, nick, rating, uid, wins: wins||0, games: games||0, input: {},
      paddleStats: paddleStats || { spd:3.375, w:54, fr:54, bm:2.32, er:1.0, fd:1.0 } };
    socket.join(room.id);
    socket.emit('mm:joined', { mySlot, roomId: room.id });
    broadcastLobby(room);
    const count = Object.keys(room.players).length;
    if (count === 1) { fillBots(room); broadcastLobby(room); startCountdown(room); }
    else if (count >= 4) { if (room.countdownTimer) { clearInterval(room.countdownTimer); room.countdownTimer = null; } startGame(room); }
    else { fillBots(room); broadcastLobby(room); startCountdown(room); }
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
      socket.emit('rejoin:game', {
        mySlot: slot, players: buildPlayers(room),
        gameOver: gs.gameOver, winner: gs.winner,
        lives: gs.lives, scores: gs.scores,
      });
      socket.emit('myslot', { mySlot: slot });
    } else if (room.status === 'waiting' || room.status === 'countdown') {
      socket.emit('rejoin:lobby', { mySlot: slot });
      broadcastLobby(room);
    } else {
      socket.emit('rejoin:fail', { reason: 'unknown_status' });
    }
  });

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
