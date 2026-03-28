const { createServer } = require('http');
const { Server } = require('socket.io');

const httpServer = createServer();
const io = new Server(httpServer, {
  cors: { origin: '*', methods: ['GET', 'POST'] }
});

const PORT = process.env.PORT || 3000;

// ── КОНСТАНТИ ──
const W = 520, H = 520, BALL_R = 8, SPEED_MAX = 13, C = 88;
const PL = 54, PLV = 54, PTH = 16, PTV = 16;
const ML = 10;
const EPU = 1 / 3, ECR = 1 / 10000;
const FIELD_DURATION = 380, BOOST_MULT = 1.55, FR = 36;
const RESPAWN_DELAY = 2000;
const TICK_RATE = 20; // 20 разів/сек

const BOT_NAMES = ['ZEPHYR', 'GLITCH', 'NOVA', 'STORM', 'BLAZE', 'PIXEL'];
const COLORS = { top: '#4aaeff', bottom: '#ff8844', left: '#44dd44', right: '#ff44aa' };
const POSITIONS = ['bottom', 'top', 'left', 'right'];

const CHAMFER_SEGS = [
  { ax: 0, ay: C, bx: C, by: 0, nx: 1 / Math.SQRT2, ny: 1 / Math.SQRT2 },
  { ax: W - C, ay: 0, bx: W, by: C, nx: -1 / Math.SQRT2, ny: 1 / Math.SQRT2 },
  { ax: 0, ay: H - C, bx: C, by: H, nx: 1 / Math.SQRT2, ny: -1 / Math.SQRT2 },
  { ax: W - C, ay: H, bx: W, by: H - C, nx: -1 / Math.SQRT2, ny: -1 / Math.SQRT2 },
];

// ── КІМНАТИ ──
const rooms = new Map();

function createRoom(roomId) {
  const room = {
    id: roomId,
    players: {}, // socketId -> { pos, nick, rating, uid }
    bots: {},    // pos -> { nick, rating }
    status: 'waiting', // waiting | countdown | playing | finished
    countdownTimer: null,
    tickInterval: null,
    game: null,
  };
  rooms.set(roomId, room);
  return room;
}

function findOrCreateRoom() {
  for (const [id, room] of rooms) {
    if (room.status === 'waiting' && Object.keys(room.players).length < 4) {
      return room;
    }
  }
  const id = 'room_' + Math.random().toString(36).slice(2, 8);
  return createRoom(id);
}

function getRoomPlayers(room) {
  return Object.values(room.players);
}

function getAvailablePosition(room) {
  const taken = getRoomPlayers(room).map(p => p.pos);
  return POSITIONS.find(p => !taken.includes(p));
}

function fillBotsForRoom(room) {
  room.bots = {};
  let bi = 0;
  for (const pos of POSITIONS) {
    const taken = getRoomPlayers(room).map(p => p.pos);
    if (!taken.includes(pos)) {
      room.bots[pos] = { nick: BOT_NAMES[bi % BOT_NAMES.length], rating: 490 + Math.floor(Math.random() * 30) };
      bi++;
    }
  }
}

function broadcastLobby(room) {
  const slots = buildSlots(room);
  io.to(room.id).emit('lobby:update', { slots, status: room.status });
}

function buildSlots(room) {
  const slots = {};
  for (const pos of POSITIONS) {
    const player = getRoomPlayers(room).find(p => p.pos === pos);
    if (player) {
      slots[pos] = { nick: player.nick, rating: player.rating, isBot: false };
    } else if (room.bots[pos]) {
      slots[pos] = { nick: room.bots[pos].nick, rating: room.bots[pos].rating, isBot: true };
    }
  }
  return slots;
}

// ── ФІЗИКА (серверна) ──

function closestPt(px, py, ax, ay, bx, by) {
  const dx = bx - ax, dy = by - ay, l2 = dx * dx + dy * dy;
  if (!l2) return { cx: ax, cy: ay };
  const t = Math.max(0, Math.min(1, ((px - ax) * dx + (py - ay) * dy) / l2));
  return { cx: ax + t * dx, cy: ay + t * dy };
}

function makeBallVelocity() {
  let vx, vy, att = 0;
  do {
    const a = (Math.random() * 0.7 + 0.15) * Math.PI * (Math.random() < 0.5 ? 1 : -1) + (Math.random() < 0.5 ? 0 : Math.PI);
    vx = Math.cos(a) * (3.5 + Math.random() * 1.5);
    vy = Math.sin(a) * (3.5 + Math.random() * 1.5);
    att++;
  } while ((Math.abs(vx) < 1.8 || Math.abs(vy) < 1.8) && att < 30);
  return { vx, vy };
}

function createGameState(room) {
  const state = {
    ball: { x: W / 2, y: H / 2, vx: 0, vy: 0 },
    respawn: { active: true, timer: RESPAWN_DELAY, vx: 0, vy: 0 },
    paddles: {},
    lives: {},
    scores: {},
    energy: {},
    fields: {},
    eliminated: {},
    botTargets: {},
    gameOver: false,
    winner: null,
  };

  const { vx, vy } = makeBallVelocity();
  state.respawn.vx = vx;
  state.respawn.vy = vy;

  for (const pos of POSITIONS) {
    const isHoriz = pos === 'top' || pos === 'bottom';
    state.paddles[pos] = {
      x: isHoriz ? W / 2 - PL / 2 : (pos === 'left' ? 2 : W - PTV - 2),
      y: isHoriz ? (pos === 'top' ? 2 : H - PTH - 2) : H / 2 - PLV / 2,
      w: isHoriz ? PL : PTV,
      h: isHoriz ? PTH : PLV,
      axis: isHoriz ? 'x' : 'y',
      minPos: C,
      maxPos: isHoriz ? W - C - PL : H - C - PLV,
    };
    state.lives[pos] = ML;
    state.scores[pos] = 0;
    state.energy[pos] = 1.0;
    state.fields[pos] = { active: false, timer: 0 };
    state.eliminated[pos] = false;
    state.botTargets[pos] = isHoriz ? W / 2 : H / 2;
  }

  return state;
}

function spawnBall(gs) {
  const { vx, vy } = makeBallVelocity();
  gs.ball = { x: W / 2, y: H / 2, vx: 0, vy: 0 };
  gs.respawn = { active: true, timer: RESPAWN_DELAY, vx, vy };
}

function predictBall(gs, axis, wallPos) {
  let bx = gs.ball.x, by = gs.ball.y, vx = gs.ball.vx, vy = gs.ball.vy;
  for (let i = 0; i < 300; i++) {
    bx += vx; by += vy;
    if (bx - BALL_R < 0) { bx = BALL_R; vx = Math.abs(vx); }
    if (bx + BALL_R > W) { bx = W - BALL_R; vx = -Math.abs(vx); }
    if (by - BALL_R < 0) { by = BALL_R; vy = Math.abs(vy); }
    if (by + BALL_R > H) { by = H - BALL_R; vy = -Math.abs(vy); }
    if (axis === 'x' && Math.abs(by - wallPos) < Math.abs(vy) + 1) return bx;
    if (axis === 'y' && Math.abs(bx - wallPos) < Math.abs(vx) + 1) return by;
  }
  return axis === 'x' ? bx : by;
}

function resolveChamfers(gs) {
  let resolved = false;
  for (const s of CHAMFER_SEGS) {
    const { cx, cy } = closestPt(gs.ball.x, gs.ball.y, s.ax, s.ay, s.bx, s.by);
    const dist = Math.hypot(gs.ball.x - cx, gs.ball.y - cy);
    if (dist < BALL_R + 1) {
      let nx = gs.ball.x - cx, ny = gs.ball.y - cy;
      const l = Math.hypot(nx, ny);
      if (l < 0.001) { nx = s.nx; ny = s.ny; } else { nx /= l; ny /= l; }
      if (nx * s.nx + ny * s.ny < 0) { nx = -nx; ny = -ny; }
      const dot = gs.ball.vx * nx + gs.ball.vy * ny;
      if (dot < 0) { gs.ball.vx -= 2 * dot * nx; gs.ball.vy -= 2 * dot * ny; }
      gs.ball.x += nx * (BALL_R + 1 - dist);
      gs.ball.y += ny * (BALL_R + 1 - dist);
      const spd = Math.hypot(gs.ball.vx, gs.ball.vy);
      if (spd > SPEED_MAX) { gs.ball.vx = gs.ball.vx / spd * SPEED_MAX; gs.ball.vy = gs.ball.vy / spd * SPEED_MAX; }
      resolved = true;
    }
  }
  return resolved;
}

function clampBall(gs) {
  for (const s of CHAMFER_SEGS) {
    const dot = (gs.ball.x - s.ax) * s.nx + (gs.ball.y - s.ay) * s.ny;
    if (dot < -BALL_R) {
      const dv = gs.ball.vx * s.nx + gs.ball.vy * s.ny;
      gs.ball.vx -= 2 * dv * s.nx; gs.ball.vy -= 2 * dv * s.ny;
      const { cx, cy } = closestPt(gs.ball.x, gs.ball.y, s.ax, s.ay, s.bx, s.by);
      gs.ball.x = cx + s.nx * (BALL_R + 1);
      gs.ball.y = cy + s.ny * (BALL_R + 1);
    }
  }
}

function hitRect(ball, p) {
  return ball.x + BALL_R > p.x && ball.x - BALL_R < p.x + p.w &&
    ball.y + BALL_R > p.y && ball.y - BALL_R < p.y + p.h;
}

function addSpin(gs, pos) {
  const p = gs.paddles[pos];
  if (p.axis === 'x') {
    const r = (gs.ball.x - (p.x + p.w / 2)) / (p.w / 2);
    gs.ball.vx += r * 2.5;
  } else {
    const r = (gs.ball.y - (p.y + p.h / 2)) / (p.h / 2);
    gs.ball.vy += r * 2.5;
  }
  const sp = Math.hypot(gs.ball.vx, gs.ball.vy);
  const ns = Math.min(sp * 1.04, SPEED_MAX * 0.75);
  gs.ball.vx = gs.ball.vx / sp * ns;
  gs.ball.vy = gs.ball.vy / sp * ns;
}

function applyForceField(gs, pos) {
  const f = gs.fields[pos];
  if (!f.active) return false;
  const p = gs.paddles[pos];
  const fcx = p.x + p.w / 2, fcy = p.y + p.h / 2;
  const dx = gs.ball.x - fcx, dy = gs.ball.y - fcy;
  const dist = Math.hypot(dx, dy);
  if (dist > FR + BALL_R) return false;
  const nx = dist > 0.001 ? dx / dist : 0;
  const ny = dist > 0.001 ? dy / dist : 1;
  const dot = gs.ball.vx * nx + gs.ball.vy * ny;
  gs.ball.vx -= 2 * dot * nx; gs.ball.vy -= 2 * dot * ny;
  const spd = Math.hypot(gs.ball.vx, gs.ball.vy);
  const ns = Math.min(spd * BOOST_MULT, SPEED_MAX);
  gs.ball.vx = gs.ball.vx / spd * ns; gs.ball.vy = gs.ball.vy / spd * ns;
  gs.ball.x = fcx + nx * (FR + BALL_R + 2);
  gs.ball.y = fcy + ny * (FR + BALL_R + 2);
  f.active = false; f.timer = 0;
  return true;
}

function activePlayers(gs) {
  return POSITIONS.filter(p => !gs.eliminated[p]);
}

function eliminatePlayer(room, gs, pos) {
  gs.eliminated[pos] = true;
  io.to(room.id).emit('game:eliminated', { pos });
  const active = activePlayers(gs);
  if (active.length === 1) {
    endGame(room, gs, active[0]);
  }
}

function endGame(room, gs, winner) {
  gs.gameOver = true;
  gs.winner = winner;
  if (room.tickInterval) { clearInterval(room.tickInterval); room.tickInterval = null; }
  room.status = 'finished';
  const slots = buildSlots(room);
  io.to(room.id).emit('game:over', { winner, slots });
}

function updateBots(room, gs, dt) {
  const BOT_SPEED = 3.5;
  const DEADZONE = 3;
  for (const pos of POSITIONS) {
    const taken = getRoomPlayers(room).map(p => p.pos);
    if (taken.includes(pos)) continue; // живий гравець
    if (gs.eliminated[pos]) continue;
    const p = gs.paddles[pos];
    // Активація силового поля
    if (!gs.fields[pos].active && gs.energy[pos] >= EPU) {
      const dist = p.axis === 'x' ? Math.abs(gs.ball.y - (p.y + p.h / 2)) : Math.abs(gs.ball.x - (p.x + p.w / 2));
      if (dist < 80 && Math.random() < 0.04) {
        gs.fields[pos].active = true;
        gs.fields[pos].timer = 0;
        gs.energy[pos] = Math.max(0, gs.energy[pos] - EPU);
      }
    }
    // Рух до передбаченої позиції м'яча
    const pred = p.axis === 'x'
      ? predictBall(gs, 'x', p.y + p.h / 2)
      : predictBall(gs, 'y', p.x + p.w / 2);
    gs.botTargets[pos] += (pred - gs.botTargets[pos]) * 0.08;
    const center = p.axis === 'x' ? p.x + p.w / 2 : p.y + p.h / 2;
    const diff = gs.botTargets[pos] - center;
    if (Math.abs(diff) > DEADZONE) {
      const step = Math.sign(diff) * Math.min(BOT_SPEED, Math.abs(diff));
      if (p.axis === 'x') p.x = Math.max(p.minPos, Math.min(p.maxPos, p.x + step));
      else p.y = Math.max(p.minPos, Math.min(p.maxPos, p.y + step));
    }
  }
}

function tickGame(room, dt) {
  const gs = room.game;
  if (!gs || gs.gameOver) return;

  // Поля та енергія
  for (const pos of POSITIONS) {
    if (gs.eliminated[pos]) continue;
    if (gs.fields[pos].active) {
      gs.fields[pos].timer += dt;
      if (gs.fields[pos].timer >= FIELD_DURATION) {
        gs.fields[pos].active = false;
        gs.fields[pos].timer = 0;
      }
    }
    if (!gs.fields[pos].active) {
      gs.energy[pos] = Math.min(1, gs.energy[pos] + ECR * dt);
    }
  }

  // Боти
  updateBots(room, gs, dt);

  // Respawn countdown
  if (gs.respawn.active) {
    gs.respawn.timer -= dt;
    if (gs.respawn.timer <= 0) {
      gs.respawn.active = false;
      gs.ball.vx = gs.respawn.vx;
      gs.ball.vy = gs.respawn.vy;
    }
    broadcastGameState(room);
    return;
  }

  // М'яч
  gs.ball.x += gs.ball.vx;
  gs.ball.y += gs.ball.vy;

  // Силові поля
  for (const pos of POSITIONS) {
    if (gs.eliminated[pos]) continue;
    if (applyForceField(gs, pos)) { broadcastGameState(room); return; }
  }

  // Кути
  for (let i = 0; i < 3; i++) if (resolveChamfers(gs)) break;
  clampBall(gs);

  // Ракетки
  for (const pos of POSITIONS) {
    if (gs.eliminated[pos]) continue;
    const p = gs.paddles[pos];
    if (hitRect(gs.ball, p)) {
      if (p.axis === 'x') {
        gs.ball.y = pos === 'top' ? p.y + p.h + BALL_R : p.y - BALL_R;
        gs.ball.vy = pos === 'top' ? Math.abs(gs.ball.vy) : -Math.abs(gs.ball.vy);
      } else {
        gs.ball.x = pos === 'left' ? p.x + p.w + BALL_R : p.x - BALL_R;
        gs.ball.vx = pos === 'left' ? Math.abs(gs.ball.vx) : -Math.abs(gs.ball.vx);
      }
      addSpin(gs, pos);
      broadcastGameState(room);
      return;
    }
  }

  // Голи
  const goalFor = (pos) => {
    gs.scores[pos]++;
    gs.lives[pos]--;
    io.to(room.id).emit('game:goal', { pos, lives: gs.lives[pos], scores: gs.scores[pos] });
    if (gs.lives[pos] <= 0) eliminatePlayer(room, gs, pos);
    else if (!gs.gameOver) spawnBall(gs);
  };

  if (gs.ball.y - BALL_R < 0 && gs.ball.x > C && gs.ball.x < W - C) {
    if (!gs.eliminated.top) goalFor('top'); else gs.ball.vy = Math.abs(gs.ball.vy);
  } else if (gs.ball.y + BALL_R > H && gs.ball.x > C && gs.ball.x < W - C) {
    if (!gs.eliminated.bottom) goalFor('bottom'); else gs.ball.vy = -Math.abs(gs.ball.vy);
  } else if (gs.ball.x - BALL_R < 0 && gs.ball.y > C && gs.ball.y < H - C) {
    if (!gs.eliminated.left) goalFor('left'); else gs.ball.vx = Math.abs(gs.ball.vx);
  } else if (gs.ball.x + BALL_R > W && gs.ball.y > C && gs.ball.y < H - C) {
    if (!gs.eliminated.right) goalFor('right'); else gs.ball.vx = -Math.abs(gs.ball.vx);
  }

  broadcastGameState(room);
}

function broadcastGameState(room) {
  const gs = room.game;
  if (!gs) return;
  io.to(room.id).emit('game:state', {
    ball: gs.ball,
    respawn: gs.respawn,
    paddles: gs.paddles,
    energy: gs.energy,
    fields: gs.fields,
    lives: gs.lives,
    scores: gs.scores,
    eliminated: gs.eliminated,
  });
}

function startGame(room) {
  room.status = 'playing';
  fillBotsForRoom(room);
  room.game = createGameState(room);
  broadcastLobby(room);
  io.to(room.id).emit('game:start', { slots: buildSlots(room) });

  let lastTick = Date.now();
  room.tickInterval = setInterval(() => {
    const now = Date.now();
    const dt = now - lastTick;
    lastTick = now;
    tickGame(room, dt);
  }, 1000 / TICK_RATE);
}

// ── SOCKET.IO ──
io.on('connection', (socket) => {
  console.log('Player connected:', socket.id);
  let myRoom = null;
  let myPos = null;

  socket.on('mm:join', ({ nick, rating, uid }) => {
    const room = findOrCreateRoom();
    myRoom = room;
    myPos = getAvailablePosition(room);
    if (!myPos) { socket.emit('mm:error', 'Кімната повна'); return; }

    room.players[socket.id] = { pos: myPos, nick, rating, uid, paddleInput: 0, boostInput: false };
    socket.join(room.id);
    socket.emit('mm:joined', { roomId: room.id, pos: myPos });
    broadcastLobby(room);

    // Починаємо відлік якщо ще не почали
    if (!room.countdownTimer && room.status === 'waiting') {
      room.status = 'countdown';
      let timeLeft = 10;
      broadcastLobby(room);
      io.to(room.id).emit('mm:countdown', { timeLeft });

      room.countdownTimer = setInterval(() => {
        timeLeft--;
        io.to(room.id).emit('mm:countdown', { timeLeft });
        if (timeLeft <= 0) {
          clearInterval(room.countdownTimer);
          room.countdownTimer = null;
          startGame(room);
        }
      }, 1000);
    }
  });

  socket.on('player:input', ({ paddlePos, boost }) => {
    if (!myRoom || !myPos) return;
    const gs = myRoom.game;
    if (!gs || gs.gameOver) return;
    const player = myRoom.players[socket.id];
    if (!player) return;

    // Оновлюємо позицію ракетки
    const p = gs.paddles[myPos];
    if (p && !gs.eliminated[myPos]) {
      if (p.axis === 'x') {
        p.x = Math.max(p.minPos, Math.min(p.maxPos, paddlePos - p.w / 2));
      } else {
        p.y = Math.max(p.minPos, Math.min(p.maxPos, paddlePos - p.h / 2));
      }
    }

    // Силове поле
    if (boost && !gs.fields[myPos].active && gs.energy[myPos] >= EPU) {
      gs.fields[myPos].active = true;
      gs.fields[myPos].timer = 0;
      gs.energy[myPos] = Math.max(0, gs.energy[myPos] - EPU);
    }
  });

  socket.on('mm:cancel', () => {
    leaveRoom();
  });

  socket.on('disconnect', () => {
    leaveRoom();
  });

  function leaveRoom() {
    if (!myRoom) return;
    delete myRoom.players[socket.id];
    socket.leave(myRoom.id);

    if (Object.keys(myRoom.players).length === 0) {
      // Всі вийшли — закриваємо кімнату
      if (myRoom.tickInterval) clearInterval(myRoom.tickInterval);
      if (myRoom.countdownTimer) clearInterval(myRoom.countdownTimer);
      rooms.delete(myRoom.id);
      console.log('Room deleted:', myRoom.id);
    } else {
      broadcastLobby(myRoom);
      // Якщо гра йшла — гравця замінює бот
      if (myRoom.game && !myRoom.game.gameOver && myPos) {
        myRoom.bots[myPos] = { nick: BOT_NAMES[0], rating: 500 };
        io.to(myRoom.id).emit('game:player_left', { pos: myPos });
      }
    }
    myRoom = null;
    myPos = null;
  }
});

httpServer.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
