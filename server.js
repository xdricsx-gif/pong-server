// Сервер v4 — slot-based, нейтральні координати
// Кожен гравець отримує slot 0-3
// Позиції ракеток передаються як percent (0.0-1.0)

const { createServer } = require('http');
const { Server } = require('socket.io');

const httpServer = createServer();
const io = new Server(httpServer, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  pingInterval: 5000,
  pingTimeout: 10000,
});

const PORT = process.env.PORT || 3000;
const ML = 10;
const SLOTS = [0, 1, 2, 3]; // 0=bottom, 1=top, 2=left, 3=right (з точки зору slot0)
const BOT_NAMES = ['ZEPHYR', 'GLITCH', 'NOVA', 'STORM', 'BLAZE', 'PIXEL'];

const rooms = new Map();

function createRoom(id) {
  return {
    id,
    players: {}, // socketId -> { slot, nick, rating, uid }
    bots: {},    // slot -> { nick, rating }
    status: 'waiting',
    countdownTimer: null,
    game: {
      lives: { 0: ML, 1: ML, 2: ML, 3: ML },
      scores: { 0: 0, 1: 0, 2: 0, 3: 0 },
      eliminated: { 0: false, 1: false, 2: false, 3: false },
      paddles: { 0: 0.5, 1: 0.5, 2: 0.5, 3: 0.5 }, // percent 0-1
      energy: { 0: 1, 1: 1, 2: 1, 3: 1 },
      fields: { 0: false, 1: false, 2: false, 3: false },
      started: false,
      winner: null,
    }
  };
}

function findOrCreateRoom() {
  for (const [, room] of rooms) {
    if ((room.status === 'waiting' || room.status === 'countdown') &&
        Object.keys(room.players).length < 4) return room;
  }
  const id = 'room_' + Math.random().toString(36).slice(2, 8);
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
  for (const slot of SLOTS) {
    const taken = Object.values(room.players).map(p => p.slot);
    if (!taken.includes(slot)) {
      room.bots[slot] = {
        nick: BOT_NAMES[bi % BOT_NAMES.length],
        rating: 490 + Math.floor(Math.random() * 30)
      };
      bi++;
    }
  }
}

// Будуємо інфо про всіх гравців для клієнта
function buildPlayers(room) {
  const result = {};
  for (const slot of SLOTS) {
    const player = Object.values(room.players).find(p => p.slot === slot);
    if (player) {
      result[slot] = { nick: player.nick, rating: player.rating, isBot: false };
    } else if (room.bots[slot]) {
      result[slot] = { nick: room.bots[slot].nick, rating: room.bots[slot].rating, isBot: true };
    }
  }
  return result;
}

function broadcastLobby(room) {
  io.to(room.id).emit('lobby:update', { players: buildPlayers(room) });
}

function activeSlots(room) {
  return SLOTS.filter(s => !room.game.eliminated[s]);
}

function startCountdown(room) {
  if (room.countdownTimer) {
    clearInterval(room.countdownTimer);
    room.countdownTimer = null;
  }
  room.status = 'countdown';
  let tl = 10;
  io.to(room.id).emit('mm:countdown', { timeLeft: tl });
  room.countdownTimer = setInterval(() => {
    tl--;
    io.to(room.id).emit('mm:countdown', { timeLeft: tl });
    if (tl <= 0) {
      clearInterval(room.countdownTimer);
      room.countdownTimer = null;
      startGame(room);
    }
  }, 1000);
}

io.on('connection', (socket) => {
  let myRoom = null, mySlot = null;

  socket.on('mm:join', ({ nick, rating, uid }) => {
    const room = findOrCreateRoom();
    myRoom = room;
    mySlot = getAvailableSlot(room);
    if (mySlot === undefined) { socket.emit('mm:error', 'Кімната повна'); return; }

    room.players[socket.id] = { slot: mySlot, nick, rating, uid };
    socket.join(room.id);

    // Повідомляємо гравця його slot і всіх інших
    socket.emit('mm:joined', { roomId: room.id, mySlot });
    broadcastLobby(room);

    const count = Object.keys(room.players).length;

    if (count === 1) {
      room.status = 'waiting';
      socket.emit('mm:waiting', {});
    } else if (count >= 4) {
      if (room.countdownTimer) { clearInterval(room.countdownTimer); room.countdownTimer = null; }
      startGame(room);
    } else {
      startCountdown(room);
    }
  });

  // Гравець надсилає свою позицію як percent (0-1)
  socket.on('player:paddle', ({ percent, energy, fieldActive }) => {
    if (!myRoom || !myRoom.game.started || mySlot === null) return;
    myRoom.game.paddles[mySlot] = percent;
    myRoom.game.energy[mySlot] = energy;
    myRoom.game.fields[mySlot] = fieldActive;
    // Транслюємо іншим
    socket.to(myRoom.id).emit('paddle:update', {
      slot: mySlot, percent, energy, fieldActive
    });
  });

  // Гол
  socket.on('goal:report', ({ slot }) => {
    if (!myRoom || !myRoom.game.started) return;
    if (myRoom.game.eliminated[slot]) return;
    const g = myRoom.game;
    g.scores[slot]++;
    g.lives[slot]--;
    io.to(myRoom.id).emit('goal:confirmed', {
      slot,
      lives: { ...g.lives },
      scores: { ...g.scores },
    });
    if (g.lives[slot] <= 0) {
      g.eliminated[slot] = true;
      io.to(myRoom.id).emit('player:eliminated', { slot });
      const active = activeSlots(myRoom);
      if (active.length === 1) endGame(myRoom, active[0]);
    }
  });

  // Синхронізація м'яча (хост = slot 0)
  socket.on('ball:spawn', ({ vx, vy }) => {
    if (!myRoom || !myRoom.game.started || mySlot !== 0) return;
    socket.to(myRoom.id).emit('ball:synced', { vx, vy });
  });

  socket.on('mm:cancel', () => leave());
  socket.on('disconnect', () => leave());

  function leave() {
    if (!myRoom) return;
    delete myRoom.players[socket.id];
    socket.leave(myRoom.id);
    const count = Object.keys(myRoom.players).length;
    if (count === 0) {
      if (myRoom.countdownTimer) clearInterval(myRoom.countdownTimer);
      rooms.delete(myRoom.id);
    } else {
      broadcastLobby(myRoom);
      if (myRoom.game.started && mySlot !== null) {
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

function startGame(room) {
  room.status = 'playing';
  fillBots(room);
  room.game.started = true;
  // Генеруємо початковий вектор м'яча
  let vx, vy, att = 0;
  do {
    const a = (Math.random() * 0.7 + 0.15) * Math.PI * (Math.random() < 0.5 ? 1 : -1) + (Math.random() < 0.5 ? 0 : Math.PI);
    vx = Math.cos(a) * 4; vy = Math.sin(a) * 4; att++;
  } while ((Math.abs(vx) < 1.8 || Math.abs(vy) < 1.8) && att < 30);

  io.to(room.id).emit('game:start', {
    players: buildPlayers(room),
    ball: { vx, vy }
  });
  console.log(`Game started: ${room.id}, ${Object.keys(room.players).length} real players`);
}

function endGame(room, winnerSlot) {
  room.game.winner = winnerSlot;
  room.status = 'finished';
  io.to(room.id).emit('game:over', { winnerSlot, players: buildPlayers(room) });
}

httpServer.listen(PORT, () => console.log(`Server on port ${PORT}`));
