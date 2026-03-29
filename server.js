// Сервер v3 — арбітр голів + розумний matchmaking
// Таймер скидається коли підключається новий гравець
// Якщо 4 гравці — старт одразу

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
const POSITIONS = ['bottom', 'top', 'left', 'right'];
const BOT_NAMES = ['ZEPHYR', 'GLITCH', 'NOVA', 'STORM', 'BLAZE', 'PIXEL'];

const rooms = new Map();

function createRoom(id) {
  return {
    id, players: {}, bots: {},
    status: 'waiting',
    countdownTimer: null,
    game: {
      lives: { top: ML, bottom: ML, left: ML, right: ML },
      scores: { top: 0, bottom: 0, left: 0, right: 0 },
      eliminated: { top: false, bottom: false, left: false, right: false },
      paddles: { top: 260, bottom: 260, left: 260, right: 260 },
      energy: { top: 1, bottom: 1, left: 1, right: 1 },
      fields: { top: false, bottom: false, left: false, right: false },
      started: false,
      winner: null,
    }
  };
}

function findOrCreateRoom() {
  for (const [id, room] of rooms) {
    if (room.status === 'waiting' || room.status === 'countdown') {
      if (Object.keys(room.players).length < 4) return room;
    }
  }
  const id = 'room_' + Math.random().toString(36).slice(2, 8);
  const room = createRoom(id);
  rooms.set(id, room);
  return room;
}

function getAvailablePos(room) {
  const taken = Object.values(room.players).map(p => p.pos);
  return POSITIONS.find(p => !taken.includes(p));
}

function fillBots(room) {
  room.bots = {};
  let bi = 0;
  for (const pos of POSITIONS) {
    const taken = Object.values(room.players).map(p => p.pos);
    if (!taken.includes(pos)) {
      room.bots[pos] = { nick: BOT_NAMES[bi % BOT_NAMES.length], rating: 490 + Math.floor(Math.random() * 30) };
      bi++;
    }
  }
}

function buildSlots(room) {
  const slots = {};
  for (const pos of POSITIONS) {
    const player = Object.values(room.players).find(p => p.pos === pos);
    if (player) slots[pos] = { nick: player.nick, rating: player.rating, isBot: false };
    else if (room.bots[pos]) slots[pos] = { nick: room.bots[pos].nick, rating: room.bots[pos].rating, isBot: true };
  }
  return slots;
}

function broadcastLobby(room) {
  io.to(room.id).emit('lobby:update', { slots: buildSlots(room) });
}

function activePlayers(room) {
  return POSITIONS.filter(p => !room.game.eliminated[p]);
}

function startCountdown(room) {
  // Зупиняємо попередній таймер якщо був
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
  let myRoom = null, myPos = null;

  socket.on('mm:join', ({ nick, rating, uid }) => {
    const room = findOrCreateRoom();
    myRoom = room;
    myPos = getAvailablePos(room);
    if (!myPos) { socket.emit('mm:error', 'Кімната повна'); return; }

    room.players[socket.id] = { pos: myPos, nick, rating, uid };
    socket.join(room.id);
    socket.emit('mm:joined', { roomId: room.id, pos: myPos });
    broadcastLobby(room);

    const playerCount = Object.keys(room.players).length;
    console.log(`Room ${room.id}: ${playerCount} player(s)`);

    if (playerCount === 1) {
      // Перший гравець — просто чекаємо, таймер не запускаємо
      room.status = 'waiting';
      socket.emit('mm:waiting', { message: 'Чекаємо суперників...' });
    } else if (playerCount >= 4) {
      // Повна кімната — старт одразу
      if (room.countdownTimer) {
        clearInterval(room.countdownTimer);
        room.countdownTimer = null;
      }
      startGame(room);
    } else {
      // 2-3 гравці — скидаємо і запускаємо таймер
      startCountdown(room);
    }
  });

  // Позиція ракетки — транслюємо іншим
  socket.on('player:paddle', ({ pos, x, energy, fieldActive }) => {
    if (!myRoom || !myRoom.game.started) return;
    myRoom.game.paddles[pos] = x;
    myRoom.game.energy[pos] = energy;
    myRoom.game.fields[pos] = fieldActive;
    socket.to(myRoom.id).emit('paddle:update', { pos, x, energy, fieldActive });
  });

  // Гол — підтверджуємо і розсилаємо
  socket.on('goal:report', ({ pos }) => {
    if (!myRoom || !myRoom.game.started) return;
    if (myRoom.game.eliminated[pos]) return;

    const g = myRoom.game;
    g.scores[pos]++;
    g.lives[pos]--;

    io.to(myRoom.id).emit('goal:confirmed', {
      pos,
      lives: { ...g.lives },
      scores: { ...g.scores },
    });

    if (g.lives[pos] <= 0) {
      g.eliminated[pos] = true;
      io.to(myRoom.id).emit('player:eliminated', { pos });
      const active = activePlayers(myRoom);
      if (active.length === 1) endGame(myRoom, active[0]);
    }
  });

  // Синхронізація м'яча від хоста
  socket.on('ball:spawn', ({ vx, vy }) => {
    if (!myRoom || !myRoom.game.started) return;
    socket.to(myRoom.id).emit('ball:synced', { vx, vy, x: 260, y: 260 });
  });

  socket.on('mm:cancel', () => leave());
  socket.on('disconnect', () => leave());

  function leave() {
    if (!myRoom) return;
    delete myRoom.players[socket.id];
    socket.leave(myRoom.id);

    const playerCount = Object.keys(myRoom.players).length;

    if (playerCount === 0) {
      // Всі вийшли — закриваємо кімнату
      if (myRoom.countdownTimer) clearInterval(myRoom.countdownTimer);
      rooms.delete(myRoom.id);
      console.log(`Room ${myRoom.id} deleted`);
    } else {
      broadcastLobby(myRoom);

      if (myRoom.game.started && myPos) {
        // Під час гри — замінюємо ботом
        myRoom.bots[myPos] = { nick: BOT_NAMES[0], rating: 500 };
        io.to(myRoom.id).emit('player:left', { pos: myPos });
      } else if (myRoom.status === 'countdown' && playerCount === 1) {
        // Залишився один — зупиняємо таймер
        if (myRoom.countdownTimer) {
          clearInterval(myRoom.countdownTimer);
          myRoom.countdownTimer = null;
        }
        myRoom.status = 'waiting';
        io.to(myRoom.id).emit('mm:waiting', { message: 'Суперник відключився. Чекаємо...' });
      }
    }

    myRoom = null;
    myPos = null;
  }
});

function startGame(room) {
  room.status = 'playing';
  fillBots(room);
  room.game.started = true;
  const a = (Math.random() * 0.7 + 0.15) * Math.PI * (Math.random() < 0.5 ? 1 : -1) + (Math.random() < 0.5 ? 0 : Math.PI);
  const vx = Math.cos(a) * 4, vy = Math.sin(a) * 4;
  console.log(`Game started in room ${room.id} with ${Object.keys(room.players).length} players`);
  io.to(room.id).emit('game:start', {
    slots: buildSlots(room),
    ball: { x: 260, y: 260, vx, vy }
  });
}

function endGame(room, winner) {
  room.game.winner = winner;
  room.status = 'finished';
  io.to(room.id).emit('game:over', { winner, slots: buildSlots(room) });
}

httpServer.listen(PORT, () => console.log(`Server on port ${PORT}`));
