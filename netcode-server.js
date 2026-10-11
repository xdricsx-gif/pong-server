'use strict';
const {performance}=require('node:perf_hooks'),{randomBytes}=require('node:crypto');
const {Worker}=require('node:worker_threads'),{join}=require('node:path');
const P=require('./galaxy-physics');
const Wire=require('./galaxy-wire');
const pool=[],matches=new Map();
function send(net,type,data={}){net.worker.postMessage({type,id:net.room.id,...data});}
function makeWorker(){
  const worker=new Worker(join(__dirname,'netcode-worker.js'));worker.matchIds=new Set();
  worker.on('message',message=>{
    const net=matches.get(message.id);if(!net||net.worker!==worker)return;
    const {room,io,endGame}=net;
    if(message.type==='state'){
      send(net,'snapshotAck');
      if(performance.now()-message.packet.sentAt>250)return;
      room.game=message.packet.world;net.lastSnapshot=message.packet;
      // Coalesce state after a main-thread stall instead of replaying a packet backlog.
      if(!net.flushQueued){net.flushQueued=true;setImmediate(()=>{
        net.flushQueued=false;if(room.status!=='playing'||!matches.has(room.id))return;
        io.to(room.id).volatile.emit('net:state',Wire.snapshot(net.lastSnapshot));
        const second=Math.floor(room.game.tick/60);
        if(second!==net.lastSecond){net.lastSecond=second;room.matchTimeLeft=Math.max(0,(room.game.config.durationTicks-room.game.tick)*P.TICK_MS);
          io.to(room.id).emit('match:timer',{timeLeft:room.matchTimeLeft});}
      });}
    }else if(message.type==='init'){
      const socket=io.sockets.sockets.get(message.socketId);
      if(socket&&room.players[socket.id]?.slot===message.slot)socket.emit('net:init',{
        version:P.VERSION,matchId:room.id,slot:message.slot,epoch:message.epoch,snapshot:message.packet});
    }else if(message.type==='commit'){
      for(const event of message.events){
        if(event.type==='finish'){
          room.game=message.world;endGame(room,message.world.winner);matches.delete(room.id);worker.matchIds.delete(room.id);return;
        }
        if(['goal','eliminated','paddle:hit'].includes(event.type))io.to(room.id).emit(event.type,event.data);
      }
    }else if(message.type==='metrics')net.metrics=message.metrics;
  });
  let failed=false;
  function fail(){
    if(failed)return;failed=true;
    const index=pool.indexOf(worker);if(index>=0)pool.splice(index,1);
    // Never manufacture a winner/rewards after losing authoritative simulation.
    for(const id of worker.matchIds){const net=matches.get(id);if(!net)continue;
      if(!net.room.managed){net.room.status='finished';net.room.game.gameOver=true;net.io.to(id).emit('net:aborted',{reason:'simulation_unavailable'});}
      for(const timer of Object.values(net.room._slotDeleteTimers||{}))clearTimeout(timer);
      matches.delete(id);net.onAbort?.(id);
    }
    worker.matchIds.clear();
  }
  worker.on('error',error=>{console.error('[net2 worker]',error.message);fail();});
  worker.on('exit',()=>fail());
  pool.push(worker);return worker;
}
function prepare(room,{io,getPlayer,endGame,onAbort}){
  const players=P.SLOTS.map(s=>{
    const player=getPlayer(room,s),bot=room.bots[s];
    return {bot:!!bot&&!player,paddleStats:player?.paddleStats||{w:54,spd:3.5,fr:54,fd:1,er:1}};
  });
  const world=P.create({players,mode:room.mode||'ranked',seed:randomBytes(4).readUInt32LE(0)});
  // Two simulation workers isolate auth/database/dashboard work from physics.
  const worker=pool.length<2?makeWorker():pool.reduce((a,b)=>a.matchIds.size<=b.matchIds.size?a:b);
  const net=room.net2={room,io,endGame,onAbort,worker,rate:new Map(),sync:new Map(),metrics:{},lastSecond:-1};
  net.authority={connect:slot=>send(net,'connect',{slot}),disconnect:(slot,voluntary)=>send(net,'disconnect',{slot,voluntary}),
    system:event=>send(net,'system',{event})};
  matches.set(room.id,net);worker.matchIds.add(room.id);room.game=world;send(net,'create',{world});
  for(const player of Object.values(room.players))net.authority.connect(player.slot);
  return net;
}
function init(room,socket,slot){if(room.net2&&socket)send(room.net2,'init',{socketId:socket.id,slot});}
function input(room,socket,packet){
  const net=room?.net2,p=room?.players[socket.id];if(!net||!p||room.status!=='playing')return;
  const now=performance.now(),old=net.rate.get(socket.id);
  const rate=!old||now-old.time>=1000?{time:now,n:0}:old;
  net.rate.set(socket.id,rate);if(++rate.n>90)return;
  packet=Wire.readInput(packet);
  if(!packet||packet.version!==P.VERSION||!Array.isArray(packet.commands)||packet.commands.length>16)return;
  send(net,'input',{slot:p.slot,packet});
}
function sync(room,socket,slot){
  const net=room?.net2;if(!net||!room.players[socket.id])return;
  const now=performance.now();if(now-(net.sync.get(socket.id)??-Infinity)<500)return;
  net.sync.set(socket.id,now);init(room,socket,slot);
}
function disconnect(room,socket,slot,voluntary){
  room.net2.authority.disconnect(slot,voluntary);room.net2.rate.delete(socket.id);room.net2.sync.delete(socket.id);
}
function close(room){const net=matches.get(room.id);if(!net)return;send(net,'close');net.worker.matchIds.delete(room.id);matches.delete(room.id);}
async function shutdown(){for(const worker of [...pool])await worker.terminate();}
module.exports={VERSION:P.VERSION,prepare,init,input,sync,disconnect,close,shutdown};
