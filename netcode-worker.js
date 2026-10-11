'use strict';
const {parentPort}=require('node:worker_threads'),{performance}=require('node:perf_hooks');
const P=require('./galaxy-physics'),{Authority}=require('./galaxy-netcode');
const rooms=new Map();
function snapshot(room){return {...room.a.snapshot(),sentAt:performance.now(),phase:room.acc/P.TICK_MS};}
parentPort.on('message',message=>{
  const {type,id}=message;
  if(type==='create'){
    const room={acc:0,last:performance.now(),sent:0,dropped:0,maxStepMs:0,maxPumpMs:0};
    room.a=new Authority({matchId:id,world:message.world,onCommit:commit=>{
      if(commit.events.some(e=>['finish','goal','eliminated','paddle:hit'].includes(e.type)))parentPort.postMessage({type:'commit',id,...commit});
    }});
    rooms.set(id,room);return;
  }
  const room=rooms.get(id);if(!room)return;
  if(type==='input')room.a.receive(message.slot,message.packet);
  if(type==='connect')room.a.connect(message.slot);
  if(type==='disconnect')room.a.disconnect(message.slot,message.voluntary);
  if(type==='system')room.a.system(message.event);
  if(type==='close')rooms.delete(id);
  if(type==='snapshotAck')room.awaitingSnapshot=false;
  if(type==='init')parentPort.postMessage({type:'init',id,socketId:message.socketId,slot:message.slot,
    epoch:room.a.epochs[message.slot],packet:snapshot(room)});
});
setInterval(()=>{
  const now=performance.now();
  for(const [id,room] of rooms){const start=performance.now();
    room.acc+=Math.max(0,now-room.last);room.last=now;
    room.dropped+=Math.max(0,room.acc-250);room.acc=Math.min(250,room.acc);
    let count=0;
    while(room.acc+1e-6>=P.TICK_MS&&count++<8){const begin=performance.now();
      room.a.advance();room.maxStepMs=Math.max(room.maxStepMs,performance.now()-begin);room.acc-=P.TICK_MS;
      if(room.a.finished){rooms.delete(id);break;}
    }
    if(!rooms.has(id))continue;
    if(!room.awaitingSnapshot&&now-room.sent>=1000/30-1){room.sent=now;room.awaitingSnapshot=true;
      parentPort.postMessage({type:'state',id,packet:snapshot(room)});
    }
    room.maxPumpMs=Math.max(room.maxPumpMs,performance.now()-start);
    if(room.a.world.tick%300===0&&room.lastMetric!==room.a.world.tick){room.lastMetric=room.a.world.tick;
      parentPort.postMessage({type:'metrics',id,metrics:{...room.a.metrics,droppedWallMs:room.dropped,maxStepMs:room.maxStepMs,maxPumpMs:room.maxPumpMs}});}
  }
},8);
