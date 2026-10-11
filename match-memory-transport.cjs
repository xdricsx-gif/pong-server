'use strict';
// Test-only Socket.IO-shaped transport. All game/service/worker code stays real.
const {EventEmitter}=require('node:events');
function memoryIO(){
 const sockets=new Map();let sequence=0;
 const io={sockets:{sockets},on(){},to(room){const broadcast={emit(event,data){for(const s of sockets.values())if(s.connected&&s.rooms.has(room))s.emit(event,structuredClone(data));},get volatile(){return broadcast;}};return broadcast;},disconnectSockets(){for(const s of sockets.values())s.client.disconnect();},close(cb){cb?.();}};
 io.client=()=>{const incoming=new EventEmitter(),outgoing=new EventEmitter(),id='test-'+(++sequence);
   const s={id,connected:true,rooms:new Set([id]),on:(e,fn)=>incoming.on(e,fn),removeAllListeners:e=>incoming.removeAllListeners(e),emit:(e,d)=>outgoing.emit(e,d),join:r=>s.rooms.add(r),leave:r=>s.rooms.delete(r)};
   const client={id,connected:true,on:(e,fn)=>outgoing.on(e,fn),once:(e,fn)=>outgoing.once(e,fn),off:(e,fn)=>outgoing.off(e,fn),emit:(e,d)=>incoming.emit(e,d),disconnect(){if(!s.connected)return;client.connected=s.connected=false;incoming.emit('disconnect');sockets.delete(id);}};
   s.client=client;client.serverSocket=s;sockets.set(id,s);return client;
 };return io;
}
module.exports={memoryIO};
