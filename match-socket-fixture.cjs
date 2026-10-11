'use strict';
const http=require('node:http'),{Server}=require('socket.io'),{database}=require('./match-test-fixture.cjs'),{createMatchService}=require('./match-service'),net=require('./netcode-server'),P=require('./galaxy-physics');
function createFixture({graceMs=30000,memory=false}={}){
 const f=database(),server=http.createServer(),io=memory?require('./match-memory-transport.cjs').memoryIO():new Server(server,{cors:{origin:'*'}}),rooms=new Map();let service;
 const createRoom=id=>({id,players:{},bots:{},status:'waiting'});
 const getPlayer=(room,slot)=>Object.values(room.players).find(p=>p.slot===slot);
 const setPlayer=(r,id,p)=>{r.players[id]=p;},clearPlayer=(r,id)=>delete r.players[id];
 function startGame(room){room.status='playing';if(room.mode==='training')for(let slot=1;slot<4;slot++)room.bots[slot]={nick:'QA Bot '+slot};
  net.prepare(room,{io,getPlayer,endGame:()=>service.finish(room),onAbort:()=>service.abort(room,'simulation_unavailable')});room.paddleVisuals=[0,1,2,3].map(()=>({paddleId:0,avgUpgrade:0}));
  const players={...Object.fromEntries(Object.entries(room.bots)),...Object.fromEntries(Object.values(room.players).map(p=>[p.slot,{nick:p.nick,rating:p.rating,avatarId:0}]))};
  io.to(room.id).emit('game:start',{players,roomId:room.id,paddleVisuals:room.paddleVisuals,netVersion:P.VERSION,matchInfo:service.info(room)});
  for(const [id,p] of Object.entries(room.players)){io.to(id).emit('myslot',{mySlot:p.slot,roomId:room.id});net.init(room,io.sockets.sockets.get(id),p.slot);}
 }
 service=createMatchService({io,rooms,createRoom,setPlayer,clearPlayer,getPlayer,startGame,net,getDb:()=>f.db,authenticate:async({idToken})=>/^qa:[abcd]$/.test(idToken)?idToken.slice(3):null,loadStats:async()=>({w:54,spd:3.375,fr:54,fd:1,er:1,paddleId:0,avgUpgrade:0}),graceMs});
 io.on('connection',s=>{s.on('np',c=>s.emit('np',{c,s:Date.now()}));s.on('net:ping',c=>s.emit('net:pong',{c,s:performance.now()}));service.attach(s);});
 async function listen(port=0){await new Promise(r=>server.listen(port,'127.0.0.1',r));return 'http://127.0.0.1:'+server.address().port;}
 async function close(){service.close();for(const r of rooms.values())net.close(r);io.disconnectSockets(true);await new Promise(r=>io.close(r));await net.shutdown?.();}
 function client(){const c=io.client();service.attach(c.serverSocket);return c;}
 return {...f,server,io,rooms,service,listen,close,client};
}
module.exports={createFixture};
