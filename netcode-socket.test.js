'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),{io}=require('socket.io-client');
const P=require('./galaxy-physics'),{Client}=require('./galaxy-netcode'),Wire=require('./galaxy-wire'),{createFixture}=require('./match-socket-fixture.cjs');
const wait=ms=>new Promise(r=>setTimeout(r,ms));
async function until(fn,timeout=6000){const at=Date.now();while(!fn()){if(Date.now()-at>timeout)throw Error('Condition timed out');await wait(20);}}
function event(s,n){return new Promise((resolve,reject)=>{const timer=setTimeout(()=>{s.off(n,done);reject(Error('timeout: '+n));},5000);function done(x){clearTimeout(timer);resolve(x);}s.once(n,done);});}
test('real Socket.IO + worker: four clients, protocol rejection, movement and authenticated reconnect',{timeout:25000},async t=>{
 const f=createFixture(),url=await f.listen(),sockets=[],clients=[],states=[];let loop;
 t.after(async()=>{clearInterval(loop);sockets.forEach(s=>s.disconnect());await f.close();});
 async function connect(index){const s=io(url,{transports:['websocket'],reconnection:false,timeout:1000});sockets.push(s);
  s.on('net:init',data=>{const c=new Client({slot:data.slot,matchId:data.matchId,epoch:data.epoch,send:p=>s.volatile.emit('net:input',Wire.input(p)),requestSync:reset=>s.emit(reset?'net:resume':'net:sync')});c.setRtt(0);c.receive(data.snapshot,performance.now());clients[index]=c;states[index]=data.snapshot;});
  s.on('net:state',raw=>{const p=Wire.readSnapshot(raw,clients[index]?.base?.config);if(p){states[index]=p;clients[index]?.receive(p,performance.now());}});
  await new Promise((resolve,reject)=>{s.once('connect',resolve);s.once('connect_error',reject);});return s;
 }
 const old=await connect(4),denied=event(old,'mm:error');old.emit('mm:join',{idToken:'qa:a',netVersion:'gf-net2-3'});assert.match(await denied,/Онови/);assert.equal(f.rooms.size,0);
 for(let i=0;i<4;i++){const s=await connect(i);s.emit('mm:join',{idToken:'qa:'+['a','b','c','d'][i],mode:'ranked',netVersion:P.VERSION});}
 await until(()=>clients.slice(0,4).filter(Boolean).length===4);assert.equal(new Set(clients.slice(0,4).map(c=>c.matchId)).size,1);assert.equal(clients[4],undefined);
 loop=setInterval(()=>{for(const c of clients.slice(0,4))if(c){c.setInput({dx:1,magnet:true});c.advance(performance.now());}},16);
 await until(()=>states[0].world.tick>30);assert.ok(states[0].world.paddles.some(x=>x>260));
 const initial=clients[0].epoch,matchId=clients[0].matchId;sockets[1].emit('net:pause');await wait(100);sockets[1].emit('net:resume');await until(()=>clients[0].epoch>initial);
 sockets[1].disconnect();await wait(100);const next=await connect(0);next.emit('rejoin',{roomId:matchId,idToken:'qa:a',netVersion:P.VERSION});await until(()=>clients[0].epoch>initial+1);assert.equal(clients[0].matchId,matchId);
 assert.equal(f.data.get('users_private/a').silver,900);assert.equal(f.data.get('users_public/a').gamesPlayed,0);
});
