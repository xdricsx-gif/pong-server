'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),{io}=require('socket.io-client'),P=require('./galaxy-physics'),{createFixture}=require('./match-socket-fixture.cjs');
const delay=ms=>new Promise(r=>setTimeout(r,ms));
function event(s,name,timeout=5000){return new Promise((resolve,reject)=>{const timer=setTimeout(()=>{s.off(name,done);reject(Error('timeout: '+name));},timeout);const done=d=>{clearTimeout(timer);resolve(d);};s.once(name,done);});}
async function connect(url,uid){const s=io(url,{transports:['websocket'],reconnection:false,timeout:2000});await Promise.race([event(s,'connect'),event(s,'connect_error').then(e=>{throw e;})]);const ready=event(s,'match:ready');s.emit('match:auth',{idToken:'qa:'+uid,netVersion:P.VERSION});await ready;return s;}
test('live sockets: 1v1 consent, no side players, reconnect deadline, durable result and no costs',{timeout:16000},async t=>{
 const f=createFixture({graceMs:300}),url=await f.listen();const sockets=[];t.after(async()=>{sockets.forEach(s=>s.disconnect());await f.close();});
 const a=await connect(url,'a'),b=await connect(url,'b');sockets.push(a,b);
 const offered=event(b,'match:invite_received');a.emit('match:invite',{toUid:'b',mode:'duel'});const invitation=await offered;assert.equal(f.rooms.size,0);
 const starts=[event(a,'net:init'),event(b,'net:init')];b.emit('match:invite_answer',{id:invitation.id,accept:true});const [init]=await Promise.all(starts);assert.deepEqual(init.snapshot.world.config.activeSlots,[0,1]);
 const room=f.rooms.get(init.matchId);assert.equal(f.data.get('users_private/a').silver,1000);
 a.emit('net:pause');await delay(70);const resumed=event(a,'net:init');a.emit('net:resume');const next=await resumed;assert.ok(next.epoch>init.epoch);
 // Expired claim is made before the worker runs. A simultaneous resume must not win the race.
 a.emit('net:pause');await delay(50);const lease=room.lease[0];lease.deadline=Date.now()-1;const denied=event(a,'match:waiting_result');a.emit('net:resume');await denied;
 const result=await event(b,'match:result');assert.equal(result.serverCommitted,true);assert.equal(result.mode,'duel');assert.equal(result.rewards[1].silver,0);assert.equal(result.participants,undefined);assert.equal(result.totals,undefined);
 const recovered=event(a,'match:result');a.emit('shop:resolve_pending',{roomId:room.id,aliveAtLastUpdate:1,type:'ranked'});await recovered;assert.equal(f.data.get('users_public/a').rating,500);assert.equal(f.data.get('users_public/a').gamesPlayed,0);
});
test('live sockets: accepted team pair remains adjacent; other pair fills opponents and pot settles once',{timeout:16000},async t=>{
 const f=createFixture(),url=await f.listen(),sockets=[];t.after(async()=>{sockets.forEach(s=>s.disconnect());await f.close();});
 for(const uid of ['a','b','c','d'])sockets.push(await connect(url,uid));const [a,b,c,d]=sockets;
 let offer=event(b,'match:invite_received');a.emit('match:invite',{toUid:'b',mode:'team'});const first=await offer;b.emit('match:invite_answer',{id:first.id,accept:true});await event(a,'match:queued');
 offer=event(d,'match:invite_received');c.emit('match:invite',{toUid:'d',mode:'team'});const second=await offer;const starts=sockets.map(s=>event(s,'net:init'));d.emit('match:invite_answer',{id:second.id,accept:true});const init=await Promise.all(starts);
 assert.deepEqual(init.map(x=>x.slot),[0,3,1,2]);const room=f.rooms.get(init[0].matchId);assert.equal(f.data.get('users_private/a').silver,900);
 const done=sockets.map(s=>event(s,'match:result'));room.net2.authority.system({type:'eliminate',slot:1});room.net2.authority.system({type:'eliminate',slot:2});await Promise.all(done);
 assert.equal(f.data.get('users_private/a').silver,1100);assert.equal(f.data.get('users_private/b').silver,1100);assert.equal(f.data.get('users_public/a').teamRating,525);
 const repeated=event(a,'match:result');a.emit('shop:resolve_pending',{roomId:room.id});await repeated;assert.equal(f.data.get('users_private/a').silver,1100);
});
