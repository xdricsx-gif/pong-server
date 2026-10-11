'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),{io}=require('socket.io-client'),P=require('./galaxy-physics'),{createFixture}=require('./match-socket-fixture.cjs');
const delay=ms=>new Promise(r=>setTimeout(r,ms));
function event(s,name,timeout=5000){return new Promise((resolve,reject)=>{const timer=setTimeout(()=>{s.off(name,done);reject(Error('timeout: '+name));},timeout);const done=d=>{clearTimeout(timer);resolve(d);};s.once(name,done);});}
async function connect(f,uid){const s=f.client();const ready=event(s,"match:ready");s.emit("match:auth",{idToken:"qa:"+uid,netVersion:P.VERSION});await ready;return s;}
test('service + real worker (memory transport): 1v1 consent, no side players, reconnect deadline, durable result and no costs',{timeout:16000},async t=>{
 const f=createFixture({graceMs:300,memory:true}),url=f;const sockets=[];t.after(async()=>{sockets.forEach(s=>s.disconnect());await f.close();});
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
test('invitations require authenticated recipient consent and duplicate acceptance cannot debit twice',async t=>{
 const f=createFixture({memory:true});t.after(()=>f.close());const a=await connect(f,'a'),b=await connect(f,'b'),c=await connect(f,'c');
 const offer=event(b,'match:invite_received');a.emit('match:invite',{toUid:'b',mode:'duel'});const v=await offer;
 const wrong=event(c,'match:error');c.emit('match:invite_answer',{id:v.id,accept:true});assert.equal((await wrong).code,'wrong_uid');assert.equal(f.rooms.size,0);
 const started=event(a,'net:init');b.emit('match:invite_answer',{id:v.id,accept:true});await started;const twice=event(b,'match:error');b.emit('match:invite_answer',{id:v.id,accept:true});assert.equal((await twice).code,'invite_expired');assert.equal(f.rooms.size,1);assert.equal(f.data.get('users_private/a').silver,1000);
});
test('cancel while entry transaction is pending refunds everyone without starting simulation',async t=>{
 const f=createFixture({memory:true});t.after(()=>f.close());const clients=[];for(const u of ['a','b','c','d'])clients.push(await connect(f,u));
 const original=f.db.runTransaction;let release,entered;const gate=new Promise(r=>release=r),ready=new Promise(r=>entered=r);let intercept=true;
 f.db.runTransaction=async fn=>{if(intercept){intercept=false;entered();await gate;}return original(fn);};
 for(const s of clients)s.emit('mm:join',{mode:'ranked',idToken:'qa:'+s.serverSocket.uid,netVersion:P.VERSION});
 await ready;const results=clients.map(s=>event(s,'match:result'));clients[0].emit('mm:cancel');release();const done=await Promise.all(results);
 assert.ok(done.every(r=>r.aborted&&r.serverCommitted));assert.equal([...f.rooms.values()][0].net2,undefined);for(const u of ['a','b','c','d'])assert.equal(f.data.get('users_private/'+u).silver,1000);
});
test('all four disconnect timeouts produce no arbitrary victory and return the 400 pot once',async t=>{
 const f=createFixture({memory:true,graceMs:100});t.after(()=>f.close());const clients=[];for(const u of ['a','b','c','d'])clients.push(await connect(f,u));
 const starts=clients.map(s=>event(s,'net:init'));for(const s of clients)s.emit('mm:join',{mode:'ranked',idToken:'qa:'+s.serverSocket.uid,netVersion:P.VERSION});await Promise.all(starts);
 const done=clients.map(s=>event(s,'match:result'));for(const s of clients)s.emit('net:pause');const results=await Promise.all(done);assert.ok(results.every(r=>r.winnerSlot===null&&r.aborted));
 for(const u of ['a','b','c','d']){assert.equal(f.data.get('users_private/'+u).silver,1000);assert.equal(f.data.get('users_public/'+u).gamesPlayed,0);}
});
test('service + real worker (memory transport): accepted team pair remains adjacent; other pair fills opponents and pot settles once',{timeout:16000},async t=>{
 const f=createFixture({memory:true}),url=f,sockets=[];t.after(async()=>{sockets.forEach(s=>s.disconnect());await f.close();});
 for(const uid of ['a','b','c','d'])sockets.push(await connect(url,uid));const [a,b,c,d]=sockets;
 let offer=event(b,'match:invite_received');a.emit('match:invite',{toUid:'b',mode:'team'});const first=await offer;b.emit('match:invite_answer',{id:first.id,accept:true});await event(a,'match:queued');
 offer=event(d,'match:invite_received');c.emit('match:invite',{toUid:'d',mode:'team'});const second=await offer;const starts=sockets.map(s=>event(s,'net:init'));d.emit('match:invite_answer',{id:second.id,accept:true});const init=await Promise.all(starts);
 assert.deepEqual(init.map(x=>x.slot),[0,3,1,2]);const room=f.rooms.get(init[0].matchId);assert.equal(f.data.get('users_private/a').silver,900);
 const done=sockets.map(s=>event(s,'match:result'));room.net2.authority.system({type:'eliminate',slot:1});room.net2.authority.system({type:'eliminate',slot:2});await Promise.all(done);
 assert.equal(f.data.get('users_private/a').silver,1100);assert.equal(f.data.get('users_private/b').silver,1100);assert.equal(f.data.get('users_public/a').teamRating,525);
 const repeated=event(a,'match:result');a.emit('shop:resolve_pending',{roomId:room.id});await repeated;assert.equal(f.data.get('users_private/a').silver,1100);
});

test('older protocol gets a readable update notice and cannot spend a stake',async t=>{
 const f=createFixture({memory:true});t.after(()=>f.close());const s=f.client(),old=event(s,'mm:error'),newer=event(s,'match:error');s.emit('mm:join',{idToken:'qa:a',netVersion:'gf-net2-3'});assert.match(await old,/Онови/);assert.equal((await newer).code,'update_required');assert.equal(f.rooms.size,0);assert.equal(f.data.get('users_private/a').silver,1000);
});
