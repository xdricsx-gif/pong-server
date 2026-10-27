'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const P=require('./galaxy-physics'),F=require('./galaxy-fleet'),{createFixture}=require('./match-socket-fixture.cjs');
const event=(socket,name,timeout=5000)=>new Promise((resolve,reject)=>{const done=value=>{clearTimeout(timer);resolve(value);},timer=setTimeout(()=>{socket.off(name,done);reject(Error('timeout: '+name));},timeout);socket.once(name,done);});
async function connect(f,uid){const socket=f.client(),ready=event(socket,'match:ready');socket.emit('match:auth',{idToken:'qa:'+uid,netVersion:P.VERSION});await ready;return socket;}
function stats(keys,level=1){const economy=F.economy();economy.ownedSkills=keys.slice();economy.equippedSkills=keys;economy.skills.push=level;return F.fromAccount({economy});}
function join(socket,uid,mode='ranked'){socket.emit('mm:join',{idToken:'qa:'+uid,netVersion:P.VERSION,mode});}
async function invite(from,to,toUid,mode){const offered=event(to,'match:invite_received');from.emit('match:invite',{toUid,mode});return offered;}

test('duel acceptance refreshes both players after loadout and level changes without another auth',async t=>{
 const current={a:stats(['push','cloak']),b:stats(['push','cloak'])},reads=[];
 const f=createFixture({memory:true,loadStats:async(uid,options)=>{reads.push({uid,strict:!!options?.strict});return current[uid];}});t.after(()=>f.close());
 const a=await connect(f,'a'),b=await connect(f,'b'),offer=await invite(a,b,'b','duel');
 current.a=stats(['magnet']);current.b=stats(['shield','push'],10);
 const starts=[event(a,'net:init'),event(b,'net:init')];b.emit('match:invite_answer',{id:offer.id,accept:true});const [init]=await Promise.all(starts);
 const players=init.snapshot.world.config.players;
 assert.deepEqual(players[0].paddleStats.equippedSkills,['magnet']);assert.deepEqual(players[1].paddleStats.equippedSkills,['shield','push']);assert.equal(players[1].paddleStats.skills.push,10);
 assert.deepEqual(reads.filter(r=>r.strict).map(r=>r.uid).sort(),['a','b']);
 assert.equal(f.data.get('users_private/a').silver,1000);assert.equal(f.data.get('users_private/b').silver,1000);
});

test('a waiting team pair gets current equipment when the opposing pair finally accepts',async t=>{
 const current=Object.fromEntries(['a','b','c','d'].map(uid=>[uid,stats(['push','cloak'])]));
 const f=createFixture({memory:true,loadStats:async uid=>current[uid]});t.after(()=>f.close());
 const clients=[];for(const uid of ['a','b','c','d'])clients.push(await connect(f,uid));const [a,b,c,d]=clients;
 const first=await invite(a,b,'b','team'),queued=event(a,'match:queued');b.emit('match:invite_answer',{id:first.id,accept:true});await queued;
 current.a=stats(['shield']);current.b=stats(['magnet']);current.c=stats(['cloak']);current.d=stats(['push'],10);
 const second=await invite(c,d,'d','team'),starts=clients.map(s=>event(s,'net:init'));d.emit('match:invite_answer',{id:second.id,accept:true});const [init]=await Promise.all(starts);
 assert.deepEqual(init.snapshot.world.config.players.map(p=>p.paddleStats.equippedSkills),[['shield'],['cloak'],['push'],['magnet']]);
 assert.equal(init.snapshot.world.config.players[2].paddleStats.skills.push,10);
});

test('failed authoritative read removes the selected queue cleanly, does not spend, and allows retry',async t=>{
 let failRead=true;
 const f=createFixture({memory:true,rankedWaitMs:5000,loadStats:async(uid,options)=>{if(options?.strict&&uid==='b'&&failRead)throw Error('database unavailable');return stats(['magnet','shield']);}});t.after(()=>f.close());
 const clients=[];for(const uid of ['a','b','c','d'])clients.push(await connect(f,uid));
 const errors=clients.map(s=>event(s,'match:error'));clients.forEach((s,i)=>join(s,['a','b','c','d'][i]));
 assert.ok((await Promise.all(errors)).every(e=>e.code==='server_unavailable'));
 assert.equal(f.rooms.size,0);assert.equal(f.service._state.inMatch.size,0);assert.equal(f.service._state.queued.size,0);assert.equal(f.service._state.queues.ranked.length,0);
 for(const uid of ['a','b','c','d']){assert.equal(f.data.get('users_private/'+uid).silver,1000);assert.equal(f.data.get('users_private/'+uid).activeMatchId,undefined);}
 assert.equal([...f.data.keys()].some(key=>key.startsWith('match_sessions/')),false);
 failRead=false;const starts=clients.map(s=>event(s,'net:init'));clients.forEach((s,i)=>join(s,['a','b','c','d'][i]));await Promise.all(starts);
 assert.equal(f.rooms.size,1);for(const uid of ['a','b','c','d'])assert.equal(f.data.get('users_private/'+uid).silver,900);
});

test('cancelling while fresh stats are loading cannot reserve currency or start stale equipment',async t=>{
 let release,entered;const gate=new Promise(resolve=>release=resolve),reading=new Promise(resolve=>entered=resolve);
 const f=createFixture({memory:true,loadStats:async(uid,options)=>{if(options?.strict){entered();await gate;}return stats(['shield']);}});t.after(()=>f.close());
 const a=await connect(f,'a'),error=event(a,'match:error');join(a,'a','training');await reading;a.emit('mm:cancel');release();
 assert.equal((await error).code,'cancelled_before_start');assert.equal(f.rooms.size,0);assert.equal(f.service._state.inMatch.size,0);assert.equal(f.data.get('users_private/a').energy,100);assert.equal(f.data.get('users_private/a').silver,1000);
 assert.equal([...f.data.keys()].some(key=>key.startsWith('match_sessions/')),false);
});

test('strict production stat loading rejects failed or missing records instead of silently returning defaults',async()=>{
 const source=fs.readFileSync(__dirname+'/server.js','utf8'),start=source.indexOf('async function loadValidatedPaddleStats'),end=source.indexOf('// Серверна валідація rating',start);
 const fallback={w:54,spd:3.75},env=vm.createContext({db:null,DEFAULT_PADDLE_STATS:fallback,UfoFleet:F,console:{error(){}}});vm.runInContext(source.slice(start,end)+'\nglobalThis.loadStats=loadValidatedPaddleStats;',env);
 await assert.rejects(env.loadStats('a',{strict:true}),/server_unavailable/);assert.deepEqual(JSON.parse(JSON.stringify(await env.loadStats('a'))),fallback);
 env.db={collection:()=>({doc:()=>({get:async()=>{throw Error('read failed');}})})};await assert.rejects(env.loadStats('a',{strict:true}),/server_unavailable/);assert.deepEqual(JSON.parse(JSON.stringify(await env.loadStats('a'))),fallback);
 env.db={collection:()=>({doc:()=>({get:async()=>({exists:false})})})};await assert.rejects(env.loadStats('a',{strict:true}),/server_unavailable/);
 const economy=F.economy();economy.ownedSkills=['shield','magnet'];economy.equippedSkills=['shield','magnet'];env.db={collection:()=>({doc:()=>({get:async()=>({exists:true,data:()=>({economy})})})})};
 assert.deepEqual(Array.from((await env.loadStats('a',{strict:true})).equippedSkills),['shield','magnet']);
});
