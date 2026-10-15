'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const P=require('./galaxy-physics'),Rules=require('./match-rules');
const {createFixture}=require('./match-socket-fixture.cjs');
const {database}=require('./match-test-fixture.cjs'),{createLedger}=require('./match-ledger');
const delay=ms=>new Promise(r=>setTimeout(r,ms));
function event(s,name,timeout=5000){return new Promise((resolve,reject)=>{const done=d=>{clearTimeout(timer);resolve(d);};const timer=setTimeout(()=>{s.off(name,done);reject(Error('timeout: '+name));},timeout);s.once(name,done);});}
async function connect(f,uid){const s=f.client(),ready=event(s,'match:ready');s.emit('match:auth',{idToken:'qa:'+uid,netVersion:P.VERSION});await ready;return s;}
function join(s,uid,mode='ranked'){s.emit('mm:join',{mode,idToken:'qa:'+uid,netVersion:P.VERSION});}

test('default ranked search waits ten seconds, fills real simulated bots and pays the full first prize once',{timeout:16000},async t=>{
 const f=createFixture({memory:true});t.after(()=>f.close());const a=await connect(f,'a');
 const start=event(a,'net:init',14000),queued=event(a,'match:queued'),began=Date.now();join(a,'a');await queued;
 await delay(100);assert.equal(f.rooms.size,0);assert.equal(f.data.get('users_private/a').silver,1000);
 const init=await start;assert.ok(Date.now()-began>=9950);const room=f.rooms.get(init.matchId);
 assert.deepEqual(init.snapshot.world.config.players.map(p=>p.bot),[false,true,true,true]);
 assert.equal(f.data.get('users_private/a').silver,900);assert.equal(f.data.get('match_sessions/'+room.id).roster.length,1);
 const done=event(a,'match:result');for(const slot of [3,2,1])room.net2.authority.system({type:'eliminate',slot,reason:'leave'});const result=await done;
 assert.equal(result.rewards[0].silver,250);assert.equal(result.rewards[1],undefined);assert.equal(result.serverCommitted,true);
 assert.equal(Object.keys(result.players).length,4);assert.equal(f.data.get('users_private/a').silver,1150);
 const repeated=event(a,'match:result');a.emit('shop:resolve_pending',{roomId:room.id});await repeated;
 assert.equal(f.data.get('users_private/a').silver,1150);assert.equal(f.data.get('users_public/a').gamesPlayed,1);
 assert.deepEqual(f.data.get('users_private/a/matches/'+room.id).opponents,['QA Bot 1','QA Bot 2','QA Bot 3']);
 assert.equal([...f.data.keys()].filter(k=>/^users_private\/[^/]+$/.test(k)).length,4);
});

test('an accepted ranked challenge keeps both humans as rivals and fills only the remaining slots',async t=>{
 const f=createFixture({memory:true,rankedWaitMs:80});t.after(()=>f.close());const a=await connect(f,'a'),b=await connect(f,'b');
 const offered=event(b,'match:invite_received');a.emit('match:invite',{toUid:'b',mode:'ranked'});const invite=await offered;
 const starts=[event(a,'net:init'),event(b,'net:init')];b.emit('match:invite_answer',{id:invite.id,accept:true});const [init,other]=await Promise.all(starts);
 assert.deepEqual([init.slot,other.slot],[0,1]);assert.deepEqual(init.snapshot.world.config.players.map(p=>p.bot),[false,false,true,true]);
 const room=f.rooms.get(init.matchId),done=[event(a,'match:result'),event(b,'match:result')];await f.service.abort(room,'test_technical_abort');await Promise.all(done);
 assert.equal(f.data.get('users_private/a').silver,1000);assert.equal(f.data.get('users_private/b').silver,1000);
});

test('four humans start immediately and the old fallback timers cannot create another match',async t=>{
 const f=createFixture({memory:true,rankedWaitMs:500});t.after(()=>f.close());const clients=[];
 for(const uid of ['a','b','c','d'])clients.push(await connect(f,uid));const starts=clients.map(s=>event(s,'net:init')),began=Date.now();
 clients.forEach((s,i)=>join(s,['a','b','c','d'][i]));const init=await Promise.all(starts);
 assert.ok(Date.now()-began<500);assert.ok(init[0].snapshot.world.config.players.every(p=>!p.bot));await delay(550);
 assert.equal(f.rooms.size,1);assert.equal([...f.data.keys()].filter(k=>k.startsWith('match_sessions/')).length,1);
});

test('cancel and disconnect clear ranked fallback; joining again gets a fresh deadline',async t=>{
 const f=createFixture({memory:true,rankedWaitMs:160});t.after(()=>f.close());const a=await connect(f,'a');
 let queued=event(a,'match:queued');join(a,'a');await queued;await delay(100);a.emit('mm:cancel');
 queued=event(a,'match:queued');join(a,'a');await queued;await delay(90);assert.equal(f.rooms.size,0);
 a.disconnect();await delay(120);assert.equal(f.rooms.size,0);assert.equal(f.data.get('users_private/a').silver,1000);
});

test('bot fallback does not fill a 2v2 team queue',async t=>{
 const f=createFixture({memory:true,rankedWaitMs:50});t.after(()=>f.close());const a=await connect(f,'a'),queued=event(a,'match:queued');join(a,'a','team');await queued;
 await delay(120);assert.equal(f.rooms.size,0);assert.equal(f.service._state.queued.size,1);assert.equal(f.data.get('users_private/a').silver,1000);
});

test('fallback rejects insufficient human funds without a bot debit or partially created match',async t=>{
 const f=createFixture({memory:true,rankedWaitMs:40});t.after(()=>f.close());f.data.get('users_private/a').silver=99;const a=await connect(f,'a'),failed=event(a,'match:error');join(a,'a');
 assert.equal((await failed).code,'not_enough_silver');assert.equal(f.rooms.size,0);assert.equal(f.data.get('users_private/a').silver,99);assert.equal(f.service._state.queued.size,0);
});

for(let place=0;place<4;place++)test('one human among bots receives unchanged ranked payout for place '+(place+1),async()=>{
 const f=database(['a']),ledger=createLedger({db:f.db,owner:'test'}),id='m_prize'+place,roster=[{uid:'a',slot:place,nick:'Human'}];
 const w=P.create({mode:'ranked'});w.winner=0;w.eliminationOrder=[3,2,1];w.gameOver=true;
 const reward=Rules.reward('ranked',w,place),players=Object.fromEntries([0,1,2,3].map(slot=>[slot,{nick:slot===place?'Human':'Bot '+slot}]));
 await ledger.reserve(id,'ranked',roster);assert.equal(f.data.get('users_private/a').silver,900);
 await ledger.recordOutcome(id,{roomId:id,mode:'ranked',players,rewards:{[place]:reward},scores:w.scores,draw:false});
 await Promise.all([ledger.settle(id),ledger.settle(id)]);
 assert.equal(f.data.get('users_private/a').silver,900+[250,150,0,0][place]);assert.equal(f.data.get('users_public/a').rating,500+[50,20,5,-20][place]);
 assert.equal([...f.data.keys()].filter(k=>/^users_private\/[^/]+$/.test(k)).length,1);
});

test('an expired bot-filled match refunds only the human stake once',async()=>{
 const f=database(['a']),clock={now:1000},ledger=createLedger({db:f.db,owner:'test',now:()=>clock.now});await ledger.reserve('m_expired','ranked',[{uid:'a',slot:0,nick:'a'}]);
 clock.now+=61000;await ledger.recover('a','m_expired');await ledger.recover('a','m_expired');assert.equal(f.data.get('users_private/a').silver,1000);assert.equal(f.data.get('users_public/a').gamesPlayed,0);
});
