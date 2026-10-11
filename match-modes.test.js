'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const P=require('./galaxy-physics'),N=require('./galaxy-netcode'),Wire=require('./galaxy-wire'),Rules=require('./match-rules'),{createLedger}=require('./match-ledger'),{database}=require('./match-test-fixture.cjs');
function world(mode){const w=P.create({mode});w.balls=[];w.respawns=[];return w;}
function goal(w,s){const positions=[[260,514,0,4],[260,6,0,-4],[6,260,-4,0],[514,260,4,0]];w.paddles[s]=150;const [x,y,vx,vy]=positions[s];w.balls=[{id:w.nextBallId++,x,y,vx,vy,contact:0}];P.step(w);}
test('1v1 has two players; unused walls reflect normally without charging a goal or boosting',()=>{
 const w=world('duel');assert.deepEqual(w.config.activeSlots,[0,1]);assert.deepEqual(w.eliminated,[false,false,true,true]);
 goal(w,2);assert.deepEqual(w.scores,[0,0,0,0]);assert.equal(w.balls[0].vx,4);assert.equal(w.gameOver,false);
 for(let i=0;i<10;i++)goal(w,0);assert.equal(w.winner,1);assert.equal(w.gameOver,true);assert.deepEqual(Rules.places(w),[1,0]);
});
test('adjacent teammates share all 20 conceded goals and lose together',()=>{
 const w=world('team');assert.deepEqual(w.config.teams,[0,1,1,0]);for(let i=0;i<9;i++){goal(w,0);goal(w,3);}
 assert.deepEqual(w.teamScores,[18,0]);assert.equal(w.lives[0],2);assert.equal(w.lives[3],2);assert.equal(w.eliminated[0],false);
 goal(w,0);goal(w,3);assert.equal(w.winnerTeam,1);assert.equal(w.eliminated[0],true);assert.equal(w.eliminated[3],true);assert.equal(Rules.reward('team',w,1).silver,200);assert.equal(Rules.reward('team',w,2).ratingDelta,25);
});
test('same-tick final team goals resolve a draw, never an arbitrary slot victory',()=>{
 const w=world('team');w.teamScores=[19,19];w.teamLives=[1,1];w.lives=[1,1,1,1];w.paddles=[150,150,150,150];w.balls=[{id:1,x:260,y:514,vx:0,vy:4},{id:2,x:260,y:6,vx:0,vy:-4}];P.step(w);
 assert.equal(w.winner,null);assert.equal(w.winnerTeam,null);assert.equal(Rules.reward('team',w,0).silver,100);
});
test('a disconnected teammate cannot turn their goal into an invulnerable wall',()=>{
 const w=world('team');P.step(w,{},[{type:'eliminate',slot:3,reason:'timeout'}]);assert.equal(w.gameOver,false);goal(w,3);assert.equal(w.teamScores[0],1);assert.equal(w.lives[0],19);
});
test('tied timed FFA refunds the whole pot instead of selecting a winner by slot number',()=>{
 const w=world('ranked');w.config.durationTicks=1;P.step(w);assert.equal(w.winner,null);assert.equal(Rules.reward('ranked',w,3).silver,100);
});
test('all-player timeout does not manufacture a win; deadline ties in private/team games refund',()=>{
 const w=world('ranked');P.step(w,{},[0,1,2,3].map(slot=>({type:'eliminate',slot,reason:'timeout'})));assert.equal(w.winner,null);
 for(const mode of ['duel','team']){const t=world(mode);t.config.durationTicks=1;P.step(t);assert.equal(t.winner,null);assert.equal(Rules.reward(mode,t,0).ratingDelta,0);}
});
for(const mode of ['duel','team'])test(mode+' survives wire serialization and late-input rollback identically',()=>{
 const w=world(mode);const a=new N.Authority({matchId:'m_replay',world:w}),b=new N.Authority({matchId:'m_replay',world:w});const command={version:P.VERSION,matchId:'m_replay',epoch:0,commands:[{tick:2,seq:1,dx:1,magnet:true,boost:1}]};
 a.receive(0,command);for(let i=0;i<6;i++){a.advance();b.advance();}b.receive(0,command);b.flush();assert.equal(P.hash(a.world),P.hash(b.world));const snap=b.snapshot();assert.equal(P.hash(Wire.readSnapshot(Wire.snapshot(snap),w.config).world),P.hash(b.world));
});
function setup(mode='ranked'){const f=database(),clock={t:100000},ledger=createLedger({db:f.db,owner:'test',now:()=>clock.t});const roster=['a','b','c','d'].slice(0,mode==='duel'?2:mode==='training'?1:4).map((uid,slot)=>({uid,slot,nick:uid}));const w=world(mode);w.gameOver=true;w.winner=0;w.winnerTeam=mode==='team'?0:null;w.eliminationOrder=[3,2,1];
 const result={roomId:'m_one',mode,participants:roster,players:{},places:Rules.places(w),scores:w.scores,draw:false,rewards:Object.fromEntries(roster.map(p=>[p.slot,Rules.reward(mode,w,p.slot)]))};return {...f,ledger,clock,roster,result};}
test('FFA stake and exact approved payouts commit once under concurrent reserve/settle/recovery',async()=>{
 const f=setup();await Promise.all([f.ledger.reserve('m_one','ranked',f.roster),f.ledger.reserve('m_one','ranked',f.roster)]);assert.equal(f.data.get('users_private/a').silver,900);
 await f.ledger.recordOutcome('m_one',f.result);await Promise.all([f.ledger.settle('m_one'),f.ledger.settle('m_one'),f.ledger.recover('a','m_one')]);
 assert.deepEqual(['a','b','c','d'].map(u=>f.data.get('users_private/'+u).silver),[1150,1050,900,900]);assert.equal(f.data.get('users_public/a').gamesPlayed,1);assert.equal(f.data.get('users_public/b').wins,0);
});
test('team payout and separate rating do not alter FFA rating',async()=>{
 const f=setup('team');await f.ledger.reserve('m_one','team',f.roster);await f.ledger.recordOutcome('m_one',f.result);await f.ledger.settle('m_one');
 for(const u of ['a','d']){assert.equal(f.data.get('users_private/'+u).silver,1100);assert.equal(f.data.get('users_public/'+u).teamRating,525);assert.equal(f.data.get('users_public/'+u).rating,500);}assert.equal(f.data.get('users_public/b').teamRating,475);
});
test('private duel has no currency, XP, energy, competitive wins or games impact',async()=>{
 const f=setup('duel');await f.ledger.reserve('m_one','duel',f.roster);await f.ledger.recordOutcome('m_one',f.result);await f.ledger.settle('m_one');const w=f.data.get('users_private/a');assert.equal(w.silver,1000);assert.equal(w.xp,0);assert.equal(w.energy,100);assert.equal(f.data.get('users_public/a').gamesPlayed,0);
});
test('training persists real rewards and the energy cost exactly once',async()=>{
 const f=setup('training');await f.ledger.reserve('m_one','training',f.roster);await f.ledger.recordOutcome('m_one',f.result);await f.ledger.settle('m_one');const w=f.data.get('users_private/a');assert.equal(w.silver,2500);assert.equal(w.xp,150);assert.equal(w.energy,80);
});
test('insufficient funds or concurrent different matches cannot partially debit',async()=>{
 const f=setup();f.data.get('users_private/d').silver=99;await assert.rejects(f.ledger.reserve('m_one','ranked',f.roster),/not_enough_silver/);assert.equal(f.data.get('users_private/a').silver,1000);
 f.data.get('users_private/d').silver=1000;await f.ledger.reserve('m_one','ranked',f.roster);await assert.rejects(f.ledger.reserve('m_two','ranked',f.roster),/already_playing/);
});
test('failed persistence never acknowledges success; retries preserve one result and one payment',async()=>{
 const f=setup();await f.ledger.reserve('m_one','ranked',f.roster);await f.ledger.recordOutcome('m_one',f.result);f.failNext();await assert.rejects(f.ledger.settle('m_one'),/write_failed/);
 assert.equal(f.data.get('match_sessions/m_one').state,'settling');assert.equal(f.data.get('users_private/a').silver,900);const result=await f.ledger.recover('a','m_one');assert.equal(result.serverCommitted,true);assert.equal(f.data.get('users_private/a').silver,1150);
});
test('expired server lease refunds stake once; another player cannot retrieve a result',async()=>{
 const f=setup();await f.ledger.reserve('m_one','ranked',f.roster);await assert.rejects(f.ledger.recover('outsider','m_one'),/wrong_uid/);f.clock.t+=61000;await f.ledger.recover('a','m_one');await f.ledger.recover('a','m_one');assert.equal(f.data.get('users_private/a').silver,1000);assert.equal(f.data.get('users_public/a').gamesPlayed,0);
});
test('recovering an old receipt reads today’s wallet, not the historical paid balance',async()=>{
 const f=setup();await f.ledger.reserve('m_one','ranked',f.roster);await f.ledger.recordOutcome('m_one',f.result);await f.ledger.settle('m_one');f.data.get('users_private/a').silver=777;
 await f.ledger.recover('a','m_one');assert.equal((await f.ledger.wallet('a')).silver,777);
});
