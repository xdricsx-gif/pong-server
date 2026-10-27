'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const F=require('./galaxy-fleet'),E=require('./economy-service'),P=require('./galaxy-physics'),Wire=require('./galaxy-wire'),{Authority}=require('./galaxy-netcode');
test('migration preserves old purchases and refunds currency exactly once',()=>{
 const old={silver:100,gold:20,ufoHangar:{sub:40},ownedPaddles:[0,19],hangars:{19:{sub:37}}};
 const a=E.apply(old,{type:'get'});assert.equal(a.economy.ships.nexus,6);assert.ok(a.gold>20);assert.deepEqual(old.ownedPaddles,[0,19]);assert.equal(old.ufoHangar.sub,40);
 const again=E.apply({...old,...a},{type:'get'});assert.equal(again.gold,a.gold);assert.equal(again.silver,a.silver);assert.deepEqual(again.migrationRefund,{silver:0,gold:0});
});
test('hull and skill prices; replayed request and insufficient wallet cannot debit',()=>{
 const a=E.apply({silver:10000,gold:0},{type:'hull',key:'nexus',expectedLevel:1});assert.equal(a.silver,7000);assert.equal(a.economy.ships.nexus,2);
 assert.throws(()=>E.apply(a,{type:'hull',key:'nexus',expectedLevel:1}),/stale_progress/);
 const b=E.apply(a,{type:'skill',key:'push',expectedLevel:1});assert.equal(b.silver,5000);assert.equal(b.economy.skills.push,2);assert.equal(b.economy.ships.nexus,2);
 assert.throws(()=>E.apply(b,{type:'skill',key:'push',expectedLevel:2}),/not_enough_silver/);
});
test('ships share skills, have independent hulls and are not charged twice',()=>{
 let p=E.apply({silver:200000,gold:1000,xp:1600},{type:'skill_buy',key:'shield'});p=E.apply(p,{type:'skill',key:'shield',expectedLevel:1});p=E.apply(p,{type:'ship',key:'swift',currency:'silver'});assert.equal(p.silver,126000);assert.equal(p.economy.ships.swift,1);assert.equal(p.economy.skills.shield,2);
 const q=E.apply(p,{type:'ship',key:'swift',currency:'gold'});assert.equal(q.gold,p.gold);assert.equal(q.silver,p.silver);
});
test('mission reward is one-time and expires at UTC midnight',()=>{
 const now=Date.UTC(2026,9,18),p={silver:0,gold:0,dailyMissions:{day:'2026-10-18',games:3,wins:1,claimed:{}}};const a=E.apply(p,{type:'quest',key:'games'},now);assert.equal(a.silver,1000);assert.throws(()=>E.apply(a,{type:'quest',key:'games'},now),/quest_unavailable/);assert.throws(()=>E.apply(p,{type:'quest',key:'wins'},now+86400000),/quest_unavailable/);
});
test('skill upgrades reach simulation and survive network roundtrip',()=>{
 const economy=F.economy();economy.skills=Object.fromEntries(F.skillKeys.map(k=>[k,5]));economy.ships.nexus=6;
 const stats=F.fromAccount({economy}),w=P.create({players:[{paddleStats:stats}]});assert.equal(w.config.players[0].paddleStats.skills.emp,5);assert.ok(Math.abs(P.skillCost(w,0,'emp')-.46)<1e-9);assert.ok(Math.abs(P.skillCost(w,0,'cloak')-.36)<1e-9);assert.equal(stats.er,1.1);assert.ok(Math.abs(stats.w-58.32)<1e-9);
 const a=new Authority({matchId:'skills',world:w}),packet=a.snapshot(),decoded=Wire.readSnapshot(Wire.snapshot(packet),w.config);assert.deepEqual(decoded.world.config.players[0].paddleStats.skills,w.config.players[0].paddleStats.skills);
});
test('concurrent socket upgrades commit once and failed commits preserve wallet',async()=>{
 const {database}=require('./match-test-fixture.cjs'),db=database(['a']),handlers={};db.data.get('users_private/a').silver=10000;
 E.attach({uid:'a',on:(name,fn)=>handlers[name]=fn},db.db);
 const request=a=>new Promise(resolve=>handlers['economy:action'](a,resolve));
 const action={type:'hull',key:'nexus',expectedLevel:1},results=await Promise.all([request(action),request(action)]);
 assert.equal(results.filter(r=>!r.error).length,1);assert.equal(db.data.get('users_private/a').silver,7000);
 db.failNext();assert.equal((await request({type:'skill',key:'push',expectedLevel:1})).error,'server_error');assert.equal(db.data.get('users_private/a').silver,7000);
});
test('invalid mission keys cannot turn wallet into NaN',()=>{
 const p={silver:50,dailyMissions:{day:'2026-10-18',games:3,wins:1}};
 for(const key of ['toString','__proto__','constructor'])assert.throws(()=>E.apply(p,{type:'quest',key},Date.UTC(2026,9,18)),/quest_unavailable/);
});
