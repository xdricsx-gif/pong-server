'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const F=require('./galaxy-fleet'),E=require('./economy-service'),P=require('./galaxy-physics'),W=require('./galaxy-wire'),{Authority}=require('./galaxy-netcode');
const standardLoadout=['push','magnet','shield','cloak'];
const near=(actual,expected,tolerance=1e-8)=>assert.ok(Math.abs(actual-expected)<tolerance,`${actual} != ${expected}`);
function account(level=1,equippedSkills=standardLoadout){const economy=F.economy();economy.ownedSkills=F.activeSkillKeys.slice();economy.skills=Object.fromEntries(F.skillKeys.map(k=>[k,level]));economy.equippedSkills=equippedSkills.slice();return {silver:2000000,gold:73,economy};}
function world(level=1,equippedSkills=standardLoadout){return P.create({seed:112233,players:[{paddleStats:F.fromAccount(account(level,equippedSkills))}]});}

test('ten levels preserve old prices, purchased strengths and historical ability records',()=>{
 assert.equal(F.MAX_SKILL_LEVEL,10);assert.equal(P.MAX_SKILL_LEVEL,10);
 assert.deepEqual(F.skillCosts.slice(0,5),[0,2000,6000,15000,35000]);
 const original=account(5);original.economy.skills.rocket=4;original.economy.skills.emp=3;
 const result=E.apply(original,{type:'get'});
 assert.deepEqual(result.economy,original.economy);assert.equal(result.silver,original.silver);assert.equal(result.gold,original.gold);
 assert.deepEqual(result.migrationRefund,{silver:0,gold:0});
 for(let level=1;level<=5;level++){
  const p=(level-1)/4,stats=F.fromAccount(account(level)),w=world(level);
  near(stats.fr,54*(1+.12*p));near(P.skillCost(w,0,'push'),1/3*(1-.1*p));near(P.skillCost(w,0,'cloak'),.4*(1-.1*p));
  near(P.magnetRadius(stats),75*(1+.12*p));
  P.step(w,{0:{cloak:1}});assert.equal(w.cloakUntil[0]-w.tick,600);assert.equal(w.cloakReadyAt[0]-w.tick,1800-Math.round(120*p));
  const shield=world(level);P.step(shield,{0:{shield:1}});assert.equal(shield.goalShields[0].until-shield.goalShields[0].activeAt,600+Math.round(60*p));
 }
});

test('each active ability reaches level ten with safe prices, stale protection and a hard cap',()=>{
 for(const key of F.activeSkillKeys){
  let p=account(1);const original=JSON.stringify(p);let spent=0;
  for(let level=1;level<F.MAX_SKILL_LEVEL;level++){
   const cost=F.skillCosts[level];assert.ok(Number.isSafeInteger(cost)&&cost>0);if(level>1)assert.ok(cost>F.skillCosts[level-1]);
   const old=p;p=E.apply(p,{type:'skill',key,expectedLevel:level});spent+=cost;
   assert.equal(p.economy.skills[key],level+1);assert.equal(p.silver,2000000-spent);assert.equal(p.gold,73);
   const snapshot=JSON.stringify(p);assert.throws(()=>E.apply(p,{type:'skill',key,expectedLevel:level}),/stale_progress/);assert.equal(JSON.stringify(p),snapshot);
   assert.equal(old.economy.skills[key],level);
  }
  assert.equal(p.economy.skills[key],10);assert.throws(()=>E.apply(p,{type:'skill',key,expectedLevel:10}),/max_level/);
  assert.equal(JSON.parse(original).economy.skills[key],1);
 }
});

test('insufficient currency and invalid upgrades leave progress and wallet intact',()=>{
 const p=account(9);p.silver=F.skillCosts[9]-1;const snapshot=JSON.stringify(p);
 assert.throws(()=>E.apply(p,{type:'skill',key:'push',expectedLevel:9}),/not_enough_silver/);
 for(const key of ['laser','toString','__proto__'])assert.throws(()=>E.apply(p,{type:'skill',key,expectedLevel:9}),/invalid_skill/);
 assert.equal(JSON.stringify(p),snapshot);
});

test('shared effect values match simulation and improve monotonically through level ten',()=>{
 let previous;
 for(let level=1;level<=10;level++){
  const w=world(level),stats=w.config.players[0].paddleStats;
  const push=F.skillEffects('push',level),magnet=F.skillEffects('magnet',level),shield=F.skillEffects('shield',level),cloak=F.skillEffects('cloak',level);
  near(push.radius,stats.fr);near(push.energyCost,P.skillCost(w,0,'push'));
  near(magnet.radius,P.magnetRadius(stats));P.updateMagnetEnergy(w,0,true,1000);near(w.energy[0],1-magnet.energyCost-magnet.drainPerSecond,1e-6);
  const sw=world(level);P.step(sw,{0:{shield:1}});near((sw.goalShields[0].until-sw.goalShields[0].activeAt)/60,shield.durationSeconds);near((sw.shieldReadyAt[0]-sw.tick)/60,shield.cooldownSeconds);
  const cw=world(level);P.step(cw,{0:{cloak:1}});near((cw.cloakUntil[0]-cw.tick)/60,cloak.durationSeconds);near((cw.cloakReadyAt[0]-cw.tick)/60,cloak.cooldownSeconds);near(P.skillCost(cw,0,'cloak'),cloak.energyCost);
  if(previous){assert.ok(push.radius>previous.push.radius);assert.ok(push.energyCost<previous.push.energyCost);assert.ok(magnet.radius>previous.magnet.radius);assert.ok(magnet.drainPerSecond<previous.magnet.drainPerSecond);assert.ok(shield.durationSeconds>previous.shield.durationSeconds);assert.ok(cloak.cooldownSeconds<previous.cloak.cooldownSeconds);assert.ok(cloak.energyCost<previous.cloak.energyCost);}
  previous={push,magnet,shield,cloak};
 }
 near(previous.push.radius,68.58);near(previous.magnet.radius,95.25);near(previous.shield.durationSeconds,12.25);near(previous.cloak.cooldownSeconds,25.5);
 const clamped=P.create({players:[{paddleStats:{level:99,skills:{push:100,magnet:100,cloak:100}}}]});
 assert.equal(clamped.config.players[0].paddleStats.level,5,'ship model index remains separate from ability level');
 assert.equal(clamped.config.players[0].paddleStats.skills.push,10);near(P.magnetRadius({skills:{magnet:100}}),previous.magnet.radius);
 assert.equal(F.skillProgress(100),2.25);assert.equal(F.skillProgress(-8),0);
});

test('loadout saves one to four current abilities without charging or changing upgrades',()=>{
 const p=account(7);const original=JSON.stringify(p);
 for(const loadout of [['magnet'],['rocket'],['emp'],['cloak','push'],['shield','magnet','cloak'],['push','rocket','emp','cloak']]){
  const next=E.apply(p,{type:'loadout',equippedSkills:loadout});assert.deepEqual(next.economy.equippedSkills,loadout);assert.equal(next.silver,p.silver);assert.equal(next.gold,p.gold);assert.deepEqual(next.economy.skills,p.economy.skills);
  assert.notEqual(next.economy.equippedSkills,loadout,'response owns a copy');const savedOrder=next.economy.equippedSkills.slice();loadout.reverse();assert.deepEqual(next.economy.equippedSkills,savedOrder);
 }
 for(const invalid of [undefined,null,{},[],['push','push'],['laser'],['push','magnet','rocket','emp','shield'],F.activeSkillKeys.slice(),['__proto__']])assert.throws(()=>E.apply(p,{type:'loadout',equippedSkills:invalid}),/invalid_loadout/);
 assert.equal(JSON.stringify(p),original);
 assert.deepEqual(F.economy().equippedSkills,['push','cloak']);
});

test('account migration now supplies an ownership-gated loadout while raw legacy physics remains compatible',()=>{
 const p=account(5);delete p.economy.equippedSkills;
 assert.deepEqual(E.apply(p,{type:'get'}).economy.equippedSkills,['push','cloak']);
 const stats=F.fromAccount(p),accountWorld=P.create({players:[{paddleStats:stats}]});assert.deepEqual(stats.equippedSkills,['push','cloak']);
 for(const key of F.skillKeys)assert.equal(P.skillEquipped(accountWorld,0,key),['push','cloak'].includes(key));
 const legacy=P.create();for(const key of F.skillKeys)assert.equal(P.skillEquipped(legacy,0,key),true);
 P.step(legacy,{0:{rocket:1,rocketTarget:1}});assert.equal(legacy.missiles.length,1,'legacy combat remains compatible');
 for(const explicit of [[],null,'push',['laser','unknown']]){
  const w=P.create({players:[{paddleStats:{equippedSkills:explicit}}]});
  assert.deepEqual(w.config.players[0].paddleStats.equippedSkills,[]);
  w.energy[0]=.5;
  P.step(w,{0:{boost:1,magnet:true,cloak:1,shield:1,rocket:1,rocketTarget:1,emp:1,empX:260,empY:100}});
  assert.ok(w.energy[0]>.5,'denied magnet cannot suppress regeneration');assert.equal(w.fields[0].active,false);assert.equal(w.magnet[0],false);assert.equal(w.cloakUntil[0],0);assert.equal(w.goalShields[0],null);assert.equal(w.missiles.length,0);assert.equal(w.empWaves.length,0);
  for(const key of ['lastBoost','lastCloak','lastShield','lastRocket','lastEmp'])assert.equal(w[key][0],1,'denied commands are consumed');
  for(const key of ['cloakReadyAt','shieldReadyAt','rocketReadyAt','empReadyAt'])assert.equal(w[key][0],0);
  const before=w.energy[0];P.updateMagnetEnergy(w,0,true,1000);assert.equal(w.energy[0],before);
 }
});

test('only equipped abilities activate even when clients submit every command',()=>{
 for(const key of F.activeSkillKeys){
  const w=world(10,[key]);P.step(w,{0:{boost:1,magnet:true,cloak:1,shield:1,rocket:1,rocketTarget:1,emp:1,empX:260,empY:100}});
  assert.equal(w.fields[0].active,key==='push');assert.equal(w.magnet[0],key==='magnet');assert.equal(w.cloakUntil[0]>w.tick,key==='cloak');assert.equal(!!w.goalShields[0],key==='shield');assert.equal(w.missiles.length,key==='rocket'?1:0);assert.equal(w.empWaves.length,key==='emp'?1:0);
 }
});

test('server and browser retain identical level-ten loadouts through replay and wire snapshots',()=>{
 const client=vm.createContext({});
 for(const file of ['galaxy-fleet.js','galaxy-physics.js']){
  const server=fs.readFileSync(__dirname+'/'+file,'utf8'),browserPath=__dirname+'/../app/www/'+file,browser=fs.existsSync(browserPath)?fs.readFileSync(browserPath,'utf8'):server;assert.equal(browser.replace(/\r\n/g,'\n'),server.replace(/\r\n/g,'\n'),file+' parity');vm.runInContext(browser,client);
 }
 const p=account(10,['shield','push','cloak','magnet']);
 assert.deepEqual(JSON.parse(JSON.stringify(client.GalaxyUfo.fromAccount(p))),F.fromAccount(p));
 const options={seed:8899,players:[{paddleStats:F.fromAccount(p)},{paddleStats:F.fromAccount(account(8,['rocket','emp']))}]},a=P.create(options),b=client.GalaxyPhysics.create(options);
 for(let tick=0;tick<1200;tick++){
  const commands={0:{dx:Math.sin(tick/80),boost:Math.floor(tick/90),shield:Math.floor(tick/500),cloak:Math.floor(tick/450),magnet:tick%300>250,rocket:tick+1,rocketTarget:1,emp:tick+1,empX:260,empY:100},1:{rocket:Math.floor(tick/550)+1,rocketTarget:0,emp:Math.floor(tick/600)+1,empX:260,empY:450}};
  P.step(a,commands);client.GalaxyPhysics.step(b,commands);if(tick%60===0)assert.equal(client.GalaxyPhysics.hash(b),P.hash(a));
 }
 const packet=new Authority({matchId:'skills10',world:a}).snapshot(),decoded=W.readSnapshot(W.snapshot(packet),a.config).world;
 assert.deepEqual(decoded.config.players[0].paddleStats.equippedSkills,p.economy.equippedSkills);assert.equal(decoded.config.players[0].paddleStats.skills.push,10);assert.equal(P.hash(decoded),P.hash(a));
});

test('concurrent level-nine upgrades commit once and loadout socket validation is atomic',async()=>{
 const {database}=require('./match-test-fixture.cjs'),db=database(['a']),handlers={};
 Object.assign(db.data.get('users_private/a'),account(9));E.attach({uid:'a',on:(name,fn)=>handlers[name]=fn},db.db);
 const request=action=>new Promise(resolve=>handlers['economy:action'](action,resolve)),action={type:'skill',key:'push',expectedLevel:9};
 const results=await Promise.all([request(action),request(action)]);assert.equal(results.filter(r=>!r.error).length,1);assert.equal(results.filter(r=>r.error==='stale_progress').length,1);
 const after=db.data.get('users_private/a');assert.equal(after.economy.skills.push,10);assert.equal(after.silver,2000000-F.skillCosts[9]);
 const before=JSON.stringify(after);assert.equal((await request({type:'loadout',equippedSkills:['laser']})).error,'invalid_loadout');assert.equal(JSON.stringify(db.data.get('users_private/a')),before);
 const saved=await request({type:'loadout',equippedSkills:['magnet','shield']});assert.deepEqual(saved.economy.equippedSkills,['magnet','shield']);assert.equal(saved.silver,after.silver);
});


test('rocket and EMP upgrades match their live effects through all ten levels and keep old strengths',()=>{
 let previous;
 for(let level=1;level<=10;level++){
  const p=(level-1)/4,rocket=F.skillEffects('rocket',level),emp=F.skillEffects('emp',level);
  const rw=world(level,['rocket']);P.step(rw,{0:{rocket:1,rocketTarget:1}});assert.equal(rw.missiles.length,1);
  near((rw.missiles[0].launchAt-rw.tick)/60,rocket.launchDelaySeconds);near((rw.rocketReadyAt[0]-rw.missiles[0].launchAt)/60,rocket.reloadSeconds);near((rw.rocketReadyAt[0]-rw.tick)/60,rocket.cooldownSeconds);near(rocket.energyCost,P.skillCost(rw,0,'rocket'));near(rocket.stunSeconds,P.constants.ROCKET_STUN/60);
  const ew=world(level,['emp']);P.step(ew,{0:{emp:1,empX:260,empY:0}});assert.equal(ew.empWaves.length,1);
  near((ew.empReadyAt[0]-ew.tick)/60,emp.cooldownSeconds);near(emp.energyCost,P.skillCost(ew,0,'emp'));near(emp.chargeSeconds,P.constants.EMP_CHARGE/60);near(emp.lockSeconds,P.constants.EMP_LOCK/60);
  if(level<=5){near(rocket.reloadSeconds,Math.round(1800*(1-.10*p))/60);near(emp.energyCost,.50*(1-.08*p));near(emp.cooldownSeconds,(1800-Math.round(120*p))/60);}
  if(previous){assert.ok(rocket.cooldownSeconds<previous.rocket.cooldownSeconds);assert.ok(emp.cooldownSeconds<previous.emp.cooldownSeconds);assert.ok(emp.energyCost<previous.emp.energyCost);}
  previous={rocket,emp};
 }
 near(previous.rocket.cooldownSeconds,24.35);near(previous.rocket.reloadSeconds,23.25);near(previous.emp.cooldownSeconds,25.5);near(previous.emp.energyCost,.41);
});
