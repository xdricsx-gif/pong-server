'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const F=require('./galaxy-fleet'),E=require('./economy-service'),P=require('./galaxy-physics');
function profile(xp=0,silver=50000){return {xp,silver,gold:29,economy:F.economy()};}

test('six visible catalog entries include exactly two starters and unlock the last purchase at player level six',()=>{
 assert.deepEqual(Object.keys(F.skillCatalog),['push','cloak','magnet','rocket','emp','shield']);
 assert.deepEqual(F.economy().ownedSkills,['push','cloak']);assert.deepEqual(F.economy().equippedSkills,['push','cloak']);
 assert.deepEqual(F.normalizeOwnedSkills({skills:Object.fromEntries(F.skillKeys.map(key=>[key,1]))}),['push','cloak']);
 for(const key of ['push','cloak']){assert.equal(F.skillCatalog[key].starter,true);assert.equal(F.skillCatalog[key].price,0);assert.equal(F.skillCatalog[key].requiredLevel,1);}
 for(const [key,level,price] of [['magnet',3,5000],['rocket',4,8000],['emp',5,10000],['shield',6,12000]]){assert.equal(F.skillCatalog[key].requiredLevel,level);assert.equal(F.skillCatalog[key].price,price);const xp=F.LEVEL_XP[level-1];assert.equal(F.skillAccess(F.economy(),key,xp-1).unlocked,false);assert.equal(F.skillAccess(F.economy(),key,xp).unlocked,true);}
 assert.equal(F.skillAccess(F.economy(),'magnet',349).unlocked,false);assert.equal(F.skillAccess(F.economy(),'magnet',350).unlocked,true);
 assert.equal(F.skillAccess(F.economy(),'shield',1599).unlocked,false);assert.equal(F.skillAccess(F.economy(),'shield',1600).unlocked,true);
});

test('authoritative XP level matches the existing client at every threshold',{skip:fs.existsSync(__dirname+'/../app/www/index.html')?false:'Optional companion app source is absent in the server-only checkout'},()=>{
 const html=fs.readFileSync(__dirname+'/../app/www/index.html','utf8'),table=html.match(/const LEVEL_XP\s*=\s*(\[[\s\S]*?\]);/)[1];
 const start=html.indexOf('function calcLevel(xp)'),end=html.indexOf('// XP до наступного рівня',start),browser=vm.createContext({});
 vm.runInContext('const LEVEL_XP='+table+';const MAX_LEVEL=20;'+html.slice(start,end)+';globalThis.level=calcLevel;',browser);
 for(const xp of [-100,0,...F.LEVEL_XP.flatMap(threshold=>[threshold-1,threshold,threshold+1]),999999])assert.equal(F.playerLevel(xp),browser.level(xp),'XP '+xp);
 assert.equal(F.playerLevel(350),3);assert.equal(F.playerLevel(1600),6);
});

test('ownership migration preserves paid upgrades and recorded purchases without gifting level-one abilities',()=>{
 const old=profile();delete old.economy.ownedSkills;old.economy.skills.magnet=3;old.economy.skills.rocket=7;old.economy.skills.emp=8;
 old.economy.equippedSkills=['magnet','shield','cloak'];const original=JSON.stringify(old),first=E.apply(old,{type:'get'});
 assert.deepEqual(first.economy.ownedSkills,['push','cloak','magnet','rocket','emp']);assert.deepEqual(first.economy.equippedSkills,['magnet','cloak']);
 assert.deepEqual(first.economy.skills,old.economy.skills);assert.equal(first.silver,old.silver);assert.equal(first.gold,old.gold);assert.deepEqual(first.migrationRefund,{silver:0,gold:0});assert.equal(JSON.stringify(old),original);
 const second=E.apply({...old,...first},{type:'get'});assert.deepEqual(second,first);
 const recorded=F.economy({economy:{...old.economy,ownedSkills:['shield','shield','emp','unknown']}});assert.deepEqual(recorded.ownedSkills,['push','cloak','magnet','rocket','emp','shield']);
 assert.equal(F.skillAccess(recorded,'shield',0).owned,true);assert.equal(F.skillAccess(recorded,'shield',0).unlocked,true,'paid ownership is retained below a new unlock threshold');
});

test('old equipped selections cannot manufacture ownership and missing selections become an explicit safe set',()=>{
 const old=profile();delete old.economy.ownedSkills;old.economy.equippedSkills=['magnet','shield'];
 const normalized=F.economy(old);assert.deepEqual(normalized.ownedSkills,['push','cloak']);assert.deepEqual(normalized.equippedSkills,['push','cloak']);
 old.economy.equippedSkills=['magnet','push'];assert.deepEqual(F.economy(old).equippedSkills,['push']);
 delete old.economy.equippedSkills;const stats=F.fromAccount(old);assert.deepEqual(stats.equippedSkills,['push','cloak']);
 const w=P.create({players:[{paddleStats:stats}]});P.step(w,{0:{magnet:true,shield:1,rocket:1,rocketTarget:1,emp:1,empX:260,empY:100}});
 assert.equal(w.magnet[0],false);assert.equal(w.goalShields[0],null);assert.equal(w.missiles.length,0);assert.equal(w.empWaves.length,0);assert.equal(w.energy[0],1);
});

test('purchases enforce authoritative XP, exact server price and wallet sufficiency',()=>{
 for(const [key,threshold,price] of [['magnet',350,5000],['rocket',650,8000],['emp',1050,10000],['shield',1600,12000]]){
  const locked=profile(threshold-1),original=JSON.stringify(locked);
  assert.throws(()=>E.apply(locked,{type:'skill_buy',key,xp:999999,playerLevel:20,price:0}),/skill_locked/);assert.equal(JSON.stringify(locked),original);
  const poor=profile(threshold,price-1),poorOriginal=JSON.stringify(poor);assert.throws(()=>E.apply(poor,{type:'skill_buy',key}),/not_enough_silver/);assert.equal(JSON.stringify(poor),poorOriginal);
  const exact=profile(threshold,price),bought=E.apply(exact,{type:'skill_buy',key,price:1,currency:'gold'});
  assert.equal(bought.silver,0);assert.equal(bought.gold,exact.gold);assert.ok(bought.economy.ownedSkills.includes(key));assert.equal(bought.economy.skills[key],1);assert.deepEqual(bought.economy.equippedSkills,['push','cloak']);
 }
});

test('unowned upgrades and equipment are denied even after the player unlocks the purchase',()=>{
 const p=profile(61000);const original=JSON.stringify(p);
 for(const key of ['magnet','rocket','emp','shield']){
  assert.equal(F.skillAccess(p.economy,key,p.xp).unlocked,true);assert.equal(F.skillAccess(p.economy,key,p.xp).owned,false);
  assert.throws(()=>E.apply(p,{type:'skill',key,expectedLevel:1}),/skill_not_owned/);
  assert.throws(()=>E.apply(p,{type:'loadout',equippedSkills:['push',key]}),/skill_not_owned/);
 }
 assert.equal(JSON.stringify(p),original);
 const bought=E.apply(p,{type:'skill_buy',key:'shield'}),upgraded=E.apply({...p,...bought},{type:'skill',key:'shield',expectedLevel:1});
 assert.equal(upgraded.economy.skills.shield,2);assert.equal(upgraded.silver,p.silver-12000-F.skillCosts[1]);
 assert.deepEqual(E.apply({...p,...upgraded},{type:'loadout',equippedSkills:['shield']}).economy.equippedSkills,['shield']);
});

test('repeat purchases are free and retain a purchased level and selection even if XP drops',()=>{
 const p=profile(1600),first=E.apply(p,{type:'skill_buy',key:'magnet'}),next={...p,...first,xp:0};next.economy.skills.magnet=7;next.economy.equippedSkills=['magnet','push'];
 const repeated=E.apply(next,{type:'skill_buy',key:'magnet'});assert.equal(repeated.silver,next.silver);assert.equal(repeated.gold,next.gold);assert.equal(repeated.economy.skills.magnet,7);assert.deepEqual(repeated.economy.equippedSkills,['magnet','push']);
 for(const key of ['push','cloak'])assert.equal(E.apply(next,{type:'skill_buy',key}).silver,next.silver);
});

test('unknown and malformed catalog keys never create a purchase or leak prototype properties',()=>{
 const p=profile(61000),original=JSON.stringify(p);
 for(const key of ['laser','toString','__proto__','constructor','',null]){
  assert.throws(()=>E.apply(p,{type:'skill_buy',key}),/invalid_skill/);
  const access=F.skillAccess(p.economy,key,p.xp);assert.equal(access.owned,false);assert.equal(access.unlocked,false);assert.equal(access.price,null);
 }
 assert.equal(JSON.stringify(p),original);
 assert.deepEqual(F.normalizeOwnedSkills({ownedSkills:['emp','rocket','constructor'],skills:{shield:1,magnet:Infinity}}),['push','cloak','rocket','emp']);
});

test('concurrent purchases charge once; failed commits and rejected equipment cannot grant ownership',async()=>{
 const {database}=require('./match-test-fixture.cjs'),db=database(['a']),handlers={};Object.assign(db.data.get('users_private/a'),profile(1600,50000));
 E.attach({uid:'a',on:(name,fn)=>handlers[name]=fn},db.db);const request=action=>new Promise(resolve=>handlers['economy:action'](action,resolve));
 const results=await Promise.all([request({type:'skill_buy',key:'magnet'}),request({type:'skill_buy',key:'magnet'})]);assert.ok(results.every(r=>!r.error));
 assert.equal(db.data.get('users_private/a').silver,45000);assert.equal(db.data.get('users_private/a').economy.ownedSkills.filter(key=>key==='magnet').length,1);
 const before=JSON.stringify(db.data.get('users_private/a'));assert.equal((await request({type:'loadout',equippedSkills:['shield']})).error,'skill_not_owned');assert.equal(JSON.stringify(db.data.get('users_private/a')),before);
 db.failNext();assert.equal((await request({type:'skill_buy',key:'shield'})).error,'server_error');assert.equal(JSON.stringify(db.data.get('users_private/a')),before);
 const bought=await request({type:'skill_buy',key:'shield'});assert.equal(bought.silver,33000);assert.ok(bought.economy.ownedSkills.includes('shield'));
});

test('socket purchase reads private XP and rejects stale unlocked UI without debiting',async()=>{
 const {database}=require('./match-test-fixture.cjs'),db=database(['a']),handlers={};Object.assign(db.data.get('users_private/a'),profile(349,50000));
 E.attach({uid:'a',on:(name,fn)=>handlers[name]=fn},db.db);const request=action=>new Promise(resolve=>handlers['economy:action'](action,resolve));
 assert.equal((await request({type:'skill_buy',key:'magnet',xp:350,playerLevel:3})).error,'skill_locked');assert.equal(db.data.get('users_private/a').silver,50000);
 db.data.get('users_private/a').xp=350;assert.equal((await request({type:'skill_buy',key:'magnet'})).silver,45000);
});

test('browser UMD and server exports agree on ownership, XP thresholds, migration and authoritative loadouts',()=>{
 const server=fs.readFileSync(__dirname+'/galaxy-fleet.js','utf8'),browserPath=__dirname+'/../app/www/galaxy-fleet.js',browser=fs.existsSync(browserPath)?fs.readFileSync(browserPath,'utf8'):server;assert.equal(browser.replace(/\r\n/g,'\n'),server.replace(/\r\n/g,'\n'));
 const env=vm.createContext({});vm.runInContext(browser,env);
 const legacy=profile(350);delete legacy.economy.ownedSkills;legacy.economy.skills.shield=4;legacy.economy.equippedSkills=['shield','magnet','push'];
 assert.deepEqual(JSON.parse(JSON.stringify(env.GalaxyUfo.economy(legacy))),F.economy(legacy));assert.deepEqual(JSON.parse(JSON.stringify(env.GalaxyUfo.fromAccount(legacy))),F.fromAccount(legacy));
 for(const key of F.activeSkillKeys)for(const xp of [0,349,350,649,650,1049,1050,1599,1600])assert.deepEqual(JSON.parse(JSON.stringify(env.GalaxyUfo.skillAccess(legacy.economy,key,xp))),F.skillAccess(legacy.economy,key,xp));
});


test('recorded level-one rocket and EMP purchases survive migration, equipment and repeated purchase',()=>{
 const p=profile(0);p.economy.ownedSkills=['rocket','emp'];p.economy.equippedSkills=['rocket','emp','push','cloak'];const original=JSON.stringify(p);
 const migrated=E.apply(p,{type:'get'});assert.deepEqual(migrated.economy.ownedSkills,['push','cloak','rocket','emp']);assert.deepEqual(migrated.economy.equippedSkills,p.economy.equippedSkills);
 for(const key of ['rocket','emp']){assert.equal(F.skillAccess(migrated.economy,key,0).owned,true);assert.equal(E.apply({...p,...migrated},{type:'skill_buy',key}).silver,p.silver);}
 const stats=F.fromAccount({...p,...migrated});assert.deepEqual(stats.equippedSkills,p.economy.equippedSkills);assert.equal(stats.skills.rocket,1);assert.equal(stats.skills.emp,1);
 const w=P.create({players:[{paddleStats:stats}]});P.step(w,{0:{rocket:1,rocketTarget:1,emp:1,empX:260,empY:0}});assert.equal(w.missiles.length,1);assert.equal(w.empWaves.length,1);
 assert.equal(JSON.stringify(p),original);
});

test('all six purchases persist while an account can equip only four of them',async()=>{
 const {database}=require('./match-test-fixture.cjs'),db=database(['a']),handlers={};Object.assign(db.data.get('users_private/a'),profile(1600,50000));
 E.attach({uid:'a',on:(name,fn)=>handlers[name]=fn},db.db);const request=action=>new Promise(resolve=>handlers['economy:action'](action,resolve));
 const keys=['magnet','rocket','emp','shield'];
 const results=await Promise.all(keys.flatMap(key=>[request({type:'skill_buy',key}),request({type:'skill_buy',key})]));assert.ok(results.every(r=>!r.error));
 const p=db.data.get('users_private/a');assert.deepEqual(p.economy.ownedSkills,F.activeSkillKeys);assert.equal(p.silver,15000);assert.deepEqual(p.economy.equippedSkills,['push','cloak']);
 const original=JSON.stringify(p);assert.equal((await request({type:'loadout',equippedSkills:F.activeSkillKeys.slice(0,5)})).error,'invalid_loadout');assert.equal(JSON.stringify(db.data.get('users_private/a')),original);
 const saved=await request({type:'loadout',equippedSkills:['rocket','emp','magnet','shield']});assert.deepEqual(saved.economy.equippedSkills,['rocket','emp','magnet','shield']);assert.equal(saved.silver,15000);
 const next=await request({type:'loadout',equippedSkills:['push','cloak']});assert.deepEqual(next.economy.ownedSkills,F.activeSkillKeys);assert.deepEqual(next.economy.equippedSkills,['push','cloak']);
});
