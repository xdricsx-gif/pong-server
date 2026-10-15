'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const Fleet=require('./galaxy-fleet'),{database}=require('./match-test-fixture.cjs');
const source=fs.readFileSync(require.resolve('./server'),'utf8');
function fixture(sub=0){
 const f=database(['a']),handlers={},messages=[];
 const account=f.data.get('users_private/a');Object.assign(account,{gold:100,ownedPaddles:[0,7,19],hangars:{0:{sub:40},19:{sub:37}},ufoHangar:{sub}});
 const context=vm.createContext({db:f.db,socket:{uid:'a',on:(event,handler)=>handlers[event]=handler,emit:(event,payload)=>messages.push({event,payload})},UfoFleet:Fleet,MAX_SUB_SRV:40,subToLevelSrv:Fleet.progress,getUpgradeCostSrv:()=>({cur:'silver',price:100}),trackEvent(){},console});
 const a=source.indexOf("  socket.on('shop:upgrade_next',"),b=source.indexOf('\n  });',a)+6;vm.runInContext(source.slice(a,b),context);
 return {...f,messages,upgrade:params=>handlers['shop:upgrade_next']({paddleId:0,fleetVersion:Fleet.VERSION,...params})};
}
test('UFO purchase updates only new progress and charges approved currency once',async()=>{
 const f=fixture(9);await f.upgrade();const account=f.data.get('users_private/a');assert.equal(account.ufoHangar.sub,10);assert.equal(account.silver,900);assert.deepEqual(account.ownedPaddles,[0,7,19]);assert.deepEqual(account.hangars,{0:{sub:40},19:{sub:37}});assert.equal(f.messages[0].payload.level,2);
});
test('stale fleet, invalid ship and insufficient funds never debit or replace old progress',async()=>{
 for(const params of [{fleetVersion:'old'},{paddleId:7}]){const f=fixture();await f.upgrade(params);assert.equal(f.data.get('users_private/a').silver,1000);assert.equal(f.data.get('users_private/a').ufoHangar.sub,0);assert.equal(f.messages[0].event,'shop:error');}
 const f=fixture();f.data.get('users_private/a').silver=99;await f.upgrade();assert.equal(f.data.get('users_private/a').ufoHangar.sub,0);assert.equal(f.data.get('users_private/a').silver,99);
});
test('concurrent requests at final upgrade do not exceed level five or double-charge',async()=>{
 const f=fixture(39);await Promise.all([f.upgrade(),f.upgrade()]);assert.equal(f.data.get('users_private/a').ufoHangar.sub,40);assert.equal(f.data.get('users_private/a').silver,900);assert.equal(f.messages.filter(m=>m.event==='shop:upgraded_next').length,1);
});
