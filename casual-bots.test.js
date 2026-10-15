'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const fs=require('node:fs'),vm=require('node:vm');
const P=require('./galaxy-physics'),Wire=require('./galaxy-wire'),{Authority}=require('./galaxy-netcode');

test('bots allow time to react, retain aim error, and move slower than players',()=>{
  const w=P.create({spawnDelayMs:100000,players:[{bot:true}]});
  w.respawns=[];w.balls=[{id:1,x:400,y:200,vx:0,vy:3,contact:0}];
  for(let i=0;i<13;i++){P.step(w);assert.equal(w.paddles[0],260);assert.equal(w.botTargets[0],260);}
  P.step(w);const target=w.botTargets[0];assert.notEqual(target,400);assert.ok(target>374&&target<426);
  const prev=w.paddles[0];w.balls[0].x=120;P.step(w);
  assert.equal(w.botTargets[0],target);assert.ok(w.paddles[0]-prev<=3.75*.72+1e-6);
});

test('bot decisions survive wire snapshot and browser replay in every bot mode',()=>{
  const browser=vm.createContext({});vm.runInContext(fs.readFileSync(require.resolve('./galaxy-physics'),'utf8'),browser);
  for(const mode of ['ranked','training','team']){
    const w=P.create({seed:321,spawnDelayMs:1,mode,players:[0,1,2,3].map(s=>({bot:s!==0}))});
    for(let i=0;i<105;i++)P.step(w,{0:{dx:.5}});
    const packet=Wire.snapshot(new Authority({world:w,matchId:'test'}).snapshot());
    const c=browser.GalaxyPhysics.clone(Wire.readSnapshot(packet,w.config).world);
    for(let i=0;i<450;i++){
      const input={0:{dx:Math.sin(i*.02),boost:i%100===0?i:0}};
      P.step(w,input);browser.GalaxyPhysics.step(c,input);
      assert.deepEqual(JSON.parse(JSON.stringify(P.checkpoint(w))),JSON.parse(JSON.stringify(browser.GalaxyPhysics.checkpoint(c))));
    }
  }
});
