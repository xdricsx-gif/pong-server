'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const P=require('./galaxy-physics');
test('battle energy waits for first launch, survives checkpoint, then continues without balls',()=>{
 const w=P.create({emptyStartEnergy:true,spawnDelayMs:1000});
 while(w.nextBallId===1){assert.deepEqual(w.energy,[0,0,0,0]);P.step(w);}
 const resumed=P.clone(w);
 for(let i=0;i<20;i++){P.step(w);P.step(resumed);}
 assert.ok(w.energy[0]>0);assert.deepEqual(w.energy,resumed.energy);
 const energy=w.energy[0];w.balls=[];w.respawns=[];P.step(w);
 assert.ok(w.energy[0]>energy);
});
