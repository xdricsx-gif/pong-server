'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),{Worker}=require('node:worker_threads'),{join}=require('node:path');
const P=require('./galaxy-physics');
const wait=ms=>new Promise(r=>setTimeout(r,ms));
test('physics worker keeps ticking through a blocked main thread and coalesces snapshots',{timeout:6000},async t=>{
  const worker=new Worker(join(__dirname,'netcode-worker.js'));t.after(()=>worker.terminate());
  const states=[];worker.on('message',m=>{if(m.type==='state')states.push(m);});
  worker.postMessage({type:'create',id:'isolated',world:P.create()});
  const start=Date.now();while(!states.length){assert.ok(Date.now()-start<3000);await wait(10);}
  const first=states[0].packet.world.tick;
  const blockUntil=performance.now()+250;while(performance.now()<blockUntil){} // Deliberate simulated main-thread stall.
  await wait(30);assert.equal(states.length,1,'worker must not enqueue obsolete snapshots');
  worker.postMessage({type:'snapshotAck',id:'isolated'});
  const end=Date.now();while(states.length<2){assert.ok(Date.now()-end<1000);await wait(10);}
  assert.ok(states[1].packet.world.tick>=first+12,'simulation continued while main thread could not run');
});
