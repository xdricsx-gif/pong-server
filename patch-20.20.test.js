'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const P=require('./galaxy-physics'),{Authority,Client}=require('./galaxy-netcode');
test('three human opponents remain continuous with jitter, lost snapshots and reversals',()=>{
 const a=new Authority({matchId:'jitter',world:P.create({spawnDelayMs:999999})});
 const c=new Client({slot:0,matchId:'jitter',epoch:0});c.setRtt(100);
 const queue=[];let arrivals=0,maxJump=0,maxStep=0,last=null;
 for(let frame=0;frame<1800;frame++){
  const now=frame*P.TICK_MS/2;
  if(frame%2===0){
   const tick=a.world.tick+1;
   for(const slot of [1,2,3]){
    const phase=Math.floor(tick/(19+slot*7))%3;
    a.receive(slot,{version:P.VERSION,matchId:'jitter',epoch:0,commands:[{tick,seq:tick,dx:phase-1,boost:0,magnet:false}]});
   }
   a.advance();
   if(tick%3===0&&tick%21!==0)queue.push({due:now+35+(tick*17)%121,packet:a.snapshot()});
  }
  c.advance(now);
  queue.sort((x,y)=>x.due-y.due);
  while(queue.length&&queue[0].due<=now){
   const before=c.sample(now),packet=queue.shift().packet;
   const accepted=c.receive(packet,now),after=c.sample(now);
   if(accepted&&before){for(const slot of [1,2,3])maxJump=Math.max(maxJump,Math.abs(after.paddles[slot]-before.paddles[slot]));arrivals++;}
  }
  const w=c.sample(now);
  if(w&&last)for(const slot of [1,2,3])maxStep=Math.max(maxStep,Math.abs(w.paddles[slot]-last.paddles[slot]));
  last=w;
 }
 assert.ok(arrivals>150);
 assert.ok(maxJump<.001,`snapshot discontinuity: ${maxJump}`);
 assert.ok(maxStep<10,`8.3ms visual displacement: ${maxStep}`);
 assert.equal(c.metrics.predictionStops,0);
 assert.ok(c.metrics.staleSnapshots>0,'out-of-order snapshots exercised');
});
test('remote reconciliation is visually continuous at 70/100ms RTT without changing physics',()=>{
 for(const rtt of [70,100]){
  const a=new Authority({matchId:'smooth',world:P.create({spawnDelayMs:999999})});
  const c=new Client({slot:0,matchId:'smooth',epoch:a.epochs[0]});c.setRtt(rtt);
  let samples=0,maxJump=0;
  for(let tick=1;tick<=600;tick++){
   const command={tick,seq:tick,dx:Math.floor(tick/40)%2?1:-1,boost:0,magnet:false};
   a.receive(1,{version:P.VERSION,matchId:'smooth',epoch:a.epochs[1],commands:[command]});a.advance();
   const now=tick*P.TICK_MS+rtt/2;c.advance(now);
   if(tick%3===0){
    const before=c.sample(now);c.receive(a.snapshot(),now);const after=c.sample(now);
    if(before){maxJump=Math.max(maxJump,Math.abs(after.paddles[1]-before.paddles[1]));samples++;}
    const hash=P.hash(c.world);c.sample(now+8);assert.equal(P.hash(c.world),hash);
   }
  }
  assert.ok(samples>100);assert.ok(maxJump<.001,'visible packet jump '+maxJump);
 }
});
test('common energy, cloak visibility, attack cancellation and EMP lock survive wire roundtrip',()=>{
 const w=P.create({mode:'team',spawnDelayMs:999999});P.step(w,{0:{cloak:1,dx:0}});
 assert.ok(w.energy[0]<.61);assert.equal(P.shipVisibility(w,0,1),0);assert.equal(P.shipVisibility(w,0,3),.35);
 P.step(w,{0:{cloak:1,rocket:2,rocketTarget:1,dx:0}});assert.equal(P.shipVisibility(w,0,1),1);assert.ok(w.energy[0]<.32);
 P.step(w,{0:{shield:3,dx:0}});assert.equal(w.goalShields[0],null);
 w.empUntil[0]=w.tick+600;w.energy[0]=0;for(let i=0;i<599;i++)P.step(w);assert.equal(w.energy[0],0);
 const Wire=require('./galaxy-wire'),a=new Authority({matchId:'wire',world:w});const p=a.snapshot();
 const decoded=Wire.readSnapshot(Wire.snapshot(p),w.config);assert.deepEqual(decoded.world,p.world);
});


test('cloak expires at ten seconds, costs once, retains cooldown and cannot bypass EMP',()=>{
 const w=P.create({spawnDelayMs:999999});P.step(w,{0:{cloak:1}});
 const end=w.cloakUntil[0],ready=w.cloakReadyAt[0];assert.equal(end-w.tick,600);assert.equal(ready-w.tick,1800);
 for(let i=0;i<599;i++)P.step(w,{0:{cloak:1}});
 assert.equal(P.shipVisibility(w,0,1),0);P.step(w);assert.equal(P.shipVisibility(w,0,1),1);
 P.step(w,{0:{cloak:2}});assert.equal(w.cloakUntil[0],end);assert.equal(w.cloakReadyAt[0],ready);
 while(w.tick<ready)P.step(w);
 w.empUntil[0]=w.tick+60;w.energy[0]=1;P.step(w,{0:{cloak:3}});assert.equal(w.cloakUntil[0],end);
});
