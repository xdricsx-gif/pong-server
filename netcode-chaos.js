'use strict';
const assert=require('node:assert/strict'),{performance}=require('node:perf_hooks');
const P=require('./galaxy-physics'),{Authority,Client,HISTORY}=require('./galaxy-netcode');
const Wire=require('./galaxy-wire');
function rng(seed){return ()=>{seed=(Math.imul(seed,1664525)+1013904223)>>>0;return seed/4294967296;};}
function percentile(a,p){if(!a.length)return 0;const s=a.slice().sort((x,y)=>x-y);return +s[Math.min(s.length-1,Math.floor(s.length*p))].toFixed(3);}
function run({rtt=100,jitter=15,loss=.02,ordered=true,burst=false,asymmetric=false,fps=60,seed=1,duration=60000}={}){
  const random=rng(seed),queue=[],commits=[],corrections=[],tickTimes=[],frameTimes=[];
  let now=0,serial=0,lastUp=[0,0,0,0],lastDown=[0,0,0,0],maxQueue=0,bytes=0,frames=0,stalled=0;
  const world=P.create({seed,spawnDelayMs:0,durationTicks:36000});
  // Keep four balls interacting throughout the test without shortening it through elimination.
  world.lives=[1000,1000,1000,1000];
  const a=new Authority({matchId:'chaos',world,onCommit:e=>commits.push(e.tick)});
  const clients=[];
  function network(slot,direction,fn,reliable=false){
    if(!reliable&&random()<loss)return;
    let oneWay=rtt/2*(asymmetric?(direction==='up'?1.6:.4):1);
    let at=now+Math.max(1,oneWay+(random()-.5)*2*jitter);
    if(burst&&Math.floor(now/9000)%2===1&&now%9000<500)at+=500-now%9000;
    const channel=direction==='up'?lastUp:lastDown;
    if(ordered){at=Math.max(at,channel[slot]+.01);channel[slot]=at;}
    queue.push({at,serial:serial++,fn});maxQueue=Math.max(maxQueue,queue.length);
  }
  function packet(){return {...a.snapshot(),sentAt:now,phase:0};}
  for(let slot=0;slot<4;slot++){
    const c=new Client({slot,matchId:'chaos',epoch:0,send:p=>network(slot,'up',()=>a.receive(slot,p)),
      requestSync:()=>network(slot,'up',()=>{const p=packet();network(slot,'down',()=>c.receive(p,now),true);},true)});
    c.setRtt(rtt);c.clockOffset=0;c.receive(packet(),0);clients.push(c);
  }
  let nextTick=P.TICK_MS,nextFrame=[0,0,0,0],lastBoost=[-1,-1,-1,-1];
  for(now=0;now<=duration;now+=1000/120){
    queue.sort((a,b)=>a.at-b.at||a.serial-b.serial);
    while(queue.length&&queue[0].at<=now)queue.shift().fn();
    while(nextTick<=now+.001){const t=performance.now();a.advance();tickTimes.push(performance.now()-t);nextTick+=P.TICK_MS;
      if(a.world.tick%2===0){const p=packet();bytes+=JSON.stringify(Wire.snapshot(p)).length*4;
        for(let slot=0;slot<4;slot++)network(slot,'down',()=>{const c=clients[slot];if(c.receive(p,now))corrections.push(...c.lastCorrection);});}}
    for(let slot=0;slot<4;slot++)if(now>=nextFrame[slot]){
      const c=clients[slot],period=600+slot*93,phase=Math.floor(now/period),boostPhase=Math.floor(now/(1400+slot*117));
      c.setInput({dx:phase%3-1,magnet:now%(2300+slot*133)<400,boost:lastBoost[slot]!==boostPhase});lastBoost[slot]=boostPhase;
      const start=performance.now();c.advance(now);const view=c.sample(now);frameTimes.push(performance.now()-start);
      frames++;if(c.needsSync)stalled++;
      assert.ok(c.pending.size<=30);assert.ok(c.world.tick<=c.base.tick+20);
      for(const b of view.balls){assert.ok(Number.isFinite(b.x)&&Number.isFinite(b.y));assert.ok(Math.abs(b.x)<1000&&Math.abs(b.y)<1000);}
      nextFrame[slot]+=1000/(slot===2?30:fps);
    }
  }
  assert.equal(new Set(commits).size,commits.length);assert.ok(a.history.size<=HISTORY+1);
  // An authoritative recovery snapshot must converge, including PRNG and all magnet flags.
  // Finish the uplink and let it commit before comparing immutable recovery state.
  // A provisional snapshot is deliberately NOT allowed to erase unresolved local input.
  for(const c of clients)a.receive(c.slot,{version:P.VERSION,matchId:a.matchId,epoch:c.epoch,commands:[...c.pending.values()].slice(-16)});
  for(let t=0;t<30;t++)a.advance();
  const final=packet();for(const c of clients){c.world=null;c.receive(final,now);assert.equal(P.hash(c.world),P.hash(a.world));}
  return {rtt,jitter,loss,ordered,burst,asymmetric,fps,seed,durationMs:duration,
    correctionPx:{p50:percentile(corrections,.5),p95:percentile(corrections,.95),p99:percentile(corrections,.99),max:percentile(corrections,1)},
    serverTickMs:{p95:percentile(tickTimes,.95),p99:percentile(tickTimes,.99),max:percentile(tickTimes,1)},
    clientFrameMs:{p95:percentile(frameTimes,.95),p99:percentile(frameTimes,.99)},
    stalledFramesPct:+(100*stalled/frames).toFixed(2),downstreamBytesPerClientSecond:Math.round(bytes/4/(duration/1000)),maxQueue,
    authority:a.metrics,clients:clients.map(c=>c.metrics)};
}
if(require.main===module){
  const scenarios=[
    {rtt:70,jitter:5,loss:0},{rtt:100,jitter:15,loss:.02},{rtt:150,jitter:25,loss:.03},
    {rtt:200,jitter:35,loss:.05},{rtt:250,jitter:45,loss:.05},
    {rtt:100,jitter:20,loss:.03,burst:true},{rtt:150,jitter:35,loss:.05,ordered:false},
    {rtt:200,jitter:30,loss:.03,asymmetric:true,fps:30}];
  const runs=[];for(const scenario of scenarios)for(const seed of [11,42,97])runs.push(run({...scenario,seed}));
  const report={createdAt:new Date().toISOString(),description:'4 independent real controllers; application snapshot loss, ordered HOL stalls and unordered delivery. Simulation, not a cellular field trial.',runs};
  if(process.argv[2])require('node:fs').writeFileSync(process.argv[2],JSON.stringify(report,null,2));
  for(const r of runs)console.log(JSON.stringify({rtt:r.rtt,seed:r.seed,burst:r.burst,ordered:r.ordered,asym:r.asymmetric,
    correction:r.correctionPx,serverP99:r.serverTickMs.p99,clientP99:r.clientFrameMs.p99,stalled:r.stalledFramesPct,late:r.authority.tooLate}));
}
module.exports={run};
