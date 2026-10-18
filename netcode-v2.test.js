'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const P=require('./galaxy-physics'),{Authority,Client,WINDOW,HISTORY}=require('./galaxy-netcode');
function world(){const w=P.create({seed:456,spawnDelayMs:0});w.respawns=[];return w;}
function authority(w=world(),commit=()=>{}){return new Authority({matchId:'test',world:w,onCommit:commit});}
const cmd=(tick,dx=0,boost=0,magnet=false)=>({tick,seq:tick,dx,boost,magnet,target:null,rocket:0,rocketTarget:-1});
function send(a,slot,...commands){return a.receive(slot,{version:P.VERSION,matchId:'test',epoch:a.epochs[slot],commands});}
test('checkpoint contains every future-affecting value, including PRNG and held balls',()=>{
  const a=P.create({seed:23,players:[{bot:true},{bot:true},{bot:true},{bot:true}],spawnDelayMs:0});
  for(let i=0;i<1500;i++)P.step(a);
  const b=P.checkpoint(a);for(let i=0;i<1800;i++){P.step(a);P.step(b);assert.equal(P.hash(a),P.hash(b));}
});
test('browser and Node execute byte-identical physics over an entire match',()=>{
  const ctx=vm.createContext({});vm.runInContext(fs.readFileSync(require.resolve('./galaxy-physics'),'utf8'),ctx);
  const a=P.create({seed:88,spawnDelayMs:0}),b=ctx.GalaxyPhysics.create({seed:88,spawnDelayMs:0});
  for(let t=1;t<=10800;t++){
    const input=Object.fromEntries(P.SLOTS.map(s=>[s,cmd(t,Math.floor(t/47+s)%3-1,Math.floor(t/91),t%211<50)]));
    P.step(a,input);ctx.GalaxyPhysics.step(b,input);
    if(t%30===0)assert.equal(P.hash(a),ctx.GalaxyPhysics.hash(b),'tick '+t);
  }
});
test('late movement AND field replay exactly as timely input for all four balls',()=>{
  const w=world();w.balls=[0,1,2,3].map((id)=>({id,x:285+id*5,y:421-id*10,vx:0,vy:4,contact:0}));
  const a=authority(w),b=authority(w);
  for(let t=1;t<=30;t++){
    send(a,0,cmd(t,1,t>=5?5:0));
    if(t>5)send(b,0,cmd(t-5,1,t-5>=5?5:0));
    a.advance();b.advance();
  }
  for(let t=26;t<=30;t++)send(b,0,cmd(t,1,5));b.flush();
  assert.equal(P.hash(a.world),P.hash(b.world));assert.ok(b.metrics.rollbacks>0);assert.ok(b.metrics.maxReplay<=WINDOW);
});
test('a provisional goal can be reversed before commit, never duplicated',()=>{
  const w=world();w.paddles[0]=170;w.paddlesPrev[0]=170;w.lives[0]=1;
  w.balls=[{id:1,x:217,y:462,vx:0,vy:6.5,contact:0}];const commits=[];
  const a=authority(w,e=>commits.push(e));
  for(let i=0;i<8;i++)a.advance();assert.ok(a.world.eliminated[0]);
  send(a,0,cmd(1,1,1));a.flush();assert.equal(a.world.lives[0],1);
  for(let i=0;i<16;i++)a.advance();
  assert.equal(commits.flatMap(x=>x.events).filter(e=>e.type==='goal').length,0);
  assert.equal(new Set(commits.map(x=>x.tick)).size,commits.length);
});
test('confirmed goals are immutable and terminal rewards are committed once',()=>{
  const w=world();w.eliminated=[false,true,true,true];w.eliminationOrder=[1,2,3];
  const events=[];const a=authority(w,e=>events.push(...e.events));
  for(let i=0;i<30;i++)a.advance();
  assert.equal(events.filter(e=>e.type==='finish').length,1);assert.ok(a.finished);
  const hash=P.hash(a.world);send(a,0,cmd(1,1,1));a.advance();assert.equal(P.hash(a.world),hash);
});
test('input validation rejects teleports, NaN, unlimited future, duplicate rewriting and old epochs',()=>{
  const a=authority();send(a,0,{...cmd(1),dx:NaN},cmd(200),{...cmd(1),boost:10});
  assert.equal(a.metrics.accepted,0);send(a,0,cmd(1,1));send(a,0,cmd(1,-1));a.advance();
  assert.equal(a.world.paddles[0],263.75);
  const epoch=a.epochs[0];a.connect(0);a.receive(0,{version:P.VERSION,matchId:'test',epoch,commands:[cmd(2,1)]});
  a.advance();assert.equal(a.world.paddles[0],263.75);
});
test('out of order input recovers without repeating a field action or charging twice',()=>{
  const a=authority();for(let i=0;i<5;i++)a.advance();
  send(a,0,cmd(4,0,2),cmd(2,0,2),cmd(3,0,2),cmd(2,0,2));a.flush();
  assert.equal(a.world.lastBoost[0],2);assert.ok(a.world.energy[0]>.66&&a.world.energy[0]<.67);
  assert.equal([...a.events.values()].flat().filter(e=>e.type==='event:field').length,1);
});
test('moving paddle CCD catches crossing at the contact coordinate, not the end coordinate',()=>{
  const w=world();w.paddlesPrev[0]=230;w.paddles[0]=240;
  const b={id:1,x:238,y:514,vx:0,vy:55,contact:0};
  assert.equal(P.paddleContact(w,0,b,{x:238,y:459}),true);assert.ok(b.vy<0);assert.ok(b.y<520);
});
test('moving field CCD catches relative crossing even when both endpoints are outside',()=>{
  const w=world();w.paddlesPrev[0]=180;w.paddles[0]=340;
  w.fields[0]={active:true,t:100,maxR:54,r:54};
  const b={id:1,x:260,y:465,vx:0,vy:0,contact:0};P.applyFFBall(w,0,b,{x:260,y:465});
  assert.equal(b.contact,1);assert.ok(Math.hypot(b.vx,b.vy)<=4.876);
});
test('four magnet balls follow one moving paddle and release once at bounded speed',()=>{
  const w=world();w.balls=[0,1,2,3].map(id=>({id,x:250+id*6,y:470,vx:0,vy:3,contact:0}));
  for(let t=1;t<=20;t++)P.step(w,{0:cmd(t,0,0,true)});
  assert.ok(w.balls.every(b=>b.mag_held_0));
  for(let t=21;t<40;t++)P.step(w,{0:cmd(t,1,0,true)});
  assert.ok(w.balls.every(b=>Math.abs(b.x-w.paddles[0])<30));
  P.step(w,{0:cmd(40,0,0,false)});assert.ok(w.balls.every(b=>!b.mag_held_0&&Math.hypot(b.vx,b.vy)<5));
  assert.equal(w.events.filter(e=>e.data.type==='magnet_release').length,4);
});
test('loss stops held movement/magnet; reconnect discards old future commands',()=>{
  const a=authority();send(a,0,cmd(1,1,0,true));for(let t=0;t<15;t++)a.advance();
  const x=a.world.paddles[0];assert.equal(a.world.magnet[0],false);
  for(let t=0;t<10;t++)a.advance();assert.equal(a.world.paddles[0],x);
  send(a,0,cmd(30,1,30));a.disconnect(0);a.connect(0);
  for(let t=0;t<10;t++)a.advance();assert.equal(a.world.lastBoost[0],0);assert.equal(a.world.paddles[0],x);
});
test('bounded memory and no rollback behind committed state',()=>{
  const a=authority();for(let t=1;t<2000;t++){send(a,0,cmd(t,t%3-1));a.advance();}
  const hash=P.hash(a.world);send(a,0,cmd(1,1,1));a.flush();assert.equal(P.hash(a.world),hash);
  assert.ok(a.history.size<=HISTORY+1);assert.ok(a.commands.size<=HISTORY+1);
});
test('client ignores out of order snapshots and pauses on a long outage',()=>{
  const a=authority(),c=new Client({slot:0,matchId:'test',epoch:0});
  const first=a.snapshot();c.receive(first,0);a.advance();c.receive(a.snapshot(),17);
  assert.equal(c.receive(first,20),false);c.advance(450);assert.ok(c.needsSync);assert.equal(c.metrics.predictionStops,1);
  c.receive(a.snapshot(),500);c.advance(510);assert.ok(!c.needsSync);assert.ok(c.world.tick<=a.world.tick+20);
});
test('rendered held balls stay anchored to the same interpolated paddle',()=>{
  const w=world();w.balls=[{id:1,x:260,y:470,vx:0,vy:0,contact:0,mag_held_0:true,mag_offX_0:0,mag_offY_0:-40}];w.magnet[0]=true;
  const a=authority(w),c=new Client({slot:0,matchId:'test',epoch:0});c.receive(a.snapshot(),0);
  c.setInput({dx:1,magnet:true});c.advance(17);
  for(const now of [17,21,26,30]){const v=c.sample(now),b=v.balls[0];assert.equal(b.x,v.paddles[0]);assert.equal(b.y,520-40);}
});
test('wire compression roundtrip preserves all physics and command state exactly',()=>{
  const Wire=require('./galaxy-wire');const a=authority(P.create({seed:33,spawnDelayMs:0}));
  for(let t=1;t<1800;t++){
    send(a,0,cmd(t,(t%3)-1,Math.floor(t/20),t%70<30));a.advance();
    if(t%60===0){const p=a.snapshot(),decoded=Wire.readSnapshot(Wire.snapshot(p),p.world.config);
      assert.deepEqual(decoded.world,p.world);assert.deepEqual(decoded.scheduled,p.scheduled);assert.deepEqual(decoded.replay,p.replay);
      const input={version:P.VERSION,matchId:'test',epoch:0,commands:[cmd(t,1,3,true)]};
      assert.deepEqual(Wire.readInput(Wire.input(input)),input);
    }
  }
});

test('successive snapshots preserve the unfinished visual correction without restarting its decay',()=>{
  const w=world();w.balls=[{id:1,x:250,y:250,vx:0,vy:0,contact:0}];
  const a=authority(w),c=new Client({slot:0,matchId:'test',epoch:0});c.setRtt(0);c.receive(a.snapshot(),0);
  c.world.balls[0].x+=10;c.receive(a.snapshot(),0);
  assert.equal(c.sample(0).balls[0].x,260);
  assert.equal(c.sample(40).balls[0].x,255);
  c.receive(a.snapshot(),40);
  assert.equal(c.sample(40).balls[0].x,255);
  assert.equal(c.sample(81).balls[0].x,250);
  c.world.balls[0].x+=10;c.receive(a.snapshot(),100);
  c.world.balls[0].x+=5;c.receive(a.snapshot(),140);
  assert.equal(c.sample(140).balls[0].x,260);
  c.world.balls[0].contact++;
  assert.equal(c.sample(145).balls[0].x,250,'a new collision invalidates the old visual offset');
});


test('a stalled authority never receives client commands beyond its accepted future window',()=>{
 const a=authority(),sent=[],c=new Client({slot:0,matchId:'test',epoch:0,send:p=>sent.push(p)});
 c.setRtt(80);c.receive(a.snapshot(),0);c.setInput({dx:1});
 for(let now=0;now<400;now+=17)c.advance(now);
 for(const p of sent)a.receive(0,p);
 assert.equal(a.metrics.rejected,0,'prediction must stay inside the authority future window');
 assert.ok(sent.length>0);
});

test('short automatic resync preserves remote hull position while authority remains exact',()=>{
 const a=authority(),c=new Client({slot:0,matchId:'test',epoch:0});c.setRtt(0);c.receive(a.snapshot(),0);
 const before=c.sample(420).paddles[1];c.advance(421);assert.equal(c.needsSync,true);
 a.world.paddles[1]+=35;a.history.set(0,P.checkpoint(a.world));
 c.receive(a.snapshot(),430);
 assert.equal(c.world.paddles[1],before+35);
 assert.ok(Math.abs(c.sample(430).paddles[1]-before)<4,'resync must not teleport the visible hull');
 assert.ok(Math.abs(c.sample(550).paddles[1]-(before+35))<.01);
});

test('small own-paddle reconciliation is visual only and settles within 100ms',()=>{
 const a=authority(),c=new Client({slot:0,matchId:'test',epoch:0});c.setRtt(0);c.receive(a.snapshot(),0);
 c.world.paddles[0]+=15;c.previous.paddles[0]+=15;
 const before=c.sample(10).paddles[0];c.receive(a.snapshot(),10);
 assert.equal(c.world.paddles[0],a.world.paddles[0]);
 assert.equal(c.sample(10).paddles[0],before);
 assert.equal(c.sample(111).paddles[0],a.world.paddles[0]);
});
