'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const P=require('./galaxy-physics'),Wire=require('./galaxy-wire'),{Authority}=require('./galaxy-netcode');
const command=(tick,magnet=true,dx=0)=>({tick,seq:tick,magnet,dx,boost:0});
function world(stats={}){const w=P.create({players:P.SLOTS.map(()=>({paddleStats:stats})),durationTicks:40000});w.respawns=[];return w;}
function ball(w,s,distance,tangent=0,speed=0){const p=P.slotToPaddle(s,w.paddles[s],w,w.config),g=P.magnetGeometry(s,p,w.config.players[s].paddleStats);return {id:1,x:g.cx+g.nx*distance+g.tx*tangent,y:g.cy+g.ny*distance+g.ty*tangent,vx:-g.nx*speed,vy:-g.ny*speed,contact:0};}
test('magnet charges 20% on a fresh press and 20% per second; holding cannot restart an empty battery',()=>{
  const w=world();P.step(w,{0:command(1)});assert.ok(Math.abs(w.magEnergy[0]-(.8-1/300))<1e-6);
  for(let t=2;t<=60;t++)P.step(w,{0:command(t)});assert.ok(Math.abs(w.magEnergy[0]-.6)<.0001);
  for(let t=61;t<=600;t++)P.step(w,{0:command(t)});assert.equal(w.magEnergy[0],0);assert.equal(w.magnet[0],false);assert.equal(w.magPressed[0],true);
  for(let t=601;t<=750;t++)P.step(w,{0:command(t,false)});assert.ok(w.magEnergy[0]>=.249);
  P.step(w,{0:command(751)});assert.ok(w.magEnergy[0]<.05);assert.equal(w.magnet[0],true);
});
test('insufficient energy consumes no partial activation and each repeated tap has its own cost',()=>{
  const w=world();w.energy[0]=.199;P.step(w,{0:command(1)});assert.equal(w.magnet[0],false);assert.equal(w.magEnergy[0],.199);
  w.energy[0]=1;P.step(w,{0:command(2,false)});P.step(w,{0:command(3)});P.step(w,{0:command(4,false)});P.step(w,{0:command(5)});assert.ok(w.magEnergy[0]>.59&&w.magEnergy[0]<.6);
});
test('attraction fades smoothly to zero, is stronger near the ship and never captures at a distance',()=>{
  const w=world();w.magnet[0]=true;
  const pulls=[42,55,70,74.99,75,80].map(d=>{const b=ball(w,0,d);P.applyMagnetBall(w,0,b);assert.ok(!b.mag_held_0);return b.vy;});
  assert.ok(pulls[0]>pulls[1]&&pulls[1]>pulls[2]&&pulls[2]>pulls[3]);assert.ok(pulls[3]<1e-6);assert.equal(pulls[4],0);assert.equal(pulls[5],0);
  const behind=ball(w,0,-5,40);P.applyMagnetBall(w,0,behind);assert.equal(behind.vy,0);
});
test('short tap only bends trajectory: no capture, release impulse or teleport',()=>{
  for(const s of P.SLOTS){const w=world();w.balls=[ball(w,s,60,10,0)];P.step(w,{[s]:command(1)});const before={...w.balls[0]};
    assert.ok(!before['mag_held_'+s]);assert.ok(Math.hypot(before.vx,before.vy)>0);
    P.step(w,{[s]:command(2,false)});const b=w.balls[0];assert.ok(Math.abs(b.vx-before.vx)<1e-6&&Math.abs(b.vy-before.vy)<1e-6);
    assert.ok(Math.abs(b.x-before.x-before.vx)<1e-6&&Math.abs(b.y-before.y-before.vy)<1e-6);assert.ok(!w.events.some(e=>e.data.type==='magnet_release'));
  }
});
test('all four orientations pull to the real paddle surface, hold through movement, then launch at field speed',()=>{
  for(const s of P.SLOTS)for(const stats of [{w:54,fr:54},{w:75.6,fr:75.6},{w:108,fr:108}]){
    const w=world(stats);w.balls=[ball(w,s,stats.w/2+25,7,0)];let tick=0;
    while(tick<100&&!w.balls[0]['mag_held_'+s])P.step(w,{[s]:command(++tick)});
    assert.ok(w.balls[0]['mag_held_'+s],`capture slot ${s}`);
    for(let i=0;i<10;i++){P.step(w,{[s]:command(++tick,true,1)});const b=w.balls[0],p=P.slotToPaddle(s,w.paddles[s],w,w.config),g=P.magnetGeometry(s,p,stats);
      assert.ok(Math.abs(Math.hypot(b.x-g.cx,b.y-g.cy)-(g.hullRadius+9))<1e-5);assert.ok(Math.abs(b.x-g.cx-b['mag_offX_'+s])<1e-5);}
    const before={...w.balls[0]};P.step(w,{[s]:command(++tick,false)});const b=w.balls[0];assert.ok(!b['mag_held_'+s]);assert.ok(Math.abs(Math.hypot(b.vx,b.vy)-4.875)<1e-5);
    assert.ok(Math.hypot(b.x-before.x,b.y-before.y)<5);assert.equal(w.events.filter(e=>e.data.type==='magnet_release').length,1);
  }
});
test('swept capture catches fast incoming balls before paddle bounce; missed paddle is not a magic save',()=>{
  for(const s of P.SLOTS){const w=world();w.balls=[ball(w,s,40,0,6.5)];P.step(w,{[s]:command(1)});assert.ok(w.balls[0]['mag_held_'+s]);
    const m=world();m.balls=[ball(m,s,40,50,6.5)];P.step(m,{[s]:command(1)});assert.ok(!m.balls[0]['mag_held_'+s]);}
});
test('energy exhaustion, stale input, disconnect and elimination release held balls exactly once',()=>{
  for(const mode of ['energy','stale','disconnect','eliminate']){const w=world();w.balls=[ball(w,0,37,0,1)];P.step(w,{0:command(1)});assert.ok(w.balls[0].mag_held_0);
    let releases=0;for(let t=2;t<=15;t++){if(mode==='energy')w.energy[0]=0;
      P.step(w,mode==='stale'?{}:{0:command(t)},t===2&&['disconnect','eliminate'].includes(mode)?[{type:mode==='disconnect'?'connected':'eliminate',slot:0,value:false}]:[]);
      releases+=w.events.filter(e=>e.data.type==='magnet_release').length;}
    assert.equal(releases,1,mode);assert.ok(!w.balls[0].mag_held_0);}
});
test('magnet range grows with real upgrade stats; every ball has at most one owner',()=>{
  assert.equal(P.magnetRadius({fr:54}),75);assert.ok(P.magnetRadius({fr:100})>75);
  const w=world({w:140,fr:180});w.balls=[0,1,2,3].map((id)=>({...ball(w,0,85,id*12-18,2),id}));
  for(let t=1;t<=40;t++)P.step(w,Object.fromEntries(P.SLOTS.map(s=>[s,command(t)])));
  assert.equal(w.balls.filter(b=>b.mag_held_0).length,4);
  for(const b of w.balls)assert.equal(P.SLOTS.filter(s=>b['mag_held_'+s]).length,1);
});
test('delayed magnet presses replay identically; wire checkpoints preserve the energy latch',()=>{
  const w=world();w.balls=[ball(w,0,50,8,1)];const a=new Authority({world:w,matchId:'m'}),b=new Authority({world:w,matchId:'m'});
  const input=t=>command(t,t<42,t>12&&t<25?1:0);
  const send=(auth,t)=>auth.receive(0,{version:P.VERSION,matchId:'m',epoch:0,commands:[input(t)]});
  for(let t=1;t<=55;t++){send(a,t);if(t>5)send(b,t-5);a.advance();b.advance();}
  for(let t=51;t<=55;t++)send(b,t);b.flush();assert.equal(P.hash(a.world),P.hash(b.world));
  const packet=a.snapshot(),decoded=Wire.readSnapshot(Wire.snapshot(packet),packet.world.config);assert.deepEqual(decoded.world,packet.world);assert.deepEqual(decoded.replay,packet.replay);
});
test('new protocol explicitly rejects an old client before a match can use different magnet physics',()=>{
  const a=new Authority({world:world(),matchId:'m'});assert.equal(a.receive(0,{version:'gf-net2-2',matchId:'m',epoch:0,commands:[command(1)]}),false);
  const source=fs.readFileSync(require.resolve('./server'),'utf8');const start=source.indexOf("socket.on('mm:join'");const guard=source.indexOf('netVersion!==netcode2.VERSION',start);assert.ok(guard>start&&guard<source.indexOf('const verifiedUid',start));
});
