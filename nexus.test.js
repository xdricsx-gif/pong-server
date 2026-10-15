'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const P=require('./galaxy-physics'),Fleet=require('./galaxy-fleet'),Wire=require('./galaxy-wire'),{Authority,Client}=require('./galaxy-netcode');
function world(mode='ranked',sub=0){const w=P.create({mode,players:P.SLOTS.map(()=>({paddleStats:Fleet.stats(sub)})),durationTicks:60000});w.respawns=[];return w;}
const input=(tick,rocket=0,target=1)=>({tick,seq:tick,dx:0,boost:0,magnet:false,target:null,rocket,rocketTarget:target});
test('five UFO stages have compact hull growth, independent field growth and preserve legacy upgrades',()=>{
  const account={ownedPaddles:[0,7,19],hangars:{0:{sub:40},19:{sub:39}},paddleId:19},before=JSON.stringify(account);
  assert.equal(Fleet.fromAccount(account).w,54);assert.equal(JSON.stringify(account),before);
  for(let lv=1;lv<=5;lv++){const st=Fleet.stats((lv-1)*10);assert.equal(st.level,lv);assert.equal(st.w,+(54*[1,1.08,1.16,1.23,1.3][lv-1]).toFixed(3));assert.equal(st.fr,+(54*[1,1.18,1.4,1.68,2][lv-1]).toFixed(3));}
  assert.equal(Fleet.fromAccount({...account,ufoHangar:{sub:40}}).level,5);
});
test('round hull CCD reflects radially in every orientation and has no rectangular corner hit',()=>{
  for(const s of P.SLOTS)for(const sub of [0,10,20,30,40]){
    const w=world('ranked',sub),p=P.slotToPaddle(s,260,w,w.config),cx=p.x+p.w/2,cy=p.y+p.h/2;
    const nx=s===2?1:s===3?-1:0,ny=s===0?-1:s===1?1:0,r=p.radius+8;
    const old={x:cx+nx*(r+10),y:cy+ny*(r+10)},b={id:1,x:old.x-nx*25,y:old.y-ny*25,vx:-nx*25,vy:-ny*25,contact:0};
    assert.equal(P.paddleContact(w,s,b,old),true);assert.ok(b.vx*nx+b.vy*ny>0);assert.ok(Math.hypot(b.x-cx,b.y-cy)>r);
    const corner={x:cx+r*.9,y:cy+r*.9};assert.equal(P.paddleContact(w,s,{...corner,vx:1,vy:0},corner),false);
  }
});

test('all upgrade hull centres sit on their goal lines and a central shot hits before scoring',()=>{
  for(const sub of [0,10,20,30,40])for(const s of P.SLOTS){
    const w=world('ranked',sub),p=P.slotToPaddle(s,260,w,w.config);
    assert.equal(s<2?p.y+p.h/2:p.x+p.w/2,s===0||s===3?520:0);
    const nx=s===2?1:s===3?-1:0,ny=s===0?-1:s===1?1:0;
    w.balls=[{id:1,x:p.x+p.w/2+nx*(p.radius+12),y:p.y+p.h/2+ny*(p.radius+12),vx:-nx*20,vy:-ny*20,contact:0}];
    P.step(w);assert.equal(w.scores[s],0);assert.equal(w.balls.length,1);assert.ok(w.balls[0].vx*nx+w.balls[0].vy*ny>0);
  }
});

test('each assigned seat appears at the bottom and rightward input moves right on screen',()=>{
  for(const s of P.SLOTS){const w=world(),p=P.slotToPaddle(s,260,w,w.config),a=Fleet.viewRotation(s),x=p.x+p.w/2-260,y=p.y+p.h/2-260;
    assert.ok(Math.abs(x*Math.cos(a)-y*Math.sin(a))<1e-8);assert.ok(Math.abs(x*Math.sin(a)+y*Math.cos(a)-260)<1e-8);
    const d=Fleet.inputDirection(s),dx=s<2?d:0,dy=s<2?0:d;assert.ok(dx*Math.cos(a)-dy*Math.sin(a)>.99);
  }
});
test('touch targets obey speed and rail limits, stop when stale and cannot bypass stun',()=>{
  const w=world();P.step(w,{0:{...input(1),target:520}});assert.equal(w.paddles[0],263.75);
  for(let i=2;i<30;i++)P.step(w);const stopped=w.paddles[0];for(let i=0;i<10;i++)P.step(w);assert.equal(w.paddles[0],stopped);
  w.stunUntil[0]=w.tick+180;const end=w.stunUntil[0];while(w.tick<end-1){P.step(w,{0:{...input(w.tick+1),target:0,boost:1,magnet:true}});assert.equal(w.paddles[0],stopped);}
  assert.ok(w.magEnergy[0]<1);P.step(w,{0:{...input(w.tick+1),target:0}});assert.ok(w.paddles[0]<stopped);
});
test('missile bay position matches each supplied model in all four rotations',()=>{
  for(const sub of [0,10,20,30,40])for(const s of P.SLOTS){const w=world('ranked',sub),p=P.slotToPaddle(s,260,w,w.config),model=Fleet.models[1+sub/10],a=[0,Math.PI,Math.PI/2,-Math.PI/2][s],m=P.rocketMuzzle(w,s);
    assert.ok(Math.abs(m.x-(p.x+p.w/2+(model.bay[0]*Math.cos(a)-model.bay[1]*Math.sin(a))*p.w/model.diameter))<1e-8);
    assert.ok(Math.abs(m.y-(p.y+p.h/2+(model.bay[0]*Math.sin(a)+model.bay[1]*Math.cos(a))*p.w/model.diameter))<1e-8);
  }
});
test('only living enemies can be targeted; team ally and inactive duel slots are rejected',()=>{
  for(const [mode,bad]of [['ranked',[0,-1,4]],['team',[0,3]],['duel',[0,2,3]]])for(const target of bad){const w=world(mode);P.step(w,{0:input(1,1,target)});assert.equal(w.missiles.length,0);assert.equal(w.rocketReadyAt[0],0);}
  const w=world();w.eliminated[1]=true;P.step(w,{0:input(1,1,1)});assert.equal(w.missiles.length,0);
});
test('bay follows moving launcher, launches once, hits through fields and stuns without goal damage',()=>{
  const w=world();const before=w.lives.slice();let launched=0,hit=null;
  for(let t=1;t<800;t++){
    P.step(w,{0:{...input(t,1,1),dx:t<45?1:0},1:{...input(t),boost:t,magnet:true}});
    if(t<67){assert.equal(w.missiles[0].age,0);assert.equal(w.missiles[0].x,P.rocketMuzzle(w,0).x);}
    launched+=w.events.filter(e=>e.type==='rocket:launch').length;
    hit=w.events.find(e=>e.type==='rocket:hit');if(hit)break;
  }
  assert.equal(launched,1);assert.ok(hit);assert.equal(w.stunUntil[1]-w.tick,180);assert.deepEqual(w.lives,before);assert.deepEqual(w.scores,[0,0,0,0]);assert.equal(w.missiles.length,0);
  assert.equal(w.rocketReadyAt[0],67+1800);
  const deadline=w.rocketReadyAt[0];while(w.tick<deadline-1)P.step(w,{0:input(w.tick+1,w.tick+1,1)});
  assert.equal(w.missiles.length,0);P.step(w,{0:input(deadline,deadline,1)});assert.equal(w.missiles.length,1);
});
test('late rocket and target inputs rewind to exactly the same world; wire preserves new state',()=>{
  const a=new Authority({world:world(),matchId:'n'}),b=new Authority({world:world(),matchId:'n'});
  const c=t=>({...input(t,t>=5?5:0,1),target:t<30?330:250});
  const send=(auth,t)=>auth.receive(0,{version:P.VERSION,matchId:'n',epoch:0,commands:[c(t)]});
  for(let t=1;t<=200;t++){send(a,t);if(t>5)send(b,t-5);a.advance();b.advance();}
  for(let t=196;t<=200;t++)send(b,t);b.flush();assert.equal(P.hash(a.world),P.hash(b.world));
  const packet=a.snapshot();assert.deepEqual(Wire.readSnapshot(Wire.snapshot(packet),packet.world.config).world,packet.world);
  const wire={version:P.VERSION,matchId:'n',epoch:0,commands:[c(201)]};assert.deepEqual(Wire.readInput(Wire.input(wire)),wire);
});
test('reconnected client sequences stay above previous missile latch and does not replay a launch',()=>{
  const a=new Authority({world:world(),matchId:'n'});a.world.lastRocket[0]=800;a.history.set(0,P.checkpoint(a.world));
  const c=new Client({slot:0,matchId:'n',epoch:0});c.receive(a.snapshot(),0);assert.equal(c.seq,800);c.advance(17);assert.equal(c.world.missiles.length,0);
});
test('browser physics and server match for every level with rockets and moving targets',()=>{
  const ctx=vm.createContext({});vm.runInContext(fs.readFileSync(require.resolve('./galaxy-physics'),'utf8'),ctx);
  for(const sub of [0,10,20,30,40]){const a=world('team',sub),b=ctx.GalaxyPhysics.clone(a);
    for(let t=1;t<=1200;t++){const commands=Object.fromEntries(P.SLOTS.map(s=>[s,{...input(t,t>=1?1:0,(s+1)%4),dx:Math.floor(t/53+s)%3-1,magnet:t%250<40,boost:Math.floor(t/97)}]));P.step(a,commands);ctx.GalaxyPhysics.step(b,commands);if(t%30===0)assert.equal(P.hash(a),ctx.GalaxyPhysics.hash(b));}
  }
});
