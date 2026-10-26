'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const P=require('./galaxy-physics'),N=require('./galaxy-netcode'),Wire=require('./galaxy-wire');
const world=(mode='ranked')=>{const w=P.create({mode,durationTicks:60000,timedGates:false});w.respawns=[];return w;};
const cmd=(t,extra={})=>({tick:t,seq:t,dx:0,boost:0,magnet:false,target:null,rocket:0,rocketTarget:-1,shield:0,emp:0,empX:null,empY:null,...extra});
const stepTo=(w,t)=>{while(w.tick<t)P.step(w);};
function shoot(w,s,axis=330){const g=P.goalGeometry(s),x=s<2?axis:g.a.x+g.nx*10,y=s<2?g.a.y+g.ny*10:axis;w.balls=[{id:1,x,y,vx:-g.nx*4,vy:-g.ny*4,contact:0}];}
test('shield anchors are fixed goal endpoints at all seats, every upgrade, and off-centre launch positions',()=>{
 for(const s of P.SLOTS)for(const x of [90,260,430])for(const width of [54,70.2]){
  const w=world();w.config.players[s].paddleStats.w=width;w.paddles[s]=x;
  P.step(w,{[s]:cmd(1,{shield:1,dx:1})});const f=P.clone(w.goalShields[s]),g=P.goalGeometry(s);
  assert.equal(s<2?g.a.x:g.a.y,54);assert.equal(s<2?g.b.x:g.b.y,466);
  assert.equal(s<2?f.x:f.y,Math.min(466-width/2,x+3.75));assert.equal(s<2?f.y:f.x,s===0||s===3?520:0);
  for(let t=2;t<=60;t++)P.step(w,{[s]:cmd(t,{dx:-1,shield:1})});assert.deepEqual(w.goalShields[s],f);assert.equal(P.shieldActive(w,s),false);
  P.step(w);assert.equal(P.shieldActive(w,s),true);assert.equal(f.until-f.activeAt,600);
  stepTo(w,f.until-1);assert.equal(P.shieldActive(w,s),true);P.step(w);assert.equal(P.shieldActive(w,s),false);
 }
});
test('shield kicks goal shots thirty percent faster, no score, then allows goals exactly on expiry',()=>{
 for(const s of P.SLOTS){const w=world();w.paddles[s]=100;P.step(w,{[s]:cmd(1,{shield:1})});stepTo(w,61);shoot(w,s);P.step(w);
  const g=P.goalGeometry(s),b=w.balls[0];assert.equal(w.scores[s],0);assert.equal(b.contact,1);assert.equal(Math.hypot(b.vx,b.vy),5.2);assert.ok(b.vx*g.nx+b.vy*g.ny>0);assert.equal(w.shieldHits[s].tick,w.tick);
  const end=w.goalShields[s].until;w.balls=[];stepTo(w,end-1);shoot(w,s);P.step(w);assert.equal(w.scores[s],1);assert.equal(w.balls.length,0);
 }
});
test('shield accelerates slow balls, sends grazing hits inward and caps repeated bounces at every goal',()=>{
 for(const s of P.SLOTS)for(const [speed,normal]of [[.2,1],[2.5,1],[4,.04],[6.5,.8],[20,1]]){
  const w=world(),g=P.goalGeometry(s),tx=-g.ny,ty=g.nx;w.paddles[s]=100;
  P.step(w,{[s]:cmd(1,{shield:1})});stepTo(w,61);
  const axis=330,x=s<2?axis:g.a.x+g.nx*8.01,y=s<2?g.a.y+g.ny*8.01:axis;
  w.balls=[{id:1,x,y,vx:speed*(-g.nx*normal+tx*Math.sqrt(1-normal*normal)),vy:speed*(-g.ny*normal+ty*Math.sqrt(1-normal*normal)),contact:0}];
  for(let n=0;n<5;n++){
   const before=Math.hypot(w.balls[0].vx,w.balls[0].vy);P.step(w);const b=w.balls[0],out=Math.hypot(b.vx,b.vy);
   assert.equal(w.scores[s],0);assert.equal(b.contact,n+1);assert.ok(Math.abs(out-Math.min(6.5,Math.max(4.875,before*1.3)))<.000002);
   assert.ok(b.vx*g.nx+b.vy*g.ny>=out*.5-.000002);
   const contacts=b.contact;P.step(w);assert.equal(b.contact,contacts,'outgoing ball must not receive another impulse');
   b.x=x;b.y=y;b.vx=-b.vx;b.vy=-b.vy;
  }
 }
});
test('shield activation needs deployment, is one-shot, and reloads after exactly thirty seconds',()=>{
 const w=world();w.paddles[0]=100;P.step(w,{0:cmd(1,{shield:1})});shoot(w,0);P.step(w);assert.equal(w.scores[0],1);w.respawns=[];
 const first=P.clone(w.goalShields[0]);for(let t=3;t<1801;t++)P.step(w,{0:cmd(t,{shield:t})});assert.deepEqual(w.goalShields[0],first);assert.equal(w.shieldReadyAt[0],1801);
 P.step(w,{0:cmd(1801,{shield:1801})});assert.equal(w.goalShields[0].startTick,1801);
});
test('directional EMP hits two enemies but neither caster nor ship outside its sector',()=>{
 const w=world();w.paddles[1]=430;w.paddles[3]=90;P.step(w,{0:cmd(1,{emp:1,empX:520,empY:0})});stepTo(w,150);
 assert.ok(w.empUntil[1]>0);assert.ok(w.empUntil[3]>0);assert.equal(w.empUntil[0],0);assert.equal(w.empUntil[2],0);assert.equal(w.energy[1],0);assert.equal(w.magEnergy[3],0);assert.equal(w.empReadyAt[0],1801);
});
test('EMP preserves movement, drains shared energy and prevents abilities for precisely ten seconds',()=>{
 const w=world();P.step(w,{0:cmd(1,{emp:1,empX:260,empY:0})});while(!w.empUntil[1])P.step(w);
 const until=w.empUntil[1],at=w.tick,x=w.paddles[1];assert.equal(until-at,600);
 for(let t=at+1;t<until;t++){P.step(w,{1:cmd(t,{dx:1,magnet:true,boost:t,rocket:t,rocketTarget:0,shield:t,emp:t,empX:260,empY:520})});
  assert.equal(w.energy[1],0);assert.equal(w.magEnergy[1],0);assert.equal(w.fields[1].active,false);assert.equal(w.magnet[1],false);assert.equal(w.missiles.some(m=>m.owner===1),false);assert.equal(w.goalShields[1],null);assert.equal(w.empWaves.some(e=>e.owner===1),false);
 }
 assert.ok(w.paddles[1]>x);P.step(w,{1:cmd(until)});assert.ok(w.energy[1]>0);assert.ok(w.magEnergy[1]>0);assert.equal(P.abilitiesLocked(w,1),false);
 P.step(w,{1:cmd(until+1,{shield:until+1})});assert.equal(w.goalShields[1],null); // EMP expires empty: recharge is required before buying a shield.
});
test('existing autonomous shield survives EMP and a captured ball is released without a boost',()=>{
 const w=world();P.step(w,{1:cmd(1,{shield:1})});stepTo(w,70);const original=P.clone(w.goalShields[1]);
 P.step(w,{0:cmd(71,{emp:71,empX:260,empY:0})});
 while(!w.empUntil[1]){const p=P.slotToPaddle(1,w.paddles[1],w,w.config);w.magEnergy[1]=1;w.magnet[1]=true;w.magPressed[1]=true;w.balls=[{id:1,x:260,y:p.radius+9,vx:0,vy:0,mag_held_1:true,mag_offX_1:0,mag_offY_1:p.radius+9,contact:0}];P.step(w,{1:cmd(w.tick+1,{magnet:true})});}
 assert.deepEqual(w.goalShields[1],original);assert.equal(P.shieldActive(w,1),true);assert.equal(w.magnet[1],false);assert.equal(w.balls[0].mag_held_1,undefined);assert.ok(Math.hypot(w.balls[0].vx,w.balls[0].vy)<=2.5);
});
test('EMP excludes teammates and inactive duel seats, and cannot be emitted outward or at zero distance',()=>{
 for(const mode of ['team','duel']){const w=world(mode);w.paddles[1]=430;w.paddles[3]=90;P.step(w,{0:cmd(1,{emp:1,empX:520,empY:0})});stepTo(w,160);assert.ok(w.empUntil[1]>0);assert.equal(w.empUntil[3],0);assert.equal(w.empUntil[2],0);}
 const w=world();P.step(w,{0:cmd(1,{emp:1,empX:260,empY:520})});assert.equal(w.empWaves.length,0);assert.equal(w.empReadyAt[0],0);
});
test('disconnected or eliminated owners cannot create effects; elimination removes an active shield',()=>{
 const w=world();P.step(w,{0:cmd(1,{shield:1,emp:1,empX:260,empY:0})},[{type:'connected',slot:0,value:false}]);assert.equal(w.goalShields[0],null);assert.equal(w.empWaves.length,0);
 P.step(w,{},[{type:'connected',slot:0,value:true}]);P.step(w,{0:cmd(3,{shield:3})});assert.ok(w.goalShields[0]);P.step(w,{},[{type:'eliminate',slot:0}]);assert.equal(w.goalShields[0],null);
});
test('EMP/shield inputs and in-flight effects survive wire, late-input rewind, duplication and browser replay',()=>{
 const a=new N.Authority({world:world(),matchId:'effects'}),b=new N.Authority({world:world(),matchId:'effects'});
 const c=t=>cmd(t,{dx:t<30?1:0,shield:t>=5?5:0,emp:t>=15?15:0,empX:520,empY:0});
 const send=(auth,t)=>{const p={version:P.VERSION,matchId:'effects',epoch:0,commands:[c(t)]};assert.deepEqual(Wire.readInput(Wire.input(p)),p);auth.receive(0,Wire.readInput(Wire.input(p)));};
 for(let t=1;t<=180;t++){send(a,t);if(t>6){send(b,t-6);send(b,t-6);}a.advance();b.advance();}
 for(let t=175;t<=180;t++)send(b,t);b.flush();assert.equal(P.hash(a.world),P.hash(b.world));
 const p=a.snapshot(),decoded=Wire.readSnapshot(Wire.snapshot(p),p.world.config);assert.deepEqual(decoded.world,p.world);assert.deepEqual(decoded.replay,p.replay);
 const ctx=vm.createContext({});vm.runInContext(fs.readFileSync(require.resolve('./galaxy-physics'),'utf8'),ctx);const local=ctx.GalaxyPhysics.clone(a.world),server=P.clone(a.world);
 for(let t=0;t<900;t++){P.step(server);ctx.GalaxyPhysics.step(local);assert.equal(P.hash(server),ctx.GalaxyPhysics.hash(local));}
});
test('reconnect and suspend cannot replay or defer a one-shot ability',()=>{
 const a=new N.Authority({world:world(),matchId:'effects'});a.world.lastShield[0]=400;a.world.lastEmp[0]=500;a.history.set(0,P.checkpoint(a.world));
 const c=new N.Client({slot:0,matchId:'effects',epoch:0});c.receive(a.snapshot(),0);assert.equal(c.seq,500);c.deployShield();c.fireEMP(260,0);c.suspend();c.resume();c.receive(a.snapshot(),1);c.advance(18);assert.equal(c.world.goalShields[0],null);assert.equal(c.world.empWaves.length,0);
});
test('bad inputs and old wire versions cannot bypass ability limits',()=>{
 const a=new N.Authority({world:world(),matchId:'effects'});for(const extra of [{shield:2},{emp:2},{emp:NaN},{empX:Infinity},{empY:-1},{shield:1.5}])a.receive(0,{version:P.VERSION,matchId:'effects',epoch:0,commands:[cmd(1,extra)]});assert.equal(a.metrics.accepted,0);
 const input=Wire.input({matchId:'effects',epoch:0,commands:[cmd(1)]});input[0]=2;assert.equal(Wire.readInput(input),null);
});
