'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),P=require('./galaxy-physics');
function world(){const w=P.create({seed:71,durationTicks:60000});w.respawns=[];return w;}
const speed=b=>Math.hypot(b.vx,b.vy);
test('slow free balls recover smoothly to cruise within 0.4 seconds and keep their heading',()=>{
 for(const initial of [.01,.25,1,2.5]){
  const w=world(),b={id:1,x:210,y:210,vx:initial*.6,vy:initial*.8,contact:1};w.balls=[b];let before=speed(b);
  for(let t=0;t<25;t++){P.step(w);const after=speed(b);assert.ok(after>=before-.000003);assert.ok(after-before<.115);assert.ok(Math.abs(b.vx*.8-b.vy*.6)<.00002);before=after;}
  assert.ok(Math.abs(speed(b)-2.5)<.00001);
 }
});
test('fast balls retain ability boosts and a fully stopped ball restarts deterministically',()=>{
 for(const initial of [2.5,3,4.875,6.5]){const w=world(),b={id:2,x:260,y:260,vx:initial,vy:0};w.balls=[b];P.step(w);assert.equal(speed(b),initial);}
 for(const pos of [[260,260],[280,480]]){const w=world();w.balls=[{id:7,x:pos[0],y:pos[1],vx:0,vy:0}];const copy=P.clone(w);
  for(let t=0;t<25;t++){P.step(w);P.step(copy);assert.equal(P.hash(w),P.hash(copy));}assert.ok(speed(w.balls[0])>2.499);
 }
});
test('a moving hull can cancel ball velocity, then the free ball recovers instead of stalling',()=>{
 const w=world(),n=Math.SQRT1_2,p=P.slotToPaddle(0,260,w,w.config),r=p.radius+8;
 w.paddlesPrev[0]=260;w.paddles[0]=258;
 const old={x:260+n*r,y:520-n*r},b={id:1,x:old.x-2,y:old.y+2,vx:-2,vy:2,contact:0};
 assert.ok(P.paddleContact(w,0,b,old));assert.ok(speed(b)<.00001);w.balls=[b];
 for(let i=0;i<25;i++)P.step(w);assert.ok(speed(b)>2.499);
});
test('magnet capture and attraction are exempt from free-flight recovery',()=>{
 const w=world(),b={id:1,x:260,y:484,vx:0,vy:0,mag_held_0:true,mag_offX_0:0,mag_offY_0:-36};w.balls=[b];
 for(let t=1;t<=30;t++)P.step(w,{0:{tick:t,magnet:true,dx:0}});
 assert.equal(b.mag_held_0,true);assert.equal(speed(b),0);
 const attracted=world();attracted.balls=[{id:1,x:260,y:445.01,vx:0,vy:0}];P.step(attracted,{0:{tick:1,magnet:true,dx:0}});
 assert.ok(speed(attracted.balls[0])<.001,'weak field-edge attraction must not become a recovery impulse');
});
