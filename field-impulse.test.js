const test=require('node:test'),assert=require('node:assert/strict'),P=require('./galaxy-physics'),F=require('./galaxy-fleet');
test('inside activation kicks outward in one tick at the unchanged speed cap, all sides and upgrades',()=>{
 for(let step=0;step<=40;step++)for(let s=0;s<4;s++){
  const stats=F.stats(0,step),w=P.create({players:Array.from({length:4},()=>({paddleStats:stats}))});
  const p=P.slotToPaddle(s,260,w,w.config),nx=[0,0,1,-1][s],ny=[-1,1,0,0][s];
  w.fields[s]={active:true,maxR:stats.fr};const b={id:1,x:p.x+p.w/2+nx*(stats.fr*.7),y:p.y+p.h/2+ny*(stats.fr*.7),vx:-nx*4,vy:-ny*4};const old={x:b.x,y:b.y};
  P.applyFFBall(w,s,b,old);assert.ok(b.vx*nx+b.vy*ny>4.8);assert.ok(Math.abs(Math.hypot(b.vx,b.vy)-4.875)<1e-9);assert.equal(b.x,old.x);assert.equal(b.y,old.y);assert.equal(b.contact,1);
  for(let i=0;i<5;i++)P.applyFFBall(w,s,b,old);assert.equal(b.contact,1);assert.equal(w.events.length,1);
 }
});
test('inside kick replays identically from a checkpoint and cannot exceed the cap with moving hulls',()=>{
 const w=P.create({seed:17}),copy=P.clone(w);for(const state of [w,copy]){state.fields[0]={active:true,maxR:54};state.paddlesPrev[0]=250;const b={id:5,x:260,y:480,vx:1,vy:2};state.balls=[b];P.applyFFBall(state,0,b,{x:260,y:478});assert.ok(Math.hypot(b.vx,b.vy)<=4.875+1e-9);}assert.equal(P.hash(w),P.hash(copy));
});
