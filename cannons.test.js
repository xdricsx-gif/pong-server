const test=require('node:test'),assert=require('node:assert/strict'),P=require('./galaxy-physics');
test('corner launches clear corner walls and preserve speed in every quadrant',()=>{
const corners=new Set();for(let seed=1;seed<=100;seed++){const w=P.create({seed,cornerLaunches:true,spawnDelayMs:0});const r={...w.respawns[0]};P.step(w);const b=w.balls[0];assert.ok(b);const x=r.vx<0?520-b.x:b.x,y=r.vy<0?520-b.y:b.y;assert.ok(x+y>54+8*Math.SQRT2);assert.ok(x>8&&y>8);assert.ok(Math.abs(Math.hypot(b.vx,b.vy)-Math.hypot(r.vx,r.vy))<.01);corners.add([r.vx<0,r.vy<0].join());}assert.equal(corners.size,4);
});
test('rollback uses center spawns',()=>{const w=P.create({cornerLaunches:false,spawnDelayMs:0});P.step(w);assert.ok(Math.abs(w.balls[0].x-260)<5);assert.ok(Math.abs(w.balls[0].y-260)<5);});
test('corner launch replay and checkpoint remain deterministic',()=>{const w=P.create({seed:78,spawnDelayMs:0}),b=P.clone(w);for(let i=0;i<600;i++){P.step(w);P.step(b);}assert.equal(P.hash(w),P.hash(b));});
