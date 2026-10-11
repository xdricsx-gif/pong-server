'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const P=require('./galaxy-physics'),{Authority,Client,WINDOW}=require('./galaxy-netcode'),Wire=require('./galaxy-wire');
function setup(balls=[]){const world=P.create({seed:42,spawnDelayMs:100000});world.balls=balls;world.respawns=[];
  const a=new Authority({matchId:'replay',world});let uplink;
  const c=new Client({slot:0,matchId:a.matchId,epoch:0,send:p=>uplink=p});c.setRtt(0);c.receive(a.snapshot(),0);
  return {a,c,send:()=>a.receive(0,uplink)};}
for(const magnet of [false,true])test('provisional snapshots retain own movement and four-ball '+(magnet?'magnet':'field')+' contacts',()=>{
  const {a,c,send}=setup([0,1,2,3].map(id=>({id,x:263+id*5,y:470-id*3,vx:0,vy:4,contact:0})));
  c.setInput({dx:1,boost:!magnet,magnet});c.advance(100);
  const predicted=P.hash(c.world),x=c.world.paddles[0];
  for(let i=0;i<6;i++)a.advance();
  const packet=a.snapshot();assert.equal(c.receive(Wire.readSnapshot(Wire.snapshot(packet),packet.world.config),100),true);
  assert.equal(c.world.paddles[0],x);assert.equal(P.hash(c.world),predicted,'complete predicted world survives delayed own uplink');
  send();a.flush();c.receive(a.snapshot(),100);assert.equal(P.hash(c.world),predicted,'accepted input does not produce a second correction');
  assert.equal(c.metrics.maxOwnPaddleCorrection,0);assert.ok(c.metrics.replayedLocalPastInputs>0);
});
test('accepted same-tick input wins and an input beyond the commit deadline is not invented',()=>{
  const {a,c}=setup();c.setInput({dx:1});c.advance(100);
  a.receive(0,{version:P.VERSION,matchId:a.matchId,epoch:0,commands:[{tick:1,seq:1,dx:-1,boost:0,magnet:false}]});
  for(let i=0;i<6;i++)a.advance();c.receive(a.snapshot(),100);
  assert.equal(c.commandsAt(1)[0].dx,-1);
  for(let i=0;i<25;i++)a.advance();c.world=null;c.receive(a.snapshot(),500);
  assert.equal(c.pending.size,0);assert.equal(P.hash(c.world),P.hash(a.world));
});
test('replay includes disconnect systems and new connections continue sequence after accepted boost',()=>{
  const {a,c}=setup();a.connect(0);a.receive(0,{version:P.VERSION,matchId:a.matchId,epoch:1,commands:[{tick:1,seq:50,dx:1,boost:50,magnet:false}]});
  a.advance();const joined=new Client({slot:0,matchId:a.matchId,epoch:1});joined.setRtt(0);joined.receive(a.snapshot(),0);
  joined.setInput({boost:true});joined.advance(20);assert.ok(joined.seq>50);assert.ok(joined.boost>50);
  a.disconnect(1);for(let i=0;i<3;i++)a.advance();c.epoch=1;c.world=null;c.receive(a.snapshot(),0);
  assert.equal(P.hash(c.world),P.hash(a.world));assert.equal(c.world.connected[1],false);
});
test('self-contained replay tolerates dropped snapshots and refuses obsolete protocol',()=>{
  const {a,c}=setup();for(let i=0;i<50;i++)a.advance();const p=a.snapshot();
  assert.equal(p.world.tick-p.replay.world.tick,WINDOW);
  assert.equal(c.receive({...p,version:'gf-net2-1'},100),false);
  assert.equal(c.receive(p,100),true);assert.equal(P.hash(c.world),P.hash(a.world));
});
