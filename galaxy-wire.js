/* Compact, self-contained snapshots; no dependency on a previous network packet.
 * Match-constant configuration arrives reliably in net:init. Numbers retain full precision.
 */
(function(root,factory){if(typeof module==='object'&&module.exports)module.exports=factory(require('./galaxy-physics'));
else root.GalaxyWire=factory(root.GalaxyPhysics);})(typeof globalThis!=='undefined'?globalThis:this,function(P){
'use strict';
const bits=a=>a.reduce((n,v,i)=>n|(v?1<<i:0),0),flags=n=>[0,1,2,3].map(i=>!!(n&(1<<i)));
function input(p){return [2,p.matchId,p.epoch,p.commands.map(c=>[c.tick,c.seq,c.dx,c.boost,c.magnet?1:0])];}
function readInput(p){
  if(!Array.isArray(p)||p.length!==4||p[0]!==2||!Array.isArray(p[3])||p[3].length>16)return null;
  if(typeof p[1]!=='string'||p[1].length>64||!Number.isSafeInteger(p[2])||p[2]<0)return null;
  if(p[3].some(c=>!Array.isArray(c)||c.length!==5||![0,1].includes(c[4])||
    ![-1,0,1].includes(c[2])||![c[0],c[1],c[3]].every(n=>Number.isSafeInteger(n)&&n>=0)))return null;
  return {version:P.VERSION,matchId:p[1],epoch:p[2],commands:p[3].map(c=>({tick:c[0],seq:c[1],dx:c[2],boost:c[3],magnet:c[4]===1}))};
}
function snapshot(p){const w=p.world;
  const b=[w.tick,w.rng,w.nextBallId,w.paddles,w.paddlesPrev,w.balls,w.respawns.map(r=>[r.timer,r.vx,r.vy]),
    w.lives,w.scores,w.energy,w.fields.map(f=>[f.active?1:0,f.t,f.r,f.maxR]),w.magEnergy,bits(w.magnet),bits(w.eliminated),
    w.eliminationOrder,bits(w.connected),w.botTargets,w.inputs.map(i=>[i.tick,i.dx,i.magnet?1:0,i.boost]),w.lastBoost,w.gameOver?1:0,w.winner];
  return [2,p.matchId,p.id,p.revision,p.committed,p.epochs,p.ack,
    p.scheduled.map(x=>[x.slot,x.command.tick,x.command.seq,x.command.dx,x.command.boost,x.command.magnet?1:0]),
    [p.confirmed.lives,p.confirmed.scores,bits(p.confirmed.eliminated)],b,p.sentAt,p.phase];
}
function readSnapshot(p,config){
  if(!Array.isArray(p)||p[0]!==2||p.length!==12||!config)return null;
  const b=p[9];if(!Array.isArray(b)||b.length!==21)return null;
  const w={version:P.VERSION,config,tick:b[0],rng:b[1],nextBallId:b[2],paddles:b[3],paddlesPrev:b[4],balls:b[5],
    respawns:b[6].map(r=>({timer:r[0],vx:r[1],vy:r[2]})),lives:b[7],scores:b[8],energy:b[9],
    fields:b[10].map(f=>({active:f[0]===1,t:f[1],r:f[2],maxR:f[3]})),magEnergy:b[11],magnet:flags(b[12]),
    eliminated:flags(b[13]),eliminationOrder:b[14],connected:flags(b[15]),botTargets:b[16],
    inputs:b[17].map(i=>({tick:i[0],dx:i[1],magnet:i[2]===1,boost:i[3]})),lastBoost:b[18],gameOver:b[19]===1,winner:b[20],events:[]};
  return {version:P.VERSION,matchId:p[1],id:p[2],revision:p[3],committed:p[4],epochs:p[5],ack:p[6],
    scheduled:p[7].map(c=>({slot:c[0],command:{tick:c[1],seq:c[2],dx:c[3],boost:c[4],magnet:c[5]===1}})),
    confirmed:{lives:p[8][0],scores:p[8][1],eliminated:flags(p[8][2])},world:w,sentAt:p[10],phase:p[11]};
}
return {input,readInput,snapshot,readSnapshot};
});
