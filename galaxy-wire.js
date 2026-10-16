/* Compact, self-contained snapshots; no dependency on a previous network packet.
 * Match-constant configuration arrives reliably in net:init. Numbers retain full precision.
 */
(function(root,factory){if(typeof module==='object'&&module.exports)module.exports=factory(require('./galaxy-physics'));
else root.GalaxyWire=factory(root.GalaxyPhysics);})(typeof globalThis!=='undefined'?globalThis:this,function(P){
'use strict';
const bits=a=>a.reduce((n,v,i)=>n|(v?1<<i:0),0),flags=n=>[0,1,2,3].map(i=>!!(n&(1<<i)));
const extra=c=>[c.shield??null,c.emp??null,c.empX??null,c.empY??null];
const readExtra=(a,n)=>({... (a[n]!==null?{shield:a[n]}:{}),... (a[n+1]!==null?{emp:a[n+1],empX:a[n+2],empY:a[n+3]}:{})});
function input(p){return [3,p.matchId,p.epoch,p.commands.map(c=>[c.tick,c.seq,c.dx,c.boost,c.magnet?1:0,c.target??null,c.rocket||0,c.rocketTarget??-1,...extra(c)])];}
function readInput(p){
  if(!Array.isArray(p)||p.length!==4||p[0]!==3||!Array.isArray(p[3])||p[3].length>16)return null;
  if(typeof p[1]!=='string'||p[1].length>64||!Number.isSafeInteger(p[2])||p[2]<0)return null;
  if(p[3].some(c=>!Array.isArray(c)||c.length!==12||![0,1].includes(c[4])||
    [c[8],c[9]].some(v=>v!==null&&(!Number.isSafeInteger(v)||v<0))||[c[10],c[11]].some(v=>v!==null&&(!Number.isFinite(v)||v<0||v>520))||
    ![-1,0,1].includes(c[2])||(c[5]!==null&&(!Number.isFinite(c[5])||c[5]<0||c[5]>520))||!Number.isSafeInteger(c[6])||c[6]<0||![-1,0,1,2,3].includes(c[7])||![c[0],c[1],c[3]].every(n=>Number.isSafeInteger(n)&&n>=0)))return null;
  return {version:P.VERSION,matchId:p[1],epoch:p[2],commands:p[3].map(c=>({tick:c[0],seq:c[1],dx:c[2],boost:c[3],magnet:c[4]===1,target:c[5],rocket:c[6],rocketTarget:c[7],...readExtra(c,8)}))};
}
function packWorld(w){
  return [w.tick,w.rng,w.nextBallId,w.paddles,w.paddlesPrev,w.balls,w.respawns.map(r=>[r.timer,r.vx,r.vy]),
    w.lives,w.scores,w.energy,w.fields.map(f=>[f.active?1:0,f.t,f.r,f.maxR]),w.magEnergy,bits(w.magnet),bits(w.eliminated),
    w.eliminationOrder,bits(w.connected),w.botTargets,w.inputs.map(i=>[i.tick,i.dx,i.magnet?1:0,i.boost,i.target??null,i.rocket||0,i.rocketTarget??-1,...extra(i)]),w.lastBoost,w.gameOver?1:0,w.winner,bits(w.magPressed),w.teamLives,w.teamScores,w.winnerTeam,w.endReason,w.missiles,w.nextMissileId,w.lastRocket,w.rocketReadyAt,w.stunUntil,w.bayOpenAt,
    [w.goalShields,w.shieldHits,w.lastShield,w.shieldReadyAt,w.empWaves,w.nextEmpId,w.lastEmp,w.empReadyAt,w.empUntil]];
}
const packCommands=a=>a.map(x=>[x.slot,x.command.tick,x.command.seq,x.command.dx,x.command.boost,x.command.magnet?1:0,x.command.target??null,x.command.rocket||0,x.command.rocketTarget??-1,...extra(x.command)]);
const readCommands=a=>a.map(c=>({slot:c[0],command:{tick:c[1],seq:c[2],dx:c[3],boost:c[4],magnet:c[5]===1,target:c[6],rocket:c[7],rocketTarget:c[8],...readExtra(c,9)}}));
function snapshot(p){
  return [3,p.matchId,p.id,p.revision,p.committed,p.epochs,p.ack,
    packCommands(p.scheduled),
    [p.confirmed.lives,p.confirmed.scores,bits(p.confirmed.eliminated)],packWorld(p.world),p.sentAt,p.phase,
    [packWorld(p.replay.world),packCommands(p.replay.commands),p.replay.systems]];
}
function unpackWorld(b,config){
  return {version:P.VERSION,config,tick:b[0],rng:b[1],nextBallId:b[2],paddles:b[3],paddlesPrev:b[4],balls:b[5],
    respawns:b[6].map(r=>({timer:r[0],vx:r[1],vy:r[2]})),lives:b[7],scores:b[8],energy:b[9],
    fields:b[10].map(f=>({active:f[0]===1,t:f[1],r:f[2],maxR:f[3]})),magEnergy:b[11],magnet:flags(b[12]),magPressed:flags(b[21]),
    eliminated:flags(b[13]),eliminationOrder:b[14],connected:flags(b[15]),botTargets:b[16],
    inputs:b[17].map(i=>({tick:i[0],dx:i[1],magnet:i[2]===1,boost:i[3],target:i[4],rocket:i[5],rocketTarget:i[6],...readExtra(i,7)})),lastBoost:b[18],missiles:b[26],nextMissileId:b[27],lastRocket:b[28],rocketReadyAt:b[29],stunUntil:b[30],bayOpenAt:b[31],gameOver:b[19]===1,winner:b[20],
    goalShields:b[32][0],shieldHits:b[32][1],lastShield:b[32][2],shieldReadyAt:b[32][3],empWaves:b[32][4],nextEmpId:b[32][5],lastEmp:b[32][6],empReadyAt:b[32][7],empUntil:b[32][8],teamLives:b[22],teamScores:b[23],winnerTeam:b[24],endReason:b[25],events:[]};
}
function readSnapshot(p,config){
  if(!Array.isArray(p)||p[0]!==3||p.length!==13||!config)return null;
  const b=p[9],r=p[12];if(!Array.isArray(b)||b.length!==33||!Array.isArray(b[32])||b[32].length!==9||!Array.isArray(r)||r.length!==3||!Array.isArray(r[0])||r[0].length!==33||!Array.isArray(r[0][32])||r[0][32].length!==9||!Array.isArray(r[1])||!Array.isArray(r[2]))return null;
  const w=unpackWorld(b,config);
  return {version:P.VERSION,matchId:p[1],id:p[2],revision:p[3],committed:p[4],epochs:p[5],ack:p[6],
    scheduled:readCommands(p[7]),replay:{world:unpackWorld(r[0],config),commands:readCommands(r[1]),systems:r[2]},
    confirmed:{lives:p[8][0],scores:p[8][1],eliminated:flags(p[8][2])},world:w,sentAt:p[10],phase:p[11]};
}
return {input,readInput,snapshot,readSnapshot};
});
