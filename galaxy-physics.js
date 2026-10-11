/* Shared authoritative physics. Same file in Node and Android/WebView.
 * No clocks, network, database or unseeded randomness. Complete replay checkpoints.
 * Existing magnet/game balance retained; moving contact resolution and step ownership are new.
 */
(function(root,factory){if(typeof module==='object'&&module.exports)module.exports=factory();
else root.GalaxyPhysics=factory();})(typeof globalThis!=='undefined'?globalThis:this,function(){
'use strict';

const VERSION='gf-net2-1';
const TICK_RATE=60,TICK_MS=1000/60,W=520,H=520,BR=8,SMAX=4.875,C=130;
const PL=54,PLV=54,PTH=16,PTV=16,ML=10,EPU=1/3,ECR=1/10000,FDR=380,FR=54,PS=3.375;
const SLOTS=[0,1,2,3],SLOT_VIEW=['bottom','top','left','right'];
const ELIM_WALL_BOOST=1.30,ELIM_WALL_RANDOM_DEG=25,ELIM_WALL_MIN_SP=3.5,ELIM_WALL_MAX_SP=6.5;
const MAG_R=75,MAG_PULL_FORCE=.35,MAG_CAPTURE_RADIUS=55,MAG_RELEASE_PUSH=3.5;
const MAG_DRAIN_PER_MS=1/5000,MAG_REGEN_PER_MS=1/10000,MAG_MIN_ENERGY=.10;
const CS=[{ax:0,ay:C,bx:C,by:0,nx:Math.SQRT1_2,ny:Math.SQRT1_2},
{ax:W-C,ay:0,bx:W,by:C,nx:-Math.SQRT1_2,ny:Math.SQRT1_2},
{ax:0,ay:H-C,bx:C,by:H,nx:Math.SQRT1_2,ny:-Math.SQRT1_2},
{ax:W-C,ay:H,bx:W,by:H-C,nx:-Math.SQRT1_2,ny:-Math.SQRT1_2}];
function getSlotPlayer(room,s){return room.players[s];}
function emit(gs,type,data){gs.events.push({type,data});}
function random(gs){gs.rng=(Math.imul(gs.rng,1664525)+1013904223)>>>0;return gs.rng/4294967296;}
function clone(s){return JSON.parse(JSON.stringify(s));}
const clamp=(x,a,b)=>Math.max(a,Math.min(b,x));
const round=x=>Math.round(x*1000000)/1000000;

function slotToPaddle(slot, cx, gs, room) {
  const view = SLOT_VIEW[slot];
  let pw = PL, pvh = PLV;
  if (room) {
    const player = getSlotPlayer(room, slot);
    if (player && player.paddleStats && player.paddleStats.w) {
      if (view === 'bottom' || view === 'top') pw = player.paddleStats.w;
      else pvh = player.paddleStats.w;
    }
    if (!player && room._disconnected && room._disconnected[slot] && room._disconnected[slot].paddleStats) {
      const dps = room._disconnected[slot].paddleStats;
      if (view === 'bottom' || view === 'top') pw = dps.w || PL;
      else pvh = dps.w || PLV;
    }
  }
  if (view==='bottom') return {x:cx-pw/2,  y:H-PTH-2, w:pw,  h:PTH, axis:'x', min:C, max:W-C-pw};
  if (view==='top')    return {x:cx-pw/2,  y:2,        w:pw,  h:PTH, axis:'x', min:C, max:W-C-pw};
  if (view==='left')   return {x:2,         y:cx-pvh/2, w:PTV, h:pvh, axis:'y', min:C, max:H-C-pvh};
  if (view==='right')  return {x:W-PTV-2,   y:cx-pvh/2, w:PTV, h:pvh, axis:'y', min:C, max:H-C-pvh};
}

function cPt(px, py, ax, ay, bx, by) {
  const dx = bx-ax, dy = by-ay, l2 = dx*dx+dy*dy;
  if (!l2) return { cx: ax, cy: ay };
  const t = Math.max(0, Math.min(1, ((px-ax)*dx+(py-ay)*dy)/l2));
  return { cx: ax+t*dx, cy: ay+t*dy };
}

function resolveChamfersBall(ball) {
  for(const s of CS){
    const{cx,cy}=cPt(ball.x,ball.y,s.ax,s.ay,s.bx,s.by);
    const d=Math.hypot(ball.x-cx,ball.y-cy);
    if(d<BR+1){
      let nx=ball.x-cx,ny=ball.y-cy;const l=Math.hypot(nx,ny);
      if(l<0.001){nx=s.nx;ny=s.ny;}else{nx/=l;ny/=l;}
      if(nx*s.nx+ny*s.ny<0){nx=-nx;ny=-ny;}
      const dot=ball.vx*nx+ball.vy*ny;if(dot<0){ball.vx-=2*dot*nx;ball.vy-=2*dot*ny;}
      ball.x+=nx*(BR+1-d);ball.y+=ny*(BR+1-d);
      const spd=Math.hypot(ball.vx,ball.vy);if(spd>SMAX){ball.vx=ball.vx/spd*SMAX;ball.vy=ball.vy/spd*SMAX;}
    }
  }
}

function clampBallObj(ball, room, gs) {
  for(const s of CS){
    const d=(ball.x-s.ax)*s.nx+(ball.y-s.ay)*s.ny;
    if(d<-BR){
      const preX = ball.x, preY = ball.y, preVx = ball.vx, preVy = ball.vy;
      const dv=ball.vx*s.nx+ball.vy*s.ny;ball.vx-=2*dv*s.nx;ball.vy-=2*dv*s.ny;
      const{cx,cy}=cPt(ball.x,ball.y,s.ax,s.ay,s.bx,s.by);
      ball.x=cx+s.nx*(BR+1);ball.y=cy+s.ny*(BR+1);
      if (room && gs) {
        emit(gs, 'event:ball', {
          type: 'chamfer_bounce',
          tick: gs.tick,
          ballId: ball.id,
          pre:  { x: preX, y: preY, vx: preVx, vy: preVy },
          post: { x: ball.x, y: ball.y, vx: ball.vx, vy: ball.vy },
        });
      }
    }
  }
}

function getFFRadius(f) {
  const maxR = f.maxR || FR;
  return Math.min(maxR, (f.t / 200) * maxR);
}

function applyEliminatedWall(gs, slot, ball, room) {
  const preX = ball.x, preY = ball.y, preVx = ball.vx, preVy = ball.vy;
  let nx, ny;
  const view = SLOT_VIEW[slot];
  if (view === 'bottom')     { nx = 0; ny = -1; }
  else if (view === 'top')   { nx = 0; ny =  1; }
  else if (view === 'left')  { nx = 1; ny =  0; }
  else                       { nx =-1; ny =  0; }
  const vDotN = ball.vx * nx + ball.vy * ny;
  let reflVx = ball.vx - 2 * vDotN * nx;
  let reflVy = ball.vy - 2 * vDotN * ny;
  const randAngle = (random(gs) * 2 - 1) * ELIM_WALL_RANDOM_DEG * Math.PI / 180;
  const cosA = Math.cos(randAngle), sinA = Math.sin(randAngle);
  const rotX = reflVx * cosA - reflVy * sinA;
  const rotY = reflVx * sinA + reflVy * cosA;
  reflVx = rotX; reflVy = rotY;
  reflVx *= ELIM_WALL_BOOST;
  reflVy *= ELIM_WALL_BOOST;
  let sp = Math.hypot(reflVx, reflVy);
  if (sp < ELIM_WALL_MIN_SP) {
    const k = ELIM_WALL_MIN_SP / Math.max(0.1, sp);
    reflVx *= k; reflVy *= k;
    sp = ELIM_WALL_MIN_SP;
  }
  if (sp > ELIM_WALL_MAX_SP) {
    const k = ELIM_WALL_MAX_SP / sp;
    reflVx *= k; reflVy *= k;
  }
  const newVDotN = reflVx * nx + reflVy * ny;
  if (newVDotN < 0.5) {
    reflVx += nx * (1.0 - newVDotN);
    reflVy += ny * (1.0 - newVDotN);
  }
  ball.vx = reflVx;
  ball.vy = reflVy;
  if (room) {
    emit(gs, 'event:ball', {
      type: 'elim_wall_bounce',
      tick: gs.tick,
      ballId: ball.id,
      slot,
      pre:  { x: preX, y: preY, vx: preVx, vy: preVy },
      post: { x: ball.x, y: ball.y, vx: ball.vx, vy: ball.vy },
    });
  }
}

function isBallInMagnet(gs, s, ball) {
  const p = slotToPaddle(s, gs.paddles[s], gs, gs.config);
  const fcx = p.x + p.w/2, fcy = p.y + p.h/2;
  const dx = ball.x - fcx, dy = ball.y - fcy;
  const dist = Math.hypot(dx, dy);
  if (dist > MAG_R + BR) return null;
  const view = SLOT_VIEW[s];
  let faceNy = 0, faceNx = 0;
  if (view === 'bottom')     { faceNx = 0; faceNy = -1; }
  else if (view === 'top')   { faceNx = 0; faceNy =  1; }
  else if (view === 'left')  { faceNx = 1; faceNy =  0; }
  else                       { faceNx =-1; faceNy =  0; }
  const dot = dx * faceNx + dy * faceNy;
  if (dot < 0) return null; // м'яч позаду ракетки — не тягнемо
  return { p, fcx, fcy, dist, dx, dy, faceNx, faceNy };
}

function applyMagnetBall(gs, s, ball, room) {
  if (!gs.magnet[s]) return false;
  if ((ball['mag_release_until_'+s]||0)>gs.tick) return false;
  const p = slotToPaddle(s, gs.paddles[s], gs, gs.config);
  const fcx = p.x + p.w/2, fcy = p.y + p.h/2;
  const view = SLOT_VIEW[s];
  let nx = 0, ny = 0;
  if (view === 'bottom')     { nx = 0; ny = -1; }
  else if (view === 'top')   { nx = 0; ny =  1; }
  else if (view === 'left')  { nx = 1; ny =  0; }
  else                       { nx =-1; ny =  0; }
  const dx = ball.x - fcx, dy = ball.y - fcy;
  const dist = Math.hypot(dx, dy);
  if (dist > MAG_R + BR) {
    if (ball['mag_held_' + s]) {
      releaseMagnetBall(gs, s, ball, 0, room);
    }
    delete ball['_magInZone_' + s];
    return false;
  }
  const frontDot = dx * nx + dy * ny;
  if (frontDot < 0) {
    if (ball['mag_held_' + s]) {
      releaseMagnetBall(gs, s, ball, 0, room);
    }
    return false;
  }
  const offKey_X = 'mag_offX_' + s;
  const offKey_Y = 'mag_offY_' + s;
  const heldKey  = 'mag_held_' + s;
  if (!ball[heldKey]) {
    const tgtX = fcx + nx * MAG_R * 0.40;
    const tgtY = fcy + ny * MAG_R * 0.40;
    const toX = tgtX - ball.x, toY = tgtY - ball.y;
    const toD = Math.hypot(toX, toY);
    if (toD < MAG_CAPTURE_RADIUS) {
      const preX = ball.x, preY = ball.y, preVx = ball.vx, preVy = ball.vy;
      ball[heldKey] = true;
      ball[offKey_X] = ball.x - fcx;
      ball[offKey_Y] = ball.y - fcy;
      ball.vx = 0;
      ball.vy = 0;
      delete ball['_magInZone_' + s];
      if (room) {
        emit(gs, 'event:ball', {
          type: 'magnet_capture',
          tick: gs.tick,
          ballId: ball.id,
          slot: s,
          pre:  { x: preX, y: preY, vx: preVx, vy: preVy },
          post: { x: ball.x, y: ball.y, vx: 0, vy: 0 },
          offX: ball[offKey_X], offY: ball[offKey_Y],
        });
      }
    } else {
      if (toD > 0.1) {
        const tnx = toX / toD, tny = toY / toD;
        const pull = MAG_PULL_FORCE * 2.0;
        ball.vx += tnx * pull;
        ball.vy += tny * pull;
      }
      ball['_magInZone_' + s] = (ball['_magInZone_' + s] || 0) + 1;
      if (ball['_magInZone_' + s] === 10 || ball['_magInZone_' + s] === 20) {
      }
    }
    return true;
  }
  const offX = ball[offKey_X] || 0;
  const offY = ball[offKey_Y] || 0;
  const offDist = Math.hypot(offX, offY);
  if (offDist > MAG_R * 0.85) {
    const scale = (MAG_R * 0.85) / offDist;
    ball[offKey_X] = offX * scale;
    ball[offKey_Y] = offY * scale;
  }
  const finalOffX = ball[offKey_X] || 0;
  const finalOffY = ball[offKey_Y] || 0;
  const newX = fcx + finalOffX;
  const newY = fcy + finalOffY;
  const moveDelta = Math.hypot(newX - ball.x, newY - ball.y);
  if (moveDelta > 15) {
  }
  ball.vx = newX - ball.x;
  ball.vy = newY - ball.y;
  ball.x = newX;
  ball.y = newY;
  return true;
}

function releaseMagnetBall(gs, s, ball, releaseIndex, room) {
  if (!ball['mag_held_' + s]) return;
  const preX = ball.x, preY = ball.y, preVx = ball.vx, preVy = ball.vy;
  const offX = ball['mag_offX_' + s] || 0;
  const offY = ball['mag_offY_' + s] || 0;
  delete ball['mag_held_' + s];
  delete ball['mag_offX_' + s];
  delete ball['mag_offY_' + s];
  const p = slotToPaddle(s, gs.paddles[s], gs, gs.config);
  const fcx = p.x + p.w/2, fcy = p.y + p.h/2;
  const view = SLOT_VIEW[s];
  let nx = 0, ny = 0, tx = 0, ty = 0;
  if (view === 'bottom')     { nx = 0; ny = -1; tx = 1; ty = 0; }
  else if (view === 'top')   { nx = 0; ny =  1; tx = 1; ty = 0; }
  else if (view === 'left')  { nx = 1; ny =  0; tx = 0; ty = 1; }
  else                       { nx =-1; ny =  0; tx = 0; ty = 1; }
  const tangentOffset = offX * tx + offY * ty;
  const half = p.w/2 * (tx !== 0 ? 1 : 0) + p.h/2 * (ty !== 0 ? 1 : 0);
  let k = tangentOffset / Math.max(half, 1);
  if (k > 1) k = 1; else if (k < -1) k = -1;
  const indexOffset = (releaseIndex||0) * 0.20; // ~11° зсув за кожний м'яч
  k = k + (k >= 0 ? -indexOffset : indexOffset);
  if (k > 1) k = 1; else if (k < -1) k = -1;
  const MAX_ANGLE = Math.PI / 3;
  const angle = k * MAX_ANGLE;
  const cosA = Math.cos(angle), sinA = Math.sin(angle);
  const outX = nx * cosA + tx * sinA;
  const outY = ny * cosA + ty * sinA;
  const targetSp = Math.min(MAG_RELEASE_PUSH, SMAX);
  ball.vx = outX * targetSp;
  ball.vy = outY * targetSp;
  // Release from the held position, not 85px away. A short capture cooldown
  // replaces the old teleport and is part of rollback state.
  ball['mag_release_until_'+s] = gs.tick + 12;
  ball.x  = Math.round(ball.x  * 10) / 10;
  ball.y  = Math.round(ball.y  * 10) / 10;
  ball.vx = Math.round(ball.vx * 10) / 10;
  ball.vy = Math.round(ball.vy * 10) / 10;
  ball.contact=(ball.contact||0)+1;
  if (room) {
    emit(gs, 'event:ball', {
      type: 'magnet_release',
      tick: gs.tick,
      ballId: ball.id,
      slot: s,
      pre:  { x: preX, y: preY, vx: preVx, vy: preVy },
      post: { x: ball.x, y: ball.y, vx: ball.vx, vy: ball.vy },
    });
  }
}

// Relative-motion CCD: the field moves WITH the paddle during this tick.
function applyFFBall(gs,s,b,old) {
  const f=gs.fields[s]; if(!f.active)return;
  const p=slotToPaddle(s,gs.paddles[s],gs,gs.config);
  const cx=p.x+p.w/2,cy=p.y+p.h/2, move=gs.paddles[s]-gs.paddlesPrev[s];
  const mx=s<2?move:0,my=s<2?0:move,r=f.maxR+BR;
  const ox=old.x-(cx-mx),oy=old.y-(cy-my);
  const dx=b.x-cx,dy=b.y-cy,rx=dx-ox,ry=dy-oy;
  const dist=Math.hypot(dx,dy),od=Math.hypot(ox,oy),key='ff_hit_'+s;
  let t=null;
  const a=rx*rx+ry*ry,bb=2*(ox*rx+oy*ry),cc=ox*ox+oy*oy-r*r;
  const disc=bb*bb-4*a*cc;
  if(a>1e-9&&disc>=0){const hit=(-bb-Math.sqrt(disc))/(2*a);if(hit>=0&&hit<=1)t=hit;}
  if(dist>r&&t===null){if(dist>r+2)b[key]=false;return;}
  const contact=t!==null&&!b[key]&&od>=r;
  let nx=contact?ox+rx*t:dx,ny=contact?oy+ry*t:dy,len=Math.hypot(nx,ny);
  if(len<1e-9){nx=s===2?1:s===3?-1:0;ny=s===0?-1:s===1?1:0;len=1;}
  nx/=len;ny/=len;
  if(contact){
    const dot=(b.vx-mx)*nx+(b.vy-my)*ny;
    if(dot<0){b.vx-=2*dot*nx;b.vy-=2*dot*ny;}
    b.vx+=mx*.7+nx;b.vy+=my*.7+ny;
    b.x=old.x+(b.x-old.x)*t+nx*2;
    b.y=old.y+(b.y-old.y)*t+ny*2;
    b[key]=true;b.contact=(b.contact||0)+1;
    emit(gs,'event:ball',{type:'field_reflect',slot:s,ballId:b.id,tick:gs.tick});
  }else{
    const penetration=clamp(1-dist/r,0,1),force=.28*(1+penetration*penetration*3.5);
    b.vx+=nx*force;b.vy+=ny*force;
  }
  const speed=Math.hypot(b.vx,b.vy);
  if(speed>.01){const k=clamp(speed,2.5,SMAX)/speed;b.vx*=k;b.vy*=k;}
}

// Swept ball vs a moving rectangle; no dependence on render frames.
function paddleContact(gs,s,b,old) {
  const p=slotToPaddle(s,gs.paddlesPrev[s],gs,gs.config);
  const move=gs.paddles[s]-gs.paddlesPrev[s],mx=s<2?move:0,my=s<2?0:move;
  const vx=b.x-old.x-mx,vy=b.y-old.y-my;
  const nx=s===2?1:s===3?-1:0,ny=s===0?-1:s===1?1:0;
  if(b.vx*nx+b.vy*ny>=0)return false;
  let enter=0,exit=1;
  for(const [o,v,lo,hi] of [[old.x,vx,p.x-BR,p.x+p.w+BR],[old.y,vy,p.y-BR,p.y+p.h+BR]]){
    if(Math.abs(v)<1e-9){if(o<lo||o>hi)return false;continue;}
    let a=(lo-o)/v,z=(hi-o)/v;if(a>z)[a,z]=[z,a];
    enter=Math.max(enter,a);exit=Math.min(exit,z);if(enter>exit)return false;
  }
  if(enter<0||enter>1)return false;
  const speed=Math.hypot(b.vx,b.vy),center=gs.paddlesPrev[s]+move*enter;
  const hitX=old.x+(b.x-old.x)*enter,hitY=old.y+(b.y-old.y)*enter;
  const offset=clamp(((s<2?hitX:hitY)-center)/(gs.config.players[s].paddleStats.w/2),-.85,.85);
  b.vx=s<2?offset*speed*.7:nx*speed*.85;
  b.vy=s<2?ny*speed*.85:offset*speed*.7;
  const actual=Math.hypot(b.vx,b.vy);if(actual>1e-9){b.vx*=speed/actual;b.vy*=speed/actual;}
  b.x=hitX+nx*.01+b.vx*(1-enter);b.y=hitY+ny*.01+b.vy*(1-enter);
  b.contact=(b.contact||0)+1;
  emit(gs,'paddle:hit',{slot:s,speed:round(speed),tick:gs.tick});return true;
}

function queueBall(gs,delay) {
  const ang=(random(gs)*.6+.2)*Math.PI*(random(gs)<.5?1:-1)+(random(gs)<.5?0:Math.PI);
  const speed=2.5+random(gs)*.5;
  const tail=gs.respawns.reduce((m,r)=>Math.max(m,r.timer),0);
  gs.respawns.push({timer:delay??Math.max(2000,tail+2000),vx:round(Math.cos(ang)*speed),vy:round(Math.sin(ang)*speed)});
}
function create(options={}) {
  const config={players:[],durationTicks:options.durationTicks||10800};
  const arr=v=>SLOTS.map(()=>typeof v==='function'?v():v);
  for(const s of SLOTS){const p=options.players?.[s]||{},st=p.paddleStats||{};
    config.players[s]={bot:!!p.bot,paddleStats:{w:clamp(st.w||PL,10,250),spd:clamp(st.spd||PS,.1,50),
      fr:clamp(st.fr||FR,1,220),fd:clamp(st.fd||1,.1,20),er:clamp(st.er||1,.1,20)}};
  }
  const gs={version:VERSION,config,tick:0,rng:(options.seed??123456789)>>>0,nextBallId:1,
    paddles:arr(260),paddlesPrev:arr(260),balls:[],respawns:[],lives:arr(ML),scores:arr(0),
    energy:arr(1),fields:arr(()=>({active:false,t:0,r:0,maxR:FR})),magEnergy:arr(1),magnet:arr(false),
    eliminated:arr(false),eliminationOrder:[],connected:arr(true),botTargets:arr(260),
    inputs:arr(()=>({tick:0,dx:0,magnet:false,boost:0})),lastBoost:arr(0),gameOver:false,winner:null,events:[]};
  for(let i=0;i<4;i++)queueBall(gs,(options.spawnDelayMs??8000)+i*2000);
  return gs;
}
function eliminate(gs,s,reason) {
  if(gs.eliminated[s])return;
  gs.eliminated[s]=true;gs.lives[s]=0;gs.eliminationOrder.push(s);
  gs.fields[s].active=false;gs.fields[s].r=0;gs.magnet[s]=false;
  let i=0;for(const b of gs.balls)if(b['mag_held_'+s])releaseMagnetBall(gs,s,b,i++,gs.config);
  emit(gs,'eliminated',{slot:s,reason,tick:gs.tick});
}
function finishIfNeeded(gs) {
  const alive=SLOTS.filter(s=>!gs.eliminated[s]);
  if(alive.length<=1||gs.tick>=gs.config.durationTicks){
    gs.gameOver=true;gs.winner=alive.sort((a,b)=>gs.lives[b]-gs.lives[a]||gs.scores[a]-gs.scores[b]||a-b)[0]??gs.eliminationOrder.at(-1)??0;
    emit(gs,'finish',{winner:gs.winner,tick:gs.tick});
  }
}
function botInput(gs,s) {
  const horizontal=s<2,wall=s===0?510:s===1?10:s===2?10:510;
  let ball=null,best=Infinity;
  for(const b of gs.balls){const vel=s===0?b.vy:s===1?-b.vy:s===2?-b.vx:b.vx;
    const d=Math.abs((horizontal?b.y:b.x)-wall),t=d/Math.max(.01,vel);
    if(vel>0&&t<best){best=t;ball=b;}}
  let target=260;if(ball)target=(horizontal?ball.x:ball.y)+(horizontal?ball.vx:ball.vy)*Math.min(best,16)*.8;
  gs.botTargets[s]+= (target+(random(gs)-.5)*5-gs.botTargets[s])*.16;
  const diff=gs.botTargets[s]-gs.paddles[s],fr=gs.config.players[s].paddleStats.fr;
  return {tick:gs.tick,dx:Math.abs(diff)>1?Math.sign(diff):0,magnet:false,
    boost:best>=5&&best<=30&&Math.abs(target-gs.paddles[s])<fr+BR+4?gs.lastBoost[s]+1:gs.lastBoost[s]};
}
function step(gs,commands={},systems=[]) {
  gs.tick++;gs.events=[];
  // Continue ticking a provisional outcome until the authority commits it.
  if(gs.gameOver)return gs;
  gs.paddlesPrev=gs.paddles.slice();
  for(const sys of systems){const s=sys.slot;if(!SLOTS.includes(s))continue;
    if(sys.type==='eliminate')eliminate(gs,s,sys.reason||'leave');
    if(sys.type==='connected'){gs.connected[s]=!!sys.value;gs.inputs[s]={tick:gs.tick,dx:0,magnet:false,boost:gs.lastBoost[s]};}}
  for(const s of SLOTS){
    if(gs.eliminated[s])continue;
    const stats=gs.config.players[s].paddleStats,f=gs.fields[s];
    if(f.active){f.t=round(f.t+TICK_MS);f.r=Math.min(f.maxR,f.t/200*f.maxR);
      if(f.t>=FDR*stats.fd){f.active=false;f.t=0;f.r=0;for(const b of gs.balls)delete b['ff_hit_'+s];}}
    else gs.energy[s]=Math.min(1,gs.energy[s]+ECR*TICK_MS*stats.er);
    let input=commands[s];
    if(gs.config.players[s].bot)input=botInput(gs,s);
    if(input)gs.inputs[s]={tick:gs.tick,dx:clamp(input.dx||0,-1,1),magnet:!!input.magnet,boost:input.boost||0};
    input=gs.inputs[s];
    // An absent client cannot keep moving/holding magnet indefinitely.
    const fresh=gs.connected[s]&&gs.tick-input.tick<=8;
    gs.paddles[s]=round(clamp(gs.paddles[s]+(fresh?input.dx:0)*stats.spd,C+stats.w/2,W-C-stats.w/2));
    if(input.boost>gs.lastBoost[s]){gs.lastBoost[s]=input.boost;
      if(fresh&&!f.active&&gs.energy[s]>=EPU){f.active=true;f.t=0;f.r=0;f.maxR=stats.fr;gs.energy[s]-=EPU;
        emit(gs,'event:field',{type:'activate',slot:s,tick:gs.tick,paddleX:gs.paddles[s],maxR:f.maxR});}}
    const was=gs.magnet[s];gs.magnet[s]=fresh&&input.magnet&&(was?gs.magEnergy[s]>0:gs.magEnergy[s]>=MAG_MIN_ENERGY);
    if(gs.magnet[s])gs.magEnergy[s]=Math.max(0,gs.magEnergy[s]-MAG_DRAIN_PER_MS*TICK_MS);
    else {gs.magEnergy[s]=Math.min(1,gs.magEnergy[s]+MAG_REGEN_PER_MS*TICK_MS);
      if(was){let i=0;for(const b of gs.balls)if(b['mag_held_'+s])releaseMagnetBall(gs,s,b,i++,gs.config);}}
  }
  for(let i=gs.respawns.length-1;i>=0;i--){const r=gs.respawns[i];r.timer=round(r.timer-TICK_MS);
    if(r.timer<=.0001){gs.balls.push({id:gs.nextBallId++,x:260,y:260,vx:r.vx,vy:r.vy,contact:0});gs.respawns.splice(i,1);}}
  for(let i=gs.balls.length-1;i>=0;i--){const b=gs.balls[i],old={x:b.x,y:b.y};
    let held=SLOTS.find(s=>b['mag_held_'+s]);
    if(held===undefined){b.x+=b.vx;b.y+=b.vy;
      for(const s of SLOTS)if(!gs.eliminated[s])applyFFBall(gs,s,b,old);}
    // A ball belongs to at most one magnet. Stable slot order resolves simultaneous capture.
    for(const s of SLOTS){if(gs.eliminated[s]||(held!==undefined&&held!==s))continue;
      const had=!!b['mag_held_'+s];applyMagnetBall(gs,s,b,gs.config);
      if(had!==!!b['mag_held_'+s])b.contact=(b.contact||0)+1;
      if(b['mag_held_'+s])held=s;}
    if(held===undefined){resolveChamfersBall(b);clampBallObj(b,gs.config,gs);
      let hit=false;for(const s of SLOTS)if(!gs.eliminated[s]&&paddleContact(gs,s,b,old)){hit=true;break;}
      if(!hit){let goal=-1;
        if(b.y-BR<0&&b.x>C&&b.x<W-C)goal=1;
        else if(b.y+BR>H&&b.x>C&&b.x<W-C)goal=0;
        else if(b.x-BR<0&&b.y>C&&b.y<H-C)goal=2;
        else if(b.x+BR>W&&b.y>C&&b.y<H-C)goal=3;
        if(goal>=0){if(gs.eliminated[goal]){
          if(goal<2)b.y=goal===0?H-BR:BR;else b.x=goal===2?BR:W-BR;
          applyEliminatedWall(gs,goal,b,gs.config);b.contact=(b.contact||0)+1;
        }else{gs.scores[goal]++;gs.lives[goal]--;emit(gs,'goal',{slot:goal,lives:gs.lives.slice(),scores:gs.scores.slice(),tick:gs.tick});
          if(gs.lives[goal]<=0)eliminate(gs,goal,'lives');gs.balls.splice(i,1);queueBall(gs);continue;}}
      }
    }
    for(const key of Object.keys(b))if(typeof b[key]==='number')b[key]=round(b[key]);
  }
  for(const s of SLOTS){gs.energy[s]=round(gs.energy[s]);gs.magEnergy[s]=round(gs.magEnergy[s]);}
  finishIfNeeded(gs);return gs;
}
function checkpoint(gs){const s=clone(gs);s.events=[];return s;}
function hash(gs){const s=checkpoint(gs),str=JSON.stringify(s);let h=2166136261;
  for(let i=0;i<str.length;i++)h=Math.imul(h^str.charCodeAt(i),16777619);return (h>>>0).toString(16);}
return {VERSION,TICK_RATE,TICK_MS,SLOTS,create,step,clone,checkpoint,hash,slotToPaddle,paddleContact,applyFFBall};
});
