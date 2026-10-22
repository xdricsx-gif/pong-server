/* Protocol 2: bounded authority rollback and fixed-tick client prediction.
 * Transport independent; the production adapter uses latest-state Socket.IO packets.
 * Gameplay effects are provisional. Only onCommit may change UI results or rewards.
 */
(function(root,factory){if(typeof module==='object'&&module.exports)module.exports=factory(require('./galaxy-physics.js'));
else root.GalaxyNetcode=factory(root.GalaxyPhysics);})(typeof globalThis!=='undefined'?globalThis:this,function(P){
'use strict';
const WINDOW=8,FUTURE=36,MAX_PREDICT=FUTURE,HISTORY=64,DT=P.TICK_MS;
const copy=P.clone,clamp=(x,a,b)=>Math.max(a,Math.min(b,x));
const validInt=x=>Number.isSafeInteger(x)&&x>=0;
class Authority {
  constructor({matchId,world,onCommit=()=>{}}){
    this.matchId=matchId;this.world=P.checkpoint(world);this.onCommit=onCommit;
    this.history=new Map([[world.tick,P.checkpoint(world)]]);this.events=new Map();
    this.commands=new Map();this.systems=new Map();this.epochs=[0,0,0,0];this.minTicks=[0,0,0,0];
    this.seen=P.SLOTS.map(()=>new Map());this.ack=[0,0,0,0];this.dirty=null;
    this.committed=world.tick;this.revision=0;this.snapshotId=0;this.finished=false;
    this.metrics={accepted:0,duplicate:0,rejected:0,tooLate:0,rollbacks:0,replayedTicks:0,maxReplay:0};
  }
  connect(slot){
    this.epochs[slot]++;this.seen[slot].clear();this.ack[slot]=0;this.minTicks[slot]=this.world.tick+1;
    // Clear previously scheduled input; an old connection cannot steer a resumed slot.
    for(const [tick,list] of this.commands)if(tick>this.world.tick)delete list[slot];
    this.system({type:'connected',slot,value:true});return this.epochs[slot];
  }
  disconnect(slot,voluntary=false){
    this.epochs[slot]++;this.minTicks[slot]=this.world.tick+1;
    for(const [tick,list] of this.commands)if(tick>this.world.tick)delete list[slot];
    this.system({type:'connected',slot,value:false});
    if(voluntary)this.system({type:'eliminate',slot,reason:'leave'});
  }
  system(event){const t=this.world.tick+1;if(!this.systems.has(t))this.systems.set(t,[]);this.systems.get(t).push(copy(event));}
  receive(slot,packet){
    if(this.finished||!P.SLOTS.includes(slot)||!packet||packet.matchId!==this.matchId||
      packet.epoch!==this.epochs[slot]||packet.version!==P.VERSION||!Array.isArray(packet.commands)||packet.commands.length>16){this.metrics.rejected++;return false;}
    for(const c of packet.commands){
      if(!c||!validInt(c.seq)||c.seq===0||!validInt(c.tick)||!validInt(c.boost)||c.boost>c.seq||
        ![-1,0,1].includes(c.dx)||(c.target!=null&&(!Number.isFinite(c.target)||c.target<0||c.target>520))||
        (c.rocket!==undefined&&(!validInt(c.rocket)||c.rocket>c.seq))||(c.rocketTarget!==undefined&&![-1,0,1,2,3].includes(c.rocketTarget))||
        ['shield','emp','cloak'].some(k=>c[k]!==undefined&&(!validInt(c[k])||c[k]>c.seq))||
        ['empX','empY'].some(k=>c[k]!=null&&(!Number.isFinite(c[k])||c[k]<0||c[k]>520))||
        typeof c.magnet!=='boolean'||c.tick>this.world.tick+FUTURE||c.tick<this.minTicks[slot]){this.metrics.rejected++;continue;}
      const seen=this.seen[slot];if(seen.has(c.seq)){this.metrics.duplicate++;continue;}
      if(c.tick<=this.committed){this.metrics.tooLate++;continue;}
      let list=this.commands.get(c.tick);if(!list){list={};this.commands.set(c.tick,list);}
      // First valid command for a slot/tick is immutable, even across replay.
      if(list[slot]){this.metrics.rejected++;continue;}
      list[slot]={seq:c.seq,tick:c.tick,dx:c.dx,boost:c.boost,magnet:c.magnet,target:c.target??null,rocket:c.rocket||0,rocketTarget:c.rocketTarget??-1,
        cloak:c.cloak||0,shield:c.shield||0,emp:c.emp||0,empX:c.empX??null,empY:c.empY??null};
      seen.set(c.seq,c.tick);this.ack[slot]=Math.max(this.ack[slot],c.seq);this.metrics.accepted++;
      if(c.tick<=this.world.tick)this.dirty=this.dirty===null?c.tick:Math.min(this.dirty,c.tick);
    }
    return true;
  }
  simulate(){
    const t=this.world.tick+1;P.step(this.world,this.commands.get(t)||{},this.systems.get(t)||[]);
    this.events.set(t,copy(this.world.events));this.history.set(t,P.checkpoint(this.world));
  }
  flush(){
    if(this.dirty===null)return;
    const end=this.world.tick,start=this.dirty;this.dirty=null;
    const before=this.history.get(start-1);if(!before)throw Error('Missing rollback checkpoint');
    this.world=P.checkpoint(before);for(let t=start;t<=end;t++)this.simulate();
    this.revision++;this.metrics.rollbacks++;this.metrics.replayedTicks+=end-start+1;
    this.metrics.maxReplay=Math.max(this.metrics.maxReplay,end-start+1);
  }
  advance(){
    if(this.finished)return;this.flush();this.simulate();
    const target=Math.max(this.committed,this.world.tick-WINDOW);
    while(this.committed<target){const tick=++this.committed;
      const events=this.events.get(tick)||[],world=this.history.get(tick);
      // No external side effect occurs while rewinding. Commit each tick exactly once.
      this.onCommit({tick,events:copy(events),world:P.checkpoint(world)});
      if(world.gameOver){this.finished=true;break;}
    }
    const cutoff=this.world.tick-HISTORY;
    for(const map of [this.history,this.events,this.commands,this.systems])for(const t of map.keys())if(t<cutoff)map.delete(t);
    for(const seen of this.seen)for(const [seq,tick] of seen)if(tick<cutoff)seen.delete(seq);
  }
  snapshot(){
    this.flush();const confirmed=this.history.get(this.committed)||this.world;
    const scheduled=[],replayCommands=[],replaySystems=[];
    for(const [tick,slots] of this.commands)if(tick>this.world.tick)for(const [slot,command] of Object.entries(slots))scheduled.push({slot:+slot,command:{...command}});
    for(const [tick,slots] of this.commands)if(tick>this.committed)for(const [slot,command] of Object.entries(slots))replayCommands.push({slot:+slot,command:{...command}});
    for(const [tick,events] of this.systems)if(tick>this.committed)replaySystems.push({tick,events:copy(events)});
    return {version:P.VERSION,matchId:this.matchId,id:++this.snapshotId,revision:this.revision,
      committed:this.committed,epochs:this.epochs.slice(),ack:this.ack.slice(),
      scheduled,replay:{world:P.checkpoint(confirmed),commands:replayCommands,systems:replaySystems},
      confirmed:{lives:confirmed.lives.slice(),scores:confirmed.scores.slice(),eliminated:confirmed.eliminated.slice()},world:P.checkpoint(this.world)};
  }
}

class Client {
  constructor({slot,matchId,epoch,send=()=>{},requestSync=()=>{}}){
    this.slot=slot;this.matchId=matchId;this.epoch=epoch;this.send=send;this.requestSync=requestSync;
    this.world=null;this.previous=null;this.base=null;this.latestId=-1;this.seq=0;this.boost=0;
    this.pending=new Map();this.input={dx:0,magnet:false,target:null};this.boostQueued=false;this.rocket=0;this.rocketTarget=-1;this.rocketQueued=false;
    this.cloak=0;this.cloakQueued=false;this.shield=0;this.emp=0;this.shieldQueued=false;this.empQueued=false;this.empX=null;this.empY=null;
    this.anchorTime=0;this.anchorTick=0;this.rtt=100;this.lastReceived=0;this.lastSent=-Infinity;
    this.clockOffset=null;this.clockSamples=[];this.hasAnchor=false;this.lastCorrection=[];
    this.remoteCommands=new Map();this.systemEvents=new Map();
    this.waitReason=null;this.lastAdvanceAt=null;
    this.lastSyncRequest=-Infinity;this.resumeRequested=false;
    this.paused=false;this.needsSync=false;this.corrections=new Map();this.paddleCorrections=new Map();this.remotePresentation=new Map();this.ballPresentation=new Map();this.recoveryUntil=0;
    this.metrics={snapshots:0,staleSnapshots:0,reconciliations:0,softCorrections:0,hardCorrections:0,
      maxCorrection:0,predictionStops:0,predictionCapFrames:0,predictionCapMs:0,waitingSnapshotMs:0,maxPending:0,ownPaddleCorrections:0,maxOwnPaddleCorrection:0,replayedLocalPastInputs:0};
  }
  setInput({dx=0,magnet=false,boost=false,target=null}){
    this.input={dx:clamp(dx,-1,1),magnet:!!magnet,target:Number.isFinite(target)?clamp(target,0,520):null};if(boost)this.boostQueued=true;
  }
  fireRocket(target){if([0,1,2,3].includes(target)){this.rocketTarget=target;this.rocketQueued=true;}}
  cloakShip(){this.cloakQueued=true;}
  deployShield(){this.shieldQueued=true;}
  fireEMP(x,y){if(Number.isFinite(x)&&Number.isFinite(y)){this.empX=clamp(x,0,520);this.empY=clamp(y,0,520);this.empQueued=true;}}
  setRtt(ms){if(Number.isFinite(ms)&&ms>=0&&ms<3000)this.rtt=clamp(ms,0,400);}
  clockSample({c,s},now){
    if(!Number.isFinite(c)||!Number.isFinite(s)||now<c||now-c>3000)return;
    const rtt=now-c;this.clockSamples.push({rtt,offset:s-(c+now)/2});
    if(this.clockSamples.length>12)this.clockSamples.shift();
    const best=this.clockSamples.reduce((a,b)=>a.rtt<b.rtt?a:b);
    this.clockOffset=best.offset;this.setRtt(this.rtt*.8+rtt*.2);
  }
  suspend(){this.paused=true;this.input={dx:0,magnet:false};this.boostQueued=false;this.rocketQueued=false;this.shieldQueued=false;this.empQueued=false;this.cloakQueued=false;this.needsSync=true;}
  requestSynchronization(now,reset=false){
    // Every entry path shares the retry clock. A between-frame resume anchors
    // its retry on the next frame without sending another immediate request.
    this.lastSyncRequest=Number.isFinite(now)?now:null;
    this.requestSync(reset);
  }
  resume(){this.paused=false;this.pending.clear();this.boostQueued=false;this.rocketQueued=false;this.shieldQueued=false;this.empQueued=false;this.cloakQueued=false;this.needsSync=true;this.resumeRequested=true;this.requestSynchronization(null,true);}
  commandsAt(tick){
    const accepted=this.remoteCommands.get(tick)||{},local=this.pending.get(tick);
    // The server's immutable accepted command wins any same-tick conflict.
    // Otherwise retain our unconfirmed command, including ticks before the latest snapshot.
    return local&&!accepted[this.slot]?{...accepted,[this.slot]:local}:accepted;
  }
  receive(packet,now){
    if(!packet||packet.version!==P.VERSION||packet.matchId!==this.matchId||packet.id<=this.latestId){this.metrics.staleSnapshots++;return false;}
    if(packet.epochs[this.slot]!==this.epoch)return false;
    if(packet.world.tick<this.base?.tick){this.metrics.staleSnapshots++;return false;}
    if(!packet.replay||packet.replay.world.tick!==packet.committed||packet.committed>packet.world.tick||packet.world.tick-packet.committed>WINDOW)return false;
    if(this.waitReason==='prediction-limit')this.recoveryUntil=now+200;
    const displayedBefore=this.world?this.sample(now):null;
    const before=this.world?P.checkpoint(this.world):null,oldTick=before?.tick||packet.world.tick;
    const priorEstimate=this.anchorTick+(now-this.anchorTime)/DT;
    const resync=this.needsSync;
    // Short delivery gaps are not a new session: retain presentation continuity.
    const resetPresentation=this.resumeRequested||this.paused||(resync&&now-this.lastReceived>1000);
    this.latestId=packet.id;this.base=P.checkpoint(packet.world);this.confirmed=copy(packet.confirmed);this.lastReceived=now;
    const age=this.clockOffset!==null&&Number.isFinite(packet.sentAt)?clamp(now+this.clockOffset-packet.sentAt,0,500):this.rtt/2;
    const estimate=this.base.tick+age/DT+(packet.phase||0);
    this.anchorTick=this.hasAnchor&&!resync&&Math.abs(estimate-priorEstimate)<6?priorEstimate+clamp(estimate-priorEstimate,-.12,.12):estimate;
    this.anchorTime=now;this.hasAnchor=true;
    // A provisional latest snapshot may not include our delayed uplink yet.
    // Start at the immutable checkpoint and replay the ENTIRE world with the known
    // accepted timeline plus our unresolved input. Never graft just a paddle position.
    this.world=P.checkpoint(packet.replay.world);this.previous=P.checkpoint(this.world);
    this.remoteCommands.clear();
    this.systemEvents.clear();
    for(const {slot,command} of packet.replay.commands){
      if(!this.remoteCommands.has(command.tick))this.remoteCommands.set(command.tick,{});
      this.remoteCommands.get(command.tick)[slot]=command;
    }
    for(const {tick,events} of packet.replay.systems)this.systemEvents.set(tick,events);
    this.boost=Math.max(this.boost,this.base.lastBoost[this.slot]);
    this.rocket=Math.max(this.rocket,this.base.lastRocket[this.slot]);
    this.shield=Math.max(this.shield,this.base.lastShield[this.slot]);this.emp=Math.max(this.emp,this.base.lastEmp[this.slot]);
    this.cloak=Math.max(this.cloak,this.base.lastCloak[this.slot]);
    this.seq=Math.max(this.seq,this.cloak,this.boost,this.rocket,this.shield,this.emp,packet.ack[this.slot]);this.metrics.snapshots++;
    // A recent server snapshot can precede receipt of our uplink. Keep redundant
    // commands until committed, otherwise one dropped packet can lose a button edge.
    for(const [tick] of this.pending)if(tick<=packet.committed)this.pending.delete(tick);
    const target=Math.min(Math.max(oldTick,this.base.tick),this.base.tick+MAX_PREDICT);
    for(let t=this.world.tick+1;t<=target;t++){
      this.previous=P.checkpoint(this.world);const c=this.pending.get(t);
      if(c&&t<=this.base.tick&&!this.remoteCommands.get(t)?.[this.slot])this.metrics.replayedLocalPastInputs++;
      P.step(this.world,this.commandsAt(t),this.systemEvents.get(t)||[]);
    }
    this.needsSync=false;
    this.resumeRequested=false;
    this.lastCorrection=[];
    const priorCorrections=this.corrections;this.corrections=new Map();
    if(before&&before.tick===this.world.tick){this.metrics.reconciliations++;
      const own=Math.abs(before.paddles[this.slot]-this.world.paddles[this.slot]);
      if(own>=.05)this.metrics.ownPaddleCorrections++;
      this.metrics.maxOwnPaddleCorrection=Math.max(this.metrics.maxOwnPaddleCorrection,own);
      for(const b of this.world.balls){const old=before.balls.find(x=>x.id===b.id);if(!old)continue;
        const dx=old.x-b.x,dy=old.y-b.y,d=Math.hypot(dx,dy);this.metrics.maxCorrection=Math.max(this.metrics.maxCorrection,d);
        this.lastCorrection.push(d);
        const prior=priorCorrections.get(b.id);
        const k=prior&&prior.contact===old.contact?clamp(1-(now-prior.time)/80,0,1):0;
        const visualDx=dx+(k?prior.dx*k:0),visualDy=dy+(k?prior.dy*k:0);
        // Do not smooth across a changed bounce or through a paddle/force field.
        const safe=old.contact===b.contact&&b.x>130&&b.x<390&&b.y>130&&b.y<390&&d<24&&Math.hypot(visualDx,visualDy)<24&&old.vx*b.vx+old.vy*b.vy>=0;
        if(safe&&d<.05&&k){this.corrections.set(b.id,prior);}
        else if(safe&&d>=.05){this.corrections.set(b.id,{dx:visualDx,dy:visualDy,time:now,contact:b.contact});this.metrics.softCorrections++;}
        else if(d>=.05)this.metrics.hardCorrections++;
      }
    }
    // Preserve the currently displayed remote hull across authoritative replay.
    // This is a render-only offset: collision state and input prediction stay exact.
    // Smooth only the correction, not normal movement, so velocity has no permanent lag.
    this.paddleCorrections.clear();
    if(resetPresentation){this.remotePresentation.clear();this.ballPresentation.clear();}
    if(displayedBefore&&!resetPresentation){
      const alpha=clamp(this.desiredTick(now)-this.world.tick,0,1);
      for(const slot of P.SLOTS){
        if(this.world.eliminated[slot]||displayedBefore.eliminated[slot])continue;
        const raw=this.previous.paddles[slot]+(this.world.paddles[slot]-this.previous.paddles[slot])*alpha;
        const offset=displayedBefore.paddles[slot]-raw;
        // Small local corrections also ease out, without delaying fresh input or changing collisions.
        const limit=slot===this.slot?40:80;
        if(Math.abs(offset)>.001&&Math.abs(offset)<limit)this.paddleCorrections.set(slot,{offset,time:now});
      }
    }
    return true;
  }
  // A small fixed input lead keeps the predicted contact horizon short.
  // The bounded server rewind absorbs delayed uplink instead of predicting a full RTT ahead.
  desiredTick(now){return this.anchorTick+(now-this.anchorTime)/DT+2+Math.ceil(Math.max(0,this.rtt-140)/(2*DT));}
  advance(now){
    const elapsed=this.lastAdvanceAt===null?0:clamp(now-this.lastAdvanceAt,0,1000);this.lastAdvanceAt=now;this.waitReason=null;
    if(!this.world||this.paused)return;
    if(this.needsSync){
      this.waitReason='synchronizing';this.metrics.waitingSnapshotMs+=elapsed;
      if(this.lastSyncRequest===null)this.lastSyncRequest=now;
      else if(now-this.lastSyncRequest>=1000)this.requestSynchronization(now,this.resumeRequested);
      return;
    }
    // A long outage suspends prediction instead of inventing seconds of collisions.
    if(now-this.lastReceived>750){this.waitReason='snapshot-gap';this.metrics.waitingSnapshotMs+=elapsed;if(!this.needsSync){this.metrics.predictionStops++;this.needsSync=true;this.requestSynchronization(now);}return;}
    const target=Math.min(Math.floor(this.desiredTick(now)),this.base.tick+MAX_PREDICT);
    if(Math.floor(this.desiredTick(now))>this.base.tick+MAX_PREDICT&&this.world.tick>=target){this.waitReason='prediction-limit';this.metrics.predictionCapFrames++;this.metrics.predictionCapMs+=elapsed;}
    let steps=0;
    while(this.world.tick<target&&steps++<MAX_PREDICT){
      const t=this.world.tick+1;
      let c=this.pending.get(t);
      if(!c){const seq=++this.seq;if(this.boostQueued){this.boost=seq;this.boostQueued=false;}
        if(this.rocketQueued){this.rocket=seq;this.rocketQueued=false;}
        if(this.shieldQueued){this.shield=seq;this.shieldQueued=false;}
        if(this.cloakQueued){this.cloak=seq;this.cloakQueued=false;}
        if(this.empQueued){this.emp=seq;this.empQueued=false;}
        c={tick:t,seq,dx:this.input.dx,magnet:this.input.magnet,boost:this.boost,target:this.input.target??null,rocket:this.rocket,rocketTarget:this.rocketTarget,
          cloak:this.cloak,shield:this.shield,emp:this.emp,empX:this.empX,empY:this.empY};this.pending.set(t,c);}
      this.previous=P.checkpoint(this.world);P.step(this.world,this.commandsAt(t),this.systemEvents.get(t)||[]);
    }
    this.metrics.maxPending=Math.max(this.metrics.maxPending,this.pending.size);
    if(now-this.lastSent>=DT*1.5){
      const commands=[...this.pending.values()].slice(-16);
      if(commands.length)this.send({version:P.VERSION,matchId:this.matchId,epoch:this.epoch,commands});
      this.lastSent=now;
    }
  }
  sample(now){
    if(!this.world)return null;
    const w=P.checkpoint(this.world),prev=this.previous||w;
    const target=this.desiredTick(now),alpha=this.needsSync||this.paused?1:clamp(target-w.tick,0,1);
    // One coherent timeline for paddles, held balls, fields and free balls.
    for(const s of P.SLOTS)w.paddles[s]=prev.paddles[s]+(w.paddles[s]-prev.paddles[s])*alpha;
    for(const [slot,c] of this.paddleCorrections){
      const age=Math.max(0,now-c.time),t=clamp(age/100,0,1);
      if(t===1||w.eliminated[slot]){this.paddleCorrections.delete(slot);continue;}
      const half=w.config.players[slot].paddleStats.w/2;
      w.paddles[slot]=clamp(w.paddles[slot]+c.offset*(1-t)*(1-t),54+half,466-half);
    }
    // Bound render-only catch-up between frames as well as at packet arrival.
    // A delayed snapshot can move the prediction horizon forward several ticks.
    // Never apply that displacement as a single visible frame to a remote hull.
    for(const slot of P.SLOTS){
      if(slot===this.slot)continue;
      const prior=this.remotePresentation.get(slot);
      if(prior&&!w.eliminated[slot]&&now>=prior.time&&now-prior.time<250){
        const travel=w.config.players[slot].paddleStats.spd*1.5*(now-prior.time)/DT;
        w.paddles[slot]=prior.position+clamp(w.paddles[slot]-prior.position,-travel,travel);
      }
      this.remotePresentation.set(slot,{position:w.paddles[slot],time:now});
    }
    for(const b of w.balls){const old=prev.balls.find(x=>x.id===b.id),held=P.SLOTS.find(s=>b['mag_held_'+s]);
      if(held!==undefined){const p=P.slotToPaddle(held,w.paddles[held],w,w.config);b.x=p.x+p.w/2+(b['mag_offX_'+held]||0);b.y=p.y+p.h/2+(b['mag_offY_'+held]||0);}
      else if(old&&old.contact===b.contact){b.x=old.x+(b.x-old.x)*alpha;b.y=old.y+(b.y-old.y)*alpha;}
      const correction=this.corrections.get(b.id);if(correction){const k=clamp(1-(now-correction.time)/80,0,1);
        if(k&&held===undefined&&correction.contact===b.contact&&b.x>130&&b.x<390&&b.y>130&&b.y<390){b.x+=correction.dx*k;b.y+=correction.dy*k;}else this.corrections.delete(b.id);}
    }
    // Rate-limit render-only catch-up after a delayed batch, never physics.
    // Contacts, held balls and goal areas snap to authority instead of smoothing
    // across collisions. Normal-speed movement remains unchanged.
    const liveIds=new Set();
    for(const b of w.balls){
      liveIds.add(b.id);const prior=this.ballPresentation.get(b.id);
      const held=P.SLOTS.some(s=>b['mag_held_'+s]);
      if(now<this.recoveryUntil&&prior&&!held&&prior.contact===b.contact&&now>=prior.time&&now-prior.time<100&&b.x>130&&b.x<390&&b.y>130&&b.y<390&&prior.x>130&&prior.x<390&&prior.y>130&&prior.y<390){
        const dx=b.x-prior.x,dy=b.y-prior.y,d=Math.hypot(dx,dy);
        const travel=Math.max(.5,Math.hypot(b.vx,b.vy))*1.5*(now-prior.time)/DT;
        if(d>travel&&d<80){b.x=prior.x+dx*travel/d;b.y=prior.y+dy*travel/d;}
      }
      this.ballPresentation.set(b.id,{x:b.x,y:b.y,contact:b.contact,time:now});
    }
    for(const id of this.ballPresentation.keys())if(!liveIds.has(id))this.ballPresentation.delete(id);
    for(const m of w.missiles||[]){const old=prev.missiles?.find(x=>x.id===m.id);
      if(old&&m.age>0&&old.age>0){m.x=old.x+(m.x-old.x)*alpha;m.y=old.y+(m.y-old.y)*alpha;
        m.angle=old.angle+Math.atan2(Math.sin(m.angle-old.angle),Math.cos(m.angle-old.angle))*alpha;}
    }
    return w;
  }
}
return {Authority,Client,WINDOW,FUTURE,MAX_PREDICT,HISTORY};
});
