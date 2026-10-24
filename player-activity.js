'use strict';
const {createHash, randomUUID}=require('node:crypto');
const {performance}=require('node:perf_hooks');

// Confirm intervals with two messages, never with an open socket. An offline or
// crashed session deliberately loses its last unconfirmed tail.
function createPlayerActivity({db,now=()=>performance.now(),wallNow=Date.now,
  heartbeatGapMs=45000,flushEveryMs=60000,idleMs=300000,maxUsers=10000,
  onChanged=()=>{},setIntervalFn=setInterval,clearIntervalFn=clearInterval}={}){
  const users=new Map(),auth=new WeakMap(),writeQueue=[];
  let activeWrites=0;
  let closed=false,flushing=null,cleaning=null,lastCleanup=-Infinity,failures=0,discardedMs=0;
  const iso=()=>new Date(wallNow()).toISOString();
  const hash=uid=>createHash('sha256').update(uid).digest('hex');
  const finite=value=>Number.isFinite(value)&&value>=0?value:0;
  function state(uid){
    let u=users.get(uid);
    if(!u){
      if(users.size>=maxUsers)return null;
      u={uid,id:hash(uid),window:randomUUID(),sessions:new Set(),intervals:[],totalMs:0,
        persistedMs:-1,measuredSince:iso(),lastForegroundAt:null,lastTouched:now(),lastSuccess:now(),lastFlush:-Infinity,inFlight:null};
      users.set(uid,u);
    }
    return u;
  }
  function interval(u,start,end){
    if(end<=start)return;
    let added=end-start;
    for(const [a,b] of u.intervals)added-=Math.max(0,Math.min(end,b)-Math.max(start,a));
    u.totalMs+=Math.max(0,added);
    const merged=[];
    for(const [a,b] of [...u.intervals,[start,end]].sort((a,b)=>a[0]-b[0])){
      const previous=merged.at(-1);
      if(previous&&a<=previous[1])previous[1]=Math.max(previous[1],b);else merged.push([a,b]);
    }
    // No future accepted interval can start earlier than this watermark.
    u.intervals=merged.filter(([,b])=>b>end-heartbeatGapMs);
  }
  function stop(record){
    if(!record)return;
    const u=users.get(record.uid);
    if(u){u.sessions.delete(record);u.lastTouched=now();}
    record.active=false;
  }
  function authenticate(socket,uid,{anonymous=false}={}){
    if(closed)return false;
    const previous=auth.get(socket);
    if(previous?.uid===uid&&!anonymous)return true;
    if(previous){stop(previous);void flush(previous.uid,true);}
    auth.delete(socket);
    if(closed||anonymous||!socket?.connected||typeof uid!=='string'||!uid||uid.length>128)return false;
    auth.set(socket,{uid,seq:-1,session:null,last:0,active:false});
    return true;
  }
  function receive(socket,payload){
    const r=auth.get(socket);
    if(closed||!socket?.connected||!r||socket.uid!==r.uid||payload?.v!==1||
      !Number.isSafeInteger(payload.seq)||payload.seq<0||payload.seq<=r.seq||
      !Number.isSafeInteger(payload.session)||payload.session<1||typeof payload.foreground!=='boolean')return false;
    const t=now();
    // Bound work even for a modified client; allow a quick final stop.
    if(r.active&&r.session===payload.session&&payload.foreground&&t-r.last<1000)return false;
    r.seq=payload.seq;
    const u=users.get(r.uid)||(payload.foreground?state(r.uid):null);
    if(!u)return false;
    const wasActive=r.active,gap=t-r.last;
    if(wasActive&&r.session===payload.session&&gap>=0&&gap<=heartbeatGapMs)interval(u,r.last,t);
    r.active=payload.foreground;r.session=payload.session;r.last=t;u.lastTouched=t;
    if(r.active)u.sessions.add(r);else u.sessions.delete(r);
    if(r.active||wasActive)u.lastForegroundAt=iso();
    if(wasActive&&!r.active)void flush(r.uid,true);
    return true;
  }
  function disconnect(socket){
    const r=auth.get(socket);auth.delete(socket);
    if(!r)return;
    stop(r);void flush(r.uid,true);
  }
  function attach(socket){
    socket.on('activity:foreground',value=>receive(socket,value));
    socket.on('disconnect',()=>disconnect(socket));
  }
  function enqueueWrite(job){
    return new Promise(resolve=>{
      writeQueue.push({job,resolve});drainWrites();
    });
  }
  function drainWrites(){
    while(activeWrites<8&&writeQueue.length){
      const next=writeQueue.shift();activeWrites++;
      Promise.resolve().then(next.job).then(next.resolve,next.resolve).finally(()=>{activeWrites--;drainWrites();});
    }
  }
  async function persist(u){
    if(u.inFlight)return u.inFlight;
    if(u.persistedMs===u.totalMs||!u.lastForegroundAt)return;
    const target={totalMs:u.totalMs,lastForegroundAt:u.lastForegroundAt};u.lastFlush=now();
    u.inFlight=enqueueWrite(async()=>{
      try{
        // Cumulative private checkpoints make ambiguous commits safe to retry.
        const presence=db.collection('dashboard_presence').doc(u.id);
        const checkpoint=db.collection('dashboard_activity_checkpoints').doc(u.id+'_'+u.window);
        await db.runTransaction(async tx=>{
          const [p,c]=await Promise.all([tx.get(presence),tx.get(checkpoint)]);
          const saved=p.data()||{},applied=finite(c.data()?.appliedMs);
          const delta=Math.max(0,target.totalMs-applied);
          const last=[saved.lastSeen,target.lastForegroundAt].filter(Boolean).sort().at(-1);
          tx.set(presence,{foregroundSeconds:finite(saved.foregroundSeconds)+delta/1000,
            measuredSince:saved.measuredSince||u.measuredSince,
            lastForegroundAt:[saved.lastForegroundAt,target.lastForegroundAt].filter(Boolean).sort().at(-1),
            lastSeen:last,activityVersion:1},{merge:true});
          tx.set(checkpoint,{appliedMs:Math.max(applied,target.totalMs),expiresAt:new Date(wallNow()+7*86400000)});
        });
        u.persistedMs=Math.max(u.persistedMs,target.totalMs);u.lastSuccess=now();
        try{onChanged(u.uid);}catch{}
      }catch{failures++;}finally{u.inFlight=null;}
    });
    return u.inFlight;
  }
  async function flush(uid,final=false){
    if(uid){
      const u=users.get(uid);
      if(u?.inFlight){await u.inFlight;return;}
      // Rapid background/foreground toggles cannot produce a write storm.
      if(u&&(final?now()-u.lastFlush>=5000:now()-u.lastFlush>=flushEveryMs))await persist(u);
      return;
    }
    if(flushing)return flushing;
    flushing=(async()=>{
      const pending=[...users.values()].filter(u=>now()-u.lastFlush>=flushEveryMs);
      for(let i=0;i<pending.length;i+=8)await Promise.all(pending.slice(i,i+8).map(persist));
    })();
    try{await flushing;}finally{flushing=null;}
  }
  async function cleanup(){
    if(cleaning)return cleaning;
    if(now()-lastCleanup<3600000)return;
    lastCleanup=now();
    cleaning=(async()=>{
      try{
        // One bounded query per hour; no console TTL setup is required.
        const expired=await db.collection('dashboard_activity_checkpoints').where('expiresAt','<',new Date(wallNow())).limit(100).get();
        if(!expired.docs.length)return;
        await db.runTransaction(async tx=>{
          const rows=await Promise.all(expired.docs.map(async doc=>({ref:doc.ref,snap:await tx.get(doc.ref)})));
          for(const {ref,snap} of rows){
            const expiry=snap.data()?.expiresAt;
            const ms=typeof expiry?.toMillis==='function'?expiry.toMillis():new Date(expiry).getTime();
            // Recheck inside the transaction: an active window may be renewed
            // between the expiry query and this delete.
            if(Number.isFinite(ms)&&ms<wallNow())tx.delete(ref);
          }
        });
      }catch{failures++;}
    })();
    try{await cleaning;}finally{cleaning=null;}
  }
  function sweep(){
    const t=now();
    for(const [uid,u] of users){
      for(const r of u.sessions)if(t-r.last>heartbeatGapMs){r.active=false;u.sessions.delete(r);}
      // Abandon unconfirmed persistence after a two-hour outage, including
      // active accounts. This is conservative and prevents a very old retry
      // from crediting an expired checkpoint for a second time.
      if(!u.inFlight&&u.persistedMs!==u.totalMs&&t-u.lastSuccess>7200000){
        discardedMs+=Math.max(0,u.totalMs-Math.max(0,u.persistedMs));
        for(const r of u.sessions)r.active=false;
        users.delete(uid);continue;
      }
      if(!u.sessions.size&&!u.inFlight&&t-u.lastTouched>idleMs&&u.persistedMs===u.totalMs)users.delete(uid);
    }
    if(!closed){void flush();void cleanup();}
  }
  const timer=setIntervalFn(sweep,10000);timer?.unref?.();
  async function close(){
    if(closed)return;closed=true;clearIntervalFn(timer);
    for(const u of users.values()){for(const r of u.sessions)r.active=false;u.sessions.clear();}
    if(flushing)await flushing;
    if(cleaning)await cleaning;
    await Promise.all([...users.values()].map(u=>u.inFlight).filter(Boolean));
    const pending=[...users.values()];
    for(let i=0;i<pending.length;i+=8)await Promise.all(pending.slice(i,i+8).map(persist));
  }
  function snapshot(){return {trackedUsers:users.size,activeUsers:[...users.values()].filter(u=>u.sessions.size).length,failures,discardedMs,activeWrites,queuedWrites:writeQueue.length};}
  return {attach,authenticate,receive,disconnect,flush,sweep,cleanup,close,snapshot};
}
module.exports={createPlayerActivity};
