'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),{createHash}=require('node:crypto');
const {createPlayerActivity}=require('./player-activity');
function fixture(options={}){
 const docs=new Map();let time=0,transactions=0,failBefore=false,failAfter=false,gate=null;
 const db={collection:name=>({doc:id=>({key:name+'/'+id}),where:(field,op,value)=>({limit:count=>({get:async()=>({docs:[...docs].filter(([key,data])=>key.startsWith(name+'/')&&data[field]<value).slice(0,count).map(([key])=>({ref:{key}}))})})})}),async runTransaction(fn){
   transactions++;if(gate)await gate;if(failBefore){failBefore=false;throw Error('offline');}
   const writes=[];await fn({get:async ref=>({data:()=>docs.get(ref.key)}),set:(ref,value,opts)=>writes.push([ref.key,value,opts]),delete:ref=>writes.push([ref.key,null])});
   for(const [key,value,opts] of writes){if(value===null)docs.delete(key);else docs.set(key,opts?.merge?{...docs.get(key),...value}:value);}
   if(failAfter){failAfter=false;throw Error('response lost after commit');}
 }};
 const tracker=createPlayerActivity({db,now:()=>time,wallNow:()=>Date.UTC(2026,9,24)+time,
   setIntervalFn:()=>({unref(){}}),clearIntervalFn:()=>{},...options});
 const socket=(uid='alice',anonymous=false)=>{const s={uid,connected:true};tracker.authenticate(s,uid,{anonymous});return s;};
 const send=(s,t,foreground=true,seq)=>{time=t;s.seq=seq??(s.seq||0)+1;if(foreground&&s.wasForeground!==true)s.session=(s.session||0)+1;s.wasForeground=foreground;return tracker.receive(s,{v:1,seq:s.seq,session:s.session||1,foreground});};
 const presence=(uid='alice')=>docs.get('dashboard_presence/'+createHash('sha256').update(uid).digest('hex'));
 return {tracker,docs,socket,send,presence,setTime:t=>{time=t;},transactions:()=>transactions,
   failBefore:()=>{failBefore=true;},failAfter:()=>{failAfter=true;},gate:promise=>{gate=promise;}};
}

test('counts foreground menu/match time and stops while hidden',async()=>{
 const f=fixture(),s=f.socket();f.send(s,0);f.send(s,30000);f.send(s,45000,false);
 f.send(s,300000,false);f.send(s,600000);f.send(s,630000);await f.tracker.close();
 assert.equal(f.presence().foregroundSeconds,75);assert.equal(f.presence().activityVersion,1);
 assert.equal(f.presence().measuredSince,'2026-10-24T00:00:00.000Z');
});
test('union of overlapping sockets counts an account once',async()=>{
 const f=fixture(),a=f.socket(),b=f.socket();f.send(a,0);f.send(b,10000);f.send(a,30000);f.send(b,40000);
 f.send(a,60000);f.send(b,70000);f.send(a,75000,false);f.send(b,80000,false);await f.tracker.close();
 assert.equal(f.presence().foregroundSeconds,80);
});
test('no guessed stale gap or disconnect tail, including reconnect',async()=>{
 const f=fixture(),s=f.socket();f.send(s,0);f.send(s,30000);f.send(s,90000);f.send(s,100000,false);
 f.setTime(120000);f.tracker.disconnect(s);const next=f.socket();f.send(next,300000);f.send(next,330000);
 f.setTime(360000);f.tracker.disconnect(next);await f.tracker.close();assert.equal(f.presence().foregroundSeconds,70);
});
test('stale sweep ends foreground and receipt after expiry starts a new interval',async()=>{
 const f=fixture(),s=f.socket();f.send(s,0);f.send(s,30000);f.setTime(80000);f.tracker.sweep();
 assert.equal(f.tracker.snapshot().activeUsers,0);f.send(s,90000);f.send(s,120000);await f.tracker.close();assert.equal(f.presence().foregroundSeconds,60);
});
test('unauthenticated, anonymous, mismatched UID and invalid/replayed input cannot add time',async()=>{
 const f=fixture(),unauth={uid:'alice',connected:true},guest=f.socket('guest',true);
 assert.equal(f.send(unauth,0),false);assert.equal(f.send(guest,0),false);
 const s=f.socket();f.send(s,0,true,1);f.send(s,30000,true,2);
 assert.equal(f.send(s,40000,true,2),false);assert.equal(f.send(s,40000,true,-1),false);
 assert.equal(f.tracker.receive(s,{v:1,seq:3,foreground:'true'}),false);
 s.uid='victim';assert.equal(f.send(s,50000,true,3),false);await f.tracker.close();
 assert.equal(f.presence().foregroundSeconds,30);assert.equal(f.presence('guest'),undefined);assert.equal(f.presence('victim'),undefined);
});
test('UID reassignment cannot credit the previous unconfirmed tail',async()=>{
 const f=fixture(),s=f.socket();f.send(s,0);f.send(s,30000);f.setTime(40000);
 s.uid='bob';f.tracker.authenticate(s,'bob');f.send(s,40000);f.send(s,70000);await f.tracker.close();
 assert.equal(f.presence().foregroundSeconds,30);assert.equal(f.presence('bob').foregroundSeconds,30);
});
test('cumulative checkpoint prevents double credit after ambiguous transaction commit',async()=>{
 const f=fixture(),s=f.socket();f.send(s,0);f.send(s,30000);f.failAfter();await f.tracker.flush();
 assert.equal(f.presence().foregroundSeconds,30);assert.equal(f.tracker.snapshot().failures,1);
 f.send(s,60000);f.send(s,90000);await f.tracker.flush();assert.equal(f.presence().foregroundSeconds,90);
 await f.tracker.close();assert.equal(f.presence().foregroundSeconds,90);
});
test('failed write retains delta and close waits for an in-flight checkpoint',async()=>{
 const f=fixture(),s=f.socket();f.send(s,0);f.send(s,30000);f.failBefore();await f.tracker.flush();assert.equal(f.presence(),undefined);
 f.send(s,60000);f.send(s,90000);let unblock;f.gate(new Promise(resolve=>{unblock=resolve;}));
 const write=f.tracker.flush();f.send(s,120000);const closed=f.tracker.close();unblock();await write;await closed;
 assert.equal(f.presence().foregroundSeconds,120);
});
test('regular writes are bounded and multiple heartbeats require no per-socket timers',async()=>{
 const f=fixture(),s=f.socket();f.send(s,0);await f.tracker.flush();
 for(let t=10000;t<=120000;t+=10000){f.send(s,t);await f.tracker.flush();}
 assert.equal(f.transactions(),3);await f.tracker.close();
});
test('memory is bounded and inactive persisted accounts are cleaned up',async()=>{
 const f=fixture({maxUsers:1,idleMs:60000}),a=f.socket(),b=f.socket('bob');f.send(a,0);f.send(a,30000,false);await f.tracker.flush('alice',true);
 assert.equal(f.send(b,40000),false);f.setTime(120000);f.tracker.sweep();assert.equal(f.tracker.snapshot().trackedUsers,0);
 assert.equal(f.send(b,120000),true);f.send(b,150000);await f.tracker.close();assert.equal(f.presence('bob').foregroundSeconds,30);
});

test('checkpoint cleanup is bounded, hourly and excludes live windows and presence',async()=>{
 const f=fixture(),base=Date.UTC(2026,9,24);
 for(let i=0;i<105;i++)f.docs.set('dashboard_activity_checkpoints/old'+i,{appliedMs:1,expiresAt:new Date(base-1)});
 for(let i=0;i<5;i++)f.docs.set('dashboard_activity_checkpoints/live'+i,{appliedMs:1,expiresAt:new Date(base+86400000)});
 f.docs.set('dashboard_presence/keep',{foregroundSeconds:55});await f.tracker.cleanup();
 assert.equal([...f.docs.keys()].filter(k=>k.startsWith('dashboard_activity_checkpoints/')).length,10);
 await f.tracker.cleanup();assert.equal(f.transactions(),1);f.setTime(3600001);await f.tracker.cleanup();
 assert.equal([...f.docs.keys()].filter(k=>k.startsWith('dashboard_activity_checkpoints/')).length,5);assert.equal(f.docs.get('dashboard_presence/keep').foregroundSeconds,55);
 await f.tracker.close();
});
test('unpersisted state expires before its durable checkpoint can be deleted',async()=>{
 const f=fixture(),s=f.socket();f.send(s,0);f.send(s,30000);f.failBefore();await f.tracker.flush();
 f.setTime(7200001);f.tracker.sweep();assert.equal(f.tracker.snapshot().trackedUsers,0);assert.equal(f.tracker.snapshot().discardedMs,30000);
 f.send(s,7201000);f.send(s,7231000);await f.tracker.close();assert.equal(f.presence().foregroundSeconds,30);
});
test('production registration order preserves disconnect flush after match-service replaces handlers',async()=>{
 const {EventEmitter}=require('node:events'),{createMatchService}=require('./match-service'),fs=require('node:fs');
 const source=fs.readFileSync(require.resolve('./server'),'utf8');
 assert.match(source,/matchService\.attach\(socket\);playerActivity\?\.attach\(socket\)/);
 const f=fixture(),s=Object.assign(new EventEmitter(),{uid:'alice',connected:true});
 const service=createMatchService({io:{},rooms:new Map(),getDb:()=>null});
 s.on('disconnect',()=>{throw Error('old handlers should be replaced');});service.attach(s);f.tracker.attach(s);f.tracker.authenticate(s,'alice');
 s.emit('activity:foreground',{v:1,seq:1,session:1,foreground:true});f.setTime(30000);s.emit('activity:foreground',{v:1,seq:2,session:1,foreground:true});
 f.setTime(50000);s.connected=false;s.emit('disconnect');await f.tracker.flush('alice',true);
 assert.equal(f.tracker.snapshot().activeUsers,0);assert.equal(f.presence().foregroundSeconds,30);service.close();await f.tracker.close();
});

test('simultaneous stop/disconnect flushes keep database concurrency bounded',async()=>{
 const f=fixture(),sockets=Array.from({length:20},(_,i)=>f.socket('player'+i));for(const s of sockets)f.send(s,0);
 let unblock;f.gate(new Promise(resolve=>{unblock=resolve;}));for(const s of sockets)f.send(s,30000,false);
 await Promise.resolve();assert.equal(f.transactions(),8);assert.equal(f.tracker.snapshot().activeWrites,8);
 unblock();await f.tracker.close();for(let i=0;i<20;i++)assert.equal(f.presence('player'+i).foregroundSeconds,30);
});

test('dropped background stop cannot turn the hidden interval into foreground time',async()=>{
 const f=fixture(),s=f.socket();f.send(s,0);f.send(s,30000);
 // The client resumes with a new foreground session even if its stop was lost.
 f.setTime(40000);f.tracker.receive(s,{v:1,seq:4,session:2,foreground:true});
 f.setTime(70000);f.tracker.receive(s,{v:1,seq:5,session:2,foreground:true});await f.tracker.close();
 assert.equal(f.presence().foregroundSeconds,60);
});

test('shop auth ACK is bound to the latest verified request and a live socket',async()=>{
 const fs=require('node:fs'),vm=require('node:vm');const source=fs.readFileSync(require.resolve('./server'),'utf8');
 const block=source.slice(source.indexOf('  let shopAuthGeneration=0;'),source.indexOf("  socket.on('tutorial:progress'"));
 const handlers=new Map(),pending=[],acks=[],authenticated=[],seen=[];
 const socket={connected:true,on:(name,fn)=>handlers.set(name,fn),emit(){}};
 vm.runInNewContext(block,{socket,welcomeEnabled:false,welcomeGateway:{attach:()=>({authenticated(){}})},resolveUid:args=>new Promise(resolve=>pending.push({args,resolve})),
   footballDashboard:{presence:uid=>seen.push(uid)},playerActivity:{authenticate:(...args)=>{authenticated.push(args);return !args[2]?.anonymous;}}});
 const first=handlers.get('shop:auth')({idToken:'old'},result=>acks.push(result));
 const latest=handlers.get('shop:auth')({idToken:'new'},result=>acks.push(result));
 pending[1].args.onVerified({uid:'new',anonymous:false});pending[1].resolve('new');await latest;
 pending[0].args.onVerified({uid:'old',anonymous:false});pending[0].resolve('old');await first;
 assert.equal(socket.uid,'new');assert.equal(acks.length,1);assert.equal(acks[0].activityVersion,1);assert.deepEqual(seen,['new']);assert.equal(authenticated.length,1);
 const disconnected=handlers.get('shop:auth')({idToken:'later'},result=>acks.push(result));socket.connected=false;pending[2].args.onVerified({uid:'later',anonymous:false});pending[2].resolve('later');await disconnected;
 assert.equal(socket.uid,'new');assert.equal(acks.length,1);
});
