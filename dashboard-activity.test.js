'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {createHash}=require('node:crypto');
const {createActivity,iso,playtime}=require('./dashboard-activity');
function fixture(){
 let time=Date.parse('2026-10-24T12:00:00Z'),reads=0;
 const rows=new Map(),counts=new Map();let hold=null,failWrite=false;
 const snapshot=(id,data)=>({id,exists:!!data,data:()=>data});
 const db={collection(name){
  const query={doc(id){return {async get(){return snapshot(id,rows.get(name+'/'+id));},async set(value){if(failWrite)throw Error('offline');rows.set(name+'/'+id,value);}};},orderBy(field,dir){query.field=field;query.dir=dir;return query;},limit(n){query.n=n;return query;},select(){return query;},async get(){
   reads++;counts.set(name,(counts.get(name)||0)+1);
   let values=[...rows].filter(([key])=>key.startsWith(name+'/')).map(([key,value])=>snapshot(key.slice(name.length+1),value));
   if(query.field)values=values.filter(d=>d.data()[query.field]!=null).sort((a,b)=>String(iso(a.data()[query.field])||a.data()[query.field]).localeCompare(String(iso(b.data()[query.field])||b.data()[query.field]))*(query.dir==='desc'?-1:1));
   const result={docs:values.slice(0,query.n),size:Math.min(values.length,query.n||Infinity)};
   if(name==='dashboard_matches'&&hold)await hold;
   return result;
  }};return query;
 }};
 const activity=createActivity({db,now:()=>time});
 return {db,rows,counts,activity,advance:n=>time+=n,setHold:p=>hold=p,fail:()=>failWrite=true,now:()=>time};
}
const range=['2026-10-01','2026-10-24'];
test('freshly persisted matches appear immediately without rescanning history; roster includes departed people',async()=>{
 const f=fixture();assert.equal((await f.activity.get(...range)).activity.count,0);
 const room={id:'m_one',startedAt:f.now()-120000,roster:{0:{uid:'a'},1:{uid:'b'}},players:{s:{uid:'b'}},_disconnected:{0:{uid:'a'}},bots:{2:{},3:{}},mode:'ranked'};
 await f.activity.record(room,false);
 const r=await f.activity.get(...range);assert.equal(r.activity.count,1);assert.equal(r.activity.rows[0].players,2);assert.equal(r.activity.rows[0].duration,120);assert.equal(r.activity.rows[0].bots,2);assert.equal(f.counts.get('dashboard_matches'),1);
 f.advance(300001);assert.equal((await f.activity.get(...range)).activity.count,1);assert.equal(f.counts.get('dashboard_matches'),2);
});
test('completed write racing an older database snapshot is preserved and deduplicated after refresh',async()=>{
 const f=fixture();let release;f.setHold(new Promise(resolve=>release=resolve));
 const read=f.activity.get(...range);await new Promise(resolve=>setImmediate(resolve));
 await f.activity.record({id:'race',startedAt:f.now()-50000,players:{s:{uid:'a'}}},true);release();
 assert.equal((await read).activity.count,1);f.setHold(null);f.advance(300001);assert.equal((await f.activity.get(...range)).activity.count,1);
});
test('recent registrations are bounded, newest first, timestamps normalized and missing time distinct from zero',async()=>{
 const f=fixture();for(let i=1;i<=40;i++)f.rows.set('users_public/u'+i,{nickname:'U'+i,createdAt:new Date(Date.UTC(2026,9,24,0,i))});
 f.rows.set('users_public/legacy',{nickname:'Unknown date'});
 const hash=createHash('sha256').update('u40').digest('hex');f.rows.set('dashboard_presence/'+hash,{measuredSince:'2026-10-24T00:00:00Z',foregroundSeconds:0,lastSeen:'2026-10-24T11:59:00Z'});
 const r=await f.activity.get(...range);assert.equal(r.recentPlayers.length,30);assert.equal(r.recentPlayers[0].uid,'u40');assert.equal(r.recentPlayers[0].foregroundSeconds,0);assert.equal(r.recentPlayers[1].foregroundSeconds,null);assert.match(r.recentPlayers[0].createdAt,/Z$/);
 await f.activity.get(...range);assert.equal(f.counts.get('users_public'),1);f.advance(60001);await f.activity.get(...range);assert.equal(f.counts.get('users_public'),2);
});
test('failed match writes are not counted as persisted and date filters stay accurate',async()=>{
 const f=fixture();await f.activity.record({id:'old',startedAt:f.now()-1000},false);assert.equal((await f.activity.get('2026-10-01','2026-10-23')).activity.count,0);
 f.fail();await assert.rejects(f.activity.record({id:'failed',startedAt:f.now()},false));assert.equal((await f.activity.get(...range)).activity.count,1);
});
test('playtime and timestamps reject invalid/missing data rather than fabricate activity',()=>{
 assert.equal(iso({toDate:()=>new Date('2026-10-24')}),'2026-10-24T00:00:00.000Z');assert.equal(iso(null),null);assert.equal(iso('invalid'),null);
 assert.equal(playtime({foregroundSeconds:123}).foregroundSeconds,null);assert.equal(playtime({measuredSince:'2026-10-24',foregroundSeconds:125.9}).foregroundSeconds,125);
});
test('fast activity route has the same owner-only protection and never needs Google reports',async()=>{
 const {createDashboard}=require('./football-dashboard');const f=fixture();let claims={email:'xdricsx@gmail.com',email_verified:true,firebase:{sign_in_provider:'google.com'}};
 const d=createDashboard({db:f.db,credentials:{},verifyIdToken:async()=>claims,live:()=>({online:3,rooms:1})});
 async function call(auth=true){let status,body;await d.handle({method:'GET',url:'/dashboard/api/activity?from=2026-10-01&to=2026-10-24',headers:auth?{authorization:'Bearer x'}:{},socket:{remoteAddress:'local'}},{writeHead:s=>status=s,end:b=>body=JSON.parse(b)});return {status,body};}
 assert.equal((await call(false)).status,401);const r=await call();assert.equal(r.status,200);assert.equal(r.body.live.online,3);assert.equal(r.body.activity.count,0);claims={...claims,email:'someone@example.com'};assert.equal((await call()).status,403);
});

test('recent registrations use trusted Firebase Auth dates, not device-clock profile dates',async()=>{
 const f=fixture();f.rows.set('users_public/a',{nickname:'A',createdAt:'2099-01-01'});f.rows.set('users_public/b',{nickname:'B',createdAt:'2020-01-01'});
 const a=createActivity({db:f.db,now:f.now,listAccounts:async()=>({users:[{uid:'a',metadata:{creationTime:'2026-10-01'}},{uid:'b',metadata:{creationTime:'2026-10-24'}},{uid:'other-app-only',metadata:{creationTime:'2026-10-24'}}]})});
 const r=await a.get(...range);assert.deepEqual(r.recentPlayers.map(p=>p.uid),['b','a']);assert.equal(r.recentPlayers[1].createdAt,'2026-10-01T00:00:00.000Z');
});

test('match day filters and daily totals follow Kyiv local midnight across the fall DST transition',async()=>{
 const f=fixture();const times=['2026-10-24T20:59:59Z','2026-10-24T21:00:00Z','2026-10-25T00:30:00Z','2026-10-25T01:30:00Z','2026-10-25T21:59:59Z','2026-10-25T22:00:00Z'];
 for(let i=0;i<times.length;i++){f.advance(Date.parse(times[i])-f.now());await f.activity.record({id:'kyiv'+i,startedAt:f.now()-1000},false);}
 const r=await f.activity.get('2026-10-25','2026-10-25');assert.equal(r.activity.count,4);assert.deepEqual(r.activity.daily,[{date:'2026-10-25',matches:4}]);
 assert.equal((await f.activity.get('2026-10-24','2026-10-24')).activity.count,1);assert.equal((await f.activity.get('2026-10-26','2026-10-26')).activity.count,1);
});
