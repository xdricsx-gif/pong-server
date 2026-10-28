'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {createHash}=require('node:crypto');
const {createPlayerAnalytics,readFilters,retentionState}=require('./dashboard-player-analytics');
function fixture(options={}){
  let at=Date.parse('2026-10-28T12:00:00Z'),authReads=0;const data=new Map(),reads=new Map(),accounts=[];
  const snap=(id,value)=>({id,exists:value!==undefined,data:()=>value});
  const db={collection(name){function query(after=null,limit=1000){return {select(){return this;},orderBy(){return this;},limit(n){limit=n;return this;},startAfter(v){after=typeof v==='string'?v:v.id;return this;},doc(id){return {async get(){reads.set(name,(reads.get(name)||0)+1);return snap(id,data.get(name+'/'+id));}};},async get(){reads.set(name,(reads.get(name)||0)+1);const rows=[...data].filter(([k])=>k.startsWith(name+'/')).map(([k,v])=>snap(k.slice(name.length+1),v)).sort((a,b)=>a.id.localeCompare(b.id)).filter(d=>!after||d.id>after).slice(0,limit);return {docs:rows,size:rows.length};}};}return query();}};
  const listAccounts=async(limit,token)=>{authReads++;const offset=Number(token||0),users=accounts.slice(offset,offset+limit);return {users,pageToken:offset+limit<accounts.length?String(offset+limit):undefined};};
  const api=createPlayerAnalytics({db,listAccounts,now:()=>at,...options});
  function player(uid,createdAt,activity={},profile={}){data.set('users_public/'+uid,{nickname:uid,rating:500,gamesPlayed:0,...profile});accounts.push({uid,metadata:{creationTime:createdAt}});if(Object.keys(activity).length)data.set('dashboard_presence/'+createHash('sha256').update(uid).digest('hex'),activity);}
  data.set('dashboard_meta/retention',{startedAt:'2026-10-01T12:00:00Z',version:1});
  return {api,db,listAccounts,data,reads,player,authReads:()=>authReads,advance:ms=>at+=ms};
}
const list=(f,params={})=>f.api.list(new URLSearchParams(params));
test('game time filters distinguish missing observation from measured zero and sort null last',async()=>{
  const f=fixture();f.player('zero','2026-10-20',{foregroundSeconds:0,measuredSince:'2026-10-21'});f.player('missing','2026-10-22');f.player('ten','2026-10-23',{foregroundSeconds:600,measuredSince:'2026-10-23'});
  assert.deepEqual((await list(f,{sort:'foregroundSeconds',direction:'asc'})).rows.map(p=>p.uid),['zero','ten','missing']);
  assert.deepEqual((await list(f,{sort:'foregroundSeconds',direction:'desc'})).rows.map(p=>p.uid),['ten','zero','missing']);
  assert.deepEqual((await list(f,{maxMinutes:0})).rows.map(p=>p.uid),['zero']);
  assert.deepEqual((await list(f,{minMinutes:10,maxMinutes:10})).rows.map(p=>p.uid),['ten']);
  assert.deepEqual((await list(f,{timeStatus:'unmeasured'})).rows.map(p=>p.uid),['missing']);
});
test('date filters use trusted Firebase creation timestamps and Kyiv calendar across DST',async()=>{
  const f=fixture();f.player('one','2026-10-24T21:00:00Z',{lastSeen:'2026-10-25T21:59:59Z'},{createdAt:'2099-01-01'});f.player('two','2026-10-25T22:00:00Z',{lastSeen:'2026-10-25T22:00:00Z'});
  const r=await list(f,{registeredFrom:'2026-10-25',registeredTo:'2026-10-25',lastSeenFrom:'2026-10-25',lastSeenTo:'2026-10-25'});
  assert.equal(r.total,1);assert.equal(r.rows[0].uid,'one');assert.equal(r.rows[0].createdAt,'2026-10-24T21:00:00.000Z');assert.equal(r.timezone,'Europe/Kyiv');
});
test('search, deterministic pagination and sorting share one bounded scan across filters',async()=>{
  const f=fixture();for(let i=0;i<35;i++)f.player('p'+String(i).padStart(2,'0'),'2026-10-20',{},{nickname:'Pilot '+i,gamesPlayed:i});
  const a=await list(f,{sort:'gamesPlayed',direction:'desc',pageSize:10,page:2});assert.equal(a.total,35);assert.equal(a.pages,4);assert.equal(a.rows[0].uid,'p24');
  assert.equal((await list(f,{q:'PILOT 12'})).rows[0].uid,'p12');assert.equal((await list(f,{q:'p12'})).total,1);
  assert.equal(f.reads.get('users_public'),1);assert.equal(f.reads.get('dashboard_presence'),1);assert.equal(f.authReads(),1);
  f.advance(60001);await list(f);assert.equal(f.reads.get('users_public'),2);assert.equal(f.authReads(),1);
  f.advance(300000);await list(f);assert.equal(f.authReads(),2);
});
test('D1 and D7 count exact return days, not later logins; pending and unknown never lower rate',async()=>{
  const f=fixture();f.player('returned','2026-10-20T10:00:00Z',{activeDays:{'2026-10-21':true,'2026-10-27':true}});
  f.player('not-returned','2026-10-20T10:00:00Z',{lastSeen:'2026-10-27T12:00:00Z',activeDays:{'2026-10-22':true}});
  f.player('pending','2026-10-27T10:00:00Z',{activeDays:{'2026-10-28':true}});
  f.player('legacy','2026-09-20T10:00:00Z',{lastSeen:'2026-10-27T12:00:00Z'});
  const r=await f.api.retention('2026-09-01','2026-10-28');
  assert.deepEqual(r.d1,{eligible:2,returned:1,percent:50,pending:1,unknown:1});assert.deepEqual(r.d7,r.d1);
  assert.equal(r.cohortPlayers,4);assert.equal(r.coverageDay,'2026-10-02');
  const drill=await list(f,{registeredFrom:'2026-10-20',registeredTo:'2026-10-20',retentionDay:7,retentionStatus:'returned'});assert.deepEqual(drill.rows.map(p=>p.uid),['returned']);assert.equal(drill.rows[0].activeDays,undefined);
});
test('retention D1 follows Kyiv midnight and seventhday is not rolling168hours',()=>{
  const context={today:'2026-11-03',startedAt:'2026-10-01T00:00:00Z',coverageDay:'2026-10-02'};
  const player={createdAt:'2026-10-24T21:30:00Z',activeDays:{'2026-10-26':true,'2026-11-01':true}};
  assert.equal(retentionState(player,1,context),'returned');assert.equal(retentionState(player,7,context),'returned');
  assert.equal(retentionState(player,7,{...context,today:'2026-11-01'}),'pending');
});
test('missing coverage fails closed and a partial presence read cannot invent nonreturns',async()=>{
  const f=fixture();f.data.delete('dashboard_meta/retention');f.player('one','2026-10-20',{activeDays:{'2026-10-21':true}});
  const r=await f.api.retention('2026-10-01','2026-10-28');assert.equal(r.d1.percent,null);assert.equal(r.d1.unknown,1);
  assert.equal(retentionState({createdAt:'2026-10-20'},1,{today:'2026-10-28',startedAt:'2026-10-01',coverageDay:'2026-10-02',presencePartial:true}),'unknown');
});
test('registration missing from Auth is unknown, not fabricated from public profile date',async()=>{
  const f=fixture();f.data.set('users_public/orphan',{nickname:'Old',createdAt:'2026-10-20'});
  const r=await list(f);assert.equal(r.rows[0].createdAt,null);assert.equal(r.rows[0].retention.d1,'unknown');
  const retention=await f.api.retention('2026-10-01','2026-10-28');assert.equal(retention.registrationUnknown,1);assert.equal(retention.cohortPlayers,0);assert.equal(retention.d1.percent,null);
});
test('invalid dates, enum values, negative durations and oversized pages are rejected before reads',async()=>{
  for(const p of [{registeredFrom:'2026-02-30'},{registeredFrom:'2026-10-28',registeredTo:'2026-10-01'},{minMinutes:-1},{minMinutes:2,maxMinutes:1},{sort:'password'},{direction:'sideways'},{pageSize:1000},{page:0},{retentionDay:1},{retentionDay:7,retentionStatus:'later'},{q:'x'.repeat(129)}])assert.throws(()=>readFilters(new URLSearchParams(p)),{status:400});
  const f=fixture();await assert.rejects(f.api.retention('2026-10-29','2026-10-30'),{status:400});assert.equal(f.reads.size,0);
});
test('all analytics endpoints require the existing owner Google session and are uncached',async()=>{
  const {createDashboard}=require('./football-dashboard');const f=fixture();f.player('one','2026-10-20');
  let claims={email:'xdricsx@gmail.com',email_verified:true,firebase:{sign_in_provider:'google.com'}};
  const d=createDashboard({db:f.db,listAccounts:f.listAccounts,credentials:{},verifyIdToken:async()=>claims,live:()=>({})});
  async function call(path,auth=true){let status,body,headers;await d.handle({method:'GET',url:path,headers:auth?{authorization:'Bearer x'}:{},socket:{remoteAddress:'local'}},{writeHead(s,h){status=s;headers=h;},end(b){body=JSON.parse(b);}});return {status,body,headers};}
  for(const path of ['/dashboard/api/players','/dashboard/api/retention?from=2026-10-01&to=2026-10-28']){assert.equal((await call(path,false)).status,401);const r=await call(path);assert.equal(r.status,200);assert.equal(r.headers['Cache-Control'],'no-store');claims={...claims,email:'other@example.com'};assert.equal((await call(path)).status,403);claims={...claims,email:'xdricsx@gmail.com'};}
  assert.equal((await call('/dashboard/api/players?maxMinutes=-1')).status,400);
});
test('scan caps remain explicit and suppress percentages from incomplete denominators',async()=>{
  const f=fixture({maxRows:2});for(const uid of ['a','b','c'])f.player(uid,'2026-10-20',{activeDays:{'2026-10-21':true}});
  const r=await list(f);assert.equal(r.partial,true);assert.equal(r.totalPlayers,2);
  const retention=await f.api.retention('2026-10-01','2026-10-28');assert.equal(retention.partial,true);assert.equal(retention.d1.percent,null);assert.equal(retention.cohorts[0].d1.percent,null);
});
test('authenticated presence writes a union of Kyiv days, crosses midnight without throttle and keeps lastSeen monotonic',async()=>{
  const {createDashboard}=require('./football-dashboard');const rows=new Map(),created=[];let writes=0;
  const db={collection(name){return {doc(id){const key=name+'/'+id;return {key,async create(value){created.push(key);rows.set(key,value);}};}}},async runTransaction(fn){await fn({get:async ref=>({data:()=>rows.get(ref.key)}),set(ref,value){writes++;rows.set(ref.key,{...rows.get(ref.key),...value});}});}};
  const d=createDashboard({db,credentials:{},verifyIdToken:async()=>({}),live:()=>({})});
  d.initialize();assert.deepEqual(created.sort(),['dashboard_meta/coverage','dashboard_meta/retention']);
  const original=Date.now;let time=Date.parse('2026-10-25T21:59:59Z');Date.now=()=>time;
  try{d.presence('one');await new Promise(resolve=>setImmediate(resolve));d.presence('one');await new Promise(resolve=>setImmediate(resolve));assert.equal(writes,1);
    time=Date.parse('2026-10-25T22:00:01Z');d.presence('one');await new Promise(resolve=>setImmediate(resolve));assert.equal(writes,2);
    const p=rows.get('dashboard_presence/'+createHash('sha256').update('one').digest('hex'));assert.deepEqual(p.activeDays,{'2026-10-25':true,'2026-10-26':true});assert.equal(p.lastSeen,'2026-10-25T22:00:01.000Z');
  }finally{Date.now=original;}
});
test('production activity polling no longer duplicates the dedicated player snapshot scans',async()=>{
  const {createActivity}=require('./dashboard-activity');const f=fixture();let authCalls=0;
  const d=createActivity({db:f.db,includeRecent:false,listAccounts:async()=>{authCalls++;return {users:[]};}});
  const result=await d.get('2026-10-01','2026-10-28');assert.equal(result.recentPlayers,null);assert.equal(authCalls,0);assert.equal(f.reads.get('users_public'),undefined);
});
