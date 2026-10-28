'use strict';
const {createHash}=require('node:crypto');
const {day,calendar,shiftDay}=require('./dashboard-time');
const {iso,playtime}=require('./dashboard-activity');
const fail=message=>Object.assign(new Error(message),{status:400});
const hash=uid=>createHash('sha256').update(uid).digest('hex');
const SORTS=new Set(['createdAt','lastSeen','foregroundSeconds','nickname','gamesPlayed','rating']);
const STATUSES=new Set(['returned','not_returned','pending','unknown']);
const finite=value=>Number.isFinite(value)?Math.max(0,value):0;

function readFilters(params){
  const get=name=>params.get(name)||'';
  const q=get('q').trim();if(q.length>128)throw fail('invalid_search');
  const filters={q,sort:get('sort')||'createdAt',direction:get('direction')||'desc',timeStatus:get('timeStatus')||'all'};
  if(!SORTS.has(filters.sort)||!['asc','desc'].includes(filters.direction)||!['all','measured','unmeasured'].includes(filters.timeStatus))throw fail('invalid_filters');
  for(const key of ['registeredFrom','registeredTo','lastSeenFrom','lastSeenTo']){const value=get(key);if(value&&!calendar(value))throw fail('invalid_dates');filters[key]=value||null;}
  for(const prefix of ['registered','lastSeen'])if(filters[prefix+'From']&&filters[prefix+'To']&&filters[prefix+'From']>filters[prefix+'To'])throw fail('invalid_dates');
  for(const key of ['minMinutes','maxMinutes']){const raw=get(key);const value=raw===''?null:Number(raw);if(value!==null&&(!Number.isFinite(value)||value<0||value>100000000))throw fail('invalid_filters');filters[key]=value;}
  if(filters.minMinutes!==null&&filters.maxMinutes!==null&&filters.minMinutes>filters.maxMinutes)throw fail('invalid_filters');
  filters.retentionDay=get('retentionDay')||null;filters.retentionStatus=get('retentionStatus')||null;
  if(filters.retentionDay&&!['1','7'].includes(filters.retentionDay)||filters.retentionStatus&&!STATUSES.has(filters.retentionStatus)||!!filters.retentionDay!==!!filters.retentionStatus)throw fail('invalid_filters');
  for(const [key,fallback,max]of [['page',1,1000000],['pageSize',30,100]]){const raw=get(key);const value=raw===''?fallback:Number(raw);if(!Number.isSafeInteger(value)||value<1||value>max)throw fail('invalid_pagination');filters[key]=value;}
  return filters;
}
function retentionState(player,offset,{today,startedAt,coverageDay,presencePartial=false}){
  const created=day(player.createdAt);if(!created||player.isAnonymous)return 'unknown';
  const target=shiftDay(created,offset);
  if(target>=today)return 'pending';
  // A last login cannot reconstruct the exact days of earlier returns. Only
  // the daily server observations, during a complete covered day, are used.
  if(!startedAt||!coverageDay||target<coverageDay)return 'unknown';
  if(player.activeDays?.[target]===true)return 'returned';
  return presencePartial?'unknown':'not_returned';
}
function metric(states){
  const counts={eligible:0,returned:0,percent:null,pending:0,unknown:0};
  for(const state of states){if(state==='returned'){counts.eligible++;counts.returned++;}else if(state==='not_returned')counts.eligible++;else counts[state]++;}
  if(counts.eligible)counts.percent=Math.round(counts.returned/counts.eligible*10000)/100;
  return counts;
}
function matches(p,f){
  if(f.q&&!(`${p.nickname}\n${p.uid}`).toLocaleLowerCase().includes(f.q.toLocaleLowerCase()))return false;
  for(const [field,prefix]of [['createdAt','registered'],['lastSeen','lastSeen']]){const date=day(p[field]);if(f[prefix+'From']&&(!date||date<f[prefix+'From']))return false;if(f[prefix+'To']&&(!date||date>f[prefix+'To']))return false;}
  const measured=p.foregroundSeconds!==null;
  if(f.timeStatus==='measured'&&!measured||f.timeStatus==='unmeasured'&&measured)return false;
  if(f.minMinutes!==null&&(!measured||p.foregroundSeconds<f.minMinutes*60))return false;
  if(f.maxMinutes!==null&&(!measured||p.foregroundSeconds>f.maxMinutes*60))return false;
  if(f.retentionDay&&(p.isAnonymous||p.retention['d'+f.retentionDay]!==f.retentionStatus))return false;
  return true;
}
function compare(field,direction){return (a,b)=>{
  const av=a[field],bv=b[field],an=av===null||av===undefined,bn=bv===null||bv===undefined;
  // Missing measurements remain last in either direction, never treated as 0.
  if(an||bn)return an===bn?a.uid.localeCompare(b.uid):an?1:-1;
  const n=typeof av==='number'&&typeof bv==='number'?av-bv:String(av).localeCompare(String(bv),'uk');
  return n*(direction==='asc'?1:-1)||a.uid.localeCompare(b.uid);
};}
const publicRow=p=>{const {activeDays,isAnonymous,...row}=p;return row;};

function createPlayerAnalytics({db,listAccounts,now=Date.now,maxRows=50000,cacheMs=60000,authCacheMs=300000}={}){
  const cache=new Map();
  async function cached(key,ttl,fn){const old=cache.get(key);if(old&&old.until>now())return old.promise;const item={until:now()+ttl,promise:null};item.promise=Promise.resolve().then(fn).catch(e=>{if(cache.get(key)===item)cache.delete(key);throw e;});cache.set(key,item);return item.promise;}
  async function collection(name,fields){
    const rows=[];let after;
    while(rows.length<maxRows){const limit=Math.min(1000,maxRows-rows.length);let query=db.collection(name).select(...fields).orderBy('__name__').limit(limit);if(after)query=query.startAfter(after);const snap=await query.get();rows.push(...snap.docs.map(d=>({id:d.id,...d.data()})));if(snap.size<limit)return {rows,partial:false};after=snap.docs.at(-1);}
    return {rows,partial:true};
  }
  async function accounts(){return cached('auth',authCacheMs,async()=>{
    if(!listAccounts)return {rows:new Map(),partial:false,available:false};
    const rows=new Map();let token;
    do{const result=await listAccounts(Math.min(1000,maxRows-rows.size),token);for(const a of result.users||[])rows.set(a.uid,iso(a.metadata?.creationTime));token=result.pageToken;}while(token&&rows.size<maxRows);
    return {rows,partial:!!token,available:true};
  });}
  async function snapshot(){return cached('snapshot',cacheMs,async()=>{
    // Reuse one bounded snapshot across all filters/pages. No profile-per-row
    // lookup and no new Firebase scan on each sorting action.
    const [profiles,presence,auth,meta]=await Promise.all([
      collection('users_public',['nickname','gamesPlayed','teamGamesPlayed','trainingGamesPlayed','rating','isAnonymous']),
      collection('dashboard_presence',['lastSeen','foregroundSeconds','measuredSince','lastForegroundAt','activeDays']),
      accounts(),db.collection('dashboard_meta').doc('retention').get()
    ]);
    const byId=new Map(presence.rows.map(p=>[p.id,p]));
    const rows=profiles.rows.map(p=>{const activity=byId.get(hash(p.id))||{};return {uid:p.id,nickname:p.nickname||'Гість',createdAt:auth.rows.get(p.id)||null,registrationSource:auth.rows.has(p.id)?'firebase_auth':null,gamesPlayed:finite(p.gamesPlayed)+finite(p.teamGamesPlayed)+finite(p.trainingGamesPlayed),rating:finite(p.rating),isAnonymous:p.isAnonymous===true,...playtime(activity),activeDays:activity.activeDays||{}};});
    return {rows,partial:profiles.partial||presence.partial||auth.partial,profilesPartial:profiles.partial,presencePartial:presence.partial,authPartial:auth.partial,registrationAvailable:auth.available,startedAt:iso(meta.data()?.startedAt),updatedAt:new Date(now()).toISOString()};
  });}
  function context(s){const today=day(new Date(now()));const startDay=day(s.startedAt);return {today,startedAt:s.startedAt,coverageDay:startDay?shiftDay(startDay,1):null,presencePartial:s.presencePartial};}
  function measuredMetric(players,key,partial){const value=metric(players.map(p=>p.retention[key]));if(partial)value.percent=null;return value;}
  function decorate(s){const ctx=context(s);return s.rows.map(p=>({...p,retention:{d1:retentionState(p,1,ctx),d7:retentionState(p,7,ctx)}}));}
  async function list(params){
    const filters=readFilters(params),s=await snapshot();
    const rows=decorate(s).filter(p=>matches(p,filters)).sort(compare(filters.sort,filters.direction));
    const pages=Math.max(1,Math.ceil(rows.length/filters.pageSize)),page=Math.min(filters.page,pages);
    return {rows:rows.slice((page-1)*filters.pageSize,page*filters.pageSize).map(publicRow),total:rows.length,totalPlayers:s.rows.length,page,pageSize:filters.pageSize,pages,partial:s.partial,updatedAt:s.updatedAt,registrationAvailable:s.registrationAvailable,registrationSource:'firebase_auth',filters,timezone:'Europe/Kyiv'};
  }
  async function retention(from,to){
    const today=day(new Date(now()));if(!calendar(from)||!calendar(to)||from>to||to>today)throw fail('invalid_dates');
    const s=await snapshot(),ctx=context(s),all=decorate(s);
    const rows=all.filter(p=>!p.isAnonymous&&p.createdAt&&day(p.createdAt)>=from&&day(p.createdAt)<=to);
    const cohorts=new Map();for(const p of rows){const date=day(p.createdAt);if(!cohorts.has(date))cohorts.set(date,[]);cohorts.get(date).push(p);}
    return {from,to,timezone:'Europe/Kyiv',d1:measuredMetric(rows,'d1',s.partial),d7:measuredMetric(rows,'d7',s.partial),cohortPlayers:rows.length,registrationUnknown:all.filter(p=>!p.createdAt).length,excludedAnonymous:all.filter(p=>p.isAnonymous).length,
      cohorts:[...cohorts].sort(([a],[b])=>b.localeCompare(a)).map(([date,players])=>({date,players:players.length,d1:measuredMetric(players,'d1',s.partial),d7:measuredMetric(players,'d7',s.partial)})),
      coverageFrom:s.startedAt,coverageDay:ctx.coverageDay,partial:s.partial,registrationAvailable:s.registrationAvailable,updatedAt:s.updatedAt,
      definition:'Повернення саме наступного або сьомого календарного дня після реєстрації. Дні рахуються за Києвом. Незавершені дні та періоди без повної історії не входять у відсоток.'};
  }
  return {list,retention,invalidate(){cache.delete('snapshot');}};
}
module.exports={createPlayerAnalytics,readFilters,retentionState,metric};
