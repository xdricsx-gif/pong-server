'use strict';
const {createHash}=require('node:crypto');
const fail=(status,message)=>Object.assign(new Error(message),{status});
const hash=x=>createHash('sha256').update(JSON.stringify(x)).digest('hex');
const fields={silver:1e9,gold:1e9,xp:1e9,rating:1e7,teamRating:1e7};
function createAdmin({db,onChanged=()=>{}}){
 const ref=(name,uid)=>db.collection(name).doc(uid);
 function state(p,w){return {silver:w.silver||0,gold:w.gold||0,xp:w.xp||0,rating:p.rating??500,teamRating:p.teamRating??500,banned:!!w.adminBanned,gamesPlayed:p.gamesPlayed||0,wins:p.wins||0,teamGamesPlayed:p.teamGamesPlayed||0,teamWins:p.teamWins||0,trainingGamesPlayed:p.trainingGamesPlayed||0,ratingToday:p.ratingToday||0};}
 const revision=(p,w)=>hash([state(p,w),w.activeMatchId||null]);
 async function detail(uid){
  const [p,w,s,h,c]=await Promise.all([ref('users_public',uid).get(),ref('users_private',uid).get(),ref('dashboard_presence',createHash('sha256').update(uid).digest('hex')).get(),ref('dashboard_admin_players',uid).collection('history').orderBy('at','desc').limit(40).get(),ref('dashboard_admin_players',uid).get()]);
  if(!p.exists||!w.exists)throw fail(404,'profile_missing');
  const wallet={...w.data(),adminBanned:!!c.data()?.banned};
  return {uid,nickname:p.data().nickname||'Гість',createdAt:p.data().createdAt||null,lastSeen:s.data()?.lastSeen||null,activeMatchId:w.data().activeMatchId||null,values:state(p.data(),wallet),revision:revision(p.data(),wallet),history:h.docs.map(d=>d.data())};
 }
 async function list(url){
  const q=(url.searchParams.get('q')||'').trim().toLocaleLowerCase();if(q.length>128)throw fail(400,'invalid_search');
  const cursor=url.searchParams.get('cursor');let query=db.collection('users_public').orderBy('__name__').limit(250);if(cursor)query=query.startAfter(cursor);
  const rows=[];let last=null,more=false,scanned=0;
  // Bounded scan, with a continuation cursor even when a search page has no hits.
  while(scanned<5000&&rows.length<50){const page=await query.get();if(!page.size){more=false;break;}let consumed=0;
   for(const d of page.docs){last=d.id;consumed++;scanned++;const p=d.data();if(!q||d.id.toLowerCase().includes(q)||String(p.nickname||'').toLocaleLowerCase().includes(q))rows.push({uid:d.id,nickname:p.nickname||'Гість',rating:p.rating??500,gamesPlayed:p.gamesPlayed||0});if(rows.length===50)break;}
   more=consumed<page.size||page.size===250;if(!more||rows.length===50)break;query=db.collection('users_public').orderBy('__name__').startAfter(last).limit(250);
  }
  const result=await Promise.all(rows.map(async p=>{const s=await ref('dashboard_presence',createHash('sha256').update(p.uid).digest('hex')).get();return {...p,lastSeen:s.data()?.lastSeen||null};}));
  return {rows:result,cursor:more?last:null,scanned};
 }
 async function mutate(uid,body,actor){
  if(!body||typeof body!=='object'||!['edit','reset','ban','unban'].includes(body.action)||typeof body.reason!=='string'||body.reason.trim().length<3||body.reason.length>500||!/^[-a-zA-Z0-9]{16,80}$/.test(body.requestId||'')||typeof body.revision!=='string')throw fail(400,'invalid_change');
  const digest=hash([uid,body]);
  await db.runTransaction(async tx=>{
   const receipt=ref('dashboard_admin_changes',body.requestId),old=await tx.get(receipt);if(old.exists){if(old.data().digest!==digest)throw fail(409,'request_conflict');return;}
   const pr=ref('users_public',uid),wr=ref('users_private',uid);const p=await tx.get(pr),w=await tx.get(wr);if(!p.exists||!w.exists)throw fail(404,'profile_missing');
   const control=ref('dashboard_admin_players',uid),cs=await tx.get(control);
   const pub=p.data(),priv={...w.data(),adminBanned:!!cs.data()?.banned};if(revision(pub,priv)!==body.revision)throw fail(409,'profile_changed');
   if(priv.activeMatchId&&!['ban','unban'].includes(body.action))throw fail(409,'match_active');
   const before=state(pub,priv),pu={},wu={};
   if(body.action==='edit'){
    if(!body.values||Array.isArray(body.values)||!Object.keys(body.values).length)throw fail(400,'invalid_values');
    for(const [key,value]of Object.entries(body.values)){if(!Object.hasOwn(fields,key)||!Number.isSafeInteger(value)||value<0||value>fields[key])throw fail(400,'invalid_values');if(['rating','teamRating'].includes(key))pu[key]=value;else wu[key]=value;if(key==='xp')pu.xp=value;}
   }else if(body.action==='reset'){
    if(body.confirm!==pub.nickname)throw fail(400,'confirmation_required');
    Object.assign(pu,{xp:0,rating:500,teamRating:500,gamesPlayed:0,wins:0,teamGamesPlayed:0,teamWins:0,trainingGamesPlayed:0,ratingToday:0});wu.xp=0;
   }else wu.adminBanned=body.action==='ban';
   const after=state({...pub,...pu},{...priv,...wu});const audit={uid,nickname:pub.nickname||'Гість',action:body.action,reason:body.reason.trim(),actor,at:new Date().toISOString(),before,after,digest};
   if(Object.hasOwn(wu,'adminBanned')){tx.set(control,{banned:wu.adminBanned,updatedAt:audit.at});delete wu.adminBanned;}
   if(Object.keys(pu).length)tx.update(pr,pu);if(Object.keys(wu).length)tx.update(wr,wu);tx.create(receipt,audit);tx.set(ref('dashboard_admin_players',uid).collection('history').doc(body.requestId),audit);
  });
  await onChanged(uid);return detail(uid);
 }
 async function handle(req,url,actor){
  if(url.pathname==='/dashboard/api/admin/players'&&req.method==='GET')return list(url);
  const match=url.pathname.match(/^\/dashboard\/api\/admin\/players\/([^/]+)$/);if(!match)throw fail(404,'not_found');
  let uid;try{uid=decodeURIComponent(match[1]);}catch{throw fail(400,'invalid_uid');}if(!uid||uid.length>128||uid.includes('/')||uid==='.'||uid==='..')throw fail(400,'invalid_uid');
  if(req.method==='GET')return detail(uid);
  let size=0,chunks=[];for await(const c of req){size+=c.length;if(size>10000)throw fail(413,'too_large');chunks.push(c);}let body;try{body=JSON.parse(Buffer.concat(chunks).toString());}catch{throw fail(400,'invalid_json');}return mutate(uid,body,actor);
 }
 return {handle,mutate,detail};
}
module.exports={createAdmin};
