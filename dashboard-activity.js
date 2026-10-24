'use strict';
const {createHash}=require('node:crypto');
const {day}=require('./dashboard-data');
const hash=uid=>createHash('sha256').update(uid).digest('hex');
function iso(value){
  if(value==null)return null;
  try{const date=value.toDate?value.toDate():new Date(value);return Number.isFinite(date.getTime())?date.toISOString():null;}catch{return null;}
}
function playtime(p={}){
  const measuredSince=iso(p.measuredSince);
  return {foregroundSeconds:measuredSince&&Number.isFinite(p.foregroundSeconds)?Math.max(0,Math.floor(p.foregroundSeconds)):null,measuredSince,lastSeen:iso(p.lastSeen),lastForegroundAt:iso(p.lastForegroundAt)};
}
// One bounded history read per five minutes; successful match writes are overlaid immediately.
function createActivity({db,now=Date.now,onFailure=()=>{}}){
  const cache=new Map(),overlay=new Map();let lastRecordedAt=null;
  async function cached(key,ttl,fn){
    const old=cache.get(key);if(old&&old.expires>now())return old.promise;
    const entry={expires:now()+ttl,promise:null};
    entry.promise=Promise.resolve().then(fn).catch(e=>{if(cache.get(key)===entry)cache.delete(key);throw e;});cache.set(key,entry);return entry.promise;
  }
  async function history(){return cached('history',300000,async()=>{
    const s=await db.collection('dashboard_matches').orderBy('endedAt','desc').limit(10000).get();
    const rows=s.docs.map(d=>({id:d.id,...d.data()}));
    for(const row of rows)overlay.delete(row.id);
    return {rows,partial:s.size===10000,updatedAt:new Date(now()).toISOString()};
  });}
  async function recentPlayers(){return cached('recent',30000,async()=>{
    const s=await db.collection('users_public').orderBy('createdAt','desc').limit(30).select('nickname','createdAt').get();
    const players=await Promise.all(s.docs.map(async d=>{
      const p=d.data();let presence={};
      try{presence=(await db.collection('dashboard_presence').doc(hash(d.id)).get()).data()||{};}catch{onFailure();}
      return {uid:d.id,nickname:p.nickname||'Гість',createdAt:iso(p.createdAt),...playtime(presence)};
    }));
    return {rows:players,updatedAt:new Date(now()).toISOString()};
  });}
  async function get(from,to){
    const result=await Promise.allSettled([history(),recentPlayers(),cached('coverage',300000,async()=>iso((await db.collection('dashboard_meta').doc('coverage').get()).data()?.startedAt))]);
    const h=result[0].status==='fulfilled'?result[0].value:null;
    const recent=result[1].status==='fulfilled'?result[1].value:null;
    const all=new Map((h?.rows||[]).map(r=>[r.id,r]));
    for(const [id,row]of overlay)all.set(id,row);
    const matches=[...all.values()].filter(m=>day(m.endedAt)>=from&&day(m.endedAt)<=to).sort((a,b)=>String(b.endedAt).localeCompare(String(a.endedAt)));
    const daily=new Map();for(const m of matches){const date=day(m.endedAt);daily.set(date,(daily.get(date)||0)+1);}
    const activity=h||overlay.size?{count:matches.length,matches:matches.length,partial:!h||h.partial,coverageFrom:result[2].status==='fulfilled'?result[2].value:null,
      withBots:matches.filter(m=>m.bots>0).length,training:matches.filter(m=>m.training).length,
      averageSeconds:matches.length?Math.round(matches.reduce((n,m)=>n+(Number(m.duration)||0),0)/matches.length):null,
      rows:matches.slice(0,30).map(({id,...m})=>m),daily:[...daily].sort(([a],[b])=>a.localeCompare(b)).map(([date,matches])=>({date,matches}))}:null;
    return {activity,recentPlayers:recent?.rows??null,recentPlayersUpdatedAt:recent?.updatedAt??null,dataUpdatedAt:[h?.updatedAt,lastRecordedAt].filter(Boolean).sort().at(-1)||null,
      playtimeNotice:'Час рахується з моменту підключення нового обліку, коли гра відкрита на екрані та є зв’язок із сервером. Старі версії й офлайн-час не вимірюються.'};
  }
  async function record(room,training){
    const id=hash(`${room.id}/${room.startedAt}`);
    const participants=new Set([...Object.values(room.roster||{}),...Object.values(room.players||{}),...Object.values(room._disconnected||{})].map(p=>p.uid).filter(Boolean));
    const row={endedAt:new Date(now()).toISOString(),duration:Math.max(0,Math.round((now()-(room.startedAt||now()))/1000)),players:participants.size,bots:Object.values(room.bots||{}).filter(Boolean).length,training:!!training,mode:room.mode||null};
    await db.collection('dashboard_matches').doc(id).set(row);
    overlay.set(id,{id,...row});lastRecordedAt=new Date(now()).toISOString();
    if(overlay.size>10000){cache.delete('history');overlay.delete(overlay.keys().next().value);}
    return row;
  }
  return {get,record,invalidatePlayers(){cache.delete('recent');}};
}
module.exports={createActivity,iso,playtime};
