'use strict';
const fs=require('node:fs');const path=require('node:path');const {createHash}=require('node:crypto');
const {GoogleAuth}=require('google-auth-library');
const {PACKAGE,day,parseInstalls,aggregateInstalls,summarizeOrders}=require('./dashboard-data');
const PUBLIC_FIREBASE={apiKey:'AIzaSyCLri9CBFVYbdXzIuJ4QLc9Lm7YNkaD9Ac',authDomain:'crazy-footbal.firebaseapp.com',projectId:'crazy-footbal',appId:'1:348183694114:web:303c7ef63e5f0a857c337b'};
function createDashboard({db,verifyIdToken,credentials,live,bucket=process.env.PLAY_REPORTS_BUCKET||'pubsite_prod_6971140736608519774',owner=process.env.DASHBOARD_OWNER_EMAIL||'xdricsx@gmail.com'}) {
  const storageAuth=new GoogleAuth({credentials,scopes:['https://www.googleapis.com/auth/devstorage.read_only']});
  const playAuth=new GoogleAuth({credentials,scopes:['https://www.googleapis.com/auth/androidpublisher']});
  const cache=new Map(); const rates=new Map(); let failureCount=0;
  async function cached(key,ms,fn){let c=cache.get(key);if(c && c.until>Date.now())return c.promise;const promise=fn();cache.set(key,{until:Date.now()+ms,promise});try{return await promise;}catch(e){cache.delete(key);throw e;}}
  async function google(auth,url,opts={}){const client=await auth.getClient();return (await client.request({url,method:'GET',timeout:20000,...opts})).data;}
  async function reports(){return cached('reports',6*3600000,async()=>{
    if(!bucket || !/^pubsite_prod_(rev_)?\d+$/.test(bucket))throw new Error('reports_not_configured');
    const root=`https://storage.googleapis.com/storage/v1/b/${bucket}/o`;
    let files=[],next;
    do {const result=await google(storageAuth,root+'?'+new URLSearchParams({prefix:`stats/installs/installs_${PACKAGE}_`,maxResults:'1000',...(next?{pageToken:next}:{})}));files.push(...(result.items||[]).filter(f=>f.name.endsWith('_country.csv')));next=result.nextPageToken;}while(next);
    const rows=[];
    for(let i=0;i<files.length;i+=4){const parts=await Promise.all(files.slice(i,i+4).map(async f=>parseInstalls(Buffer.from(await google(storageAuth,`${root}/${encodeURIComponent(f.name)}?alt=media`,{responseType:'arraybuffer'})))));parts.forEach(p=>rows.push(...p));}
    return {rows,files:files.length,syncedAt:new Date().toISOString()};
  });}
  async function collection(name,fields){let all=[],after;do{let query=db.collection(name).select(...fields).orderBy('__name__').limit(1000);if(after)query=query.startAfter(after);const s=await query.get();all.push(...s.docs.map(d=>({id:d.id,...d.data()})));after=s.docs.at(-1);if(s.size<1000)return {rows:all,partial:false};}while(all.length<50000);return {rows:all,partial:true};}
  async function base(){return cached('base',300000,async()=>{
    const jobs=[collection('users_public',['nickname','createdAt','gamesPlayed','wins','rating','isAnonymous']),collection('play_purchase_ledger',['uid','productId','quantity','gold','orderId','testPurchase','grantedAt','consumed']),collection('dashboard_presence',['lastSeen','firstSeen']),db.collection('dashboard_matches').orderBy('endedAt','desc').limit(10000).get(),db.collection('dashboard_meta').doc('coverage').get()];
    const r=await Promise.allSettled(jobs);return {profiles:r[0].status==='fulfilled'?r[0].value:null,ledger:r[1].status==='fulfilled'?r[1].value:null,presence:r[2].status==='fulfilled'?r[2].value:null,matches:r[3].status==='fulfilled'?{rows:r[3].value.docs.map(d=>d.data()),partial:r[3].value.size===10000}:null,coverage:r[4].status==='fulfilled'?r[4].value.data()?.startedAt:null};
  });}
  async function order(orderId){if(!orderId || !/^GPA\.[\w.-]+$/.test(orderId))return null;return cached('order:'+orderId,15*60000,()=>google(playAuth,`https://androidpublisher.googleapis.com/androidpublisher/v3/applications/${PACKAGE}/orders/${encodeURIComponent(orderId)}?fields=state,total,developerRevenueInBuyerCurrency,createTime`));}
  async function summary(from,to){
    const [b,r]=await Promise.all([base(),reports().then(value=>({ok:true,...value})).catch(e=>({ok:false,error:e.response?.status===403?'access_denied':e.message==='reports_not_configured'?'not_configured':'sync_failed'}))]);
    const ledger=b.ledger?.rows.filter(p=>day(p.grantedAt)>=from && day(p.grantedAt)<=to)||[];
    const orders=new Map();const ids=[...new Set(ledger.filter(p=>!p.testPurchase).map(p=>p.orderId).filter(Boolean))];
    for(let i=0;i<ids.length;i+=5)await Promise.all(ids.slice(i,i+5).map(async id=>{try{orders.set(id,await order(id));}catch{}}));
    const matches=b.matches?.rows.filter(m=>day(m.endedAt)>=from&&day(m.endedAt)<=to)||[];
    const profiles=b.profiles?.rows||[];
    const online=live();
    return {generatedAt:new Date().toISOString(),from,to,timezone:'UTC',live:online,
      installs:r.ok?{ok:true,...aggregateInstalls(r.rows,from,to),syncedAt:r.syncedAt,files:r.files}:r,
      audience:b.profiles?{total:profiles.length,partial:b.profiles.partial,newProfiles:profiles.filter(p=>day(p.createdAt)>=from&&day(p.createdAt)<=to).length,played:profiles.filter(p=>(p.gamesPlayed||0)>0).length,participations:profiles.reduce((s,p)=>s+(p.gamesPlayed||0),0),leaders:profiles.sort((a,b)=>(b.gamesPlayed||0)-(a.gamesPlayed||0)).slice(0,20).map(p=>({nickname:p.nickname||'Гість',games:p.gamesPlayed||0,wins:p.wins||0,rating:p.rating||0})),seenToday:b.presence?b.presence.rows.filter(p=>day(p.lastSeen)===day(new Date())).length:null}:null,
      activity:b.matches?{count:matches.length,partial:b.matches.partial,coverageFrom:b.coverage||null,withBots:matches.filter(m=>m.bots>0).length,training:matches.filter(m=>m.training).length,averageSeconds:matches.length?Math.round(matches.reduce((n,m)=>n+m.duration,0)/matches.length):null,rows:matches.slice(0,30),daily:[...new Set(matches.map(m=>day(m.endedAt)))].sort().map(date=>({date,matches:matches.filter(m=>day(m.endedAt)===date).length}))}:null,
      purchases:b.ledger?{...summarizeOrders(ledger,orders),partial:b.ledger.partial}:null,
      collectionFailures:failureCount,
    };
  }
  const headers={'Cache-Control':'no-store','X-Content-Type-Options':'nosniff','X-Frame-Options':'DENY','Referrer-Policy':'no-referrer','X-Robots-Tag':'noindex, nofollow','Content-Security-Policy':"default-src 'self'; script-src 'self' https://www.gstatic.com https://apis.google.com; style-src 'self'; img-src 'self' data:; connect-src 'self' https://*.googleapis.com https://*.firebaseapp.com https://*.google.com; frame-src https://crazy-footbal.firebaseapp.com https://accounts.google.com; object-src 'none'; base-uri 'self'; frame-ancestors 'none'"};
  function send(res,status,data,type='application/json; charset=utf-8'){res.writeHead(status,{...headers,'Content-Type':type});res.end(type.startsWith('application/json')?JSON.stringify(data):data);}
  async function handle(req,res){
    const url=new URL(req.url,'http://local');
    if(req.method!=='GET')return send(res,405,{error:'method_not_allowed'});
    const files={'/dashboard/':'dashboard.html','/dashboard':'dashboard.html','/dashboard/app.js':'dashboard-app.js','/dashboard/style.css':'dashboard-style.css'};
    if(Object.hasOwn(files,url.pathname)){const name=files[url.pathname];return send(res,200,fs.readFileSync(path.join(__dirname,name)),name.endsWith('.js')?'text/javascript; charset=utf-8':name.endsWith('.css')?'text/css; charset=utf-8':'text/html; charset=utf-8');}
    if(url.pathname==='/dashboard/config')return send(res,200,{firebase:PUBLIC_FIREBASE});
    if(url.pathname!=='/dashboard/api/summary')return send(res,404,{error:'not_found'});
    const ip=req.socket.remoteAddress;const at=Date.now();let rate=rates.get(ip);if(!rate||at-rate.at>60000)rates.set(ip,rate={at,n:0});if(rates.size>10000)rates.clear();if(++rate.n>60)return send(res,429,{error:'rate_limit'});
    const token=req.headers.authorization?.match(/^Bearer (.{1,10000})$/)?.[1];if(!token)return send(res,401,{error:'sign_in_required'});
    let user;try{user=await verifyIdToken(token);}catch{return send(res,401,{error:'invalid_session'});}
    if(user.email_verified!==true||user.firebase?.sign_in_provider!=='google.com'||user.email?.toLowerCase()!==owner.toLowerCase())return send(res,403,{error:'owner_only'});
    const to=url.searchParams.get('to')||day(new Date()),from=url.searchParams.get('from')||day(Date.now()-29*86400000);
    if(![from,to].every(d=>/^\d{4}-\d{2}-\d{2}$/.test(d)&&day(d)===d)||from>to||to>day(new Date()))return send(res,400,{error:'invalid_dates'});
    try{send(res,200,await summary(from,to));}catch{send(res,503,{error:'temporarily_unavailable'});}
  }
  function initialize(){db.collection('dashboard_meta').doc('coverage').create({startedAt:new Date().toISOString()}).catch(e=>{if(e.code!==6)failureCount++;});}
  const seen=new Map();
  function presence(uid){if(!uid || (seen.get(uid)||0)>Date.now()-300000)return;seen.set(uid,Date.now());if(seen.size>10000)seen.clear();const id=createHash('sha256').update(uid).digest('hex');db.collection('dashboard_presence').doc(id).set({lastSeen:new Date().toISOString()},{merge:true}).catch(()=>failureCount++);}
  function match(room,training){const id=createHash('sha256').update(`${room.id}/${room.startedAt}`).digest('hex');const participants=new Set([...Object.values(room.players||{}),...Object.values(room._disconnected||{})].map(p=>p.uid).filter(Boolean));db.collection('dashboard_matches').doc(id).set({endedAt:new Date().toISOString(),duration:Math.max(0,Math.round((Date.now()-(room.startedAt||Date.now()))/1000)),players:participants.size,bots:Object.values(room.bots||{}).filter(Boolean).length,training:!!training}).catch(()=>failureCount++);}
  return {handle,initialize,presence,match};
}
module.exports={createDashboard};
