'use strict';
const fs=require('node:fs');const path=require('node:path');const {createHash}=require('node:crypto');
const {GoogleAuth}=require('google-auth-library');
const {PACKAGE,day,aggregateInstalls,summarizeOrders}=require('./dashboard-data');
const {createActivity}=require('./dashboard-activity');
const {createPlayReports}=require('./dashboard-play-reports');
const PUBLIC_FIREBASE={apiKey:'AIzaSyCLri9CBFVYbdXzIuJ4QLc9Lm7YNkaD9Ac',authDomain:'crazy-footbal.firebaseapp.com',projectId:'crazy-footbal',appId:'1:348183694114:web:303c7ef63e5f0a857c337b'};
function createDashboard({db,verifyIdToken,credentials,live,onPlayerChanged,bucket=process.env.PLAY_REPORTS_BUCKET||'pubsite_prod_6971140736608519774',owner=process.env.DASHBOARD_OWNER_EMAIL||'xdricsx@gmail.com'}) {
  const playerAdmin=require('./dashboard-admin').createAdmin({db,onChanged:async uid=>{cache.clear();activityData.invalidatePlayers();await onPlayerChanged?.(uid);}});
  const storageAuth=new GoogleAuth({credentials,scopes:['https://www.googleapis.com/auth/devstorage.read_only']});
  const playAuth=new GoogleAuth({credentials,scopes:['https://www.googleapis.com/auth/androidpublisher']});
  const cache=new Map(); const rates=new Map(); let failureCount=0,lastManualRefresh=0;
  const activityData=createActivity({db,onFailure:()=>failureCount++});
  const playReports=createPlayReports({db,bucket,download:({bucket,name,signal})=>google(storageAuth,`https://storage.googleapis.com/storage/v1/b/${bucket}/o/${encodeURIComponent(name)}?alt=media`,{responseType:'arraybuffer',signal})});
  async function cached(key,ms,fn){let c=cache.get(key);if(c && c.until>Date.now())return c.promise;const promise=fn();cache.set(key,{until:Date.now()+ms,promise});try{return await promise;}catch(e){if(cache.get(key)?.promise===promise)cache.delete(key);throw e;}}
  async function google(auth,url,opts={}){const client=await auth.getClient();try{return (await client.request({url,method:'GET',timeout:20000,...opts})).data;}catch(e){let body=e.response?.data;try{if(body?.text)body=await body.text();if(Buffer.isBuffer(body)||ArrayBuffer.isView(body)||body instanceof ArrayBuffer)body=Buffer.from(body).toString();if(typeof body==='string')body=JSON.parse(body);e.reportReason=body?.error?.errors?.[0]?.reason||body?.error?.status||'unknown';}catch{}throw e;}}
  async function importReports(req){
    let size=0;const chunks=[];for await(const chunk of req){size+=chunk.length;if(size>10000000){const e=new Error('too_large');e.status=413;throw e;}chunks.push(chunk);}
    let files;try{files=JSON.parse(Buffer.concat(chunks).toString('utf8')).files;}catch{throw Object.assign(new Error('invalid_reports'),{status:400});}
    return playReports.importFiles(files);
  }
  async function collection(name,fields){let all=[],after;do{let query=db.collection(name).select(...fields).orderBy('__name__').limit(1000);if(after)query=query.startAfter(after);const s=await query.get();all.push(...s.docs.map(d=>({id:d.id,...d.data()})));after=s.docs.at(-1);if(s.size<1000)return {rows:all,partial:false};}while(all.length<50000);return {rows:all,partial:true};}
  async function base(){return cached('base',300000,async()=>{
    const jobs=[collection('users_public',['nickname','createdAt','gamesPlayed','wins','rating','isAnonymous']),collection('play_purchase_ledger',['uid','productId','quantity','gold','orderId','testPurchase','grantedAt','consumed']),collection('dashboard_presence',['lastSeen'])];
    const r=await Promise.allSettled(jobs);return {profiles:r[0].status==='fulfilled'?r[0].value:null,ledger:r[1].status==='fulfilled'?r[1].value:null,presence:r[2].status==='fulfilled'?r[2].value:null,updatedAt:new Date().toISOString()};
  });}
  async function activity(from,to){return {generatedAt:new Date().toISOString(),from,to,timezone:'UTC',live:live(),...await activityData.get(from,to)};}
  async function order(orderId){if(!orderId || !/^GPA\.[\w.-]+$/.test(orderId))return null;return cached('order:'+orderId,15*60000,()=>google(playAuth,`https://androidpublisher.googleapis.com/androidpublisher/v3/applications/${PACKAGE}/orders/${encodeURIComponent(orderId)}?fields=state,total,developerRevenueInBuyerCurrency,createTime`));}
  async function summary(from,to,refresh=false){
    if(refresh&&Date.now()-lastManualRefresh>=60000){lastManualRefresh=Date.now();cache.delete('base');activityData.invalidatePlayers();}
    const [b,r,a]=await Promise.all([base(),playReports.get({refresh}),activity(from,to)]);
    const ledger=b.ledger?.rows.filter(p=>day(p.grantedAt)>=from && day(p.grantedAt)<=to)||[];
    const orders=new Map();const ids=[...new Set(ledger.filter(p=>!p.testPurchase).map(p=>p.orderId).filter(Boolean))];
    for(let i=0;i<ids.length;i+=5)await Promise.all(ids.slice(i,i+5).map(async id=>{try{orders.set(id,await order(id));}catch{}}));
    const profiles=b.profiles?.rows||[];
    return {...a,generatedAt:new Date().toISOString(),summaryUpdatedAt:b.updatedAt,
      installs:r.ok?{...Object.fromEntries(Object.entries(r).filter(([key])=>key!=='rows')),...aggregateInstalls(r.rows,from,to)}:r,
      audience:b.profiles?{total:profiles.length,partial:b.profiles.partial,newProfiles:profiles.filter(p=>day(p.createdAt)>=from&&day(p.createdAt)<=to).length,played:profiles.filter(p=>(p.gamesPlayed||0)>0).length,participations:profiles.reduce((s,p)=>s+(p.gamesPlayed||0),0),leaders:profiles.sort((a,b)=>(b.gamesPlayed||0)-(a.gamesPlayed||0)).slice(0,20).map(p=>({nickname:p.nickname||'Гість',games:p.gamesPlayed||0,wins:p.wins||0,rating:p.rating||0})),seenToday:b.presence?b.presence.rows.filter(p=>day(p.lastSeen)===day(new Date())).length:null}:null,
      purchases:b.ledger?{...summarizeOrders(ledger,orders),partial:b.ledger.partial}:null,
      collectionFailures:failureCount,
    };
  }
  const headers={'Cache-Control':'no-store','X-Content-Type-Options':'nosniff','X-Frame-Options':'DENY','Referrer-Policy':'no-referrer','X-Robots-Tag':'noindex, nofollow','Content-Security-Policy':"default-src 'self'; script-src 'self' https://www.gstatic.com https://apis.google.com; style-src 'self'; img-src 'self' data:; connect-src 'self' https://*.googleapis.com https://*.firebaseapp.com https://*.google.com; frame-src https://crazy-footbal.firebaseapp.com https://accounts.google.com; object-src 'none'; base-uri 'self'; frame-ancestors 'none'"};
  function send(res,status,data,type='application/json; charset=utf-8'){res.writeHead(status,{...headers,'Content-Type':type});res.end(type.startsWith('application/json')?JSON.stringify(data):data);}
  async function handle(req,res){
    const url=new URL(req.url,'http://local');
    const adminRoute=url.pathname.startsWith('/dashboard/api/admin/');
    const importing=url.pathname==='/dashboard/api/import-installs';
    if(adminRoute?!['GET','POST'].includes(req.method):req.method!==(importing?'POST':'GET'))return send(res,405,{error:'method_not_allowed'});
    const files={'/dashboard/':'dashboard.html','/dashboard':'dashboard.html','/dashboard/app.js':'dashboard-app.js','/dashboard/style.css':'dashboard-style.css','/dashboard/admin.js':'dashboard-admin-app.js','/dashboard/admin.css':'dashboard-admin.css'};
    if(Object.hasOwn(files,url.pathname)){const name=files[url.pathname];return send(res,200,fs.readFileSync(path.join(__dirname,name)),name.endsWith('.js')?'text/javascript; charset=utf-8':name.endsWith('.css')?'text/css; charset=utf-8':'text/html; charset=utf-8');}
    if(url.pathname==='/dashboard/config')return send(res,200,{firebase:PUBLIC_FIREBASE});
    if(url.pathname!=='/dashboard/api/summary'&&url.pathname!=='/dashboard/api/activity'&&!importing&&!adminRoute)return send(res,404,{error:'not_found'});
    const ip=req.socket.remoteAddress;const at=Date.now();let rate=rates.get(ip);if(!rate||at-rate.at>60000)rates.set(ip,rate={at,n:0});if(rates.size>10000)rates.clear();if(++rate.n>60)return send(res,429,{error:'rate_limit'});
    const token=req.headers.authorization?.match(/^Bearer (.{1,10000})$/)?.[1];if(!token)return send(res,401,{error:'sign_in_required'});
    let user;try{user=await verifyIdToken(token);}catch{return send(res,401,{error:'invalid_session'});}
    if(user.email_verified!==true||user.firebase?.sign_in_provider!=='google.com'||user.email?.toLowerCase()!==owner.toLowerCase())return send(res,403,{error:'owner_only'});
    if(adminRoute){
      if(req.method==='POST'){
        if(!req.headers['content-type']?.startsWith('application/json'))return send(res,415,{error:'json_required'});
        if(req.headers.origin!==`https://${req.headers.host}`)return send(res,403,{error:'invalid_origin'});
      }
      try{return send(res,200,await playerAdmin.handle(req,url,user.email));}catch(e){return send(res,e.status||503,{error:e.status?e.message:'temporarily_unavailable'});}
    }
    if(importing){
      if(!req.headers['content-type']?.startsWith('application/json'))return send(res,415,{error:'json_required'});
      if(req.headers.origin&&req.headers.origin!==`https://${req.headers.host}`)return send(res,403,{error:'invalid_origin'});
      try{return send(res,200,await importReports(req));}catch(e){return send(res,e.status||503,{error:e.status?e.message:'import_failed'});}
    }
    const to=url.searchParams.get('to')||day(new Date()),from=url.searchParams.get('from')||day(Date.now()-29*86400000);
    if(![from,to].every(d=>/^\d{4}-\d{2}-\d{2}$/.test(d)&&day(d)===d)||from>to||to>day(new Date()))return send(res,400,{error:'invalid_dates'});
    try{send(res,200,url.pathname==='/dashboard/api/activity'?await activity(from,to):await summary(from,to,url.searchParams.get('refresh')==='1'));}catch{send(res,503,{error:'temporarily_unavailable'});}
  }
  function initialize(){db.collection('dashboard_meta').doc('coverage').create({startedAt:new Date().toISOString()}).catch(e=>{if(e.code!==6)failureCount++;});}
  const seen=new Map();
  function presence(uid){if(!uid || (seen.get(uid)||0)>Date.now()-300000)return;seen.set(uid,Date.now());if(seen.size>10000)seen.clear();const id=createHash('sha256').update(uid).digest('hex');db.collection('dashboard_presence').doc(id).set({lastSeen:new Date().toISOString()},{merge:true}).catch(()=>failureCount++);}
  function match(room,training){return activityData.record(room,training).catch(()=>{failureCount++;});}
  return {handle,initialize,presence,match};
}
module.exports={createDashboard};
