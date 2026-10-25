'use strict';
const {createHash}=require('node:crypto');
const {readJson}=require('./push-service');
function createPushHttp({getService,verifyIdToken,now=Date.now}={}){
 const rates=new Map(),origins=new Set(['https://localhost','capacitor://localhost']);
 function permit(key,limit){const at=now(),r=rates.get(key);if(!r||at-r.at>=60000){if(rates.size>=5000)rates.delete(rates.keys().next().value);rates.set(key,{at,n:1});return true;}return ++r.n<=limit;}
 return async function handle(req,res){let url;try{url=new URL(req.url,'http://localhost');}catch{return false;}if(!url.pathname.startsWith('/push/'))return false;
  const headers={'Content-Type':'application/json','Cache-Control':'no-store','X-Content-Type-Options':'nosniff','Vary':'Origin'},origin=req.headers.origin;
  const reply=(status,value)=>{if(!res.writableEnded){res.writeHead(status,headers);res.end(JSON.stringify(value));}};
  if(origin&&!origins.has(origin)){reply(403,{error:'permission'});return true;}if(origin)headers['Access-Control-Allow-Origin']=origin;
  if(req.method==='OPTIONS'){Object.assign(headers,{'Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'Authorization, Content-Type','Access-Control-Max-Age':'600'});reply(204,{});return true;}
  if(req.method!=='POST'){reply(405,{error:'method'});return true;}if(!['/push/register','/push/unregister','/push/event'].includes(url.pathname)){reply(404,{error:'not_found'});return true;}
  if(!permit('ip:'+(req.socket?.remoteAddress||'unknown'),1200)){reply(429,{error:'rate_limit'});return true;}
  try{const service=getService();if(!service)throw Object.assign(Error('unavailable'),{status:503});const body=await readJson(req,12288);if(!body||typeof body!=='object'||Array.isArray(body))throw Object.assign(Error('invalid_request'),{status:400});const header=req.headers.authorization;let uid=null;
   if(header){const token=/^Bearer ([A-Za-z0-9_.-]{20,16000})$/.exec(header)?.[1];if(!token)throw Object.assign(Error('auth_required'),{status:401});const key=createHash('sha256').update(token).digest('hex');if(!permit('token:'+key,120))throw Object.assign(Error('rate_limit'),{status:429});let identity;try{identity=await verifyIdToken(token,true);}catch{throw Object.assign(Error('auth_required'),{status:401});}if(typeof identity?.uid!=='string'||!identity.uid||identity.firebase?.sign_in_provider==='anonymous'||!Number.isFinite(identity.exp)||identity.exp*1000<=now())throw Object.assign(Error('auth_required'),{status:401});uid=identity.uid;if(!permit('uid:'+uid,120))throw Object.assign(Error('rate_limit'),{status:429});}
   if((url.pathname!=='/push/event'||body.type==='login')&&!uid)throw Object.assign(Error('auth_required'),{status:401});
   if(url.pathname==='/push/event'){const scope=createHash('sha256').update(String(body.receiptKey||'')).digest('hex');if(!permit('receipt:'+scope,60))throw Object.assign(Error('rate_limit'),{status:429});}
   const result=url.pathname==='/push/register'?await service.register(uid,body):url.pathname==='/push/unregister'?await service.unregister(uid,body):await service.event(uid,body);reply(200,result);
  }catch(e){reply(e.status||503,{error:e.status?e.message:'temporarily_unavailable'});}return true;
 };
}
module.exports={createPushHttp};
