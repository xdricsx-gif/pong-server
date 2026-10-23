'use strict';
const {createHash}=require('node:crypto');
// Exchange only a verified Firebase session for the same UID. No user-supplied UID or claims.
function createAuthSession({getAuth,getDb,now=Date.now}){
 const rates=new Map(),allowed=new Set(['https://localhost','capacitor://localhost']);
 function permit(key){const t=now(),old=rates.get(key);if(!old||t-old.at>60000){if(rates.size>=2000)rates.delete(rates.keys().next().value);rates.set(key,{at:t,n:1});return true;}return ++old.n<=12;}
 return async function handle(req,res){
  if(req.url!=='/auth/session')return false;
  const origin=req.headers.origin;
  const headers={'Content-Type':'application/json','Cache-Control':'no-store','Pragma':'no-cache','X-Content-Type-Options':'nosniff','Vary':'Origin'};
  if(origin&&allowed.has(origin))headers['Access-Control-Allow-Origin']=origin;
  const reply=(status,value)=>{if(!res.writableEnded){res.writeHead(status,headers);res.end(JSON.stringify(value));}};
  if(origin&&!allowed.has(origin)){reply(403,{error:'permission'});return true;}
  if(req.method==='OPTIONS'){Object.assign(headers,{'Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'Authorization, Content-Type','Access-Control-Max-Age':'600'});reply(204,{});return true;}
  if(req.method!=='POST'){reply(405,{error:'method'});return true;}
  // Body is unused and bounded; credentials travel only in the authorization header.
  if(Number(req.headers['content-length']||0)>1024||req.headers['transfer-encoding']){reply(413,{error:'invalid_request'});return true;}
  req.resume();
  const auth=getAuth();if(!auth){reply(503,{error:'unavailable'});return true;}
  const token=/^Bearer ([A-Za-z0-9_.-]{20,16000})$/.exec(req.headers.authorization||'')?.[1];
  if(!token){reply(401,{error:'auth_required'});return true;}
  if(!permit('token:'+createHash('sha256').update(token).digest('hex'))){reply(429,{error:'rate_limit'});return true;}
  try{
   const identity=await auth.verifyIdToken(token,true);
   if(identity.firebase?.sign_in_provider!=='playgames.google.com'){reply(403,{error:'permission'});return true;}
   if(!identity.uid||typeof identity.uid!=='string')throw Error('invalid_identity');
   if(!permit('uid:'+identity.uid)){reply(429,{error:'rate_limit'});return true;}
   const db=getDb();if(!db){reply(503,{error:'unavailable'});return true;}
   if((await db.collection('dashboard_admin_players').doc(identity.uid).get()).data()?.banned){reply(403,{error:'permission'});return true;}
   const customToken=await auth.createCustomToken(identity.uid);
   reply(200,{token:customToken});
  }catch{reply(401,{error:'auth_required'});}
  return true;
 };
}
module.exports={createAuthSession};
