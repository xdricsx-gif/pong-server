'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {Readable}=require('node:stream');
const {createDashboard}=require('./football-dashboard');
const owner={email:'xdricsx@gmail.com',email_verified:true,firebase:{sign_in_provider:'google.com'}};
function fixture(claims=owner){const calls=[];const dashboard=createDashboard({db:{},credentials:{},verifyIdToken:async()=>claims,live:()=>({}),pushService:{async adminHandle(req,url,actor){calls.push({method:req.method,path:url.pathname,actor});return {ok:true};}}});return {dashboard,calls};}
async function request(f,{method='POST',auth=true,origin='https://dashboard.example',type='application/json',path='/dashboard/api/admin/push/campaigns'}={}){const req=Readable.from([Buffer.from('{}')]);Object.assign(req,{method,url:path,headers:{host:'dashboard.example',...(origin?{origin}:{}),'content-type':type,...(auth?{authorization:'Bearer verified'}:{})},socket:{remoteAddress:'test'}});let status,body,headers;await f.dashboard.handle(req,{writeHead(s,h){status=s;headers=h;},end(value){body=value;}});return {status,body,headers};}
test('push sending inherits owner, verified Google identity, JSON and same-origin protections',async()=>{
 for(const args of [{auth:false},{origin:'https://other.example'},{origin:null},{type:'text/plain'}]){const f=fixture();assert.ok([401,403,415].includes((await request(f,args)).status));assert.equal(f.calls.length,0);}
 for(const claims of [{...owner,email:'other@example.com'},{...owner,email_verified:false},{...owner,firebase:{sign_in_provider:'password'}}]){const f=fixture(claims);assert.equal((await request(f)).status,403);assert.equal(f.calls.length,0);}
 const f=fixture();assert.equal((await request(f)).status,200);assert.deepEqual(f.calls,[{method:'POST',path:'/dashboard/api/admin/push/campaigns',actor:owner.email}]);
});
test('push reports require owner but static presentation has no recipient data',async()=>{
 const f=fixture();assert.equal((await request(f,{method:'GET',path:'/dashboard/api/admin/push/summary',auth:false})).status,401);
 assert.equal((await request(f,{method:'GET',path:'/dashboard/api/admin/push/summary'})).status,200);
 for(const path of ['/dashboard/push.js','/dashboard/push.css','/dashboard/time.js']){const r=await request(f,{method:'GET',path,auth:false});assert.equal(r.status,200);assert.equal(r.headers['Cache-Control'],'no-store');}
 assert.equal((await request(f,{method:'GET',path:'/dashboard/push-service.js',auth:false})).status,404);
});
