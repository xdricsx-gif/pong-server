'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const {createHash}=require('node:crypto');
const {createPushService}=require('./push-service');
const NOW=Date.parse('2026-10-25T10:00:00Z'),DAY=86400000;
const hash=value=>createHash('sha256').update(value).digest('hex');
const clone=value=>structuredClone(value);
const TOKEN='fcm-token-for-testing-only-12345678901234567890';
const INSTALL='installation-test-00000001';
const REQUEST='campaign-request-00000001';
function database(initial={}){
 const data=new Map(Object.entries(initial).map(([key,value])=>[key,clone(value)])),versions=new Map();let beforeCommit=null,failWrites=false;
 function external(key,value){data.set(key,clone(value));versions.set(key,(versions.get(key)||0)+1);}
 function snap(key){const value=clone(data.get(key));return {id:key.split('/').at(-1),ref:doc(key),exists:data.has(key),data:()=>clone(value)};}
 function doc(key){return {id:key.split('/').at(-1),key,collection:name=>query(key+'/'+name),get:async()=>snap(key),set:async(value,options)=>external(key,options?.merge?{...data.get(key),...value}:value)};}
 function query(path,filters=[],order=null,limit=Infinity,cursor=null,selected=null){return {
  doc:id=>doc(path+'/'+id),where:(field,op,value)=>query(path,[...filters,[field,op,value]],order,limit,cursor,selected),orderBy:(field,direction='asc')=>query(path,filters,[field,direction],limit,cursor,selected),limit:n=>query(path,filters,order,n,cursor,selected),startAfter:id=>query(path,filters,order,limit,id,selected),select:(...fields)=>query(path,filters,order,limit,cursor,fields),
  async get(){let rows=[...data].filter(([key])=>key.startsWith(path+'/')&&key.slice(path.length+1).indexOf('/')<0).map(([key])=>snap(key));
   for(const [field,op,value]of filters)rows=rows.filter(d=>op==='in'?value.includes(d.data()[field]):op==='=='?d.data()[field]===value:false);
   const [field,direction]=order||['__name__','asc'];const val=d=>field==='__name__'?d.id:d.data()[field];const compare=(a,b)=>((val(a)<val(b)?-1:val(a)>val(b)?1:0)||(a.id<b.id?-1:a.id>b.id?1:0))*(direction==='desc'?-1:1);rows.sort(compare);if(cursor)rows=rows.filter(d=>typeof cursor==='string'?d.id>cursor:compare(d,cursor)>0);rows=rows.slice(0,limit);
   if(selected)rows=rows.map(d=>({...d,data:()=>Object.fromEntries(selected.filter(k=>d.data()[k]!==undefined).map(k=>[k,d.data()[k]]))}));return {docs:rows,size:rows.length,empty:!rows.length};}
 };}
 const db={collection:name=>query(name),async runTransaction(fn){for(let attempt=0;attempt<100;attempt++){const reads=new Map(),writes=[];let writing=false;const tx={async get(r){assert.equal(writing,false,'read before every write');reads.set(r.key,versions.get(r.key)||0);return snap(r.key);},set(r,value,options){writing=true;writes.push({key:r.key,value:clone(value),merge:options?.merge});},create(r,value){writing=true;writes.push({key:r.key,value:clone(value),create:true});},delete(r){writing=true;writes.push({key:r.key,delete:true});}};const result=await fn(tx);if(beforeCommit){const hook=beforeCommit;beforeCommit=null;await hook();}if([...reads].some(([k,v])=>(versions.get(k)||0)!==v))continue;if(failWrites&&writes.length)throw Error('database_unavailable');for(const w of writes){if(w.create&&data.has(w.key))throw Error('exists');}for(const w of writes){if(w.delete){data.delete(w.key);versions.set(w.key,(versions.get(w.key)||0)+1);}else external(w.key,w.merge?{...data.get(w.key),...w.value}:w.value);}return result;}throw Error('too_many_retries');}};
 return {db,data,external,before(fn){beforeCommit=fn;},fail(value){failWrites=value;}};
}
function fixture(initial={},options={}){let time=NOW;const f=database({'users_public/alice':{nickname:'Alice'},'users_public/bob':{nickname:'Bob'},...initial});const sent=[];const send=async message=>{sent.push(clone(message));return 'fcm-message-id';};const service=createPushService({db:f.db,now:()=>time,send,...options});return {...f,service,sent,advance(ms){time+=ms;},now:()=>time,send};}
const reg=(extra={})=>({installationId:INSTALL,token:TOKEN,permission:'granted',notificationsEnabled:true,channelEnabled:true,appVersion:'20.32',...extra});
const campaignBody=(extra={})=>({requestId:REQUEST,title:'Час до гри',body:'Арена чекає',audience:'all',...extra});
async function launch(f,body={}){await f.service.register('alice',reg());const result=await f.service.createCampaign(campaignBody(body),'owner');await f.service.drain();return result;}
const eventBody=(f,type,index=0)=>({...f.sent[index].data,type});

test('unique players are counted across devices; denied, channel-disabled, stale and banned excluded from send',async()=>{
 const f=fixture();await f.service.register('alice',reg());await f.service.register('alice',reg({installationId:'installation-test-00000002',token:TOKEN+'2'}));await f.service.register('bob',reg({installationId:'installation-test-00000003',token:TOKEN+'3',permission:'denied'}));
 const summary=await f.service.summary();assert.equal(summary.enabledPlayers,1);assert.equal(summary.eligibleDevices,2);assert.equal(summary.disabledPlayers,1);assert.equal(summary.unknownOlderClients,true);
 const preview=await f.service.preview(campaignBody({audience:'selected',uids:['alice','bob']}));assert.deepEqual(preview,{recipientCount:1,deviceCount:2,excludedCount:1,partial:false});
 f.advance(31*DAY);assert.equal((await f.service.summary()).staleEnabledPlayers,1);assert.equal((await f.service.preview(campaignBody())).deviceCount,0);
 await f.service.register('alice',reg({channelEnabled:false}));assert.equal((await f.service.preview(campaignBody())).deviceCount,0);
 f.external('dashboard_admin_players/alice',{banned:true});await assert.rejects(f.service.register('alice',reg()),/permission/);
});

test('registration is tied to verified uid and unregister cannot revoke another user device',async()=>{
 const f=fixture();await f.service.register('alice',{...reg(),uid:'bob'});assert.equal(f.data.get('push_installations/'+hash(INSTALL)).uid,'alice');await assert.rejects(f.service.unregister('bob',{installationId:INSTALL}),/permission/);
 await f.service.unregister('alice',{installationId:INSTALL});assert.equal(f.data.get('push_installations/'+hash(INSTALL)).enabled,false);assert.equal(f.data.has('push_token_owners/'+hash(TOKEN)),false);
});

test('token rotation and account switch keep only one active installation for each FCM token',async()=>{
 const f=fixture();await f.service.register('alice',reg());await f.service.register('bob',reg({installationId:'installation-test-00000002'}));assert.equal(f.data.get('push_installations/'+hash(INSTALL)).enabled,false);assert.equal((await f.service.summary()).enabledPlayers,1);
 await f.service.register('bob',reg({installationId:'installation-test-00000002',token:TOKEN+'new'}));assert.equal(f.data.has('push_token_owners/'+hash(TOKEN)),false);assert.equal(f.data.get('push_token_owners/'+hash(TOKEN+'new')).installation,hash('installation-test-00000002'));
});

test('campaigns require explicit actor, valid bounded audience and nonempty fresh targets',async()=>{
 const f=fixture();await assert.rejects(f.service.createCampaign(campaignBody(),null),/permission/);await assert.rejects(f.service.createCampaign(campaignBody(),'owner'),/empty_audience/);await assert.rejects(f.service.preview(campaignBody({title:' '.repeat(2)})),/invalid_campaign/);await assert.rejects(f.service.preview(campaignBody({audience:'selected',uids:['bad/uid']})),/invalid_audience/);await assert.rejects(f.service.register('alice',reg({token:'bad'})),/invalid_registration/);
});

test('FCM uses data-only high-priority TTL24h; receipt key is hashed durably and never exposed through admin',async()=>{
 const f=fixture();await launch(f);assert.equal(f.sent.length,1);const message=f.sent[0];assert.equal(message.notification,undefined);assert.equal(message.android.priority,'high');assert.equal(message.android.ttl,DAY);assert.equal(message.data.ownerUid,'alice');assert.match(message.data.receiptKey,/^[A-Za-z0-9_-]{43}$/);
 const persisted=JSON.stringify([...f.data]);assert.ok(!persisted.includes(message.data.receiptKey));const detail=await f.service.campaignDetail(REQUEST);assert.equal(detail.metrics.accepted,1);assert.equal(detail.metrics.received,0);assert.equal(detail.recipientCount,1);assert.ok(!JSON.stringify(detail).includes(TOKEN));assert.ok(!JSON.stringify(detail).includes('receiptHash'));
});

test('same idempotency request concurrent or after restart never sends twice; changed request conflicts',async()=>{
 const f=fixture();await f.service.register('alice',reg());await Promise.all(Array.from({length:10},()=>f.service.createCampaign(campaignBody(),'owner')));await f.service.drain();assert.equal(f.sent.length,1);
 const next=createPushService({db:f.db,send:f.send,now:f.now});await next.createCampaign(campaignBody(),'owner');await next.drain();assert.equal(f.sent.length,1);await assert.rejects(next.createCampaign(campaignBody({body:'different'}),'owner'),/request_conflict/);
});

test('two workers racing a queued campaign claim once, with bounded concurrency',async()=>{
 const f=fixture();for(let i=0;i<9;i++)await f.service.register('alice',reg({installationId:INSTALL+'-'+i,token:TOKEN+i}));
 let active=0,max=0;const delayed=async m=>{active++;max=Math.max(max,active);await new Promise(resolve=>setTimeout(resolve,4));f.sent.push(m);active--;};const a=createPushService({db:f.db,send:delayed,now:f.now}),b=createPushService({db:f.db,send:delayed,now:f.now});await Promise.all([a.createCampaign(campaignBody(),'owner'),b.createCampaign(campaignBody(),'owner')]);await Promise.all([a.drain(),b.drain()]);assert.equal(f.sent.length,9);assert.ok(max<=4);const detail=await a.campaignDetail(REQUEST);assert.equal(detail.metrics.accepted,1);assert.equal(detail.deviceCount,9);assert.equal(detail.recipients[0].devices,9);
});

test('crash after attempt claim becomes uncertain on resume and is never resent',async()=>{
 const f=fixture();await launch(f);const key='push_campaigns/'+REQUEST+'/deliveries/'+hash(INSTALL),record=f.data.get(key);f.external(key,{...record,status:'sending',acceptedAt:null});const c=f.data.get('push_campaigns/'+REQUEST);f.external('push_campaigns/'+REQUEST,{...c,status:'sending',leaseOwner:'dead-process',leaseUntil:NOW-1});const resumed=createPushService({db:f.db,send:f.send,now:f.now,setIntervalFn:()=>({unref(){}}),clearIntervalFn:()=>{}});await resumed.resume();await resumed.drain();assert.equal(f.sent.length,1);assert.equal(f.data.get(key).status,'uncertain');assert.equal((await resumed.campaignDetail(REQUEST)).metrics.uncertain,1);await resumed.stop();
});

test('provider timeout is uncertain, not delivered and never blindly retried',async()=>{
 const f=fixture({}, {send:()=>new Promise(()=>{}),sendTimeoutMs:5});await launch(f);const detail=await f.service.campaignDetail(REQUEST);assert.equal(detail.metrics.uncertain,1);assert.equal(detail.metrics.accepted,0);await f.service.createCampaign(campaignBody(),'owner');await f.service.drain();assert.equal(f.data.get('push_campaigns/'+REQUEST+'/deliveries/'+hash(INSTALL)).status,'uncertain');
});

test('invalid FCM token deactivates only that still-current token',async()=>{
 const f=fixture({}, {send:async()=>{throw Object.assign(Error('invalid token'),{code:'messaging/registration-token-not-registered'});}});await launch(f);assert.equal(f.data.get('push_installations/'+hash(INSTALL)).enabled,false);assert.equal((await f.service.campaignDetail(REQUEST)).metrics.failed,1);
});

test('permission revoked or ban applied after audience snapshot prevents sending',async()=>{
 const f=fixture();await f.service.register('alice',reg());await f.service.stop();await f.service.createCampaign(campaignBody(),'owner');f.external('dashboard_admin_players/alice',{banned:true});const s=createPushService({db:f.db,send:f.send,now:f.now});await s.createCampaign(campaignBody(),'owner');await s.drain();assert.equal(f.sent.length,0);assert.equal((await s.campaignDetail(REQUEST)).metrics.skipped,1);
});

test('invalid or other-device receipt cannot forge telemetry; client match is refused',async()=>{
 const f=fixture();await launch(f);await assert.rejects(f.service.event(null,{...eventBody(f,'opened'),receiptKey:'a'.repeat(43)}),/invalid_receipt/);await assert.rejects(f.service.event(null,{...eventBody(f,'received'),installationId:'installation-nonexistent-0001'}),/invalid_receipt/);await assert.rejects(f.service.event(null,eventBody(f,'match')),/invalid_event/);await assert.rejects(f.service.event('bob',eventBody(f,'opened')),/permission/);assert.equal((await f.service.campaignDetail(REQUEST)).metrics.opened,0);
});

test('received and opened are idempotent unique player metrics across multiple devices',async()=>{
 const f=fixture();await f.service.register('alice',reg());await f.service.register('alice',reg({installationId:INSTALL+'-2',token:TOKEN+'2'}));await f.service.createCampaign(campaignBody(),'owner');await f.service.drain();for(let i=0;i<2;i++)await Promise.all(Array.from({length:5},()=>f.service.event(null,eventBody(f,'opened',i))));const detail=await f.service.campaignDetail(REQUEST);assert.equal(detail.metrics.opened,1);assert.equal(detail.metrics.received,1);assert.equal(detail.recipientCount,1);assert.ok(detail.recipients[0].openedAt);
});

test('only login AFTER opening from same verified UID attributes; match requires authoritative hook',async()=>{
 const f=fixture();await launch(f);await assert.rejects(f.service.event('alice',eventBody(f,'login')),e=>e.status===409&&e.message==='open_pending');await f.service.markAuthenticated('alice');assert.equal((await f.service.campaignDetail(REQUEST)).metrics.login,0);
 await f.service.event(null,eventBody(f,'opened'));await assert.rejects(f.service.event(null,eventBody(f,'login')),/auth_required/);await assert.rejects(f.service.event('bob',eventBody(f,'login')),/permission/);f.advance(1000);await f.service.event('alice',eventBody(f,'login'));await f.service.recordMatch('bob');assert.equal((await f.service.campaignDetail(REQUEST)).metrics.match,0);await f.service.recordMatch('alice');await f.service.recordMatch('alice');const detail=await f.service.campaignDetail(REQUEST);assert.equal(detail.metrics.login,1);assert.equal(detail.metrics.match,1);assert.ok(detail.recipients[0].lastLoginAt);
});

test('restart preserves attribution, counts and current login date; only latest tap wins',async()=>{
 const f=fixture();await launch(f);await f.service.event(null,eventBody(f,'opened'));f.advance(2000);await f.service.createCampaign(campaignBody({requestId:REQUEST+'-two'}),'owner');await f.service.drain();await f.service.event(null,eventBody(f,'opened',1));const reopened=createPushService({db:f.db,send:f.send,now:f.now});f.advance(1000);await reopened.markAuthenticated('alice');await reopened.recordMatch('alice');assert.equal((await reopened.campaignDetail(REQUEST)).metrics.login,0);const second=await reopened.campaignDetail(REQUEST+'-two');assert.equal(second.metrics.login,1);assert.equal(second.metrics.match,1);assert.equal(second.recipients[0].lastLoginAt,new Date(f.now()).toISOString());
});

test('late or repeated open cannot extend24h conversion window',async()=>{
 const f=fixture();await launch(f);await f.service.event(null,eventBody(f,'opened'));const opened=f.data.get('push_attribution/'+hash('alice')).openedAt;f.advance(DAY+1);await assert.rejects(f.service.event(null,eventBody(f,'opened')),/receipt_expired/);assert.equal(f.data.get('push_attribution/'+hash('alice')).openedAt,opened);assert.equal((await f.service.event('alice',eventBody(f,'login'))).attributed,false);await f.service.markAuthenticated('alice');await f.service.recordMatch('alice');const detail=await f.service.campaignDetail(REQUEST);assert.equal(detail.metrics.login,0);assert.equal(detail.metrics.match,0);
});

test('receipt cannot follow installation reassignment into another account',async()=>{
 const f=fixture();await launch(f);await f.service.register('bob',reg());await assert.rejects(f.service.event(null,eventBody(f,'opened')),/permission/);assert.equal((await f.service.campaignDetail(REQUEST)).metrics.opened,0);
});

test('recipient metrics and receipt commit atomically under retry',async()=>{
 const f=fixture();await launch(f);f.fail(true);await assert.rejects(f.service.event(null,eventBody(f,'opened')),/database_unavailable/);f.fail(false);assert.equal((await f.service.campaignDetail(REQUEST)).metrics.opened,0);await f.service.event(null,eventBody(f,'opened'));await f.service.event(null,eventBody(f,'opened'));assert.equal((await f.service.campaignDetail(REQUEST)).metrics.opened,1);
});

test('selected audience does not silently broaden and target bound fails before campaign persists',async()=>{
 const f=fixture({}, {maxDevices:1});await f.service.register('alice',reg());await f.service.register('bob',reg({installationId:INSTALL+'-bob',token:TOKEN+'bob'}));await assert.rejects(f.service.createCampaign(campaignBody(),'owner'),/audience_limit/);assert.equal(f.data.has('push_campaigns/'+REQUEST),false);await f.service.createCampaign(campaignBody({audience:'selected',uids:['bob']}),'owner');await f.service.drain();assert.equal(f.sent.length,1);assert.equal(f.sent[0].data.ownerUid,'bob');
});

test('pending login from a previously opened device cannot steal attribution from a newer campaign tap',async()=>{
 const f=fixture();await launch(f);await f.service.event(null,eventBody(f,'opened'));f.advance(1000);await f.service.register('alice',reg({installationId:INSTALL+'-second-device',token:TOKEN+'-second'}));await f.service.createCampaign(campaignBody({requestId:REQUEST+'-new'}),'owner');await f.service.drain();const newer=f.sent.findIndex(m=>m.data.campaignId===REQUEST+'-new'&&m.data.installationId!==INSTALL);await f.service.event(null,eventBody(f,'opened',newer));assert.equal((await f.service.event('alice',eventBody(f,'login',0))).attributed,false);assert.equal((await f.service.event('alice',eventBody(f,'login',newer))).attributed,true);assert.equal((await f.service.campaignDetail(REQUEST)).metrics.login,0);assert.equal((await f.service.campaignDetail(REQUEST+'-new')).metrics.login,1);
});

test('all campaign history remains accessible through stable cursors even for identical timestamps',async()=>{
 const f=fixture();for(let i=0;i<45;i++){const id='campaign-history-'+String(i).padStart(8,'0');f.external('push_campaigns/'+id,{id,title:'History',body:'Record',audience:'all',createdAt:NOW,status:'complete',recipientCount:1,deviceCount:1});}
 const seen=[];let cursor=null;do{const url=new URL('https://example.test/dashboard/api/admin/push/campaigns');if(cursor)url.searchParams.set('cursor',cursor);const page=await f.service.listCampaigns(url);seen.push(...page.rows.map(row=>row.id));cursor=page.cursor;}while(cursor);assert.equal(seen.length,45);assert.equal(new Set(seen).size,45);await assert.rejects(f.service.listCampaigns(new URL('https://example.test/?cursor=invalid')),/invalid_cursor/);
});

test('an invalid old token response cannot disable a newer registration that rotated during send',async()=>{
 let f;f=fixture({}, {send:async()=>{await f.service.register('alice',reg({token:TOKEN+'-rotated'}));throw Object.assign(Error('old token invalid'),{code:'messaging/registration-token-not-registered'});}});await launch(f);const device=f.data.get('push_installations/'+hash(INSTALL));assert.equal(device.enabled,true);assert.equal(device.tokenHash,hash(TOKEN+'-rotated'));assert.equal((await f.service.summary()).freshEnabledPlayers,1);
});

test('transient provider errors remain uncertain instead of claiming definitive delivery failure',async()=>{
 const f=fixture({}, {send:async()=>{throw Object.assign(Error('FCM unavailable'),{code:'messaging/server-unavailable'});}});await launch(f);const detail=await f.service.campaignDetail(REQUEST);assert.equal(detail.metrics.uncertain,1);assert.equal(detail.metrics.failed,0);assert.equal(f.data.get('push_installations/'+hash(INSTALL)).enabled,true);
});

test('failed authenticated-login persistence can retry immediately without losing last login or attribution',async()=>{
 const f=fixture();await launch(f);await f.service.event(null,eventBody(f,'opened'));f.fail(true);await assert.rejects(f.service.markAuthenticated('alice'),/database_unavailable/);f.fail(false);const retry=await f.service.markAuthenticated('alice');assert.equal(retry.throttled,undefined);assert.equal(retry.attributed,true);assert.equal(f.data.get('push_players/'+hash('alice')).lastLoginAt,NOW);assert.equal((await f.service.campaignDetail(REQUEST)).metrics.login,1);
});

test('late failure of an older login cannot remove the throttle for a newer successful login',async()=>{
 const f=fixture();let rejectOlder,first=true;const db={...f.db,runTransaction(fn){if(first){first=false;return new Promise((_,reject)=>{rejectOlder=reject;});}return f.db.runTransaction(fn);}};const s=createPushService({db,send:f.send,now:f.now});const older=s.markAuthenticated('alice');f.advance(60001);await s.markAuthenticated('alice');rejectOlder(Error('old_write_failed'));await assert.rejects(older,/old_write_failed/);const retry=await s.markAuthenticated('alice');assert.equal(retry.throttled,true);assert.equal(f.data.get('push_players/'+hash('alice')).lastLoginAt,f.now());
});
