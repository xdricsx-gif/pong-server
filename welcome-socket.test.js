'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const {createWelcomeGateway} = require('./welcome-socket');
const fs = require('node:fs'), vm = require('node:vm');
const START = 1792864800000;
function deferred() {let resolve; const promise = new Promise(r => {resolve = r;}); return {promise, resolve};}
const flush = () => new Promise(resolve => setImmediate(resolve));
const result = uid => ({eligible:true, delivered:true, created:false, claimable:true, claimed:false, senderUid:'verified-drics', messageId:'welcome_v1', chatId:uid+'_verified-drics', gold:100});
function fixture(options = {}) {
  let time = START, session = {uid:'verified-player',generation:1,expiresAt:START+3600000,verified:true,anonymous:false};
  const calls = [], handlers = new Map();
  const service = {
    async ensure(uid) {calls.push(['ensure',uid]); return options.ensure ? options.ensure(uid) : result(uid);},
    async claim(uid) {calls.push(['claim',uid]); return options.claim ? options.claim(uid) : {...result(uid),claimed:true,claimable:false,gold:200,grantedGold:100};},
  };
  const gateway = createWelcomeGateway({getService:()=>options.unavailable?null:service,now:()=>time});
  const socket = {connected:true,uid:session.uid,on:(name,fn)=>handlers.set(name,fn)};
  const connection = gateway.attach(socket,{getSession:()=>session});
  async function invoke(name,payload={}) {let ack; await handlers.get(name)(payload,value=>{ack=value;}); return ack;}
  return {gateway,connection,socket,handlers,calls,invoke,getSession:()=>session,setSession(value){session=value;},setTime(value){time=value;},advance(value){time+=value;}};
}

test('raw socket UID and payload token/UID never authorize welcome events', async () => {
  const f=fixture(); f.setSession(null); f.socket.uid='forged-player';
  for(const event of ['welcomeStatus','claimWelcomeReward']) assert.equal((await f.invoke(event,{uid:'victim',idToken:'fake',amount:99999})).error,'auth_required');
  assert.deepEqual(f.calls,[]);
});

test('expired, anonymous and mismatched identities are denied before accessing the service', async () => {
  for(const change of [{expiresAt:START},{verified:false},{anonymous:true},{uid:'other'}]) {
    const f=fixture(); f.setSession({...f.getSession(),...change});
    assert.equal((await f.invoke('claimWelcomeReward')).error,'auth_required'); assert.deepEqual(f.calls,[]);
  }
});

test('trusted events use only verified UID and return authoritative server values', async () => {
  const f=fixture();
  assert.equal((await f.invoke('welcomeStatus',{uid:'victim'})).ok,true);
  const claim=await f.invoke('claimWelcomeReward',{uid:'victim',gold:99999,senderUid:'attacker'});
  assert.equal(claim.gold,200); assert.equal(claim.grantedGold,100);
  assert.deepEqual(f.calls,[['ensure','verified-player'],['claim','verified-player']]);
});

test('login background work and status request share the same pending read without blocking login', async () => {
  const wait=deferred(),f=fixture({ensure:()=>wait.promise});
  assert.equal(f.connection.authenticated(),undefined);
  const status=f.invoke('welcomeStatus'); await flush(); assert.equal(f.calls.length,1);
  wait.resolve(result('verified-player')); assert.equal((await status).ok,true);
});

test('many sockets and repeated auth calls do not multiply background transactions', async () => {
  const f=fixture({ensure:uid=>({...result(uid),delivered:false,eligible:false})});
  for(let n=0;n<100;n++) {
    const socket={connected:true,uid:'verified-player',on(){}};
    f.gateway.attach(socket,{getSession:()=>f.getSession()}).authenticated();
  }
  await flush(); assert.equal(f.calls.length,1);
  f.advance(60000); f.connection.authenticated(); await flush(); assert.equal(f.calls.length,2);
});

test('a known delivered welcome is not re-read on every login', async () => {
  const f=fixture(); f.connection.authenticated(); await flush();
  f.advance(60000); f.connection.authenticated(); await flush(); assert.equal(f.calls.length,1);
  await f.invoke('welcomeStatus'); assert.equal(f.calls.length,2);
});

test('status and claim flood limits are independent and claim balances are never cached', async () => {
  let gold=200; const f=fixture({claim:uid=>({...result(uid),claimed:true,claimable:false,gold,grantedGold:0,alreadyClaimed:true})});
  assert.equal((await f.invoke('welcomeStatus')).ok,true);
  assert.equal((await f.invoke('welcomeStatus')).error,'rate_limit');
  assert.equal((await f.invoke('claimWelcomeReward')).gold,200);
  assert.equal((await f.invoke('claimWelcomeReward')).error,'rate_limit');
  gold=170; f.advance(2000);
  assert.equal((await f.invoke('claimWelcomeReward')).gold,170);
});

test('only one pending claim per UID is permitted across sockets', async () => {
  const wait=deferred(),f=fixture({claim:()=>wait.promise});
  const first=f.invoke('claimWelcomeReward'); await flush();
  assert.equal((await f.invoke('claimWelcomeReward')).error,'busy'); assert.equal(f.calls.length,1);
  wait.resolve({...result('verified-player'),claimed:true,gold:200}); assert.equal((await first).ok,true);
});

test('an account switch while a claim runs never sends the old balance to the new session', async () => {
  const wait=deferred(),f=fixture({claim:()=>wait.promise});
  const request=f.invoke('claimWelcomeReward'); await flush();
  f.setSession({...f.getSession(),uid:'new-player',generation:2}); f.socket.uid='new-player';
  wait.resolve({...result('verified-player'),gold:200}); const ack=await request;
  assert.deepEqual(ack,{ok:false,error:'auth_changed'}); assert.equal('gold' in ack,false);
});

test('disconnect during a request suppresses its acknowledgment', async () => {
  const wait=deferred(),f=fixture({ensure:()=>wait.promise}); const request=f.invoke('welcomeStatus'); await flush();
  f.socket.connected=false; wait.resolve(result('verified-player')); assert.equal(await request,undefined);
});

test('tutorial completion rechecks once if the initial login read was still incomplete', async () => {
  const wait=deferred(); let reads=0;
  const f=fixture({ensure:uid=>(++reads===1?wait.promise:result(uid))});
  f.connection.authenticated(); await flush();
  for(let n=0;n<50;n++) f.connection.afterTutorial('verified-player');
  wait.resolve({...result('verified-player'),eligible:false,delivered:false}); await flush(); await flush();
  assert.equal(f.calls.length,2);
});

test('tutorial hook for a different user cannot cause delivery', async () => {
  const f=fixture(); f.connection.afterTutorial('victim'); await flush(); assert.deepEqual(f.calls,[]);
});

test('background failures are contained and unknown service errors never expose private data', async () => {
  const f=fixture({ensure:()=>{throw new Error('token=secret details');}});
  f.connection.authenticated(); await flush();
  assert.deepEqual(await f.invoke('welcomeStatus'),{ok:false,error:'server_unavailable'});
});

test('events without an ACK do not trigger side effects', async () => {
  const f=fixture(); await f.handlers.get('claimWelcomeReward')({uid:'victim'}); assert.deepEqual(f.calls,[]);
});

function serverFixture() {
  const source=fs.readFileSync(require.resolve('./server'),'utf8');
  const start=source.indexOf('  let shopAuthGeneration=0;'),end=source.indexOf('  // ── Обмін золото',start);
  assert.ok(start>=0&&end>start);
  const handlers=new Map(),auths=[],calls=[],progress=[],acks=[];
  const socket={connected:true,uid:'unverified-matchmaking-user',on:(name,fn)=>handlers.set(name,fn),emit(){}};
  const service={async ensure(uid){calls.push(['ensure',uid]);return result(uid);},async claim(uid){calls.push(['claim',uid]);return {...result(uid),gold:200,claimed:true};}};
  const welcomeGateway=createWelcomeGateway({getService:()=>service});
  const context={socket,welcomeGateway,welcomeEnabled:true,
    resolveUid:args=>new Promise(resolve=>auths.push({args,resolve})),
    footballDashboard:{presence(){}},playerActivity:{authenticate:()=>true},
    require:name=>{assert.equal(name,'./flight-school');return {async saveFlightSchool(db,uid,value){progress.push([uid,value]);return {progress:{completed:true},reward:0,silver:300};}};},db:{},
  };
  vm.runInNewContext(source.slice(start,end),context);
  async function event(name,payload={}) {let ack;await handlers.get(name)(payload,r=>{ack=r;});return ack;}
  async function login(uid,{verified=true,anonymous=false,welcomeVersion=1}={}) {
    const promise=handlers.get('shop:auth')({idToken:'test',uid,welcomeVersion},r=>acks.push(r));
    const job=auths.at(-1);if(verified)job.args.onVerified({uid,anonymous,expiresAt:Date.now()+3600000});job.resolve(uid);await promise;await flush();
  }
  return {source,handlers,auths,calls,progress,acks,socket,event,login};
}

test('server integration derives welcome privilege only from successful verified shop auth', async () => {
  const f=serverFixture(); assert.equal((await f.event('welcomeStatus')).error,'auth_required');
  await f.login('legacy-fallback',{verified:false}); assert.equal(f.acks.at(-1).welcomeVersion,null);
  assert.equal((await f.event('claimWelcomeReward')).error,'auth_required');
  await f.login('guest',{anonymous:true}); assert.equal(f.acks.at(-1).welcomeVersion,null); assert.equal(f.calls.length,0);
  await f.login('verified-user'); assert.equal(f.acks.at(-1).welcomeVersion,1);
  assert.deepEqual(f.calls,[['ensure','verified-user']]);
  const claim=await f.event('claimWelcomeReward',{uid:'victim',amount:9000});
  assert.equal(claim.gold,200); assert.deepEqual(f.calls.at(-1),['claim','verified-user']);
});

test('server integration invalidates the old welcome session immediately when new auth starts', async () => {
  const f=serverFixture(); await f.login('first-user');
  const next=f.handlers.get('shop:auth')({idToken:'next',welcomeVersion:1},()=>{});
  assert.equal((await f.event('claimWelcomeReward')).error,'auth_required');
  const job=f.auths.at(-1);job.args.onVerified({uid:'next-user',anonymous:false,expiresAt:Date.now()+3600000});job.resolve('next-user');await next;await flush();
  assert.equal((await f.event('claimWelcomeReward')).gold,200); assert.deepEqual(f.calls.at(-1),['claim','next-user']);
});

test('server tutorial hook refuses mismatched identity and uses only the active verified user', async () => {
  const f=serverFixture(); await f.login('verified-user');
  const wrong=f.event('tutorial:progress',{idToken:'different-account',version:2,step:6,completed:true});
  f.auths.at(-1).resolve('other-user'); assert.equal((await wrong).error,'auth_changed'); assert.equal(f.progress.length,0);
  const correct=f.event('tutorial:progress',{idToken:'same-account',version:2,step:6,completed:true});
  f.auths.at(-1).resolve('verified-user'); assert.equal((await correct).ok,true);
  assert.equal(f.progress[0][0],'verified-user');
});

test('server uses only the configured sender UID and exposes token expiry to the trusted session', () => {
  const source=fs.readFileSync(require.resolve('./server'),'utf8');
  assert.match(source,/createWelcome\(\{db,senderUid:process\.env\.WELCOME_SENDER_UID,senderNick:'DRICS'\}\)/);
  assert.match(source,/expiresAt: Number\(decoded\.exp\)\*1000/);
  assert.doesNotMatch(source,/senderUid:\s*['"][^'"]+['"]/);
});

test('old clients do not receive a welcome until a verified new client opts in', async () => {
  const f=serverFixture();
  const old=f.handlers.get('shop:auth')({idToken:'old-client'},r=>f.acks.push(r));
  const job=f.auths.at(-1);job.args.onVerified({uid:'legacy-player',anonymous:false,expiresAt:Date.now()+3600000});job.resolve('legacy-player');await old;await flush();
  assert.equal(f.acks.at(-1).welcomeVersion,null);
  assert.equal(f.calls.length,0);
  assert.equal((await f.event('welcomeStatus')).error,'auth_required');
  const completed=f.event('tutorial:progress',{idToken:'old-client',version:2,step:6,completed:true});
  f.auths.at(-1).resolve('legacy-player');assert.equal((await completed).ok,true);await flush();
  assert.equal(f.calls.length,0);
  await f.login('legacy-player',{welcomeVersion:1});
  assert.equal(f.acks.at(-1).welcomeVersion,1);
  assert.equal(f.calls.filter(([operation,uid])=>operation==='ensure'&&uid==='legacy-player').length,1);
});
