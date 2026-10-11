'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),{spawn}=require('node:child_process');
const {io}=require('socket.io-client'),{once}=require('node:events'),P=require('./galaxy-physics');
const {Client}=require('./galaxy-netcode');
const Wire=require('./galaxy-wire');
const wait=ms=>new Promise(r=>setTimeout(r,ms));
async function until(predicate,timeout=6000){const start=Date.now();while(!predicate()){if(Date.now()-start>timeout)throw Error('Condition timed out');await wait(20);}}
test('real server/socket integration: four clients, protocol separation, background resume and reconnect',{timeout:30000},async t=>{
  const port=4437;
  const server=spawn(process.execPath,['--require','./netcode-test-auth.cjs','server.js'],{cwd:__dirname,
    env:{PATH:process.env.PATH,SystemRoot:process.env.SystemRoot,PORT:String(port),NODE_ENV:'test',NETCODE_V2:'1'},stdio:['ignore','pipe','pipe']});
  let output='';server.stdout.on('data',b=>output+=b);server.stderr.on('data',b=>output+=b);
  t.after(()=>server.kill());await until(()=>output.includes('Server on port'),10000);
  assert.ok(!output.includes('Firebase Admin initialized'));
  const sockets=[],states=[],clients=[],errors=[];let loop;
  t.after(()=>{clearInterval(loop);for(const s of sockets)s.disconnect();});
  function connect(index,version=P.VERSION){
    const socket=io('http://127.0.0.1:'+port,{transports:['websocket'],reconnection:false});sockets.push(socket);
    socket.on('connect_error',e=>errors.push(e.message));socket.on('mm:error',e=>errors.push(e));
    socket.on('net:init',data=>{
      const c=new Client({slot:data.slot,matchId:data.matchId,epoch:data.epoch,send:p=>socket.volatile.emit('net:input',Wire.input(p)),requestSync:reset=>socket.emit(reset?'net:resume':'net:sync')});
      c.setRtt(0);c.receive(data.snapshot,performance.now());clients[index]=c;states[index]=data.snapshot;
    });
    socket.on('net:state',raw=>{const p=Wire.readSnapshot(raw,clients[index]?.base?.config);if(p){states[index]=p;clients[index]?.receive(p,performance.now());}});
    return socket;
  }
  const legacy=connect(4,'legacy');await once(legacy,'connect');legacy.emit('mm:join',{nick:'Legacy test',uid:'old',idToken:'local-test:old'});
  const ids=[];
  for(let i=0;i<4;i++){
    const s=connect(i);await once(s,'connect');s.on('mm:joined',x=>ids[i]=x.roomId);
    s.emit('mm:join',{nick:'Local '+i,uid:'u'+i,idToken:'local-test:u'+i,netVersion:P.VERSION});
  }
  await until(()=>clients.slice(0,4).filter(Boolean).length===4);
  assert.equal(new Set(ids).size,1);assert.equal(clients[4],undefined);assert.deepEqual(errors,[]);
  loop=setInterval(()=>{for(const c of clients.slice(0,4))if(c){c.setInput({dx:1,magnet:true});c.advance(performance.now());}},16);
  await until(()=>states[0]?.world.tick>30);assert.ok(states[0].world.paddles.some(x=>x>260));
  const firstEpoch=clients[0].epoch;sockets[1].emit('net:pause');clients[0].suspend();
  await wait(170);sockets[1].emit('net:resume');await until(()=>clients[0].epoch>firstEpoch);
  const matchId=clients[0].matchId,slot=clients[0].slot;
  sockets[1].disconnect();await wait(150);
  const replacement=connect(0),competitor=connect(5);await Promise.all([once(replacement,'connect'),once(competitor,'connect')]);
  const accepted=[],rejected=[];
  for(const s of [replacement,competitor]){
    s.on('net:init',()=>accepted.push(s));s.on('rejoin:fail',()=>rejected.push(s));
    s.emit('rejoin',{roomId:matchId,slot,uid:'u0',idToken:'local-test:u0',nick:'Local 0',netVersion:P.VERSION});
  }
  await until(()=>accepted.length===1&&rejected.length===1);
  assert.notEqual(accepted[0],rejected[0]);
  if(accepted[0]===competitor)clients[0]=clients[5];
  assert.equal(clients[0].matchId,matchId);assert.ok(clients[0].epoch>firstEpoch);
  await wait(300);assert.ok(clients[0].base.connected[slot]);assert.deepEqual(errors,[]);
  assert.ok(!/TICK ERROR|ReferenceError|TypeError/.test(output),output);
});
