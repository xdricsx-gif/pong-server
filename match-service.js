'use strict';
const {randomUUID}=require('node:crypto');
const P=require('./galaxy-physics'),Rules=require('./match-rules'),{createLedger}=require('./match-ledger');
const ERRORS={update_required:'Онови гру в Google Play, щоб грати в нових режимах.',auth_required:'Увійди в акаунт повторно.',already_playing:'Спочатку заверши поточний матч або пошук.',not_enough_silver:'Для цього бою потрібно 100 срібла в кожного учасника.',not_enough_energy:'Для тренування потрібно 20 енергії.',offline:'Гравець зараз не в мережі.',busy:'Гравець уже в матчі або пошуку.',invite_pending:'Запрошення цьому гравцю вже надіслано.',invite_rate_limit:'Забагато запрошень. Зачекай хвилину.',invite_expired:'Запрошення вже неактивне.',server_unavailable:'Сервер тимчасово недоступний. Спробуй ще раз.',cancelled_before_start:'Матч скасовано до початку.',wrong_uid:'Це запрошення або матч належить іншому гравцю.'};
function createMatchService({io,rooms,createRoom,setPlayer,clearPlayer,getPlayer,startGame,net,getDb,authenticate,loadStats,onSettled=()=>{},now=Date.now,graceMs=30000,rankedWaitMs=10000}){
  const owner=randomUUID(),online=new Map(),profiles=new Map(),inMatch=new Map(),queued=new Map(),queues={ranked:[],team:[]},invites=new Map();
  let ledger,chain=Promise.resolve(),closed=false;
  const store=()=>{if(!getDb())fail('server_unavailable');return ledger||(ledger=createLedger({db:getDb(),owner,now}));};
  const socketFor=uid=>{const s=online.get(uid);return s?.connected?s:null;};
  const busy=uid=>inMatch.has(uid)||queued.has(uid);
  const error=(socket,e)=>{const code=e.code||e.message,message=ERRORS[code]||'Не вдалося виконати дію. Спробуй ще раз.';
    socket?.emit('match:error',{code,message});
    // Older releases only listen to mm:error. Never leave them searching forever.
    if(code==='update_required')socket?.emit('mm:error',message);
  };
  const serial=(s,fn)=>{const p=chain.then(fn);chain=p.catch(e=>error(s,e));return chain;};
  const fail=code=>{throw Object.assign(new Error(code),{code});};
  async function sendResult(uid,result){const s=socketFor(uid);if(!s)return;
    // A historical receipt must never roll the displayed balance back to settlement time.
    const wallet=await store().wallet(uid);if(socketFor(uid)!==s)return;
    const {totals,participants,...publicResult}=result;s.emit('match:result',{...publicResult,wallet,mySlot:participants?.find(p=>p.uid===uid)?.slot??Object.values(rooms.get(result.roomId)?.roster||{}).find(p=>p.uid===uid)?.slot});
  }
  async function recoverAndNotify(uid,id){const result=await store().recover(uid,id);if(result){await sendResult(uid,result);return true;}
    const pending=await store().pending(uid);if(pending){socketFor(uid)?.emit('match:recovering',pending);return true;}return false;
  }
  async function auth(s,data={}){
    if(data.netVersion!==P.VERSION)fail('update_required');
    const uid=await authenticate({idToken:data.idToken,fallbackUid:null});if(!uid)fail('auth_required');if(!s.connected)return null;
    const db=getDb();if(!db)fail('server_unavailable');
    const snap=await db.collection('users_public').doc(uid).get();if(!snap.exists)fail('account_missing');const u=snap.data();
    if(!s.connected)return null;
    const p={uid,nick:String(u.nickname||'Гравець').slice(0,32),rating:u.rating??500,teamRating:u.teamRating??500,wins:u.wins||0,games:u.gamesPlayed||0,avatarId:u.avatarId||0,paddleStats:await loadStats(uid)};
    if(!s.connected)return null;s.uid=uid;s.matchAuthenticated=true;online.set(uid,s);profiles.set(uid,p);return p;
  }
  function ensure(s){if(!s.matchAuthenticated||online.get(s.uid)!==s)fail('auth_required');return s.uid;}
  function cancelQueue(uid){const group=queued.get(uid);if(!group)return;
    clearTimeout(group.timer);
    queues[group.mode]=queues[group.mode].filter(g=>g!==group);for(const id of group.uids){queued.delete(id);socketFor(id)?.emit('match:queue_cancelled');}
  }
  function cancelInvites(uid){for(const [id,v] of invites)if(v.from===uid||v.to===uid){invites.delete(id);socketFor(v.from)?.emit('match:invite_closed',{id});socketFor(v.to)?.emit('match:invite_closed',{id});}}
  async function enqueue(mode,uids){
    for(const uid of uids){if(!socketFor(uid))fail('offline');if(busy(uid))fail('busy');}
    const group={mode,uids};for(const uid of uids){queued.set(uid,group);cancelInvites(uid);socketFor(uid).emit('match:queued',{mode,count:uids.length});}
    queues[mode].push(group);
    if(mode==='ranked'){
      // The oldest waiting group keeps its deadline when more players arrive.
      // Missing slots are filled by the existing ranked bots in startGame.
      group.timer=setTimeout(()=>serial(null,async()=>{
        if(closed||!queues.ranked.includes(group))return;
        const selected=[];let count=0;
        for(const g of queues.ranked){if(count+g.uids.length<=4){selected.push(g);count+=g.uids.length;}if(count===4)break;}
        if(count)await createMatch('ranked',selected.flatMap(g=>g.uids),selected);
      }),rankedWaitMs);group.timer.unref?.();
      while(queues.ranked.reduce((n,g)=>n+g.uids.length,0)>=4){let selected=[],count=0;
        for(const g of queues.ranked){if(count+g.uids.length<=4){selected.push(g);count+=g.uids.length;}if(count===4)break;}
        if(count!==4)break;await createMatch(mode,selected.flatMap(g=>g.uids),selected);
      }
    }else {
      while(queues.team.reduce((n,g)=>n+g.uids.length,0)>=4){const groups=queues.team.slice().sort((a,b)=>b.uids.length-a.uids.length),pairs=[];
        while(groups.length&&pairs.length<2){const g=groups.shift();if(g.uids.length===2)pairs.push({uids:g.uids,groups:[g]});
          else {const i=groups.findIndex(x=>x.uids.length===1);if(i<0)break;const other=groups.splice(i,1)[0];pairs.push({uids:[...g.uids,...other.uids],groups:[g,other]});}}
        if(pairs.length!==2)break;
        // Canonical sides: first allied pair at bottom/right, second at top/left.
        await createMatch(mode,[pairs[0].uids[0],pairs[1].uids[0],pairs[1].uids[1],pairs[0].uids[1]],pairs.flatMap(p=>p.groups));
      }
    }
  }
  async function createMatch(mode,uids,groups=[]){
    for(const uid of uids)if(!socketFor(uid)||inMatch.has(uid))fail('busy');
    for(const g of groups){clearTimeout(g.timer);queues[mode]=queues[mode].filter(x=>x!==g);for(const uid of g.uids)queued.delete(uid);}
    const room=createRoom('m_'+randomUUID());room.managed=true;room.mode=mode;room.protocol=2;room.roster={};room.lease={};room.status='reserving';room.onEliminated=(world,event)=>serial(null,()=>depart(room,world,event));
    for(let slot=0;slot<uids.length;slot++){const uid=uids[slot];room.roster[slot]={...profiles.get(uid),slot};cancelInvites(uid);}
    for(const uid of uids)inMatch.set(uid,room);rooms.set(room.id,room);
    try{
      // Invitations and waiting groups can outlive a hangar change. Freeze fresh
      // authoritative equipment for every participant before any entry fee.
      let refreshed;
      try{
        refreshed=await Promise.all(uids.map(uid=>loadStats(uid,{strict:true})));
        if(refreshed.some(stats=>!stats||typeof stats!=='object'||Array.isArray(stats)))fail('server_unavailable');
      }catch(_){fail('server_unavailable');}
      if(room.cancelled)fail('cancelled_before_start');
      if(uids.some(uid=>!socketFor(uid)))fail('offline');
      for(let slot=0;slot<uids.length;slot++){
        const uid=uids[slot],profile={...profiles.get(uid),paddleStats:P.clone(refreshed[slot])};
        profiles.set(uid,profile);room.roster[slot]={...profile,slot};
      }
      const roster=Object.values(room.roster).map(p=>({uid:p.uid,slot:p.slot,nick:p.nick}));
      await store().reserve(room.id,mode,roster);room.reserved=true;
      if(room.cancelled||uids.some(uid=>!socketFor(uid))){await abort(room,'cancelled_before_start');return;}
      for(const p of Object.values(room.roster)){const s=socketFor(p.uid);setPlayer(room,s.id,{...p});s.join(room.id);s.emit('mm:joined',{mySlot:p.slot,roomId:room.id});}
      startGame(room);
    }catch(e){
      if(room.reserved){await abort(room,'start_failed');}
      else {rooms.delete(room.id);for(const uid of uids){inMatch.delete(uid);error(socketFor(uid),e);}}
    }
  }
  function info(room){return {mode:room.mode,activeSlots:Rules.get(room.mode).slots,teams:Rules.get(room.mode).teams||null,terms:Rules.terms(room.mode)};}
  function players(room){return {...Object.fromEntries(Object.entries(room.bots||{}).map(([s,b])=>[s,{nick:b.nick,isBot:true}])),...Object.fromEntries(Object.values(room.roster).map(p=>[p.slot,{nick:p.nick,rating:room.mode==='team'?p.teamRating:p.rating,avatarId:p.avatarId||0}]))};}
  function publicResult(room,world,reason){
    const draw=world.winner===null,aborted=!!reason||world.endReason==='abandoned',rewards={};
    for(const p of Object.values(room.roster))rewards[p.slot]=Rules.reward(room.mode,world,p.slot);
    return {roomId:room.id,mode:room.mode,winnerSlot:world.winner,winnerTeam:world.winnerTeam??null,places:Rules.places(world),players:players(room),participants:Object.values(room.roster).map(p=>({uid:p.uid,slot:p.slot})),scores:world.scores,lives:world.lives,teamScores:world.teamScores||null,draw,aborted,reason:reason||world.endReason,rewards,serverCommitted:false};
  }
  async function finish(room){
    if(room.finishing)return;room.finishing=true;room.status='settling';
    for(const l of Object.values(room.lease))clearTimeout(l.timer);
    room.result=publicResult(room,room.game);
    io.to(room.id).emit('match:settling',{roomId:room.id});await persist(room);
  }
  async function persist(room){
    try{await store().recordOutcome(room.id,room.result);const result=await store().settle(room.id);
      room.status='finished';room.result=result;
      if(!room.analyticsRecorded){room.analyticsRecorded=true;try{onSettled(room,result);}catch(e){console.error('[match analytics]',e.message);}}
      for(const p of Object.values(room.roster))if(inMatch.get(p.uid)===room)inMatch.delete(p.uid);
      await Promise.all(Object.values(room.roster).filter(p=>(!room.departed?.has(p.uid)||room.mode==='team')&&!inMatch.has(p.uid)).map(p=>sendResult(p.uid,result)));
      const timer=setTimeout(()=>rooms.delete(room.id),30000);timer.unref?.();
    }catch(e){
      io.to(room.id).emit('match:settling',{roomId:room.id,retrying:true});
      const timer=setTimeout(()=>{if(!closed)persist(room);},5000);timer.unref?.();room.retry=timer;
    }
  }
  async function abort(room,reason){
    if(room.status==='finished'||room.finishing)return;net.close?.(room);room.finishing=true;room.status='settling';
    for(const l of Object.values(room.lease))clearTimeout(l.timer);
    const world=P.create({mode:room.mode});world.winner=null;world.gameOver=true;world.endReason='abandoned';
    room.result=publicResult(room,world,reason);await persist(room);
  }
  async function depart(room,world,event){
    const p=room.roster[event.slot];if(!p||room.departed?.has(p.uid))return;
    // Only committed elimination may release a profile; speculative rollback cannot.
    if(!world.eliminated[p.slot]||world.gameOver||world.config.activeSlots.every(s=>!world.connected[s]))return;
    try{
      if(room.mode==='team')await store().release(p.uid,room.id);
      else {
        const rule=Rules.get(room.mode),place=rule.slots.length-1-world.eliminationOrder.indexOf(p.slot);
        if(place<0||place>=rule.slots.length)throw Error('invalid_place');
        const result={...publicResult(room,world),partial:true,places:[p.slot],draw:false,aborted:false,reason:event.reason||'eliminated',
          rewards:{[p.slot]:{place,draw:false,stake:rule.stake,silver:rule.payouts[place],xp:rule.xp[place],ratingDelta:rule.rating[place]}},
          participants:[{uid:p.uid,slot:p.slot}]};
        const paid=await store().settle(room.id,p.uid,result);
        if(inMatch.get(p.uid)===room)await sendResult(p.uid,paid);
      }
      if(!room.departed)room.departed=new Set();room.departed.add(p.uid);
      if(inMatch.get(p.uid)===room)inMatch.delete(p.uid);
      const s=socketFor(p.uid);if(s){s.leave(room.id);clearPlayer(room,s.id);
        s.emit('match:released',{roomId:room.id,teamPending:room.mode==='team'});}
    }catch(e){const timer=setTimeout(()=>{if(!closed)serial(null,()=>depart(room,world,event));},1000);timer.unref?.();}
  }
  function expire(room,slot){const l=room.lease[slot];if(!l||l.expired||room.status!=='playing')return;
    l.expired=true; // Claim timeout synchronously before queueing a worker event.
    room.net2.authority.system({type:'eliminate',slot,reason:l.voluntary?'leave':'timeout'});
  }
  function pause(uid,voluntary=false){const room=inMatch.get(uid);if(!room)return;const p=Object.values(room.roster).find(p=>p.uid===uid);if(!p)return;
    if(room.status==='reserving'){room.cancelled=true;return;}
    if(room.status!=='playing')return;
    let l=room.lease[p.slot];
    if(!l){l=room.lease[p.slot]={deadline:now()+graceMs,expired:false,voluntary};room.net2.authority.disconnect(p.slot,false);
      l.timer=setTimeout(()=>expire(room,p.slot),graceMs);l.timer.unref?.();}
    if(voluntary){l.voluntary=true;clearTimeout(l.timer);expire(room,p.slot);}
  }
  async function resume(s,id){
    const uid=ensure(s);let room=inMatch.get(uid);
    if(id&&room&&room.id!==id)fail('wrong_uid');
    if(!room){if(!await recoverAndNotify(uid,id))s.emit('rejoin:fail',{reason:'room_gone'});return;}
    if(room.status==='settling'){s.emit('match:settling',{roomId:room.id});return;}
    if(room.status!=='playing')return;
    const p=Object.values(room.roster).find(p=>p.uid===uid),l=room.lease[p.slot];
    if(l&&(l.expired||l.voluntary||now()>=l.deadline)){expire(room,p.slot);s.join(room.id);s.emit('match:waiting_result',{roomId:room.id});return;}
    const restoreView=!!l||room.players[s.id]?.uid!==uid;
    if(l){clearTimeout(l.timer);delete room.lease[p.slot];}
    for(const [sid,other] of Object.entries(room.players))if(other.uid===uid&&sid!==s.id){io.sockets.sockets.get(sid)?.leave(room.id);clearPlayer(room,sid);}
    setPlayer(room,s.id,{...p});s.join(room.id);room.net2.authority.connect(p.slot);
    if(restoreView)s.emit('rejoin:game',{mySlot:p.slot,roomId:room.id,players:players(room),paddleVisuals:room.paddleVisuals,netVersion:P.VERSION,matchInfo:info(room)});net.init(room,s,p.slot);
  }
  function attach(s){
    for(const event of ['mm:join','mm:cancel','leave_game','disconnect','rejoin','net:input','net:sync','net:pause','net:resume','shop:resolve_pending'])s.removeAllListeners(event);
    const on=(event,fn)=>s.on(event,data=>serial(s,()=>fn(data||{})));
    on('match:auth',async data=>{const p=await auth(s,data);if(!p)return;s.emit('match:ready',{teamRating:p.teamRating});
      if(inMatch.get(p.uid)?.status==='playing'){await resume(s);return;}
      await recoverAndNotify(p.uid);
    });
    on('mm:join',async data=>{const p=await auth(s,data);if(!p)return;if(busy(p.uid))fail('already_playing');const mode=data.mode||'ranked';Rules.get(mode);
      if(mode==='duel')fail('invite_required');
      await store().recover(p.uid);if(mode==='training')await createMatch(mode,[p.uid]);else await enqueue(mode,[p.uid]);
    });
    on('match:invite',async data=>{const uid=ensure(s);if(!['ranked','team','duel'].includes(data.mode))fail('invalid_mode');
      if(typeof data.toUid!=='string'||data.toUid===uid)fail('wrong_uid');if(busy(uid))fail('already_playing');if(busy(data.toUid))fail('busy');
      if(!socketFor(data.toUid))fail('offline');
      const recent=(s.inviteTimes||[]).filter(t=>t>now()-60000);if(recent.length>=5)fail('invite_rate_limit');s.inviteTimes=[...recent,now()];
      if([...invites.values()].some(v=>(v.from===uid&&v.to===data.toUid)||(v.to===uid&&v.from===data.toUid)))fail('invite_pending');
      const id=randomUUID(),v={id,from:uid,to:data.toUid,mode:data.mode,expiresAt:now()+30000};invites.set(id,v);
      socketFor(v.to).emit('match:invite_received',{...v,nick:profiles.get(uid).nick,terms:Rules.terms(v.mode)});s.emit('match:invite_sent',{id,nick:profiles.get(v.to).nick});
      const timer=setTimeout(()=>{if(invites.delete(id)){socketFor(v.from)?.emit('match:invite_closed',{id});socketFor(v.to)?.emit('match:invite_closed',{id});}},30000);timer.unref?.();
    });
    on('match:invite_answer',async data=>{const uid=ensure(s),v=invites.get(data.id);if(!v||now()>=v.expiresAt)fail('invite_expired');if(v.to!==uid)fail('wrong_uid');
      invites.delete(v.id);socketFor(v.from)?.emit('match:invite_closed',{id:v.id});s.emit('match:invite_closed',{id:v.id});
      if(data.accept!==true)return;
      for(const id of [v.from,v.to]){if(!socketFor(id))fail('offline');if(busy(id))fail('busy');}
      if(v.mode==='duel')await createMatch('duel',[v.from,v.to]);else await enqueue(v.mode,[v.from,v.to]);
    });
    on('rejoin',async data=>{if(!s.matchAuthenticated)await auth(s,data);await resume(s,data.roomId);});
    on('shop:resolve_pending',async data=>{const uid=ensure(s);await recoverAndNotify(uid,data.roomId);});
    s.on('net:input',packet=>{if(online.get(s.uid)!==s)return;net.input(inMatch.get(s.uid),s,packet);});
    s.on('net:sync',()=>{const r=inMatch.get(s.uid),p=r&&Object.values(r.roster).find(p=>p.uid===s.uid);if(p&&online.get(s.uid)===s)net.sync(r,s,p.slot);});
    on('net:resume',()=>resume(s));
    s.on('net:pause',()=>{if(online.get(s.uid)===s)pause(s.uid);});
    s.on('mm:cancel',()=>{cancelQueue(s.uid);pause(s.uid,true);});
    s.on('leave_game',()=>{cancelQueue(s.uid);pause(s.uid,true);});
    s.on('disconnect',()=>{if(online.get(s.uid)!==s)return;online.delete(s.uid);cancelQueue(s.uid);cancelInvites(s.uid);pause(s.uid);});
  }
  const heartbeat=setInterval(()=>{
    if(!getDb())return;
    const ids=[...rooms.values()].filter(r=>r.managed&&r.status==='playing').map(r=>r.id);
    store().renew(ids).then(()=>store().sweep()).catch(e=>console.error('[match ledger]',e.code||e.message));
  },15000);heartbeat.unref?.();
  return {attach,finish,abort,info,close(){closed=true;clearInterval(heartbeat);for(const group of new Set(queued.values()))clearTimeout(group.timer);for(const room of rooms.values()){for(const l of Object.values(room.lease||{}))clearTimeout(l.timer);clearTimeout(room.retry);}},_state:{online,profiles,inMatch,queued,queues,invites},_expire:expire};
}
module.exports={createMatchService};
