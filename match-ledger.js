'use strict';
const Rules=require('./match-rules');
const fail=code=>Object.assign(new Error(code),{code});
const number=(n,fallback=0)=>Number.isFinite(n)?n:fallback;
// All reads precede writes. The session receipt, wallet changes, statistics and
// per-player history commit in one transaction; no client-reported outcome is trusted.
function createLedger({db,owner,now=Date.now}){
  const session=id=>db.collection('match_sessions').doc(id);
  const priv=uid=>db.collection('users_private').doc(uid);
  const pub=uid=>db.collection('users_public').doc(uid);
  function available(){if(!db)throw fail('server_unavailable');}
  async function reserve(id,mode,roster){available();const rule=Rules.get(mode);
    if(!roster.length||new Set(roster.map(p=>p.uid)).size!==roster.length||new Set(roster.map(p=>p.slot)).size!==roster.length||roster.some(p=>typeof p.uid!=='string'||!p.uid||!rule.slots.includes(p.slot)))throw fail('invalid_roster');
    // Ranked bots have no account, entry debit or payout. Human prizes stay fixed
    // at 250 / 150 / 0 / 0 even when fewer than four humans fund the match.
    if(mode==='ranked'&&roster.length>4||mode==='team'&&roster.length!==4||mode==='duel'&&roster.length!==2||mode==='training'&&roster.length!==1)throw fail('invalid_roster');
    return db.runTransaction(async tx=>{
      const ref=session(id),old=await tx.get(ref);if(old.exists){const d=old.data();
        if(d.mode!==mode||JSON.stringify(d.roster)!==JSON.stringify(roster))throw fail('match_conflict');return d;}
      const wallets=[],profiles=[];for(const p of roster){wallets.push(await tx.get(priv(p.uid)));profiles.push(await tx.get(pub(p.uid)));}
      for(let i=0;i<roster.length;i++){
        if(!wallets[i].exists||!profiles[i].exists)throw fail('account_missing');
        const w=wallets[i].data();if(w.activeMatchId)throw fail('already_playing');
        if(number(w.silver)<rule.stake)throw fail('not_enough_silver');
        const energy=Math.min(100,number(w.energy,100)+Math.floor(Math.max(0,now()-number(w.energyLastRegen,now()))/90000));
        if(energy<(rule.energy||0))throw fail('not_enough_energy');
      }
      for(let i=0;i<roster.length;i++){
        const w=wallets[i].data(),update={activeMatchId:id,lastMatchId:id,silver:number(w.silver)-rule.stake};
        if(rule.energy){update.energy=Math.min(100,number(w.energy,100)+Math.floor(Math.max(0,now()-number(w.energyLastRegen,now()))/90000))-rule.energy;update.energyLastRegen=now();}
        tx.update(priv(roster[i].uid),update);
      }
      const receipt={id,mode,roster,state:'playing',owner,createdAt:now(),leaseUntil:now()+60000,stake:rule.stake,energyCost:rule.energy||0};
      tx.create(ref,receipt);return receipt;
    });
  }
  async function recordOutcome(id,result){available();return db.runTransaction(async tx=>{
    const ref=session(id),snap=await tx.get(ref);if(!snap.exists)throw fail('match_not_found');const d=snap.data();
    if(d.state==='settled'||d.state==='settling')return d;
    tx.update(ref,{state:'settling',result,leaseUntil:now()});return {...d,state:'settling',result};
  });}
  async function settle(id,onlyUid=null,earlyResult=null){available();return db.runTransaction(async tx=>{
    const ref=session(id),snap=await tx.get(ref);if(!snap.exists)throw fail('match_not_found');const d=snap.data();
    if(d.state==='settled')return d.result;
    if(onlyUid&&d.partialResults?.[onlyUid])return d.partialResults[onlyUid];
    if(!onlyUid&&(d.state!=='settling'||!d.result))throw fail('result_pending');
    if(onlyUid&&(!earlyResult||!d.roster.some(p=>p.uid===onlyUid)))throw fail('invalid_reward');
    const wallets=[],profiles=[];
    for(const p of d.roster){wallets.push(await tx.get(priv(p.uid)));profiles.push(await tx.get(pub(p.uid)));}
    const result={...(onlyUid?earlyResult:d.result),serverCommitted:true},totals={};
    for(let i=0;i<d.roster.length;i++){
      const p=d.roster[i],w=wallets[i].data(),u=profiles[i].data();
      if(onlyUid&&p.uid!==onlyUid)continue;
      if(d.partialResults?.[p.uid]){result.rewards[p.slot]=d.partialResults[p.uid].rewards[p.slot];continue;}
      if(!w||!u)throw fail('account_missing');const r=result.rewards[p.slot];
      if(!r||!Number.isSafeInteger(r.silver)||r.silver<0)throw fail('invalid_reward');
      const wallet={silver:number(w.silver)+r.silver,xp:number(w.xp)+r.xp};
      if(w.activeMatchId===id)wallet.activeMatchId=null;
      if(result.aborted&&d.energyCost)wallet.energy=Math.min(100,number(w.energy)+d.energyCost);
      const updates=r.xp?{xp:wallet.xp}:{};
      if(d.mode!=='duel'&&!result.draw&&!result.aborted){
        if(d.mode==='team'){
          updates.teamRating=Math.max(0,number(u.teamRating,500)+r.ratingDelta);
          updates.teamGamesPlayed=number(u.teamGamesPlayed)+1;
          if(r.place===0)updates.teamWins=number(u.teamWins)+1;
        }else if(d.mode==='ranked'){
          updates.gamesPlayed=number(u.gamesPlayed)+1;
          updates.rating=Math.max(0,number(u.rating,500)+r.ratingDelta);
          updates.ratingDate=new Date(now()).toISOString().slice(0,10);updates.ratingToday=(u.ratingDate===updates.ratingDate?number(u.ratingToday):0)+r.ratingDelta;
          if(r.place===0)updates.wins=number(u.wins)+1;
        }else if(d.mode==='training'){
          updates.trainingGamesPlayed=number(u.trainingGamesPlayed)+1;
        }
      }
      totals[p.uid]={...wallet,rating:updates.rating??number(u.rating,500),teamRating:updates.teamRating??number(u.teamRating,500),games:updates.gamesPlayed??number(u.gamesPlayed),wins:updates.wins??number(u.wins)};
      tx.update(priv(p.uid),wallet);if(Object.keys(updates).length)tx.update(pub(p.uid),updates);
      tx.set(priv(p.uid).collection('matches').doc(id),{matchId:id,type:d.mode,place:r.place,goalsConceded:result.scores?.[p.slot]||0,ratingDelta:r.ratingDelta,xp:r.xp,silver:r.silver,stake:d.stake,netSilver:r.silver-d.stake,date:now(),draw:!!result.draw,aborted:!!result.aborted,
        teamGoalsConceded:d.mode==='team'?result.teamScores?.[Rules.get('team').teams[p.slot]]??0:null,
        opponents:Object.keys(result.players||{}).length?Object.entries(result.players).filter(([slot])=>Number(slot)!==p.slot).map(([,player])=>player.nick):d.roster.filter(x=>x.uid!==p.uid).map(x=>x.nick)});
    }
    // Totals are stored privately and filtered by the service before sending.
    result.totals=totals;
    if(onlyUid)tx.update(ref,{partialResults:{...d.partialResults,[onlyUid]:result}});
    else tx.update(ref,{state:'settled',result,settledAt:now(),leaseUntil:Number.MAX_SAFE_INTEGER});return result;
  });}
  async function release(uid,id){return db.runTransaction(async tx=>{
    const r=priv(uid),w=await tx.get(r);if(w.data()?.activeMatchId===id)tx.update(r,{activeMatchId:null});
  });}
  async function recover(uid,id){available();const w=await priv(uid).get();id=id||w.data()?.activeMatchId||w.data()?.lastMatchId;
    if(typeof id!=='string'||!/^m_[a-zA-Z0-9_-]+$/.test(id))return null;
    const snap=await session(id).get();if(!snap.exists)return null;let d=snap.data();if(!d.roster.some(p=>p.uid===uid))throw fail('wrong_uid');
    if(d.state==='playing'&&d.leaseUntil<now()){
      const result={roomId:id,mode:d.mode,winnerSlot:null,places:d.roster.map(p=>p.slot),draw:true,aborted:true,reason:'server_restart',participants:d.roster.map(p=>({uid:p.uid,slot:p.slot})),players:Object.fromEntries(d.roster.map(p=>[p.slot,{nick:p.nick}])),rewards:{}};
      for(const p of d.roster)result.rewards[p.slot]={place:null,draw:true,stake:d.stake,silver:d.stake,xp:0,ratingDelta:0};
      d=await recordOutcome(id,result);
    }
    if(d.state==='settling')return settle(id);return d.state==='settled'?d.result:d.partialResults?.[uid]||null;
  }
  async function renew(ids){available();for(const id of ids)await db.runTransaction(async tx=>{const ref=session(id),s=await tx.get(ref);if(s.exists&&s.data().state==='playing'&&s.data().owner===owner)tx.update(ref,{leaseUntil:now()+60000});});}
  async function wallet(uid){const [w,u]=await Promise.all([priv(uid).get(),pub(uid).get()]);const a=w.data()||{},b=u.data()||{};
    return {silver:number(a.silver),gold:number(a.gold),xp:number(a.xp),energy:number(a.energy,100),rating:number(b.rating,500),teamRating:number(b.teamRating,500),games:number(b.gamesPlayed),wins:number(b.wins)};
  }
  async function pending(uid){const w=await priv(uid).get(),id=w.data()?.activeMatchId;if(!id)return null;const s=await session(id).get(),d=s.data();
    return d&&d.state!=='settled'&&d.roster.some(p=>p.uid===uid)?{roomId:id,mode:d.mode}:null;
  }
  async function sweep(){available();const expired=await db.collection('match_sessions').where('leaseUntil','<',now()).limit(25).get();
    for(const s of expired.docs){const d=s.data();if(d.roster?.length)await recover(d.roster[0].uid,s.id);}
  }
  return {reserve,recordOutcome,settle,release,recover,renew,sweep,wallet,pending};
}
module.exports={createLedger};
