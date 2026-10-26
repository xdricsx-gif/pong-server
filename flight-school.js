'use strict';
const FINAL_STEP=4;
function normalize(p){
 if(!p||![1,2,3].includes(p.version)||!Number.isInteger(p.step)||p.step<0||p.step>(p.version===1?11:p.version===2?6:FINAL_STEP))throw Error('invalid_progress');
 const old=p.version===1?[0,1,2,3,3,4,4,5,5,6,6,6][p.step]:p.step;
 const step=p.version===3?old:[0,1,2,3,3,3,4][old];
 return {version:3,step,completed:p.completed===true&&p.step===(p.version===1?11:p.version===2?6:FINAL_STEP)};
}
async function saveFlightSchool(db,uid,value){
 if(!uid||!db)throw Error('auth_required');
 const incoming=normalize(value),ref=db.collection('users_private').doc(uid),receipt=db.collection('flight_school_rewards').doc(uid);
 return db.runTransaction(async tx=>{
  const [profile,claim]=await Promise.all([tx.get(ref),tx.get(receipt)]);
  if(!profile.exists)throw Error('profile_missing');
  const data=profile.data();let old;try{old=normalize(data.flightSchool);}catch{old={step:0,completed:false};}
  const completed=old.completed||incoming.completed,progress={version:3,step:completed?FINAL_STEP:Math.max(old.step,incoming.step),completed};
  const reward=completed&&!claim.exists?300:0,silver=(Number(data.silver)||0)+reward;
  tx.set(ref,{flightSchool:progress,...(reward?{silver}:{})},{merge:true});
  if(reward)tx.set(receipt,{version:3,silver:300,claimedAt:Date.now()});
  return {progress,reward,silver};
 });
}
module.exports={saveFlightSchool,normalize};
