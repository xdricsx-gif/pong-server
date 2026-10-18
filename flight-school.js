'use strict';
// Separate server-owned receipt prevents repeat rewards even if UI progress is reset.
async function saveFlightSchool(db,uid,value){
 if(!uid||!db)throw Error('auth_required');
 if(value?.version!==1||!Number.isInteger(value.step)||value.step<0||value.step>11)throw Error('invalid_progress');
 const ref=db.collection('users_private').doc(uid),receipt=db.collection('flight_school_rewards').doc(uid);
 return db.runTransaction(async tx=>{
  const [profile,claim]=await Promise.all([tx.get(ref),tx.get(receipt)]);
  if(!profile.exists)throw Error('profile_missing');
  const data=profile.data(),old=data.flightSchool||{},completed=!!old.completed||(value.completed===true&&value.step===11);
  const progress={version:1,step:Math.max(old.step||0,value.step),completed};
  const reward=completed&&!claim.exists?300:0,silver=(Number(data.silver)||0)+reward;
  tx.set(ref,{flightSchool:progress,...(reward?{silver}:{})},{merge:true});
  if(reward)tx.set(receipt,{version:1,silver:300,claimedAt:Date.now()});
  return {progress,reward,silver};
 });
}
module.exports={saveFlightSchool};
