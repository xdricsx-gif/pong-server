(function(root,factory){if(typeof module==='object'&&module.exports)module.exports=factory();else root.GalaxyMatchRules=factory();})(typeof globalThis!=='undefined'?globalThis:this,function(){
'use strict';
const MODES=Object.freeze({
  ranked:Object.freeze({id:'ranked',title:'Кожен сам за себе',slots:[0,1,2,3],stake:100,payouts:[250,150,0,0],rating:[50,20,5,-20],xp:[100,50,20,0],durationTicks:10800,lives:10}),
  team:Object.freeze({id:'team',title:'Командний 2×2',slots:[0,1,2,3],teams:[0,1,1,0],stake:100,payouts:[200,0],rating:[25,-25],xp:[100,0],durationTicks:10800,lives:20}),
  duel:Object.freeze({id:'duel',title:'Дружній 1×1',slots:[0,1],stake:0,payouts:[0,0],rating:[0,0],xp:[0,0],durationTicks:10800,lives:10}),
  training:Object.freeze({id:'training',title:'Тренування',slots:[0,1,2,3],stake:0,payouts:[1500,700,200,0],rating:[0,0,0,0],xp:[150,75,30,0],energy:20,durationTicks:10800,lives:10})
});
function get(mode){if(!Object.prototype.hasOwnProperty.call(MODES,mode))throw Error('invalid_mode');return MODES[mode];}
function places(world){const slots=world.config.activeSlots||[0,1,2,3];
  if(world.config.mode==='team')return slots.slice().sort((a,b)=>((world.config.teams[a]===world.winnerTeam)?0:1)-((world.config.teams[b]===world.winnerTeam)?0:1)||a-b);
  const out=world.winner===null?[]:[world.winner];
  out.push(...slots.filter(s=>!out.includes(s)&&!world.eliminated[s]).sort((a,b)=>world.lives[b]-world.lives[a]||world.scores[a]-world.scores[b]||a-b));
  for(const s of [...world.eliminationOrder].reverse())if(slots.includes(s)&&!out.includes(s))out.push(s);
  for(const s of slots)if(!out.includes(s))out.push(s);return out;
}
function reward(mode,world,slot){const r=get(mode),order=places(world),draw=world.winner===null;
  const place=mode==='team'?(world.config.teams[slot]===world.winnerTeam?0:1):order.indexOf(slot);
  if(!r.slots.includes(slot)||place<0)throw Error('invalid_slot');
  return {place:draw?null:place,draw,stake:r.stake,silver:draw?r.stake:r.payouts[place],xp:draw?0:r.xp[place],ratingDelta:draw?0:r.rating[place],ratingField:mode==='team'?'teamRating':'rating'};
}
function terms(mode){const r=get(mode);if(mode==='duel')return 'Без ставки, нагород і змін рейтингу.';
  if(mode==='training')return '20 енергії. За місця: 1500 / 700 / 200 / 0 срібла та 150 / 75 / 30 / 0 XP. Рейтинг не змінюється.';
  if(mode==='team')return 'Внесок: 100 срібла з кожного. Перемога: 200 срібла (чистими +100), +25 рейтингу 2×2, +100 XP. Поразка: −100 срібла, −25 рейтингу, 0 XP.';
  return 'Внесок: 100 срібла. Виплати за місця: 250 / 150 / 0 / 0 (чистими +150 / +50 / −100 / −100). Рейтинг: +50 / +20 / +5 / −20. XP: 100 / 50 / 20 / 0.';
}
return {MODES,get,places,reward,terms};
});
