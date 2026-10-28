'use strict';
const {calendar,day}=require('./dashboard-time');

// Retain exact calendar-day evidence so historical D1/D7 cohorts stay stable.
// The dates come only from authenticated server activity, never client clocks.
function retentionPresenceFields(saved={},at,observedDays){
  const today=day(at),activeDays={};
  if(!today)throw new TypeError('invalid_activity_time');
  for(const [key,value] of Object.entries(saved.activeDays||{})){
    if(value===true&&calendar(key)&&key<=today)activeDays[key]=true;
  }
  for(const value of observedDays===undefined?[today]:observedDays){
    if(calendar(value)&&value<=today)activeDays[value]=true;
  }
  return {activeDays};
}
module.exports={retentionPresenceFields};
