'use strict';
// One calendar for server filters and owner UI. Google Play date-only rows stay literal.
(function(root,factory){const api=factory();if(typeof module==='object'&&module.exports)module.exports=api;else root.GalaxyDashboardTime=api;})(globalThis,function(){
 const ZONE='Europe/Kyiv';
 const parts=new Intl.DateTimeFormat('en-GB',{timeZone:ZONE,year:'numeric',month:'2-digit',day:'2-digit'});
 const dates=new Intl.DateTimeFormat('uk-UA',{timeZone:ZONE,day:'2-digit',month:'2-digit',year:'numeric'});
 const stamps=new Intl.DateTimeFormat('uk-UA',{timeZone:ZONE,dateStyle:'short',timeStyle:'short'});
 function instant(value){if(value===null||value===undefined||value==='')return null;try{const d=value?.toDate?value.toDate():new Date(value);return Number.isFinite(d.getTime())?d:null;}catch{return null;}}
 function calendar(value){if(typeof value!=='string'||!/^\d{4}-\d{2}-\d{2}$/.test(value))return null;const d=instant(value+'T00:00:00.000Z');return d&&d.toISOString().slice(0,10)===value?value:null;}
 function day(value){if(typeof value==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(value))return calendar(value);const d=instant(value);if(!d)return null;const p=Object.fromEntries(parts.formatToParts(d).map(p=>[p.type,p.value]));return `${p.year}-${p.month}-${p.day}`;}
 function shiftDay(value,amount){const valid=calendar(value);if(!valid||!Number.isSafeInteger(amount))return null;const d=new Date(valid+'T12:00:00.000Z');d.setUTCDate(d.getUTCDate()+amount);return d.toISOString().slice(0,10);}
 function date(value){const literal=calendar(value);if(literal)return literal.split('-').reverse().join('.');const d=instant(value);return d?dates.format(d):'—';}
 function stamp(value){const d=instant(value);return d?stamps.format(d):'—';}
 return {ZONE,instant,calendar,day,shiftDay,date,stamp};
});
