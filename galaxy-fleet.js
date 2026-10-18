/* New UFO fleet. Independent progress leaves every legacy purchase intact. */
(function(root,factory){if(typeof module==='object'&&module.exports)module.exports=factory();else root.GalaxyUfo=factory();})(globalThis,function(){
  'use strict';
  const VERSION='ufo-v2',MAX_SUB=40;
  const base=Object.freeze({id:0,name:'Nexus UFO',desc:'Круглий корабель із ракетним відсіком',price:0,cur:'free',lvl:1,spd:3.75,w:54,fr:54,bm:2.32,er:1,fd:1});
  const models=Object.freeze([
    null,{diameter:1200,texture:1254,bay:[354.5,-288.5],scale:1},
    {diameter:456,texture:512,bay:[122,-122],scale:1.08},
    {diameter:456,texture:512,bay:[124,-124],scale:1.16},
    {diameter:456,texture:512,bay:[130,-130],scale:1.23},
    {diameter:456,texture:512,bay:[129,-129],scale:1.3}
  ]);
  const clampSub=sub=>Math.max(0,Math.min(MAX_SUB,Math.trunc(Number(sub)||0)));
  function progress(sub){sub=clampSub(sub);return {sub,level:Math.min(5,Math.floor(sub/10)+1),sublevel:sub===40?10:sub%10};}
  function stats(sub){const p=progress(sub),m=[0,1,1.06,1.13,1.21,1.3][p.level],scale=models[p.level].scale;
    const out={...base,...p,paddleId:0,avgUpgrade:p.sub/MAX_SUB};
    for(const k of ['spd','bm','er','fd'])out[k]=+(base[k]*m).toFixed(3);
    out.w=+(base.w*scale).toFixed(3);out.fr=+(base.fr*[0,1,1.18,1.4,1.68,2][p.level]).toFixed(3);return out;
  }
  const hullCosts=[0,3000,7000,15000,30000,60000],skillCosts=[0,2000,6000,15000,35000];
  const skillKeys=['push','magnet','rocket','shield','emp','cloak'];
  const ships=[{id:'nexus',name:'Nexus Vanguard',silver:0,gold:0},{id:'swift',name:'Nexus Swift',silver:60000,gold:600},{id:'bastion',name:'Nexus Bastion',silver:80000,gold:800},{id:'flux',name:'Nexus Flux',silver:100000,gold:1000}];
  function economy(priv={}){if(priv.economy?.version===2)return JSON.parse(JSON.stringify(priv.economy));const sub=clampSub(priv.ufoHangar?.sub),hull=Math.min(6,1+Math.ceil(sub/8));return {version:2,active:'nexus',ships:{nexus:hull},skills:Object.fromEntries(skillKeys.map(k=>[k,1])),migratedFrom:sub};}
  function migration(priv={}){const state=economy(priv);if(priv.economy?.version===2)return {state,silver:0,gold:0};let silver=0,gold=0;for(let i=1;i<=state.migratedFrom;i++){const raw=i<=10?1500+(i-1)*800:i<=20?12000+(i-11)*2000:i<=30?35000+(i-21)*3500:100+(i-31)*20;if(i<=30)silver+=Math.max(10,Math.round(raw/15/10)*10);else gold+=Math.max(1,Math.round(raw/15));}return {state,silver:Math.max(0,silver-hullCosts.slice(0,state.ships.nexus).reduce((a,b)=>a+b,0)),gold};}
  function fromAccount(priv){const e=economy(priv),h=Math.max(1,Math.min(6,e.ships[e.active]||1)),p=(h-1)/5,visual=Math.round(p*40),out=stats(visual);Object.assign(out,{hullLevel:h,shipId:e.active,w:54*(1+.08*p),spd:3.75*(1+.10*p),fr:54,er:1+.10*p,fd:1,bm:2.32,skills:Object.fromEntries(skillKeys.map(k=>[k,Math.max(1,Math.min(5,e.skills[k]||1))]))});if(e.active==='swift'){out.spd*=1.08;out.w*=.94;}if(e.active==='bastion'){out.w*=1.08;out.spd*=.94;}if(e.active==='flux'){out.er*=1.08;out.spd*=.94;}out.fr*=1+.12*(out.skills.push-1)/4;return out;}
  const viewRotation=slot=>[0,Math.PI,-Math.PI/2,Math.PI/2][slot]||0;
  const inputDirection=slot=>slot===1||slot===3?-1:1;
  return {VERSION,MAX_SUB,base,models,clampSub,progress,stats,fromAccount,viewRotation,inputDirection,economy,migration,hullCosts,skillCosts,skillKeys,ships};
});
