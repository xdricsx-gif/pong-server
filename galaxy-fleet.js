/* New UFO fleet. Independent progress leaves every legacy purchase intact. */
(function(root,factory){if(typeof module==='object'&&module.exports)module.exports=factory();else root.GalaxyUfo=factory();})(globalThis,function(){
  'use strict';
  const VERSION='ufo-v1',MAX_SUB=40;
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
  function fromAccount(priv){return stats(priv?.ufoHangar?.sub);}
  const viewRotation=slot=>[0,Math.PI,-Math.PI/2,Math.PI/2][slot]||0;
  const inputDirection=slot=>slot===1||slot===3?-1:1;
  return {VERSION,MAX_SUB,base,models,clampSub,progress,stats,fromAccount,viewRotation,inputDirection};
});
