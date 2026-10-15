// In-memory Firestore substitute for tests only. Never imported by server.js.
'use strict';
function database(users=['a','b','c','d']){
 const data=new Map();for(const uid of users){data.set('users_private/'+uid,{silver:1000,xp:0,energy:100});data.set('users_public/'+uid,{nickname:uid,rating:500,gamesPlayed:0,wins:0});}
 let queue=Promise.resolve(),failCommits=0;
 const snapshot=ref=>({id:ref.id,exists:data.has(ref.path),data:()=>structuredClone(data.get(ref.path))});
 const ref=path=>({path,id:path.split('/').at(-1),get:async()=>snapshot(ref(path)),collection:name=>collection(path+'/'+name)});
 function collection(name){return {doc:id=>ref(name+'/'+id),where:(field,op,value)=>({limit:n=>({get:async()=>({docs:[...data].filter(([p,d])=>p.startsWith(name+'/')&&p.split('/').length===name.split('/').length+1&&d[field]<value).slice(0,n).map(([p])=>snapshot(ref(p)))})})})};}
 const db={collection,runTransaction:fn=>{const p=queue.then(async()=>{let wrote=false;const writes=[];
   const out=await fn({get:async r=>{if(wrote)throw Error('read_after_write');return snapshot(r);},
     create:(r,d)=>{wrote=true;if(data.has(r.path))throw Error('exists');writes.push(()=>data.set(r.path,structuredClone(d)));},
     update:(r,d)=>{wrote=true;writes.push(()=>{if(!data.has(r.path))throw Error('missing_doc');const next=structuredClone(data.get(r.path));for(const [key,value]of Object.entries(d)){const parts=key.split('.');let obj=next;for(const p of parts.slice(0,-1))obj=obj[p]||(obj[p]={});obj[parts.at(-1)]=structuredClone(value);}data.set(r.path,next);});},
     set:(r,d)=>{wrote=true;writes.push(()=>data.set(r.path,structuredClone(d)));}});
   if(failCommits>0){failCommits--;throw Error('write_failed');}writes.forEach(fn=>fn());return out;});queue=p.catch(()=>{});return p;}};
 return {db,data,failNext:()=>failCommits++};
}
module.exports={database};
