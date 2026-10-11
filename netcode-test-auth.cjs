// Test process ONLY: no Firebase credentials, database or production network calls.
// Loaded explicitly by the integration test's child process, never by server.js.
const Module=require('node:module'),load=Module._load;
Module._load=function(name,...args){
  if(name==='firebase-admin')return {apps:[],auth:()=>({verifyIdToken:async token=>{
    if(typeof token!=='string'||!token.startsWith('local-test:'))throw Error('Invalid test token');
    return {uid:token.slice(11)};
  }})};
  return load.call(this,name,...args);
};
