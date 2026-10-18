'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
test('retired legacy upgrade endpoint refuses purchases without touching balances',()=>{
 const handlers={},messages=[],source=fs.readFileSync(require.resolve('./server'),'utf8');
 const line=source.split('\n').find(s=>s.includes("socket.on('shop:upgrade_next',"));
 vm.runInNewContext(line,{socket:{on:(k,f)=>handlers[k]=f,emit:(k,v)=>messages.push({k,v})}});
 handlers['shop:upgrade_next']({paddleId:0});assert.equal(messages[0].k,'shop:error');assert.equal(messages[0].v.msg,'update_required');
});
