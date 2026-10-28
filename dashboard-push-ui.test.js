'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
class Element{
 constructor(tag){this.tagName=tag;this.children=[];this.dataset={};this.attributes={};this.value='';this.hidden=false;this.disabled=false;this._text='';}
 set textContent(v){this._text=String(v);this.children=[];}get textContent(){return this._text+this.children.map(c=>c.textContent).join('');}
 set innerHTML(v){throw Error('Unsafe HTML renderer');}
 append(...children){for(const c of children){c.parentNode=this;this.children.push(c);}}
 replaceChildren(...children){this._text='';this.children=[];this.append(...children);}
 setAttribute(k,v){this.attributes[k]=v;}
 remove(){if(this.parentNode)this.parentNode.children=this.parentNode.children.filter(c=>c!==this);}
 showModal(){this.open=true;}close(){this.open=false;}focus(){}scrollIntoView(){}
}
function fixture(overrides={}){
 const body=new Element('body'),root=new Element('section');body.append(root);let user={getIdToken:async()=> 'test-token'},count=0;const calls=[];
 const summary={enabledPlayers:8,freshEnabledPlayers:6,staleEnabledPlayers:2,disabledPlayers:3,knownPlayers:11,eligibleDevices:7,observedAt:'2026-10-24T21:15Z',freshnessDays:30};
 const campaign={id:'c1',title:'<img src=x onerror=alert(1)>',body:'Body <script>bad</script>',createdAt:'2026-10-24T21:15Z',status:'completed',recipientCount:1,deviceCount:1,metrics:{accepted:1,received:0,opened:0,login:0,match:0},recipients:[{nickname:'<b>Alice</b>',lastLoginAt:'2026-10-24T21:15Z',acceptedAt:'2026-10-24T21:16Z',receivedAt:null}]};
 const context={globalThis:null,document:{body,createElement:t=>new Element(t),getElementById:()=>root},Intl,Map,Set,Date,Number,String,Object,Promise,URLSearchParams,AbortController,setTimeout,clearTimeout,crypto:{randomUUID:()=> 'uuid-'+(++count)},fetch:async(url,opts)=>{const payload=opts.body?JSON.parse(opts.body):undefined;calls.push({url,opts,payload});let result;if(overrides.fetch)result=await overrides.fetch(url,payload,calls);if(result===undefined){result=url.endsWith('/summary')?summary:url.endsWith('/preview')?{recipientCount:1,deviceCount:1,excludedCount:0}:url.endsWith('/campaigns')?(payload?campaign:{rows:[campaign]}):url.includes('/players?')?{rows:[{uid:'u1',nickname:'Alice',enabled:true,fresh:true}],cursor:null}:campaign;}return {ok:true,status:200,json:async()=>result};}};
 context.globalThis=context;context.GalaxyDashboardTime=require('./dashboard-time');vm.createContext(context);vm.runInContext(fs.readFileSync(require.resolve('./dashboard-push-app.js'),'utf8').replace(/^import[^\n]*\n/,'').replace('export function initPush','function initPush')+'\nthis.api=initPush(()=>this.getUser());',Object.assign(context,{getUser:()=>user}));
 const all=(element=body)=>[element,...element.children.flatMap(c=>all(c))];const find=(tag,text)=>all().find(e=>e.tagName===tag&&(text===undefined|| (typeof text==='function'?text(e):e.textContent===text)));
 return {api:context.api,root,body,calls,all,find,campaign,user:set=>user=set,fill(){find('input',e=>e.maxLength===80).value='Hello';find('textarea').value='Come play';}};
}
test('initial load is read-only, permission counts and campaign fields are safely rendered',async()=>{
 const f=fixture();await f.api.load();assert.equal(f.calls.length,2);assert.ok(f.calls.every(c=>c.opts.method==='GET'));assert.match(f.root.textContent,/<img src=x onerror=alert\(1\)>/);assert.match(f.root.textContent,/25\.10\.26, 00:15/);assert.match(f.root.textContent,/Прийнято Google/);assert.equal(f.all().some(e=>e.tagName==='img'||e.tagName==='script'),false);
 await f.find('button',e=>e.className==='push-campaign').onclick();assert.match(f.body.textContent,/<b>Alice<\/b>/);assert.match(f.body.textContent,/—/);assert.match(f.body.textContent,/Вхід після натискання/);
});
test('review checks audience before send; double click makes one campaign and selected UID is exact',async()=>{
 let release,hold=new Promise(resolve=>release=resolve);const f=fixture({fetch:async(url,payload)=>{if(url.endsWith('/campaigns')&&payload){await hold;return {...f.campaign,id:'c2'};}}});f.fill();f.api.selectPlayer({uid:'u<&>',nickname:'<unsafe>'});
 await f.find('button','Переглянути перед надсиланням').onclick();assert.equal(f.calls.length,1);assert.ok(f.calls[0].url.endsWith('/preview'));assert.deepEqual(f.calls[0].payload.uids,['u<&>']);
 const send=f.find('button','Надіслати сповіщення');const one=send.onclick(),two=send.onclick();await Promise.resolve();release();await Promise.all([one,two]);assert.equal(f.calls.filter(c=>c.opts.method==='POST'&&c.url.endsWith('/campaigns')).length,1);
});
test('lost send ACK retains requestId, locks new compose and retries idempotently',async()=>{
 let sends=0;const f=fixture({fetch:(url,payload)=>{if(url.endsWith('/campaigns')&&payload&&++sends===1)throw Error('network-lost');}});f.fill();await f.find('button','Переглянути перед надсиланням').onclick();await f.find('button','Надіслати сповіщення').onclick();assert.equal(f.find('fieldset').disabled,true);await f.find('button','Перевірити / повторити').onclick();const requests=f.calls.filter(c=>c.opts.method==='POST'&&c.url.endsWith('/campaigns'));assert.equal(requests.length,2);assert.equal(requests[0].payload.requestId,requests[1].payload.requestId);assert.equal(f.find('fieldset').disabled,false);
});
test('empty input or zero eligible recipients cannot send; logout discards stale preview and recipients',async()=>{
 const empty=fixture();await empty.find('button','Переглянути перед надсиланням').onclick();assert.equal(empty.calls.length,0);
 const zero=fixture({fetch:url=>url.endsWith('/preview')?{recipientCount:0,deviceCount:0,excludedCount:1}:undefined});zero.fill();await zero.find('button','Переглянути перед надсиланням').onclick();assert.equal(zero.find('dialog'),undefined);assert.equal(zero.calls.length,1);
 let release;const pending=new Promise(r=>release=r),f=fixture({fetch:url=>url.endsWith('/preview')?pending:undefined});f.fill();f.api.selectPlayer({uid:'a',nickname:'Alice'});const checking=f.find('button','Переглянути перед надсиланням').onclick();await new Promise(r=>setImmediate(r));f.user(null);f.api.clear();release({recipientCount:1,deviceCount:1});await checking;assert.equal(f.find('dialog'),undefined);assert.doesNotMatch(f.root.textContent,/Alice/);assert.equal(f.find('input',e=>e.maxLength===80).value,'');
});

test('campaign detail paginates all recipients, deduplicates UID and refresh starts at first page',async()=>{
 let page=0;const f=fixture({fetch:(url,payload)=>{
  if(url.includes('/campaigns/c1')){page++;return {...f.campaign,recipientCount:3,recipients:url.includes('cursor=')?[{uid:'b',nickname:'Bee'},{uid:'c',nickname:'See'}]:[{uid:'a',nickname:'Aye'},{uid:'b',nickname:'Bee'}],cursor:url.includes('cursor=')?null:'page-two'};}
 }});await f.api.load();await f.find('button',e=>e.className==='push-campaign').onclick();assert.match(f.body.textContent,/Показано 2 із 3/);
 await f.find('button','Показати ще одержувачів').onclick();assert.match(f.body.textContent,/Показано 3 із 3/);assert.equal(f.all().filter(e=>e.tagName==='td'&&e.dataset.label==='Гравець').length,3);
 await f.find('button','↻ Оновити результати').onclick();assert.match(f.body.textContent,/Показано 2 із 3/);assert.equal(page,3);
});

test('campaign history pagination keeps every campaign reachable and deduplicates returned IDs',async()=>{
 const f=fixture({fetch:(url,payload)=>{if(url.endsWith('/campaigns')&&!payload)return {rows:[f.campaign],cursor:'older'};if(url.includes('/campaigns?'))return {rows:[f.campaign,{...f.campaign,id:'c2',title:'Older campaign'}],cursor:null};}});
 await f.api.load();assert.equal(f.find('button','Показати попередні сповіщення').hidden,false);await f.find('button','Показати попередні сповіщення').onclick();assert.equal(f.all().filter(e=>e.className==='push-campaign').length,2);assert.equal(f.find('button','Показати попередні сповіщення').hidden,true);assert.match(f.root.textContent,/Показано: 2/);
 await f.api.load();assert.equal(f.all().filter(e=>e.className==='push-campaign').length,1);assert.equal(f.find('button','Показати попередні сповіщення').hidden,false);
});

test('nicknames in push selection and recipients link to profiles without becoming send actions',async()=>{
 const f=fixture();f.campaign.recipients[0].uid='u<&>';f.api.selectPlayer({uid:'u<&>',nickname:'<unsafe>'});
 let links=f.all().filter(e=>e.tagName==='a');assert.equal(links.length,1);assert.equal(links[0].dataset.playerUid,'u<&>');assert.equal(links[0].href,'#admin');assert.equal(links[0].onclick,undefined);assert.equal(f.calls.length,0);
 await f.api.load();await f.find('button',e=>e.className==='push-campaign').onclick();links=f.all().filter(e=>e.tagName==='a');assert.ok(links.some(e=>e.dataset.playerUid==='u<&>'&&e.textContent==='<b>Alice</b>'));assert.ok(f.calls.every(c=>c.opts.method==='GET'));
});
