'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {csvRows,parseInstalls,aggregateInstalls,summarizeOrders}=require('./dashboard-data');
const {createDashboard}=require('./football-dashboard');
test('CSV preserves quoted commas, newlines, escaped quotes and UTF16 BOM',()=>{assert.deepEqual(csvRows(Buffer.from('\uFEFFa,b\r\n"hello, \"\"world\"\"","two\nlines"\r\n','utf16le')),[{a:'hello, "world"',b:'two\nlines'}]);});
test('current Google columns, including lowercase name; unrelated app excluded',()=>{const r=parseInstalls('Date,Package name,Country,Daily Device Installs,Install events,Uninstall events,Active Device Installs\n2026-10-01,com.galaxyfootball.app,UA,0,8,2,35\n2026-10-01,ua.chessarena.app,UA,0,999,99,900');assert.equal(r.length,1);assert.equal(r[0].installs,8);assert.equal(r[0].installed,35);assert.equal(r[0].uninstalls,2);});
test('snapshots use last day, duplicate dates/countries not counted twice, null not zero',()=>{const a={date:'2026-10-01',country:'UA',installs:4,uninstalls:1,installed:10};const b={...a,date:'2026-10-02',installs:2,installed:11};const r=aggregateInstalls([a,a,b],'2026-10-01','2026-10-02');assert.equal(r.installs,6);assert.equal(r.installed,11);assert.equal(r.reportDays,2);assert.equal(aggregateInstalls([],'2026-01-01','2026-01-02').installs,null);assert.equal(aggregateInstalls([{...a,installs:null}],'2026-10-01','2026-10-02').installs,null);});
test('tests excluded, missing amounts not invented, currencies never combined',()=>{const ledger=[{uid:'a',orderId:'1',testPurchase:false,productId:'gold_100',grantedAt:'2026-10-01',consumed:true},{uid:'a',orderId:'2',testPurchase:false,productId:'gold_100',grantedAt:'2026-10-02',consumed:true},{uid:'b',orderId:'3',testPurchase:false,productId:'gold_250',grantedAt:'2026-10-03'}, {testPurchase:true}];const r=summarizeOrders(ledger,new Map([['1',{state:'PROCESSED',total:{currencyCode:'UAH',units:'349'}}],['2',{state:'REFUNDED',total:{currencyCode:'USD',units:'8',nanos:990000000},developerRevenueInBuyerCurrency:{currencyCode:'USD',units:'0'}}]]));assert.equal(r.tests,1);assert.equal(r.count,3);assert.equal(r.buyers,2);assert.equal(r.missing,1);assert.equal(r.refunds,1);assert.equal(r.totals.UAH.paid,349);assert.equal(r.totals.USD.paid,8.99);assert.equal(r.totals.USD.revenue,0);});
function fixture(claims){return createDashboard({db:{},credentials:{},verifyIdToken:async()=>claims,live:()=>({})});}
async function req(d,auth,url='/dashboard/api/summary'){let status,body,headers;await d.handle({url,method:'GET',headers:auth?{authorization:'Bearer test'}:{},socket:{remoteAddress:'local'}},{writeHead(s,h){status=s;headers=h;},end(b){body=b;}});return {status,body,headers};}
test('API denies anonymous and nonowner',async()=>{assert.equal((await req(fixture({}),false)).status,401);assert.equal((await req(fixture({email:'someone@example.com',email_verified:true,firebase:{sign_in_provider:'google.com'}}),true)).status,403);});
test('owner email alone is insufficient; provider and verification are enforced',async()=>{for(const claims of [{email:'xdricsx@gmail.com',email_verified:false,firebase:{sign_in_provider:'google.com'}},{email:'xdricsx@gmail.com',email_verified:true,firebase:{sign_in_provider:'password'}}])assert.equal((await req(fixture(claims),true)).status,403);});
test('invalid date denied after owner auth, static files restricted and uncached',async()=>{const d=fixture({email:'xdricsx@gmail.com',email_verified:true,firebase:{sign_in_provider:'google.com'}});assert.equal((await req(d,true,'/dashboard/api/summary?from=2026-12-99')).status,400);assert.equal((await req(d,false,'/dashboard/server.js')).status,404);const r=await req(d,false,'/dashboard/');assert.equal(r.status,200);assert.equal(r.headers['Cache-Control'],'no-store');assert.match(r.headers['Content-Security-Policy'],/frame-ancestors 'none'/);});

const {validateReportUpload}=require('./dashboard-data');
const {Readable}=require('node:stream');
const sampleCsv='Date,Package name,Country,Install events,Uninstall events,Active Device Installs\n2026-10-01,com.galaxyfootball.app,UA,8,2,35';
const sampleFile={name:'stats_installs_installs_com.galaxyfootball.app_202610_country.csv',base64:Buffer.from('\uFEFF'+sampleCsv,'utf16le').toString('base64')};
test('report upload accepts UTF16 original, prevents cross-app or malformed metrics',()=>{
 assert.equal(validateReportUpload([sampleFile,sampleFile]).length,1);
 for(const files of [[],[{...sampleFile,name:'secret.csv'}],[{...sampleFile,base64:Buffer.from(sampleCsv.replace('com.galaxyfootball.app','other.app')).toString('base64')}],[{...sampleFile,base64:Buffer.from(sampleCsv.replace(',8,2,35',',-8,2,35')).toString('base64')}],[{...sampleFile,base64:Buffer.from(sampleCsv.replace('2026-10-01','2026-09-01')).toString('base64')}]])assert.throws(()=>validateReportUpload(files));
});
async function post(d,{claims=true,origin='https://dashboard.example',files=[sampleFile]}={}){
 const request=Readable.from([Buffer.from(JSON.stringify({files}))]);Object.assign(request,{method:'POST',url:'/dashboard/api/import-installs',headers:{host:'dashboard.example',origin,'content-type':'application/json',...(claims?{authorization:'Bearer test'}:{})},socket:{remoteAddress:'test'}});
 let status,body;await d.handle(request,{writeHead(s){status=s;},end(b){body=JSON.parse(b);}});return {status,body};
}
test('report import enforces owner and origin before writing',async()=>{
 assert.equal((await post(fixture({}),{claims:false})).status,401);
 assert.equal((await post(fixture({email:'other@example.com',email_verified:true,firebase:{sign_in_provider:'google.com'}}))).status,403);
 const d=fixture({email:'xdricsx@gmail.com',email_verified:true,firebase:{sign_in_provider:'google.com'}});
 assert.equal((await post(d,{origin:'https://evil.example'})).status,403);
 assert.equal((await post(d,{files:[]})).status,400);
});
test('owner import stores daily private rows, repeat imports replace rather than duplicate',async()=>{
 const stored=new Map();const db={collection:name=>({doc:id=>({name,id})}),batch:()=>({set(ref,value){stored.set(ref.name+'/'+ref.id,value);},async commit(){}})};
 const d=createDashboard({db,credentials:{},verifyIdToken:async()=>({email:'xdricsx@gmail.com',email_verified:true,firebase:{sign_in_provider:'google.com'}}),live:()=>({})});
 assert.equal((await post(d)).status,200);assert.equal((await post(d)).body.days,1);
 assert.equal(stored.size,1);assert.equal(stored.get('dashboard_play_installs/2026-10-01').rows[0].installs,8);
});
