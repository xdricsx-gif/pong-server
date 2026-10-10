'use strict';
const PACKAGE = 'com.galaxyfootball.app';
const day = value => { const d = value?.toDate ? value.toDate() : new Date(value); return Number.isFinite(+d) ? d.toISOString().slice(0,10) : null; };
function csvRows(input) {
  let text = Buffer.isBuffer(input) ? input.toString(input[0]===255 && input[1]===254 ? 'utf16le' : 'utf8') : input;
  text = text.replace(/^\uFEFF/,'');
  const rows=[]; let row=[], cell='', quoted=false;
  for(let i=0;i<text.length;i++) {
    const c=text[i];
    if(c==='"') { if(quoted && text[i+1]==='"'){cell+='"';i++;} else quoted=!quoted; }
    else if(c===',' && !quoted){row.push(cell);cell='';}
    else if(c==='\n' && !quoted){row.push(cell.replace(/\r$/,''));rows.push(row);row=[];cell='';}
    else cell+=c;
  }
  if(cell || row.length){row.push(cell.replace(/\r$/,''));rows.push(row);}
  const headers=rows.shift()||[];
  return rows.filter(r=>r.some(Boolean)).map(r=>Object.fromEntries(headers.map((h,i)=>[h,r[i]??''])));
}
const num = v => v!=='' && v!==undefined && v!==null && Number.isFinite(Number(v)) ? Number(v) : null;
function parseInstalls(input) {
  const map={installs:['Install events','Daily Device Installs'],uninstalls:['Uninstall events','Daily Device Uninstalls'],installed:['Active Device Installs','Installs on active devices','Current Device Installs'],activeInstalled:['Active Device Installs','Installs on active devices'],newUsers:['Daily User Installs'],lostUsers:['Daily User Uninstalls'],totalUsers:['Total User Installs']};
  return csvRows(input).filter(r=>(r['Package Name']||r['Package name'])===PACKAGE && /^\d{4}-\d{2}-\d{2}$/.test(r.Date)).map(r=>({date:r.Date,country:r.Country||r['Country / Region']||'Unknown',...Object.fromEntries(Object.entries(map).map(([k,v])=>[k,num(r[v.find(h=>Object.hasOwn(r,h))])]))}));
}
function aggregateInstalls(rows,from,to) {
  // Country is the single chosen dimension. Dedup prevents repeated imports doubling totals.
  const unique=[...new Map(rows.map(r=>[`${r.date}/${r.country}`,r])).values()];
  const dates=[...new Set(unique.filter(r=>r.date<=to).map(r=>r.date))].sort();
  const latest=dates.at(-1)||null;
  const sum=(xs,k)=>xs.length && xs.every(r=>r[k]!==null && r[k]!==undefined) ? xs.reduce((n,r)=>n+r[k],0) : null;
  const period=unique.filter(r=>r.date>=from && r.date<=to);
  const snapshot=unique.filter(r=>r.date===latest);
  return {from, to, latestDate:latest, firstDate:dates[0]||null, reportDays:new Set(period.map(r=>r.date)).size,
    installs:sum(period,'installs'),uninstalls:sum(period,'uninstalls'),installed:sum(snapshot,'installed'),activeInstalled:sum(snapshot,'activeInstalled'),
    daily:[...new Set(period.map(r=>r.date))].sort().map(date=>{const xs=period.filter(r=>r.date===date);return {date,installs:sum(xs,'installs'),uninstalls:sum(xs,'uninstalls'),installed:sum(xs,'installed')};}),
    countries:snapshot.map(r=>({country:r.country,installed:r.installed})).sort((a,b)=>(b.installed||0)-(a.installed||0)).slice(0,12)};
}
function money(m) {
  if(!m || !/^[A-Z]{3}$/.test(m.currencyCode)) return null;
  const value=Number(m.units||0)+Number(m.nanos||0)/1e9;
  return Number.isFinite(value)?{currency:m.currencyCode,value}:null;
}
function summarizeOrders(ledger,orders) {
  const real=ledger.filter(p=>!p.testPurchase); const totals={}; let missing=0,refunds=0,consumptionPending=0;
  const products={}; const rows=[];
  for(const p of real) {
    const order=orders.get(p.orderId); const paid=money(order?.total);const revenue=money(order?.developerRevenueInBuyerCurrency);
    if(!order || !paid)missing++;
    if(order && /REFUND/.test(order.state))refunds++;
    if(!p.consumed)consumptionPending++;
    products[p.productId]=(products[p.productId]||0)+1;
    for(const [key,m] of [['paid',paid],['revenue',revenue]]) if(m){const t=totals[m.currency] ||= {paid:0,revenue:0,paidOrders:0,revenueOrders:0};t[key]+=m.value;t[key+'Orders']++;}
    rows.push({date:p.grantedAt,product:p.productId,quantity:p.quantity,gold:p.gold,state:order?.state||'UNAVAILABLE',paid,revenue});
  }
  return {count:real.length, buyers:new Set(real.map(p=>p.uid)).size,tests:ledger.length-real.length,refunds,consumptionPending,missing,totals,products,rows:rows.sort((a,b)=>b.date.localeCompare(a.date)).slice(0,100)};
}
module.exports={PACKAGE,day,csvRows,parseInstalls,aggregateInstalls,money,summarizeOrders};
