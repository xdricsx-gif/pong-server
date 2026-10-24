import {initAdmin} from '/dashboard/admin.js';
import {initializeApp} from 'https://www.gstatic.com/firebasejs/10.12.2/firebase-app.js';
import {getAuth,GoogleAuthProvider,signInWithPopup,onAuthStateChanged,signOut} from 'https://www.gstatic.com/firebasejs/10.12.2/firebase-auth.js';
const $=id=>document.getElementById(id), esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const number=n=>n===null||n===undefined?'—':new Intl.NumberFormat('uk-UA').format(n);
const validDate=s=>s&&Number.isFinite(new Date(s).getTime());
const date=s=>validDate(s)?new Date(s).toLocaleDateString('uk-UA',{timeZone:'UTC'}):'—';
const stamp=s=>validDate(s)?new Date(s).toLocaleString('uk-UA',{timeZone:'UTC',dateStyle:'short',timeStyle:'short'}):'—';
const duration=seconds=>{if(seconds===null||seconds===undefined||!Number.isFinite(Number(seconds)))return 'Ще не виміряно';const s=Math.max(0,Math.floor(Number(seconds)));return s>=3600?`${number(Math.floor(s/3600))} год ${number(Math.floor(s%3600/60))} хв`:s>=60?`${number(Math.floor(s/60))} хв`:`${number(s)} с`;};
const iso=d=>new Date(d).toISOString().slice(0,10),money=m=>m?new Intl.NumberFormat('uk-UA',{style:'currency',currency:m.currency}).format(m.value):'—';
const empty=s=>`<div class="empty">${esc(s)}</div>`;
let auth,user,data,busy=false,activityBusy=false,pendingSummary=false,pendingActivity=false,forceSummary=false,forceActivity=false,lastActivity=null,lastActivityRange='',lastActivityTime=0;
const currentRange=()=>new URLSearchParams({from:$('from').value,to:$('to').value}).toString();
const sameRequest=(requestUser,range)=>user===requestUser&&range===currentRange();
const setRefreshState=()=>{$('refresh').disabled=busy||activityBusy;};
function period(days){$('to').value=iso(Date.now());$('from').value=days==='all'?'2020-01-01':iso(Date.now()-(Number(days)-1)*86400000);document.querySelectorAll('[data-days]').forEach(b=>b.classList.toggle('selected',b.dataset.days===String(days)));}
period(30);$('to').max=iso(Date.now());$('from').max=$('to').max;
function metric(label,value,note,i,primary=false){return `<article class="metric ${primary?'primary-metric':''}"><span class="metric-index">0${i}</span><div class="metric-label">${esc(label)}</div><div class="metric-value">${esc(value)}</div><small>${esc(note)}</small></article>`;}
function rank(rows,kind=''){if(!rows.length)return empty('Ще немає даних за цей період');const max=Math.max(1,...rows.map(r=>r.value||0));return rows.map(r=>`<div class="rank-row ${kind}"><span>${esc(r.label)}</span><strong>${number(r.value)}</strong><progress class="bar" max="${max}" value="${r.value||0}" aria-label="${esc(r.label)}"></progress></div>`).join('');}
function chart(rows){
  if(!rows.length)return empty('Google ще не надав даних за вибраний період');
  const w=680,h=210,left=35,right=14,top=15,bottom=30,iw=w-left-right,ih=h-top-bottom;
  const max=Math.max(1,...rows.flatMap(r=>[r.installs||0,r.uninstalls||0]));
  const x=i=>left+(rows.length===1?iw/2:i/(rows.length-1)*iw),y=v=>top+ih-(v/max)*ih;
  const grid=[0,.5,1].map(p=>`<line x1="${left}" y1="${y(p*max)}" x2="${w-right}" y2="${y(p*max)}" stroke="#29364a" stroke-dasharray="3 5"/><text x="0" y="${y(p*max)+4}">${number(Math.round(p*max))}</text>`).join('');
  const line=(key,color)=>{const points=rows.map((r,i)=>r[key]===null?null:[x(i),y(r[key]),r]).filter(Boolean);return `<polyline points="${points.map(p=>p.slice(0,2).join(',')).join(' ')}" fill="none" stroke="${color}" stroke-width="2.5"/>`+points.map(p=>`<circle cx="${p[0]}" cy="${p[1]}" r="${rows.length<32?3:1.5}" fill="${color}"><title>${esc(date(p[2].date))}: ${key==='installs'?'встановлення':'видалення'} ${number(p[2][key])}</title></circle>`).join('');};
  const labels=[...new Set([0,Math.floor((rows.length-1)/2),rows.length-1])].map(i=>`<text x="${x(i)}" y="${h-3}" text-anchor="middle">${date(rows[i].date)}</text>`).join('');
  return `<svg viewBox="0 0 ${w} ${h}" role="img" aria-label="Графік встановлень і видалень за датами">${grid}${line('installs','#70e0d4')}${line('uninstalls','#de97be')}${labels}</svg>`;
}
function renderActivity(d){
  const g=d.activity,a=d.audience||(data?.from===$('from').value&&data?.to===$('to').value?data.audience:null);
  if(d.live){$('online').textContent=number(d.live.online);$('rooms').textContent=`${number(d.live.rooms)} матчів триває`;}
  $('activity-since').textContent=g?.coverageFrom?`Облік матчів із ${stamp(g.coverageFrom)} UTC`:'Початок збору ще не підтверджено';
  $('activity-updated').textContent=`Дані активності: ${stamp(d.dataUpdatedAt||d.generatedAt)} UTC · перевіряємо оновлення кожні 15 с`;
  $('activity-metrics').innerHTML=[metric('Нові профілі',number(a?.newProfiles),'За вибраний період',1),metric('На сервері сьогодні',number(a?.seenToday),'Профілі з підтвердженим входом',2),metric('Завершено матчів',number(g?.count??g?.matches),g?`${number(g.withBots)} із ботами · ${number(g.training)} тренувальних`:'Новий серверний облік',3),metric('Середня тривалість',g?.averageSeconds!==null&&g?.averageSeconds!==undefined?number(g.averageSeconds)+' с':'—','Завершені матчі за період',4)].join('');
  $('matches').innerHTML=g?.rows?.length?g.rows.map(r=>`<tr><td>${stamp(r.endedAt)}</td><td>${number(r.players)} / ${number(r.bots)}</td><td>${r.training?'Тренування':r.bots?'Рейтинг + боти':'Рейтинг'}</td><td>${number(r.duration)} с</td></tr>`).join(''):'<tr><td colspan="4">Нові матчі з’являться після завершення гри</td></tr>';
  const recent=d.recentPlayers;
  $('recent-players-updated').textContent=d.recentPlayersUpdatedAt?`Профілі отримано ${stamp(d.recentPlayersUpdatedAt)} UTC`:'';
  $('recent-players').innerHTML=Array.isArray(recent)?(recent.length?recent.map(r=>`<tr><td data-label="Гравець"><strong class="player-name">${esc(r.nickname||'Без імені')}</strong>${r.active?'<span class="player-active">Зараз у грі</span>':''}</td><td data-label="Реєстрація · UTC">${stamp(r.createdAt)}</td><td data-label="Останній вхід · UTC">${stamp(r.lastSeen)}</td><td data-label="Час у грі"><span class="playtime ${r.foregroundSeconds==null?'unmeasured':''}">${esc(duration(r.foregroundSeconds))}</span>${r.measuredSince?`<small class="time-coverage">облік із ${esc(date(r.measuredSince))}</small>`:''}</td></tr>`).join(''):'<tr class="empty-row"><td colspan="4">Ще немає профілів із датою реєстрації</td></tr>'):'<tr class="empty-row"><td colspan="4">Останні реєстрації тимчасово недоступні</td></tr>';
  $('playtime-note').textContent=d.playtimeNotice||'Рахуємо час, коли гра відкрита на екрані та має зв’язок із сервером: меню, навчання й матчі. Облік починається після оновлення гри; минулий та офлайн-час не відновлюється.';
  $('activity-incomplete').hidden=!g?.partial;$('activity-incomplete').textContent=g?.partial?'Частина активності недоступна. Показано лише підтверджені дані.':'';
}
function reportSource(i){
  const through=i.dataThrough||i.latestDate,error=i.automaticError||(!i.ok&&i.error);
  let state=!i.ok?'Немає звітів':i.stale?'Дані застаріли':error?'Резервні дані':i.mode==='imported'?'Завантажені звіти':'Автосинхронізація';
  if(i.refreshing)state+=' · оновлюється';
  const notes=[];
  if(through)notes.push(`Дані Google до ${date(through)}.`);
  if(i.lastSuccessAt||i.syncedAt)notes.push(`Останнє успішне отримання: ${stamp(i.lastSuccessAt||i.syncedAt)} UTC.`);
  if(error){const reason=error==='access_denied'?'Google відхилив доступ до звітів.':error==='not_configured'?'Доступ до звітів ще не налаштовано.':'Остання автосинхронізація не вдалася.';notes.push(reason);if(i.lastAttemptAt)notes.push(`Спроба: ${stamp(i.lastAttemptAt)} UTC.`);}
  notes.push('Google публікує ці звіти із затримкою 3–7 днів.');
  return ['Google Play · встановлення',state,notes.join(' ')];
}
function render(d){
  data=d;const i=d.installs,a=d.audience,p=d.purchases,g=d.activity;
  $('updated').textContent=`${date(d.from)} — ${date(d.to)} · Зведення отримано ${stamp(d.generatedAt)} UTC`;
  const paid=p?Object.entries(p.totals).map(([currency,t])=>money({currency,value:t.paid})).join(' + '):'';
  const notes=[];if(!i.ok)notes.push(i.refreshing?'Отримуємо звіти Google Play. Матчі та нові профілі оновлюються окремо.':'Звіти Google Play тимчасово недоступні. Встановлення й видалення не підмінені нулями.');if(i.ok&&i.automaticError)notes.push('Автосинхронізація Google Play не вдалася. Показано останні успішно отримані звіти; деталі — у джерелах даних.');if(i.stale)notes.push(`Дані Google Play застаріли${i.dataThrough?': останній звіт за '+date(i.dataThrough):''}. Звичайна затримка Google — 3–7 днів.`);if(p?.missing)notes.push(`Для ${p.missing} покупок Google ще не повернув суму; підсумок неповний.`);if([a,p,g].some(v=>v?.partial))notes.push('Досягнуто ліміту завантаження: частина показників неповна.');if(d.collectionFailures)notes.push('Були помилки запису активності. Статистика матчів може бути неповною.');
  if(i.ok && i.firstDate && d.from<i.firstDate)notes.push(`Звіти встановлень доступні з ${date(i.firstDate)}. Показано наявну історію.`);
  $('notice').hidden=!notes.length;$('notice').textContent=notes.join(' ');
  $('metrics').innerHTML=[metric('Встановлення',number(i.ok?i.installs:null),'Події встановлення за період',1,true),metric('Видалення',number(i.ok?i.uninstalls:null),'Події видалення за період',2),metric('Залишилося на пристроях',number(i.ok?i.installed:null),i.latestDate?`Активна база на ${date(i.latestDate)}`:'Чекаємо на звіт Google Play',3),metric('Ігрові профілі',number(a?.total),'Усього створено в базі гри',4),metric('Покупки',number(p?.count),'Справжні операції за період',5),metric('Оплачено гравцями',p?(p.count===0?'0':((p.missing?'≥ ':'')+(paid||'—'))):'—','Сума замовлень за даними Google',6)].join('');
  $('report-date').textContent=i.dataThrough||i.latestDate?`Звіти до ${date(i.dataThrough||i.latestDate)}`:i.refreshing?'Оновлюється…':'Звіт недоступний';$('report-date').classList.toggle('warning',!!i.stale||!!i.automaticError);$('installs-chart').innerHTML=i.ok?chart(i.daily||[]):empty(i.refreshing?'Отримуємо звіти Google Play…':'Не вдалося синхронізувати Google Play');
  let names;try{names=new Intl.DisplayNames(['uk'],{type:'region'});}catch{}
  $('countries').innerHTML=rank((i.countries||[]).map(c=>{let name=c.country;try{name=names?.of(c.country)||name;}catch{}return {label:name,value:c.installed};}));
  $('money-stats').innerHTML=[['Покупців',p?.buyers],['Тестових операцій',p?.tests],['Замовлень із поверненням',p?.refunds],['Потребують синхронізації',p?.consumptionPending]].map(([s,v])=>`<div><span>${esc(s)}</span><strong>${number(v)}</strong></div>`).join('');
  $('products').innerHTML=rank(Object.entries(p?.products||{}).map(([label,value])=>({label:label.replace('gold_','')+' золота',value})),'product');
  $('currencies').innerHTML=p&&Object.keys(p.totals).length?Object.entries(p.totals).map(([currency,t])=>`<div class="currency"><span>${esc(currency)} · оплачено<br><b>${money({currency,value:t.paid})}</b><br><small>${t.paidOrders} замовлень із сумою</small></span><span>Дохід після відрахувань<br><b>${t.revenueOrders?money({currency,value:t.revenue}):'—'}</b><br><small>${t.revenueOrders} замовлень із доходом</small></span></div>`).join(''):empty(p?.count===0?'Покупок за цей період поки немає':'Суми ще недоступні');
  const states={PROCESSED:'Оплачено',REFUNDED:'Повернено',PARTIALLY_REFUNDED:'Часткове повернення',PENDING_REFUND:'Повернення в обробці',CANCELED:'Скасовано',UNAVAILABLE:'Сума уточнюється'};
  $('orders').innerHTML=p?.rows.length?p.rows.map(r=>`<tr><td>${stamp(r.date)}</td><td>${esc(r.product)}</td><td>${money(r.paid)}</td><td>${money(r.revenue)}</td><td>${esc(states[r.state]||r.state)}</td></tr>`).join(''):'<tr><td colspan="5">Немає операцій для відображення</td></tr>';
  renderActivity(lastActivity&&lastActivityRange===currentRange()?lastActivity:d);
  $('players').innerHTML=a?.leaders.length?a.leaders.map(r=>`<tr><td>${esc(r.nickname)}</td><td>${number(r.games)}</td><td>${number(r.wins)}</td><td>${number(r.rating)}</td></tr>`).join(''):'<tr><td colspan="4">Ще немає профілів</td></tr>';
  $('sources').innerHTML=[reportSource(i),['Firebase · гра',a?'Підключено':'Немає даних','Матчі й останні реєстрації перевіряються кожні 15 секунд, поки ця сторінка відкрита. Загальні підсумки профілів можуть відставати до 5 хвилин.'],['Google Play · покупки',p?'Підключено':'Немає даних','Серверний журнал підтверджених покупок. Суми та повернення уточнюються через Google до 15 хвилин.']].map(([name,state,note])=>`<div class="source">${esc(name)}<br><strong>${esc(state)}</strong><p>${esc(note)}</p></div>`).join('');
}
const adminPanel=initAdmin(()=>user);
function requestError(status){return new Error(status===403?'Доступ дозволено лише акаунту власника.':status===400?'Перевір дати: початок має бути до кінця періоду.':'Не вдалося отримати дані. Спробуй оновити сторінку.');}
async function load(refresh=false){
  if(!user)return;forceSummary=forceSummary||refresh;if(busy){pendingSummary=true;return;}
  busy=true;const requestUser=user,range=currentRange(),force=forceSummary;forceSummary=false;setRefreshState();
  try{
    const token=await requestUser.getIdToken();const r=await fetch('/dashboard/api/summary?'+range+(force?'&refresh=1':''),{headers:{Authorization:'Bearer '+token},cache:'no-store'});
    if(!r.ok)throw requestError(r.status);const result=await r.json();if(!sameRequest(requestUser,range))return;
    render(result);$('dashboard').hidden=false;$('login').hidden=true;
  }catch(e){if(sameRequest(requestUser,range)){if(data||!$('dashboard').hidden){$('notice').textContent=e.message;$('notice').hidden=false;}else $('login-status').textContent=e.message;}}
  finally{busy=false;setRefreshState();if(user&&(pendingSummary||range!==currentRange())){pendingSummary=false;load();}}
}
async function loadActivity(refresh=false){
  if(!user)return;forceActivity=forceActivity||refresh;if(activityBusy){pendingActivity=true;return;}
  activityBusy=true;const requestUser=user,range=currentRange(),force=forceActivity;forceActivity=false;setRefreshState();
  try{
    const token=await requestUser.getIdToken();const r=await fetch('/dashboard/api/activity?'+range+(force?'&refresh=1':''),{headers:{Authorization:'Bearer '+token},cache:'no-store'});
    if(!r.ok)throw requestError(r.status);const result=await r.json();if(!sameRequest(requestUser,range))return;
    lastActivity=result;lastActivityRange=range;lastActivityTime=Date.now();renderActivity(result);$('activity-status').hidden=true;
    $('dashboard').hidden=false;$('login').hidden=true;
  }catch(e){if(sameRequest(requestUser,range)){$('activity-status').textContent=e.message+(lastActivity&&lastActivityRange===range?' Показано останні отримані дані активності.':'');$('activity-status').hidden=false;}}
  finally{activityBusy=false;setRefreshState();if(user&&(pendingActivity||range!==currentRange())){pendingActivity=false;loadActivity();}}
}
function reload(refresh=false){loadActivity(refresh);load(refresh);}
document.querySelectorAll('[data-days]').forEach(b=>b.onclick=()=>{period(b.dataset.days);reload();});$('apply').onclick=()=>{document.querySelectorAll('[data-days]').forEach(b=>b.classList.remove('selected'));reload();};$('refresh').onclick=()=>reload(true);
document.addEventListener('visibilitychange',()=>{if(!document.hidden&&Date.now()-lastActivityTime>=15000)loadActivity();});
document.querySelectorAll('nav a').forEach(a=>a.onclick=()=>{document.querySelectorAll('nav a').forEach(x=>x.classList.remove('active'));a.classList.add('active');});
$('export').onclick=()=>{if(!data)return;const rows=[['Дата','Встановлення','Видалення','Активні пристрої з грою'],...(data.installs.daily||[]).map(r=>[r.date,r.installs,r.uninstalls,r.installed])];const text='\uFEFF'+rows.map(r=>r.map(v=>'"'+String(v??'').replace(/"/g,'""')+'"').join(',')).join('\r\n');const url=URL.createObjectURL(new Blob([text],{type:'text/csv;charset=utf-8'}));const a=document.createElement('a');a.href=url;a.download=`galaxy-football-${data.from}-${data.to}.csv`;a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);};

$('import-reports').onclick=()=>$('report-files').click();
$('report-files').onchange=async()=>{
  const selected=[...$('report-files').files];if(!selected.length||!user)return;
  const status=$('import-status');$('import-reports').disabled=true;
  try{
    if(selected.length>24||selected.reduce((n,f)=>n+f.size,0)>7000000)throw new Error('Обери до 24 CSV загальним розміром до 7 МБ.');
    status.textContent='Завантажую та перевіряю звіти…';
    const files=await Promise.all(selected.map(file=>new Promise((resolve,reject)=>{const reader=new FileReader();reader.onload=()=>resolve({name:file.name,base64:String(reader.result).split(',')[1]});reader.onerror=()=>reject(new Error('Не вдалося прочитати файл.'));reader.readAsDataURL(file);}))); 
    const r=await fetch('/dashboard/api/import-installs',{method:'POST',headers:{Authorization:'Bearer '+await user.getIdToken(),'Content-Type':'application/json'},body:JSON.stringify({files})});
    const result=await r.json();if(!r.ok)throw new Error(r.status===400?'Потрібні оригінальні CSV встановлень Galaxy Football із розподілом за країнами.':'Не вдалося зберегти звіти. Спробуй пізніше.');
    status.textContent='Збережено днів: '+result.days+'. Повторний імпорт не подвоює показники.';await load(true);
  }catch(e){status.textContent=e.message;}finally{$('import-reports').disabled=false;$('report-files').value='';}
};

try{const config=await (await fetch('/dashboard/config')).json();auth=getAuth(initializeApp(config.firebase));$('signin').onclick=async()=>{try{$('login-status').textContent='Відкриваю вхід Google…';await signInWithPopup(auth,new GoogleAuthProvider());}catch(e){$('login-status').textContent=e.code==='auth/unauthorized-domain'?'Адресу дашборда ще потрібно дозволити у Firebase.':e.code==='auth/popup-closed-by-user'?'Вікно входу закрито. Натисни кнопку, щоб повторити.':'Не вдалося увійти через Google. Спробуй ще раз.';}};$('logout').onclick=()=>signOut(auth);onAuthStateChanged(auth,u=>{user=u;$('logout').hidden=!u;$('account').textContent=u?.email||'';if(u){reload();adminPanel.load();}else{adminPanel.clear();data=null;lastActivity=null;lastActivityRange='';lastActivityTime=0;pendingSummary=false;pendingActivity=false;forceSummary=false;forceActivity=false;$('dashboard').hidden=true;$('login').hidden=false;$('login-status').textContent='Статистика доступна лише власнику гри.';}});setInterval(()=>{if(!document.hidden)load();},60000);setInterval(()=>{if(!document.hidden)loadActivity();},15000);}catch{$('login-status').textContent='Не вдалося завантажити вхід. Перевір з’єднання й онови сторінку.';}
