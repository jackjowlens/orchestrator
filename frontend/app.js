const $ = s => document.querySelector(s);
const stressNames={Seed:"Загрузка 8 срезов",Update:"Обновление 4 срезов",Read:"Чтение 3 срезов",Audit:"Проверка 8 срезов"};
const labels = {QUEUED:"В очереди",WAITING:"Ожидает",RUNNING:"В работе",COMPLETED:"Завершён",FAILED:"Ошибка",CANCELLED:"Отменён"};
let rows=[], catalog=[], selected=null, selectedCell=null;
const sliceName=source=>/^Load\d+$/.test(source)?"Источник "+(Number(source.slice(4))+1):source;
const sliceText=([source,day])=>sliceName(source)+" / "+day;
const processName=id=>stressNames[id]??([id,catalog.find(p=>p.id===id)?.name].filter(Boolean).join(" · "));
const actorName=id=>id.replace(/^stress-(\d+)$/,"Участник $1").replace(/^audit$/,"Проверка").replace(/^timer$/,"Таймер");
function element(tag,text,cls){
  const node=document.createElement(tag); node.textContent=text;
  if(cls) node.className=cls; return node;
}
function choose(id){selected=id; render();}
function linkRun(id){
  const btn=element("button","#"+id); btn.onclick=()=>choose(id); return btn;
}
function render(){
  renderStress();
  renderCells();
  drawTimeline();
  renderFlows();
  const body=$("tbody"); body.replaceChildren();
  $("#empty").hidden=rows.length>0;
  $("#count").textContent=String(rows.length);
  for(const r of rows){
    const tr=element("tr","",r.id===selected?"selected":"");
    const id=element("td",""); id.append(linkRun(r.id)); tr.append(id);
    tr.append(element("td",processName(r.process)),element("td",r.day),
      element("td",labels[r.state],r.state),element("td",r.result??"—"));
    body.append(tr);
  }
  const r=rows.find(x=>x.id===selected)||rows[0], detail=$("#detail");
  if(!r)return;
  detail.replaceChildren(element("strong","#"+r.id+" · "+processName(r.process)),
    element("p",(r.trigger==="scheduled"?"Регламент":"Оператор")+" · "+actorName(r.actor)),
    element("p","Дата среза · "+r.day));
  for(const [title,keys] of [["Читает",r.reads],["Записывает",r.writes]]){
    if(keys?.length)detail.append(element("p",title+" · "+keys.map(sliceText).join(", ")));
  }
  if(r.blocker){
    const wait=r.waits.at(-1);
    detail.append(element("p","Ожидает #"+wait.owner+" · "+sliceText(wait.resource)));
  }
  if(r.hold)detail.append(element("p","Время включает демонстрационную задержку "+r.hold.toLocaleString("ru",{maximumFractionDigits:3})+" с"));
  if(r.timing?.length){
    const first=r.timing[0][1], active=r.timing.find(e=>e[0]==="RUNNING")?.[1];
    if(active!==undefined){
      detail.append(element("p","Допуск · "+duration(active-first)));
      if(["COMPLETED","FAILED","CANCELLED"].includes(r.state))
        detail.append(element("p","Выполнение · "+duration(r.timing.at(-1)[1]-active)));
    }
  }
  if(r.reads?.length && r.version!==null && r.version!==undefined)
    detail.append(element("p",(r.snapshot?"Опубликованный снимок":"Входные данные")+" · версия "+r.version));
  if(r.bypassed?.length){
    const bypass=element("p","Запущен в обход ");
    bypass.append(...r.bypassed.map(linkRun));
    detail.append(bypass);
  }
  if(r.values && r.state==="COMPLETED")detail.append(element("p","Опубликованные числа · "+r.values.join(", ")));
  if(r.error)detail.append(element("p",r.error,"FAILED"));
  if(r.result!==null)detail.append(element("p","Результат · "+r.result));
  const history=element("ol","");
  for(const [stamp,state,blocker] of r.events){
    history.append(element("li",new Date(stamp).toLocaleTimeString("ru")+"  "+labels[state]+(blocker?" · "+blocker.replace(/Load\d+/g,sliceName):"")));
  }
  detail.append(history);
}
async function post(path,data){
  const res=await fetch(path,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(data)});
  if(!res.ok)throw Error(await res.text()); return res.json();
}
async function action(button,fn){
  button.disabled=true; $("#message").textContent="";
  try{await fn();render()}catch(err){$("#message").textContent=err.message}finally{button.disabled=false}
}
$("form").onsubmit=e=>{e.preventDefault();action($(".primary"),async()=>{
  const data={process:$("#process").value,day:$("#day").value,hold:Number($("#hold").value)};
  if(!$("#read-policy").hidden)data.snapshot=$("#snapshot").value==="true";
  if(data.process==="P1"){
    const input=$("#values").value.trim();
    if(!/^-?\d+(?:[\s,;]+-?\d+)*$/.test(input))throw Error("Введите целые числа через запятую или пробел.");
    data.values=input.split(/[\s,;]+/).map(Number);
    if(data.values.length>128 || data.values.some(n=>Math.abs(n)>1e9))throw Error("До 128 чисел от −1 млрд до 1 млрд.");
  }
  selected=(await post("/api/runs",data)).id;
})};
$("#demo").onclick=()=>{
  const hold=$("#scenario-hold");
  if(!hold.reportValidity())return;
  action($("#demo"),async()=>{
    selected=(await post("/api/demo",{scenario:$("#scenario").value,hold:hold.valueAsNumber})).ids[1];
  });
};
function resources(){
  const p=catalog.find(x=>x.id===$("#process").value);
  const names=items=>items.map(([id,n])=>id+(n?" (пред. сутки)":"")).join(", ")||"—";
  $("#numbers").hidden=p?.id!=="P1";
  $("#read-policy").hidden=p?.writes.length!==0;
  if(p)$("#resources").textContent="Чтение: "+names(p.reads)+"   ·   Запись: "+names(p.writes);
}
async function init(){
  catalog=await (await fetch("/api/catalog")).json();
  for(const p of catalog)$("#process").add(new Option(p.id+" · "+p.name,p.id));
  resources(); $("#process").onchange=resources;
  const stream=new EventSource("/api/events");
  stream.onmessage=e=>{rows=JSON.parse(e.data); received=performance.now();$("#connection").textContent="● На связи";render()};
  stream.onerror=()=>$("#connection").textContent="Переподключение…";
}
init().catch(err=>$("#message").textContent=err.message);

const descriptions={
  refresh:"Обновление занимает A за 20 ноября. Сумма за 20-е ждёт, за 19-е — доступна сразу.",
  window:"Разница за 20 ноября использует значения за 19-е и 20-е. Обновление вчерашнего среза задерживает расчёт.",
  writers:"Обе сводки читают A и B. Записать общий результат C за одну дату может только одна сводка.",
  queue:"Первый читатель занимает A. Писатель встаёт в очередь, а следующий читатель ждёт за писателем.",
  stress:"120 запусков на 48 срезах. Загрузки занимают по 8 срезов, каждое обновление занимает по 4, а финальные проверки сверяют суммы."
};
function describe(){
  const scenario=$("#scenario").value;
  $("#scenarioDesc").textContent=descriptions[scenario];
  $("#scenario-hold").value={refresh:3,window:4,writers:4,queue:4,stress:.7}[scenario];
}
$("#scenario").onchange=describe; describe();
function renderFlows(){
  const target=$("#flows"); target.replaceChildren();
  const recent=rows.filter(r=>r.waits?.length);
  const focus=rows.find(r=>r.id===selected);
  const shown=recent.filter(r=>r.state==="WAITING").slice(0,6);
  for(const r of [focus,...recent]){
    if(r?.waits?.length && !shown.includes(r) && (shown.length<3 || r===focus))shown.push(r);
  }
  if(!shown.length)target.append(element("p","Ожиданий пока нет. Запустите сценарий выше."));
  for(const r of shown){
    const chain=element("article","","chain");
    const who=element("div","");
    who.append(element("small",r.state==="WAITING"?"Сейчас ожидает":"Ожидание в истории"),linkRun(r.id),
      element("b",processName(r.process)));
    chain.append(who);
    const intervals=r.timing?.flatMap(([state,start],i)=>
      state==="WAITING"?[{start,end:r.timing[i+1]?.[1],stamp:r.events[i][0]}]:[]
    )??[];
    for(const [index,w] of r.waits.entries()){
      const why=element("div","","reason");
      const mode=w.mode==="write"?"запись":"чтение";
      const other=w.owner_mode==="write"?"запись":"чтение";
      why.append(element("span",sliceText(w.resource),"slice"),element("span","Запрошено: "+mode),
        element("span",w.queued?"Раньше в очереди на "+other:"Занимает срез на "+other),
        linkRun(w.owner),element("span",processName(w.process)));
      const interval=intervals[index];
      if(interval){
        const stamp=new Date(interval.stamp).toLocaleTimeString("ru",{
          hour:"2-digit",minute:"2-digit",second:"2-digit",fractionalSecondDigits:3
        });
        why.append(element("span","С "+stamp,"wait-clock"));
        const elapsed=element("span","","wait-duration");
        elapsed.dataset.start=interval.start;
        if(interval.end!==undefined)elapsed.dataset.end=interval.end;
        why.append(elapsed);
      }
      chain.append(why);
    }
    target.append(chain);
  }
  updateWaitDurations();
}

let received=performance.now();
function duration(us){
  return us<1000?Math.round(us)+" мкс":us<1000000?(us/1000).toFixed(1)+" мс":(us/1000000).toFixed(2)+" с";
}
function renderStress(){
  const batch=rows.find(r=>r.batch)?.batch;
  if(!batch)return;
  $("#stress-report").hidden=false;
  const runs=rows.filter(r=>r.batch===batch);
  const finished=new Set(["COMPLETED","FAILED","CANCELLED"]);
  const done=runs.filter(r=>finished.has(r.state)).length;
  const errors=runs.filter(r=>r.state==="FAILED"||r.state==="CANCELLED").length;
  const waits=[], edges=[];
  for(const r of runs){
    const first=r.timing[0][1];
    const start=r.timing.find(([state])=>state==="RUNNING")?.[1];
    if(start!==undefined){
      edges.push([start,1]);
      if(finished.has(r.state))edges.push([r.timing.at(-1)[1],-1]);
      if(r.waits.length)waits.push(start-first);
    }
  }
  edges.sort((a,b)=>a[0]-b[0]||a[1]-b[1]);
  let peak=0, active=0;
  for(const [,delta] of edges){active+=delta;peak=Math.max(peak,active)}
  const audits=runs.filter(r=>r.process==="Audit"&&finished.has(r.state));
  const passed=audits.filter(r=>r.state==="COMPLETED"&&r.result===852).length;
  const metrics=[
    ["Завершено",done+" / 120"],
    ["Срезы","48"],
    ["Пик одновременно",String(peak)],
    ["Ждали ресурс",String(runs.filter(r=>r.waits.length).length)],
    ["Среднее ожидание",waits.length?duration(waits.reduce((a,b)=>a+b,0)/waits.length):"—"],
    ["Проверки данных",passed+" / 6"],
    ["Длительность прогона",duration(Math.max(...runs.map(r=>r.timing.at(-1)[1]))
      -Math.min(...runs.map(r=>r.timing[0][1])))],
    ["Сбои и отмены",String(errors)]
  ];
  const panel=$("#stress-metrics");
  panel.replaceChildren(...metrics.map(([label,value])=>{
    const card=element("article",label);
    card.append(element("strong",value));
    return card;
  }));
}
function drawTimeline(){
  $("#running").textContent=rows.filter(r=>r.state==="RUNNING").length;
  $("#waiting").textContent=rows.filter(r=>r.state==="WAITING").length;
  const focus=rows.find(r=>r.id===selected && r.batch);
  const eligible=rows.filter(r=>r.timing?.length &&
    (!focus || (r.batch===focus.batch && r.day===focus.day)));
  const priority=eligible.filter(r=>r.id===selected || r.state==="RUNNING" || r.state==="WAITING");
  const visible=[...priority,...eligible.filter(r=>!priority.includes(r))]
    .slice(0,8).sort((a,b)=>a.id-b.id);
  const target=$("#timeline"); target.replaceChildren();
  if(!visible.length){target.textContent="Запустите процесс, чтобы увидеть его длительность.";return}
  const latest=Math.max(...visible.map(r=>r.timing.at(-1)[1]));
  const now=latest+(performance.now()-received)*1000;
  const series=visible.map(r=>{
    const first=r.timing[0][1], active=r.timing.find(e=>e[0]==="RUNNING")?.[1];
    const end=["RUNNING","WAITING","QUEUED"].includes(r.state)?now:r.timing.at(-1)[1];
    return {r,first,active,end};
  });
  const done=rows.find(r=>r.state==="COMPLETED" && r.timing?.length);
  if(done)$("#latency").textContent=duration(done.timing.at(-1)[1]-done.timing.find(e=>e[0]==="RUNNING")[1]);
  const start=Math.min(...series.map(s=>s.first)), stop=Math.max(...series.map(s=>s.end));
  for(const {r,first,active,end} of series){
    const row=element("button","","trackrow"); row.onclick=()=>choose(r.id);
    row.append(element("b","#"+r.id+" · "+processName(r.process)));
    const track=element("span","","track");
    function bar(a,b,cls){
      if(b<=a)return;
      const segment=element("i","",cls);
      segment.style.left=(100*(a-start)/Math.max(1,stop-start))+"%";
      segment.style.width=(100*(b-a)/Math.max(1,stop-start))+"%";
      track.append(segment);
    }
    const waited=r.timing.some(e=>e[0]==="WAITING");
    if(waited)bar(first,active??end,"waitbar");
    if(active!==undefined)bar(active,end,"runbar");
    row.append(track,element("span",duration(end-first)));
    row.title="Ожидание: "+duration(waited?(active??end)-first:0)+" · Выполнение: "+duration(active===undefined?0:end-active);
    target.append(row);
  }
  target.append(element("small","Общая шкала · "+duration(stop-start)+(focus?" · "+focus.day:"")+" · до 8 запусков"));
}
function updateWaitDurations(){
  const clocks=document.querySelectorAll(".wait-duration");
  if(!clocks.length)return;
  const latest=Math.max(...rows.map(r=>r.timing?.at(-1)?.[1]??0));
  const now=latest+(performance.now()-received)*1000;
  for(const clock of clocks){
    const end=clock.dataset.end===undefined?now:Number(clock.dataset.end);
    const ms=Math.max(0,Math.round((end-Number(clock.dataset.start))/1000));
    clock.textContent=(clock.dataset.end===undefined?"Ждёт ":"Ожидал ")+ms.toLocaleString("ru")+" мс";
  }
}
setInterval(()=>{
  if(rows.some(r=>r.state==="RUNNING"))drawTimeline();
  if(rows.some(r=>r.state==="WAITING"))updateWaitDurations();
},250);

function renderCells(){
  const matrix=$("#cell-matrix"), detail=$("#cell-detail");
  const cells=new Map();
  function get(source,day){
    const key=JSON.stringify([source,day]);
    if(!cells.has(key))cells.set(key,{source,day,key,reads:[],writes:[],waits:[]});
    return cells.get(key);
  }
  for(const run of rows){
    for(const mode of ["reads","writes"]){
      for(const [source,day] of run[mode]??[])get(source,day)[mode].push(run);
    }
    let index=0;
    for(let position=0;position<run.timing.length;position++){
      if(run.timing[position][0]!=="WAITING")continue;
      const wait=run.waits[index];
      const end=run.timing[position+1]?.[1];
      get(...wait.resource).waits.push({run,wait,end,start:run.timing[position][1]});
      index++;
    }
  }
  matrix.replaceChildren();
  detail.replaceChildren();
  if(!cells.size){
    matrix.textContent="Запустите процесс или сценарий, чтобы увидеть срезы.";
    return;
  }
  const days=[...new Set([...cells.values()].map(c=>c.day))].sort().reverse();
  const sources=[...new Set([...cells.values()].map(c=>c.source))].sort();
  const table=element("table","","cell-table");
  const header=element("tr","");
  header.append(element("th","Дата"));
  for(const source of sources)header.append(element("th",sliceName(source)));
  table.append(header);
  if(!cells.has(selectedCell)){
    selectedCell=[...cells.values()].find(c=>c.waits.some(w=>w.end===undefined))?.key ??
      [...cells.values()].find(c=>c.waits.length)?.key ?? cells.keys().next().value;
  }
  for(const day of days){
    const row=element("tr","");
    row.append(element("th",day));
    for(const source of sources){
      const td=element("td","");
      const cell=cells.get(JSON.stringify([source,day]));
      if(cell){
        const count=mode=>cell[mode].filter(r=>r.state==="RUNNING").length;
        const waiting=cell.waits.filter(w=>w.end===undefined).length;
        const text="Ч "+count("reads")+" · З "+count("writes")+" · О "+waiting;
        const style=count("writes")?"cell-write":count("reads")?"cell-read":waiting?"cell-wait":"";
        const button=element("button",text,style);
        button.setAttribute("aria-pressed",String(cell.key===selectedCell));
        button.setAttribute("aria-label",sliceText([source,day])+" · "+text);
        button.onclick=()=>{selectedCell=cell.key;renderCells()};
        td.append(button);
      }else td.textContent="—";
      row.append(td);
    }
    table.append(row);
  }
  matrix.append(table);
  const cell=cells.get(selectedCell);
  detail.append(element("h3",sliceText([cell.source,cell.day])));
  const waits=cell.waits.filter(w=>w.end!==undefined);
  const total=waits.reduce((sum,w)=>sum+(w.end-w.start),0);
  detail.append(element("p",new Set(cell.waits.map(w=>w.run.id)).size+
    " запусков ждали этот срез · "+duration(total)+" суммарного завершённого ожидания"));
  function group(title,runs){
    const line=element("p",title+" ");
    if(!runs.length)line.append("—");
    for(const r of runs){
      const link=linkRun(r.id);
      link.title=processName(r.process);
      line.append(link," ");
    }
    detail.append(line);
  }
  group("Пишут сейчас",cell.writes.filter(r=>r.state==="RUNNING"));
  group("Читают сейчас",cell.reads.filter(r=>r.state==="RUNNING"&&!r.snapshot));
  if(cell.reads.some(r=>r.state==="RUNNING"&&r.snapshot))
    group("Читают опубликованный снимок",cell.reads.filter(r=>r.state==="RUNNING"&&r.snapshot));
  const pending=cell.waits.filter(w=>w.end===undefined);
  if(!pending.length)detail.append(element("p","Ждут сейчас —"));
  for(const {run,wait} of pending){
    const line=element("p","");
    line.append(linkRun(run.id),wait.queued?" ждёт своей очереди за ":" ждёт освобождения среза от ",linkRun(wait.owner));
    detail.append(line);
  }
  const committed=runs=>runs.filter(r=>r.state==="COMPLETED");
  group("Завершённые чтения",committed(cell.reads));
  group("Завершённые записи",committed(cell.writes));
}
