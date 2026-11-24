const $ = s => document.querySelector(s);
const labels = {QUEUED:"В очереди",WAITING:"Ожидает",RUNNING:"В работе",COMPLETED:"Завершён",FAILED:"Ошибка",CANCELLED:"Отменён"};
let rows=[], catalog=[], selected=null;
function element(tag,text,cls){
  const node=document.createElement(tag); node.textContent=text;
  if(cls) node.className=cls; return node;
}
function choose(id){selected=id; render();}
function linkRun(id){
  const btn=element("button","#"+id); btn.onclick=()=>choose(id); return btn;
}
function render(){
  drawTimeline();
  renderFlows();
  const body=$("tbody"); body.replaceChildren();
  $("#empty").hidden=rows.length>0;
  $("#count").textContent=String(rows.length);
  for(const r of rows){
    const tr=element("tr","",r.id===selected?"selected":"");
    const id=element("td",""); id.append(linkRun(r.id)); tr.append(id);
    const name=catalog.find(p=>p.id===r.process)?.name||"";
    tr.append(element("td",r.process+" · "+name),element("td",r.day),
      element("td",labels[r.state],r.state),element("td",r.result??"—"));
    body.append(tr);
  }
  const r=rows.find(x=>x.id===selected)||rows[0], detail=$("#detail");
  if(!r)return;
  detail.replaceChildren(element("strong","#"+r.id+" · "+r.process),
    element("p",(r.trigger==="scheduled"?"Регламент":"Оператор")+" · "+r.actor),
    element("p","Дата среза · "+r.day));
  for(const [title,keys] of [["Читает",r.reads],["Записывает",r.writes]]){
    if(keys?.length)detail.append(element("p",title+" · "+keys.map(k=>k.join(" / ")).join(", ")));
  }
  if(r.blocker)detail.append(element("p","Ожидает "+r.blocker));
  if(r.hold)detail.append(element("p","Время включает демонстрационную задержку "+r.hold+" с"));
  if(r.timing?.length){
    const first=r.timing[0][1], active=r.timing.find(e=>e[0]==="RUNNING")?.[1];
    if(active!==undefined){
      detail.append(element("p","Допуск · "+duration(active-first)));
      if(["COMPLETED","FAILED","CANCELLED"].includes(r.state))
        detail.append(element("p","Выполнение · "+duration(r.timing.at(-1)[1]-active)));
    }
  }
  if(r.values)detail.append(element("p","Опубликованные числа · "+r.values.join(", ")));
  if(r.error)detail.append(element("p",r.error,"FAILED"));
  if(r.result!==null)detail.append(element("p","Результат · "+r.result));
  const history=element("ol","");
  for(const [stamp,state,blocker] of r.events){
    history.append(element("li",new Date(stamp).toLocaleTimeString("ru")+"  "+labels[state]+(blocker?" · "+blocker:"")));
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
  if(data.process==="P1"){
    const input=$("#values").value.trim();
    if(!/^-?\d+(?:[\s,;]+-?\d+)*$/.test(input))throw Error("Введите целые числа через запятую или пробел.");
    data.values=input.split(/[\s,;]+/).map(Number);
    if(data.values.length>128 || data.values.some(n=>Math.abs(n)>1e9))throw Error("До 128 чисел от −1 млрд до 1 млрд.");
  }
  selected=(await post("/api/runs",data)).id;
})};
$("#demo").onclick=()=>action($("#demo"),async()=>{selected=(await post("/api/demo",{scenario:$("#scenario").value})).ids[1]});
function resources(){
  const p=catalog.find(x=>x.id===$("#process").value);
  const names=items=>items.map(([id,n])=>id+(n?" (пред. сутки)":"")).join(", ")||"—";
  $("#numbers").hidden=p?.id!=="P1";
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
  queue:"Первый читатель занимает A. Писатель встаёт в очередь, а следующий читатель ждёт за писателем."
};
function describe(){ $("#scenarioDesc").textContent=descriptions[$("#scenario").value]; }
$("#scenario").onchange=describe; describe();
function renderFlows(){
  const target=$("#flows"); target.replaceChildren();
  const recent=rows.filter(r=>r.waits?.length);
  const focus=rows.find(r=>r.id===selected);
  const shown=recent.filter(r=>r.state==="WAITING");
  for(const r of [focus,...recent]){
    if(r?.waits?.length && !shown.includes(r) && (shown.length<3 || r===focus))shown.push(r);
  }
  if(!shown.length)target.append(element("p","Ожиданий пока нет. Запустите сценарий выше."));
  for(const r of shown){
    const chain=element("article","","chain");
    const who=element("div","");
    who.append(element("small",r.state==="WAITING"?"Сейчас ожидает":"Ожидание в истории"),linkRun(r.id),
      element("b",catalog.find(p=>p.id===r.process)?.name||r.process));
    chain.append(who);
    const intervals=r.timing?.flatMap(([state,start],i)=>
      state==="WAITING"?[{start,end:r.timing[i+1]?.[1],stamp:r.events[i][0]}]:[]
    )??[];
    for(const [index,w] of r.waits.entries()){
      const why=element("div","","reason");
      const mode=w.mode==="write"?"запись":"чтение";
      const other=w.owner_mode==="write"?"запись":"чтение";
      why.append(element("span",w.resource.join(" / "),"slice"),element("span","Запрошено: "+mode),
        element("span",w.queued?"Раньше в очереди на "+other:"Занимает срез на "+other),
        linkRun(w.owner),element("span",catalog.find(p=>p.id===w.process)?.name||w.process));
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
function drawTimeline(){
  $("#running").textContent=rows.filter(r=>r.state==="RUNNING").length;
  $("#waiting").textContent=rows.filter(r=>r.state==="WAITING").length;
  const visible=rows.filter(r=>r.timing?.length).slice(0,8).reverse();
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
    row.append(element("b","#"+r.id+" · "+r.process));
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
  target.append(element("small","Общая шкала · "+duration(stop-start)+" · до 8 последних запусков"));
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
