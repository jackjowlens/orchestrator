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
  if(r.blocker)detail.append(element("p","Ожидает "+r.blocker));
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
  selected=(await post("/api/runs",{process:$("#process").value,day:$("#day").value})).id;
})};
$("#demo").onclick=()=>action($("#demo"),async()=>{selected=(await post("/api/demo",{})).ids[1]});
function resources(){
  const p=catalog.find(x=>x.id===$("#process").value);
  const names=items=>items.map(([id,n])=>id+(n?" (пред. сутки)":"")).join(", ")||"—";
  if(p)$("#resources").textContent="Чтение: "+names(p.reads)+"   ·   Запись: "+names(p.writes);
}
async function init(){
  catalog=await (await fetch("/api/catalog")).json();
  for(const p of catalog)$("#process").add(new Option(p.id+" · "+p.name,p.id));
  resources(); $("#process").onchange=resources;
  const stream=new EventSource("/api/events");
  stream.onmessage=e=>{rows=JSON.parse(e.data);$("#connection").textContent="● На связи";render()};
  stream.onerror=()=>$("#connection").textContent="Переподключение…";
}
init().catch(err=>$("#message").textContent=err.message);
