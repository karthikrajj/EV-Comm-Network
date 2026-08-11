const socket = io();

const NODES = {
  SERVER:{x:450,y:70}, J1:{x:150,y:230}, J2:{x:350,y:230}, J3:{x:550,y:230}, J4:{x:250,y:380}, HOSPITAL_1:{x:650,y:410}
};
const EDGES = [
  {a:'J1',b:'J2',w:2,id:'e-J1-J2'}, {a:'J2',b:'J3',w:2,id:'e-J2-J3'},
  {a:'J3',b:'HOSPITAL_1',w:2,id:'e-J3-HOSPITAL'}, {a:'J1',b:'J4',w:4,id:'e-J1-J4'},
  {a:'J4',b:'HOSPITAL_1',w:4,id:'e-J4-HOSPITAL'}
];

let nodeStatus = { J1:'up', J2:'up', J3:'up', J4:'up' };
let ambulances = [];
let edgeUsageCount = {J1:0,J2:0,J3:0,J4:0};
let sent = 0;
let latencyHistory = [];
let currentState = null;

function edgeIdBetween(a,b){
  if(b === 'HOSPITAL_1') b = 'HOSPITAL';
  if(a === 'HOSPITAL_1') a = 'HOSPITAL';
  const e = EDGES.find(e=>(e.a===a&&e.b===b)||(e.a===b&&e.b===a));
  return e ? e.id : null;
}

function pathToD(path){ 
    return path.map(n=>{
        let p = NODES[n];
        if(!p) return "";
        return p.x+','+p.y;
    }).join(' L').replace(/^/,'M'); 
}

/* ===================== RENDERING ===================== */
function renderNodeStatus(){
  if (!currentState) return;
  const down_nodes = currentState.down_nodes || [];
  const degraded_nodes = currentState.degraded_nodes || [];
  
  ['J1','J2','J3','J4'].forEach(id=>{
    nodeStatus[id] = down_nodes.includes(id) ? 'down' : (degraded_nodes.includes(id) ? 'degraded' : 'up');
    const circle = document.getElementById('node-'+id);
    const glow = document.getElementById('glow-'+id);
    const up = nodeStatus[id]==='up' || nodeStatus[id]==='degraded';
    if(circle) {
        circle.classList.toggle('up', up);
        circle.classList.toggle('down', !up);
        if(nodeStatus[id] === 'degraded') {
            circle.style.stroke = 'var(--amber)';
            circle.style.strokeDasharray = '4 2';
        } else {
            circle.style.stroke = '';
            circle.style.strokeDasharray = '';
        }
    }
    if(glow) glow.style.display = up ? '' : 'none';
  });
  const onlineCount = Object.values(nodeStatus).filter(s=>s==='up').length;
  document.getElementById('kpi-nodes').textContent = onlineCount+'/4';
  const pill = document.getElementById('net-status');
  if(onlineCount<4){ pill.classList.add('warn'); pill.lastChild.textContent='DEGRADED — REROUTING'; }
  else { pill.classList.remove('warn'); pill.lastChild.textContent='NETWORK OPERATIONAL'; }
}

function renderRoads(){
  if (!currentState) return;
  
  EDGES.forEach(e=>{
    const el = document.getElementById(e.id);
    if(el) {
        el.classList.remove('active','down');
        let nodeA = e.a;
        let nodeB = e.b === 'HOSPITAL' ? 'HOSPITAL_1' : e.b;
        const down = (nodeStatus[nodeA]==='down') || (nodeStatus[nodeB]==='down');
        if(down) el.classList.add('down');
    }
  });

  const active_routes = currentState.active_routes || {};
  for (const amb_id in active_routes) {
    const route = active_routes[amb_id];
    for(let i=0;i<route.length-1;i++){
      const id = edgeIdBetween(route[i], route[i+1]);
      if(id) {
          const el = document.getElementById(id);
          if(el) el.classList.add('active');
      }
    }
  }
}

function renderJunctionList(){
  const box = document.getElementById('junction-list'); box.innerHTML='';
  ['J1','J2','J3','J4'].forEach(id=>{
    const up = nodeStatus[id]==='up';
    let inUse = false;
    if(currentState && currentState.active_routes) {
        for(let a in currentState.active_routes){
            if(currentState.active_routes[a].includes(id)) inUse = true;
        }
    }
    const cls = !up ? 'red' : (inUse ? 'green' : (nodeStatus[id]==='degraded' ? 'amber' : 'green'));
    const label = !up ? 'DOWN' : (nodeStatus[id]==='degraded' ? 'DEGRADED (50% LOSS)' : (inUse ? 'GREEN — PRIORITY' : 'STANDBY'));
    const div = document.createElement('div');
    div.className='junction-item';
    div.onclick=()=>toggleNode(id);
    div.innerHTML = `<span class="name">Junction ${id.slice(1)}</span><span class="signal"><span class="signal-dot ${cls}"></span>${label}</span>`;
    box.appendChild(div);
  });
}

function toggleNode(id){
    socket.emit('toggle_node', { id });
}

function renderQueue(){
  if(!currentState) return;
  const box = document.getElementById('queue-list');
  const active = currentState.request_queue || [];
  if(active.length===0){ box.innerHTML = '<div class="queue-empty">No active units in queue.</div>'; return; }
  
  box.innerHTML='';
  active.forEach(a=>{
    const badgeCls = a.priority==='CRITICAL'?'critical':'stable';
    const div = document.createElement('div');
    div.className='queue-item';
    div.innerHTML = `<div class="qi-top"><span class="qi-id">${a.ambulance}</span><span class="badge ${badgeCls}">${a.priority}</span></div>
      <div class="qi-route">Wait time: ${a.wait_time.toFixed(1)}s</div>
      <div style="margin-top:5px;"><span class="badge wait">QUEUED</span></div>`;
    box.appendChild(div);
  });
  document.getElementById('stat-active').textContent = Object.keys(currentState.active_routes || {}).length;
}

function renderStats(){
  if(!currentState || !currentState.stats) return;
  const stats = currentState.stats;
  document.getElementById('kpi-total').textContent = stats.total_requests;
  document.getElementById('kpi-arrived').textContent = stats.successful_requests;
  document.getElementById('kpi-avgresp').textContent = stats.avg_response_time > 0 ? stats.avg_response_time.toFixed(1) + 's' : '—';
  
  let busiest = stats.busiest_junction || '—';
  document.getElementById('kpi-busiest').textContent = busiest;
  const onlineCount = Object.values(nodeStatus).filter(s=>s==='up').length;
  document.getElementById('kpi-uptime').textContent = Math.round((onlineCount/4)*100)+'%';
  
  if (currentState.dashboard_congestion !== undefined) {
      document.getElementById('in-congestion').value = currentState.dashboard_congestion;
      document.getElementById('congestion-val').textContent = currentState.dashboard_congestion + '%';
      document.getElementById('kpi-loss').textContent = (2 + currentState.dashboard_congestion*0.25).toFixed(1)+'%';
  }
}

/* ===================== LATENCY CANVAS ===================== */
function drawLatency(){
  const canvas = document.getElementById('latency-canvas');
  if(!canvas) return;
  const ctx = canvas.getContext('2d');
  const w = canvas.width = canvas.clientWidth*2, h = canvas.height = canvas.clientHeight*2;
  ctx.clearRect(0,0,w,h);
  if(latencyHistory.length<2) return;
  const max = Math.max(...latencyHistory, 60), min = 0;
  ctx.beginPath(); ctx.lineWidth=3; ctx.strokeStyle = '#4FA3FF';
  latencyHistory.forEach((v,i)=>{
    const x = (i/(latencyHistory.length-1))*w;
    const y = h - ((v-min)/(max-min))*h*0.85 - h*0.08;
    i===0 ? ctx.moveTo(x,y) : ctx.lineTo(x,y);
  });
  ctx.stroke();
}

/* ===================== AMBULANCE ANIMATION ===================== */
const svgNS = "http://www.w3.org/2000/svg";
const animatedAmbulances = {};

function syncAmbulances(){
    if(!currentState || !currentState.ambulance_map) return;
    const layer = document.getElementById('amb-layer');
    
    // Add new ones
    for(const amb_id in currentState.ambulance_map){
        if(!animatedAmbulances[amb_id]){
            const amb = currentState.ambulance_map[amb_id];
            if(amb.arrived) continue;
            
            const g = document.createElementNS(svgNS,'g');
            g.id = 'marker-'+amb_id;
            const txt = document.createElementNS(svgNS,'text');
            txt.setAttribute('class','amb-marker'); txt.setAttribute('x','-9'); txt.setAttribute('y','6');
            txt.textContent = '\u{1F691}';
            const anim = document.createElementNS(svgNS,'animateMotion');
            anim.id = 'motion-'+amb_id;
            anim.setAttribute('dur', Math.max(2, (amb.route.length)*0.7)+'s');
            anim.setAttribute('repeatCount','1');
            anim.setAttribute('fill','freeze');
            anim.setAttribute('path', pathToD(amb.route));
            g.appendChild(txt); g.appendChild(anim);
            layer.appendChild(g);
            anim.beginElement();
            
            animatedAmbulances[amb_id] = true;
        }
    }
    
    // Remove old ones
    for(const amb_id in animatedAmbulances){
        if(!currentState.ambulance_map[amb_id] || currentState.ambulance_map[amb_id].arrived){
            const g = document.getElementById('marker-'+amb_id);
            if(g){ g.style.transition='opacity .6s ease'; g.style.opacity='0'; setTimeout(()=>g.remove(),650); }
            delete animatedAmbulances[amb_id];
        }
    }
}

/* ===================== SOCKETIO ===================== */
socket.on('connect', () => {
    socket.emit('request_state');
    console.log("Connected to server");
});

socket.on('state_update', (data) => {
    currentState = data;
    renderNodeStatus();
    renderRoads();
    renderJunctionList();
    renderQueue();
    renderStats();
    syncAmbulances();
});

socket.on('packet_log', (data) => {
    const logBody = document.getElementById('log-body');
    const ts = new Date(data.timestamp).toLocaleTimeString('en-GB');
    
    let typeMap = { 'EMERGENCY_REQUEST':['REQ','tag-req'], 'ACK':['ACK','tag-ack'], 'HEARTBEAT':['HB','tag-hb'], 'ROUTE_UPDATE':['RTE','tag-route'] };
    let mapping = typeMap[data.type] || ['SYS','tag-sys'];
    const [tag, cls] = mapping;
    
    // Structured JSON log output
    const logData = {
        seq: data.seq_no,
        sender: data.sender,
        recv: data.receiver,
        payload: data.payload
    };
    
    const line = document.createElement('div'); line.className='log-line';
    line.innerHTML = `<span class="ts">[${ts}]</span> <span class="tag ${cls}">${tag.padEnd(4)}</span> <span style="color:#A78BFA">${data.type}</span> ${JSON.stringify(logData)}`;
    logBody.appendChild(line); logBody.scrollTop = logBody.scrollHeight;
    while(logBody.children.length>60) logBody.removeChild(logBody.firstChild);
    
    sent++; document.getElementById('stat-sent').textContent = sent;
    
    if(data.type === 'HEARTBEAT' && data.latency_ms) {
        latencyHistory.push(data.latency_ms);
        if(latencyHistory.length>40) latencyHistory.shift();
        document.getElementById('stat-latency').textContent = Math.round(data.latency_ms) + ' ms';
        drawLatency();
    }
});

/* ===================== INIT ===================== */
function tickClock(){ document.getElementById('clock').textContent = new Date().toLocaleTimeString('en-GB'); }
setInterval(tickClock,1000); tickClock();

document.addEventListener('DOMContentLoaded', () => {
    // UI Tabs
    document.querySelectorAll('.tab-btn').forEach(btn=>{
      btn.addEventListener('click', ()=>{
        document.querySelectorAll('.tab-btn').forEach(b=>b.classList.remove('active'));
        document.querySelectorAll('.tab-pane').forEach(p=>p.classList.remove('active'));
        btn.classList.add('active');
        document.getElementById('pane-'+btn.dataset.tab).classList.add('active');
      });
    });

    const btnDispatch = document.getElementById('btn-dispatch');
    if(btnDispatch) {
        btnDispatch.addEventListener('click', () => {
            const origin = document.getElementById('in-origin').value;
            const priority = document.getElementById('in-priority').value;
            socket.emit('client_dispatch', { origin, priority });
        });
    }

    const btnReset = document.getElementById('btn-reset');
    if(btnReset) {
        btnReset.addEventListener('click', () => {
            socket.emit('reset_network');
        });
    }

    const inCongestion = document.getElementById('in-congestion');
    if(inCongestion) {
        inCongestion.addEventListener('input', (e) => {
            socket.emit('set_congestion', { value: parseInt(e.target.value) });
        });
    }

    // Chaos controls
    document.getElementById('btn-chaos-kill')?.addEventListener('click', () => {
        const nodes = ['J1', 'J2', 'J3', 'J4'];
        socket.emit('chaos_inject', { action: 'kill_node', target: nodes[Math.floor(Math.random()*nodes.length)] });
    });
    document.getElementById('btn-chaos-degrade')?.addEventListener('click', () => {
        const nodes = ['J1', 'J2', 'J3', 'J4'];
        socket.emit('chaos_inject', { action: 'degrade_node', target: nodes[Math.floor(Math.random()*nodes.length)] });
    });
    document.getElementById('btn-chaos-flood')?.addEventListener('click', () => {
        socket.emit('chaos_inject', { action: 'flood_requests' });
    });
});
