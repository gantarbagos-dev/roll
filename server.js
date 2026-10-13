const express=require('express');
const http=require('http');
const WebSocket=require('ws');
const path=require('path');

const app=express();
const server=http.createServer(app);
const WSS_URL='wss://developer.mig33.id/developer/ws';
const sessions=new Map();
const rollState={running:false,stop:false,room:'',delay:0,message:''};
const floodState={running:false,stop:false,room:'',messages:[],sent:0,failed:0,message:'FLOOD belum berjalan.'};

app.use(express.json({limit:'10mb'}));
app.use(express.static(path.join(__dirname,'public')));

function makeId(){return Math.random().toString(36).slice(2)+Date.now().toString(36)}
function safe(s){return String(s||'').slice(0,300)}
function closeSession(s){
 try{s.ws?.close()}catch{}
 if(s.ping)clearInterval(s.ping);
 s.status='closed';
}
function wait(ms){return new Promise(r=>setTimeout(r,Math.max(0,ms)))}
function sendAndWait(account,payload,expectedType,timeoutMs=5000){
 return new Promise((resolve,reject)=>{
  const ws=account.ws;
  if(account.status!=='ready'||!ws||ws.readyState!==WebSocket.OPEN)return reject(new Error('WebSocket tidak siap'));
  let done=false;
  const finish=(err,value)=>{
   if(done)return;done=true;clearTimeout(timer);ws.off('message',onMessage);
   err?reject(err):resolve(value);
  };
  const onMessage=raw=>{
   let m;try{m=JSON.parse(raw.toString())}catch{return}
   if(m.type===expectedType)finish(null,m);
   else if(m.type==='error')finish(new Error(m.data?.message||m.data?.error||'API error'));
  };
  const timer=setTimeout(()=>finish(new Error('Timeout menunggu '+expectedType)),timeoutMs);
  ws.on('message',onMessage);
  try{ws.send(JSON.stringify(payload))}catch(e){finish(e)}
 });
}

function connectAccount(account){
 return new Promise((resolve,reject)=>{
  let settled=false;
  const finish=(ok,err)=>{
   if(settled)return; settled=true;
   if(ok)resolve(account); else reject(err);
  };
  let ws;
  try{ws=new WebSocket(WSS_URL)}catch(e){finish(false,e);return}
  account.ws=ws; account.status='connecting';

  const timeout=setTimeout(()=>{
   if(!settled){account.status='error';try{ws.close()}catch{};finish(false,new Error('Timeout login 15 detik'))}
  },15000);

  ws.on('open',()=>{account.status='authenticating'});

  ws.on('message',raw=>{
   let m; try{m=JSON.parse(raw.toString())}catch{return}
   account.last=m;
   if(m.type==='auth.required'){
    try{ws.send(JSON.stringify({type:'developer.login',username:account.username,password:account.password}))}
    catch(e){account.status='error';clearTimeout(timeout);finish(false,e)}
   }else if(m.type==='session.ready'){
    account.status='ready';
    account.permissions=m.data?.developer?.permissions||[];
    account.wallet=m.data?.wallet||null;
    if(account.wallet)account.balance=account.wallet.label||String(account.wallet.balance_cr||'');
    account.ping=setInterval(()=>{
     if(ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify({type:'ping'}));
    },40000);
    clearTimeout(timeout); finish(true);
   }else if(m.type==='wallet.balance.result'){
    account.wallet=m.data?.wallet||account.wallet;
    if(account.wallet)account.balance=account.wallet.label||String(account.wallet.balance_cr||'');
   }else if(m.type==='error' && !settled){
    account.status='error'; clearTimeout(timeout);
    finish(false,new Error(m.data?.message||m.data?.error||'Login gagal'));
   }else if(m.type==='session.replaced'){
    account.status='error'; clearTimeout(timeout);
    finish(false,new Error('Session digantikan oleh koneksi lain'));
   }
  });

  ws.on('close',()=>{
   clearTimeout(timeout);
   if(account.ping)clearInterval(account.ping);
   if(account.status!=='error' && account.status!=='closed')account.status='closed';
   if(!settled)finish(false,new Error('WebSocket ditutup sebelum login selesai'));
  });
  ws.on('error',e=>{
   account.status='error'; clearTimeout(timeout);
   if(!settled)finish(false,e);
  });
 });
}

async function sendToOpen(action,room){
 let sent=0,failed=0;
 for(const a of sessions.values()){
  if(a.status!=='ready'||a.ws?.readyState!==WebSocket.OPEN){failed++;continue}
  try{a.ws.send(JSON.stringify({type:action==='join'?'room.join':'room.leave',room}));sent++}
  catch{failed++}
 }
 return {sent,failed};
}

app.post('/api/accounts',async(req,res)=>{
 for(const a of sessions.values())closeSession(a);
 sessions.clear();
 const rawList=Array.isArray(req.body.accounts)?req.body.accounts:[];
 const list=rawList.filter(a=>a&&String(a.username||'').trim());
 if(list.length>50)return res.status(400).json({ok:false,error:'Maksimal 50 WebSocket dalam array'});
 const created=list.map(raw=>({
  id:makeId(),username:String(raw.username).trim(),password:String(raw.password||''),
  status:'new',permissions:[],balance:''
 }));
 created.forEach(a=>sessions.set(a.id,a));

 // Login bertahap: satu perintah HTTP mengendalikan seluruh koneksi, tetapi
 // WebSocket tidak dibuka sekaligus. Ini mencegah burst koneksi membuat server/API
 // menolak akun setelah belasan/dua puluh koneksi. Tidak ada batas jumlah akun
 // yang ditanamkan di aplikasi; akun diproses dalam batch kecil sampai selesai.
 const LOGIN_BATCH_SIZE=Math.max(1,Math.min(5,Number(process.env.LOGIN_BATCH_SIZE)||5));
 const LOGIN_BATCH_GAP_MS=Math.max(0,Number(process.env.LOGIN_BATCH_GAP_MS)||500);
 const results=[];
 for(let start=0;start<created.length;start+=LOGIN_BATCH_SIZE){
  const batch=created.slice(start,start+LOGIN_BATCH_SIZE);
  const batchResults=await Promise.all(batch.map(async a=>{
   try{await connectAccount(a)}
   catch(e){a.status='error';a.error=safe(e.message)}
   return {id:a.id,username:a.username,status:a.status,permissions:a.permissions,balance:a.balance||'',error:a.error||''};
  }));
  results.push(...batchResults);
  if(start+LOGIN_BATCH_SIZE<created.length)await wait(LOGIN_BATCH_GAP_MS);
 }
 const ready=results.filter(a=>a.status==='ready').length;
 res.json({ok:true,count:results.length,ready,accounts:results});
});

app.post('/api/close',(_,res)=>{
 rollState.stop=true; rollState.running=false;
 floodState.stop=true; floodState.running=false;
 for(const a of sessions.values())closeSession(a);
 sessions.clear();
 res.json({ok:true});
});

app.get('/api/status',(_,res)=>res.json({
 ok:true,
 accounts:[...sessions.values()].map(a=>({
  id:a.id,username:a.username,status:a.status,permissions:a.permissions,
  balance:a.balance||'',error:a.error||''
 }))
}));

app.post('/api/room',async(req,res)=>{
 const room=String(req.body.room||'').trim();
 const action=req.body.action;
 if(!room||!['join','leave'].includes(action))
  return res.status(400).json({ok:false,error:'room/action invalid'});
 const result=await sendToOpen(action,room);
 res.json({ok:result.failed===0,...result});
});

app.get('/api/roll/status',(_,res)=>res.json({
 ok:true,running:rollState.running,room:rollState.room,delay:rollState.delay,message:rollState.message
}));

app.post('/api/roll/start',async(req,res)=>{
 if(rollState.running)return res.status(409).json({ok:false,error:'ROLL sedang berjalan'});
 if(floodState.running)return res.status(409).json({ok:false,error:'Hentikan FLOOD sebelum menjalankan ROLL'});
 const room=String(req.body.room||'').trim();
 const delay=Math.max(0,Number(req.body.delay)||0);
 if(!room)return res.status(400).json({ok:false,error:'room wajib diisi'});

 // ROLL hanya menggunakan WebSocket yang sudah login dan masih terbuka.
 // Tidak melakukan LOGIN ulang, LOGOUT, atau menutup koneksi pada akhir putaran.
 const ids=[...sessions.values()].filter(a=>a.status==='ready'&&a.ws?.readyState===WebSocket.OPEN);
 if(!ids.length)return res.status(409).json({ok:false,error:'Tidak ada WebSocket yang sudah login dan siap digunakan'});
 if(ids.length>50)return res.status(409).json({ok:false,error:'ROLL dibatasi maksimal 50 WebSocket'});

 rollState.running=true;rollState.stop=false;rollState.room=room;rollState.delay=delay;
 rollState.message=`ROLL berjalan: ${ids.length} WebSocket siap; tanpa LOGIN/LOGOUT`;

 (async()=>{
  let i=0;
  try{
   while(rollState.running&&!rollState.stop){
    const index=i%ids.length;
    const a=ids[index];
    if(a.status!=='ready'||a.ws?.readyState!==WebSocket.OPEN){
     rollState.message=`WebSocket ${index+1}/${ids.length} tidak siap; dilewati`;
     i++;
     await wait(100);
     continue;
    }
    rollState.message=`WebSocket ${index+1}/${ids.length}: ENTER ROOM`;
    a.ws.send(JSON.stringify({type:'room.join',room}));
    if(rollState.stop||!rollState.running)break;

    rollState.message=`WebSocket ${index+1}/${ids.length}: LEAVE ROOM`;
    if(a.ws?.readyState===WebSocket.OPEN){
     a.ws.send(JSON.stringify({type:'room.leave',room}));
    }
    i++;
    // Delay adalah jeda antar-WebSocket, bukan jeda sebelum ENTER ROOM.
    if(rollState.running&&!rollState.stop&&delay>0){
     rollState.message=`WebSocket berikutnya dalam ${delay} ms`;
     await wait(delay);
    }
   }
  }catch(e){
   rollState.message='ROLL berhenti karena error: '+safe(e.message);
  }finally{
   // Jangan logout atau menutup WebSocket: koneksi tetap login untuk penggunaan berikutnya.
   rollState.running=false;
   rollState.stop=false;
   if(!rollState.message.startsWith('ROLL berhenti'))rollState.message='ROLL dihentikan; WebSocket tetap login.';
  }
 })();

 res.json({ok:true,message:`ROLL dimulai: ENTER → LEAVE berulang pada ${ids.length} WebSocket yang sudah login; tanpa LOGIN/LOGOUT.`});
});

app.post('/api/roll/stop',(_,res)=>{
 rollState.stop=true;
 rollState.running=false;
 rollState.message='ROLL dihentikan.';
 res.json({ok:true,message:rollState.message});
});


app.get('/api/flood/status',(_,res)=>res.json({ok:true,running:floodState.running,room:floodState.room,sent:floodState.sent,failed:floodState.failed,message:floodState.message}));

app.post('/api/flood/start',async(req,res)=>{
 if(floodState.running)return res.status(409).json({ok:false,error:'FLOOD sedang berjalan'});
 if(rollState.running)return res.status(409).json({ok:false,error:'Hentikan ROLL sebelum menjalankan FLOOD'});
 const room=String(req.body.room||'').trim();
 const messages=[req.body.flood1,req.body.flood2,req.body.flood3].map(v=>String(v||'').trim().slice(0,300)).filter(Boolean);
 if(!room)return res.status(400).json({ok:false,error:'Nama room wajib diisi'});
 if(!messages.length)return res.status(400).json({ok:false,error:'Isi minimal satu textbox Flood'});
 const ready=[...sessions.values()].filter(a=>a.status==='ready'&&a.ws?.readyState===WebSocket.OPEN);
 if(!ready.length)return res.status(409).json({ok:false,error:'Tidak ada WebSocket yang sudah login dan siap'});
 if(ready.length>50)return res.status(409).json({ok:false,error:'FLOOD dibatasi maksimal 50 WebSocket'});
 const permitted=ready.filter(a=>Array.isArray(a.permissions)&&a.permissions.includes('messaging.send')&&a.permissions.includes('rooms.join'));
 if(!permitted.length)return res.status(403).json({ok:false,error:'Tidak ada WebSocket dengan izin rooms.join dan messaging.send'});
 floodState.running=true;floodState.stop=false;floodState.room=room;floodState.messages=messages;floodState.sent=0;floodState.failed=0;
 floodState.message=`Memasukkan ${permitted.length} WebSocket ke room sebelum mengirim pesan.`;
 res.json({ok:true,message:`FLOOD dimulai; ${permitted.length} WebSocket akan join room. Setiap akun mengirim maksimal 1 pesan/detik.`});
 (async()=>{
  try{
   // Join all eligible sockets first and wait for direct join results before sending messages.
   const joined=await Promise.all(permitted.map(async a=>{
    try{await sendAndWait(a,{type:'room.join',room},'room.join.result',5000);return a}
    catch(e){floodState.failed++;return null}
   }));
   const active=joined.filter(Boolean);
   if(floodState.stop||!floodState.running)return;
   if(!active.length){floodState.message='FLOOD berhenti: tidak ada WebSocket berhasil masuk room.';return}
   floodState.message=`${active.length} WebSocket masuk room; pengiriman bergantian aktif.`;
   // Independent worker per WebSocket. 125 ms between sends limits each WebSocket to at most 8 sendtext operations per second.
   await Promise.all(active.map(async a=>{
    let idx=0;
    while(floodState.running&&!floodState.stop&&a.status==='ready'&&a.ws?.readyState===WebSocket.OPEN){
     const message=floodState.messages[idx%floodState.messages.length];
     try{
      a.ws.send(JSON.stringify({type:'room.send_message',room:floodState.room,message}));
      floodState.sent++;idx++;
      floodState.message=`FLOOD aktif · terkirim ${floodState.sent} · gagal ${floodState.failed}`;
     }catch{floodState.failed++;break}
     await wait(125);
    }
   }));
  }catch(e){floodState.message='FLOOD error: '+safe(e.message)}
  finally{floodState.running=false;floodState.stop=false;if(!floodState.message.startsWith('FLOOD berhenti:'))floodState.message=`STOP FLOOD: pengiriman baru dihentikan. Terkirim ${floodState.sent}, gagal ${floodState.failed}.`;}
 })();
});

app.post('/api/flood/stop',(_,res)=>{
 floodState.stop=true;floodState.running=false;floodState.message='STOP FLOOD: menghentikan pengiriman pesan baru.';
 res.json({ok:true,message:floodState.message,sent:floodState.sent,failed:floodState.failed});
});

const port=Number(process.env.PORT)||3000;
server.listen(port,()=>console.log('MigMaster Roll listening on '+port));
