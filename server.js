const express=require('express');
const http=require('http');
const WebSocket=require('ws');
const path=require('path');
const app=express();
const server=http.createServer(app);
const WSS_URL='wss://developer.mig33.id/developer/ws';
const sessions=new Map();
app.use(express.json({limit:'256kb'}));
app.use(express.static(path.join(__dirname,'public')));
function id(){return Math.random().toString(36).slice(2)+Date.now().toString(36)}
function safe(s){return String(s||'').slice(0,300)}
function closeSession(s){try{s.ws.close()}catch{} if(s.ping)clearInterval(s.ping); sessions.delete(s.id)}
function connectAccount(account){return new Promise((resolve,reject)=>{
 const ws=new WebSocket(WSS_URL); account.ws=ws; account.status='connecting'; let settled=false;
 const done=(ok,err)=>{if(settled)return;settled=true;ok?resolve():reject(err)};
 ws.on('open',()=>{account.status='authenticating'});
 ws.on('message',raw=>{let m;try{m=JSON.parse(raw.toString())}catch{return} account.last=m;
   if(m.type==='auth.required'){try{ws.send(JSON.stringify({type:'developer.login',username:account.username,password:account.password}))}catch(e){done(false,e)}}
   else if(m.type==='session.ready'){account.status='ready';account.permissions=m.data?.developer?.permissions||[];account.wallet=m.data?.wallet||null;account.ping=setInterval(()=>{if(ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify({type:'ping'}))},40000);done(true);}
   else if(m.type==='error' && !settled){account.status='error';done(false,new Error(m.data?.message||m.data?.error||'login failed'))}
 });
 ws.on('close',()=>{if(account.ping)clearInterval(account.ping);if(account.status!=='closed')account.status='closed';if(!settled)done(false,new Error('WebSocket closed before session.ready'))});
 ws.on('error',e=>{account.status='error';if(!settled)done(false,e)});
 })}
app.post('/api/accounts',async(req,res)=>{for(const a of sessions.values())closeSession(a); sessions.clear(); const accounts=Array.isArray(req.body.accounts)?req.body.accounts.slice(0,50):[]; const results=[]; for(const raw of accounts){if(!raw?.username)continue;const a={id:id(),username:String(raw.username),password:String(raw.password||''),status:'new',permissions:[]};sessions.set(a.id,a);try{await connectAccount(a);results.push({id:a.id,username:a.username,status:a.status,permissions:a.permissions})}catch(e){results.push({id:a.id,username:a.username,status:'error',error:safe(e.message)})}}res.json({ok:true,count:results.length,accounts:results})});
app.post('/api/close',(_,res)=>{for(const a of sessions.values())closeSession(a);sessions.clear();res.json({ok:true})});
app.get('/api/status',(_,res)=>res.json({ok:true,accounts:[...sessions.values()].map(a=>({id:a.id,username:a.username,status:a.status,permissions:a.permissions}))}));
app.post('/api/room', (req,res)=>{const room=String(req.body.room||'').trim();const action=req.body.action;if(!room||!['join','leave'].includes(action))return res.status(400).json({ok:false,error:'room/action invalid'});let sent=0,failed=0;for(const a of sessions.values()){if(a.ws?.readyState!==WebSocket.OPEN){failed++;continue}try{a.ws.send(JSON.stringify({type:action==='join'?'room.join':'room.leave',room}));sent++}catch{failed++}}res.json({ok:failed===0,sent,failed})});
app.post('/api/roll/step', (req,res)=>{const room=String(req.body.room||'').trim();const action=req.body.action;if(!room||!['join','leave'].includes(action))return res.status(400).json({ok:false,error:'room/action invalid'});const arr=[...sessions.values()].filter(a=>a.ws?.readyState===WebSocket.OPEN);res.json({ok:true,count:arr.length});});
app.post('/api/roll/send',(req,res)=>{const sessionId=String(req.body.sessionId||'');const room=String(req.body.room||'').trim();const action=req.body.action;const a=sessions.get(sessionId);if(!a||!room||!['join','leave'].includes(action))return res.status(400).json({ok:false,error:'invalid'});if(a.ws?.readyState!==WebSocket.OPEN)return res.status(409).json({ok:false,error:'WebSocket tidak terhubung'});try{a.ws.send(JSON.stringify({type:action==='join'?'room.join':'room.leave',room}));res.json({ok:true,username:a.username,action})}catch(e){res.status(500).json({ok:false,error:safe(e.message)})}});
app.listen(process.env.PORT||3000,()=>console.log('MigMaster Roll listening on '+(process.env.PORT||3000)));
