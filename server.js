const { createServer } = require('http');
const { Server } = require('socket.io');

const httpServer = createServer();
const io = new Server(httpServer, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  pingInterval: 10000,
  pingTimeout: 5000,
});

const PORT = process.env.PORT || 3000;

const W=520,H=520,BALL_R=8,SPEED_MAX=13,C=88;
const PL=54,PLV=54,PTH=16,PTV=16;
const ML=10,EPU=1/3,ECR=1/10000;
const FDR=380,BOOST_MULT=1.55,FR=36,RD=2000;
const TICK_RATE=20;
const BOT_NAMES=['ZEPHYR','GLITCH','NOVA','STORM','BLAZE','PIXEL'];
const POSITIONS=['bottom','top','left','right'];

const CHAMFER_SEGS=[
  {ax:0,ay:C,bx:C,by:0,nx:1/Math.SQRT2,ny:1/Math.SQRT2},
  {ax:W-C,ay:0,bx:W,by:C,nx:-1/Math.SQRT2,ny:1/Math.SQRT2},
  {ax:0,ay:H-C,bx:C,by:H,nx:1/Math.SQRT2,ny:-1/Math.SQRT2},
  {ax:W-C,ay:H,bx:W,by:H-C,nx:-1/Math.SQRT2,ny:-1/Math.SQRT2},
];

const rooms=new Map();

function createRoom(id){
  const room={id,players:{},bots:{},status:'waiting',countdownTimer:null,tickInterval:null,game:null};
  rooms.set(id,room);return room;
}
function findOrCreateRoom(){
  for(const[id,room]of rooms){if(room.status==='waiting'&&Object.keys(room.players).length<4)return room;}
  const id='room_'+Math.random().toString(36).slice(2,8);
  return createRoom(id);
}
function getRoomPlayers(room){return Object.values(room.players);}
function getAvailablePosition(room){
  const taken=getRoomPlayers(room).map(p=>p.pos);
  return POSITIONS.find(p=>!taken.includes(p));
}
function fillBots(room){
  room.bots={};let bi=0;
  for(const pos of POSITIONS){
    const taken=getRoomPlayers(room).map(p=>p.pos);
    if(!taken.includes(pos)){room.bots[pos]={nick:BOT_NAMES[bi%BOT_NAMES.length],rating:490+Math.floor(Math.random()*30)};bi++;}
  }
}
function buildSlots(room){
  const slots={};
  for(const pos of POSITIONS){
    const player=getRoomPlayers(room).find(p=>p.pos===pos);
    if(player)slots[pos]={nick:player.nick,rating:player.rating,isBot:false};
    else if(room.bots[pos])slots[pos]={nick:room.bots[pos].nick,rating:room.bots[pos].rating,isBot:true};
  }
  return slots;
}
function broadcastLobby(room){io.to(room.id).emit('lobby:update',{slots:buildSlots(room),status:room.status});}

function closestPt(px,py,ax,ay,bx,by){
  const dx=bx-ax,dy=by-ay,l2=dx*dx+dy*dy;
  if(!l2)return{cx:ax,cy:ay};
  const t=Math.max(0,Math.min(1,((px-ax)*dx+(py-ay)*dy)/l2));
  return{cx:ax+t*dx,cy:ay+t*dy};
}
function makeBV(){
  let vx,vy,a=0;
  do{const ang=(Math.random()*0.7+0.15)*Math.PI*(Math.random()<0.5?1:-1)+(Math.random()<0.5?0:Math.PI);vx=Math.cos(ang)*(3.5+Math.random()*1.5);vy=Math.sin(ang)*(3.5+Math.random()*1.5);a++;}
  while((Math.abs(vx)<1.8||Math.abs(vy)<1.8)&&a<30);
  return{vx,vy};
}

function createGameState(room){
  const gs={ball:{x:W/2,y:H/2,vx:0,vy:0},respawn:{active:true,timer:RD,vx:0,vy:0},paddles:{},lives:{},scores:{},energy:{},fields:{},eliminated:{},botTargets:{},gameOver:false,winner:null};
  const{vx,vy}=makeBV();gs.respawn.vx=vx;gs.respawn.vy=vy;
  for(const pos of POSITIONS){
    const hz=pos==='top'||pos==='bottom';
    gs.paddles[pos]={
      x:hz?W/2-PL/2:(pos==='left'?2:W-PTV-2),
      y:hz?(pos==='top'?2:H-PTH-2):H/2-PLV/2,
      w:hz?PL:PTV,h:hz?PTH:PLV,
      axis:hz?'x':'y',
      minPos:C,maxPos:hz?W-C-PL:H-C-PLV,
    };
    gs.lives[pos]=ML;gs.scores[pos]=0;gs.energy[pos]=1;
    gs.fields[pos]={active:false,timer:0};gs.eliminated[pos]=false;
    gs.botTargets[pos]=hz?W/2:H/2;
  }
  return gs;
}
function spawnBall(gs){const{vx,vy}=makeBV();gs.ball={x:W/2,y:H/2,vx:0,vy:0};gs.respawn={active:true,timer:RD,vx,vy};}

function resolveChamfers(gs){
  let r=false;
  for(const s of CHAMFER_SEGS){
    const{cx,cy}=closestPt(gs.ball.x,gs.ball.y,s.ax,s.ay,s.bx,s.by);
    const dist=Math.hypot(gs.ball.x-cx,gs.ball.y-cy);
    if(dist<BALL_R+1){
      let nx=gs.ball.x-cx,ny=gs.ball.y-cy;const l=Math.hypot(nx,ny);
      if(l<0.001){nx=s.nx;ny=s.ny;}else{nx/=l;ny/=l;}
      if(nx*s.nx+ny*s.ny<0){nx=-nx;ny=-ny;}
      const dot=gs.ball.vx*nx+gs.ball.vy*ny;
      if(dot<0){gs.ball.vx-=2*dot*nx;gs.ball.vy-=2*dot*ny;}
      gs.ball.x+=nx*(BALL_R+1-dist);gs.ball.y+=ny*(BALL_R+1-dist);
      const spd=Math.hypot(gs.ball.vx,gs.ball.vy);
      if(spd>SPEED_MAX){gs.ball.vx=gs.ball.vx/spd*SPEED_MAX;gs.ball.vy=gs.ball.vy/spd*SPEED_MAX;}
      r=true;
    }
  }
  return r;
}
function clampBall(gs){
  for(const s of CHAMFER_SEGS){
    const dot=(gs.ball.x-s.ax)*s.nx+(gs.ball.y-s.ay)*s.ny;
    if(dot<-BALL_R){
      const dv=gs.ball.vx*s.nx+gs.ball.vy*s.ny;gs.ball.vx-=2*dv*s.nx;gs.ball.vy-=2*dv*s.ny;
      const{cx,cy}=closestPt(gs.ball.x,gs.ball.y,s.ax,s.ay,s.bx,s.by);
      gs.ball.x=cx+s.nx*(BALL_R+1);gs.ball.y=cy+s.ny*(BALL_R+1);
    }
  }
}
function hitRect(ball,p){return ball.x+BALL_R>p.x&&ball.x-BALL_R<p.x+p.w&&ball.y+BALL_R>p.y&&ball.y-BALL_R<p.y+p.h;}
function addSpin(gs,pos){
  const p=gs.paddles[pos];
  if(p.axis==='x'){const r=(gs.ball.x-(p.x+p.w/2))/(p.w/2);gs.ball.vx+=r*2.5;}
  else{const r=(gs.ball.y-(p.y+p.h/2))/(p.h/2);gs.ball.vy+=r*2.5;}
  const sp=Math.hypot(gs.ball.vx,gs.ball.vy),ns=Math.min(sp*1.04,SPEED_MAX*0.75);
  gs.ball.vx=gs.ball.vx/sp*ns;gs.ball.vy=gs.ball.vy/sp*ns;
}
function applyFF(gs,pos){
  const f=gs.fields[pos];if(!f.active)return false;
  const p=gs.paddles[pos];const fcx=p.x+p.w/2,fcy=p.y+p.h/2;
  const dx=gs.ball.x-fcx,dy=gs.ball.y-fcy,dist=Math.hypot(dx,dy);
  if(dist>FR+BALL_R)return false;
  const nx=dist>0.001?dx/dist:0,ny=dist>0.001?dy/dist:1;
  const dot=gs.ball.vx*nx+gs.ball.vy*ny;gs.ball.vx-=2*dot*nx;gs.ball.vy-=2*dot*ny;
  const spd=Math.hypot(gs.ball.vx,gs.ball.vy),ns=Math.min(spd*BOOST_MULT,SPEED_MAX);
  gs.ball.vx=gs.ball.vx/spd*ns;gs.ball.vy=gs.ball.vy/spd*ns;
  gs.ball.x=fcx+nx*(FR+BALL_R+2);gs.ball.y=fcy+ny*(FR+BALL_R+2);
  f.active=false;f.timer=0;return true;
}
function actP(gs){return POSITIONS.filter(p=>!gs.eliminated[p]);}

function predBall(gs,axis,wp){
  let bx=gs.ball.x,by=gs.ball.y,vx=gs.ball.vx,vy=gs.ball.vy;
  for(let i=0;i<300;i++){
    bx+=vx;by+=vy;
    if(bx-BALL_R<0){bx=BALL_R;vx=Math.abs(vx);}if(bx+BALL_R>W){bx=W-BALL_R;vx=-Math.abs(vx);}
    if(by-BALL_R<0){by=BALL_R;vy=Math.abs(vy);}if(by+BALL_R>H){by=H-BALL_R;vy=-Math.abs(vy);}
    if(axis==='x'&&Math.abs(by-wp)<Math.abs(vy)+1)return bx;
    if(axis==='y'&&Math.abs(bx-wp)<Math.abs(vx)+1)return by;
  }
  return axis==='x'?bx:by;
}

function updateBots(room,gs,dt){
  for(const pos of POSITIONS){
    const taken=getRoomPlayers(room).map(p=>p.pos);
    if(taken.includes(pos)||gs.eliminated[pos])continue;
    const p=gs.paddles[pos];
    if(!gs.fields[pos].active&&gs.energy[pos]>=EPU){
      const d=p.axis==='x'?Math.abs(gs.ball.y-(p.y+p.h/2)):Math.abs(gs.ball.x-(p.x+p.w/2));
      if(d<80&&Math.random()<0.04){gs.fields[pos].active=true;gs.fields[pos].timer=0;gs.energy[pos]=Math.max(0,gs.energy[pos]-EPU);}
    }
    const pred=p.axis==='x'?predBall(gs,'x',p.y+p.h/2):predBall(gs,'y',p.x+p.w/2);
    gs.botTargets[pos]+=(pred-gs.botTargets[pos])*0.08;
    const center=p.axis==='x'?p.x+p.w/2:p.y+p.h/2;
    const diff=gs.botTargets[pos]-center;
    if(Math.abs(diff)>3){
      const step=Math.sign(diff)*Math.min(3.5,Math.abs(diff));
      if(p.axis==='x')p.x=Math.max(p.minPos,Math.min(p.maxPos,p.x+step));
      else p.y=Math.max(p.minPos,Math.min(p.maxPos,p.y+step));
    }
  }
}

// Приймаємо input від гравця — ОДРАЗУ застосовуємо до стану
function applyPlayerInput(room,gs,pos,paddlePos,boost){
  if(gs.eliminated[pos])return;
  const p=gs.paddles[pos];
  // Обмежуємо позицію і застосовуємо
  const half=p.axis==='x'?p.w/2:p.h/2;
  const clamped=Math.max(p.minPos+half,Math.min(p.maxPos+half,paddlePos));
  if(p.axis==='x')p.x=clamped-p.w/2;
  else p.y=clamped-p.h/2;
  // Силове поле
  if(boost&&!gs.fields[pos].active&&gs.energy[pos]>=EPU){
    gs.fields[pos].active=true;gs.fields[pos].timer=0;
    gs.energy[pos]=Math.max(0,gs.energy[pos]-EPU);
  }
}

function tickGame(room,dt){
  const gs=room.game;if(!gs||gs.gameOver)return;
  for(const pos of POSITIONS){
    if(gs.eliminated[pos])continue;
    if(gs.fields[pos].active){gs.fields[pos].timer+=dt;if(gs.fields[pos].timer>=FDR){gs.fields[pos].active=false;gs.fields[pos].timer=0;}}
    if(!gs.fields[pos].active)gs.energy[pos]=Math.min(1,gs.energy[pos]+ECR*dt);
  }
  updateBots(room,gs,dt);
  if(gs.respawn.active){gs.respawn.timer-=dt;if(gs.respawn.timer<=0){gs.respawn.active=false;gs.ball.vx=gs.respawn.vx;gs.ball.vy=gs.respawn.vy;}broadcastState(room);return;}
  gs.ball.x+=gs.ball.vx;gs.ball.y+=gs.ball.vy;
  for(const pos of POSITIONS){if(gs.eliminated[pos])continue;if(applyFF(gs,pos)){broadcastState(room);return;}}
  for(let i=0;i<3;i++)if(resolveChamfers(gs))break;
  clampBall(gs);
  for(const pos of POSITIONS){
    if(gs.eliminated[pos])continue;
    const p=gs.paddles[pos];
    if(hitRect(gs.ball,p)){
      if(p.axis==='x'){gs.ball.y=pos==='top'?p.y+p.h+BALL_R:p.y-BALL_R;gs.ball.vy=pos==='top'?Math.abs(gs.ball.vy):-Math.abs(gs.ball.vy);}
      else{gs.ball.x=pos==='left'?p.x+p.w+BALL_R:p.x-BALL_R;gs.ball.vx=pos==='left'?Math.abs(gs.ball.vx):-Math.abs(gs.ball.vx);}
      addSpin(gs,pos);broadcastState(room);return;
    }
  }
  const goal=(pos)=>{
    gs.scores[pos]++;gs.lives[pos]--;
    io.to(room.id).emit('game:goal',{pos,lives:gs.lives[pos],scores:gs.scores[pos]});
    if(gs.lives[pos]<=0){gs.eliminated[pos]=true;io.to(room.id).emit('game:eliminated',{pos});if(actP(gs).length===1)endGame(room,gs,actP(gs)[0]);else if(!gs.gameOver)spawnBall(gs);}
    else if(!gs.gameOver)spawnBall(gs);
  };
  if(gs.ball.y-BALL_R<0&&gs.ball.x>C&&gs.ball.x<W-C){if(!gs.eliminated.top)goal('top');else gs.ball.vy=Math.abs(gs.ball.vy);}
  else if(gs.ball.y+BALL_R>H&&gs.ball.x>C&&gs.ball.x<W-C){if(!gs.eliminated.bottom)goal('bottom');else gs.ball.vy=-Math.abs(gs.ball.vy);}
  else if(gs.ball.x-BALL_R<0&&gs.ball.y>C&&gs.ball.y<H-C){if(!gs.eliminated.left)goal('left');else gs.ball.vx=Math.abs(gs.ball.vx);}
  else if(gs.ball.x+BALL_R>W&&gs.ball.y>C&&gs.ball.y<H-C){if(!gs.eliminated.right)goal('right');else gs.ball.vx=-Math.abs(gs.ball.vx);}
  broadcastState(room);
}

function broadcastState(room){
  const gs=room.game;if(!gs)return;
  // Надсилаємо тільки м'яч, поля, енергію — НЕ позиції ракеток гравців (вони вже локально)
  io.to(room.id).emit('game:state',{
    ball:gs.ball,respawn:gs.respawn,
    paddles:gs.paddles, // сервер надсилає для корекції якщо треба
    energy:gs.energy,fields:gs.fields,
    lives:gs.lives,scores:gs.scores,eliminated:gs.eliminated,
  });
}

function endGame(room,gs,winner){
  gs.gameOver=true;if(room.tickInterval){clearInterval(room.tickInterval);room.tickInterval=null;}
  room.status='finished';io.to(room.id).emit('game:over',{winner,slots:buildSlots(room)});
}

function startGame(room){
  room.status='playing';fillBots(room);room.game=createGameState(room);
  broadcastLobby(room);io.to(room.id).emit('game:start',{slots:buildSlots(room)});
  let last=Date.now();
  room.tickInterval=setInterval(()=>{const now=Date.now();tickGame(room,now-last);last=now;},1000/TICK_RATE);
}

io.on('connection',(socket)=>{
  console.log('Connected:',socket.id);
  let myRoom=null,myPos=null;

  socket.on('mm:join',({nick,rating,uid})=>{
    const room=findOrCreateRoom();myRoom=room;myPos=getAvailablePosition(room);
    if(!myPos){socket.emit('mm:error','Кімната повна');return;}
    room.players[socket.id]={pos:myPos,nick,rating,uid};
    socket.join(room.id);socket.emit('mm:joined',{roomId:room.id,pos:myPos});broadcastLobby(room);
    if(!room.countdownTimer&&room.status==='waiting'){
      room.status='countdown';let tl=10;broadcastLobby(room);io.to(room.id).emit('mm:countdown',{timeLeft:tl});
      room.countdownTimer=setInterval(()=>{
        tl--;io.to(room.id).emit('mm:countdown',{timeLeft:tl});
        if(tl<=0){clearInterval(room.countdownTimer);room.countdownTimer=null;startGame(room);}
      },1000);
    }
  });

  socket.on('player:input',({paddlePos,boost})=>{
    if(!myRoom||!myPos||!myRoom.game||myRoom.game.gameOver)return;
    applyPlayerInput(myRoom,myRoom.game,myPos,paddlePos,boost);
  });

  socket.on('mm:cancel',()=>leave());
  socket.on('disconnect',()=>leave());

  function leave(){
    if(!myRoom)return;
    delete myRoom.players[socket.id];socket.leave(myRoom.id);
    if(Object.keys(myRoom.players).length===0){
      if(myRoom.tickInterval)clearInterval(myRoom.tickInterval);
      if(myRoom.countdownTimer)clearInterval(myRoom.countdownTimer);
      rooms.delete(myRoom.id);
    }else{
      broadcastLobby(myRoom);
      if(myRoom.game&&!myRoom.game.gameOver&&myPos){
        myRoom.bots[myPos]={nick:BOT_NAMES[0],rating:500};
        io.to(myRoom.id).emit('game:player_left',{pos:myPos});
      }
    }
    myRoom=null;myPos=null;
  }
});

httpServer.listen(PORT,()=>console.log(`Server on port ${PORT}`));
