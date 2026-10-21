// Servidor que lê a live do TikTok pelo @ e repassa comentários e presentes para o site.
const http = require('http');
const { WebSocketServer } = require('ws');
const { WebcastPushConnection } = require('tiktok-live-connector');
const PORT = process.env.PORT || 3000;
const srv = http.createServer((q, r) => { r.writeHead(200); r.end('ok'); });
const wss = new WebSocketServer({ server: srv });
const rooms = new Map(); // @ -> { conn, clients, ready }

const bc = (r, o) => { const s = JSON.stringify(o); r.clients.forEach(c => c.readyState === 1 && c.send(s)); };

function start(user, r) {
  const c = r.conn;
  const go = () => c.connect()
    .then(() => { r.ready = true; bc(r, { t: 'status', ok: true, msg: 'Conectado a @' + user }); })
    .catch(() => {
      r.ready = false; bc(r, { t: 'status', ok: false, msg: 'Live de @' + user + ' fechada ou não encontrada. Tentando de novo...' });
      r.timer = setTimeout(() => r.clients.size ? go() : rooms.delete(user), 10000);
    });
  c.on('chat', d => bc(r, { t: 'chat', text: d.comment || '', user: d.nickname || d.uniqueId }));
  c.on('gift', d => {
    if (d.giftType === 1 && !d.repeatEnd) return; // sequência ainda em andamento
    const ic = d.extendedGiftInfo && d.extendedGiftInfo.icon && d.extendedGiftInfo.icon.url_list;
    bc(r, { t: 'gift', name: d.giftName || '', coins: (d.diamondCount || 1) * (d.repeatCount || 1), pic: d.giftPictureUrl || (ic && ic[0]) || '', user: d.nickname || d.uniqueId });
  });
  c.on('disconnected', () => {
    r.ready = false; bc(r, { t: 'status', ok: false, msg: 'Live desconectada. Reconectando...' });
    setTimeout(() => r.clients.size ? go() : rooms.delete(user), 5000);
  });
  go();
}
function leave(ws) {
  const r = rooms.get(ws.room); if (!r) return;
  r.clients.delete(ws);
  if (!r.clients.size) { clearTimeout(r.timer); try { r.conn.disconnect(); } catch (e) {} rooms.delete(ws.room); }
  ws.room = null;
}
function join(ws, user) {
  leave(ws);
  user = String(user || '').replace('@', '').trim().toLowerCase();
  if (!/^[a-z0-9._]{2,30}$/.test(user)) return ws.send(JSON.stringify({ t: 'status', ok: false, msg: '@ inválido' }));
  let r = rooms.get(user);
  if (!r) { r = { clients: new Set(), conn: new WebcastPushConnection(user), ready: false }; rooms.set(user, r); start(user, r); }
  r.clients.add(ws); ws.room = user;
  if (r.ready) ws.send(JSON.stringify({ t: 'status', ok: true, msg: 'Conectado a @' + user }));
}
wss.on('connection', ws => {
  ws.on('message', m => { try { const d = JSON.parse(m); if (d.t === 'connect') join(ws, d.user); } catch (e) {} });
  ws.on('close', () => leave(ws));
});
srv.listen(PORT, () => console.log('Servidor na porta ' + PORT));
