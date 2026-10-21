
const http = require('http');
const { WebSocketServer } = require('ws');
const { WebcastPushConnection } = require('tiktok-live-connector');

const PORT = process.env.PORT || 3000;
const srv = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Arena Live servidor online');
});

const wss = new WebSocketServer({ server: srv });
const rooms = new Map();

function broadcast(room, data) {
  const message = JSON.stringify(data);
  room.clients.forEach(client => {
    if (client.readyState === 1) client.send(message);
  });
}

function start(user, room) {
  const conn = room.conn;

  async function connect() {
    if (!room.clients.size || room.connecting) return;
    room.connecting = true;

    try {
      await conn.connect();
      room.ready = true;
      broadcast(room, {
        t: 'status',
        ok: true,
        msg: 'Conectado à LIVE de @' + user
      });
      console.log('Conectado ao TikTok @' + user);
    } catch (err) {
      room.ready = false;
      console.error('Erro TikTok @' + user + ':', err?.message || err);

      broadcast(room, {
        t: 'status',
        ok: false,
        msg: 'Não foi possível conectar à LIVE de @' + user + '. Verifique os logs do servidor.'
      });
    } finally {
      room.connecting = false;
    }

    if (!room.ready && room.clients.size) {
      clearTimeout(room.timer);
      room.timer = setTimeout(connect, 15000);
    }
  }

  conn.on('chat', data => {
    broadcast(room, {
      t: 'chat',
      text: data.comment || '',
      user: data.nickname || data.uniqueId || 'Usuário'
    });
  });

  conn.on('gift', data => {
    if (data.giftType === 1 && !data.repeatEnd) return;

    const icons = data.extendedGiftInfo?.icon?.url_list;
    broadcast(room, {
      t: 'gift',
      name: data.giftName || 'Presente',
      coins: (data.diamondCount || 1) * (data.repeatCount || 1),
      pic: data.giftPictureUrl || icons?.[0] || '',
      user: data.nickname || data.uniqueId || 'Usuário'
    });
  });

  conn.on('disconnected', () => {
    room.ready = false;
    broadcast(room, {
      t: 'status',
      ok: false,
      msg: 'LIVE desconectada. Tentando reconectar...'
    });

    clearTimeout(room.timer);
    room.timer = setTimeout(connect, 5000);
  });

  connect();
}

function leave(ws) {
  const user = ws.room;
  if (!user) return;

  const room = rooms.get(user);
  ws.room = null;
  if (!room) return;

  room.clients.delete(ws);

  if (room.clients.size === 0) {
    clearTimeout(room.timer);
    try {
      room.conn.disconnect();
    } catch (err) {
      console.error('Erro ao desconectar:', err.message);
    }
    rooms.delete(user);
  }
}

function join(ws, username) {
  leave(ws);

  const user = String(username || '')
    .replace(/^@/, '')
    .trim()
    .toLowerCase();

  if (!/^[a-z0-9._]{2,30}$/.test(user)) {
    ws.send(JSON.stringify({
      t: 'status',
      ok: false,
      msg: 'Nome de usuário do TikTok inválido.'
    }));
    return;
  }

  let room = rooms.get(user);

  if (!room) {
    room = {
      clients: new Set(),
      conn: new WebcastPushConnection(user),
      ready: false,
      connecting: false,
      timer: null
    };
    rooms.set(user, room);
    start(user, room);
  }

  room.clients.add(ws);
  ws.room = user;

  if (room.ready) {
    ws.send(JSON.stringify({
      t: 'status',
      ok: true,
      msg: 'Conectado à LIVE de @' + user
    }));
  }
}

wss.on('connection', ws => {
  ws.on('message', message => {
    try {
      const data = JSON.parse(message.toString());
      if (data.t === 'connect') join(ws, data.user);
    } catch (err) {
      console.error('Mensagem WebSocket inválida:', err.message);
    }
  });

  ws.on('close', () => leave(ws));
  ws.on('error', err => {
    console.error('Erro WebSocket:', err.message);
  });
});

srv.listen(PORT, () => {
  console.log('Arena Live servidor iniciado na porta ' + PORT);
});
