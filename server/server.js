// Kleiner Presence- und Wurf-Broadcast-Server fuer den Wuerfeltisch.
//
// Haelt pro Verbindung einen Namen und sendet bei jeder Aenderung die
// aktuelle Liste aller aktiven Spieler an alle verbundenen Clients.
// Zusaetzlich werden Wuerfe an alle anderen Clients weitergereicht, damit
// alle am Tisch dieselbe Wurf-Historie live sehen.
//
// Start lokal:  npm install && npm start

const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const HEARTBEAT_INTERVAL_MS = 30000;
const MAX_NAME_LENGTH = 20;
const MAX_HISTORY = 20;

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Wuerfeltisch presence server is running.\n');
});

const wss = new WebSocketServer({ server });

const clients = new Map();
let sharedHistory = [];

function sanitizeName(rawName) {
  if (typeof rawName !== 'string') return '';
  return rawName.trim().slice(0, MAX_NAME_LENGTH);
}

function broadcast(message, exclude) {
  const payload = JSON.stringify(message);
  for (const client of wss.clients) {
    if (client.readyState === client.OPEN && client !== exclude) {
      client.send(payload);
    }
  }
}

function broadcastPresence() {
  const names = [...clients.values()]
    .map((entry) => entry.name)
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b, 'de'));
  broadcast({ type: 'presence', names });
}

wss.on('connection', (ws) => {
  clients.set(ws, { name: '' });
  ws.isAlive = true;

  ws.send(JSON.stringify({ type: 'presence', names: [...clients.values()].map((e) => e.name).filter(Boolean) }));
  ws.send(JSON.stringify({ type: 'history', entries: sharedHistory }));

  ws.on('pong', () => {
    ws.isAlive = true;
  });

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch (error) {
      return;
    }

    if (!msg || typeof msg.type !== 'string') return;

    if (msg.type === 'join') {
      const entry = clients.get(ws);
      if (!entry) return;
      entry.name = sanitizeName(msg.name);
      broadcastPresence();
      return;
    }

    if (msg.type === 'roll') {
      const entry = clients.get(ws);
      const name = (entry && entry.name) || 'Spieler';
      const rollEntry = {
        name,
        sides: String(msg.sides || ''),
        roll: Number(msg.roll),
        result: msg.result && typeof msg.result === 'object'
          ? {
              success: Boolean(msg.result.success),
              threshold: msg.result.threshold,
              detail: typeof msg.result.detail === 'string' ? msg.result.detail.slice(0, 200) : ''
            }
          : null,
        time: new Date().toLocaleTimeString('de-DE', { hour: '2-digit', minute: '2-digit' })
      };
      sharedHistory.unshift(rollEntry);
      if (sharedHistory.length > MAX_HISTORY) sharedHistory.pop();
      broadcast({ type: 'roll', entry: rollEntry });
      return;
    }
  });

  ws.on('close', () => {
    clients.delete(ws);
    broadcastPresence();
  });

  ws.on('error', () => {
    clients.delete(ws);
    broadcastPresence();
  });
});

const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, HEARTBEAT_INTERVAL_MS);

wss.on('close', () => clearInterval(heartbeat));

server.listen(PORT, () => {
  console.log(`Wuerfeltisch presence server listening on port ${PORT}`);
});
