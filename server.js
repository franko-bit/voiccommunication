const path = require('path');
const http = require('http');
const express = require('express');
const cors = require('cors');
const { WebSocketServer } = require('ws');

const app = express();
app.use(cors());
app.use(express.static(path.join(__dirname, 'public')));

const server = http.createServer(app);
const wss = new WebSocketServer({ server, perMessageDeflate: false });
const rooms = new Map();

wss.on('connection', (ws) => {
  ws.userId = `user_${Math.random().toString(36).slice(2, 10)}`;
  ws.isAlive = true;

  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', (raw) => {
    try {
      const message = JSON.parse(raw.toString());
      if (message.type === 'JOIN_ROOM') joinRoom(ws, message);
      else if (message.type === 'LEAVE_ROOM') leaveRoom(ws);
      else if (message.type === 'GET_ROOMS') sendRoomList(ws);
      else if (['OFFER', 'ANSWER', 'CANDIDATE', 'AUDIO_LEVEL'].includes(message.type)) {
        forwardMessage(ws, message);
      }
    } catch (error) {
      console.error('Invalid WebSocket message:', error.message);
    }
  });
  ws.on('close', () => leaveRoom(ws));
});

function joinRoom(ws, message) {
  leaveRoom(ws);
  const roomName = String(message.room || 'lobby').trim().slice(0, 80) || 'lobby';
  const username = String(message.username || 'Anonymous').trim().slice(0, 40) || 'Anonymous';
  if (!rooms.has(roomName)) rooms.set(roomName, new Map());
  const room = rooms.get(roomName);
  const peers = [...room.values()].map(({ userId, username: peerName }) => ({ userId, username: peerName }));
  room.set(ws.userId, { ws, userId: ws.userId, username });
  ws.currentRoom = roomName;
  send(ws, { type: 'ROOM_JOINED', room: roomName, userId: ws.userId, peers });
  broadcast(room, ws.userId, { type: 'NEW_PEER', peer: { userId: ws.userId, username } });
}

function leaveRoom(ws) {
  if (!ws.currentRoom) return;
  const room = rooms.get(ws.currentRoom);
  if (room) {
    room.delete(ws.userId);
    broadcast(room, ws.userId, { type: 'PEER_LEFT', peerId: ws.userId });
    if (room.size === 0) rooms.delete(ws.currentRoom);
  }
  ws.currentRoom = null;
}

function forwardMessage(ws, message) {
  const room = rooms.get(ws.currentRoom);
  if (!room) return;
  const outgoing = { ...message, sender: ws.userId };
  if (message.target && room.has(message.target)) send(room.get(message.target).ws, outgoing);
  else broadcast(room, ws.userId, outgoing);
}

function sendRoomList(ws) {
  send(ws, { type: 'ROOM_LIST', rooms: [...rooms.entries()].map(([name, room]) => ({ name, count: room.size })) });
}

function broadcast(room, senderId, message) {
  room.forEach(({ ws, userId }) => {
    if (userId !== senderId) send(ws, message);
  });
}

function send(ws, message) {
  if (ws.readyState === 1) ws.send(JSON.stringify(message));
}

const heartbeat = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);
wss.on('close', () => clearInterval(heartbeat));

const port = process.env.PORT || 10000;
server.listen(port, () => console.log(`Voice Chat Pro running at http://localhost:${port}`));
