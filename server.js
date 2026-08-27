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
      else if (['OFFER', 'ANSWER', 'CANDIDATE', 'AUDIO_LEVEL', 'CHAT_MESSAGE', 'ROOM_SETTINGS'].includes(message.type)) {
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
  if (!rooms.has(roomName)) rooms.set(roomName, { members: new Map(), hostId: ws.userId, topic: String(message.topic || '').slice(0, 100), description: String(message.description || '').slice(0, 300), limit: Math.max(2, Math.min(20, Number(message.limit) || 6)) });
  const room = rooms.get(roomName);
  if (room.members.size >= room.limit && !room.members.has(ws.userId)) return send(ws, { type: 'ROOM_ERROR', message: 'This room is full.' });
  const peers = [...room.members.values()].map(({ userId, username: peerName }) => ({ userId, username: peerName }));
  room.members.set(ws.userId, { ws, userId: ws.userId, username });
  ws.currentRoom = roomName;
  send(ws, { type: 'ROOM_JOINED', room: roomName, userId: ws.userId, peers, hostId: room.hostId, topic: room.topic, description: room.description, limit: room.limit });
  broadcast(room, ws.userId, { type: 'NEW_PEER', peer: { userId: ws.userId, username } });
}

function leaveRoom(ws) {
  if (!ws.currentRoom) return;
  const room = rooms.get(ws.currentRoom);
  if (room) {
    room.members.delete(ws.userId);
    broadcast(room, ws.userId, { type: 'PEER_LEFT', peerId: ws.userId });
    if (room.members.size === 0) rooms.delete(ws.currentRoom);
  }
  ws.currentRoom = null;
}

function forwardMessage(ws, message) {
  const room = rooms.get(ws.currentRoom);
  if (!room) return;
  if (message.type === 'ROOM_SETTINGS') {
    if (ws.userId !== room.hostId) return;
    room.topic = String(message.topic || '').slice(0, 100);
    room.description = String(message.description || '').slice(0, 300);
    room.limit = Math.max(2, Math.min(20, Number(message.limit) || 6));
  }
  const outgoing = { ...message, sender: ws.userId, username: ws.username || 'Guest' };
  if (message.target && room.members.has(message.target)) send(room.members.get(message.target).ws, outgoing);
  else broadcast(room, ws.userId, outgoing);
}

function sendRoomList(ws) {
  send(ws, { type: 'ROOM_LIST', rooms: [...rooms.entries()].map(([name, room]) => ({ name, count: room.members.size })) });
}

function broadcast(room, senderId, message) {
  room.members.forEach(({ ws, userId }) => {
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
