const path = require('path');
const fs = require('fs');
const http = require('http');
const express = require('express');
const cors = require('cors');
const { WebSocketServer } = require('ws');

const app = express();
app.use(cors());
app.use(express.static(path.join(__dirname, 'public')));

const server = http.createServer(app);
const wss = new WebSocketServer({ server, perMessageDeflate: false });
const roomsFile = path.join(__dirname, 'rooms.json');
const emptyRoomLifetime = 2 * 60 * 1000;
const rooms = loadRooms();

wss.on('connection', (ws) => {
  ws.userId = `user_${Math.random().toString(36).slice(2, 10)}`;
  ws.isAlive = true;

  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', (raw) => {
    try {
      const message = JSON.parse(raw.toString());
      if (message.type === 'JOIN_ROOM') joinRoom(ws, message);
      else if (message.type === 'CREATE_ROOM') createRoom(ws, message);
      else if (message.type === 'LEAVE_ROOM') leaveRoom(ws);
      else if (message.type === 'GET_ROOMS') sendRoomList(ws);
      else if (['OFFER', 'ANSWER', 'CANDIDATE', 'AUDIO_LEVEL', 'CHAT_MESSAGE', 'ROOM_SETTINGS', 'MUTE_PARTICIPANT', 'REMOVE_PARTICIPANT'].includes(message.type)) {
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
  if (!rooms.has(roomName)) return send(ws, { type: 'ROOM_ERROR', message: 'That room does not exist.' });
  const room = rooms.get(roomName);
  if (room.emptyTimer) {
    clearTimeout(room.emptyTimer);
    room.emptyTimer = null;
  }
  room.members ??= new Map();
  room.hostId ??= ws.userId;
  if (room.members.size >= room.limit && !room.members.has(ws.userId)) return send(ws, { type: 'ROOM_ERROR', message: 'This room is full.' });
  const peers = [...room.members.values()].map(({ userId, username: peerName, muted }) => ({ userId, username: peerName, muted: Boolean(muted) }));
  room.members.set(ws.userId, { ws, userId: ws.userId, username, muted: false });
  ws.currentRoom = roomName;
  ws.username = username;
  send(ws, { type: 'ROOM_JOINED', room: roomName, userId: ws.userId, peers, hostId: room.hostId, topic: room.topic, description: room.description, limit: room.limit });
  broadcast(room, ws.userId, { type: 'NEW_PEER', peer: { userId: ws.userId, username } });
  broadcastRoomLists();
}

function createRoom(ws, message) {
  const name = String(message.name || '').trim().slice(0, 80);
  const lang = String(message.lang || 'English').trim().slice(0, 40) || 'English';
  const description = String(message.description || '').trim().slice(0, 300);
  const limit = Math.max(2, Math.min(20, Number(message.limit) || 6));
  if (!name) return send(ws, { type: 'ROOM_ERROR', message: 'Room name is required.' });
  if (rooms.has(name)) return send(ws, { type: 'ROOM_ERROR', message: 'A room with that name already exists.' });
  const room = { name, lang, description, limit, members: new Map(), hostId: null, createdBy: ws.userId };
  rooms.set(name, room);
  scheduleEmptyRoomDeletion(room);
  saveRooms();
  send(ws, { type: 'ROOM_CREATED', room: serializeRoom(room) });
  broadcastRoomLists();
}

function leaveRoom(ws) {
  if (!ws.currentRoom) return;
  const room = rooms.get(ws.currentRoom);
  if (room) {
    room.members.delete(ws.userId);
    if (room.hostId === ws.userId) {
      room.hostId = room.members.keys().next().value;
      if (room.hostId) send(room.members.get(room.hostId).ws, { type: 'HOST_CHANGED', hostId: room.hostId });
    }
    broadcast(room, ws.userId, { type: 'PEER_LEFT', peerId: ws.userId });
    if (room.members.size === 0) scheduleEmptyRoomDeletion(room);
    broadcastRoomLists();
  }
  ws.currentRoom = null;
}

function forwardMessage(ws, message) {
  const room = rooms.get(ws.currentRoom);
  if (!room) return;
  if (['OFFER', 'ANSWER', 'CANDIDATE'].includes(message.type)) {
    const target = message.target;
    if (!target || target === ws.userId) {
      console.warn(`Blocked self-signaling message from ${ws.userId}`);
      return;
    }
    if (room.members.has(target)) {
      send(room.members.get(target).ws, { ...message, sender: ws.userId });
    }
    return;
  }
  if (['MUTE_PARTICIPANT', 'REMOVE_PARTICIPANT'].includes(message.type)) {
    if (ws.userId !== room.hostId || !room.members.has(message.target) || message.target === ws.userId) return;
    const target = room.members.get(message.target);
    if (message.type === 'MUTE_PARTICIPANT') {
      const muted = message.muted !== false;
      target.muted = muted;
      send(target.ws, { type: 'FORCE_MUTE', sender: ws.userId, muted });
      broadcast(room, ws.userId, { type: 'PARTICIPANT_MUTED', peerId: message.target, muted });
      send(ws, { type: 'PARTICIPANT_MUTED', peerId: message.target, muted });
    } else {
      send(target.ws, { type: 'REMOVED_FROM_ROOM', message: 'The room host removed you.' });
      room.members.delete(message.target);
      target.ws.currentRoom = null;
      broadcast(room, ws.userId, { type: 'PEER_LEFT', peerId: message.target });
      if (room.members.size === 0) scheduleEmptyRoomDeletion(room);
      broadcastRoomLists();
    }
    return;
  }
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
  send(ws, { type: 'ROOM_LIST', rooms: getRoomList() });
}

function broadcastRoomLists() {
  const message = { type: 'ROOM_LIST', rooms: getRoomList() };
  wss.clients.forEach(ws => send(ws, message));
}

function getRoomList() {
  return [...rooms.values()].map(serializeRoom);
}

function serializeRoom(room) {
  return {
    name: room.name,
    lang: room.lang,
    description: room.description,
    count: room.members.size,
    limit: room.limit,
    members: [...room.members.values()].map(({ userId, username, muted }) => ({ userId, username, muted: Boolean(muted) }))
  };
}

function loadRooms() {
  try {
    const savedRooms = JSON.parse(fs.readFileSync(roomsFile, 'utf8'));
    const loadedRooms = new Map(savedRooms.map((room) => [room.name, { ...room, members: new Map(), hostId: null }]));
    loadedRooms.forEach(scheduleEmptyRoomDeletion);
    return loadedRooms;
  } catch (error) {
    if (error.code !== 'ENOENT') console.warn('Could not load rooms.json:', error.message);
    return new Map();
  }
}

function saveRooms() {
  const savedRooms = [...rooms.values()].map(({ name, lang, description, limit, createdBy }) => ({ name, lang, description, limit, createdBy }));
  fs.writeFileSync(roomsFile, JSON.stringify(savedRooms, null, 2));
}

function scheduleEmptyRoomDeletion(room) {
  if (room.emptyTimer) clearTimeout(room.emptyTimer);
  room.emptyTimer = setTimeout(() => {
    if (room.members.size > 0 || rooms.get(room.name) !== room) return;
    rooms.delete(room.name);
    saveRooms();
    broadcastRoomLists();
  }, emptyRoomLifetime);
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
