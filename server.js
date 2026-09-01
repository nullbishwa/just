/**
 * EduFlow WebRTC Cloud Signaling Server
 * Fully compatible with EduFlow Frontend (Room management, SDP Offer/Answer relay, ICE candidates, and Whiteboard/Chat events)
 */

const express = require('express');
const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());

// Render uses PORT environment variable (default 10000 or 3000)
const PORT = process.env.PORT || 10000;

// HTTP Health Check for Render zero-downtime health probes
app.get('/', (req, res) => {
  res.json({
    status: 'online',
    service: 'EduFlow WebRTC Signaling Gateway',
    timestamp: new Date().toISOString(),
    activeRooms: rooms.size,
    totalConnections: wss.clients.size,
  });
});

app.get('/health', (req, res) => {
  res.status(200).send('OK');
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// Map to store active rooms: roomId -> Map(participantId -> { ws, participant })
const rooms = new Map();

// Helper: Broadcast to all peers in room except sender
function broadcastToRoom(roomId, senderId, message) {
  const room = rooms.get(roomId);
  if (!room) return;

  const dataStr = JSON.stringify(message);
  for (const [peerId, client] of room.entries()) {
    if (peerId !== senderId && client.ws.readyState === WebSocket.OPEN) {
      client.ws.send(dataStr);
    }
  }
}

// Helper: Send direct message to a specific target peer
function sendToPeer(roomId, targetId, message) {
  const room = rooms.get(roomId);
  if (!room) return;

  const targetClient = room.get(targetId);
  if (targetClient && targetClient.ws.readyState === WebSocket.OPEN) {
    targetClient.ws.send(JSON.stringify(message));
  }
}

// WebSocket Connection Handling
wss.on('connection', (ws, req) => {
  let userRoomId = null;
  let userParticipantId = null;

  console.log(`[WS] New client connected from ${req.socket.remoteAddress}`);

  ws.on('message', (rawMessage) => {
    try {
      const message = JSON.parse(rawMessage);
      const { type, roomId, senderId, targetId, payload } = message;

      switch (type) {
        // 1. Join Room
        case 'join-room': {
          userRoomId = roomId;
          userParticipantId = senderId;

          if (!rooms.has(roomId)) {
            rooms.set(roomId, new Map());
          }
          const room = rooms.get(roomId);

          // Get list of existing peers in the room before adding newcomer
          const existingPeers = [];
          for (const [peerId, peerData] of room.entries()) {
            existingPeers.push(peerData.participant || { id: peerId });
          }

          // Register new peer
          room.set(senderId, { ws, participant: payload?.participant });
          console.log(`[ROOM] User ${senderId} joined room "${roomId}". Room size: ${room.size}`);

          // Send list of existing users to newcomer
          ws.send(
            JSON.stringify({
              type: 'room-users',
              roomId,
              payload: { users: existingPeers },
            })
          );

          // Notify existing peers about the newcomer
          broadcastToRoom(roomId, senderId, {
            type: 'user-joined',
            roomId,
            senderId,
            payload: payload || {},
          });
          break;
        }

        // 2. WebRTC SDP Offer (Direct Relay)
        case 'offer': {
          console.log(`[WebRTC] Relay Offer: ${senderId} -> ${targetId} in room ${roomId}`);
          sendToPeer(roomId, targetId, {
            type: 'offer',
            roomId,
            senderId,
            targetId,
            payload,
          });
          break;
        }

        // 3. WebRTC SDP Answer (Direct Relay)
        case 'answer': {
          console.log(`[WebRTC] Relay Answer: ${senderId} -> ${targetId} in room ${roomId}`);
          sendToPeer(roomId, targetId, {
            type: 'answer',
            roomId,
            senderId,
            targetId,
            payload,
          });
          break;
        }

        // 4. ICE Candidates Relay
        case 'ice-candidate': {
          sendToPeer(roomId, targetId, {
            type: 'ice-candidate',
            roomId,
            senderId,
            targetId,
            payload,
          });
          break;
        }

        // 5. Leave Room
        case 'leave-room': {
          handleUserLeave(roomId, senderId);
          break;
        }

        // 6. Broadcast Events (Whiteboard strokes, Chat messages, Hand raises, Polls)
        default: {
          if (targetId) {
            sendToPeer(roomId, targetId, message);
          } else if (roomId) {
            broadcastToRoom(roomId, senderId, message);
          }
          break;
        }
      }
    } catch (err) {
      console.error('[WS Error] Error processing message:', err.message);
    }
  });

  // Handle client disconnection / heartbeat loss
  ws.on('close', () => {
    if (userRoomId && userParticipantId) {
      handleUserLeave(userRoomId, userParticipantId);
    }
    console.log(`[WS] Client disconnected (${userParticipantId || 'unregistered'})`);
  });

  ws.on('error', (err) => {
    console.error(`[WS Socket Error]`, err);
  });
});

function handleUserLeave(roomId, senderId) {
  const room = rooms.get(roomId);
  if (!room) return;

  room.delete(senderId);
  console.log(`[ROOM] User ${senderId} left room "${roomId}". Remaining: ${room.size}`);

  // Broadcast leave event
  broadcastToRoom(roomId, senderId, {
    type: 'user-left',
    roomId,
    senderId,
  });

  // Clean up empty room
  if (room.size === 0) {
    rooms.delete(roomId);
    console.log(`[ROOM] Room "${roomId}" is now empty and removed.`);
  }
}

// Start Server
server.listen(PORT, '0.0.0.0', () => {
  console.log(`=========================================`);
  console.log(`🚀 EduFlow Signaling Server running`);
  console.log(`📡 Port: ${PORT}`);
  console.log(`🔗 Health Check: http://0.0.0.0:${PORT}/health`);
  console.log(`=========================================`);
});
