const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*' }
});

app.use(express.static(path.join(__dirname, 'public')));

// File d'attente par pays : { country: [socketId, ...] }
const queues = {};
// Utilisateurs connectés : { socketId: { country, partnerId } }
const users = {};

function findMatch(socketId, country) {
  const user = users[socketId];
  if (!user) return null;

  const targets = country === 'ANY'
    ? Object.keys(queues)
    : [country, 'ANY'];

  for (const c of targets) {
    if (!queues[c]) continue;
    const idx = queues[c].findIndex(id => id !== socketId);
    if (idx !== -1) {
      const partnerId = queues[c].splice(idx, 1)[0];
      if (queues[c].length === 0) delete queues[c];
      return partnerId;
    }
  }
  return null;
}

function addToQueue(socketId, country) {
  if (!queues[country]) queues[country] = [];
  if (!queues[country].includes(socketId)) {
    queues[country].push(socketId);
  }
}

function removeFromQueue(socketId) {
  for (const c in queues) {
    queues[c] = queues[c].filter(id => id !== socketId);
    if (queues[c].length === 0) delete queues[c];
  }
}

io.on('connection', (socket) => {
  console.log('✅ Connecté:', socket.id);
  users[socket.id] = { country: 'ANY', partnerId: null };

  // Recherche d'un partenaire
  socket.on('find-partner', ({ country }) => {
    const user = users[socket.id];
    if (!user) return;

    // Si déjà en appel, ignorer
    if (user.partnerId) return;

    user.country = country || 'ANY';
    removeFromQueue(socket.id);

    const partnerId = findMatch(socket.id, user.country);

    if (partnerId && users[partnerId]) {
      // Match trouvé !
      user.partnerId = partnerId;
      users[partnerId].partnerId = socket.id;

      // On désigne un "initiateur" WebRTC
      socket.emit('match-found', { partnerId, isInitiator: true });
      io.to(partnerId).emit('match-found', { partnerId: socket.id, isInitiator: false });

      console.log(`🔗 Match: ${socket.id} ↔ ${partnerId}`);
    } else {
      // Mise en file d'attente
      addToQueue(socket.id, user.country);
      socket.emit('waiting');
      console.log(`⏳ En attente [${user.country}]: ${socket.id}`);
    }
  });

  // Relais signaling WebRTC
  socket.on('signal', ({ to, signal }) => {
    io.to(to).emit('signal', { from: socket.id, signal });
  });

  // Passer au suivant
  socket.on('next', () => {
    const user = users[socket.id];
    if (!user) return;

    if (user.partnerId) {
      const partner = users[user.partnerId];
      if (partner) {
        partner.partnerId = null;
        io.to(user.partnerId).emit('partner-left');
      }
      user.partnerId = null;
    }
    removeFromQueue(socket.id);
    socket.emit('ready'); // prêt à chercher à nouveau
  });

  // Message chat
  socket.on('chat-message', ({ message }) => {
    const user = users[socket.id];
    if (user && user.partnerId) {
      io.to(user.partnerId).emit('chat-message', { message });
    }
  });

  // Déconnexion
  socket.on('disconnect', () => {
    const user = users[socket.id];
    if (user && user.partnerId) {
      const partner = users[user.partnerId];
      if (partner) {
        partner.partnerId = null;
        io.to(user.partnerId).emit('partner-left');
      }
    }
    removeFromQueue(socket.id);
    delete users[socket.id];
    console.log('❌ Déconnecté:', socket.id);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🚀 Serveur lancé sur le port ${PORT}`));
