const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mqtt = require('mqtt');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

// Route Halaman Utama
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

// Konfigurasi Socket.io Connection
io.on('connection', (socket) => {
  console.log('⚡ Client terhubung via Socket.io:', socket.id);
  
  socket.on('disconnect', () => {
    console.log('❌ Client terputus:', socket.id);
  });
});

// Konfigurasi HiveMQ Cloud Broker
const brokerUrl = 'mqtts://d6c3e7f55ab046e4ad3b0230393872d3.s1.eu.hivemq.cloud:8883';
const options = {
  clientId: 'nodejs_backend_' + Math.random().toString(16).substr(2, 8),
  username: 'hidroponik_user',
  password: 'PASSWORD_YANG_ANDA_BUAT', // Pastikan password HiveMQ Anda benar
  rejectUnauthorized: true
};

const mqttClient = mqtt.connect(brokerUrl, options);

mqttClient.on('connect', () => {
  console.log('✅ Terhubung ke HiveMQ Cloud Broker!');
  
  mqttClient.subscribe('hidroponik/sensor', (err) => {
    if (!err) console.log('Subscribed to hidroponik/sensor');
  });
  
  mqttClient.subscribe('hidroponik/status/#', (err) => {
    if (!err) console.log('Subscribed to hidroponik/status/#');
  });
});

mqttClient.on('message', (topic, message) => {
  try {
    const payload = JSON.parse(message.toString());
    console.log(`[MQTT] Data masuk (${topic}):`, payload);
    // Teruskan data sensor ke frontend real-time via Socket.io
    io.emit('sensorData', payload);
  } catch (err) {
    console.log(`[MQTT] Pesan teks (${topic}):`, message.toString());
  }
});

mqttClient.on('error', (err) => {
  console.error('❌ Gagal terhubung ke HiveMQ Cloud:', err.message);
});

// Jalankan Server HTTP (Bukan app.listen)
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🚀 Server berjalan di port ${PORT}`);
});