const express = require('express');
const mqtt = require('mqtt');
const path = require('path');

const app = express();

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

// ROUTE HALAMAN UTAMA
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

// Konfigurasi HiveMQ Cloud
const brokerUrl = 'mqtts://d6c3e7f55ab046e4ad3b0230393872d3.s1.eu.hivemq.cloud:8883';
const options = {
  clientId: 'nodejs_backend_' + Math.random().toString(16).substr(2, 8),
  username: 'hidroponik_user',
  password: 'PASSWORD_YANG_ANDA_BUAT',
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

mqttClient.on('error', (err) => {
  console.error('❌ Gagal terhubung ke HiveMQ Cloud:', err.message);
});

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log(`🚀 Server berjalan di port ${PORT}`);
});