if (!process.env.TZ) {
  process.env.TZ = 'Asia/Jakarta';
}

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mqtt = require('mqtt');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

// Middleware & Static Files (index.html berada di folder 'public')
app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

function getLocalDateParts(dateObj = new Date()) {
  const year = dateObj.getFullYear();
  const month = String(dateObj.getMonth() + 1).padStart(2, '0');
  const day = String(dateObj.getDate()).padStart(2, '0');
  const hours = String(dateObj.getHours()).padStart(2, '0');
  const minutes = String(dateObj.getMinutes()).padStart(2, '0');
  const seconds = String(dateObj.getSeconds()).padStart(2, '0');
  return {
    tanggal: `${year}-${month}-${day}`,
    waktu: `${hours}:${minutes}:${seconds}`,
    jamMenit: `${hours}:${minutes}`
  };
}

function isValidNumber(val) {
  return typeof val === 'number' && Number.isFinite(val);
}

// --- Konfigurasi Default Pengaturan Sistem (Tersinkronisasi dengan SQLite) ---
const DEFAULT_SYSTEM_SETTINGS = {
  tempMin: 20,
  tempMax: 30,
  phMin: 5.5,
  phMax: 6.8,
  ecMin: 1.0,
  ecMax: 3.0,
  phDriftMinSafe: 5.5,
  phDriftMaxSafe: 6.8,
  maxDosingVolume: 500,
  deviceId: 'ESP32-MELON-01',
  farmLocation: 'Kebun Mitra Cocopeat',
  pumpEnclosureNote: 'Pompa dipisah di box terpisah + konektor 6-pin GX16 (anti EMI/noise)',
  calibrationNote: 'pH & EC dikalibrasi berkala menggunakan larutan buffer & pH meter',
  updatedAt: '-'
};

let systemSettings = { ...DEFAULT_SYSTEM_SETTINGS };

function sanitizeSystemSettings(raw = {}, prev = systemSettings) {
  const num = (val, fallback, min, max, decimals = 2) => {
    const n = Number(val);
    if (!Number.isFinite(n)) return fallback;
    const clamped = Math.min(max, Math.max(min, n));
    const factor = Math.pow(10, decimals);
    return Math.round(clamped * factor) / factor;
  };

  let tempMin = num(raw.tempMin, prev.tempMin, 0, 50, 1);
  let tempMax = num(raw.tempMax, prev.tempMax, 0, 60, 1);
  if (tempMin > tempMax) {
    const tmp = tempMin;
    tempMin = tempMax;
    tempMax = tmp;
  }

  let phMin = num(raw.phMin, prev.phMin, 0, 14, 2);
  let phMax = num(raw.phMax, prev.phMax, 0, 14, 2);
  if (phMin > phMax) {
    const tmp = phMin;
    phMin = phMax;
    phMax = tmp;
  }

  let ecMin = num(raw.ecMin, prev.ecMin, 0, 10, 2);
  let ecMax = num(raw.ecMax, prev.ecMax, 0, 15, 2);
  if (ecMin > ecMax) {
    const tmp = ecMin;
    ecMin = ecMax;
    ecMax = tmp;
  }

  let phDriftMinSafe = num(raw.phDriftMinSafe, prev.phDriftMinSafe, 0, 14, 2);
  let phDriftMaxSafe = num(raw.phDriftMaxSafe, prev.phDriftMaxSafe, 0, 14, 2);
  if (phDriftMinSafe > phDriftMaxSafe) {
    const tmp = phDriftMinSafe;
    phDriftMinSafe = phDriftMaxSafe;
    phDriftMaxSafe = tmp;
  }

  const maxDosingVolume = Math.round(num(raw.maxDosingVolume, prev.maxDosingVolume, 1, 500, 0));

  const str = (val, fallback, maxLen = 120) => {
    if (typeof val !== 'string') return fallback;
    return val.trim().slice(0, maxLen);
  };

  return {
    tempMin,
    tempMax,
    phMin,
    phMax,
    ecMin,
    ecMax,
    phDriftMinSafe,
    phDriftMaxSafe,
    maxDosingVolume,
    deviceId: str(raw.deviceId, prev.deviceId, 60) || DEFAULT_SYSTEM_SETTINGS.deviceId,
    farmLocation: str(raw.farmLocation, prev.farmLocation, 100) || DEFAULT_SYSTEM_SETTINGS.farmLocation,
    pumpEnclosureNote: str(raw.pumpEnclosureNote, prev.pumpEnclosureNote, 180),
    calibrationNote: str(raw.calibrationNote, prev.calibrationNote, 180),
    updatedAt: prev.updatedAt || '-'
  };
}

function emitSystemSettings(target = io) {
  target.emit('systemSettingsStatus', { ...systemSettings });
}

// Referensi Fase Tanaman Melon (bisa disesuaikan dengan standar budidaya)
const CROP_PHASE_PROFILES = {
  BIBIT: {
    key: 'BIBIT',
    label: 'Fase Bibit / Semai',
    hstRange: 'HST 0 – 14',
    phGuide: '5.8 – 6.5',
    ecGuide: '0.8 – 1.4 mS/cm',
    tempGuide: '22 – 28 °C',
    summary: 'Perakaran awal pada media cocopeat; jaga kelembapan stabil tanpa tergenang dan gunakan dosis nutrisi ringan.'
  },
  VEGETATIF: {
    key: 'VEGETATIF',
    label: 'Fase Vegetatif',
    hstRange: 'HST 15 – 30',
    phGuide: '5.5 – 6.5',
    ecGuide: '1.5 – 2.2 mS/cm',
    tempGuide: '22 – 29 °C',
    summary: 'Pertumbuhan daun, batang, dan sulur aktif; pertahankan pH serta EC konsisten untuk pembentukan kanopi.'
  },
  PEMBUNGAAN_PEMBUAHAN: {
    key: 'PEMBUNGAAN_PEMBUAHAN',
    label: 'Fase Pembungaan & Pembuahan',
    hstRange: 'HST 31 – 45',
    phGuide: '5.5 – 6.5',
    ecGuide: '2.0 – 2.6 mS/cm',
    tempGuide: '22 – 29 °C',
    summary: 'Muncul bunga, polinasi/seleksi buah, dan pembesaran awal; jaga kestabilan nutrisi agar calon buah tidak rontok.'
  },
  PEMBESARAN_PANEN: {
    key: 'PEMBESARAN_PANEN',
    label: 'Fase Pembesaran & Pematangan Buah',
    hstRange: 'HST 46 – Panen',
    phGuide: '5.8 – 6.8',
    ecGuide: '2.2 – 2.8 mS/cm',
    tempGuide: '22 – 30 °C',
    summary: 'Pembesaran buah dan peningkatan kualitas hasil; pantau EC, pH, serta jadwal dosing secara cermat.'
  }
};

let currentCropPhase = {
  phaseKey: 'VEGETATIF',
  hss: 20,
  hst: 15,
  notes: 'Pertumbuhan vegetatif awal pada media cocopeat.',
  updatedAt: '-'
};

function buildCropPhasePayload() {
  const profile = CROP_PHASE_PROFILES[currentCropPhase.phaseKey] || CROP_PHASE_PROFILES.VEGETATIF;
  return {
    ...currentCropPhase,
    profile,
    availablePhases: Object.values(CROP_PHASE_PROFILES)
  };
}

function emitCropPhase(target = io) {
  target.emit('cropPhaseStatus', buildCropPhasePayload());
}

// 1. Inisialisasi Database SQLite Lokal
const dbFile = path.join(__dirname, 'hydroponic.db');
const db = new sqlite3.Database(dbFile, (err) => {
  if (err) {
    console.error('Gagal terhubung ke database SQLite:', err.message);
  } else {
    console.log('Terhubung ke database SQLite lokal.');
    db.serialize(() => {
      db.run(`CREATE TABLE IF NOT EXISTS sensor_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tanggal TEXT,
        waktu TEXT,
        suhuAir REAL,
        ph REAL,
        ec REAL
      )`);

      db.run(`CREATE TABLE IF NOT EXISTS dosing_schedules (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        jam TEXT NOT NULL,
        pompa TEXT NOT NULL,
        volume INTEGER DEFAULT 0,
        volumeA INTEGER DEFAULT 0,
        volumeB INTEGER DEFAULT 0,
        enabled INTEGER DEFAULT 1,
        lastTriggeredDate TEXT DEFAULT NULL
      )`);

      db.run(`CREATE TABLE IF NOT EXISTS crop_phase (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        phaseKey TEXT NOT NULL,
        hss INTEGER DEFAULT 0,
        hst INTEGER DEFAULT 0,
        notes TEXT DEFAULT '',
        updatedAt TEXT DEFAULT ''
      )`, () => {
        db.run(`ALTER TABLE crop_phase ADD COLUMN hss INTEGER DEFAULT 0`, () => {
          db.get(`SELECT * FROM crop_phase WHERE id = 1`, [], (rowErr, row) => {
            if (rowErr) {
              console.error('Gagal membaca fase tanaman:', rowErr.message);
              return;
            }
            if (row) {
              currentCropPhase = {
                phaseKey: CROP_PHASE_PROFILES[row.phaseKey] ? row.phaseKey : 'VEGETATIF',
                hss: Number(row.hss) || 0,
                hst: Number(row.hst) || 0,
                notes: row.notes || '',
                updatedAt: row.updatedAt || '-'
              };
            } else {
              const { tanggal, waktu } = getLocalDateParts();
              const initStamp = `${tanggal} ${waktu}`;
              db.run(
                `INSERT INTO crop_phase (id, phaseKey, hss, hst, notes, updatedAt) VALUES (1, ?, ?, ?, ?, ?)`,
                [currentCropPhase.phaseKey, currentCropPhase.hss, currentCropPhase.hst, currentCropPhase.notes, initStamp],
                () => {
                  currentCropPhase.updatedAt = initStamp;
                }
              );
            }
          });
        });
      });

      db.run(`CREATE TABLE IF NOT EXISTS system_settings (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        tempMin REAL,
        tempMax REAL,
        phMin REAL,
        phMax REAL,
        ecMin REAL,
        ecMax REAL,
        phDriftMinSafe REAL,
        phDriftMaxSafe REAL,
        maxDosingVolume INTEGER,
        deviceId TEXT,
        farmLocation TEXT,
        pumpEnclosureNote TEXT,
        calibrationNote TEXT,
        updatedAt TEXT
      )`, () => {
        db.get(`SELECT * FROM system_settings WHERE id = 1`, [], (sErr, row) => {
          if (sErr) {
            console.error('Gagal membaca pengaturan sistem:', sErr.message);
            return;
          }
          if (row) {
            systemSettings = sanitizeSystemSettings(row, {
              ...DEFAULT_SYSTEM_SETTINGS,
              updatedAt: row.updatedAt || '-'
            });
          } else {
            const { tanggal, waktu } = getLocalDateParts();
            const initStamp = `${tanggal} ${waktu}`;
            systemSettings = { ...DEFAULT_SYSTEM_SETTINGS, updatedAt: initStamp };
            db.run(
              `INSERT INTO system_settings (
                id, tempMin, tempMax, phMin, phMax, ecMin, ecMax,
                phDriftMinSafe, phDriftMaxSafe, maxDosingVolume,
                deviceId, farmLocation, pumpEnclosureNote, calibrationNote, updatedAt
              ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
              [
                systemSettings.tempMin, systemSettings.tempMax,
                systemSettings.phMin, systemSettings.phMax,
                systemSettings.ecMin, systemSettings.ecMax,
                systemSettings.phDriftMinSafe, systemSettings.phDriftMaxSafe,
                systemSettings.maxDosingVolume, systemSettings.deviceId,
                systemSettings.farmLocation, systemSettings.pumpEnclosureNote,
                systemSettings.calibrationNote, systemSettings.updatedAt
              ]
            );
          }
        });
      });
    });
  }
});

// Status global aktuator untuk sinkronisasi awal saat klien terhubung
let actuatorStates = {
  main: false,
  nutA: false,
  nutB: false,
  phUp: false,
  phDown: false,
  valve: false
};

let lastSensorUpdate = null;

// --- Status pH Drift Lock ---
let phDriftLock = {
  locked: false,
  reason: '',
  lastPh: null,
  updatedAt: null
};

function emitPhDriftLock(target = io) {
  target.emit('phDriftLockStatus', {
    ...phDriftLock,
    minSafe: systemSettings.phDriftMinSafe,
    maxSafe: systemSettings.phDriftMaxSafe
  });
}

function evaluatePhDriftLock(phValue, waktuStr) {
  if (!isValidNumber(phValue)) return;
  const minSafe = systemSettings.phDriftMinSafe;
  const maxSafe = systemSettings.phDriftMaxSafe;

  if (phValue < minSafe || phValue > maxSafe) {
    const reason = phValue < minSafe
      ? `pH (${phValue}) di bawah batas aman (${minSafe})`
      : `pH (${phValue}) di atas batas aman (${maxSafe})`;

    const justLocked = !phDriftLock.locked || phDriftLock.reason !== reason;
    phDriftLock = {
      locked: true,
      reason,
      lastPh: phValue,
      updatedAt: waktuStr
    };
    if (justLocked) {
      emitPhDriftLock(io);
    }
  } else {
    phDriftLock.lastPh = phValue;
  }
}

function formatScheduleRow(row) {
  return {
    id: row.id,
    jam: row.jam,
    pompa: row.pompa,
    volume: Number(row.volume || 0),
    volumeA: Number(row.volumeA || 0),
    volumeB: Number(row.volumeB || 0),
    enabled: Boolean(row.enabled),
    lastTriggeredDate: row.lastTriggeredDate || '-'
  };
}

function emitSchedulesList(target = io) {
  db.all(`SELECT * FROM dosing_schedules ORDER BY jam ASC, id ASC`, [], (err, rows) => {
    if (err) {
      console.error('Gagal membaca jadwal dosing:', err.message);
      return;
    }
    target.emit('schedulesList', (rows || []).map(formatScheduleRow));
  });
}

function publishDosingPayload(payloadObj) {
  if (!payloadObj || typeof payloadObj !== 'object') {
    return { ok: false, message: 'Payload perintah dosing tidak valid.' };
  }

  if ((payloadObj.pompa === 'PH_UP' || payloadObj.pompa === 'PH_DOWN') && phDriftLock.locked) {
    return {
      ok: false,
      blockedByDriftLock: true,
      message: `Dosing ${payloadObj.pompa} diblokir oleh pH Drift Lock (${phDriftLock.reason}).`
    };
  }

  const maxVol = Number(systemSettings.maxDosingVolume) || 500;

  if (payloadObj.pompa === 'AB') {
    const volA = Number(payloadObj.volumeA);
    const volB = Number(payloadObj.volumeB);
    if (!Number.isFinite(volA) || !Number.isFinite(volB) || volA <= 0 || volB <= 0 || volA > maxVol || volB > maxVol) {
      return { ok: false, message: `Volume dosing AB harus antara 1 - ${maxVol} mL.` };
    }
    mqttClient.publish('hidroponik/dosing/set', `AB:${volA}:${volB}`);
    return { ok: true, message: `Perintah dosing AB (${volA} mL & ${volB} mL) dikirim.` };
  }

  if (['A', 'B', 'PH_UP', 'PH_DOWN'].includes(payloadObj.pompa)) {
    const vol = Number(payloadObj.volume);
    if (!Number.isFinite(vol) || vol <= 0 || vol > maxVol) {
      return { ok: false, message: `Volume dosing harus antara 1 - ${maxVol} mL.` };
    }
    mqttClient.publish('hidroponik/dosing/set', `${payloadObj.pompa}:${vol}`);
    return { ok: true, message: `Perintah dosing ${payloadObj.pompa} (${vol} mL) dikirim.` };
  }

  return { ok: false, message: 'Jenis pompa dosing tidak dikenali.' };
}

// 2. Setup Koneksi MQTT Broker
const mqttBrokerUrl = process.env.MQTT_BROKER_URL || 'wss://broker.emqx.io:8084/mqtt';
const mqttOptions = {
  clientId: process.env.MQTT_CLIENT_ID || ('ServerBackend_' + Math.random().toString(16).slice(2, 10)),
  clean: true,
  reconnectPeriod: 3000
};
if (process.env.MQTT_USERNAME) mqttOptions.username = process.env.MQTT_USERNAME;
if (process.env.MQTT_PASSWORD) mqttOptions.password = process.env.MQTT_PASSWORD;

const mqttClient = mqtt.connect(mqttBrokerUrl, mqttOptions);

mqttClient.on('connect', () => {
  console.log('Backend berhasil terhubung ke Broker MQTT:', mqttBrokerUrl);
  mqttClient.subscribe('hidroponik/sensor/data');
  mqttClient.subscribe('hidroponik/dosing/status');
  mqttClient.subscribe('hidroponik/kontrol/status');
  io.emit('mqttStatus', { connected: true });
});

mqttClient.on('offline', () => {
  io.emit('mqttStatus', { connected: false });
});

mqttClient.on('error', (err) => {
  console.error('MQTT Error:', err.message);
});

mqttClient.on('message', (topic, message) => {
  const msgStr = message.toString().trim();
  console.log(`[MQTT Receive] Topic: ${topic} | Msg: ${msgStr}`);

  if (topic === 'hidroponik/sensor/data') {
    try {
      const data = JSON.parse(msgStr);
      const suhuAir = Number(data.suhuAir);
      const ph = Number(data.ph);
      const ec = Number(data.ec);

      if (!isValidNumber(suhuAir) || !isValidNumber(ph) || !isValidNumber(ec)) {
        console.warn('Data sensor tidak valid, dilewati:', msgStr);
        return;
      }

      const { tanggal, waktu } = getLocalDateParts();
      lastSensorUpdate = { tanggal, waktu, suhuAir, ph, ec };

      const query = `INSERT INTO sensor_logs (tanggal, waktu, suhuAir, ph, ec) VALUES (?, ?, ?, ?, ?)`;
      db.run(query, [tanggal, waktu, suhuAir, ph, ec], (err) => {
        if (err) console.error('Gagal menyimpan ke database:', err.message);
      });

      evaluatePhDriftLock(ph, waktu);
      io.emit('sensorData', { tanggal, waktu, suhuAir, ph, ec });
    } catch (e) {
      console.error('Gagal parsing JSON dari MQTT sensor:', e);
    }
  } else if (topic === 'hidroponik/dosing/status') {
    let stateChanged = false;
    if (msgStr.includes('PROSES_POMPA_A')) {
      actuatorStates.nutA = true;
      stateChanged = true;
    } else if (msgStr.includes('PROSES_POMPA_B')) {
      actuatorStates.nutB = true;
      stateChanged = true;
    } else if (msgStr.includes('PROSES_POMPA_SIMULTAN_AB')) {
      actuatorStates.nutA = true;
      actuatorStates.nutB = true;
      stateChanged = true;
    } else if (msgStr.includes('PROSES_POMPA_PH_UP')) {
      actuatorStates.phUp = true;
      stateChanged = true;
    } else if (msgStr.includes('PROSES_POMPA_PH_DOWN')) {
      actuatorStates.phDown = true;
      stateChanged = true;
    } else if (msgStr.includes('SELESAI') || msgStr.includes('UNLOCKED')) {
      actuatorStates.nutA = false;
      actuatorStates.nutB = false;
      actuatorStates.phUp = false;
      actuatorStates.phDown = false;
      stateChanged = true;
    }

    if (stateChanged) {
      io.emit('initialActuatorStates', actuatorStates);
    }
    io.emit('dosingStatus', msgStr);
  } else if (topic === 'hidroponik/kontrol/status') {
    try {
      const statusData = JSON.parse(msgStr);
      if (statusData.device && Object.prototype.hasOwnProperty.call(actuatorStates, statusData.device)) {
        actuatorStates[statusData.device] = Boolean(statusData.state);
        io.emit('initialActuatorStates', actuatorStates);
      }
    } catch (e) {
      console.warn('Format kontrol/status bukan JSON:', msgStr);
    }
  }
});

// 3. Scheduler Penjadwalan Dosing Otomatis (Cek Tiap 15 Detik)
setInterval(() => {
  const { tanggal, jamMenit } = getLocalDateParts();

  db.all(
    `SELECT * FROM dosing_schedules WHERE enabled = 1 AND jam = ? AND (lastTriggeredDate IS NULL OR lastTriggeredDate != ?)`,
    [jamMenit, tanggal],
    (err, rows) => {
      if (err) {
        console.error('Gagal mengecek jadwal dosing otomatis:', err.message);
        return;
      }
      if (!rows || rows.length === 0) return;

      rows.forEach((sch) => {
        const result = publishDosingPayload({
          pompa: sch.pompa,
          volume: sch.volume,
          volumeA: sch.volumeA,
          volumeB: sch.volumeB
        });

        db.run(
          `UPDATE dosing_schedules SET lastTriggeredDate = ? WHERE id = ?`,
          [tanggal, sch.id],
          (updErr) => {
            if (updErr) {
              console.error('Gagal memperbarui lastTriggeredDate jadwal:', updErr.message);
            } else {
              emitSchedulesList(io);
            }
          }
        );

        const volInfo = sch.pompa === 'AB'
          ? `A: ${sch.volumeA} mL & B: ${sch.volumeB} mL`
          : `${sch.volume} mL`;
        const execMsg = result.ok
          ? `Jadwal ${sch.jam} (${sch.pompa}, ${volInfo}) dieksekusi pada ${tanggal} ${jamMenit}.`
          : `Jadwal ${sch.jam} (${sch.pompa}) tidak dijalankan: ${result.message}`;

        io.emit('scheduleExecuted', {
          id: sch.id,
          jam: sch.jam,
          pompa: sch.pompa,
          ok: result.ok,
          message: execMsg
        });
      });
    }
  );
}, 15000);

// 4. Endpoint REST API untuk Export Data Riwayat ke CSV
app.get('/api/export', (req, res) => {
  const dateParam = req.query.date;
  const isValidDate = typeof dateParam === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(dateParam);

  const sql = isValidDate
    ? `SELECT id, tanggal, waktu, suhuAir, ph, ec FROM sensor_logs WHERE tanggal = ? ORDER BY waktu ASC`
    : `SELECT id, tanggal, waktu, suhuAir, ph, ec FROM sensor_logs ORDER BY tanggal ASC, waktu ASC`;
  const params = isValidDate ? [dateParam] : [];

  db.all(sql, params, (err, rows) => {
    if (err) {
      return res.status(500).send('Gagal mengambil data untuk ekspor CSV.');
    }

    const header = 'id,tanggal,waktu,suhuAir_C,ph,ec_mS_cm\n';
    const lines = rows.map(r => `${r.id},${r.tanggal},${r.waktu},${r.suhuAir},${r.ph},${r.ec}`).join('\n');
    const filename = isValidDate ? `sensor_logs_${dateParam}.csv` : 'sensor_logs_all.csv';

    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.send(header + lines);
  });
});

// 5. Setup Socket.io untuk Komunikasi dengan Frontend
io.on('connection', (socket) => {
  console.log('Klien Web terhubung via Socket.io:', socket.id);

  socket.emit('mqttStatus', { connected: mqttClient.connected });
  socket.emit('initialActuatorStates', actuatorStates);
  emitSystemSettings(socket);
  emitPhDriftLock(socket);
  emitCropPhase(socket);
  if (lastSensorUpdate) {
    socket.emit('sensorData', lastSensorUpdate);
  }
  emitSchedulesList(socket);

  socket.on('getSystemSettings', () => {
    emitSystemSettings(socket);
  });

  socket.on('updateSystemSettings', (raw) => {
    if (!raw || typeof raw !== 'object') return;
    const { tanggal, waktu } = getLocalDateParts();
    const stamp = `${tanggal} ${waktu}`;
    const nextSettings = sanitizeSystemSettings(raw, { ...systemSettings, updatedAt: stamp });
    systemSettings = nextSettings;

    db.run(
      `INSERT INTO system_settings (
        id, tempMin, tempMax, phMin, phMax, ecMin, ecMax,
        phDriftMinSafe, phDriftMaxSafe, maxDosingVolume,
        deviceId, farmLocation, pumpEnclosureNote, calibrationNote, updatedAt
      ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(id) DO UPDATE SET
        tempMin = excluded.tempMin,
        tempMax = excluded.tempMax,
        phMin = excluded.phMin,
        phMax = excluded.phMax,
        ecMin = excluded.ecMin,
        ecMax = excluded.ecMax,
        phDriftMinSafe = excluded.phDriftMinSafe,
        phDriftMaxSafe = excluded.phDriftMaxSafe,
        maxDosingVolume = excluded.maxDosingVolume,
        deviceId = excluded.deviceId,
        farmLocation = excluded.farmLocation,
        pumpEnclosureNote = excluded.pumpEnclosureNote,
        calibrationNote = excluded.calibrationNote,
        updatedAt = excluded.updatedAt`,
      [
        systemSettings.tempMin, systemSettings.tempMax,
        systemSettings.phMin, systemSettings.phMax,
        systemSettings.ecMin, systemSettings.ecMax,
        systemSettings.phDriftMinSafe, systemSettings.phDriftMaxSafe,
        systemSettings.maxDosingVolume, systemSettings.deviceId,
        systemSettings.farmLocation, systemSettings.pumpEnclosureNote,
        systemSettings.calibrationNote, systemSettings.updatedAt
      ],
      (err) => {
        if (err) {
          console.error('Gagal menyimpan pengaturan sistem:', err.message);
          socket.emit('systemSettingsFeedback', { ok: false, message: 'Gagal menyimpan pengaturan ke database.' });
          return;
        }

        if (isValidNumber(phDriftLock.lastPh)) {
          evaluatePhDriftLock(phDriftLock.lastPh, waktu);
        }

        emitSystemSettings(io);
        emitPhDriftLock(io);
        socket.emit('systemSettingsFeedback', {
          ok: true,
          message: `Pengaturan sistem disimpan (${stamp}).`
        });
      }
    );
  });

  socket.on('getCropPhase', () => {
    emitCropPhase(socket);
  });

  socket.on('updateCropPhase', (data) => {
    if (!data || typeof data !== 'object') return;
    const requestedKey = String(data.phaseKey || '').trim();
    if (!CROP_PHASE_PROFILES[requestedKey]) {
      socket.emit('cropPhaseFeedback', { ok: false, message: 'Fase tanaman tidak dikenali.' });
      return;
    }

    const hssNum = Number(data.hss);
    const hssVal = Number.isFinite(hssNum) && hssNum >= 0 && hssNum <= 160 ? Math.round(hssNum) : 0;
    const hstNum = Number(data.hst);
    const hstVal = Number.isFinite(hstNum) && hstNum >= 0 && hstNum <= 150 ? Math.round(hstNum) : 0;
    const notesVal = String(data.notes || '').trim().slice(0, 200);
    const { tanggal, waktu } = getLocalDateParts();
    const stamp = `${tanggal} ${waktu}`;

    currentCropPhase = {
      phaseKey: requestedKey,
      hss: hssVal,
      hst: hstVal,
      notes: notesVal,
      updatedAt: stamp
    };

    db.run(
      `INSERT INTO crop_phase (id, phaseKey, hss, hst, notes, updatedAt) VALUES (1, ?, ?, ?, ?, ?)
       ON CONFLICT(id) DO UPDATE SET phaseKey = excluded.phaseKey, hss = excluded.hss, hst = excluded.hst, notes = excluded.notes, updatedAt = excluded.updatedAt`,
      [currentCropPhase.phaseKey, currentCropPhase.hss, currentCropPhase.hst, currentCropPhase.notes, currentCropPhase.updatedAt],
      (err) => {
        if (err) {
          console.error('Gagal menyimpan fase tanaman:', err.message);
          socket.emit('cropPhaseFeedback', { ok: false, message: 'Gagal menyimpan fase tanaman ke database.' });
          return;
        }
        emitCropPhase(io);
        const label = CROP_PHASE_PROFILES[requestedKey].label;
        socket.emit('cropPhaseFeedback', {
          ok: true,
          message: `Fase diperbarui ke ${label} (HSS ${hssVal} | HST ${hstVal}).`
        });
      }
    );
  });

  socket.on('getInitialActuatorStates', () => {
    socket.emit('initialActuatorStates', actuatorStates);
  });

  socket.on('unlockPhDriftLock', () => {
    const { waktu } = getLocalDateParts();
    phDriftLock = {
      locked: false,
      reason: 'Dibuka manual oleh operator',
      lastPh: phDriftLock.lastPh,
      updatedAt: waktu
    };
    emitPhDriftLock(io);
    socket.emit('commandFeedback', {
      ok: true,
      message: 'pH Drift Lock telah dibuka (UNLOCKED). Periksa sensor pH sebelum dosing ulang.'
    });
  });

  socket.on('getSchedules', () => {
    emitSchedulesList(socket);
  });

  socket.on('addSchedule', (data) => {
    if (!data || typeof data !== 'object') return;
    const jam = String(data.jam || '').trim();
    const pompa = String(data.pompa || '').trim();
    const maxVol = Number(systemSettings.maxDosingVolume) || 500;

    if (!/^\d{2}:\d{2}$/.test(jam)) {
      socket.emit('scheduleFeedback', { ok: false, message: 'Format jam harus HH:MM.' });
      return;
    }
    if (!['AB', 'A', 'B', 'PH_UP', 'PH_DOWN'].includes(pompa)) {
      socket.emit('scheduleFeedback', { ok: false, message: 'Jenis pompa tidak valid.' });
      return;
    }

    let vol = 0;
    let volA = 0;
    let volB = 0;

    if (pompa === 'AB') {
      volA = Number(data.volumeA);
      volB = Number(data.volumeB);
      if (!Number.isFinite(volA) || !Number.isFinite(volB) || volA <= 0 || volB <= 0 || volA > maxVol || volB > maxVol) {
        socket.emit('scheduleFeedback', { ok: false, message: `Volume A dan B harus 1 - ${maxVol} mL.` });
        return;
      }
    } else {
      vol = Number(data.volume);
      if (!Number.isFinite(vol) || vol <= 0 || vol > maxVol) {
        socket.emit('scheduleFeedback', { ok: false, message: `Volume harus 1 - ${maxVol} mL.` });
        return;
      }
    }

    db.run(
      `INSERT INTO dosing_schedules (jam, pompa, volume, volumeA, volumeB, enabled) VALUES (?, ?, ?, ?, ?, 1)`,
      [jam, pompa, vol, volA, volB],
      function (err) {
        if (err) {
          console.error('Gagal menambah jadwal dosing:', err.message);
          socket.emit('scheduleFeedback', { ok: false, message: 'Gagal menyimpan jadwal ke database.' });
          return;
        }
        emitSchedulesList(io);
        socket.emit('scheduleFeedback', { ok: true, message: `Jadwal ${jam} (${pompa}) berhasil ditambahkan.` });
      }
    );
  });

  socket.on('toggleSchedule', (data) => {
    const id = Number(data && data.id);
    const enabled = data && data.enabled ? 1 : 0;
    if (!Number.isInteger(id) || id <= 0) return;

    db.run(`UPDATE dosing_schedules SET enabled = ? WHERE id = ?`, [enabled, id], (err) => {
      if (err) {
        console.error('Gagal mengubah status jadwal:', err.message);
        return;
      }
      emitSchedulesList(io);
      socket.emit('scheduleFeedback', {
        ok: true,
        message: `Jadwal #${id} ${enabled ? 'diaktifkan' : 'dinonaktifkan'}.`
      });
    });
  });

  socket.on('deleteSchedule', (data) => {
    const id = Number(data && data.id);
    if (!Number.isInteger(id) || id <= 0) return;

    db.run(`DELETE FROM dosing_schedules WHERE id = ?`, [id], (err) => {
      if (err) {
        console.error('Gagal menghapus jadwal:', err.message);
        return;
      }
      emitSchedulesList(io);
      socket.emit('scheduleFeedback', { ok: true, message: `Jadwal #${id} berhasil dihapus.` });
    });
  });

  socket.on('dosingCommand', (data) => {
    if (!data || typeof data !== 'object') return;
    console.log('Perintah Dosing diterima:', data);
    const res = publishDosingPayload(data);
    socket.emit('commandFeedback', res);
  });

  socket.on('controlCommand', (data) => {
    if (!data || !Object.prototype.hasOwnProperty.call(actuatorStates, data.device)) return;
    const nextState = Boolean(data.state);
    actuatorStates[data.device] = nextState;

    console.log('Perintah Kontrol Manual:', data.device, nextState);
    mqttClient.publish('hidroponik/kontrol/manual', JSON.stringify({ device: data.device, state: nextState }));
    io.emit('initialActuatorStates', actuatorStates);
  });

  socket.on('getHistoryData', (data) => {
    const selectedDate = data && data.date;
    if (!selectedDate || !/^\d{4}-\d{2}-\d{2}$/.test(selectedDate)) {
      socket.emit('historyDataResult', { data: [] });
      return;
    }

    const query = `SELECT * FROM sensor_logs WHERE tanggal = ? ORDER BY waktu ASC`;
    db.all(query, [selectedDate], (err, rows) => {
      if (err) {
        console.error('Gagal query database riwayat:', err.message);
        socket.emit('historyDataResult', { data: [] });
      } else {
        socket.emit('historyDataResult', { data: rows });
      }
    });
  });

  socket.on('disconnect', () => {
    console.log('Klien Web terputus:', socket.id);
  });
});

// Jalankan Server pada Port 3000
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server Smart Hydroponic berjalan di http://localhost:${PORT}`);
});