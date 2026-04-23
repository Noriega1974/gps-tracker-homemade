require('dotenv').config({ path: require('path').join(__dirname, '.env') });
const dgram = require('dgram');
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const { Pool } = require('pg');
const path = require('path');
const os = require('os');
const crypto = require('crypto');

const UDP_PORT = parseInt(process.env.UDP_PORT) || 5005;
const WEB_PORT = parseInt(process.env.PORT) || 8080;

// Validar variables críticas antes de arrancar
const requiredEnv = ['DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_NAME'];
requiredEnv.forEach(variable => {
    if (!process.env[variable]) {
        console.error(`[FATAL] La variable de entorno ${variable} no está definida.`);
        process.exit(1);
    }
});

// =====================================================
//  CONFIGURACION BASE DE DATOS — SEGURA
// =====================================================
const pool = new Pool({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    port: parseInt(process.env.DB_PORT) || 5432,
    ssl: { rejectUnauthorized: false }
});
let wss;
let packetCount = 0;

function getLocalIP() {
    var interfaces = os.networkInterfaces();
    for (var name in interfaces) {
        var iface = interfaces[name];
        for (var i = 0; i < iface.length; i++) {
            if (iface[i].family === 'IPv4' && !iface[i].internal) {
                return iface[i].address;
            }
        }
    }
    return '0.0.0.0';
}

var serverIP = getLocalIP();

function timestamp() {
    const d = new Date();
    return d.toTimeString().split(' ')[0] + '.' + String(d.getMilliseconds()).padStart(3, '0');
}

async function initDB() {
    try {
        const client = await pool.connect();
        console.log(`[DB] Conectado a PostgreSQL RDS (${process.env.DB_NAME})`);
        client.release();
    } catch (err) {
        console.error('[DB] Error de conexión:', err.message);
        throw err;
    }
}

async function migrateDB() {
    const client = await pool.connect();
    try {
        await client.query('ALTER TABLE ubicaciones ADD COLUMN IF NOT EXISTS usuario TEXT');
        await client.query('ALTER TABLE ubicaciones ADD COLUMN IF NOT EXISTS rpm NUMERIC');
        await client.query('ALTER TABLE ubicaciones ADD COLUMN IF NOT EXISTS combustible_usado NUMERIC');
        await client.query('ALTER TABLE ubicaciones ADD COLUMN IF NOT EXISTS combustible_hoy NUMERIC');
        const { rows } = await client.query(
            "SELECT 1 FROM information_schema.columns WHERE table_name='ubicaciones' AND column_name='protocolo'"
        );
        if (rows.length > 0) {
            await client.query('ALTER TABLE ubicaciones DROP COLUMN protocolo');
            console.log('[DB] Columna protocolo eliminada');
        }
        console.log('[DB] Migracion OK');
    } catch (err) {
        console.error('[DB] Error en migracion:', err.message);
    } finally {
        client.release();
    }
}

async function guardarUbicacion(data, ipOrigen) {
    try {
        const sql = 'INSERT INTO ubicaciones (latitud, longitud, timestamp_gps, ip_origen) VALUES ($1, $2, $3, $4) RETURNING id';
        const valores = [data.lat, data.lon, data.time, ipOrigen];
        const result = await pool.query(sql, valores);
        return result.rows[0].id;
    } catch (err) {
        console.error('[DB] Error al guardar:', err.message);
        return null;
    }
}

function broadcast(data) {
    if (!wss) return;
    const msg = JSON.stringify({ type: 'nueva_ubicacion', data: data });
    wss.clients.forEach(function(client) {
        if (client.readyState === WebSocket.OPEN) {
            client.send(msg);
        }
    });
}

function parsearDatos(raw) {
    try {
        const payload = JSON.parse(raw.toString('utf8').trim());
        
        // Validacion estricta de esquema
        const lat = parseFloat(payload.lat);
        const lon = parseFloat(payload.lon);
        const time = payload.time;

        if (isNaN(lat) || isNaN(lon) || !time) {
            console.warn('[PARSE] Datos incompletos o invalidos:', payload);
            return null;
        }

        // Retornamos un objeto limpio con tipos de datos correctos
        return {
            lat: lat,
            lon: lon,
            time: String(time).trim()
        };
    } catch (e) {
        console.error('[PARSE] Error al procesar paquete:', e.message);
        return null;
    }
}

function iniciarUDP() {
    const server = dgram.createSocket('udp4');

    server.on('message', async function(msg, rinfo) {
        const data = parsearDatos(msg);
        
        // Solo procedemos si el dato es valido y paso el filtro de esquema
        if (!data) return;

        packetCount++;
        console.log(`\n[${timestamp()}] UDP #${packetCount} de ${rinfo.address}:${rinfo.port}`);
        console.log(`  Lat: ${data.lat}, Lon: ${data.lon}, Time: ${data.time}`);
        console.log(`  Bytes: ${msg.length}`);

        try {
            const id = await guardarUbicacion(data, rinfo.address);
            if (id === null) {
                console.warn('[UDP] Paquete recibido pero no almacenado (error DB). Broadcast omitido.');
                return;
            }

            const registro = {
                id: id,
                latitud: data.lat,
                longitud: data.lon,
                timestamp_gps: data.time,
                protocolo: 'UDP',
                ip_origen: rinfo.address,
                puerto_origen: rinfo.port,
                ip_destino: serverIP,
                puerto_destino: UDP_PORT,
                longitud_bytes: msg.length,
                fecha_recepcion: new Date().toISOString()
            };
            
            broadcast(registro);
        } catch (err) {
            console.error('[UDP] Error en el flujo de procesamiento:', err.message);
        }
    });

    server.on('error', function(err) {
        console.error('[UDP] Error:', err.message);
        server.close();
    });

    server.bind(UDP_PORT, '0.0.0.0', function() {
        console.log('[UDP] Escuchando en puerto ' + UDP_PORT);
    });
}

function iniciarWeb() {
    const app = express();
    const server = http.createServer(app);
    wss = new WebSocket.Server({ server: server });

    const fs = require('fs');
    const instanceName = process.env.INSTANCE_NAME || 'GPS Tracker';
    // Extraemos el ID del nombre (ej: "GPS-DEV-JOSE" -> "jose") asegurando limpieza
    const currentId = instanceName.toLowerCase().trim().split('-').pop();

    // Leer y procesar el HTML una sola vez al arrancar — no en cada request
    const cachedHtml = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8')
        .replace(/{{INSTANCE_NAME}}/g, instanceName)
        .replace(/{{CURRENT_ID}}/g, currentId);

    app.get('/', function(req, res) {
        res.send(cachedHtml);
    });

    // ── Auth ─────────────────────────────────────────────────────────
    const UPLOAD_SECRET = process.env.UPLOAD_PASSWORD || '';

    function generarToken(usuario) {
        const ts = Date.now();
        const sig = crypto.createHmac('sha256', UPLOAD_SECRET).update(usuario + ts).digest('hex');
        return Buffer.from(JSON.stringify({ usuario, ts, sig })).toString('base64url');
    }

    function verificarToken(token) {
        try {
            const { usuario, ts, sig } = JSON.parse(Buffer.from(token, 'base64url').toString());
            const expected = crypto.createHmac('sha256', UPLOAD_SECRET).update(usuario + ts).digest('hex');
            return sig === expected && (Date.now() - ts) < 86400000 ? usuario : null;
        } catch { return null; }
    }

    function autenticar(req, res, next) {
        const auth = req.headers['authorization'] || '';
        const token = auth.startsWith('Bearer ') ? auth.slice(7) : null;
        const usuario = token ? verificarToken(token) : null;
        if (!usuario) return res.status(401).json({ error: 'No autorizado' });
        req.usuario = usuario;
        next();
    }

    app.post('/api/login', express.json({ limit: '1mb' }), function(req, res) {
        if (!UPLOAD_SECRET) return res.status(503).json({ error: 'UPLOAD_PASSWORD no configurado en el servidor' });
        const { password, usuario } = req.body || {};
        if (!password || password !== UPLOAD_SECRET)
            return res.status(401).json({ error: 'Contraseña incorrecta' });
        if (!usuario || !usuario.trim())
            return res.status(400).json({ error: 'Nombre de usuario requerido' });
        res.json({ token: generarToken(usuario.trim()), usuario: usuario.trim() });
    });

    // ── CSV Parser ────────────────────────────────────────────────────
    function parsearLineaCSV(line) {
        const cols = []; let cur = ''; let inQ = false;
        for (const ch of line) {
            if (ch === '"') { inQ = !inQ; }
            else if (ch === ',' && !inQ) { cols.push(cur); cur = ''; }
            else cur += ch;
        }
        cols.push(cur);
        return cols;
    }

    function parsearCSVReporte(csvText, usuario, fecha) {
        const lines = csvText.split('\n').filter(l => l.trim());
        if (lines.length < 2) return [];

        const header = parsearLineaCSV(lines[0]);
        const idxRpm  = header.findIndex(h => h.replace(/"/g,'').trim().startsWith('Revoluciones del motor (rpm)'));
        const idxComb = header.findIndex(h => h.replace(/"/g,'').trim() === 'Combustible usado (L)');
        const idxHoy  = header.findIndex(h => h.replace(/"/g,'').trim().startsWith('Combustible usado (Hoy)'));

        if (idxRpm === -1 || idxComb === -1 || idxHoy === -1) return [];

        const registros = [];
        let lastComb = null, lastHoy = null;

        for (let i = 1; i < lines.length; i++) {
            const cols = lines[i].split(',');
            const time = (cols[0] || '').trim();
            if (!time) continue;

            const rpm  = parseFloat(cols[idxRpm]);
            const comb = parseFloat(cols[idxComb]);
            const hoy  = parseFloat(cols[idxHoy]);

            if (!isNaN(comb)) lastComb = comb;
            if (!isNaN(hoy))  lastHoy  = hoy;

            if (!isNaN(rpm) && lastComb !== null) {
                registros.push({
                    timestamp: fecha + ' ' + time,
                    usuario,
                    rpm,
                    combustible_usado: lastComb,
                    combustible_hoy:   lastHoy
                });
            }
        }
        return registros;
    }

    app.post('/api/upload-reporte', express.text({ type: '*/*', limit: '10mb' }), autenticar, async function(req, res) {
        const csvText = req.body;
        const fecha = (req.headers['x-fecha'] || new Date().toISOString().split('T')[0]);

        if (!csvText || typeof csvText !== 'string')
            return res.status(400).json({ error: 'CSV vacio o invalido' });

        const registros = parsearCSVReporte(csvText, req.usuario, fecha);
        if (registros.length === 0)
            return res.status(400).json({ error: 'No se encontraron columnas RPM/combustible en el CSV' });

        let insertados = 0;
        for (const r of registros) {
            try {
                await pool.query(
                    'INSERT INTO ubicaciones (timestamp_gps, usuario, rpm, combustible_usado, combustible_hoy) VALUES ($1, $2, $3, $4, $5)',
                    [r.timestamp, r.usuario, r.rpm, r.combustible_usado, r.combustible_hoy]
                );
                insertados++;
            } catch (err) {
                console.error('[UPLOAD] Error insertando:', err.message);
            }
        }

        console.log(`[UPLOAD] ${req.usuario} subio ${insertados}/${registros.length} registros (${fecha})`);
        res.json({ ok: true, insertados, total: registros.length });
    });

    // Ultimo punto (para real-time al cargar)
    app.get('/api/ubicaciones', async function(req, res) {
        try {
            const limit = Math.min(parseInt(req.query.limit) || 50, 500);
            const result = await pool.query(
                'SELECT * FROM ubicaciones ORDER BY id DESC LIMIT $1', [limit]
            );
            res.json(result.rows);
        } catch (err) {
            res.status(500).json({ error: err.message });
        }
    });

    // Historico filtrado por fecha — el filtro se hace en SQL, no en el cliente
    // Params: desde (YYYY-MM-DD), hasta (YYYY-MM-DD), horaDesde (HH:MM), horaHasta (HH:MM)
    app.get('/api/historico', async function(req, res) {
        try {
            var desde = req.query.desde || null;
            var hasta = req.query.hasta || null;
            var horaDesde = req.query.horaDesde || null;
            var horaHasta = req.query.horaHasta || null;

            var conditions = [];
            var params = [];
            var idx = 1;

            if (desde) {
                var tsDesde = desde + ' ' + (horaDesde ? horaDesde + ':00' : '00:00:00');
                conditions.push("timestamp_gps::timestamp >= $" + idx + "::timestamp");
                params.push(tsDesde);
                idx++;
            }

            if (hasta) {
                var tsHasta = hasta + ' ' + (horaHasta ? horaHasta + ':00' : '23:59:59');
                conditions.push("timestamp_gps::timestamp <= $" + idx + "::timestamp");
                params.push(tsHasta);
                idx++;
            }

            var where = conditions.length > 0 ? ' WHERE ' + conditions.join(' AND ') : '';
            var sql = 'SELECT * FROM ubicaciones' + where + ' ORDER BY id DESC LIMIT 500';

            console.log('[HIST] SQL:', sql, '| Params:', params);
            var result = await pool.query(sql, params);
            res.json(result.rows);
        } catch (err) {
            console.error('[HIST] Error:', err.message);
            res.status(500).json({ error: err.message });
        }
    });

    // Filtro por zona geografica — devuelve registros dentro de un radio (metros) desde un punto dado
    app.get('/api/zona', async function(req, res) {
        try {
            var lat   = parseFloat(req.query.lat);
            var lon   = parseFloat(req.query.lon);
            var radio = parseFloat(req.query.radio) || 200;

            if (isNaN(lat) || isNaN(lon)) {
                return res.status(400).json({ error: 'Coordenadas invalidas' });
            }

            // Haversine en SQL. LEAST(1.0,...) evita errores de dominio en acos por imprecision float.
            var distExpr =
                '(6371000 * acos(LEAST(1.0, ' +
                '  cos(radians($1)) * cos(radians(latitud::float)) * ' +
                '  cos(radians(longitud::float) - radians($2)) + ' +
                '  sin(radians($1)) * sin(radians(latitud::float))' +
                ')))';

            var sql = 'SELECT * FROM ubicaciones WHERE ' + distExpr + ' <= $3 ORDER BY id ASC LIMIT 500';
            console.log('[ZONA] SQL:', sql, '| Params:', [lat, lon, radio]);
            var result = await pool.query(sql, [lat, lon, radio]);
            res.json(result.rows);
        } catch (err) {
            console.error('[ZONA] Error:', err.message);
            res.status(500).json({ error: err.message });
        }
    });

    app.get('/api/stats', async function(req, res) {
        try {
            const [totalRes, byProtoRes, ultimaRes] = await Promise.all([
                pool.query('SELECT COUNT(*) as total FROM ubicaciones'),
                pool.query('SELECT protocolo, COUNT(*) as total FROM ubicaciones GROUP BY protocolo'),
                pool.query('SELECT * FROM ubicaciones ORDER BY id DESC LIMIT 1')
            ]);
            res.json({
                total: totalRes.rows[0].total,
                por_protocolo: byProtoRes.rows,
                ultima_ubicacion: ultimaRes.rows[0] || null
            });
        } catch (err) {
            res.status(500).json({ error: err.message });
        }
    });

    // Heartbeat: detectar y limpiar sockets muertos cada 30 segundos
    const heartbeatInterval = setInterval(function() {
        wss.clients.forEach(function(ws) {
            if (ws.isAlive === false) {
                console.log('[WS] Terminando socket sin respuesta (heartbeat timeout)');
                return ws.terminate();
            }
            ws.isAlive = false;
            ws.ping();
        });
    }, 30000);

    wss.on('close', function() {
        clearInterval(heartbeatInterval);
    });

    wss.on('connection', function(ws, req) {
        ws.isAlive = true;
        ws.on('pong', function() { ws.isAlive = true; });

        var clientIP = req.socket.remoteAddress;
        console.log('[WS] Cliente web conectado desde ' + clientIP);

        ws.send(JSON.stringify({
            type: 'bienvenida',
            udp_count: packetCount
        }));

        ws.on('close', function() {
            console.log('[WS] Cliente web desconectado (' + clientIP + ')');
        });
    });

    server.listen(WEB_PORT, '0.0.0.0', function() {
        console.log('[WEB] Escuchando en http://' + serverIP + ':' + WEB_PORT + ' (' + (process.env.INSTANCE_NAME || 'sin nombre') + ')');
    });
}

async function main() {
    console.log('');
    console.log('=====================================================');
    console.log('  GPS TRACKER SERVER v2 — UDP + PostgreSQL + WebSocket');
    console.log('  IP del servidor: ' + serverIP);
    console.log('=====================================================');
    console.log('');

    await initDB();
    await migrateDB();
    iniciarUDP();
    iniciarWeb();

    console.log('');
    console.log('Todos los servicios activos. Esperando datos...');
}

main().catch(function(err) {
    console.error('[FATAL]', err.message);
    process.exit(1);
});

process.on('SIGINT', async function() {
    console.log('\nCerrando servidor...');
    console.log('  UDP recibidos: ' + packetCount);
    await pool.end();
    process.exit(0);
});
