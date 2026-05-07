require('dotenv').config({ path: require('path').join(__dirname, '.env') });
const dgram = require('dgram');
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const { Pool } = require('pg');
const path = require('path');
const os = require('os');

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
        await client.query('ALTER TABLE ubicaciones DROP COLUMN IF EXISTS usuario');
        await client.query('ALTER TABLE ubicaciones ADD COLUMN IF NOT EXISTS rpm NUMERIC');
        await client.query('ALTER TABLE ubicaciones ADD COLUMN IF NOT EXISTS combustible_usado NUMERIC');
        await client.query('ALTER TABLE ubicaciones ADD COLUMN IF NOT EXISTS combustible_hoy NUMERIC');
        await client.query('ALTER TABLE ubicaciones ALTER COLUMN latitud DROP NOT NULL');
        await client.query('ALTER TABLE ubicaciones ALTER COLUMN longitud DROP NOT NULL');
        await client.query('ALTER TABLE ubicaciones ALTER COLUMN ip_origen DROP NOT NULL');
        await client.query('ALTER TABLE ubicaciones ADD COLUMN IF NOT EXISTS vehiculo TEXT');
        await client.query('ALTER TABLE ubicaciones ADD COLUMN IF NOT EXISTS temp_motor INTEGER');
        await client.query("UPDATE ubicaciones SET vehiculo = 'V1' WHERE vehiculo IS NULL");
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
        const sql = 'INSERT INTO ubicaciones (latitud, longitud, timestamp_gps, ip_origen, vehiculo, rpm, combustible_hoy, temp_motor) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id';
        const valores = [data.lat, data.lon, data.time, ipOrigen, data.vehiculo, data.rpm, data.fuel_today_l, data.temp_motor];
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
            time: String(time).trim(),
            vehiculo: String(payload.vehiculo || 'V1').trim(),
            rpm: (payload.rpm !== null && payload.rpm !== undefined) ? parseFloat(payload.rpm) : null,
            fuel_today_l: (payload.fuel_today_l !== null && payload.fuel_today_l !== undefined) ? parseFloat(payload.fuel_today_l) : null,
            temp_motor: (payload.temp_motor !== null && payload.temp_motor !== undefined) ? parseInt(payload.temp_motor) : null
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
        console.log(`  Lat: ${data.lat}, Lon: ${data.lon}, Time: ${data.time}, Vehiculo: ${data.vehiculo}`);
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
                vehiculo: data.vehiculo,
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

    // Ultimo punto (para real-time al cargar)
    app.get('/api/ubicaciones', async function(req, res) {
        try {
            const limit = Math.min(parseInt(req.query.limit) || 50, 2000);
            const result = await pool.query(
                'SELECT * FROM ubicaciones WHERE latitud IS NOT NULL AND longitud IS NOT NULL ORDER BY id DESC LIMIT $1', [limit]
            );
            res.json(result.rows);
        } catch (err) {
            res.status(500).json({ error: err.message });
        }
    });

    // Ultimo punto por vehiculo — para inicializar el mapa real-time con todos los vehiculos activos
    app.get('/api/ultimo-por-vehiculo', async function(req, res) {
        try {
            const result = await pool.query(
                'SELECT DISTINCT ON (vehiculo) * FROM ubicaciones WHERE latitud IS NOT NULL AND longitud IS NOT NULL ORDER BY vehiculo, id DESC'
            );
            res.json(result.rows);
        } catch (err) {
            res.status(500).json({ error: err.message });
        }
    });

    // Lista de vehiculos distintos conocidos
    app.get('/api/vehiculos', async function(req, res) {
        try {
            const result = await pool.query(
                "SELECT DISTINCT vehiculo FROM ubicaciones WHERE vehiculo IS NOT NULL ORDER BY vehiculo"
            );
            res.json(result.rows.map(function(r) { return r.vehiculo; }));
        } catch (err) {
            res.status(500).json({ error: err.message });
        }
    });

    // Historico filtrado por fecha — el filtro se hace en SQL, no en el cliente
    // Params: desde (YYYY-MM-DD), hasta (YYYY-MM-DD), horaDesde (HH:MM), horaHasta (HH:MM), vehiculo (opcional)
    app.get('/api/historico', async function(req, res) {
        try {
            var desde = req.query.desde || null;
            var hasta = req.query.hasta || null;
            var horaDesde = req.query.horaDesde || null;
            var horaHasta = req.query.horaHasta || null;
            var vehiculo = req.query.vehiculo || null;

            var conditions = ['latitud IS NOT NULL', 'longitud IS NOT NULL'];
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

            if (vehiculo) {
                conditions.push('vehiculo = $' + idx);
                params.push(vehiculo);
                idx++;
            }

            var where = conditions.length > 0 ? ' WHERE ' + conditions.join(' AND ') : '';
            var sql = 'SELECT * FROM ubicaciones' + where + ' ORDER BY id DESC LIMIT 2000';

            console.log('[HIST] SQL:', sql, '| Params:', params);
            var result = await pool.query(sql, params);
            res.json(result.rows);
        } catch (err) {
            console.error('[HIST] Error:', err.message);
            res.status(500).json({ error: err.message });
        }
    });

    // Filtro por zona geografica — devuelve registros dentro de un radio (metros) desde un punto dado
    // Params: lat, lon, radio (metros), vehiculo (opcional)
    app.get('/api/zona', async function(req, res) {
        try {
            var lat      = parseFloat(req.query.lat);
            var lon      = parseFloat(req.query.lon);
            var radio    = parseFloat(req.query.radio) || 200;
            var vehiculo = req.query.vehiculo || null;

            if (isNaN(lat) || isNaN(lon)) {
                return res.status(400).json({ error: 'Coordenadas invalidas' });
            }

            var params = [lat, lon, radio];
            var idx = 4;

            // Haversine en SQL. LEAST(1.0,...) evita errores de dominio en acos por imprecision float.
            var distExpr =
                '(6371000 * acos(LEAST(1.0, ' +
                '  cos(radians($1)) * cos(radians(latitud::float)) * ' +
                '  cos(radians(longitud::float) - radians($2)) + ' +
                '  sin(radians($1)) * sin(radians(latitud::float))' +
                ')))';

            var conditions = ['latitud IS NOT NULL', 'longitud IS NOT NULL', distExpr + ' <= $3'];

            if (vehiculo) {
                conditions.push('vehiculo = $' + idx);
                params.push(vehiculo);
                idx++;
            }

            var sql = 'SELECT * FROM ubicaciones WHERE ' + conditions.join(' AND ') + ' ORDER BY id ASC LIMIT 2000';
            console.log('[ZONA] SQL:', sql, '| Params:', params);
            var result = await pool.query(sql, params);
            res.json(result.rows);
        } catch (err) {
            console.error('[ZONA] Error:', err.message);
            res.status(500).json({ error: err.message });
        }
    });

    app.get('/api/stats', async function(req, res) {
        try {
            const [totalRes, ultimaRes] = await Promise.all([
                pool.query('SELECT COUNT(*) as total FROM ubicaciones'),
                pool.query('SELECT * FROM ubicaciones WHERE latitud IS NOT NULL AND longitud IS NOT NULL ORDER BY id DESC LIMIT 1')
            ]);
            res.json({
                total: totalRes.rows[0].total,
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
