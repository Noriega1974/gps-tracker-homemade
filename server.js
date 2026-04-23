const dgram = require('dgram');
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const { Pool } = require('pg');
const path = require('path');
const os = require('os');

const UDP_PORT = parseInt(process.env.UDP_PORT) || 5005;
const WEB_PORT = parseInt(process.env.PORT) || 8080;

// =====================================================
//  CONFIGURACION BASE DE DATOS — CAMBIA TU PASSWORD
// =====================================================
const pool = new Pool({
    host: 'database-1.culqegkq4tq5.us-east-1.rds.amazonaws.com',
    user: 'postgres',
    password: 'J50911711n-database',   // <-- cambia esto
    database: 'gps_tracker',
    port: 5432,
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
    const client = await pool.connect();
    console.log('[DB] Conectado a PostgreSQL RDS (gps_tracker)');
    client.release();
}

async function guardarUbicacion(data, ipOrigen) {
    try {
        const sql = 'INSERT INTO ubicaciones (latitud, longitud, timestamp_gps, protocolo, ip_origen) VALUES ($1, $2, $3, $4, $5) RETURNING id';
        const valores = [data.lat, data.lon, data.time, 'UDP', ipOrigen];
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
        return JSON.parse(raw.toString('utf8').trim());
    } catch (e) {
        console.error('[PARSE] JSON invalido:', raw.toString('utf8').substring(0, 200));
        return null;
    }
}

function iniciarUDP() {
    const server = dgram.createSocket('udp4');

    server.on('message', async function(msg, rinfo) {
        const data = parsearDatos(msg);
        if (data) {
            packetCount++;
            console.log('\n[' + timestamp() + '] UDP #' + packetCount + ' de ' + rinfo.address + ':' + rinfo.port);
            console.log('  Lat: ' + data.lat + ', Lon: ' + data.lon + ', Time: ' + data.time);
            console.log('  Bytes: ' + msg.length);

            const id = await guardarUbicacion(data, rinfo.address);

            var registro = {
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

    app.get('/', function(req, res) {
        const html = fs.readFileSync(path.join(__dirname, 'public/index.html'), 'utf8')
            .replace('{{INSTANCE_NAME}}', instanceName);
        res.send(html);
    });

    app.use(express.static(path.join(__dirname, 'public')));

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
                conditions.push("timestamp_gps >= $" + idx);
                params.push(tsDesde);
                idx++;
            }

            if (hasta) {
                var tsHasta = hasta + ' ' + (horaHasta ? horaHasta + ':00' : '23:59:59');
                conditions.push("timestamp_gps <= $" + idx);
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

    app.get('/api/zona', async function(req, res) {
        try {
            var lat      = parseFloat(req.query.lat);
            var lon      = parseFloat(req.query.lon);
            var radio    = parseFloat(req.query.radio) || 200;
            var desde    = req.query.desde    || null;
            var hasta    = req.query.hasta    || null;
            var horaDesde = req.query.horaDesde || null;
            var horaHasta = req.query.horaHasta || null;

            if (isNaN(lat) || isNaN(lon)) {
                return res.status(400).json({ error: 'Coordenadas invalidas' });
            }

            // Haversine en SQL para calcular distancia en metros desde el centro dado.
            // LEAST(1.0, ...) previene errores de dominio en acos por imprecisión de punto flotante.
            var distExpr =
                '(6371000 * acos(LEAST(1.0, ' +
                '  cos(radians($1)) * cos(radians(latitud::float)) * ' +
                '  cos(radians(longitud::float) - radians($2)) + ' +
                '  sin(radians($1)) * sin(radians(latitud::float))' +
                ')))';

            var conditions = [distExpr + ' <= $3'];
            var params = [lat, lon, radio];
            var idx = 4;

            if (desde) {
                var tsDesde = desde + ' ' + (horaDesde ? horaDesde + ':00' : '00:00:00');
                conditions.push('timestamp_gps >= $' + idx);
                params.push(tsDesde);
                idx++;
            }

            if (hasta) {
                var tsHasta = hasta + ' ' + (horaHasta ? horaHasta + ':00' : '23:59:59');
                conditions.push('timestamp_gps <= $' + idx);
                params.push(tsHasta);
                idx++;
            }

            // ORDER BY id ASC para que el trayecto se dibuje en orden cronológico
            var where = ' WHERE ' + conditions.join(' AND ');
            var sql = 'SELECT * FROM ubicaciones' + where + ' ORDER BY id ASC LIMIT 500';

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
            const total = await pool.query('SELECT COUNT(*) as total FROM ubicaciones');
            const byProto = await pool.query('SELECT protocolo, COUNT(*) as total FROM ubicaciones GROUP BY protocolo');
            const ultima = await pool.query('SELECT * FROM ubicaciones ORDER BY id DESC LIMIT 1');
            res.json({
                total: total.rows[0].total,
                por_protocolo: byProto.rows,
                ultima_ubicacion: ultima.rows[0] || null
            });
        } catch (err) {
            res.status(500).json({ error: err.message });
        }
    });

    wss.on('connection', function(ws, req) {
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
        console.log('[WEB] http://gpstracker3.ddns.net:' + WEB_PORT);
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
