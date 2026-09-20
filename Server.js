const http = require('http');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const dotenv = require('dotenv');

dotenv.config({ path: path.join(__dirname, '.env') });

const PORT = Number(process.env.PORT || 10000);
const CONTROL_PANEL_ORIGIN = process.env.CONTROL_PANEL_ORIGIN || '*';
const SERVER_NAME = 'AudioBridge';
const SERVER_VERSION = '4.1.0';

const REGISTRATION_TIMEOUT_MS = 10000;
const HEARTBEAT_INTERVAL_MS = 30000;
const MAX_RECEIVER_BUFFERED_BYTES = 2 * 1024 * 1024;

const ADMIN_TOKEN =
    process.env.ADMIN_TOKEN || 'CHANGE_ME_ADMIN_TOKEN';

const DATA_FILE =
    process.env.DATA_FILE ||
    path.join(__dirname, 'audiobridge-data.json');

const rooms = {
    am: {
        name: 'AM',
        transmitters: new Set(),
        receivers: new Set(),
        config: { sampleRate: 44100, channels: 2 }
    },
    fm: {
        name: 'FM',
        transmitters: new Set(),
        receivers: new Set(),
        config: { sampleRate: 44100, channels: 2 }
    }
};

const VALID_STATES =
    new Set([
        'active',
        'frozen',
        'paused',
        'muted',
        'disabled'
    ]);

let db = loadData();


// ============================================================
// DATA
// ============================================================

function loadData() {

    const defaults = {
        routes: [],
        pending: [],
        audit: [],
        listenerStats: {}
    };

    try {

        if (fs.existsSync(DATA_FILE)) {

            const parsed =
                JSON.parse(
                    fs.readFileSync(
                        DATA_FILE,
                        'utf8'
                    )
                );

            const data = {
                ...defaults,
                ...parsed,
                routes: parsed.routes || [],
                pending: parsed.pending || [],
                audit: parsed.audit || [],
                listenerStats: parsed.listenerStats || {}
            };

            // Migrate older routes to route-specific endpoints.
            let changed = false;

            for (const route of data.routes) {

                const endpoints =
                    ensureRouteEndpoints(route);

                if (
                    route.txPath !== endpoints.txPath ||
                    route.rxPath !== endpoints.rxPath
                ) {
                    route.txPath = endpoints.txPath;
                    route.rxPath = endpoints.rxPath;
                    changed = true;
                }
            }

            if (changed) {
                try {
                    fs.writeFileSync(
                        DATA_FILE,
                        JSON.stringify(data, null, 2)
                    );
                } catch (_) {}
            }

            return data;
        }

    } catch (e) {

        console.error(
            '[DATA] load failed:',
            e.message
        );
    }

    return defaults;
}


function saveData() {

    try {

        fs.writeFileSync(
            DATA_FILE,
            JSON.stringify(db, null, 2)
        );

    } catch (e) {

        console.error(
            '[DATA] save failed:',
            e.message
        );
    }
}


function audit(action, details = {}) {

    db.audit.unshift({
        time: new Date().toISOString(),
        action,
        ...details
    });

    db.audit =
        db.audit.slice(0, 1000);

    saveData();
}


// ============================================================
// LISTENER ANALYTICS
// ============================================================

function monthKey(date = new Date()) {
    return date.toISOString().slice(0, 7);
}

function dayKey(date = new Date()) {
    return date.toISOString().slice(0, 10);
}

function listenerStationKey(ws) {
    return String(
        ws.routeId ||
        `legacy-${ws.stationRoom}`
    );
}

function hashListenerId(value) {
    return crypto
        .createHash('sha256')
        .update(String(value))
        .digest('hex');
}

function ensureListenerBucket(month, ws) {
    db.listenerStats ||= {};
    db.listenerStats[month] ||= {};

    const key = listenerStationKey(ws);

    if (!db.listenerStats[month][key]) {
        db.listenerStats[month][key] = {
            routeId: ws.routeId || null,
            stationId: ws.stationId || null,
            station: ws.stationName || ws.stationRoom.toUpperCase(),
            channel: ws.stationRoom,
            sessions: 0,
            listenerSeconds: 0,
            uniqueListenerKeys: [],
            days: {}
        };
    }

    return db.listenerStats[month][key];
}

function recordListenerStart(ws) {
    if (!ws.isReceiver || ws.listenerRecorded) {
        return;
    }

    ws.listenerRecorded = true;
    ws.listenerStartedAtMs = Date.now();

    const month = monthKey();
    const day = dayKey();
    const bucket = ensureListenerBucket(month, ws);

    bucket.sessions += 1;

    const listenerId =
        ws.listenerId ||
        null;

    if (listenerId) {
        const key = hashListenerId(listenerId);

        if (!bucket.uniqueListenerKeys.includes(key)) {
            bucket.uniqueListenerKeys.push(key);
        }
    }

    bucket.days[day] ||= {
        sessions: 0,
        listenerSeconds: 0,
        uniqueListenerKeys: []
    };

    bucket.days[day].sessions += 1;

    if (listenerId) {
        const key = hashListenerId(listenerId);

        if (!bucket.days[day].uniqueListenerKeys.includes(key)) {
            bucket.days[day].uniqueListenerKeys.push(key);
        }
    }

    saveData();

    audit(
        'listener-connected',
        {
            routeId: ws.routeId || null,
            station: ws.stationName || ws.stationRoom.toUpperCase(),
            channel: ws.stationRoom,
            sessionId: ws.listenerSessionId
        }
    );
}

function recordListenerEnd(ws) {
    if (
        !ws.isReceiver ||
        !ws.listenerRecorded ||
        ws.listenerEnded
    ) {
        return;
    }

    ws.listenerEnded = true;

    const started =
        Number(ws.listenerStartedAtMs || Date.now());

    const seconds = Math.max(
        0,
        Math.round((Date.now() - started) / 1000)
    );

    const month =
        monthKey(
            new Date(started)
        );

    const day =
        dayKey(
            new Date(started)
        );

    const bucket =
        ensureListenerBucket(
            month,
            ws
        );

    bucket.listenerSeconds += seconds;

    bucket.days[day] ||= {
        sessions: 0,
        listenerSeconds: 0,
        uniqueListenerKeys: []
    };

    bucket.days[day].listenerSeconds += seconds;

    saveData();

    audit(
        'listener-disconnected',
        {
            routeId: ws.routeId || null,
            station: ws.stationName || ws.stationRoom.toUpperCase(),
            channel: ws.stationRoom,
            sessionId: ws.listenerSessionId,
            durationSeconds: seconds
        }
    );
}

function listenerReport(month) {
    const source =
        db.listenerStats?.[month] || {};

    const stations = Object.values(source).map(
        bucket => ({
            routeId: bucket.routeId,
            stationId: bucket.stationId,
            station: bucket.station,
            channel: String(bucket.channel || '').toUpperCase(),
            sessions: Number(bucket.sessions || 0),
            uniqueListeners:
                Array.isArray(bucket.uniqueListenerKeys)
                    ? bucket.uniqueListenerKeys.length
                    : 0,
            listenerMinutes:
                Math.round(
                    Number(bucket.listenerSeconds || 0) / 60
                ),
            listenerHours:
                Math.round(
                    Number(bucket.listenerSeconds || 0) / 3600 * 100
                ) / 100
        })
    );

    stations.sort(
        (a, b) =>
            a.channel.localeCompare(b.channel) ||
            a.station.localeCompare(b.station)
    );

    return {
        month,
        stations,
        totals: {
            sessions: stations.reduce(
                (n, x) => n + x.sessions,
                0
            ),
            uniqueListeners: stations.reduce(
                (n, x) => n + x.uniqueListeners,
                0
            ),
            listenerMinutes: stations.reduce(
                (n, x) => n + x.listenerMinutes,
                0
            ),
            listenerHours:
                Math.round(
                    stations.reduce(
                        (n, x) => n + x.listenerHours,
                        0
                    ) * 100
                ) / 100
        }
    };
}

function listenerDailyReport(month) {
    const source =
        db.listenerStats?.[month] || {};

    const rows = [];

    for (const bucket of Object.values(source)) {
        for (const [day, data] of Object.entries(bucket.days || {})) {
            rows.push({
                date: day,
                station: bucket.station,
                channel: String(bucket.channel || '').toUpperCase(),
                routeId: bucket.routeId || '',
                sessions: Number(data.sessions || 0),
                uniqueListeners:
                    Array.isArray(data.uniqueListenerKeys)
                        ? data.uniqueListenerKeys.length
                        : 0,
                listenerMinutes:
                    Math.round(
                        Number(data.listenerSeconds || 0) / 60
                    )
            });
        }
    }

    rows.sort(
        (a, b) =>
            a.date.localeCompare(b.date) ||
            a.channel.localeCompare(b.channel) ||
            a.station.localeCompare(b.station)
    );

    return rows;
}


// ============================================================
// ROUTE ENDPOINTS
// ============================================================

function encodeRouteId(routeId) {

    return encodeURIComponent(
        String(routeId)
    );
}


function ensureRouteEndpoints(route) {

    const encoded =
        encodeRouteId(route.routeId);

    const txPath =
        route.txPath ||
        `/stations/${encoded}/tx`;

    const rxPath =
        route.rxPath ||
        `/stations/${encoded}/rx`;

    return {
        txPath,
        rxPath
    };
}


function getRouteByPath(pathname) {

    for (const route of db.routes) {

        const endpoints =
            ensureRouteEndpoints(route);

        if (
            pathname === endpoints.txPath ||
            pathname === endpoints.rxPath
        ) {
            return route;
        }
    }

    return null;
}


function getRoutePathRole(pathname) {

    const route =
        getRouteByPath(pathname);

    if (!route) {
        return null;
    }

    const endpoints =
        ensureRouteEndpoints(route);

    return {
        route,
        role:
            pathname === endpoints.txPath
                ? 'transmitter'
                : 'receiver'
    };
}


// ============================================================
// LEGACY ENDPOINTS
// ============================================================

function getRoomFromLegacyPath(pathname) {

    if (
        pathname === '/amtx' ||
        pathname === '/amrx'
    ) {
        return 'am';
    }

    if (
        pathname === '/fmtx' ||
        pathname === '/fmrx'
    ) {
        return 'fm';
    }

    return null;
}


function isLegacyTxPath(pathname) {

    return (
        pathname === '/amtx' ||
        pathname === '/fmtx'
    );
}


function isLegacyRxPath(pathname) {

    return (
        pathname === '/amrx' ||
        pathname === '/fmrx'
    );
}


// ============================================================
// COMMON HELPERS
// ============================================================

function sendJson(ws, payload) {

    if (
        !ws ||
        ws.readyState !== WebSocket.OPEN
    ) {
        return false;
    }

    try {

        ws.send(
            JSON.stringify(payload)
        );

        return true;

    } catch (_) {

        return false;
    }
}


function closeSocket(
    ws,
    code,
    reason
) {

    try {

        if (
            ws.readyState ===
            WebSocket.OPEN
        ) {

            ws.close(
                code,
                reason
            );

        } else {

            ws.terminate();
        }

    } catch (_) {

        try {
            ws.terminate();
        } catch (_) {}
    }
}


function removeFromRoom(ws) {

    if (
        !ws ||
        !ws.stationRoom
    ) {
        return;
    }

    const room =
        rooms[ws.stationRoom];

    if (!room) {
        return;
    }

    room.transmitters.delete(ws);
    room.receivers.delete(ws);
}


function getActiveCasterId(room) {

    for (
        const tx of room.transmitters
    ) {

        if (
            tx.registered &&
            tx.casterId
        ) {
            return tx.casterId;
        }
    }

    return null;
}


function findRoute(
    casterId,
    channel
) {

    return db.routes.find(
        route =>
            route.enabled !== false &&
            route.casterId === casterId &&
            route.channel === channel
    ) || null;
}


function findRouteAny(casterId) {

    return db.routes.find(
        route =>
            route.casterId === casterId
    ) || null;
}


function routeForConnection(ws) {

    if (!ws.routeId) {
        return null;
    }

    return db.routes.find(
        route =>
            route.routeId === ws.routeId
    ) || null;
}


function publicRoute(route) {

    const endpoints =
        ensureRouteEndpoints(route);

    return {
        routeId: route.routeId,
        stationId: route.stationId,
        station: route.station,
        casterId: route.casterId,
        channel: route.channel,

        // Route-specific endpoints.
        txPath: endpoints.txPath,
        rxPath: endpoints.rxPath,

        enabled:
            route.enabled !== false,

        streamState:
            route.streamState || 'active',

        createdAt:
            route.createdAt,

        lastControlAt:
            route.lastControlAt,

        lastControlReason:
            route.lastControlReason
    };
}


function adminAuthorized(req) {

    const auth =
        String(
            req.headers.authorization || ''
        );

    return (
        auth ===
            `Bearer ${ADMIN_TOKEN}` ||

        req.headers[
            'x-admin-token'
        ] === ADMIN_TOKEN
    );
}


function json(
    res,
    code,
    payload
) {

    res.writeHead(
        code,
        {
            'Content-Type':
                'application/json; charset=utf-8',

            'Cache-Control':
                'no-cache',

            'Access-Control-Allow-Origin':
                '*'
        }
    );

    res.end(
        JSON.stringify(payload)
    );
}


function parseBody(req) {

    return new Promise(
        (resolve, reject) => {

            let body = '';

            req.on(
                'data',
                chunk => {

                    body += chunk;

                    if (
                        body.length >
                        1024 * 1024
                    ) {
                        req.destroy();
                    }
                }
            );

            req.on(
                'end',
                () => {

                    try {

                        resolve(
                            body
                                ? JSON.parse(body)
                                : {}
                        );

                    } catch (e) {

                        reject(e);
                    }
                }
            );

            req.on(
                'error',
                reject
            );
        }
    );
}


function applyRouteState(
    ws,
    state,
    reason = ''
) {

    const route =
        routeForConnection(ws);

    if (
        !route ||
        !VALID_STATES.has(state)
    ) {
        return false;
    }

    route.streamState =
        state;

    route.lastControlAt =
        new Date().toISOString();

    route.lastControlReason =
        reason;

    ws.streamState =
        state;

    saveData();

    sendJson(
        ws,
        {
            type:
                'stream-control',

            state,
            reason,

            routeId:
                route.routeId,

            txPath:
                ensureRouteEndpoints(route)
                    .txPath,

            rxPath:
                ensureRouteEndpoints(route)
                    .rxPath
        }
    );

    return true;
}


// ============================================================
// HTTP SERVER
// ============================================================

const server =
function setCorsHeaders(res) {
  res.setHeader('Access-Control-Allow-Origin', CONTROL_PANEL_ORIGIN);
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Admin-Token');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PATCH, OPTIONS');
  res.setHeader('Access-Control-Max-Age', '86400');
}

    http.createServer(
        async (req, res) => {
  setCorsHeaders(res);

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    return res.end();
  }


            if (
                req.method === 'OPTIONS'
            ) {

                res.writeHead(
                    204,
                    {
                        'Access-Control-Allow-Origin':
                            '*',

                        'Access-Control-Allow-Headers':
                            'Content-Type, Authorization, X-Admin-Token',

                        'Access-Control-Allow-Methods':
                            'GET,POST,PATCH,DELETE,OPTIONS'
                    }
                );

                return res.end();
            }


            let url;

            try {

                url =
                    new URL(
                        req.url,
                        `http://${req.headers.host || 'localhost'}`
                    );

            } catch (_) {

                return json(
                    res,
                    400,
                    {
                        error:
                            'Invalid URL'
                    }
                );
            }


            // ==================================================
            // HEALTH
            // ==================================================

            if (
                url.pathname ===
                '/health'
            ) {

                return json(
                    res,
                    200,
                    {
                        status: 'ok',
                        service: SERVER_NAME,
                        version: SERVER_VERSION,
                        websocket: true
                    }
                );
            }


            // ==================================================
            // STATUS
            // ==================================================

            if (
                url.pathname ===
                '/status'
            ) {

                return json(
                    res,
                    200,
                    {
                        service:
                            SERVER_NAME,

                        version:
                            SERVER_VERSION,

                        am: {
                            transmitters:
                                rooms.am.transmitters.size,

                            receivers:
                                rooms.am.receivers.size,

                            activeCasterId:
                                getActiveCasterId(
                                    rooms.am
                                )
                        },

                        fm: {
                            transmitters:
                                rooms.fm.transmitters.size,

                            receivers:
                                rooms.fm.receivers.size,

                            activeCasterId:
                                getActiveCasterId(
                                    rooms.fm
                                )
                        },

                        routes:
                            db.routes.length,

                        pendingRegistrations:
                            db.pending.length,

                        listeners: {
                            am:
                                rooms.am.receivers.size,
                            fm:
                                rooms.fm.receivers.size
                        },

                        timestamp:
                            new Date().toISOString()
                    }
                );
            }


            // ==================================================
            // ROOT
            // ==================================================

            if (
                url.pathname === '/' &&
                req.method === 'GET'
            ) {

                return json(
                    res,
                    200,
                    {
                        service:
                            SERVER_NAME,

                        version:
                            SERVER_VERSION,

                        websocket: {
                            legacy: {
                                amtx: '/amtx',
                                amrx: '/amrx',
                                fmtx: '/fmtx',
                                fmrx: '/fmrx'
                            },

                            routeFormat:
                                '/stations/<routeId>/tx',

                            receiverFormat:
                                '/stations/<routeId>/rx'
                        },

                        control:
                            '/control/'
                    }
                );
            }


            // ==================================================
            // ADMIN API
            // ==================================================

            if (
                url.pathname.startsWith(
                    '/api/'
                )
            ) {

                if (
                    !adminAuthorized(req)
                ) {

                    return json(
                        res,
                        401,
                        {
                            error:
                                'ADMIN_AUTH_REQUIRED'
                        }
                    );
                }

                try {

                    // ------------------------------------------
                    // ROUTE LIST
                    // ------------------------------------------

                    if (
                        req.method === 'GET' &&
                        url.pathname ===
                            '/api/routes'
                    ) {

                        return json(
                            res,
                            200,
                            {
                                routes:
                                    db.routes.map(
                                        publicRoute
                                    )
                            }
                        );
                    }


                    // ------------------------------------------
                    // PENDING
                    // ------------------------------------------

                    if (
                        req.method === 'GET' &&
                        url.pathname ===
                            '/api/registrations'
                    ) {

                        return json(
                            res,
                            200,
                            {
                                pending:
                                    db.pending
                            }
                        );
                    }


                    // ------------------------------------------
                    // CONNECTIONS
                    // ------------------------------------------

                    if (
                        req.method === 'GET' &&
                        url.pathname ===
                            '/api/connections'
                    ) {

                        const list = [];

                        for (
                            const [
                                channel,
                                room
                            ]
                            of Object.entries(
                                rooms
                            )
                        ) {

                            for (
                                const tx
                                of room.transmitters
                            ) {

                                const route =
                                    routeForConnection(
                                        tx
                                    );

                                const endpoints =
                                    route
                                        ? ensureRouteEndpoints(
                                            route
                                        )
                                        : null;

                                list.push({
                                    role:
                                        'transmitter',

                                    channel,

                                    routeId:
                                        tx.routeId,

                                    casterId:
                                        tx.casterId,

                                    station:
                                        tx.stationName,

                                    state:
                                        route
                                            ?.streamState ||
                                        'active',

                                    format:
                                        tx.format,

                                    bitrate:
                                        tx.bitrate,

                                    sampleRate:
                                        tx.sampleRate,

                                    channels:
                                        tx.channels,

                                    connectedAt:
                                        tx.connectedAt,

                                    lastAudioAt:
                                        tx.lastAudioAt,

                                    audioPackets:
                                        tx.audioPackets,

                                    audioBytes:
                                        tx.audioBytes,

                                    endpoint:
                                        tx.endpoint,

                                    txPath:
                                        endpoints?.txPath ||
                                        null,

                                    rxPath:
                                        endpoints?.rxPath ||
                                        null
                                });
                            }


                            for (
                                const rx
                                of room.receivers
                            ) {

                                const route =
                                    routeForConnection(
                                        rx
                                    );

                                const endpoints =
                                    route
                                        ? ensureRouteEndpoints(
                                            route
                                        )
                                        : null;

                                list.push({
                                    role:
                                        'receiver',

                                    channel,

                                    routeId:
                                        rx.routeId,

                                    casterId:
                                        rx.casterId,

                                    station:
                                        rx.stationName,

                                    state:
                                        route
                                            ?.streamState ||
                                        'active',

                                    endpoint:
                                        rx.endpoint,

                                    connectedAt:
                                        rx.connectedAt,

                                    txPath:
                                        endpoints?.txPath ||
                                        null,

                                    rxPath:
                                        endpoints?.rxPath ||
                                        null
                                });
                            }
                        }

                        return json(
                            res,
                            200,
                            {
                                connections:
                                    list
                            }
                        );
                    }


                    // ------------------------------------------
                    // LISTENER ANALYTICS
                    // ------------------------------------------

                    if (
                        req.method === 'GET' &&
                        url.pathname === '/api/listeners'
                    ) {

                        const month =
                            /^\d{4}-\d{2}$/.test(
                                url.searchParams.get('month') || ''
                            )
                                ? url.searchParams.get('month')
                                : monthKey();

                        const report =
                            listenerReport(month);

                        const current = {};

                        for (
                            const [channel, room]
                            of Object.entries(rooms)
                        ) {
                            for (const rx of room.receivers) {
                                const key =
                                    rx.routeId ||
                                    `legacy-${channel}`;

                                current[key] ||= {
                                    routeId: rx.routeId || null,
                                    station:
                                        rx.stationName ||
                                        channel.toUpperCase(),
                                    channel:
                                        channel.toUpperCase(),
                                    listeners: 0
                                };

                                current[key].listeners += 1;
                            }
                        }

                        return json(
                            res,
                            200,
                            {
                                ...report,
                                currentListeners:
                                    Object.values(current)
                            }
                        );
                    }


                    // ------------------------------------------
                    // LISTENER EXCEL EXPORT
                    // ------------------------------------------

                    if (
                        req.method === 'GET' &&
                        url.pathname === '/api/listeners/export'
                    ) {

                        const month =
                            /^\d{4}-\d{2}$/.test(
                                url.searchParams.get('month') || ''
                            )
                                ? url.searchParams.get('month')
                                : monthKey();

                        const report =
                            listenerReport(month);

                        const daily =
                            listenerDailyReport(month);

                        const XLSX =
                            require('xlsx');

                        const workbook =
                            XLSX.utils.book_new();

                        const summaryRows =
                            report.stations.map(
                                x => ({
                                    Station: x.station,
                                    Channel: x.channel,
                                    Route: x.routeId || '',
                                    'Listener Sessions':
                                        x.sessions,
                                    'Unique Listeners':
                                        x.uniqueListeners,
                                    'Listener Minutes':
                                        x.listenerMinutes,
                                    'Listener Hours':
                                        x.listenerHours
                                })
                            );

                        summaryRows.push({
                            Station: 'TOTAL',
                            Channel: '',
                            Route: '',
                            'Listener Sessions':
                                report.totals.sessions,
                            'Unique Listeners':
                                report.totals.uniqueListeners,
                            'Listener Minutes':
                                report.totals.listenerMinutes,
                            'Listener Hours':
                                report.totals.listenerHours
                        });

                        const summarySheet =
                            XLSX.utils.json_to_sheet(
                                summaryRows
                            );

                        const dailySheet =
                            XLSX.utils.json_to_sheet(
                                daily.map(
                                    x => ({
                                        Date: x.date,
                                        Station: x.station,
                                        Channel: x.channel,
                                        Route: x.routeId,
                                        'Listener Sessions':
                                            x.sessions,
                                        'Unique Listeners':
                                            x.uniqueListeners,
                                        'Listener Minutes':
                                            x.listenerMinutes
                                    })
                                )
                            );

                        XLSX.utils.book_append_sheet(
                            workbook,
                            summarySheet,
                            'Monthly Summary'
                        );

                        XLSX.utils.book_append_sheet(
                            workbook,
                            dailySheet,
                            'Daily Breakdown'
                        );

                        const buffer =
                            XLSX.write(
                                workbook,
                                {
                                    bookType: 'xlsx',
                                    type: 'buffer'
                                }
                            );

                        const filename =
                            `audiobridge-listeners-${month}.xlsx`;

                        res.writeHead(
                            200,
                            {
                                'Content-Type':
                                    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',

                                'Content-Disposition':
                                    `attachment; filename="${filename}"`,

                                'Cache-Control':
                                    'no-store'
                            }
                        );

                        return res.end(buffer);
                    }


                    // ------------------------------------------
                    // LOGS
                    // ------------------------------------------

                    if (
                        req.method === 'GET' &&
                        url.pathname ===
                            '/api/logs'
                    ) {

                        return json(
                            res,
                            200,
                            {
                                logs:
                                    db.audit.slice(
                                        0,
                                        200
                                    )
                            }
                        );
                    }


                    // ------------------------------------------
                    // SERVER CONFIGURATION (SAFE / NON-SECRET)
                    // ------------------------------------------

                    if (
                        req.method === 'GET' &&
                        url.pathname === '/api/config'
                    ) {
                        return json(
                            res,
                            200,
                            {
                                server: SERVER_NAME,
                                version: SERVER_VERSION,
                                port: PORT,
                                dataFile: DATA_FILE,
                                environment: process.env.NODE_ENV || 'development',
                                websocket: true
                            }
                        );
                    }


                    // ------------------------------------------
                    // CREATE ROUTE
                    // ------------------------------------------

                    if (
                        req.method === 'POST' &&
                        url.pathname ===
                            '/api/routes'
                    ) {

                        const b =
                            await parseBody(req);

                        const required = [
                            'routeId',
                            'stationId',
                            'station',
                            'casterId',
                            'channel'
                        ];

                        if (
                            required.some(
                                key =>
                                    !b[key]
                            ) ||
                            ![
                                'am',
                                'fm'
                            ].includes(
                                String(
                                    b.channel
                                ).toLowerCase()
                            )
                        ) {

                            return json(
                                res,
                                400,
                                {
                                    error:
                                        'INVALID_ROUTE'
                                }
                            );
                        }

                        const routeId =
                            String(
                                b.routeId
                            ).trim();

                        const casterId =
                            String(
                                b.casterId
                            ).trim();

                        if (
                            db.routes.some(
                                r =>
                                    r.routeId ===
                                    routeId
                            )
                        ) {

                            return json(
                                res,
                                409,
                                {
                                    error:
                                        'ROUTE_ID_ALREADY_EXISTS'
                                }
                            );
                        }

                        if (
                            findRouteAny(
                                casterId
                            )
                        ) {

                            return json(
                                res,
                                409,
                                {
                                    error:
                                        'CASTER_ALREADY_REGISTERED'
                                }
                            );
                        }

                        const channel =
                            String(
                                b.channel
                            )
                            .toLowerCase();

                        const r = {
                            routeId,

                            stationId:
                                String(
                                    b.stationId
                                ).trim(),

                            station:
                                String(
                                    b.station
                                ).trim(),

                            casterId,

                            channel,

                            enabled:
                                b.enabled !== false,

                            streamState:
                                'active',

                            createdAt:
                                new Date()
                                    .toISOString()
                        };

                        const endpoints =
                            ensureRouteEndpoints(
                                r
                            );

                        r.txPath =
                            endpoints.txPath;

                        r.rxPath =
                            endpoints.rxPath;

                        db.routes.push(r);

                        audit(
                            'route-created',
                            {
                                routeId:
                                    r.routeId,

                                stationId:
                                    r.stationId,

                                casterId:
                                    r.casterId,

                                channel:
                                    r.channel,

                                txPath:
                                    r.txPath,

                                rxPath:
                                    r.rxPath
                            }
                        );

                        return json(
                            res,
                            201,
                            {
                                route:
                                    publicRoute(
                                        r
                                    )
                            }
                        );
                    }


                    // ------------------------------------------
                    // UPDATE ROUTE

                    const routeMatch =
                        url.pathname.match(
                            /^\/api\/routes\/([^/]+)$/
                        );

                    if (
                        routeMatch &&
                        req.method ===
                            'PATCH'
                    ) {

                        const id =
                            decodeURIComponent(
                                routeMatch[1]
                            );

                        const r =
                            db.routes.find(
                                x =>
                                    x.routeId ===
                                    id
                            );

                        if (!r) {

                            return json(
                                res,
                                404,
                                {
                                    error:
                                        'ROUTE_NOT_FOUND'
                                }
                            );
                        }

                        const b =
                            await parseBody(req);

                        const oldRouteId = r.routeId;
                        const oldCasterId = r.casterId;
                        const oldChannel = r.channel;

                        const connected =
                            [];

                        for (
                            const room of Object.values(rooms)
                        ) {
                            for (
                                const ws of room.transmitters
                            ) {
                                if (
                                    ws.routeId ===
                                    r.routeId
                                ) {
                                    connected.push(ws);
                                }
                            }
                        }

                        // Editable identity fields.
                        if (
                            b.routeId !== undefined
                        ) {
                            const newRouteId =
                                String(
                                    b.routeId
                                ).trim();

                            if (
                                !newRouteId
                            ) {
                                return json(
                                    res,
                                    400,
                                    {
                                        error:
                                            'INVALID_ROUTE_ID'
                                    }
                                );
                            }

                            if (
                                newRouteId !==
                                    r.routeId &&
                                db.routes.some(
                                    x =>
                                        x.routeId ===
                                        newRouteId
                                )
                            ) {
                                return json(
                                    res,
                                    409,
                                    {
                                        error:
                                            'ROUTE_ID_ALREADY_EXISTS'
                                    }
                                );
                            }

                            r.routeId =
                                newRouteId;
                        }

                        if (
                            b.stationId !==
                            undefined
                        ) {
                            r.stationId =
                                String(
                                    b.stationId
                                ).trim();
                        }

                        if (
                            b.station !==
                            undefined
                        ) {
                            r.station =
                                String(
                                    b.station
                                ).trim();
                        }

                        if (
                            b.casterId !==
                            undefined
                        ) {
                            const newCasterId =
                                String(
                                    b.casterId
                                ).trim();

                            if (
                                !newCasterId
                            ) {
                                return json(
                                    res,
                                    400,
                                    {
                                        error:
                                            'INVALID_CASTER_ID'
                                    }
                                );
                            }

                            if (
                                newCasterId !==
                                    r.casterId &&
                                db.routes.some(
                                    x =>
                                        x !== r &&
                                        x.casterId ===
                                        newCasterId
                                )
                            ) {
                                return json(
                                    res,
                                    409,
                                    {
                                        error:
                                            'CASTER_ALREADY_REGISTERED'
                                    }
                                );
                            }

                            r.casterId =
                                newCasterId;
                        }

                        if (
                            b.channel !==
                            undefined
                        ) {
                            const newChannel =
                                String(
                                    b.channel
                                )
                                .trim()
                                .toLowerCase();

                            if (
                                ![
                                    'am',
                                    'fm'
                                ].includes(
                                    newChannel
                                )
                            ) {
                                return json(
                                    res,
                                    400,
                                    {
                                        error:
                                            'INVALID_CHANNEL'
                                    }
                                );
                            }

                            r.channel =
                                newChannel;
                        }

                        if (
                            b.enabled !==
                            undefined
                        ) {
                            r.enabled =
                                !!b.enabled;
                        }

                        if (
                            b.streamState !==
                            undefined
                        ) {

                            if (
                                !VALID_STATES.has(
                                    b.streamState
                                )
                            ) {
                                return json(
                                    res,
                                    400,
                                    {
                                        error:
                                            'INVALID_STREAM_STATE'
                                    }
                                );
                            }

                            r.streamState =
                                b.streamState;

                            r.lastControlAt =
                                new Date()
                                    .toISOString();
                        }

                        // Route endpoints always follow routeId.
                        const endpoints =
                            ensureRouteEndpoints(
                                r
                            );

                        r.txPath =
                            endpoints.txPath;

                        r.rxPath =
                            endpoints.rxPath;

                        const identityChanged =
                            oldRouteId !==
                                r.routeId ||
                            oldCasterId !==
                                r.casterId ||
                            oldChannel !==
                                r.channel;

                        saveData();

                        audit(
                            'route-updated',
                            {
                                routeId:
                                    r.routeId,

                                previousRouteId:
                                    oldRouteId,

                                previousCasterId:
                                    oldCasterId,

                                previousChannel:
                                    oldChannel,

                                changes:
                                    b,

                                activeConnections:
                                    connected.length
                            }
                        );

                        // Identity changes invalidate the old socket
                        // because its route binding has changed.
                        // The caster's normal reconnect logic can then
                        // reconnect to the new route endpoint.
                        if (
                            identityChanged
                        ) {

                            for (
                                const ws of connected
                            ) {

                                closeSocket(
                                    ws,
                                    1012,
                                    'Route updated; reconnect using the new route endpoint'
                                );
                            }

                        } else {

                            for (
                                const tx of connected
                            ) {

                                applyRouteState(
                                    tx,
                                    r.streamState
                                );
                            }
                        }

                        return json(
                            res,
                            200,
                            {
                                route:
                                    publicRoute(
                                        r
                                    ),

                                reconnectRequired:
                                    identityChanged
                            }
                        );
                    }


                    // REGISTRATION DECISION
                    // ------------------------------------------

                    const pendingMatch =
                        url.pathname.match(
                            /^\/api\/registrations\/([^/]+)$/
                        );

                    if (
                        pendingMatch &&
                        req.method ===
                            'POST'
                    ) {

                        const id =
                            decodeURIComponent(
                                pendingMatch[1]
                            );

                        const p =
                            db.pending.find(
                                x =>
                                    x.requestId ===
                                    id
                            );

                        if (!p) {

                            return json(
                                res,
                                404,
                                {
                                    error:
                                        'REGISTRATION_NOT_FOUND'
                                }
                            );
                        }

                        const b =
                            await parseBody(req);

                        const action =
                            b.action;

                        if (
                            action ===
                            'reject'
                        ) {

                            db.pending =
                                db.pending.filter(
                                    x =>
                                        x.requestId !==
                                        id
                                );

                            audit(
                                'registration-rejected',
                                {
                                    requestId:
                                        id,

                                    casterId:
                                        p.casterId
                                }
                            );

                            saveData();

                            if (p.wsRef) {

                                closeSocket(
                                    p.wsRef,
                                    1008,
                                    'Registration rejected by administrator'
                                );
                            }

                            return json(
                                res,
                                200,
                                {
                                    ok: true
                                }
                            );
                        }


                        if (
                            action ===
                            'accept'
                        ) {

                            if (
                                findRouteAny(
                                    p.casterId
                                )
                            ) {

                                return json(
                                    res,
                                    409,
                                    {
                                        error:
                                            'CASTER_ALREADY_REGISTERED'
                                    }
                                );
                            }

                            const channel =
                                String(
                                    b.channel ||
                                    p.channel
                                ).toLowerCase();

                            const r = {
                                routeId:
                                    String(
                                        b.routeId ||
                                        p.casterId
                                    ),

                                stationId:
                                    String(
                                        b.stationId ||
                                        p.casterId
                                    ),

                                station:
                                    String(
                                        b.station ||
                                        p.station ||
                                        p.casterId
                                    ),

                                casterId:
                                    p.casterId,

                                channel,

                                enabled:
                                    true,

                                streamState:
                                    'active',

                                createdAt:
                                    new Date()
                                        .toISOString()
                            };

                            if (
                                db.routes.some(
                                    x =>
                                        x.routeId ===
                                        r.routeId
                                )
                            ) {

                                return json(
                                    res,
                                    409,
                                    {
                                        error:
                                            'ROUTE_ID_ALREADY_EXISTS'
                                    }
                                );
                            }

                            const endpoints =
                                ensureRouteEndpoints(
                                    r
                                );

                            r.txPath =
                                endpoints.txPath;

                            r.rxPath =
                                endpoints.rxPath;

                            db.routes.push(r);

                            db.pending =
                                db.pending.filter(
                                    x =>
                                        x.requestId !==
                                        id
                                );

                            audit(
                                'registration-accepted',
                                {
                                    requestId:
                                        id,

                                    casterId:
                                        p.casterId,

                                    routeId:
                                        r.routeId,

                                    txPath:
                                        r.txPath,

                                    rxPath:
                                        r.rxPath
                                }
                            );

                            saveData();

                            if (p.wsRef) {

                                p.wsRef.routeId =
                                    r.routeId;

                                p.wsRef.registered =
                                    true;

                                p.wsRef.streamState =
                                    'active';

                                roomAccept(
                                    p.wsRef,
                                    r
                                );
                            }

                            return json(
                                res,
                                201,
                                {
                                    route:
                                        publicRoute(
                                            r
                                        )
                                }
                            );
                        }

                        return json(
                            res,
                            400,
                            {
                                error:
                                    'ACTION_MUST_BE_ACCEPT_OR_REJECT'
                            }
                        );
                    }


                    // ------------------------------------------
                    // CASTER CONTROL
                    // ------------------------------------------

                    const controlMatch =
                        url.pathname.match(
                            /^\/api\/casters\/([^/]+)\/control$/
                        );

                    if (
                        controlMatch &&
                        req.method ===
                            'POST'
                    ) {

                        const casterId =
                            decodeURIComponent(
                                controlMatch[1]
                            );

                        const b =
                            await parseBody(req);

                        if (
                            !VALID_STATES.has(
                                b.state
                            )
                        ) {

                            return json(
                                res,
                                400,
                                {
                                    error:
                                        'INVALID_STREAM_STATE'
                                }
                            );
                        }

                        const r =
                            findRouteAny(
                                casterId
                            );

                        if (!r) {

                            return json(
                                res,
                                404,
                                {
                                    error:
                                        'CASTER_NOT_FOUND'
                                }
                            );
                        }

                        r.streamState =
                            b.state;

                        r.lastControlAt =
                            new Date()
                                .toISOString();

                        r.lastControlReason =
                            String(
                                b.reason ||
                                ''
                            );

                        saveData();

                        let found = 0;

                        for (
                            const tx
                            of rooms[r.channel]
                                .transmitters
                        ) {

                            if (
                                tx.casterId ===
                                casterId
                            ) {

                                applyRouteState(
                                    tx,
                                    b.state,
                                    b.reason || ''
                                );

                                found++;
                            }
                        }

                        audit(
                            'caster-control',
                            {
                                casterId,
                                state:
                                    b.state,

                                connected:
                                    found > 0
                            }
                        );

                        return json(
                            res,
                            200,
                            {
                                ok: true,
                                connected:
                                    found > 0,

                                route:
                                    publicRoute(
                                        r
                                    )
                            }
                        );
                    }


                    return json(
                        res,
                        404,
                        {
                            error:
                                'API_NOT_FOUND'
                        }
                    );

                } catch (e) {

                    return json(
                        res,
                        500,
                        {
                            error:
                                'API_ERROR',

                            message:
                                e.message
                        }
                    );
                }
            }


            // ==================================================
            // CONTROL PANEL
            // ==================================================

            if (
                url.pathname ===
                    '/control/' ||
                url.pathname ===
                    '/control'
            ) {

                const html =
                    fs.readFileSync(
                        path.join(
                            __dirname,
                            'control-panel.html'
                        ),
                        'utf8'
                    );

                res.writeHead(
                    200,
                    {
                        'Content-Type':
                            'text/html; charset=utf-8'
                    }
                );

                return res.end(html);
            }


            res.writeHead(
                404,
                {
                    'Content-Type':
                        'text/plain'
                }
            );

            res.end(
                'Not Found'
            );
        }
    );


// ============================================================
// WEBSOCKET SERVER
// ============================================================

const wss =
    new WebSocket.Server({
        noServer: true,
        perMessageDeflate: false,
        maxPayload:
            2 * 1024 * 1024
    });


// ============================================================
// WEBSOCKET UPGRADE
// ============================================================

server.on(
    'upgrade',
    (
        request,
        socket,
        head
    ) => {

        let url;

        try {

            url =
                new URL(
                    request.url,
                    `http://${request.headers.host || 'localhost'}`
                );

        } catch (_) {

            socket.destroy();
            return;
        }

        const pathname =
            url.pathname;


        const dynamic =
            getRoutePathRole(
                pathname
            );


        const legacy =
            getRoomFromLegacyPath(
                pathname
            );


        if (
            !dynamic &&
            !legacy
        ) {

            socket.write(
                'HTTP/1.1 404 Not Found\r\n' +
                'Connection: close\r\n' +
                '\r\n'
            );

            socket.destroy();
            return;
        }


        wss.handleUpgrade(
            request,
            socket,
            head,
            ws => {

                wss.emit(
                    'connection',
                    ws,
                    request
                );
            }
        );
    }
);


// ============================================================
// ACCEPT TRANSMITTER
// ============================================================

function roomAccept(
    ws,
    route
) {

    const room =
        rooms[route.channel];

    const endpoints =
        ensureRouteEndpoints(
            route
        );

    ws.stationRoom =
        route.channel;

    ws.routeId =
        route.routeId;

    ws.registered =
        true;

    ws.streamState =
        route.streamState ||
        'active';

    ws.routeSpecific =
        true;

    room.transmitters.add(
        ws
    );

    if (
        ws.registrationTimer
    ) {

        clearTimeout(
            ws.registrationTimer
        );

        ws.registrationTimer =
            null;
    }

    sendJson(
        ws,
        {
            type:
                'transmitter-accepted',

            role:
                'transmitter',

            station:
                route.channel.toUpperCase(),

            stationName:
                route.station,

            casterId:
                ws.casterId,

            routeId:
                route.routeId,

            streamState:
                ws.streamState,

            txEndpoint:
                endpoints.txPath,

            rxEndpoint:
                endpoints.rxPath,

            server:
                SERVER_NAME,

            version:
                SERVER_VERSION,

            audioRequired:
                false,

            idleAllowed:
                true
        }
    );
}


// ============================================================
// WEBSOCKET CONNECTION
// ============================================================

wss.on(
    'connection',
    (
        ws,
        req
    ) => {

        const pathname =
            new URL(
                req.url,
                `http://${req.headers.host || 'localhost'}`
            ).pathname;


        const dynamic =
            getRoutePathRole(
                pathname
            );


        const legacy =
            getRoomFromLegacyPath(
                pathname
            );


        const isDynamic =
            !!dynamic;

        const route =
            dynamic?.route ||
            null;

        const role =
            dynamic?.role ||
            (
                isLegacyTxPath(pathname)
                    ? 'transmitter'
                    : 'receiver'
            );


        const station =
            route
                ? route.channel
                : legacy;


        const room =
            rooms[station];


        ws.stationRoom =
            station;

        ws.endpoint =
            pathname;

        ws.isTransmitter =
            role === 'transmitter';

        ws.isReceiver =
            role === 'receiver';

        ws.registered =
            !ws.isTransmitter;

        ws.routeSpecific =
            isDynamic;

        ws.routeId =
            route?.routeId ||
            null;

        ws.stationId =
            route?.stationId ||
            null;

        ws.casterId =
            null;

        const requestUrl =
            new URL(
                req.url,
                `http://${req.headers.host || 'localhost'}`
            );

        ws.listenerId =
            String(
                requestUrl.searchParams.get('listenerId') ||
                req.headers['x-listener-id'] ||
                ''
            ).trim() || null;

        ws.listenerSessionId =
            `L-${Date.now()}-${crypto.randomBytes(4).toString('hex')}`;

        ws.listenerRecorded =
            false;

        ws.listenerEnded =
            false;

        ws.listenerStartedAtMs =
            null;

        ws.isAlive =
            true;

        ws.connectedAt =
            new Date().toISOString();

        ws.lastAudioAt =
            null;

        ws.audioPackets =
            0;

        ws.audioBytes =
            0;

        ws.registrationTimer =
            null;

        ws.streamState =
            route?.streamState ||
            'active';


        ws.on(
            'pong',
            () => {
                ws.isAlive = true;
            }
        );


        // ------------------------------------------
        // ROUTE-SPECIFIC RECEIVER
        // ------------------------------------------

        if (
            ws.isReceiver
        ) {

            if (
                !route ||
                route.enabled === false ||
                route.streamState ===
                    'disabled'
            ) {

                sendJson(
                    ws,
                    {
                        type:
                            'receiver-rejected',

                        reason:
                            'ROUTE_DISABLED'
                    }
                );

                closeSocket(
                    ws,
                    1008,
                    'Route disabled'
                );

                return;
            }

            ws.registered =
                true;

            ws.stationName =
                route.station;

            ws.casterId =
                route.casterId;

            room.receivers.add(
                ws
            );

            recordListenerStart(ws);

            sendJson(
                ws,
                {
                    type:
                        'status',

                    role:
                        'receiver',

                    station:
                        station.toUpperCase(),

                    stationName:
                        route.station,

                    routeId:
                        route.routeId,

                    casterId:
                        route.casterId,

                    streamState:
                        route.streamState,

                    sampleRate:
                        room.config.sampleRate,

                    channels:
                        room.config.channels,

                    endpoint:
                        pathname,

                    server:
                        SERVER_NAME,

                    version:
                        SERVER_VERSION
                }
            );
        }


        // ------------------------------------------
        // LEGACY RECEIVER
        // ------------------------------------------

        if (
            ws.isReceiver &&
            !isDynamic
        ) {

            // Legacy receiver routes do not carry a routeId in the URL.
            // Bind the receiver to the currently active caster so
            // telemetry/control messages can still be routed correctly.
            ws.casterId =
                getActiveCasterId(room);

            const legacyRoute =
                ws.casterId
                    ? findRouteAny(ws.casterId)
                    : null;

            ws.routeId =
                legacyRoute?.routeId ||
                null;

            ws.stationId =
                legacyRoute?.stationId ||
                null;

            ws.stationName =
                legacyRoute?.station ||
                station.toUpperCase();

            room.receivers.add(
                ws
            );

            recordListenerStart(ws);

            sendJson(
                ws,
                {
                    type:
                        'status',

                    role:
                        'receiver',

                    station:
                        station.toUpperCase(),

                    sampleRate:
                        room.config.sampleRate,

                    channels:
                        room.config.channels,

                    endpoint:
                        pathname,

                    server:
                        SERVER_NAME,

                    version:
                        SERVER_VERSION,

                    legacy:
                        true
                }
            );
        }


        // ------------------------------------------
        // DYNAMIC TRANSMITTER
        // ------------------------------------------

        if (
            ws.isTransmitter &&
            isDynamic
        ) {

            if (
                !route ||
                route.enabled === false ||
                route.streamState ===
                    'disabled'
            ) {

                sendJson(
                    ws,
                    {
                        type:
                            'transmitter-rejected',

                        reason:
                            'ROUTE_DISABLED',

                        routeId:
                            route?.routeId ||
                            null
                    }
                );

                closeSocket(
                    ws,
                    1008,
                    'Route disabled'
                );

                return;
            }

            ws.registrationTimer =
                setTimeout(
                    () => {

                        if (
                            ws.readyState ===
                                WebSocket.OPEN &&
                            !ws.registered
                        ) {

                            sendJson(
                                ws,
                                {
                                    type:
                                        'transmitter-rejected',

                                    reason:
                                        'REGISTRATION_TIMEOUT'
                                }
                            );

                            closeSocket(
                                ws,
                                1008,
                                'Transmitter registration timeout'
                            );
                        }

                    },
                    REGISTRATION_TIMEOUT_MS
                );
        }


        // ------------------------------------------
        // LEGACY TRANSMITTER
        // ------------------------------------------

        if (
            ws.isTransmitter &&
            !isDynamic
        ) {

            ws.registrationTimer =
                setTimeout(
                    () => {

                        if (
                            ws.readyState ===
                                WebSocket.OPEN &&
                            !ws.registered
                        ) {

                            sendJson(
                                ws,
                                {
                                    type:
                                        'transmitter-rejected',

                                    reason:
                                        'REGISTRATION_TIMEOUT'
                                }
                            );

                            closeSocket(
                                ws,
                                1008,
                                'Transmitter registration timeout'
                            );
                        }

                    },
                    REGISTRATION_TIMEOUT_MS
                );
        }


        // ------------------------------------------
        // MESSAGES
        // ------------------------------------------

        ws.on(
            'message',
            (
                message,
                isBinary
            ) => {

                // --------------------------------------------------
                // RECEIVER CONTROL / TELEMETRY
                // --------------------------------------------------
                // AudioBridge audio remains binary. JSON messages on
                // receiver sockets are control/telemetry only.
                if (ws.isReceiver) {

                    if (!isBinary) {
                        let control = null;

                        try {
                            control = JSON.parse(
                                message.toString()
                            );
                        } catch (_) {
                            return;
                        }

                        if (!control || !control.type) {
                            return;
                        }

                        const allowedTypes = new Set([
                            'receiver-telemetry',
                            'receiver-config-ack',
                            'receiver-calibration',
                            'receiver-ready',
                            'diagnostic'
                        ]);

                        if (!allowedTypes.has(control.type)) {
                            return;
                        }

                        const targetRouteId =
                            ws.routeId || null;

                        let forwarded = 0;

                        room.transmitters.forEach(
                            transmitter => {

                                if (
                                    transmitter.readyState !==
                                    WebSocket.OPEN
                                ) {
                                    return;
                                }

                                if (
                                    targetRouteId &&
                                    transmitter.routeId !==
                                    targetRouteId
                                ) {
                                    return;
                                }

                                if (
                                    !targetRouteId &&
                                    ws.casterId &&
                                    transmitter.casterId !==
                                    ws.casterId
                                ) {
                                    return;
                                }

                                try {
                                    transmitter.send(
                                        JSON.stringify(control)
                                    );
                                    forwarded++;
                                } catch (_) {}
                            }
                        );

                        // Do not print periodic telemetry. Only report
                        // actual forwarding failures when useful.
                        if (
                            control.type ===
                            'receiver-config-ack' &&
                            forwarded === 0
                        ) {
                            console.warn(
                                `[CONTROL ${station.toUpperCase()}] ` +
                                `receiver-config-ack had no transmitter target`
                            );
                        }
                    }

                    return;
                }


                // --------------------------------------------------
                // TRANSMITTER CONTROL
                // --------------------------------------------------
                // The caster can send receiver tuning commands over
                // its already-authenticated WebSocket connection.
                if (
                    ws.isTransmitter &&
                    ws.registered &&
                    !isBinary
                ) {

                    let control = null;

                    try {
                        control = JSON.parse(
                            message.toString()
                        );
                    } catch (_) {
                        control = null;
                    }

                    if (
                        control &&
                        [
                            'receiver-config',
                            'receiver-telemetry-request',
                            'receiver-auto-calibrate',
                            'receiver-auto-calibrate-stop',
                            'diagnostic'
                        ].includes(control.type)
                    ) {

                        const activeRoute =
                            routeForConnection(ws);

                        if (
                            !activeRoute ||
                            activeRoute.enabled === false
                        ) {
                            return;
                        }

                        let forwarded = 0;

                        room.receivers.forEach(
                            receiver => {

                                if (
                                    receiver.readyState !==
                                    WebSocket.OPEN
                                ) {
                                    return;
                                }

                                if (
                                    receiver.routeId !==
                                    activeRoute.routeId
                                ) {
                                    return;
                                }

                                try {
                                    receiver.send(
                                        JSON.stringify(control)
                                    );
                                    forwarded++;
                                } catch (_) {}
                            }
                        );

                        if (
                            control.type ===
                            'receiver-config' &&
                            forwarded === 0
                        ) {
                            console.warn(
                                `[CONTROL ${station.toUpperCase()}] ` +
                                `receiver-config had no receiver target`
                            );
                        }

                        return;
                    }

                    // Unknown text from a registered transmitter is
                    // not an audio packet and should not enter the
                    // binary forwarding path.
                    return;
                }


                // --------------------------------------
                // REGISTRATION
                // --------------------------------------

                if (
                    ws.isTransmitter &&
                    !ws.registered
                ) {

                    if (isBinary) {

                        sendJson(
                            ws,
                            {
                                type:
                                    'transmitter-rejected',

                                reason:
                                    'REGISTRATION_REQUIRED'
                            }
                        );

                        closeSocket(
                            ws,
                            1008,
                            'Register transmitter first'
                        );

                        return;
                    }


                    let p;

                    try {

                        p =
                            JSON.parse(
                                message.toString()
                            );

                    } catch (_) {

                        sendJson(
                            ws,
                            {
                                type:
                                    'transmitter-rejected',

                                reason:
                                    'INVALID_REGISTRATION_JSON'
                            }
                        );

                        closeSocket(
                            ws,
                            1008,
                            'Invalid registration JSON'
                        );

                        return;
                    }


                    if (
                        !p ||
                        p.type !==
                            'register-transmitter'
                    ) {

                        sendJson(
                            ws,
                            {
                                type:
                                    'transmitter-rejected',

                                reason:
                                    'REGISTRATION_REQUIRED'
                            }
                        );

                        return;
                    }


                    const casterId =
                        String(
                            p.casterId ||
                            ''
                        ).trim();


                    const requestedChannel =
                        String(
                            p.channel ||
                            station
                        )
                        .trim()
                        .toLowerCase();


                    if (
                        !casterId
                    ) {

                        return rejectRegistration(
                            ws,
                            'MISSING_CASTER_ID'
                        );
                    }


                    if (
                        requestedChannel !==
                        station
                    ) {

                        return rejectRegistration(
                            ws,
                            'CHANNEL_MISMATCH'
                        );
                    }


                    // Dynamic route:
                    // the URL already identifies the route.
                    if (
                        isDynamic
                    ) {

                        if (
                            casterId !==
                            route.casterId
                        ) {

                            return rejectRegistration(
                                ws,
                                'CASTER_NOT_AUTHORIZED_FOR_ROUTE'
                            );
                        }

                        if (
                            route.enabled ===
                                false
                        ) {

                            return rejectRegistration(
                                ws,
                                'CASTER_DISABLED'
                            );
                        }

                        if (
                            route.streamState ===
                                'disabled'
                        ) {

                            return rejectRegistration(
                                ws,
                                'STREAM_DISABLED'
                            );
                        }

                        ws.casterId =
                            casterId;

                        ws.stationName =
                            String(
                                p.station ||
                                route.station
                            );

                        ws.format =
                            String(
                                p.format ||
                                'Opus'
                            );

                        ws.bitrate =
                            String(
                                p.bitrate ||
                                '128 kbps'
                            );

                        ws.sampleRate =
                            Number(
                                p.sampleRate ||
                                room.config.sampleRate
                            );

                        ws.channels =
                            Number(
                                p.channels ||
                                room.config.channels
                            );

                        roomAccept(
                            ws,
                            route
                        );

                        audit(
                            'caster-accepted',
                            {
                                casterId,
                                routeId:
                                    route.routeId,

                                mode:
                                    'route-endpoint'
                            }
                        );

                        return;
                    }


                    // Legacy route:
                    // preserve the existing behavior.
                    const legacyRoute =
                        findRoute(
                            casterId,
                            station
                        );


                    if (
                        !legacyRoute
                    ) {

                        const existing =
                            db.pending.find(
                                x =>
                                    x.casterId ===
                                        casterId &&
                                    x.channel ===
                                        station
                            );


                        const pending =
                            existing ||
                            {
                                requestId:
                                    `REG-${Date.now()}-${Math.random().toString(36).slice(2,8)}`,

                                casterId,

                                channel:
                                    station,

                                station:
                                    String(
                                        p.station ||
                                        casterId
                                    ),

                                format:
                                    String(
                                        p.format ||
                                        'unknown'
                                    ),

                                bitrate:
                                    String(
                                        p.bitrate ||
                                        ''
                                    ),

                                sampleRate:
                                    Number(
                                        p.sampleRate ||
                                        0
                                    ),

                                channels:
                                    Number(
                                        p.channels ||
                                        0
                                    ),

                                requestedAt:
                                    new Date()
                                        .toISOString()
                            };


                        pending.wsRef =
                            ws;


                        if (
                            !existing
                        ) {

                            db.pending.push(
                                pending
                            );
                        }


                        audit(
                            'registration-pending',
                            {
                                requestId:
                                    pending.requestId,

                                casterId,

                                channel:
                                    station
                            }
                        );


                        saveData();


                        ws.casterId =
                            casterId;

                        ws.routeId =
                            null;


                        sendJson(
                            ws,
                            {
                                type:
                                    'registration-pending',

                                requestId:
                                    pending.requestId,

                                reason:
                                    'ROUTE_NOT_AUTHORIZED'
                            }
                        );

                        return;
                    }


                    if (
                        legacyRoute.enabled ===
                            false
                    ) {

                        return rejectRegistration(
                            ws,
                            'CASTER_DISABLED'
                        );
                    }


                    if (
                        legacyRoute.streamState ===
                            'disabled'
                    ) {

                        return rejectRegistration(
                            ws,
                            'STREAM_DISABLED'
                        );
                    }


                    ws.casterId =
                        casterId;

                    ws.stationName =
                        String(
                            p.station ||
                            legacyRoute.station
                        );

                    ws.format =
                        String(
                            p.format ||
                            'Opus'
                        );

                    ws.bitrate =
                        String(
                            p.bitrate ||
                            '128 kbps'
                        );

                    ws.sampleRate =
                        Number(
                            p.sampleRate ||
                            room.config.sampleRate
                        );

                    ws.channels =
                        Number(
                            p.channels ||
                            room.config.channels
                        );


                    roomAccept(
                        ws,
                        legacyRoute
                    );


                    audit(
                        'caster-accepted',
                        {
                            casterId,
                            routeId:
                                legacyRoute.routeId,

                            mode:
                                'legacy-endpoint'
                        }
                    );

                    return;
                }


                // --------------------------------------
                // AUDIO
                // --------------------------------------

                if (
                    !ws.registered ||
                    !isBinary
                ) {
                    return;
                }


                const activeRoute =
                    routeForConnection(
                        ws
                    );


                if (
                    !activeRoute ||
                    activeRoute.enabled ===
                        false ||
                    activeRoute.streamState ===
                        'disabled'
                ) {
                    return;
                }


                let delivered = 0;
                let skipped = 0;


                // IMPORTANT:
                // Dynamic route receivers are isolated
                // from other routes.
                const targetReceivers =
                    room.receivers;


                if (
                    activeRoute.streamState !==
                        'frozen' &&
                    activeRoute.streamState !==
                        'paused' &&
                    activeRoute.streamState !==
                        'muted'
                ) {

                    targetReceivers.forEach(
                        receiver => {

                            // Route-specific receiver:
                            // only receive its own route.
                            if (
                                receiver.routeId !==
                                activeRoute.routeId
                            ) {
                                return;
                            }


                            if (
                                receiver.readyState !==
                                    WebSocket.OPEN
                            ) {

                                skipped++;
                                return;
                            }


                            if (
                                receiver.bufferedAmount >
                                MAX_RECEIVER_BUFFERED_BYTES
                            ) {

                                skipped++;

                                try {
                                    receiver.close(
                                        1008,
                                        'Receiver too slow'
                                    );
                                } catch (_) {}

                                return;
                            }


                            try {

                                receiver.send(
                                    message,
                                    {
                                        binary:
                                            true
                                    }
                                );

                                delivered++;

                            } catch (_) {

                                skipped++;

                                try {
                                    receiver.terminate();
                                } catch (__) {}
                            }
                        }
                    );
                }


                ws.audioPackets++;
                ws.lastAudioAt =
                    Date.now();

                ws.audioBytes +=
                    message.length;


                if (
                    ws.audioPackets %
                    500 ===
                    0
                ) {

                    console.log(
                        `[AUDIO ${station.toUpperCase()}] ` +
                        `route=${activeRoute.routeId} ` +
                        `caster=${ws.casterId} ` +
                        `state=${activeRoute.streamState} ` +
                        `packets=${ws.audioPackets} ` +
                        `bytes=${ws.audioBytes} ` +
                        `receivers=${room.receivers.size} ` +
                        `delivered=${delivered} ` +
                        `skipped=${skipped}`
                    );
                }
            }
        );


        ws.on(
            'close',
            (
                code,
                reason
            ) => {

                if (
                    ws.registrationTimer
                ) {

                    clearTimeout(
                        ws.registrationTimer
                    );
                }

                recordListenerEnd(ws);

                removeFromRoom(
                    ws
                );

                console.log(
                    `[-] ` +
                    `${ws.isTransmitter ? 'TX' : 'RX'} ` +
                    `${station.toUpperCase()} ` +
                    `route=${ws.routeId || 'legacy'} ` +
                    `caster=${ws.casterId || 'unknown'} ` +
                    `code=${code} ` +
                    `${reason || ''}`
                );
            }
        );


        ws.on(
            'error',
            e => {

                console.error(
                    `[WS ERROR ${station.toUpperCase()} ` +
                    `${ws.casterId || ''}]`,
                    e.message
                );
            }
        );
    }
);


function rejectRegistration(
    ws,
    reason
) {

    sendJson(
        ws,
        {
            type:
                'transmitter-rejected',

            reason
        }
    );

    closeSocket(
        ws,
        1008,
        reason
    );
}


// ============================================================
// HEARTBEAT
// ============================================================

setInterval(
    () => {

        wss.clients.forEach(
            ws => {

                if (
                    ws.readyState !==
                    WebSocket.OPEN
                ) {
                    return;
                }

                if (
                    ws.isAlive ===
                    false
                ) {

                    try {
                        ws.terminate();
                    } catch (_) {}

                    return;
                }

                ws.isAlive =
                    false;

                try {

                    ws.ping();

                } catch (_) {

                    try {
                        ws.terminate();
                    } catch (__) {}
                }
            }
        );

    },
    HEARTBEAT_INTERVAL_MS
);


// ============================================================
// SHUTDOWN
// ============================================================

function shutdown() {

    wss.clients.forEach(
        ws => {

            try {

                ws.close(
                    1001,
                    'Server shutting down'
                );

            } catch (_) {}
        }
    );

    server.close(
        () => process.exit(0)
    );

    setTimeout(
        () => process.exit(0),
        5000
    );
}


process.on(
    'SIGTERM',
    () => shutdown()
);

process.on(
    'SIGINT',
    () => shutdown()
);


// ============================================================
// START
// ============================================================

server.listen(
    PORT,
    '0.0.0.0',
    () => {

        console.log(
            `${SERVER_NAME} ` +
            `${SERVER_VERSION} ` +
            `listening on ${PORT}`
        );
    }
);
