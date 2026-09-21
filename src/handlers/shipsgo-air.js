'use strict';

// Air (AWB) counterpart of shipsgo-containers.js. Reads distinct AWB numbers
// off the orders table, registers/fetches each from ShipsGo's air API and
// upserts one row per AWB into `air_shipments`. The full payload is kept in
// `raw` so new fields can be surfaced later without re-fetching.

require('dotenv').config();
const log = require('../lib/logger');
const { getPool, closePool } = require('../db');
const { getShipmentByAwb, parseAirShipment } = require('../services/shipsgo');

// Mirrors shipsgo-containers: stop re-fetching an AWB once every order on it
// has reached the warehouse, so the shared ShipsGo rate limit is spent on
// shipments still in transit.
const SETTLED_STATUSES = [
    'ARRIVED_AT_WAREHOUSE', 'RECEIVED', 'PARTIALLY_RECEIVED',
    'IN_WAREHOUSE', 'MINTSOFT', 'DESTROYED',
];

async function getDistinctAwbNumbers(conn) {
    const [rows] = await conn.query(`
        SELECT TRIM(awb_number) AS awb
        FROM orders
        WHERE awb_number IS NOT NULL
          AND TRIM(awb_number) <> ''
          AND deleted_at IS NULL
        GROUP BY TRIM(awb_number)
        HAVING SUM(status NOT IN (?)) > 0
    `, [SETTLED_STATUSES]);
    return rows.map(r => r.awb).filter(Boolean);
}

// Source of truth for the writable column list. Excludes awb_number (PK) and
// fetched_at/updated_at (set by the SQL itself).
const WRITABLE_COLS = [
    'reference', 'airline_name', 'airline_iata', 'status',
    'origin_name', 'origin_iata', 'origin_country',
    'destination_name', 'destination_iata', 'destination_country',
    'transshipments',
    'current_lat', 'current_lng',
    'departure_date', 'departure_is_actual', 'departure_initial',
    'arrival_date', 'arrival_is_actual',
    'eta', 'ata', 'eta_initial',
    'total_transit_time', 'transit_percentage', 'ts_count',
    'milestones', 'tags', 'checked_at',
    'shipsgo_id', 'route_geojson', 'raw',
];

const JSON_COLS = new Set(['transshipments', 'milestones', 'tags', 'route_geojson', 'raw']);

async function upsertAirShipment(conn, parsed) {
    if (!parsed.awb_number) return;

    const cols = ['awb_number', ...WRITABLE_COLS, 'fetched_at'];
    const placeholders = cols.map(c => c === 'fetched_at' ? 'NOW()' : '?').join(', ');
    const updates = WRITABLE_COLS.map(c => `${c} = VALUES(${c})`).concat('fetched_at = NOW()').join(',\n             ');

    const values = [parsed.awb_number, ...WRITABLE_COLS.map(c => {
        const v = parsed[c];
        if (v == null) return null;
        if (JSON_COLS.has(c)) {
            if (Array.isArray(v) && v.length === 0) return null;
            return JSON.stringify(v);
        }
        return v;
    })];

    await conn.execute(
        `INSERT INTO air_shipments (${cols.join(', ')})
         VALUES (${placeholders})
         ON DUPLICATE KEY UPDATE
             ${updates}`,
        values
    );
}

exports.handler = async () => {
    const pool = getPool();
    const conn = await pool.getConnection();
    try {
        const awbNumbers = await getDistinctAwbNumbers(conn);
        log.info(`[shipsgo-air] ${awbNumbers.length} distinct AWB numbers`);

        let succeeded = 0;
        let failed = 0;
        const errors = [];

        for (const awb of awbNumbers) {
            try {
                const raw = await getShipmentByAwb(awb);
                const parsed = parseAirShipment(raw);
                if (!parsed.awb_number) parsed.awb_number = awb;
                await upsertAirShipment(conn, parsed);
                succeeded++;
                log.info(`[shipsgo-air] ${awb} OK`);
            } catch (err) {
                failed++;
                errors.push({ awb, error: err.message });
                log.warn(`[shipsgo-air] ${awb} FAIL: ${err.message}`);
            }
        }

        return {
            statusCode: 200,
            body: JSON.stringify({ total: awbNumbers.length, succeeded, failed, errors }),
        };
    } catch (err) {
        log.error('[shipsgo-air] Fatal:', err.message);
        return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
    } finally {
        conn.release();
    }
};

if (require.main === module) {
    exports.handler()
        .then(r => console.log(r.body))
        .catch(err => console.error('Fatal:', err))
        .finally(() => closePool());
}
