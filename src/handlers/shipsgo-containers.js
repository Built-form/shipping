'use strict';

require('dotenv').config();
const log = require('../lib/logger');
const { getPool, closePool } = require('../db');
const { getShipmentByContainer, parseShipment } = require('../services/shipsgo');

// The `containers` table (one row per container_number, arrays as JSON, the
// full ShipsGo payload in `raw` so new fields can be surfaced later without
// re-fetching) is schema applied at deploy time from src/db/migrate/.

// ISO 6346: 3-letter owner code + equipment category (U/J/Z) + 6 digits + check
// digit. Operators sometimes put an AWB or a courier tracking number in
// external_container_number; ShipsGo rejects those with a 422 that still costs
// us a request against the account rate limit, so they're filtered out here.
const CONTAINER_NUMBER_RE = /^[A-Z]{4}\d{7}$/;

// Statuses past the point ShipsGo tells us anything new — the goods are already
// at (or through) the warehouse. A container is only re-fetched while at least
// one of its orders is still short of these, which keeps the per-run request
// count down: the account rate limit is shared across every ShipsGo call, and
// most container numbers on file belong to long-since-received orders.
const SETTLED_STATUSES = [
    'ARRIVED_AT_WAREHOUSE', 'RECEIVED', 'PARTIALLY_RECEIVED',
    'IN_WAREHOUSE', 'MINTSOFT', 'DESTROYED',
];

async function getDistinctExternalContainers(conn) {
    const [rows] = await conn.query(`
        SELECT TRIM(external_container_number) AS cn
        FROM orders
        WHERE external_container_number IS NOT NULL
          AND TRIM(external_container_number) <> ''
          AND deleted_at IS NULL
        GROUP BY TRIM(external_container_number)
        HAVING SUM(status NOT IN (?)) > 0
    `, [SETTLED_STATUSES]);
    const all = rows.map(r => r.cn).filter(Boolean);
    const valid = all.filter(cn => CONTAINER_NUMBER_RE.test(cn.toUpperCase()));
    const skipped = all.filter(cn => !CONTAINER_NUMBER_RE.test(cn.toUpperCase()));
    if (skipped.length) {
        log.info(`[shipsgo-containers] skipping ${skipped.length} non-container values: ${skipped.join(', ')}`);
    }
    return valid;
}

// Source of truth for the writable column list. Excludes container_number
// (PK) and fetched_at/updated_at (set by the SQL itself).
const WRITABLE_COLS = [
    'bl_number', 'booking_ref', 'vessel_imo', 'vessel_name', 'voyage', 'shipping_line',
    'carrier_scac', 'carrier_ref_number', 'bl_type', 'service_name',
    'pol_name', 'pol_locode', 'pol_country',
    'pod_name', 'pod_locode', 'pod_country',
    'por_name', 'por_locode',
    'final_delivery_name', 'final_delivery_locode',
    'discharge_terminal', 'transshipments',
    'current_lat', 'current_lng',
    'departure_date', 'departure_is_actual',
    'arrival_date', 'arrival_is_actual',
    'eta', 'ata', 'eta_initial', 'total_transit_days', 'carrier_last_updated',
    'milestones', 'co2_emissions', 'delay_status', 'container_size_type',
    'seal_number', 'holds', 'free_time_info', 'tags', 'demurrage_info',
    'shipsgo_id', 'route_geojson', 'raw',
];

const JSON_COLS = new Set([
    'transshipments', 'milestones', 'holds', 'free_time_info', 'tags', 'demurrage_info',
    'route_geojson', 'raw',
]);

async function upsertContainer(conn, parsed) {
    if (!parsed.container_number) return;

    const cols = ['container_number', ...WRITABLE_COLS, 'fetched_at'];
    const placeholders = cols.map(c => c === 'fetched_at' ? 'NOW()' : '?').join(', ');
    const updates = WRITABLE_COLS.map(c => `${c} = VALUES(${c})`).concat('fetched_at = NOW()').join(',\n             ');

    const values = [parsed.container_number, ...WRITABLE_COLS.map(c => {
        const v = parsed[c];
        if (v == null) return null;
        if (JSON_COLS.has(c)) {
            // Skip empty arrays so we store NULL instead of "[]"
            if (Array.isArray(v) && v.length === 0) return null;
            return JSON.stringify(v);
        }
        return v;
    })];

    await conn.execute(
        `INSERT INTO containers (${cols.join(', ')})
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
        const containerNumbers = await getDistinctExternalContainers(conn);
        log.info(`[shipsgo-containers] ${containerNumbers.length} distinct external container numbers`);

        let succeeded = 0;
        let failed = 0;
        const errors = [];

        for (const cn of containerNumbers) {
            try {
                const raw = await getShipmentByContainer(cn, { mapPoint: true });
                const parsed = parseShipment(raw);
                if (!parsed.container_number) parsed.container_number = cn;
                await upsertContainer(conn, parsed);
                succeeded++;
                log.info(`[shipsgo-containers] ${cn} OK`);
            } catch (err) {
                failed++;
                errors.push({ container: cn, error: err.message });
                log.warn(`[shipsgo-containers] ${cn} FAIL: ${err.message}`);
            }
        }

        return {
            statusCode: 200,
            body: JSON.stringify({ total: containerNumbers.length, succeeded, failed, errors }),
        };
    } catch (err) {
        log.error('[shipsgo-containers] Fatal:', err.message);
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
