'use strict';

require('dotenv').config();
const log = require('../lib/logger');
const { getPool, closePool } = require('../db');
const { getShipmentByContainer, parseShipment } = require('../services/shipsgo');

// One row per container_number; arrays (transshipments, milestones, demurrage)
// stored as JSON. The full ShipsGo payload is kept in `raw` so new fields can
// be surfaced later without re-fetching.
async function ensureContainersTable(conn) {
    await conn.execute(`
        CREATE TABLE IF NOT EXISTS containers (
            container_number VARCHAR(50) NOT NULL PRIMARY KEY,
            bl_number VARCHAR(100) NULL,
            booking_ref VARCHAR(100) NULL,
            vessel_imo VARCHAR(20) NULL,
            vessel_name VARCHAR(255) NULL,
            voyage VARCHAR(50) NULL,
            shipping_line VARCHAR(255) NULL,
            carrier_scac VARCHAR(10) NULL,
            carrier_ref_number VARCHAR(100) NULL,
            bl_type VARCHAR(50) NULL,
            service_name VARCHAR(100) NULL,
            pol_name VARCHAR(255) NULL,
            pol_locode VARCHAR(10) NULL,
            pol_country VARCHAR(2) NULL,
            pod_name VARCHAR(255) NULL,
            pod_locode VARCHAR(10) NULL,
            pod_country VARCHAR(2) NULL,
            por_name VARCHAR(255) NULL,
            por_locode VARCHAR(10) NULL,
            final_delivery_name VARCHAR(255) NULL,
            final_delivery_locode VARCHAR(10) NULL,
            discharge_terminal VARCHAR(255) NULL,
            transshipments JSON NULL,
            current_lat DECIMAL(10,6) NULL,
            current_lng DECIMAL(10,6) NULL,
            departure_date DATETIME NULL,
            departure_is_actual TINYINT(1) NULL,
            arrival_date DATETIME NULL,
            arrival_is_actual TINYINT(1) NULL,
            eta DATETIME NULL,
            ata DATETIME NULL,
            eta_initial DATETIME NULL,
            total_transit_days INT NULL,
            carrier_last_updated DATETIME NULL,
            milestones JSON NULL,
            co2_emissions DECIMAL(12,2) NULL,
            delay_status VARCHAR(50) NULL,
            container_size_type VARCHAR(50) NULL,
            seal_number VARCHAR(100) NULL,
            holds JSON NULL,
            free_time_info JSON NULL,
            tags JSON NULL,
            demurrage_info JSON NULL,
            shipsgo_id VARCHAR(100) NULL,
            route_geojson JSON NULL,
            raw JSON NULL,
            fetched_at DATETIME NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
    `);

    // Idempotent column adds for tables created before these fields existed.
    const columnsToAdd = [
        'ALTER TABLE containers ADD COLUMN carrier_scac VARCHAR(10) NULL',
        'ALTER TABLE containers ADD COLUMN carrier_ref_number VARCHAR(100) NULL',
        'ALTER TABLE containers ADD COLUMN bl_type VARCHAR(50) NULL',
        'ALTER TABLE containers ADD COLUMN service_name VARCHAR(100) NULL',
        'ALTER TABLE containers ADD COLUMN pol_country VARCHAR(2) NULL',
        'ALTER TABLE containers ADD COLUMN pod_country VARCHAR(2) NULL',
        'ALTER TABLE containers ADD COLUMN por_name VARCHAR(255) NULL',
        'ALTER TABLE containers ADD COLUMN por_locode VARCHAR(10) NULL',
        'ALTER TABLE containers ADD COLUMN final_delivery_name VARCHAR(255) NULL',
        'ALTER TABLE containers ADD COLUMN final_delivery_locode VARCHAR(10) NULL',
        'ALTER TABLE containers ADD COLUMN discharge_terminal VARCHAR(255) NULL',
        'ALTER TABLE containers ADD COLUMN carrier_last_updated DATETIME NULL',
        'ALTER TABLE containers ADD COLUMN seal_number VARCHAR(100) NULL',
        'ALTER TABLE containers ADD COLUMN holds JSON NULL',
        'ALTER TABLE containers ADD COLUMN free_time_info JSON NULL',
        'ALTER TABLE containers ADD COLUMN tags JSON NULL',
        'ALTER TABLE containers ADD COLUMN total_transit_days INT NULL',
        'ALTER TABLE containers ADD COLUMN eta_initial DATETIME NULL',
        'ALTER TABLE containers DROP COLUMN total_transit_minutes',
        'ALTER TABLE containers ADD COLUMN route_geojson JSON NULL',
    ];
    for (const ddl of columnsToAdd) {
        try { await conn.execute(ddl); } catch (e) { /* already exists */ }
    }
}

async function getDistinctExternalContainers(conn) {
    const [rows] = await conn.query(`
        SELECT DISTINCT TRIM(external_container_number) AS cn
        FROM orders
        WHERE external_container_number IS NOT NULL
          AND TRIM(external_container_number) <> ''
    `);
    return rows.map(r => r.cn).filter(Boolean);
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
        await ensureContainersTable(conn);
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
