'use strict';

// Push ShipsGo data straight from `containers` into `orders`, joined by
// container_number. Replaces the indirect ShipsGo → Asana → orders path
// (via update-imported-dates.js) once the Asana import is retired.
//
// Caveat while Asana import is still active: any column this handler writes
// gets wiped on the next 10-min `TRUNCATE TABLE orders`. Run frequency is
// only meaningful once import-asana-orders.handler is disabled.

require('dotenv').config();
const { getPool, closePool } = require('../db');
const log = require('../lib/logger');

function dateOnly(val) {
    if (!val) return null;
    const d = val instanceof Date ? val : new Date(val);
    if (isNaN(d.getTime())) return null;
    return d.toISOString().slice(0, 10);
}

// ShipsGo statuses meaning the shipment is in transit OR beyond. We trigger the
// CONSOLIDATED → ON_SEA / ON_AIR advance on any of these (not just the first
// in-transit status) so an order can't get stuck at CONSOLIDATED if the
// shipment skips the in-transit status between hourly syncs. Lifecycle:
//   sea: NEW→INPROGRESS→BOOKED→LOADED→[SAILING→ARRIVED→DISCHARGED]
//   air: NEW→INPROGRESS→BOOKED→[EN_ROUTE→LANDED→DELIVERED]
const SEA_DEPARTED_STATUSES = new Set(['SAILING', 'ARRIVED', 'DISCHARGED']);
const AIR_DEPARTED_STATUSES = new Set(['EN_ROUTE', 'LANDED', 'DELIVERED']);

// Auto-advances are recorded in audit_log attributed to this pseudo-user, so an
// order's history shows ShipsGo (not a person) moved it to ON_SEA / ON_AIR.
const AUDIT_USER = 'shipsgo';

// One audit row per auto-advance, matching the shape orders.js writes for a
// status change (entity 'order', action 'update', diffed before/after).
async function recordStatusAdvance(conn, orderId, fromStatus, toStatus) {
    try {
        await conn.execute(
            `INSERT INTO audit_log (entity_type, entity_id, action, before_json, after_json, user_email)
             VALUES ('order', ?, 'update', ?, ?, ?)`,
            [orderId, JSON.stringify({ status: fromStatus }), JSON.stringify({ status: toStatus }), AUDIT_USER]
        );
    } catch (err) {
        log.warn('[orders-from-shipsgo] audit insert failed', { orderId, error: err.message });
    }
}

async function syncOrdersFromShipsGo(conn) {
    const [rows] = await conn.query(`
        SELECT o.id,
               o.status           AS o_status,
               c.delay_status     AS c_status,
               c.eta              AS c_eta,
               c.vessel_name      AS c_vessel_name,
               c.arrival_date     AS c_arrival_date,
               c.arrival_is_actual,
               c.departure_date   AS c_departure_date,
               c.departure_is_actual
        FROM orders o
        JOIN containers c
          ON c.container_number = TRIM(o.external_container_number)
        WHERE o.external_container_number IS NOT NULL
          AND TRIM(o.external_container_number) <> ''
          AND o.deleted_at IS NULL
    `);

    let updated = 0;
    let advanced = 0;
    for (const r of rows) {
        // Scalar dates → flat columns. dates JSON is reserved for status
        // transitions and is left untouched here.
        const arrivedDate = r.arrival_is_actual === 1 ? dateOnly(r.c_arrival_date) : null;
        const shippedDate = r.departure_is_actual === 1 ? dateOnly(r.c_departure_date) : null;

        // Auto-advance CONSOLIDATED → ON_SEA once the container is in transit or
        // later (SAILING / ARRIVED / DISCHARGED). The `status = 'CONSOLIDATED'`
        // guard in the CASE means we only ever move forward from that one status
        // and never touch any other order (so it can never move back).
        const departed = SEA_DEPARTED_STATUSES.has(String(r.c_status || '').toUpperCase()) ? 1 : 0;

        await conn.execute(
            `UPDATE orders
                SET eta           = COALESCE(?, eta),
                    vessel_name   = COALESCE(?, vessel_name),
                    arrived_date  = COALESCE(?, arrived_date),
                    shipped_date  = COALESCE(?, shipped_date),
                    status        = CASE WHEN ? = 1 AND status = 'CONSOLIDATED'
                                         THEN 'ON_SEA' ELSE status END
              WHERE id = ?`,
            [
                dateOnly(r.c_eta),
                r.c_vessel_name || null,
                arrivedDate,
                shippedDate,
                departed,
                r.id,
            ]
        );
        if (departed && r.o_status === 'CONSOLIDATED') {
            advanced++;
            await recordStatusAdvance(conn, r.id, 'CONSOLIDATED', 'ON_SEA');
        }
        updated++;
    }

    return { matched: rows.length, updated, advancedToOnSea: advanced };
}

// Air counterpart: push tracking from `air_shipments` into `orders`, joined by
// AWB number. eta / arrived (RCF actual) / shipped (DEP actual) map to the same
// flat columns the sea sync uses. There's no air vessel; vessel_name is left
// untouched so a sea value isn't clobbered if an order ever carries both.
async function syncOrdersFromAir(conn) {
    const [rows] = await conn.query(`
        SELECT o.id,
               o.status           AS o_status,
               a.status           AS a_status,
               a.eta              AS a_eta,
               a.arrival_date     AS a_arrival_date,
               a.arrival_is_actual,
               a.departure_date   AS a_departure_date,
               a.departure_is_actual
        FROM orders o
        JOIN air_shipments a
          ON a.awb_number = TRIM(o.awb_number)
        WHERE o.awb_number IS NOT NULL
          AND TRIM(o.awb_number) <> ''
          AND o.deleted_at IS NULL
    `);

    let updated = 0;
    let advanced = 0;
    for (const r of rows) {
        const arrivedDate = r.arrival_is_actual === 1 ? dateOnly(r.a_arrival_date) : null;
        const shippedDate = r.departure_is_actual === 1 ? dateOnly(r.a_departure_date) : null;

        // Auto-advance CONSOLIDATED → ON_AIR once the air shipment is in transit
        // or later (EN_ROUTE / LANDED / DELIVERED). Mirrors the sea rule: the
        // `status = 'CONSOLIDATED'` guard means we only ever move forward from
        // that one status and never touch any other order (never moves back).
        const departed = AIR_DEPARTED_STATUSES.has(String(r.a_status || '').toUpperCase()) ? 1 : 0;

        await conn.execute(
            `UPDATE orders
                SET eta           = COALESCE(?, eta),
                    arrived_date  = COALESCE(?, arrived_date),
                    shipped_date  = COALESCE(?, shipped_date),
                    status        = CASE WHEN ? = 1 AND status = 'CONSOLIDATED'
                                         THEN 'ON_AIR' ELSE status END
              WHERE id = ?`,
            [dateOnly(r.a_eta), arrivedDate, shippedDate, departed, r.id]
        );
        if (departed && r.o_status === 'CONSOLIDATED') {
            advanced++;
            await recordStatusAdvance(conn, r.id, 'CONSOLIDATED', 'ON_AIR');
        }
        updated++;
    }

    return { matched: rows.length, updated, advancedToOnAir: advanced };
}

exports.handler = async () => {
    const pool = getPool();
    const conn = await pool.getConnection();
    try {
        log.info('[orders-from-shipsgo] starting');
        const sea = await syncOrdersFromShipsGo(conn);

        // Air sync is best-effort: air_shipments is created by shipsgo-air,
        // which may not have run yet. A missing table must not break sea sync.
        let air = { matched: 0, updated: 0 };
        try {
            air = await syncOrdersFromAir(conn);
        } catch (err) {
            if (err.code === 'ER_NO_SUCH_TABLE') {
                log.warn('[orders-from-shipsgo] air_shipments table not present yet; skipping air sync');
            } else {
                throw err;
            }
        }

        const result = { sea, air };
        log.info('[orders-from-shipsgo] done', result);
        return { statusCode: 200, body: JSON.stringify(result) };
    } catch (err) {
        log.error('[orders-from-shipsgo] failed', err);
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
