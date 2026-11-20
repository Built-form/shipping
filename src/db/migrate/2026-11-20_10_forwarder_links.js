'use strict';

// Forwarder links (2026-11-20): a container's forwarder and a forwarder
// payment name a row of ShipLine's Forwarders list (`forwarders`), and a
// forwarder payment may be priced from a freight booking or one stock
// movement. Rules: src/lib/forwarder-links.js, src/lib/payment-extras.js.
//
//   shipments.forwarder_id          with shipments.forwarder (the name), one
//                                   value: every write sets both
//   payment_extras.forwarder_id     the payee from the list; supplier_name
//                                   stays the recorded name (sign-off keys and
//                                   transfer matching are still by name)
//   payment_extras.freight_booking_id / freight_movement_id
//                                   where a forwarder payment's price came
//                                   from. Columns of their own, never
//                                   source_kind / source_id: ShipLine's editor
//                                   sends source_kind back on every edit, and
//                                   code from before this change rewrites it
//   payment_extras.freight_source_key  generated; UNIQUE = one live extra per
//                                   booking or movement (a soft delete frees it)
//   freight_bookings.live_shipment_id  generated; UNIQUE = one live booking
//                                   (requested or confirmed) per container.
//                                   Until now only an app check
//                                   (CONTAINER_TAKEN), under a lock on the
//                                   booking row rather than the container
//
// Checks (MySQL enforces CHECK from 8.0.16; TEST runs 8.4): at most one
// freight source per extra, and a freight-priced extra is a forwarder payment
// with no invoice source.
//
// No foreign keys, like the rest of this schema. Each step looks at
// information_schema first, so a re-run adds nothing. It refuses, before
// changing anything, while two live bookings hold one container: the unique
// key could not be added.

const has = async (conn, sql, params) => Number((await conn.query(sql, params))[0][0].n) > 0;
const hasColumn = (conn, table, column) => has(conn,
    `SELECT COUNT(*) AS n FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?`, [table, column]);
const hasIndex = (conn, table, index) => has(conn,
    `SELECT COUNT(*) AS n FROM information_schema.STATISTICS
      WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = ?`, [table, index]);
const hasCheck = (conn, table, name) => has(conn,
    `SELECT COUNT(*) AS n FROM information_schema.TABLE_CONSTRAINTS
      WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND CONSTRAINT_NAME = ? AND CONSTRAINT_TYPE = 'CHECK'`, [table, name]);

const STEPS = [
    ['column', 'shipments', 'forwarder_id', 'ALTER TABLE shipments ADD COLUMN forwarder_id INT NULL AFTER forwarder'],
    ['index', 'shipments', 'idx_shipments_forwarder', 'ALTER TABLE shipments ADD KEY idx_shipments_forwarder (forwarder_id)'],

    ['column', 'payment_extras', 'forwarder_id', 'ALTER TABLE payment_extras ADD COLUMN forwarder_id INT NULL AFTER supplier_key'],
    ['index', 'payment_extras', 'idx_payment_extras_forwarder', 'ALTER TABLE payment_extras ADD KEY idx_payment_extras_forwarder (forwarder_id)'],
    ['column', 'payment_extras', 'freight_booking_id', 'ALTER TABLE payment_extras ADD COLUMN freight_booking_id INT NULL AFTER source_id'],
    ['index', 'payment_extras', 'idx_payment_extras_freight_booking', 'ALTER TABLE payment_extras ADD KEY idx_payment_extras_freight_booking (freight_booking_id)'],
    ['column', 'payment_extras', 'freight_movement_id', 'ALTER TABLE payment_extras ADD COLUMN freight_movement_id INT NULL AFTER freight_booking_id'],
    ['index', 'payment_extras', 'idx_payment_extras_freight_movement', 'ALTER TABLE payment_extras ADD KEY idx_payment_extras_freight_movement (freight_movement_id)'],
    ['column', 'payment_extras', 'freight_source_key',
        `ALTER TABLE payment_extras ADD COLUMN freight_source_key VARCHAR(24) GENERATED ALWAYS AS (CASE WHEN deleted_at IS NOT NULL THEN NULL WHEN freight_booking_id IS NOT NULL THEN CONCAT('b:', freight_booking_id) WHEN freight_movement_id IS NOT NULL THEN CONCAT('m:', freight_movement_id) END) STORED`],
    ['index', 'payment_extras', 'uq_payment_extras_freight_source', 'ALTER TABLE payment_extras ADD UNIQUE KEY uq_payment_extras_freight_source (freight_source_key)'],
    ['check', 'payment_extras', 'chk_payment_extras_one_freight_source',
        'ALTER TABLE payment_extras ADD CONSTRAINT chk_payment_extras_one_freight_source CHECK (freight_booking_id IS NULL OR freight_movement_id IS NULL)'],
    ['check', 'payment_extras', 'chk_payment_extras_freight_source_alone',
        `ALTER TABLE payment_extras ADD CONSTRAINT chk_payment_extras_freight_source_alone CHECK ((freight_booking_id IS NULL AND freight_movement_id IS NULL) OR (source_kind IS NULL AND source_id IS NULL AND rides_with = 'shipment'))`],

    ['column', 'freight_bookings', 'live_shipment_id',
        `ALTER TABLE freight_bookings ADD COLUMN live_shipment_id INT GENERATED ALWAYS AS (IF(status IN ('requested', 'confirmed'), shipment_id, NULL)) STORED`],
    ['index', 'freight_bookings', 'uq_freight_bookings_live_shipment', 'ALTER TABLE freight_bookings ADD UNIQUE KEY uq_freight_bookings_live_shipment (live_shipment_id)'],
];

exports.STEPS = STEPS;

exports.up = async (conn, { log }) => {
    const [dupes] = await conn.query(
        `SELECT shipment_id, GROUP_CONCAT(id ORDER BY id) AS ids
           FROM freight_bookings
          WHERE status IN ('requested', 'confirmed') AND shipment_id IS NOT NULL
          GROUP BY shipment_id
         HAVING COUNT(*) > 1`
    );
    if (dupes.length) {
        throw new Error(dupes.map(d => `Container ${d.shipment_id} has ${String(d.ids).split(',').length} live freight bookings (${d.ids}).`).join(' ')
            + " Cancel all but one on ShipLine's Freight page, then run the migration again.");
    }
    let added = 0;
    for (const [kind, table, name, sql] of STEPS) {
        const present = kind === 'column' ? await hasColumn(conn, table, name)
            : kind === 'index' ? await hasIndex(conn, table, name)
                : await hasCheck(conn, table, name);
        if (present) continue;
        await conn.query(sql);
        added++;
    }
    log(`forwarder links: ${added} of ${STEPS.length} schema change(s) applied`);
};
