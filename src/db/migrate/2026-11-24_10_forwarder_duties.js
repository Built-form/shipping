'use strict';

// Three duties on a container (2026-11-24, docs/forwarder-containers.md): a
// container is no longer one forwarder's. Rules: src/lib/forwarder-containers.js
// (dutiesOf). The view that tells the portal: 2026-11-24_11.
//
//   shipments.delivery_forwarder_id   who carries it from the UK port to the
//                                     warehouse, picked by hand. An id from the
//                                     Forwarders list (its name is always read
//                                     from the list, never stored here)
//   shipments.customs_forwarder_id    who handles its customs entry, picked by
//                                     hand: an attached forwarder, or a broker
//                                     that carries nothing
//   freight_bookings.scope            what a booking covers: 'port_to_port'
//                                     (freight only: what every booking covered
//                                     until now, and the default) or 'end_to_end'
//                                     (freight and delivery). A CHECK holds the
//                                     vocabulary: a scope that covered nothing
//                                     would be a live booking that holds no duty
//
// shipments.forwarder / forwarder_id (2026-11-20) stay as they are: they are the
// FREIGHT pick. One live booking per container (uq_freight_bookings_live_shipment)
// stays too: a second live booking, for the UK leg alone, comes with its own
// migration.
//
// Additive: every existing row reads as it did (no delivery or customs pick,
// every booking port to port). No foreign keys, like the rest of this schema.
// Each step looks at information_schema first, so a re-run adds nothing.

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
    ['column', 'shipments', 'delivery_forwarder_id', 'ALTER TABLE shipments ADD COLUMN delivery_forwarder_id INT NULL AFTER forwarder_id'],
    ['index', 'shipments', 'idx_shipments_delivery_forwarder', 'ALTER TABLE shipments ADD KEY idx_shipments_delivery_forwarder (delivery_forwarder_id)'],
    ['column', 'shipments', 'customs_forwarder_id', 'ALTER TABLE shipments ADD COLUMN customs_forwarder_id INT NULL AFTER delivery_forwarder_id'],
    ['index', 'shipments', 'idx_shipments_customs_forwarder', 'ALTER TABLE shipments ADD KEY idx_shipments_customs_forwarder (customs_forwarder_id)'],

    ['column', 'freight_bookings', 'scope', "ALTER TABLE freight_bookings ADD COLUMN scope VARCHAR(16) NOT NULL DEFAULT 'port_to_port' AFTER status"],
    ['check', 'freight_bookings', 'chk_freight_bookings_scope',
        "ALTER TABLE freight_bookings ADD CONSTRAINT chk_freight_bookings_scope CHECK (scope IN ('port_to_port', 'end_to_end'))"],
];

exports.STEPS = STEPS;

exports.up = async (conn, { log }) => {
    let added = 0;
    for (const [kind, table, name, sql] of STEPS) {
        const present = kind === 'column' ? await hasColumn(conn, table, name)
            : kind === 'index' ? await hasIndex(conn, table, name)
                : await hasCheck(conn, table, name);
        if (present) continue;
        await conn.query(sql);
        added++;
    }
    log(`forwarder duties: ${added} of ${STEPS.length} schema change(s) applied`);
};
