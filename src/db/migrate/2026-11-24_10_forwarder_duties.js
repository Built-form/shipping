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
//                                     or 'delivery' (the delivery alone: a
//                                     booking on a delivery request)
//
// The delivery leg through Freight quotes (docs/freight-quotes.md "A delivery
// request"):
//
//   freight_quote_requests.kind       'freight' (the default, and what every
//                                     request made before this is) or 'delivery':
//                                     a UK haulier is asked to collect at the UK
//                                     port and deliver to the warehouse. Set when
//                                     the request is made; no statement changes it.
//                                     A booking's scope follows its request's kind.
//   freight_bookings.live_freight_shipment_id, live_delivery_shipment_id
//                                     generated, each UNIQUE: a container takes at
//                                     most one live booking that covers the
//                                     freight and at most one that covers the
//                                     delivery (an end-to-end booking is in both).
//
// A customs entry's MRN (docs/forwarder-containers.md "The customs entry"):
//
//   shipment_forwarder_documents.reference   the MRN of a kept customs entry;
//                                     NULL for every other kind of document.
//
// shipments.forwarder / forwarder_id (2026-11-20) stay as they are: they are the
// FREIGHT pick.
//
// THE ONE STEP THAT IS NOT ADDITIVE is the last: the unique key of 2026-11-20,
// uq_freight_bookings_live_shipment (one live booking per CONTAINER), is dropped,
// once the two per-leg keys exist. Its generated column stays, unused. The two
// new keys forbid everything the old one did except a freight booking beside a
// delivery booking, which only this release's code can make. Code from before
// this migration still checks "one live booking" itself before it attaches, but
// has no key behind that check any more, and would read a delivery booking as a
// freight one: once a delivery request exists, shipping is not rolled back
// below this release (docs/forwarder-containers.md "Rolling back").
//
// Everything else is additive: every existing row reads as it did (no delivery
// or customs pick, every booking port to port, every request a freight
// request). No foreign keys, like the rest of this schema. Each step looks at
// information_schema first, so a re-run changes nothing.

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
        "ALTER TABLE freight_bookings ADD CONSTRAINT chk_freight_bookings_scope CHECK (scope IN ('port_to_port', 'end_to_end', 'delivery'))"],

    ['column', 'freight_quote_requests', 'kind', "ALTER TABLE freight_quote_requests ADD COLUMN kind VARCHAR(16) NOT NULL DEFAULT 'freight' AFTER forwarder_name"],
    ['check', 'freight_quote_requests', 'chk_freight_quote_requests_kind',
        "ALTER TABLE freight_quote_requests ADD CONSTRAINT chk_freight_quote_requests_kind CHECK (kind IN ('freight', 'delivery'))"],

    // One live booking per LEG of a container.
    ['column', 'freight_bookings', 'live_freight_shipment_id',
        "ALTER TABLE freight_bookings ADD COLUMN live_freight_shipment_id INT GENERATED ALWAYS AS (IF(status IN ('requested', 'confirmed') AND scope IN ('port_to_port', 'end_to_end'), shipment_id, NULL)) STORED"],
    ['index', 'freight_bookings', 'uq_freight_bookings_live_freight', 'ALTER TABLE freight_bookings ADD UNIQUE KEY uq_freight_bookings_live_freight (live_freight_shipment_id)'],
    ['column', 'freight_bookings', 'live_delivery_shipment_id',
        "ALTER TABLE freight_bookings ADD COLUMN live_delivery_shipment_id INT GENERATED ALWAYS AS (IF(status IN ('requested', 'confirmed') AND scope IN ('end_to_end', 'delivery'), shipment_id, NULL)) STORED"],
    ['index', 'freight_bookings', 'uq_freight_bookings_live_delivery', 'ALTER TABLE freight_bookings ADD UNIQUE KEY uq_freight_bookings_live_delivery (live_delivery_shipment_id)'],

    ['column', 'shipment_forwarder_documents', 'reference', 'ALTER TABLE shipment_forwarder_documents ADD COLUMN reference VARCHAR(32) NULL AFTER kind'],

    // LAST, and only once the two keys above exist: the key for the whole container goes.
    ['dropindex', 'freight_bookings', 'uq_freight_bookings_live_shipment', 'ALTER TABLE freight_bookings DROP INDEX uq_freight_bookings_live_shipment'],
];

exports.STEPS = STEPS;

exports.up = async (conn, { log }) => {
    let added = 0;
    for (const [kind, table, name, sql] of STEPS) {
        if (kind === 'dropindex') {
            // Never before both per-leg keys are in place: they are what replaces it.
            if (!await hasIndex(conn, table, name)) continue;
            for (const key of ['uq_freight_bookings_live_freight', 'uq_freight_bookings_live_delivery']) {
                if (!await hasIndex(conn, table, key)) throw new Error(`${key} is missing: ${name} is not dropped`);
            }
            await conn.query(sql);
            added++;
            continue;
        }
        const present = kind === 'column' ? await hasColumn(conn, table, name)
            : kind === 'index' ? await hasIndex(conn, table, name)
                : await hasCheck(conn, table, name);
        if (present) continue;
        await conn.query(sql);
        added++;
    }
    log(`forwarder duties: ${added} of ${STEPS.length} schema change(s) applied`);
};
