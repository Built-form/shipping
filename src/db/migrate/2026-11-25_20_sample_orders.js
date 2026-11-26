'use strict';

// Sample orders (2026-11-25, docs/sample-orders.md). A purchase order gains a
// KIND, and so does each of its lines. Rules: src/lib/sample-orders.js.
//
//   purchase_orders.kind             'stock' (the default, and what every PO
//                                    made before this is) or 'sample'. Set when
//                                    the PO is made; no statement changes it.
//   orders.order_kind                the same word on every line, stamped by the
//                                    server from the line's PO. Kept on the line
//                                    on purpose: the readers that must leave
//                                    samples out (stock totals, alerts, the AI
//                                    importer) filter one column with no join,
//                                    and a legacy line with no PO row is 'stock'.
//   orders.courier_name,
//   orders.courier_tracking          how a sample travels. Never awb_number:
//                                    ShipsGo's air sync and JFPRO's landed-cost
//                                    import both key on that.
//   orders.sample_received_qty       how many arrived (the hand-over; the
//                                    cupboard count is JFPRO's). The ordered
//                                    quantity, and so the bill, is not touched.
//   orders.sample_status_changed_at  when a sample line's status last changed.
//                                    A supplier's "mark as sent" made on a page
//                                    loaded before this moment is stale.
//   orders.sample_ever_sent          1 from the first time a sample line reaches
//                                    ON_AIR. Never cleared, corrections included.
//   orders.sample_lot_key            generated, UNIQUE: the sample code a sample
//                                    line carries, so ONE line carries a given
//                                    code, for good. NULL for a stock line, and
//                                    for a sample line deleted without ever
//                                    having been sent (its code is free again).
//                                    The database holds this, not a
//                                    check-then-write: two creates for one code
//                                    cannot both win.
//   orders.idx_orders_lot_number     JFPRO's guard looks orders up by lot number
//                                    before it lets a sample's code change.
//
// Everything is additive: every existing row reads as it did (a stock PO, stock
// lines). The two CHECKs hold the vocabulary. No foreign keys, like the rest of
// this schema. Each step looks at information_schema first, so a re-run changes
// nothing.
//
// The view that reads JFPRO's sample lots: 2026-11-25_21. The portal's grant:
// 2026-11-25_22. NOTHING here arms the feature: no sample order can be made
// until this stage's row is in app_migrations (tools/check-sample-orders.js).

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
    ['column', 'purchase_orders', 'kind', "ALTER TABLE purchase_orders ADD COLUMN kind VARCHAR(16) NOT NULL DEFAULT 'stock' AFTER company_id"],
    ['check', 'purchase_orders', 'chk_purchase_orders_kind',
        "ALTER TABLE purchase_orders ADD CONSTRAINT chk_purchase_orders_kind CHECK (kind IN ('stock', 'sample'))"],

    ['column', 'orders', 'order_kind', "ALTER TABLE orders ADD COLUMN order_kind VARCHAR(16) NOT NULL DEFAULT 'stock'"],
    ['check', 'orders', 'chk_orders_order_kind',
        "ALTER TABLE orders ADD CONSTRAINT chk_orders_order_kind CHECK (order_kind IN ('stock', 'sample'))"],

    ['column', 'orders', 'courier_name', 'ALTER TABLE orders ADD COLUMN courier_name VARCHAR(64) NULL'],
    ['column', 'orders', 'courier_tracking', 'ALTER TABLE orders ADD COLUMN courier_tracking VARCHAR(64) NULL'],
    ['column', 'orders', 'sample_received_qty', 'ALTER TABLE orders ADD COLUMN sample_received_qty INT NULL'],
    ['column', 'orders', 'sample_status_changed_at', 'ALTER TABLE orders ADD COLUMN sample_status_changed_at DATETIME NULL'],
    ['column', 'orders', 'sample_ever_sent', 'ALTER TABLE orders ADD COLUMN sample_ever_sent TINYINT NOT NULL DEFAULT 0'],

    // One sample line per sample code. Trimmed and upper-cased, so a code typed
    // in another case is the same code. Stock lines hold NULL here, and NULLs
    // never collide.
    ['column', 'orders', 'sample_lot_key',
        "ALTER TABLE orders ADD COLUMN sample_lot_key VARCHAR(255) GENERATED ALWAYS AS (IF(order_kind = 'sample' AND (deleted_at IS NULL OR sample_ever_sent = 1), UPPER(TRIM(lot_number)), NULL)) STORED"],
    ['index', 'orders', 'uq_orders_sample_lot_key', 'ALTER TABLE orders ADD UNIQUE KEY uq_orders_sample_lot_key (sample_lot_key)'],

    ['index', 'orders', 'idx_orders_lot_number', 'ALTER TABLE orders ADD KEY idx_orders_lot_number (lot_number)'],
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
    log(`sample orders: ${added} of ${STEPS.length} schema change(s) applied`);
};
