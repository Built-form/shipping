'use strict';

// Local orders (docs/local-orders.md): a third kind of purchase order, 'local',
// for stock a supplier delivers to the warehouse themselves. It uses the kind
// columns sample orders added (2026-11-25_20); this widens the two CHECKs that
// hold their vocabulary, and adds nothing else.
//
//   purchase_orders.kind   'stock' | 'sample' | 'local'
//   orders.order_kind      the same word on every line, stamped by the server
//
// Every existing row reads as it did. Each CHECK is dropped and re-added in one
// ALTER, so the table is never without one. A re-run changes nothing: a CHECK
// that already allows 'local' is left alone. Needs 2026-11-25_20 first (the
// columns and the sample-only CHECKs).

const CHECKS = [
    ['purchase_orders', 'chk_purchase_orders_kind', 'kind'],
    ['orders', 'chk_orders_order_kind', 'order_kind'],
];

const clauseOf = async (conn, name) => {
    const [rows] = await conn.query(
        `SELECT CHECK_CLAUSE FROM information_schema.CHECK_CONSTRAINTS
          WHERE CONSTRAINT_SCHEMA = DATABASE() AND CONSTRAINT_NAME = ?`, [name]);
    return rows.length ? String(rows[0].CHECK_CLAUSE || '') : null;
};

exports.CHECKS = CHECKS;

exports.up = async (conn, { log }) => {
    let changed = 0;
    for (const [table, name, column] of CHECKS) {
        const clause = await clauseOf(conn, name);
        if (clause === null) {
            throw new Error(`local orders: ${table}.${name} is missing; run 2026-11-25_20_sample_orders first`);
        }
        if (/'local'/.test(clause)) continue;
        await conn.query(
            `ALTER TABLE ${table} DROP CHECK ${name},
               ADD CONSTRAINT ${name} CHECK (${column} IN ('stock', 'sample', 'local'))`
        );
        changed++;
    }
    log(`local orders: ${changed} of ${CHECKS.length} kind check(s) widened`);
};
