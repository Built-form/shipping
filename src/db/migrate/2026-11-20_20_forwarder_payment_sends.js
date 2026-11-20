'use strict';

// "Send to forwarder" (2026-11-20; "A forwarder's payment" in
// docs/payment-proof-send.md): a recorded payment to a freight forwarder is
// sent to that forwarder's contacts as a link to the portal, with the send
// log's own machinery (supplier_payment_sends, notice_kind 'forwarder').
//
//   supplier_payment_sends.forwarder_id   the forwarder a 'forwarder' send
//       went to (forwarders.id); NULL on every supplier send. The portal's
//       view (2026-11-20_21) shows a payment to a forwarder only while a send
//       that went out carries the forwarder the payment is linked to NOW, so a
//       payment re-pointed at another forwarder is not shown to either until
//       it is sent again. A name is never the test: a name is not who someone is.
//
// No foreign key, like the rest of this schema. Each step looks at
// information_schema first, so a re-run adds nothing.

const has = async (conn, sql, params) => Number((await conn.query(sql, params))[0][0].n) > 0;
const hasColumn = (conn, table, column) => has(conn,
    `SELECT COUNT(*) AS n FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?`, [table, column]);
const hasIndex = (conn, table, index) => has(conn,
    `SELECT COUNT(*) AS n FROM information_schema.STATISTICS
      WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = ?`, [table, index]);

const STEPS = [
    ['column', 'supplier_payment_sends', 'forwarder_id', 'ALTER TABLE supplier_payment_sends ADD COLUMN forwarder_id INT NULL AFTER supplier_name'],
    ['index', 'supplier_payment_sends', 'idx_payment_sends_forwarder', 'ALTER TABLE supplier_payment_sends ADD KEY idx_payment_sends_forwarder (forwarder_id, supplier_payment_id)'],
];

exports.STEPS = STEPS;

exports.up = async (conn, { log }) => {
    let added = 0;
    for (const [kind, table, name, sql] of STEPS) {
        const present = kind === 'column' ? await hasColumn(conn, table, name) : await hasIndex(conn, table, name);
        if (present) continue;
        await conn.query(sql);
        added++;
    }
    log(`forwarder payment sends: ${added} of ${STEPS.length} schema change(s) applied`);
};
