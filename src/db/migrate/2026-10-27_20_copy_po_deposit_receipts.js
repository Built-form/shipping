'use strict';

// Bring the supplier portal's deposit ticks over from jportal.po_deposit_receipts
// into this schema's po_deposit_receipts (2026-10-27_10_po_deposit_receipts.sql),
// which replaces it. INSERT IGNORE on the PO's unique key, so a re-run copies
// nothing twice. A server without the jportal schema, table or grant (production
// has no portal) has nothing to copy and is not an error.

const { jportalSchema } = require('../../lib/jportal');

// Unknown database, no such table, command / column denied.
const NOTHING_TO_COPY = new Set([1049, 1146, 1142, 1143]);

exports.up = async (conn, { log }) => {
    let rows;
    try {
        [rows] = await conn.query(
            `SELECT purchase_order_id, supplier_id, user_id, confirmed_by_email, received_on,
                    created_at, updated_at
               FROM ${jportalSchema()}.po_deposit_receipts`
        );
    } catch (e) {
        if (!NOTHING_TO_COPY.has(e.errno)) throw e;
        log(`no ${jportalSchema()}.po_deposit_receipts on this server; nothing to copy`);
        return;
    }
    let copied = 0;
    for (const r of rows) {
        const [res] = await conn.query(
            `INSERT IGNORE INTO po_deposit_receipts
                (purchase_order_id, supplier_id, user_id, confirmed_by_email, received_on, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)`,
            [r.purchase_order_id, r.supplier_id, r.user_id, r.confirmed_by_email, r.received_on, r.created_at, r.updated_at]
        );
        copied += res.affectedRows;
    }
    log(`${copied} of ${rows.length} receipt(s) copied, the rest already here`);
};
