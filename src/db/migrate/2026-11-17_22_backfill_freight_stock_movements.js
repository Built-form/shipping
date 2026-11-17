'use strict';

// Copy the stock movements of requests already sent out of their JSON column
// (freight_quote_requests.stock_movements, 2026-11-16_30) into
// freight_stock_movements (2026-11-17_20), one row each, unpriced. The "to" of
// the old shape is dropped: a movement no longer has one.
//
// The rows take the request's batch_id as their add_key (as a new send's do),
// and INSERT IGNORE on (request_id, add_key, line_no) makes a re-run a no-op.
// A JSON entry that would not pass today's rules (no factory, no volume) is
// left behind rather than guessed at.

const F = require('../../lib/freight-quotes');

exports.up = async (conn, { log }) => {
    const [requests] = await conn.query(
        `SELECT id, batch_id, stock_movements, created_by_email
           FROM freight_quote_requests
          WHERE stock_movements IS NOT NULL`
    );
    let copied = 0;
    let skipped = 0;
    for (const r of requests) {
        const list = F.jsonArray(r.stock_movements);
        for (let i = 0; i < list.length; i++) {
            const checked = F.validateMovements([list[i]]);
            if (checked.code) { skipped++; continue; }
            const m = checked.value[0];
            const [ins] = await conn.query(
                `INSERT IGNORE INTO freight_stock_movements
                    (request_id, add_key, line_no, factory, cbm, weight_kg, created_by_email)
                 VALUES (?, ?, ?, ?, ?, ?, ?)`,
                [r.id, r.batch_id, i + 1, m.from, m.cbm, m.weightKg, r.created_by_email || null]
            );
            copied += ins.affectedRows || 0;
        }
    }
    log(`${copied} stock movement(s) copied from ${requests.length} request(s)${skipped ? `, ${skipped} unreadable left behind` : ''}`);
};
