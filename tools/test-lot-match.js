'use strict';
// Verifies leading-zero-insensitive lot matching in qc-report-check.matchOrders.
require('dotenv').config();
const { getPool, closePool } = require('../src/db');
const { matchOrders } = require('../src/services/qc-report-check');

(async () => {
    const pool = getPool();
    const conn = await pool.getConnection();
    try {
        // A real order whose lot is zero-padded.
        const [cands] = await conn.query(
            `SELECT jf_code, lot_number FROM orders
              WHERE lot_number LIKE '0%' AND lot_number REGEXP '^[0-9]+$'
                AND jf_code IS NOT NULL AND deleted_at IS NULL
              LIMIT 1`
        );
        if (!cands.length) { console.log('No zero-padded lot found to test.'); return; }
        const { jf_code, lot_number } = cands[0];
        const stripped = lot_number.replace(/^0+/, '');
        const extra = '00' + lot_number;

        for (const [label, lot] of [['stored as-is', lot_number], ['zero stripped', stripped], ['extra zeros', extra]]) {
            const m = await matchOrders(conn, jf_code, lot);
            const hit = m.rows.some(r => r.jf_code === jf_code && r.lot_number === lot_number);
            console.log(`${jf_code} lot "${lot}" (${label}) → how=${m.how}, matched order with lot "${lot_number}": ${hit ? 'YES' : 'no'} (${m.rows.length} row(s))`);
        }

        // Negative control: a different jf_code with the same digits must NOT match.
        const other = await matchOrders(conn, jf_code.replace(/\d$/, d => (Number(d) + 1) % 10), stripped);
        console.log(`negative control (different jf_code, same lot digits) → ${other.rows.length} row(s) (expect 0 unless that code legitimately exists)`);
    } finally { conn.release(); await closePool(); }
})().catch(e => { console.error(e.message); process.exit(1); });
