'use strict';
// Probe what GET /api/v1/orders would emit for one row.
require('dotenv').config();
const { getPool, closePool } = require('../src/db');

(async () => {
    const id = process.argv[2] || '485';
    const pool = getPool();
    const conn = await pool.getConnection();
    try {
        const [rows] = await conn.query(
            `SELECT orders.*,
                    COALESCE((SELECT SUM(quantity) FROM order_receipts WHERE order_id = orders.id AND type = 'received'), 0) AS received_quantity,
                    COALESCE((SELECT SUM(quantity) FROM order_receipts WHERE order_id = orders.id AND type = 'not_received'), 0) AS not_received_quantity
               FROM orders
              WHERE orders.deleted_at IS NULL AND orders.id = ?`, [id]);
        // Re-import the handler to use its rowToOrder. Easier: copy the body
        // here.
        const r = rows[0];
        function formatDate(val){ if(!val) return null; return val.toISOString?.().slice(0,10) ?? val; }
        function parseDates(raw){ if(typeof raw==='string') return JSON.parse(raw)||{}; return raw||{}; }
        const shape = {
            id: r.id,
            jfCode: r.jf_code,
            scheduledDate: formatDate(r.scheduled_date),
            status: r.status,
        };
        console.log(JSON.stringify(shape, null, 2));
    } finally {
        conn.release();
        await closePool();
    }
})();
