'use strict';

// One-off: find purchase_order_documents rows with the most line items and
// largest file sizes, so we can pick a richer doc for a visual QA send.

require('dotenv').config();
const { getPool, closePool } = require('../src/db');

(async () => {
    const pool = getPool();
    const conn = await pool.getConnection();
    try {
        const [rows] = await conn.query(`
            SELECT d.id AS doc_id,
                   d.purchase_order_id,
                   d.version,
                   d.file_size,
                   po.po_number,
                   po.supplier,
                   (SELECT COUNT(*) FROM orders WHERE purchase_order_id = d.purchase_order_id AND deleted_at IS NULL) AS line_count
              FROM purchase_order_documents d
              JOIN purchase_orders po ON po.id = d.purchase_order_id
             WHERE po.deleted_at IS NULL
             ORDER BY d.file_size DESC, line_count DESC
             LIMIT 15
        `);
        console.table(rows);
    } catch (err) {
        console.error('Fatal:', err.message);
    } finally {
        conn.release();
        await closePool();
    }
})();
