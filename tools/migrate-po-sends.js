'use strict';

// One-off: create the purchase_order_document_sends table on the configured
// database. The endpoint creates it lazily on first hit, but this lets us
// verify the SQL upfront.

require('dotenv').config();
const { getPool, closePool } = require('../src/db');

(async () => {
    const pool = getPool();
    const conn = await pool.getConnection();
    try {
        await conn.execute(`
            CREATE TABLE IF NOT EXISTS purchase_order_document_sends (
                id INT NOT NULL AUTO_INCREMENT,
                purchase_order_document_id INT NOT NULL,
                sent_to JSON NOT NULL,
                subject VARCHAR(255) NULL,
                front_message_uid VARCHAR(128) NULL,
                front_conversation_id VARCHAR(64) NULL,
                sent_by_email VARCHAR(255) NULL,
                sent_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                KEY idx_document_id (purchase_order_document_id),
                KEY idx_sent_at (sent_at)
            )
        `);
        const [rows] = await conn.query(`DESCRIBE purchase_order_document_sends`);
        console.log('Table purchase_order_document_sends:');
        console.table(rows.map(r => ({ field: r.Field, type: r.Type, null: r.Null, key: r.Key, default: r.Default })));
    } catch (err) {
        console.error('Fatal:', err.message);
        process.exitCode = 1;
    } finally {
        conn.release();
        await closePool();
    }
})();
