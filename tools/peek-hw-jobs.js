'use strict';
require('dotenv').config();
const { getPool, closePool } = require('../src/db');

(async () => {
    const conn = await getPool().getConnection();
    try {
        const [rows] = await conn.query(
            `SELECT country, report_type, status, document_id IS NOT NULL AS has_doc,
                    error, requested_at, completed_at, processed_at
             FROM amazon_report_jobs
             WHERE batch_date = CURDATE() AND account = 'Hangerworld'
             ORDER BY country, report_type`
        );
        console.log('Hangerworld jobs for today:');
        for (const r of rows) {
            console.log(`  ${r.country.padEnd(4)} ${r.report_type.padEnd(18)} status=${r.status.padEnd(11)} doc=${r.has_doc ? 'Y' : 'N'} err=${r.error || ''}`);
        }
    } finally {
        conn.release();
        await closePool();
    }
})().catch(err => { console.error(err); process.exit(1); });
