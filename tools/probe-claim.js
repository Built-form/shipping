'use strict';
require('dotenv').config();
const { getPool } = require('../src/db');

(async () => {
    const conn = await getPool().getConnection();
    try {
        const [rows] = await conn.query(
            `SELECT al.batch_date, al.account, al.country, al.completed_at
             FROM amazon_report_jobs al
             WHERE al.account = 'JFA' AND al.country IN ('ES','IT')
             ORDER BY al.batch_date DESC
             LIMIT 5`
        );
        for (const r of rows) {
            console.log('---');
            console.log('country:', r.country);
            console.log('typeof batch_date:', typeof r.batch_date, '|', r.batch_date && r.batch_date.constructor.name);
            console.log('raw batch_date:', r.batch_date);
            console.log('String:', String(r.batch_date));
            console.log('slice(0,10):', String(r.batch_date).slice(0, 10));
            console.log('typeof completed_at:', typeof r.completed_at, '|', r.completed_at && r.completed_at.constructor.name);
            console.log('raw completed_at:', r.completed_at);
        }
    } finally {
        conn.release();
        process.exit(0);
    }
})();
