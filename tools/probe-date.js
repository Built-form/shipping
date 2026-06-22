'use strict';
require('dotenv').config();
const { getPool } = require('../src/db');

(async () => {
    const conn = await getPool().getConnection();
    try {
        const [r] = await conn.query(
            `SELECT batch_date FROM amazon_report_jobs
             WHERE batch_date = CURDATE() AND account = 'JFA'
             LIMIT 1`
        );
        console.log('typeof batch_date:', typeof r[0].batch_date);
        console.log('constructor:', r[0].batch_date && r[0].batch_date.constructor.name);
        console.log('raw:', r[0].batch_date);
        console.log('String():', String(r[0].batch_date));
        console.log('.slice(0,10):', String(r[0].batch_date).slice(0, 10));
    } finally {
        conn.release();
        process.exit(0);
    }
})();
