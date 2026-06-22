'use strict';
// Show today's amazon_report_jobs for both accounts, flagging anything not
// PROCESSED (FAILED / REQUESTED / DONE-but-stuck) and printing the error.
//   node tools/peek-jobs.js [YYYY-MM-DD]
require('dotenv').config();
const { getPool, closePool } = require('../src/db');

const date = (process.argv[2] || '').trim();
const dateClause = date ? '?' : 'CURDATE()';

(async () => {
    const conn = await getPool().getConnection();
    try {
        const [rows] = await conn.query(
            `SELECT account, country, report_type, status,
                    document_id IS NOT NULL AS has_doc, result_cache IS NOT NULL AS has_cache,
                    error, requested_at, completed_at, processed_at
             FROM amazon_report_jobs
             WHERE batch_date = ${dateClause}
             ORDER BY account, FIELD(country,'ALL','DE','UK','FR','ES','IT','US'), report_type`,
            date ? [date] : []
        );
        const byStatus = {};
        for (const r of rows) byStatus[r.status] = (byStatus[r.status] || 0) + 1;
        console.log(`jobs for ${date || 'today'}: ${rows.length} rows  ${JSON.stringify(byStatus)}\n`);

        for (const r of rows) {
            const flag = r.status === 'PROCESSED' ? '   ' : '>> ';
            console.log(`${flag}${r.account.padEnd(12)} ${r.country.padEnd(4)} ${r.report_type.padEnd(16)} ${r.status.padEnd(11)} doc=${r.has_doc?'Y':'N'} cache=${r.has_cache?'Y':'N'} ${r.error ? 'ERR='+r.error : ''}`);
        }

        const bad = rows.filter(r => r.status !== 'PROCESSED');
        console.log(`\n${bad.length === 0 ? 'All jobs PROCESSED.' : bad.length + ' job(s) NOT processed (flagged >> above).'}`);
    } finally {
        conn.release();
        await closePool();
    }
})().catch(err => { console.error(err); process.exit(1); });
