'use strict';
// Validate the GET /api/v1/qc-reports?jfCode= filter logic against live data.
// Usage: node tools/test-qc-search.js JF0920
require('dotenv').config();
const { getPool, closePool } = require('../src/db');

const raw = process.argv[2] || 'JF0920';
const jfNorm = raw.toUpperCase().replace(/[\s\-_.]/g, '');

(async () => {
    const pool = getPool();
    const conn = await pool.getConnection();
    try {
        const [rows] = await conn.query(
            `SELECT r.id, r.report_title, r.supplier, r.matched_count, r.source
               FROM qc_reports r
              WHERE r.deleted_at IS NULL
                AND (
                  EXISTS (
                    SELECT 1 FROM order_qc_reports l
                     WHERE l.qc_report_id = r.id
                       AND UPPER(REPLACE(REPLACE(REPLACE(l.jf_code,' ',''),'-',''),'_','')) = ?
                  )
                  OR REPLACE(REPLACE(REPLACE(LOWER(CAST(r.result_json AS CHAR)),' ',''),'-',''),'_','')
                     LIKE CONCAT('%', ?, '%')
                )
              ORDER BY r.created_at DESC, r.id DESC`,
            [jfNorm, jfNorm.toLowerCase()]
        );
        console.log(`jfCode "${raw}" (norm ${jfNorm}) → ${rows.length} report(s):`);
        for (const r of rows) {
            console.log(`  #${r.id}  ${(r.source || 'manual').padEnd(16)}  ${(r.report_title || '').slice(0, 40).padEnd(41)} ${r.supplier || ''}`);
        }
    } finally { conn.release(); await closePool(); }
})().catch(e => { console.error(e.message); process.exit(1); });
