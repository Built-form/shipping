'use strict';
// Quick read-only dump of current QC reports (to spot duplicates / origins).
require('dotenv').config();
const { getPool, closePool } = require('../src/db');
(async () => {
    const p = getPool();
    const c = await p.getConnection();
    try {
        const [r] = await c.query(
            `SELECT id, source, status, report_title, supplier, matched_count, item_count,
                    DATE_FORMAT(source_received_at, '%Y-%m-%d') email_date,
                    DATE_FORMAT(COALESCE(source_received_at, created_at), '%Y-%m-%d %H:%i') sort_key
               FROM qc_reports WHERE deleted_at IS NULL
              ORDER BY COALESCE(source_received_at, created_at) DESC, id DESC`
        );
        for (const x of r) {
            console.log([
                String(x.id).padStart(3),
                (x.email_date || 'no-email').padEnd(11),
                (x.source || 'manual').padEnd(16),
                (x.status || '').padEnd(10),
                (String(x.matched_count ?? '-') + 'm').padStart(4),
                (String(x.item_count ?? '-') + 'i').padStart(4),
                (x.supplier || '').slice(0, 20).padEnd(21),
                (x.report_title || '').slice(0, 34).padEnd(35),
            ].join(' '));
        }
        console.log('rows:', r.length);
    } finally { c.release(); await closePool(); }
})().catch(e => { console.error(e.message); process.exit(1); });
