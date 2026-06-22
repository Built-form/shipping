'use strict';
// Re-runs today's collector for ONE account against the already-downloaded
// report documents (no new Amazon report requests). Resets today's jobs back
// to a processable state, re-derives the pan_eu cache with the current filter,
// and loops the collector handler until every (account,country) unit is
// reprocessed with the current code. Overwrites today's snapshot rows only.
//   node tools/rerun-today.js JFA
require('dotenv').config();
const { getPool, closePool } = require('../src/db');
const { handler } = require('../src/handlers/amazon-collector');

const account = process.argv[2];
if (!account) { console.error('usage: node tools/rerun-today.js <account>'); process.exit(1); }

(async () => {
    const pool = getPool();
    {
        const conn = await pool.getConnection();
        try {
            const [r1] = await conn.query(
                `UPDATE amazon_report_jobs
                 SET status='DONE', result_cache=NULL, processed_at=NULL, claim_token=NULL, claimed_at=NULL
                 WHERE batch_date=CURDATE() AND account=? AND report_type='pan_eu'`, [account]);
            const [r2] = await conn.query(
                `UPDATE amazon_report_jobs
                 SET status='DONE', processed_at=NULL, claim_token=NULL, claimed_at=NULL
                 WHERE batch_date=CURDATE() AND account=? AND report_type IN ('active_listings','health')
                   AND status IN ('PROCESSED','DONE')`, [account]);
            console.log(`reset ${account}: pan_eu=${r1.affectedRows}, active/health=${r2.affectedRows}`);
        } finally { conn.release(); }
    }

    let idle = 0;
    for (let i = 0; i < 40 && idle < 3; i++) {
        const res = await handler({});
        const proc = JSON.parse(res.body).processed;
        console.log(`run ${String(i + 1).padStart(2)}: ${proc ? JSON.stringify(proc).slice(0, 150) : 'no unit claimed'}`);
        if (!proc) idle++; else idle = 0;
    }
    await closePool();
    console.log('\ndone');
})().catch(e => { console.error(e); process.exit(1); });
