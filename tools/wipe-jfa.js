'use strict';
require('dotenv').config();
const { getPool, closePool } = require('../src/db');

(async () => {
    const conn = await getPool().getConnection();
    try {
        for (const tbl of [
            'amazon_stock_country_snapshots',
            'amazon_stock_raw_snapshots',
            'amazon_active_listings',
        ]) {
            const [r] = await conn.execute(
                `DELETE FROM ${tbl} WHERE company = 'JFA'`
            );
            console.log(`  ${tbl}: deleted ${r.affectedRows}`);
        }
        const [j] = await conn.execute(
            `DELETE FROM amazon_report_jobs WHERE account = 'JFA'`
        );
        console.log(`  amazon_report_jobs: deleted ${j.affectedRows}`);
    } finally {
        conn.release();
        await closePool();
    }
})().catch(err => { console.error(err); process.exit(1); });
