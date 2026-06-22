'use strict';

// Quick operational status: how fresh is each scheduled job's output,
// and what's in the amazon_report_jobs queue today.
//
// Usage: node tools/status.js

require('dotenv').config();
const { getPool, closePool } = require('../src/db');

(async () => {
    const pool = getPool();
    const conn = await pool.getConnection();
    try {
        console.log(`== Data freshness (server time: ${new Date().toISOString()}) ==\n`);

        // Orders (importAsanaOrders, every 5 min → INSERT)
        const [orders] = await conn.query(
            `SELECT COUNT(*) AS total, MAX(created_at) AS latest,
                    TIMESTAMPDIFF(MINUTE, MAX(created_at), NOW()) AS mins_ago
             FROM orders`
        );
        console.log(`orders                        rows=${orders[0].total}  last_insert=${orders[0].latest?.toISOString?.() || orders[0].latest}  (${orders[0].mins_ago}m ago)`);

        // Mintsoft stock_snapshots (every 1 hour)
        const [ms] = await conn.query(
            `SELECT MAX(date_ran) AS latest_date, COUNT(DISTINCT asin) AS asins_today
             FROM stock_snapshots WHERE date_ran = CURDATE()`
        );
        const [msLatest] = await conn.query(
            `SELECT MAX(created_at) AS latest_created,
                    TIMESTAMPDIFF(MINUTE, MAX(created_at), NOW()) AS mins_ago
             FROM stock_snapshots`
        );
        console.log(`stock_snapshots (Mintsoft)    latest_date=${ms[0].latest_date}  asins_today=${ms[0].asins_today}  last_write=${msLatest[0].mins_ago}m ago`);

        // Amazon country snapshots per (country, company)
        console.log(`\n== amazon_stock_country_snapshots — latest per (country, company) ==`);
        const [amzLatest] = await conn.query(
            `SELECT country, company, MAX(date_ran) AS latest_date,
                    COUNT(DISTINCT asin) AS asins
             FROM amazon_stock_country_snapshots
             WHERE date_ran = (SELECT MAX(date_ran) FROM amazon_stock_country_snapshots
                               WHERE country = amazon_stock_country_snapshots.country
                                 AND company = amazon_stock_country_snapshots.company)
             GROUP BY country, company
             ORDER BY company, country`
        );
        for (const r of amzLatest) {
            const today = new Date().toISOString().slice(0,10);
            const flag = String(r.latest_date).slice(0,10) === today ? '✅' : '⚠️ stale';
            console.log(`  ${r.country.padEnd(4)} ${r.company.padEnd(12)} latest=${String(r.latest_date).slice(0,10)}  asins=${r.asins}  ${flag}`);
        }

        // amazon_report_jobs today
        console.log(`\n== amazon_report_jobs — today (${new Date().toISOString().slice(0,10)}) ==`);
        const [jobs] = await conn.query(
            `SELECT account, country, report_type, status, COUNT(*) AS n,
                    SUM(document_id IS NOT NULL) AS has_doc
             FROM amazon_report_jobs
             WHERE batch_date = CURDATE()
             GROUP BY account, country, report_type, status
             ORDER BY account, country, report_type`
        );
        if (jobs.length === 0) console.log('  (no jobs today)');
        for (const j of jobs) {
            console.log(`  ${j.account.padEnd(12)} ${j.country.padEnd(4)} ${j.report_type.padEnd(16)} ${j.status.padEnd(10)} n=${j.n}  has_doc=${j.has_doc}`);
        }

        // Summary: units still outstanding (not PROCESSED) per account
        const [outstanding] = await conn.query(
            `SELECT account, status, COUNT(*) AS n
             FROM amazon_report_jobs
             WHERE batch_date = CURDATE() AND status NOT IN ('PROCESSED','FAILED')
             GROUP BY account, status
             ORDER BY account, status`
        );
        console.log(`\n== Outstanding (not PROCESSED/FAILED) today ==`);
        if (outstanding.length === 0) console.log('  ✅ all done');
        for (const r of outstanding) {
            console.log(`  ${r.account.padEnd(12)} ${r.status.padEnd(10)} n=${r.n}`);
        }

        // active_listings freshness
        console.log(`\n== amazon_active_listings — latest per (country, company) ==`);
        const [alLatest] = await conn.query(
            `SELECT country, company, MAX(date_ran) AS latest_date,
                    COUNT(DISTINCT asin) AS asins
             FROM amazon_active_listings
             GROUP BY country, company
             ORDER BY company, country`
        );
        for (const r of alLatest) {
            console.log(`  ${r.country.padEnd(4)} ${r.company.padEnd(12)} latest=${String(r.latest_date).slice(0,10)}  asins=${r.asins}`);
        }
    } finally {
        conn.release();
        await closePool();
    }
})().catch(err => {
    console.error('Fatal:', err.message);
    process.exit(1);
});
