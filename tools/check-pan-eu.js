'use strict';

// One-shot: for a given ASIN, show whether it's in today's pan_eu cache
// per account, and what (country, company) rows exist in
// amazon_stock_country_snapshots on the latest date. Also shows whether
// an active listing exists per country. Helps answer "why is IT missing?"
//
// Usage: node tools/check-pan-eu.js B095FXJ2BB

require('dotenv').config();
const { getPool, closePool } = require('../src/db');

const ASIN = (process.argv[2] || '').trim().toUpperCase();
if (!ASIN) { console.error('usage: node tools/check-pan-eu.js <ASIN>'); process.exit(1); }

(async () => {
    const pool = getPool();
    const conn = await pool.getConnection();
    try {
        const [jobs] = await conn.query(
            `SELECT batch_date, account, status, result_cache IS NOT NULL AS has_cache,
                    CHAR_LENGTH(result_cache) AS cache_len
             FROM amazon_report_jobs
             WHERE country = 'ALL' AND report_type = 'pan_eu'
             ORDER BY batch_date DESC, account
             LIMIT 20`
        );
        console.log(`\n== pan_eu jobs (most recent 20) ==`);
        for (const j of jobs) {
            console.log(`  ${String(j.batch_date).slice(0,10)}  ${j.account.padEnd(12)} status=${j.status.padEnd(10)} has_cache=${!!j.has_cache} len=${j.cache_len}`);
        }

        console.log(`\n== Is ${ASIN} in today's pan_eu cache? ==`);
        const [cacheRows] = await conn.query(
            `SELECT batch_date, account, result_cache
             FROM amazon_report_jobs
             WHERE country = 'ALL' AND report_type = 'pan_eu'
               AND batch_date = (SELECT MAX(batch_date) FROM amazon_report_jobs
                                 WHERE country = 'ALL' AND report_type = 'pan_eu')
               AND result_cache IS NOT NULL`
        );
        for (const r of cacheRows) {
            let list;
            try { list = JSON.parse(r.result_cache) || []; } catch { list = []; }
            const set = new Set(list);
            console.log(`  ${String(r.batch_date).slice(0,10)}  ${r.account.padEnd(12)} pan_eu_count=${set.size} contains_${ASIN}=${set.has(ASIN)}`);
        }

        console.log(`\n== amazon_stock_country_snapshots: latest date per (country, company) for ${ASIN} ==`);
        const [snapRows] = await conn.query(
            `SELECT country, company, MAX(date_ran) AS latest,
                    SUM(fulfillable) AS fulfillable,
                    SUM(reserved) AS reserved
             FROM amazon_stock_country_snapshots
             WHERE asin = ?
               AND date_ran = (SELECT MAX(date_ran) FROM amazon_stock_country_snapshots
                               WHERE asin = ?)
             GROUP BY country, company
             ORDER BY company, country`,
            [ASIN, ASIN]
        );
        if (snapRows.length === 0) console.log('  (no rows)');
        for (const r of snapRows) {
            console.log(`  ${r.country.padEnd(4)} ${r.company.padEnd(12)} ${String(r.latest).slice(0,10)}  fulfillable=${r.fulfillable} reserved=${r.reserved}`);
        }

        console.log(`\n== amazon_active_listings: latest date per country for ${ASIN} ==`);
        const [alRows] = await conn.query(
            `SELECT country, company, MAX(date_ran) AS latest, COUNT(*) AS rows_on_latest
             FROM amazon_active_listings a
             WHERE asin = ?
               AND date_ran = (SELECT MAX(date_ran) FROM amazon_active_listings
                               WHERE asin = a.asin AND country = a.country AND company = a.company)
             GROUP BY country, company
             ORDER BY company, country`,
            [ASIN]
        );
        if (alRows.length === 0) console.log('  (no rows)');
        for (const r of alRows) {
            console.log(`  ${r.country.padEnd(4)} ${r.company.padEnd(12)} ${String(r.latest).slice(0,10)}  rows=${r.rows_on_latest}`);
        }

        console.log(`\n== amazon_stock_raw_snapshots: latest date per country for ${ASIN} ==`);
        const [rawRows] = await conn.query(
            `SELECT country, company, MAX(date_ran) AS latest, SUM(fulfillable) AS fulfillable
             FROM amazon_stock_raw_snapshots
             WHERE asin = ?
               AND date_ran = (SELECT MAX(date_ran) FROM amazon_stock_raw_snapshots
                               WHERE asin = ?)
             GROUP BY country, company
             ORDER BY company, country`,
            [ASIN, ASIN]
        );
        if (rawRows.length === 0) console.log('  (no rows)');
        for (const r of rawRows) {
            console.log(`  ${r.country.padEnd(4)} ${r.company.padEnd(12)} ${String(r.latest).slice(0,10)}  fulfillable=${r.fulfillable}`);
        }
    } finally {
        conn.release();
        await closePool();
    }
})().catch(err => {
    console.error('Fatal:', err.message);
    process.exit(1);
});
