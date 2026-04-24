'use strict';

// Re-run writeSnapshots for a single (account, country) using today's stored
// document_ids and pan_eu cache. Does not change job status. Prints enough
// detail to see why a country wrote 0 rows when it shouldn't have.
//
// Usage:
//   node reprocess-country.js DE JFA
//   node reprocess-country.js IT JFA

require('dotenv').config();
const { getPool } = require('./db');
const {
    REPORT_TYPES, getMarketplace, getTokenManager,
    downloadReport, parseTsvReport, fetchInventorySummaries,
    writeSnapshots,
} = require('./amazon-stock-shared');

const COUNTRY = process.argv[2] || 'DE';
const ACCOUNT = process.argv[3] || 'JFA';

(async () => {
    const pool = getPool();

    // Phase A: gather job + pan_eu cache
    const conn = await pool.getConnection();
    let active_doc, health_doc, marketplace_id, panEuAsins, dateRan;
    try {
        const [jobs] = await conn.query(
            `SELECT batch_date, report_type, document_id, marketplace_id, status
             FROM amazon_report_jobs
             WHERE batch_date = CURDATE() AND account = ? AND country = ?
               AND report_type IN ('active_listings','health')`,
            [ACCOUNT, COUNTRY]
        );
        const al = jobs.find(j => j.report_type === 'active_listings');
        const h  = jobs.find(j => j.report_type === 'health');
        if (!al || !h) { console.log('missing job'); process.exit(1); }
        if (!al.document_id || !h.document_id) {
            console.log(`missing doc: active=${al.document_id} health=${h.document_id}`);
            process.exit(1);
        }
        active_doc = al.document_id;
        health_doc = h.document_id;
        marketplace_id = al.marketplace_id || getMarketplace(COUNTRY).marketplaceId;
        dateRan = String(al.batch_date).slice(0, 10);

        const [peRows] = await conn.query(
            `SELECT status, result_cache FROM amazon_report_jobs
             WHERE batch_date = ? AND account = ? AND country = 'ALL' AND report_type = ?`,
            [al.batch_date, ACCOUNT, REPORT_TYPES.PAN_EU]
        );
        if (peRows.length === 0 || !peRows[0].result_cache) {
            console.log(`pan_eu missing or not cached`);
            process.exit(1);
        }
        panEuAsins = new Set(JSON.parse(peRows[0].result_cache) || []);

        console.log(`account=${ACCOUNT} country=${COUNTRY} dateRan=${dateRan}`);
        console.log(`active_doc=${active_doc}`);
        console.log(`health_doc=${health_doc}`);
        console.log(`marketplace_id=${marketplace_id}`);
        console.log(`panEuAsins=${panEuAsins.size}`);
    } finally {
        conn.release();
    }

    // Phase B: HTTP (no DB connection held)
    const tokenManager = getTokenManager(ACCOUNT);
    const endpoint = require('./amazon-stock-shared').getEndpoint();

    console.log(`\n[download] active listings...`);
    const listingTsv = await downloadReport(tokenManager, endpoint, active_doc);
    const listingRows = parseTsvReport(listingTsv);
    console.log(`  listingRows=${listingRows.length}`);

    console.log(`[download] health...`);
    const healthTsv = await downloadReport(tokenManager, endpoint, health_doc);
    const reportRows = parseTsvReport(healthTsv);
    console.log(`  reportRows=${reportRows.length}`);

    console.log(`[api] inventory summaries...`);
    const apiSummaries = await fetchInventorySummaries(
        tokenManager, endpoint, marketplace_id, `${ACCOUNT}/${COUNTRY}`, ACCOUNT
    );
    console.log(`  apiSummaries=${apiSummaries.length}`);

    // Dry-run preview of what would be written, before the real write
    const activeAsinsPreview = new Set();
    for (const row of listingRows) {
        const asin = (row['asin1'] || row['asin'] || '').trim();
        if (asin) activeAsinsPreview.add(asin);
    }
    let healthMatched = 0, healthPanEuFiltered = 0, healthKept = 0;
    for (const row of reportRows) {
        const asin = row['asin'] || '';
        if (!asin || !activeAsinsPreview.has(asin)) continue;
        healthMatched++;
        if (panEuAsins.has(asin) && COUNTRY !== 'DE' && COUNTRY !== 'UK') {
            healthPanEuFiltered++;
            continue;
        }
        healthKept++;
    }
    console.log(`\n[preview]`);
    console.log(`  activeAsins=${activeAsinsPreview.size}`);
    console.log(`  health rows matching active=${healthMatched}`);
    console.log(`  health rows filtered by pan_eu=${healthPanEuFiltered}`);
    console.log(`  health rows kept (would feed byAsin)=${healthKept}`);

    // Phase C: write with fresh connection
    const writeConn = await pool.getConnection();
    try {
        const before = await writeConn.query(
            `SELECT COUNT(*) AS c FROM amazon_stock_country_snapshots
             WHERE date_ran = ? AND country = ? AND company = ?`,
            [dateRan, COUNTRY, ACCOUNT]
        );
        console.log(`\n[before] country_snapshots rows for today=${before[0][0].c}`);

        const stats = await writeSnapshots(writeConn, {
            accountName: ACCOUNT, countryCode: COUNTRY, dateRan,
            listingRows, reportRows, apiSummaries, panEuAsins,
        });

        const after = await writeConn.query(
            `SELECT COUNT(*) AS c FROM amazon_stock_country_snapshots
             WHERE date_ran = ? AND country = ? AND company = ?`,
            [dateRan, COUNTRY, ACCOUNT]
        );
        console.log(`[after]  country_snapshots rows for today=${after[0][0].c}`);
        console.log(`[stats] ${JSON.stringify(stats)}`);
    } finally {
        writeConn.release();
        process.exit(0);
    }
})().catch(err => {
    console.error('Fatal:', err.message, err.stack);
    process.exit(1);
});
