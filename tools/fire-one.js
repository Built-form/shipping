'use strict';
// Re-fire a single (account, country, report_type) report request.
// Resets today's job row to REQUESTED and clears its document_id, so the
// next collector poll will pick it up, mark it DONE, and the next reprocess
// will see fresh data.
//
// Usage:
//   node tools/fire-one.js JFA FR active_listings
//   node tools/fire-one.js JFA DE health

require('dotenv').config();
const log = require('../src/lib/logger');
const { getPool } = require('../src/db');
const {
    SP_REPORT_TYPE,
    getMarketplace, getEndpointForCountry, getRegionForCountry, getTokenManager,
    requestReport, rateLimitedRequest,
} = require('../src/services/amazon-stock-shared');

const account = process.argv[2] || 'JFA';
const country = process.argv[3] || 'FR';
const reportType = process.argv[4] || 'active_listings';

if (!SP_REPORT_TYPE[reportType]) {
    console.error(`Unknown report type: ${reportType}. Valid: ${Object.keys(SP_REPORT_TYPE).join(', ')}`);
    process.exit(1);
}

(async () => {
    const batchDate = new Date().toISOString().slice(0, 10);
    const pool = getPool();
    const tokenManager = getTokenManager(account, getRegionForCountry(country));
    const endpoint = getEndpointForCountry(country);
    const { marketplaceId } = getMarketplace(country);
    const label = `${account}/${country}/${reportType}`;

    let reportId = null, error = null;
    try {
        reportId = await rateLimitedRequest(
            () => requestReport(tokenManager, endpoint, SP_REPORT_TYPE[reportType], [marketplaceId]),
            account
        );
    } catch (err) {
        log.error(`[${label}] Request failed: ${err.message}`);
        error = err.message;
    }

    const conn = await pool.getConnection();
    try {
        await conn.execute(
            `INSERT INTO amazon_report_jobs
                (batch_date, account, country, report_type, marketplace_id, report_id, status, error)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)
             ON DUPLICATE KEY UPDATE
                report_id = VALUES(report_id),
                marketplace_id = VALUES(marketplace_id),
                status = VALUES(status),
                error = VALUES(error),
                requested_at = CURRENT_TIMESTAMP,
                polled_at = NULL, completed_at = NULL, processed_at = NULL,
                document_id = NULL, result_cache = NULL,
                claim_token = NULL, claimed_at = NULL`,
            [batchDate, account, country, reportType, marketplaceId, reportId,
             error ? 'FAILED' : 'REQUESTED', error || null]
        );
    } finally { conn.release(); }

    if (reportId) {
        log.info(`[${label}] Re-requested ${reportId}. Wait for the next collector poll to mark it DONE, then run:`);
        log.info(`  node tools/reprocess-country.js ${country} ${account}`);
    }
    process.exit(0);
})().catch(err => { console.error(err); process.exit(1); });
