'use strict';
// One-off: fire today's amazon report requests for JFA only.

require('dotenv').config();
const log = require('../src/lib/logger');
const { getPool } = require('../src/db');

// Reach into the requester module's internals via require.cache miss → reload.
// We need requestAccount() but it's not exported. Easiest: copy the wiring.
const {
    getAccountCountries, REPORT_TYPES, SP_REPORT_TYPE,
    getMarketplace, getEndpoint, getTokenManager,
    requestReport, rateLimitedRequest,
} = require('../src/services/amazon-stock-shared');

async function upsertJobRow(conn, { batchDate, account, country, reportType, marketplaceId, reportId, error }) {
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
}

async function fireOne(pool, ctx) {
    const { batchDate, account, country, reportType, marketplaceId, tokenManager, endpoint } = ctx;
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
        await upsertJobRow(conn, {
            batchDate, account, country, reportType, marketplaceId, reportId, error,
        });
    } finally { conn.release(); }
    if (reportId) log.info(`[${label}] Requested ${reportId}`);
}

(async () => {
    const accountName = 'JFA';
    const batchDate = new Date().toISOString().slice(0, 10);
    const pool = getPool();
    const tokenManager = getTokenManager(accountName);
    const endpoint = getEndpoint();

    // pan_eu (account-wide)
    await fireOne(pool, {
        batchDate, account: accountName, country: 'ALL',
        reportType: REPORT_TYPES.PAN_EU,
        marketplaceId: 'A1PA6795UKMFR9',
        tokenManager, endpoint,
    });

    // active_listings + health per country
    for (const countryCode of getAccountCountries(accountName)) {
        const { marketplaceId } = getMarketplace(countryCode);
        for (const reportType of [REPORT_TYPES.ACTIVE_LISTINGS, REPORT_TYPES.HEALTH]) {
            await fireOne(pool, {
                batchDate, account: accountName, country: countryCode,
                reportType, marketplaceId, tokenManager, endpoint,
            });
        }
    }

    log.info(`[Requester JFA-only] batch=${batchDate} done`);
    process.exit(0);
})().catch(err => { console.error(err); process.exit(1); });
