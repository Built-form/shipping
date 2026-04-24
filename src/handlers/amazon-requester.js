'use strict';

require('dotenv').config();
const log = require('../lib/logger');
const { getPool } = require('../db');
const {
    ACCOUNT_NAMES, COUNTRY_CODES, REPORT_TYPES, SP_REPORT_TYPE,
    getMarketplace, getEndpoint, getTokenManager, requestReport,
    rateLimitedRequest,
} = require('../services/amazon-stock-shared');

// Fires one SP-API POST /reports request per (account, country, report_type),
// plus one pan_eu per account. Inserts each into amazon_report_jobs.
//
// Idempotent: rows already REQUESTED/DONE/PROCESSED for today's batch are
// skipped. Only missing rows and previously-FAILED rows are re-fired. Meant
// to be scheduled hourly so each run picks up whatever's still outstanding.

async function loadExistingJobs(conn, batchDate) {
    const [rows] = await conn.query(
        `SELECT account, country, report_type, status
         FROM amazon_report_jobs WHERE batch_date = ?`,
        [batchDate]
    );
    const map = new Map();
    for (const r of rows) {
        map.set(`${r.account}|${r.country}|${r.report_type}`, r.status);
    }
    return map;
}

function shouldSkip(existing, account, country, reportType) {
    const status = existing.get(`${account}|${country}|${reportType}`);
    return status === 'REQUESTED' || status === 'DONE' || status === 'PROCESSED';
}

async function upsertJobRow(conn, {
    batchDate, account, country, reportType, marketplaceId, reportId, error,
}) {
    await conn.execute(
        `INSERT INTO amazon_report_jobs
            (batch_date, account, country, report_type, marketplace_id, report_id, status, error)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         ON DUPLICATE KEY UPDATE
            report_id     = VALUES(report_id),
            marketplace_id = VALUES(marketplace_id),
            status        = VALUES(status),
            error         = VALUES(error),
            requested_at  = CURRENT_TIMESTAMP,
            polled_at     = NULL,
            completed_at  = NULL,
            processed_at  = NULL,
            document_id   = NULL,
            result_cache  = NULL,
            claim_token   = NULL,
            claimed_at    = NULL`,
        [batchDate, account, country, reportType, marketplaceId, reportId,
         error ? 'FAILED' : 'REQUESTED', error || null]
    );
}

// Each fireOne does one long SP-API call (with retries) then one short DB
// upsert. The SP-API call can exceed MySQL's wait_timeout on a held
// connection, so we only acquire a DB connection around the upsert.
async function fireOne(pool, { batchDate, account, country, reportType, marketplaceId, tokenManager, endpoint }) {
    const label = `${account}/${country}/${reportType}`;
    let reportId = null;
    let error = null;
    try {
        reportId = await rateLimitedRequest(
            () => requestReport(tokenManager, endpoint, SP_REPORT_TYPE[reportType], [marketplaceId]),
            account
        );
    } catch (err) {
        log.error(`[${label}] Request failed: ${err.message}`);
        error = err.message;
    }

    await withConn(pool, (conn) => upsertJobRow(conn, {
        batchDate, account, country, reportType, marketplaceId,
        reportId, error,
    }));

    if (reportId) log.info(`[${label}] Requested ${reportId}`);
    return reportId ? 'success' : 'failed';
}

async function requestAccount(pool, accountName, batchDate, existing) {
    const tokenManager = getTokenManager(accountName);
    const endpoint = getEndpoint();
    const results = { success: 0, failed: 0, skipped: 0 };

    // ── Pan-EU (account-wide) ──────────────────────────────────────────────
    if (shouldSkip(existing, accountName, 'ALL', REPORT_TYPES.PAN_EU)) {
        results.skipped++;
    } else {
        const r = await fireOne(pool, {
            batchDate, account: accountName, country: 'ALL',
            reportType: REPORT_TYPES.PAN_EU,
            marketplaceId: 'A1PA6795UKMFR9',
            tokenManager, endpoint,
        });
        results[r]++;
    }

    // ── Active Listings + Health per country ───────────────────────────────
    for (const countryCode of COUNTRY_CODES) {
        const { marketplaceId } = getMarketplace(countryCode);

        for (const reportType of [REPORT_TYPES.ACTIVE_LISTINGS, REPORT_TYPES.HEALTH]) {
            if (shouldSkip(existing, accountName, countryCode, reportType)) {
                results.skipped++;
                continue;
            }
            const r = await fireOne(pool, {
                batchDate, account: accountName, country: countryCode,
                reportType, marketplaceId, tokenManager, endpoint,
            });
            results[r]++;
        }
    }

    return results;
}

async function withConn(pool, fn) {
    const conn = await pool.getConnection();
    try { return await fn(conn); }
    finally { conn.release(); }
}

const handler = async () => {
    const batchDate = new Date().toISOString().slice(0, 10);
    const pool = getPool();

    const existing = await withConn(pool, (conn) => loadExistingJobs(conn, batchDate));
    const totals = { success: 0, failed: 0, skipped: 0 };

    for (const accountName of ACCOUNT_NAMES) {
        try {
            const res = await requestAccount(pool, accountName, batchDate, existing);
            totals.success += res.success;
            totals.failed  += res.failed;
            totals.skipped += res.skipped;
        } catch (err) {
            log.error(`[${accountName}] Account-level failure: ${err.message}`);
        }
    }

    log.info(`[Requester] batch=${batchDate} — fired ${totals.success}, failed ${totals.failed}, skipped ${totals.skipped}`);
    return { statusCode: 200, body: JSON.stringify({ batchDate, ...totals }) };
};

exports.handler = handler;

// ── CLI ────────────────────────────────────────────────────────────────────────
// node src/handlers/amazon-requester.js
if (require.main === module) {
    handler().then(r => {
        log.info('Result:', r.body);
        process.exit(0);
    }).catch(err => {
        log.error('Fatal:', err.message, err.stack);
        process.exit(1);
    });
}
