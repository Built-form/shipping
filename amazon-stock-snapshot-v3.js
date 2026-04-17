'use strict';

const axios = require('axios');
require('dotenv').config();
const log = require('./logger');
const { getPool } = require('./db');

// ── Accounts ────────────────────────────────────────────────────────────────────
const ACCOUNTS = {
    JFA: {
        clientId: process.env.AMAZON_SP_CLIENT_ID,
        clientSecret: process.env.AMAZON_SP_CLIENT_SECRET,
        refreshToken: process.env.AMAZON_SP_REFRESH_TOKEN,
    },
    Hangerworld: {
        clientId: process.env.AMAZON_SP_CLIENT_ID_HW,
        clientSecret: process.env.AMAZON_SP_CLIENT_SECRET_HW,
        refreshToken: process.env.AMAZON_SP_REFRESH_TOKEN_HW,
    },
};

// ── Regions ─────────────────────────────────────────────────────────────────────
const REGIONS = {
    EU: {
        endpoint: 'https://sellingpartnerapi-eu.amazon.com',
        marketplaces: {
            DE: 'A1PA6795UKMFR9', UK: 'A1F83G8C2ARO7P',
            FR: 'A13V1IB3VIYZZH', ES: 'A1RKKUPIHCS9HS', IT: 'APJ6JRA9NG5V4',
        },
    },
};

const COUNTRY_CODES = Object.values(REGIONS).flatMap(r => Object.keys(r.marketplaces));
const ACCOUNT_NAMES = Object.keys(ACCOUNTS);

function getMarketplace(countryCode) {
    for (const [, region] of Object.entries(REGIONS)) {
        if (region.marketplaces[countryCode]) {
            return { endpoint: region.endpoint, marketplaceId: region.marketplaces[countryCode] };
        }
    }
    throw new Error(`Unknown country code: ${countryCode}`);
}

// ── SP-API token manager ────────────────────────────────────────────────────────
class SPTokenManager {
    constructor(clientId, clientSecret, refreshToken) {
        this._clientId = clientId;
        this._clientSecret = clientSecret;
        this._refreshToken = refreshToken;
        this._token = null;
        this._fetchedAt = 0;
    }
    async getToken() {
        if (this._token && (Date.now() - this._fetchedAt) < 45 * 60 * 1000) return this._token;
        log.info('[SP] Refreshing access token...');
        const res = await axios.post('https://api.amazon.com/auth/o2/token', {
            grant_type: 'refresh_token', refresh_token: this._refreshToken,
            client_id: this._clientId, client_secret: this._clientSecret,
        });
        this._token = res.data.access_token;
        this._fetchedAt = Date.now();
        return this._token;
    }
}

// ── Retry & Rate Limit helpers ─────────────────────────────────────────────────
async function withRetry(fn, label) {
    for (let attempt = 1; attempt <= 5; attempt++) {
        try { return await fn(); } catch (err) {
            if (attempt === 5) throw err;
            const is429 = err.response?.status === 429;
            const retryAfter = parseInt(err.response?.headers?.['retry-after'], 10);
            const delay = is429
                ? (retryAfter || 15) * 1000 * attempt
                : 5 * 1000 * attempt;
            log.warn(`[${label}] Attempt ${attempt}/5 failed: ${err.message} — retrying in ${delay / 1000}s...`);
            await new Promise(r => setTimeout(r, delay));
        }
    }
}

const rateLimitQueues = {};
function rateLimitedRequest(fn, accountName) {
    if (!rateLimitQueues[accountName]) rateLimitQueues[accountName] = Promise.resolve();
    rateLimitQueues[accountName] = rateLimitQueues[accountName].then(() =>
        new Promise(r => setTimeout(r, 2000))
    ).then(() => fn());
    return rateLimitQueues[accountName];
}

// ── Reports API: request, poll, download ────────────────────────────────────────
async function requestReport(tokenManager, endpoint, reportType, marketplaceIds) {
    const token = await tokenManager.getToken();
    const res = await withRetry(() => axios.post(`${endpoint}/reports/2021-06-30/reports`, {
        reportType,
        marketplaceIds,
    }, {
        headers: { 'x-amz-access-token': token, 'Content-Type': 'application/json' },
    }), `requestReport ${reportType}`);
    return res.data.reportId;
}

async function pollReport(tokenManager, endpoint, reportId, label, maxWaitMs = 300000) {
    const start = Date.now();
    while (Date.now() - start < maxWaitMs) {
        const token = await tokenManager.getToken();
        const res = await axios.get(`${endpoint}/reports/2021-06-30/reports/${reportId}`, {
            headers: { 'x-amz-access-token': token },
        });
        const status = res.data.processingStatus;
        if (status === 'DONE') return res.data.reportDocumentId;
        if (status === 'CANCELLED' || status === 'FATAL') {
            throw new Error(`Report ${reportId} ended with status ${status}`);
        }
        await new Promise(r => setTimeout(r, 10000));
    }
    throw new Error(`Report ${reportId} timed out`);
}

async function downloadReport(tokenManager, endpoint, reportDocumentId) {
    const token = await tokenManager.getToken();
    const res = await axios.get(`${endpoint}/reports/2021-06-30/documents/${reportDocumentId}`, {
        headers: { 'x-amz-access-token': token },
    });
    const docUrl = res.data.url;
    const doc = await axios.get(docUrl, { responseType: 'text' });
    return doc.data;
}

function parseTsvReport(tsv) {
    const lines = tsv.trim().split('\n');
    if (lines.length < 2) return [];
    const headers = lines[0].split('\t').map(h => h.trim());
    return lines.slice(1).map(line => {
        const vals = line.split('\t');
        const row = {};
        headers.forEach((h, i) => { row[h] = (vals[i] || '').trim(); });
        return row;
    });
}

// ── Core Fetchers ──────────────────────────────────────────────────────────────
async function fetchInventoryHealthReport(tokenManager, endpoint, marketplaceId, label) {
    const reportId = await requestReport(tokenManager, endpoint, 'GET_FBA_INVENTORY_PLANNING_DATA', [marketplaceId]);
    const docId = await pollReport(tokenManager, endpoint, reportId, label);
    const tsv = await downloadReport(tokenManager, endpoint, docId);
    return parseTsvReport(tsv);
}

async function fetchInventorySummaries(tokenManager, endpoint, marketplaceId, label, accountName) {
    const summaries = [];
    let nextToken = null;
    do {
        const token = await tokenManager.getToken();
        const params = {
            details: true,
            granularityType: 'Marketplace',
            granularityId: marketplaceId,
            marketplaceIds: marketplaceId,
        };
        if (nextToken) params.nextToken = nextToken;

        const res = await withRetry(() => rateLimitedRequest(() =>
            axios.get(`${endpoint}/fba/inventory/v1/summaries`, {
                params,
                headers: { 'x-amz-access-token': token },
            })
        , accountName), `${label} API summaries`);

        const payload = res.data.payload || res.data;
        if (payload.inventorySummaries) {
            summaries.push(...payload.inventorySummaries);
        }
        nextToken = res.data.pagination?.nextToken || payload.pagination?.nextToken || null;
    } while (nextToken);
    return summaries;
}

// ── Single Job: one account + one country ──────────────────────────────────────
async function runJob(accountName, countryCode) {
    const account = ACCOUNTS[accountName];
    if (!account) throw new Error(`Unknown account: ${accountName}`);
    if (!account.clientId || !account.clientSecret || !account.refreshToken) {
        log.warn(`[${accountName}] SP-API credentials not configured — skipping`);
        return { marketplace: `${accountName}/${countryCode}`, asins: 0, skipped: true };
    }

    const conn = await getPool().getConnection();
    try {
    const { endpoint, marketplaceId } = getMarketplace(countryCode);
    const label = `${accountName}/${countryCode}`;
    const dateRan = new Date().toISOString().slice(0, 10);
    const tokenManager = new SPTokenManager(account.clientId, account.clientSecret, account.refreshToken);

    log.info(`[${label}] Fetching Health Report...`);
    const reportRows = await fetchInventoryHealthReport(tokenManager, endpoint, marketplaceId, label);
    log.info(`[${label}] Fetching API summaries...`);
    const apiSummaries = await fetchInventorySummaries(tokenManager, endpoint, marketplaceId, label, accountName);

    const byAsin = {};

    // 1. Process the Health Report (Physical Splits & Active Stock)
    for (const row of reportRows) {
        const asin = row['asin'] || '';
        if (!asin) continue;

        const fnsku = (row['fnsku'] || '').trim();
        const sku = row['sku'] || '';
        const isBprefix = fnsku.toUpperCase().startsWith('B');

        if (isBprefix && countryCode !== 'DE' && countryCode !== 'UK') continue;

        if (!byAsin[asin]) {
            byAsin[asin] = {
                seen_fnskus: new Set(),
                seen_skus: new Set(),
                fnsku: '', sku: '',
                condition_type: row['condition'] || 'New',
                fulfillable: 0, inbound_working: 0,
                inbound_shipped: 0, inbound_receiving: 0, reserved: 0,
            };
        }

        const a = byAsin[asin];
        if (!a.seen_fnskus.has(fnsku)) {
            a.seen_fnskus.add(fnsku);
            a.fnsku = a.fnsku ? a.fnsku + ',' + fnsku : fnsku;
        }
        if (!a.seen_skus.has(sku)) {
            a.seen_skus.add(sku);
            a.sku = a.sku ? a.sku + ',' + sku : sku;
        }

        if (!a.condition_type && row['condition']) a.condition_type = row['condition'];

        a.fulfillable       += parseInt(row['available'] || '0', 10);
        a.inbound_working   += parseInt(row['inbound-working'] || '0', 10);
        a.inbound_shipped   += parseInt(row['inbound-shipped'] || '0', 10);
        a.inbound_receiving += parseInt(row['inbound-received'] || '0', 10);
        a.reserved          += parseInt(row['Total Reserved Quantity'] || '0', 10);
    }

    // 2. Process the API Fallback (Catch 0-stock items with Inbound pipelines)
    let recoveredCount = 0;
    for (const item of apiSummaries) {
        const asin = item.asin || '';
        if (!asin) continue;
        if (byAsin[asin]) continue;

        const fnsku = (item.fnSku || '').trim();
        const sku = item.sellerSku || '';
        const details = item.inventoryDetails || {};

        const effectiveFnsku = fnsku || asin;
        const isBprefix = effectiveFnsku.toUpperCase().startsWith('B');
        if (isBprefix && countryCode !== 'DE' && countryCode !== 'UK') continue;

        byAsin[asin] = {
            seen_fnskus: new Set([fnsku]),
            fnsku, sku,
            condition_type: item.condition || 'New',
            fulfillable: 0, reserved: 0,
            inbound_working: details.inboundWorkingQuantity || 0,
            inbound_shipped: details.inboundShippedQuantity || 0,
            inbound_receiving: details.inboundReceivingQuantity || 0,
        };
        recoveredCount++;
    }

    // 3. Upsert to aggregated table
    let upsertCount = 0;
    for (const [asin, a] of Object.entries(byAsin)) {
        await conn.execute(
            `INSERT INTO amazon_stock_country_snapshots
                (date_ran, country, asin, company, fnsku, sku, condition_type,
                 fulfillable, inbound_working, inbound_shipped, inbound_receiving, reserved)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON DUPLICATE KEY UPDATE
                 fnsku = VALUES(fnsku), sku = VALUES(sku),
                 condition_type = VALUES(condition_type),
                 fulfillable = VALUES(fulfillable),
                 inbound_working = VALUES(inbound_working),
                 inbound_shipped = VALUES(inbound_shipped),
                 inbound_receiving = VALUES(inbound_receiving),
                 reserved = VALUES(reserved)`,
            [dateRan, countryCode, asin, accountName, a.fnsku, a.sku, a.condition_type,
             a.fulfillable, a.inbound_working, a.inbound_shipped, a.inbound_receiving, a.reserved]
        );
        upsertCount++;
    }

    // 4. Upsert to raw table (no pan-EU filtering, sum fulfillable per asin per country)
    const rawByAsin = {};
    for (const row of reportRows) {
        const asin = row['asin'] || '';
        if (!asin) continue;
        if (!rawByAsin[asin]) rawByAsin[asin] = 0;
        rawByAsin[asin] += parseInt(row['available'] || '0', 10);
    }
    for (const item of apiSummaries) {
        const asin = item.asin || '';
        if (!asin || rawByAsin[asin] !== undefined) continue;
        rawByAsin[asin] = item.inventoryDetails?.fulfillableQuantity || 0;
    }

    let rawCount = 0;
    for (const [asin, fulfillable] of Object.entries(rawByAsin)) {
        await conn.execute(
            `INSERT INTO amazon_stock_raw_snapshots
                (date_ran, country, asin, company, fulfillable)
             VALUES (?, ?, ?, ?, ?)
             ON DUPLICATE KEY UPDATE
                 fulfillable = VALUES(fulfillable)`,
            [dateRan, countryCode, asin, accountName, fulfillable]
        );
        rawCount++;
    }

    log.info(`[${label}] ${upsertCount} ASINs (recovered ${recoveredCount} missing inbound pipelines), ${rawCount} raw rows`);
    return { marketplace: label, asins: upsertCount };
    } finally {
        conn.release();
    }
}

// ── Backfill & Fallback ────────────────────────────────────────────────────────
async function runBackfill(conn) {
    const dateRan = new Date().toISOString().slice(0, 10);
    let totalBackfilled = 0;
    for (let i = 0; i < 100; i++) {
        const [result] = await conn.execute(
            `INSERT INTO amazon_stock_country_snapshots
             (date_ran, country, asin, company, fnsku, sku, condition_type,
              fulfillable, inbound_working, inbound_shipped, inbound_receiving, reserved)
             SELECT DATE_ADD(s.date_ran, INTERVAL 1 DAY),
                    s.country, s.asin, s.company, s.fnsku, s.sku, s.condition_type,
                    0, s.inbound_working, s.inbound_shipped, s.inbound_receiving, 0
             FROM amazon_stock_country_snapshots s
             LEFT JOIN amazon_stock_country_snapshots nxt
                 ON nxt.date_ran = DATE_ADD(s.date_ran, INTERVAL 1 DAY)
                 AND nxt.country = s.country AND nxt.asin = s.asin
                 AND nxt.company = s.company
             WHERE nxt.asin IS NULL
               AND DATE_ADD(s.date_ran, INTERVAL 1 DAY) <= ?
             ON DUPLICATE KEY UPDATE
                fulfillable = 0,
                inbound_working = VALUES(inbound_working),
                inbound_shipped = VALUES(inbound_shipped),
                inbound_receiving = VALUES(inbound_receiving),
                reserved = 0`,
            [dateRan]
        );
        if (result.affectedRows === 0) break;
        totalBackfilled += result.affectedRows;
    }
    if (totalBackfilled > 0) {
        log.info(`[Backfill] Carried forward ${totalBackfilled} rows filling gaps up to ${dateRan}`);
    }
}

async function copyFailedFromPrevious(conn, countryCode, accountName) {
    const dateRan = new Date().toISOString().slice(0, 10);
    const [lastDate] = await conn.query(
        `SELECT MAX(date_ran) as last_date FROM amazon_stock_country_snapshots WHERE date_ran < ? AND country = ? AND company = ?`,
        [dateRan, countryCode, accountName]
    );
    if (!lastDate[0]?.last_date) return;
    const prevDate = lastDate[0].last_date;
    log.info(`[${accountName}/${countryCode}] Failed — copying data from ${prevDate}`);
    await conn.execute(
        `INSERT INTO amazon_stock_country_snapshots
         (date_ran, country, asin, company, fnsku, sku, condition_type,
          fulfillable, inbound_working, inbound_shipped, inbound_receiving, reserved)
         SELECT ?, country, asin, company, fnsku, sku, condition_type,
                fulfillable, inbound_working, inbound_shipped, inbound_receiving, reserved
         FROM amazon_stock_country_snapshots WHERE date_ran = ? AND country = ? AND company = ?
         ON DUPLICATE KEY UPDATE
            fnsku = VALUES(fnsku), sku = VALUES(sku),
            condition_type = VALUES(condition_type),
            fulfillable = VALUES(fulfillable),
            inbound_working = VALUES(inbound_working),
            inbound_shipped = VALUES(inbound_shipped),
            inbound_receiving = VALUES(inbound_receiving),
            reserved = VALUES(reserved)`,
        [dateRan, prevDate, countryCode, accountName]
    );
}

// ── Lambda handler (single entry point, routed by event) ───────────────────────
// Event shape: { account: "JFA", country: "DE" }
//   — runs that single job
// Event shape: { job: "backfill" }
//   — runs backfill only
// Event shape: {} or scheduled (no account/country)
//   — runs all 10 jobs (companies parallel, regions sequential) + backfill
const handler = async (event = {}) => {
    const account = event.account;
    const country = event.country?.toUpperCase();

    // Single job mode
    if (account && country) {
        try {
            const result = await runJob(account, country);
            return { statusCode: 200, body: JSON.stringify(result) };
        } catch (err) {
            log.error(`[${account}/${country}] Failed: ${err.message}`);
            const conn = await getPool().getConnection();
            try { await copyFailedFromPrevious(conn, country, account); } finally { conn.release(); }
            return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
        }
    }

    // Backfill-only mode
    if (event.job === 'backfill') {
        const conn = await getPool().getConnection();
        try {
            await runBackfill(conn);
            return { statusCode: 200, body: 'backfill complete' };
        } finally {
            conn.release();
        }
    }

    // All jobs mode: companies parallel, regions sequential
    const results = { success: [], failed: [] };

    async function processAccount(accountName) {
        for (const countryCode of COUNTRY_CODES) {
            try {
                const result = await runJob(accountName, countryCode);
                if (!result.skipped) results.success.push(result);
            } catch (err) {
                log.error(`[${accountName}/${countryCode}] Failed: ${err.message}`);
                results.failed.push({ marketplace: `${accountName}/${countryCode}`, error: err.message });
            }
        }
    }

    await Promise.all(ACCOUNT_NAMES.map(name => processAccount(name)));

    const conn = await getPool().getConnection();
    try {
        for (const f of results.failed) {
            const [a, c] = f.marketplace.split('/');
            if (a && c) await copyFailedFromPrevious(conn, c, a);
        }
        await runBackfill(conn);
    } finally {
        conn.release();
    }

    log.info(`Done — ${results.success.length} OK, ${results.failed.length} failed.`);
    return { statusCode: 200, body: JSON.stringify(results) };
};

exports.handler = handler;
exports.REGIONS = REGIONS;

// ── CLI ────────────────────────────────────────────────────────────────────────
// node amazon-stock-snapshot-v3.js                → all jobs
// node amazon-stock-snapshot-v3.js JFA DE         → single job
// node amazon-stock-snapshot-v3.js backfill       → backfill only
if (require.main === module) {
    const [,, arg1, arg2] = process.argv;

    let event = {};
    if (arg1 && arg2) {
        event = { account: arg1, country: arg2 };
    } else if (arg1 === 'backfill') {
        event = { job: 'backfill' };
    }

    handler(event).then(r => {
        log.info('Result:', JSON.stringify(r, null, 2));
        process.exit(0);
    }).catch(err => {
        log.error('Fatal:', err.message, err.stack);
        process.exit(1);
    });
}
