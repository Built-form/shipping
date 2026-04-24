'use strict';

const axios = require('axios');
const log = require('./logger');

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

// Report type identifiers used in amazon_report_jobs.report_type
const REPORT_TYPES = {
    PAN_EU: 'pan_eu',
    ACTIVE_LISTINGS: 'active_listings',
    HEALTH: 'health',
};

// SP-API report type → SP-API string
const SP_REPORT_TYPE = {
    pan_eu: 'GET_PAN_EU_OFFER_STATUS',
    active_listings: 'GET_MERCHANT_LISTINGS_DATA',
    health: 'GET_FBA_INVENTORY_PLANNING_DATA',
};

function getMarketplace(countryCode) {
    for (const [, region] of Object.entries(REGIONS)) {
        if (region.marketplaces[countryCode]) {
            return { endpoint: region.endpoint, marketplaceId: region.marketplaces[countryCode] };
        }
    }
    throw new Error(`Unknown country code: ${countryCode}`);
}

function getEndpoint() {
    return REGIONS.EU.endpoint;
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

function getTokenManager(accountName) {
    const account = ACCOUNTS[accountName];
    if (!account) throw new Error(`Unknown account: ${accountName}`);
    if (!account.clientId || !account.clientSecret || !account.refreshToken) {
        throw new Error(`Account ${accountName} missing SP-API credentials`);
    }
    return new SPTokenManager(account.clientId, account.clientSecret, account.refreshToken);
}

// ── Retry & Rate Limit helpers ─────────────────────────────────────────────────
async function withRetry(fn, label, maxAttempts = 5) {
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        try { return await fn(); } catch (err) {
            if (attempt === maxAttempts) throw err;
            const is429 = err.response?.status === 429;
            const retryAfter = parseInt(err.response?.headers?.['retry-after'], 10);
            const delay = is429
                ? (retryAfter || 15) * 1000 * attempt
                : 5 * 1000 * attempt;
            log.warn(`[${label}] Attempt ${attempt}/${maxAttempts} failed: ${err.message} — retrying in ${delay / 1000}s...`);
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

// ── Reports API primitives ─────────────────────────────────────────────────────
async function requestReport(tokenManager, endpoint, reportType, marketplaceIds) {
    const token = await tokenManager.getToken();
    const res = await withRetry(() => axios.post(`${endpoint}/reports/2021-06-30/reports`, {
        reportType, marketplaceIds,
    }, {
        headers: { 'x-amz-access-token': token, 'Content-Type': 'application/json' },
    }), `requestReport ${reportType}`);
    return res.data.reportId;
}

// Returns { status, documentId } — does NOT wait. Caller decides what to do.
async function checkReport(tokenManager, endpoint, reportId) {
    const token = await tokenManager.getToken();
    const res = await axios.get(`${endpoint}/reports/2021-06-30/reports/${reportId}`, {
        headers: { 'x-amz-access-token': token },
    });
    return {
        status: res.data.processingStatus,
        documentId: res.data.reportDocumentId || null,
    };
}

async function downloadReport(tokenManager, endpoint, reportDocumentId) {
    const token = await tokenManager.getToken();
    const res = await axios.get(`${endpoint}/reports/2021-06-30/documents/${reportDocumentId}`, {
        headers: { 'x-amz-access-token': token },
    });
    const doc = await axios.get(res.data.url, { responseType: 'text' });
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

// ── Snapshot writers ──────────────────────────────────────────────────────────
// Given parsed active listings + health report + API summaries + pan-EU ASIN set,
// write into amazon_active_listings, amazon_stock_country_snapshots, amazon_stock_raw_snapshots.
async function writeSnapshots(conn, {
    accountName, countryCode, dateRan,
    listingRows, reportRows, apiSummaries, panEuAsins,
}) {
    // Non-DE/UK health reports echo DE's pool (Pan-EU and EFN both do this —
    // the same FNSKU shows up as fulfillable in every EU marketplace it can
    // serve). Dedupe by reading DE's already-written rows for this date and
    // skipping any ASIN whose FNSKU matches DE. Assumes DE is processed first
    // (see claimNextUnit ordering + defer in processUnit).
    let deFnskusByAsin = null;
    if (countryCode !== 'DE' && countryCode !== 'UK') {
        const [deRows] = await conn.query(
            `SELECT asin, fnsku FROM amazon_stock_country_snapshots
             WHERE date_ran = ? AND country = 'DE' AND company = ?`,
            [dateRan, accountName]
        );
        deFnskusByAsin = new Map();
        for (const r of deRows) {
            const fnskus = (r.fnsku || '').split(',').map(s => s.trim()).filter(Boolean);
            if (fnskus.length) deFnskusByAsin.set(r.asin, new Set(fnskus));
        }
    }

    const activeAsins = new Set();

    // ── Active Listings ────────────────────────────────────────────────────
    for (const row of listingRows) {
        // FR (and possibly ES/IT) return GET_MERCHANT_LISTINGS_DATA with a
        // different schema than DE/UK: no asin1/asin2/asin3, only product-id
        // plus product-id-type. Type '1' means product-id is an ASIN.
        const productIdType = String(row['product-id-type'] || '').trim();
        const asin = (
            row['asin1'] ||
            row['asin'] ||
            (productIdType === '1' ? row['product-id'] : '') ||
            ''
        ).trim();
        if (!asin) continue;
        activeAsins.add(asin);

        const sku = (row['seller-sku'] || row['sku'] || '').trim();
        const fnsku = (row['fulfillment-channel-sku'] || row['fnsku'] || '').trim();
        const productName = (row['item-name'] || row['product-name'] || '').trim();
        const price = parseFloat(row['price'] || '0') || 0;

        await conn.execute(
            `INSERT INTO amazon_active_listings
                (date_ran, country, company, asin, sku, fnsku, product_name, status, price)
             VALUES (?, ?, ?, ?, ?, ?, ?, 'Active', ?)
             ON DUPLICATE KEY UPDATE
                 fnsku = VALUES(fnsku), product_name = VALUES(product_name),
                 status = VALUES(status), price = VALUES(price)`,
            [dateRan, countryCode, accountName, asin, sku, fnsku, productName, price]
        );
    }

    // ── Health Report → byAsin ─────────────────────────────────────────────
    // Report rows are per-SKU but quantities are per-FNSKU (physical inventory
    // pool). Multiple SKUs pointing at the same FNSKU echo the same pool
    // quantities, so we dedupe quantity accumulation by FNSKU. SKU/FNSKU string
    // accumulation stays outside so all SKUs are still captured.
    const byAsin = {};
    for (const row of reportRows) {
        const asin = row['asin'] || '';
        if (!asin) continue;
        if (!activeAsins.has(asin)) continue;

        const fnsku = (row['fnsku'] || '').trim();
        const sku = row['sku'] || '';

        if (panEuAsins.has(asin) && countryCode !== 'DE' && countryCode !== 'UK') continue;
        if (deFnskusByAsin && fnsku && deFnskusByAsin.get(asin)?.has(fnsku)) continue;

        if (!byAsin[asin]) {
            byAsin[asin] = {
                _fromHealth: true,
                seen_qty_keys: new Set(), seen_fnskus: new Set(), seen_skus: new Set(),
                fnsku: '', sku: '',
                condition_type: row['condition'] || 'New',
                fulfillable: 0, inbound_working: 0,
                inbound_shipped: 0, inbound_receiving: 0, reserved: 0,
            };
        }

        const a = byAsin[asin];
        if (!a.condition_type && row['condition']) a.condition_type = row['condition'];

        // Quantity dedup key: FNSKU when present, else SKU. If both blank, fall
        // back to a per-row unique token so we don't under-count.
        const qtyKey = fnsku ? `f:${fnsku}` : (sku ? `s:${sku}` : `r:${a.seen_qty_keys.size}`);
        if (!a.seen_qty_keys.has(qtyKey)) {
            a.seen_qty_keys.add(qtyKey);
            a.fulfillable       += parseInt(row['available'] || '0', 10);
            a.inbound_working   += parseInt(row['inbound-working'] || '0', 10);
            a.inbound_shipped   += parseInt(row['inbound-shipped'] || '0', 10);
            a.inbound_receiving += parseInt(row['inbound-received'] || '0', 10);
            a.reserved          += parseInt(row['Total Reserved Quantity'] || '0', 10);
        }

        if (fnsku && !a.seen_fnskus.has(fnsku)) {
            a.seen_fnskus.add(fnsku);
            a.fnsku = a.fnsku ? a.fnsku + ',' + fnsku : fnsku;
        }
        if (sku && !a.seen_skus.has(sku)) {
            a.seen_skus.add(sku);
            a.sku = a.sku ? a.sku + ',' + sku : sku;
        }
    }

    // ── API Fallback (ASINs missing from health report) ───────────────────
    // Sum across every FNSKU pool the API returns for the same ASIN — Amazon
    // can return 7+ FNSKUs per ASIN (re-stickered units, returns, etc.) and
    // skipping all but the first silently drops real stock. Dedup by FNSKU.
    let recoveredCount = 0;
    for (const item of apiSummaries) {
        const asin = item.asin || '';
        if (!asin) continue;
        if (byAsin[asin] && byAsin[asin]._fromHealth) continue;
        if (!activeAsins.has(asin)) continue;
        if (panEuAsins.has(asin) && countryCode !== 'DE' && countryCode !== 'UK') continue;

        const fnsku = (item.fnSku || '').trim();
        if (deFnskusByAsin && fnsku && deFnskusByAsin.get(asin)?.has(fnsku)) continue;

        const details = item.inventoryDetails || {};

        if (!byAsin[asin]) {
            byAsin[asin] = {
                seen_fnskus: new Set(), seen_skus: new Set(),
                fnsku: '', sku: '',
                condition_type: item.condition || 'New',
                fulfillable: 0, reserved: 0,
                inbound_working: 0, inbound_shipped: 0, inbound_receiving: 0,
            };
            recoveredCount++;
        }
        const a = byAsin[asin];
        if (fnsku && !a.seen_fnskus.has(fnsku)) {
            a.seen_fnskus.add(fnsku);
            a.fnsku = a.fnsku ? a.fnsku + ',' + fnsku : fnsku;
            a.fulfillable       += details.fulfillableQuantity        || 0;
            a.inbound_working   += details.inboundWorkingQuantity     || 0;
            a.inbound_shipped   += details.inboundShippedQuantity     || 0;
            a.inbound_receiving += details.inboundReceivingQuantity   || 0;
            a.reserved          += details.reservedQuantity?.totalReservedQuantity || 0;
        }
        const sku = (item.sellerSku || '').trim();
        if (sku && !a.seen_skus.has(sku)) {
            a.seen_skus.add(sku);
            a.sku = a.sku ? a.sku + ',' + sku : sku;
        }
    }

    // ── Upsert country snapshots ───────────────────────────────────────────
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

    // ── Raw snapshots ─────────────────────────────────────────────────────
    // Same FNSKU-pool dedup as above — sum each pool's fulfillable once per ASIN.
    const rawByAsin = {};
    const rawSeenQtyKeys = {};
    for (const row of reportRows) {
        const asin = row['asin'] || '';
        if (!asin || !activeAsins.has(asin)) continue;
        const fnsku = (row['fnsku'] || '').trim();
        const sku = row['sku'] || '';
        if (!rawSeenQtyKeys[asin]) rawSeenQtyKeys[asin] = new Set();
        const qtyKey = fnsku ? `f:${fnsku}` : (sku ? `s:${sku}` : `r:${rawSeenQtyKeys[asin].size}`);
        if (rawSeenQtyKeys[asin].has(qtyKey)) continue;
        rawSeenQtyKeys[asin].add(qtyKey);
        if (!rawByAsin[asin]) rawByAsin[asin] = 0;
        rawByAsin[asin] += parseInt(row['available'] || '0', 10);
    }
    for (const item of apiSummaries) {
        const asin = item.asin || '';
        if (!asin || rawByAsin[asin] !== undefined || !activeAsins.has(asin)) continue;
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

    // ── Pan-EU broadcast ───────────────────────────────────────────────────
    let panEuBroadcast = 0;
    const panEuAsinList = [...panEuAsins];

    if (panEuAsinList.length > 0 && countryCode === 'DE') {
        const placeholders = panEuAsinList.map(() => '?').join(',');
        const [result] = await conn.execute(
            `INSERT INTO amazon_stock_raw_snapshots (date_ran, country, asin, company, fulfillable)
             SELECT r.date_ran, al.country, r.asin, r.company, r.fulfillable
             FROM amazon_stock_raw_snapshots r
             JOIN amazon_active_listings al
                 ON al.asin = r.asin AND al.company = r.company AND al.date_ran = r.date_ran
             WHERE r.date_ran = ? AND r.country = 'DE' AND r.company = ?
               AND r.asin IN (${placeholders})
               AND al.country NOT IN ('DE', 'UK')
             ON DUPLICATE KEY UPDATE
                 fulfillable = VALUES(fulfillable)`,
            [dateRan, accountName, ...panEuAsinList]
        );
        panEuBroadcast = result.affectedRows;
    } else if (panEuAsinList.length > 0 && countryCode !== 'UK') {
        const placeholders = panEuAsinList.map(() => '?').join(',');
        const [result] = await conn.execute(
            `INSERT INTO amazon_stock_raw_snapshots (date_ran, country, asin, company, fulfillable)
             SELECT ?, ?, r.asin, r.company, r.fulfillable
             FROM amazon_stock_raw_snapshots r
             WHERE r.country = 'DE' AND r.company = ?
               AND r.asin IN (${placeholders})
               AND r.date_ran = (SELECT MAX(date_ran) FROM amazon_stock_raw_snapshots
                                 WHERE country = 'DE' AND company = ?)
               AND r.asin IN (SELECT asin FROM amazon_active_listings
                              WHERE country = ? AND company = ? AND date_ran = ?)
             ON DUPLICATE KEY UPDATE
                 fulfillable = VALUES(fulfillable)`,
            [dateRan, countryCode, accountName, ...panEuAsinList, accountName, countryCode, accountName, dateRan]
        );
        panEuBroadcast = result.affectedRows;
    }

    return { upsertCount, recoveredCount, rawCount, panEuBroadcast };
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
    return totalBackfilled;
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

module.exports = {
    ACCOUNTS, ACCOUNT_NAMES, REGIONS, COUNTRY_CODES,
    REPORT_TYPES, SP_REPORT_TYPE,
    getMarketplace, getEndpoint, getTokenManager,
    SPTokenManager, withRetry, rateLimitedRequest,
    requestReport, checkReport, downloadReport,
    parseTsvReport, fetchInventorySummaries,
    writeSnapshots, runBackfill, copyFailedFromPrevious,
};
