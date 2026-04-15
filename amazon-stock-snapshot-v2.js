'use strict';

const axios = require('axios');
require('dotenv').config();
const log = require('./logger');
const { getPool } = require('./db');

// ── Accounts ────────────────────────────────────────────────────────────────────
const ACCOUNTS = [
    {
        name: 'JFA',
        clientId: process.env.AMAZON_SP_CLIENT_ID,
        clientSecret: process.env.AMAZON_SP_CLIENT_SECRET,
        refreshToken: process.env.AMAZON_SP_REFRESH_TOKEN,
    },
    {
        name: 'Hangerworld',
        clientId: process.env.AMAZON_SP_CLIENT_ID_HW,
        clientSecret: process.env.AMAZON_SP_CLIENT_SECRET_HW,
        refreshToken: process.env.AMAZON_SP_REFRESH_TOKEN_HW,
    },
];

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

// ── Retry helper ───────────────────────────────────────────────────────────────
async function withRetry(fn, label) {
    for (let attempt = 1; attempt <= 3; attempt++) {
        try { return await fn(); } catch (err) {
            if (attempt === 3) throw err;
            const delay = (err.response?.headers?.['retry-after'] ?? 2) * 1000 * attempt;
            log.warn(`[${label}] Attempt ${attempt}/3 failed: ${err.message} — retrying in ${delay / 1000}s...`);
            await new Promise(r => setTimeout(r, delay));
        }
    }
}

// ── Rate limiter — getInventorySummaries allows 2 req/s ────────────────────────
let lastRequestTime = 0;
async function rateLimitedRequest(fn) {
    const now = Date.now();
    const elapsed = now - lastRequestTime;
    if (elapsed < 500) await new Promise(r => setTimeout(r, 500 - elapsed));
    lastRequestTime = Date.now();
    return fn();
}

// ── Fetch all inventory summaries for a marketplace (paginated) ────────────────
async function fetchInventorySummaries(tokenManager, endpoint, marketplaceId, label) {
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
        ), `${label} inventory`);

        const payload = res.data.payload || res.data;
        if (payload.inventorySummaries) {
            summaries.push(...payload.inventorySummaries);
        }
        nextToken = payload.pagination?.nextToken || null;

        if (nextToken) {
            log.info(`[${label}] Fetched ${summaries.length} items so far, paginating...`);
        }
    } while (nextToken);

    return summaries;
}

// ── Main handler ───────────────────────────────────────────────────────────────
const handler = async () => {
    const dateRan = new Date().toISOString().slice(0, 10);
    const conn = await getPool().getConnection();

    try {
        log.info(`Amazon inventory snapshot ${dateRan}`);
        const results = { success: [], failed: [] };

        async function processAccount(account) {
            if (!account.clientId || !account.clientSecret || !account.refreshToken) {
                log.warn(`[${account.name}] SP-API credentials not configured — skipping`);
                return;
            }

            log.info(`[${account.name}] Starting...`);
            const tokenManager = new SPTokenManager(account.clientId, account.clientSecret, account.refreshToken);

            // Process marketplaces sequentially to respect rate limit
            for (const [, { endpoint, marketplaces }] of Object.entries(REGIONS)) {
                for (const [code, marketplaceId] of Object.entries(marketplaces)) {
                    const label = `${account.name}/${code}`;
                    try {
                        const summaries = await fetchInventorySummaries(tokenManager, endpoint, marketplaceId, label);
                        log.info(`[${label}] API returned ${summaries.length} raw items`);

                        const byAsin = {};
                        for (const item of summaries) {
                            const asin = item.asin || '';
                            if (!asin) continue;

                            const fnsku = (item.fnSku || '').trim();
                            const isBprefix = fnsku.toUpperCase().startsWith('B');

                            // Skip B-prefix in FR/ES/IT — Pan-European duplicates
                            if (isBprefix && code !== 'DE' && code !== 'UK') continue;

                            if (!byAsin[asin]) {
                                byAsin[asin] = {
                                    fnsku: fnsku,
                                    sku: item.sellerSku || '',
                                    condition_type: item.condition || '',
                                    fulfillable: 0, inbound_working: 0,
                                    inbound_shipped: 0, inbound_receiving: 0, reserved: 0,
                                };
                            }

                            const a = byAsin[asin];
                            const details = item.inventoryDetails || {};

                            if (!a.condition_type && item.condition) a.condition_type = item.condition;

                            a.fulfillable       += details.fulfillableQuantity || 0;
                            a.inbound_working   += details.inboundWorkingQuantity || 0;
                            a.inbound_shipped   += details.inboundShippedQuantity || 0;
                            a.inbound_receiving += details.inboundReceivingQuantity || 0;
                            a.reserved          += details.reservedQuantity?.totalReservedQuantity || 0;
                        }

                        let upsertCount = 0;
                        for (const [asin, a] of Object.entries(byAsin)) {
                            await conn.execute(
                                `INSERT INTO amazon_stock_country_snapshots2
                                    (date_ran, country, asin, fnsku, sku, condition_type,
                                     fulfillable, inbound_working, inbound_shipped, inbound_receiving, reserved)
                                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                 ON DUPLICATE KEY UPDATE
                                     fnsku = VALUES(fnsku), sku = VALUES(sku),
                                     condition_type = VALUES(condition_type),
                                     fulfillable = VALUES(fulfillable),
                                     inbound_working = VALUES(inbound_working),
                                     inbound_shipped = VALUES(inbound_shipped),
                                     inbound_receiving = VALUES(inbound_receiving),
                                     reserved = VALUES(reserved)`,
                                [dateRan, code, asin, a.fnsku, a.sku, a.condition_type,
                                 a.fulfillable, a.inbound_working, a.inbound_shipped, a.inbound_receiving, a.reserved]
                            );
                            upsertCount++;
                        }

                        log.info(`[${label}] ${upsertCount} unique ASINs`);
                        results.success.push({ marketplace: `${label}`, asins: upsertCount });
                    } catch (err) {
                        log.error(`[${label}] Failed: ${err.message}`);
                        results.failed.push({ marketplace: `${label}`, error: err.message });
                    }
                }
            }
        }

        // Process accounts sequentially
        for (const account of ACCOUNTS) {
            await processAccount(account);
        }

        // Per-marketplace fallback
        for (const f of results.failed) {
            const [, country] = f.marketplace.split('/');
            if (!country) continue;

            const [lastDate] = await conn.query(
                `SELECT MAX(date_ran) as last_date FROM amazon_stock_country_snapshots2 WHERE date_ran < ? AND country = ?`,
                [dateRan, country]
            );
            if (!lastDate[0]?.last_date) continue;

            const prevDate = lastDate[0].last_date;
            log.info(`[${f.marketplace}] Failed — copying ${country} data from ${prevDate}`);
            await conn.execute(
                `INSERT INTO amazon_stock_country_snapshots2
                 (date_ran, country, asin, fnsku, sku, condition_type,
                  fulfillable, inbound_working, inbound_shipped, inbound_receiving, reserved)
                 SELECT ?, country, asin, fnsku, sku, condition_type,
                        fulfillable, inbound_working, inbound_shipped, inbound_receiving, reserved
                 FROM amazon_stock_country_snapshots2 WHERE date_ran = ? AND country = ?
                 ON DUPLICATE KEY UPDATE
                    fnsku = VALUES(fnsku), sku = VALUES(sku),
                    condition_type = VALUES(condition_type),
                    fulfillable = VALUES(fulfillable),
                    inbound_working = VALUES(inbound_working),
                    inbound_shipped = VALUES(inbound_shipped),
                    inbound_receiving = VALUES(inbound_receiving),
                    reserved = VALUES(reserved)`,
                [dateRan, prevDate, country]
            );
        }

        // Backfill historical gaps by rippling forward one day at a time.
        // Since this API returns zero-stock ASINs, gaps only come from Lambda failures,
        // so carry forward ALL values (not zeros).
        let totalBackfilled = 0;
        for (let i = 0; i < 100; i++) {
            const [result] = await conn.execute(
                `INSERT INTO amazon_stock_country_snapshots2
                 (date_ran, country, asin, fnsku, sku, condition_type,
                  fulfillable, inbound_working, inbound_shipped, inbound_receiving, reserved)
                 SELECT DATE_ADD(s.date_ran, INTERVAL 1 DAY),
                        s.country, s.asin, s.fnsku, s.sku, s.condition_type,
                        s.fulfillable, s.inbound_working, s.inbound_shipped, s.inbound_receiving, s.reserved
                 FROM amazon_stock_country_snapshots2 s
                 LEFT JOIN amazon_stock_country_snapshots2 nxt
                     ON nxt.date_ran = DATE_ADD(s.date_ran, INTERVAL 1 DAY)
                     AND nxt.country = s.country AND nxt.asin = s.asin
                 WHERE nxt.asin IS NULL
                   AND DATE_ADD(s.date_ran, INTERVAL 1 DAY) <= ?
                 ON DUPLICATE KEY UPDATE
                    fulfillable = VALUES(fulfillable),
                    inbound_working = VALUES(inbound_working),
                    inbound_shipped = VALUES(inbound_shipped),
                    inbound_receiving = VALUES(inbound_receiving),
                    reserved = VALUES(reserved)`,
                [dateRan]
            );
            if (result.affectedRows === 0) break;
            totalBackfilled += result.affectedRows;
        }
        if (totalBackfilled > 0) {
            log.info(`[Backfill] Carried forward ${totalBackfilled} rows filling gaps up to ${dateRan}`);
        }

        log.info(`Done — ${results.success.length} OK, ${results.failed.length} failed.`);
        return { statusCode: 200, body: JSON.stringify(results) };

    } finally {
        conn.release();
    }
};

exports.handler = handler;
exports.REGIONS = REGIONS;

if (require.main === module) {
    handler().then(r => {
        log.info('Result:', JSON.stringify(r, null, 2));
        process.exit(0);
    }).catch(err => {
        log.error('Fatal:', err.message, err.stack);
        process.exit(1);
    });
}
