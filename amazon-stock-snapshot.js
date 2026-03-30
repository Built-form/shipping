'use strict';

const axios = require('axios');
require('dotenv').config();
const log = require('./logger');
const { getPool } = require('./db');
// Note: Database table creation should ideally be handled by a migration script
// and not within the Lambda handler for production environments.
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

// ── Regions — one SP-API endpoint per region, multiple marketplace IDs ────────
// UK always has its own inventory pool. DE/FR/ES/IT may share a Pan-European
// pool (identical stock) or have per-marketplace stock depending on the product.
const REGIONS = {
    EU: {
        endpoint: 'https://sellingpartnerapi-eu.amazon.com',
        marketplaces: {
            UK: 'A1F83G8C2ARO7P', DE: 'A1PA6795UKMFR9',
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
            const delay = (err.response?.headers?.['retry-after'] ?? 5) * 1000 * attempt;
            log.warn(`[${label}] Attempt ${attempt}/3 failed: ${err.message} — retrying in ${delay / 1000}s...`);
            await new Promise(r => setTimeout(r, delay));
        }
    }
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

async function pollReport(tokenManager, endpoint, reportId, label, maxWaitMs = 120000) {
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
        log.info(`[${label}] ${status}`);
        await new Promise(r => setTimeout(r, 10000));
    }
    throw new Error(`Report ${reportId} timed out after ${maxWaitMs / 1000}s`);
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

// ── Fetch per-marketplace inventory health via Reports API ───────────────────
async function fetchInventoryHealthReport(tokenManager, endpoint, marketplaceId, label) {
    const reportId = await requestReport(tokenManager, endpoint, 'GET_FBA_INVENTORY_PLANNING_DATA', [marketplaceId]);
    const docId = await pollReport(tokenManager, endpoint, reportId, label);
    const tsv = await downloadReport(tokenManager, endpoint, docId);
    return parseTsvReport(tsv);
}

// ── Main handler ───────────────────────────────────────────────────────────────
const handler = async () => {
    const dateRan = new Date().toISOString().slice(0, 10);
    const conn = await getPool().getConnection();

    try {
        log.info(`Amazon snapshot ${dateRan}`);
        const results = { success: [], failed: [] };

        async function processAccount(account) {
            if (!account.clientId || !account.clientSecret || !account.refreshToken) {
                log.warn(`[${account.name}] SP-API credentials not configured — skipping`);
                return;
            }

            log.info(`[${account.name}] Starting...`);
            const tokenManager = new SPTokenManager(account.clientId, account.clientSecret, account.refreshToken);

            const marketplaceTasks = [];
            for (const [region, { endpoint, marketplaces }] of Object.entries(REGIONS)) {
                for (const [code, marketplaceId] of Object.entries(marketplaces)) {
                    marketplaceTasks.push({ code, endpoint, marketplaceId });
                }
            }

            await Promise.all(marketplaceTasks.map(async ({ code, endpoint, marketplaceId }) => {
                try {
                    const rows = await fetchInventoryHealthReport(tokenManager, endpoint, marketplaceId, `${account.name}/${code}`);

                    const byAsin = {};
                    for (const row of rows) {
                        const asin = row['asin'] || '';
                        if (!asin) continue;
                        if (!byAsin[asin]) {
                            byAsin[asin] = {
                                fnsku: row['fnsku'] || '', sku: row['sku'] || '',
                                condition_type: row['condition'] || '',
                                fulfillable: 0, inbound_working: 0,
                                inbound_shipped: 0, inbound_receiving: 0, reserved: 0,
                            };
                        }
                        const a = byAsin[asin];
                        a.fulfillable       += parseInt(row['available'] || '0', 10);
                        a.inbound_working   += parseInt(row['inbound-working'] || '0', 10);
                        a.inbound_shipped   += parseInt(row['inbound-shipped'] || '0', 10);
                        a.inbound_receiving += parseInt(row['inbound-received'] || '0', 10);
                        a.reserved          += parseInt(row['Total Reserved Quantity'] || '0', 10);
                    }

                    for (const [asin, a] of Object.entries(byAsin)) {
                        await conn.execute(
                            `INSERT INTO amazon_stock_country_snapshots
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
                    }

                    const asinCount = Object.keys(byAsin).length;
                    log.info(`[${account.name}/${code}] ${asinCount} ASINs`);
                    results.success.push({ marketplace: `${account.name}/${code}-health-report`, rows: rows.length });
                } catch (err) {
                    log.error(`[${account.name}/${code}] Failed: ${err.message}`);
                    results.failed.push({ marketplace: `${account.name}/${code}-health-report`, error: err.message });
                }
            }));
        }

        await Promise.all(ACCOUNTS.map(account => processAccount(account)));

        // Fallback: if all failed, copy last available snapshot to today
        if (results.success.length === 0 && results.failed.length > 0) {
            log.warn('All marketplaces failed — falling back to last available snapshot');

            const [lastDate] = await conn.query(
                `SELECT MAX(date_ran) as last_date FROM amazon_stock_country_snapshots WHERE date_ran < ?`,
                [dateRan]
            );

            if (lastDate[0]?.last_date) {
                const yesterday = lastDate[0].last_date;
                log.info(`Copying data from ${yesterday} to ${dateRan}`);

                await conn.execute(
                    `INSERT INTO amazon_stock_country_snapshots
                     (date_ran, country, asin, fnsku, sku, condition_type,
                      fulfillable, inbound_working, inbound_shipped, inbound_receiving, reserved)
                     SELECT ?, country, asin, fnsku, sku, condition_type,
                            fulfillable, inbound_working, inbound_shipped, inbound_receiving, reserved
                     FROM amazon_stock_country_snapshots WHERE date_ran = ?
                     ON DUPLICATE KEY UPDATE
                        fnsku = VALUES(fnsku), sku = VALUES(sku),
                        condition_type = VALUES(condition_type),
                        fulfillable = VALUES(fulfillable),
                        inbound_working = VALUES(inbound_working),
                        inbound_shipped = VALUES(inbound_shipped),
                        inbound_receiving = VALUES(inbound_receiving),
                        reserved = VALUES(reserved)`,
                    [dateRan, yesterday]
                );

                log.info('Fallback snapshot copied');
                results.fallback = `Used data from ${yesterday}`;
            } else {
                log.warn('No previous snapshot available for fallback');
            }
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
