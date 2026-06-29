'use strict';

const axios = require('axios');
const log = require('../lib/logger');

// ── Regions ─────────────────────────────────────────────────────────────────────
// SP-API endpoints are per-region: a report requested against the NA endpoint
// must also be polled/downloaded against NA. Country code → region is derived
// from these marketplace maps (see getRegionForCountry / getEndpointForCountry).
const REGIONS = {
    EU: {
        endpoint: 'https://sellingpartnerapi-eu.amazon.com',
        marketplaces: {
            DE: 'A1PA6795UKMFR9', UK: 'A1F83G8C2ARO7P',
            FR: 'A13V1IB3VIYZZH', ES: 'A1RKKUPIHCS9HS', IT: 'APJ6JRA9NG5V4',
        },
    },
    NA: {
        endpoint: 'https://sellingpartnerapi-na.amazon.com',
        marketplaces: {
            US: 'ATVPDKIKX0DER',
        },
    },
};

// ── Accounts ────────────────────────────────────────────────────────────────────
// Credentials are per-region: an LWA refresh token is only valid for the region
// it was authorized against. Hangerworld's NA authorization reuses the same HW
// SP-API app (client id/secret) with a separate NA-authorized refresh token, so
// only the refresh token differs between EU and NA. `countries` lists which
// marketplaces each account actually sells in (JFA is EU-only; HW adds US).
const ACCOUNTS = {
    JFA: {
        countries: ['DE', 'UK', 'FR', 'ES', 'IT'],
        credentials: {
            EU: {
                clientId: process.env.AMAZON_SP_CLIENT_ID,
                clientSecret: process.env.AMAZON_SP_CLIENT_SECRET,
                refreshToken: process.env.AMAZON_SP_REFRESH_TOKEN,
            },
        },
    },
    Hangerworld: {
        countries: ['DE', 'UK', 'FR', 'ES', 'IT', 'US'],
        credentials: {
            EU: {
                clientId: process.env.AMAZON_SP_CLIENT_ID_HW,
                clientSecret: process.env.AMAZON_SP_CLIENT_SECRET_HW,
                refreshToken: process.env.AMAZON_SP_REFRESH_TOKEN_HW,
            },
            NA: {
                clientId: process.env.AMAZON_SP_CLIENT_ID_HW,
                clientSecret: process.env.AMAZON_SP_CLIENT_SECRET_HW,
                refreshToken: process.env.AMAZON_SP_REFRESH_TOKEN_US,
            },
        },
    },
};

// FR/ES/IT receive DE's pooled Pan-EU/EFN inventory as cross-border echoes, so
// their snapshots must be deduped against DE. DE is the primary pool; UK and US
// are standalone single-marketplace regions that never pool with DE.
const PAN_EU_POOLED_COUNTRIES = new Set(['FR', 'ES', 'IT']);
function isPanEuPooled(countryCode) {
    return PAN_EU_POOLED_COUNTRIES.has(countryCode);
}

const COUNTRY_CODES = Object.values(REGIONS).flatMap(r => Object.keys(r.marketplaces));
const ACCOUNT_NAMES = Object.keys(ACCOUNTS);

function getAccountCountries(accountName) {
    return ACCOUNTS[accountName]?.countries || [];
}

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

// Region for a country code. 'ALL' (account-scoped pan_eu) is EU-scoped.
function getRegionForCountry(countryCode) {
    if (countryCode === 'ALL') return 'EU';
    for (const [regionName, region] of Object.entries(REGIONS)) {
        if (region.marketplaces[countryCode]) return regionName;
    }
    throw new Error(`Unknown country code: ${countryCode}`);
}

// SP-API endpoint for a country code. 'ALL' (pan_eu) uses EU.
function getEndpointForCountry(countryCode) {
    if (countryCode === 'ALL') return REGIONS.EU.endpoint;
    return getMarketplace(countryCode).endpoint;
}

// Back-compat default endpoint (EU). Prefer getEndpointForCountry for anything
// that can target a non-EU marketplace.
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

// Manager cache: a fresh instance has _token=null, so its 45-minute LWA cache
// is per-instance. Sharing one instance per (account, region) across all callers
// keeps us under the LWA rate limit (one refresh per ~45 min, not one per call).
// Cached per region because EU and NA use different refresh tokens.
// Module-level state survives across warm Lambda invocations.
const tokenManagers = new Map();
function getTokenManager(accountName, region = 'EU') {
    const key = `${accountName}|${region}`;
    if (tokenManagers.has(key)) return tokenManagers.get(key);
    const account = ACCOUNTS[accountName];
    if (!account) throw new Error(`Unknown account: ${accountName}`);
    const creds = account.credentials?.[region];
    if (!creds) throw new Error(`Account ${accountName} has no ${region} credentials`);
    if (!creds.clientId || !creds.clientSecret || !creds.refreshToken) {
        throw new Error(`Account ${accountName} missing SP-API credentials for ${region}`);
    }
    const manager = new SPTokenManager(creds.clientId, creds.clientSecret, creds.refreshToken);
    tokenManagers.set(key, manager);
    return manager;
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
// Rate-limited + retried: pollPending fires up to 10 of these back-to-back,
// and the Reports API getReport bucket is small (2/sec), so unrate-limited
// bursts trip 429 quickly. Account name is used to scope the per-account
// queue in rateLimitedRequest.
async function checkReport(tokenManager, endpoint, reportId, accountName) {
    const token = await tokenManager.getToken();
    const res = await withRetry(() => rateLimitedRequest(() =>
        axios.get(`${endpoint}/reports/2021-06-30/reports/${reportId}`, {
            headers: { 'x-amz-access-token': token },
        })
    , accountName), `checkReport ${reportId}`);
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
    // SP-API can serve documents either plain or gzip-compressed. Always
    // download as bytes and sniff the gzip magic (1f 8b) — don't rely on
    // res.data.compressionAlgorithm alone, since treating gzipped bytes as
    // text yields garbage rows with garbage keys and silently writes 0 ASINs.
    const doc = await axios.get(res.data.url, { responseType: 'arraybuffer' });
    const buf = Buffer.from(doc.data);
    const isGzip = buf.length >= 2 && buf[0] === 0x1f && buf[1] === 0x8b;
    if (isGzip) return require('zlib').gunzipSync(buf).toString('utf8');
    return buf.toString('utf8');
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
    // Per-country locality (applied in the health + API loops below): each
    // marketplace's report echoes every other marketplace's FBA pool, so a
    // country only counts the pools it lists as its own SKU. That attributes
    // each physical pool — including separate per-country MCI pools, and stock
    // left over after a Pan-EU enrolment ended — to its home marketplace.
    //
    // FR/ES/IT additionally drop pools sharing one of DE's own FNSKUs: those
    // are cross-border echoes of DE's single physical pool (true Pan-EU), not
    // local stock. DE is processed first and written locality-filtered, so its
    // stored FNSKU list is exactly DE's own pools — read it here to spot echoes.
    // (Assumes DE first: see claimNextUnit ordering + defer in processUnit.)
    let deFnskusByAsin = null;
    if (isPanEuPooled(countryCode)) {
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
    const activeSkus = new Set();

    // ── SKU → ASIN map for EAN-keyed listing recovery ──────────────────────
    // Some listings (especially newer ones in FR/ES/IT) use product-id-type=4
    // (EAN) instead of type=1 (ASIN), so the listings TSV holds an EAN in the
    // product-id column and our ASIN extraction returns empty. The API
    // summaries call always carries both sellerSku and asin per FNSKU pool,
    // so we use it as a fallback resolver.
    const skuToAsin = new Map();
    for (const item of apiSummaries) {
        const sku = (item.sellerSku || '').trim();
        const asin = (item.asin || '').trim();
        if (sku && asin && !skuToAsin.has(sku)) skuToAsin.set(sku, asin);
    }

    // ── FNSKU → live reserved (real-time, matches Seller Central) ──────────
    // The health report's reserved columns are a once-daily snapshot that
    // badly under-reports pending-customer-order reserved on fast-movers
    // (observed 14 vs 115 and 11 vs 61 on single UK pools — the dashboard's
    // "Amazon Total" then reads low vs Seller Central). The
    // /fba/inventory/v1/summaries call we already make for the fallback is
    // real-time and marketplace-scoped, so its totalReservedQuantity is the
    // authoritative reserved for each physical pool. Key by FNSKU (the pool);
    // multiple SKUs can map to one FNSKU with identical quantities, first wins.
    const apiReservedByFnsku = new Map();
    for (const item of apiSummaries) {
        const fnsku = (item.fnSku || '').trim();
        if (!fnsku || apiReservedByFnsku.has(fnsku)) continue;
        const r = item.inventoryDetails?.reservedQuantity?.totalReservedQuantity;
        if (typeof r === 'number') apiReservedByFnsku.set(fnsku, r);
    }

    // ── Active Listings ────────────────────────────────────────────────────
    for (const row of listingRows) {
        // FR (and possibly ES/IT) return GET_MERCHANT_LISTINGS_DATA with a
        // different schema than DE/UK: no asin1/asin2/asin3, only product-id
        // plus product-id-type. Type '1' means product-id is an ASIN; type 2
        // (UPC), 3 (ISBN), 4 (EAN) need resolution via the API-summary map.
        const productIdType = String(row['product-id-type'] || '').trim();
        const sku = (row['seller-sku'] || row['sku'] || '').trim();
        let asin = (
            row['asin1'] ||
            row['asin'] ||
            (productIdType === '1' ? row['product-id'] : '') ||
            ''
        ).trim();
        if (!asin && sku) asin = skuToAsin.get(sku) || '';
        if (!asin) continue;
        activeAsins.add(asin);

        const fnsku = (row['fulfillment-channel-sku'] || row['fnsku'] || '').trim();
        const productName = (row['item-name'] || row['product-name'] || '').trim();
        const price = parseFloat(row['price'] || '0') || 0;
        if (sku) activeSkus.add(sku);

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

        // Locality: only count a pool this marketplace lists as its own SKU.
        // Every other country's pool merely echoes in this report; this pins
        // each physical pool to its home marketplace (MCI, EFN, or stock left
        // behind after an ended Pan-EU enrolment).
        if (!activeSkus.has(sku)) continue;
        // FR/ES/IT: a surviving pool sharing one of DE's own FNSKUs is a
        // cross-border echo of DE's single physical pool (true Pan-EU), not
        // local stock — drop it so it isn't double-counted against DE.
        if (isPanEuPooled(countryCode) && fnsku && deFnskusByAsin?.get(asin)?.has(fnsku)) {
            continue;
        }

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
            // Amazon's `Total Reserved Quantity` is internally inconsistent: it
            // frequently omits the FC-transfer (transshipment) portion, dropping
            // hundreds of in-transit units from reserved (~25-55% of SKUs with
            // FC transfer in our reports). The same report also breaks reserved
            // into mutually-exclusive components, so take the larger of the
            // reported total and the component sum — never under-count FC
            // transfer, but keep the total when it captures extra reserved
            // buckets (e.g. Staging, which overlaps FC transfer and is excluded
            // here) that the three components don't. If a marketplace's report
            // lacks the component columns they read as 0, so this degrades to
            // exactly the reported total.
            const totalReserved = parseInt(row['Total Reserved Quantity'] || '0', 10);
            const componentReserved =
                parseInt(row['Reserved Customer Order'] || '0', 10) +
                parseInt(row['Reserved FC Transfer']     || '0', 10) +
                parseInt(row['Reserved FC Processing']   || '0', 10);
            // The live inventory API is real-time and matches Seller Central,
            // whereas the health report's reserved is a stale daily snapshot
            // that under-reports it; fold in the live per-pool value so we
            // never under-count. Take the largest of all three — max (not sum)
            // keeps this safe against the same pool being seen twice.
            const liveReserved = fnsku ? apiReservedByFnsku.get(fnsku) : undefined;
            a.reserved          += Math.max(totalReserved, componentReserved, liveReserved || 0);
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
    //
    // SKU-scoped: /fba/inventory/v1/summaries with granularityType=Marketplace
    // does NOT actually filter to that marketplace's physical FBA — it returns
    // every FNSKU pool registered to the seller account, including pools
    // physically held in other countries' FBAs (e.g. ES/IT-stickered units
    // appear in the DE call). Counting all of them inflates the country's
    // fulfillable. So we only count an item when its sellerSku is listed in
    // THIS country's active_listings — that's the load-bearing locality
    // signal.
    let recoveredCount = 0;
    for (const item of apiSummaries) {
        const asin = item.asin || '';
        if (!asin) continue;
        if (byAsin[asin] && byAsin[asin]._fromHealth) continue;
        if (!activeAsins.has(asin)) continue;

        const sku = (item.sellerSku || '').trim();
        if (!sku || !activeSkus.has(sku)) continue;

        const fnsku = (item.fnSku || '').trim();

        // FR/ES/IT echo drop (see health-report branch); locality is already
        // enforced above via activeSkus.has(sku).
        if (isPanEuPooled(countryCode) && fnsku && deFnskusByAsin?.get(asin)?.has(fnsku)) {
            continue;
        }

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
    const rawFromHealth = new Set();
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
        rawFromHealth.add(asin);
    }
    // API fallback: only fires for ASINs the health report didn't mention.
    // Inside a single API fallback we still accumulate across multiple FNSKU
    // pools for the same ASIN, with the same SKU-scoping the country-snapshot
    // fallback uses (otherwise non-DE FBA pools leak into DE's count).
    for (const item of apiSummaries) {
        const asin = item.asin || '';
        if (!asin || !activeAsins.has(asin)) continue;
        if (rawFromHealth.has(asin)) continue;

        const sku = (item.sellerSku || '').trim();
        if (!sku || !activeSkus.has(sku)) continue;

        const fnsku = (item.fnSku || '').trim();
        if (!rawSeenQtyKeys[asin]) rawSeenQtyKeys[asin] = new Set();
        const qtyKey = fnsku ? `f:${fnsku}` : `s:${sku}`;
        if (rawSeenQtyKeys[asin].has(qtyKey)) continue;
        rawSeenQtyKeys[asin].add(qtyKey);

        if (rawByAsin[asin] === undefined) rawByAsin[asin] = 0;
        rawByAsin[asin] += item.inventoryDetails?.fulfillableQuantity || 0;
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
               AND al.country IN ('FR', 'ES', 'IT')
             ON DUPLICATE KEY UPDATE
                 fulfillable = VALUES(fulfillable)`,
            [dateRan, accountName, ...panEuAsinList]
        );
        panEuBroadcast = result.affectedRows;
    } else if (panEuAsinList.length > 0 && isPanEuPooled(countryCode)) {
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
    getAccountCountries, isPanEuPooled,
    getRegionForCountry, getEndpointForCountry,
    getMarketplace, getEndpoint, getTokenManager,
    SPTokenManager, withRetry, rateLimitedRequest,
    requestReport, checkReport, downloadReport,
    parseTsvReport, fetchInventorySummaries,
    writeSnapshots, runBackfill, copyFailedFromPrevious,
};
