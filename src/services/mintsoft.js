const axios = require('axios');
const log = require('../lib/logger');
require('dotenv').config();

const MINTSOFT_API_KEY = process.env.MINTSOFT_API_KEY;
const MINTSOFT_CLIENT_ID = process.env.MINTSOFT_CLIENT_ID
    ? Number(process.env.MINTSOFT_CLIENT_ID)
    : null;

if (!MINTSOFT_API_KEY) {
    log.warn('MINTSOFT_API_KEY is not configured.');
}

const mintsoftClient = axios.create({
    baseURL: 'https://api.mintsoft.co.uk/api',
    timeout: 15000,
    headers: { 'Content-Type': 'application/json' },
    params: { APIKey: MINTSOFT_API_KEY }
});

// Which child SKUs of a JF code count toward its stock. Deliberately a
// WHITELIST, not a `<CODE>_<anything>` pattern: Mintsoft also carries children
// that must never inflate the figure — _GOMPELS (customer-allocated), _OLD,
// _FAULTY, and pack variants like _50 / _SINGLE whose "units" aren't singles.
// A 2026-07-29 sweep found pattern-matching would have added 19,193 units
// across 428 codes, a quarter of it one customer's allocated stock.
//
// _TR (trade) counts — trade units are sellable stock like any other.
// New suffixes must be added here explicitly; until then they're invisible,
// which is the safe failure direction.
const SKU_SUFFIXES = ['_USA', '_QC', '_READY', '_pp', '_LABELLED', '_PACKED', '_IFU', '_FRANCE', '_TR', ''];

async function getProductsByJfCode(jfCode) {
    if (!jfCode) throw new Error('JF code is required');

    const skusToCheck = new Set(
        [jfCode, ...SKU_SUFFIXES.map(s => jfCode + s)].map(s => s.toUpperCase())
    );
    const isMatch = (sku) => skusToCheck.has(sku);

    const res = await mintsoftClient.get('/Product/Search', {
        params: {
            ...mintsoftClient.defaults.params,
            Search: jfCode,
            IncludeBundles: false,
            IncludeDiscontinued: false
        }
    });

    const data = res?.data;
    if (!data) throw new Error(`No response data for JF code "${jfCode}"`);

    const results = Array.isArray(data) ? data : [data];
    const matched = results.filter(p => p?.ID && p.SKU && isMatch(p.SKU.toUpperCase()));

    if (matched.length === 0) return [];

    return matched.map(p => ({ productId: p.ID, sku: p.SKU || '' }));
}

async function getProductStock(productId) {

    const res = await mintsoftClient.get('/Product/StockLevels', {
        params: {
            ...mintsoftClient.defaults.params,
            ProductId: productId,
            Breakdown: true
        }
    });

    if (!res.data) throw new Error(`No stock data for Product ID ${productId}`);

    const records = Array.isArray(res.data) ? res.data : [res.data];
    if (records.length === 0) throw new Error(`Empty stock response for Product ID ${productId}`);

    return records.map(record => {
        const warehouseId     = record.WarehouseId ?? 0;
        const totalStock      = record.TotalStockLevel ?? record.Level ?? 0;
        const availableLevel  = record.Level ?? 0;
        const breakdown       = Array.isArray(record.Breakdown) ? record.Breakdown : [];

        const allocated = totalStock - availableLevel;
        const quarantine = breakdown
            .filter(b => b.Type?.toLowerCase() === 'quarantine')
            .reduce((sum, b) => sum + (b.Quantity ?? 0), 0);

        // Floor at 0 — when quarantine exceeds the free pool (e.g. all stock
        // allocated to orders AND some held in quarantine), the raw subtraction
        // goes negative. Negative "available" is meaningless to consumers
        // and historically rendered as a bug-looking figure in the UI.
        const available = Math.max(0, availableLevel - quarantine);
        return { warehouseId, stockLevel: totalStock, available, allocated, quarantine };
    });
}

async function getProductDetails(productId) {
    const res = await mintsoftClient.get(`/Product/${productId}`, {
        params: { ...mintsoftClient.defaults.params }
    });

    if (!res.data) throw new Error(`No details for Product ID ${productId}`);

    const p = res.data;
    return {
        productId: p.ID,
        sku: p.SKU || '',
        weight: p.Weight ?? 0,
        height: p.Height ?? 0,
        width: p.Width ?? 0,
        depth: p.Depth ?? 0,
        cartonQty: p.CartonQuantity ?? 0,
    };
}

async function getProductCartons(productId) {
    const res = await mintsoftClient.get(`/Product/${productId}/Cartons`, {
        params: { ...mintsoftClient.defaults.params }
    });
    if (!res.data) return [];
    return Array.isArray(res.data) ? res.data : [res.data];
}

async function getWarehouses() {
    const res = await mintsoftClient.get('/Warehouse');
    if (!res.data) return [];
    const list = Array.isArray(res.data) ? res.data : [res.data];
    return list.map(w => ({
        warehouseId: w.ID,
        name: w.Name || '',
        code: w.Code || '',
        active: !!w.Active,
    }));
}

// Fetch a single product's image URL via GET /Product/{id}.
//
// The Mintsoft API sits behind Cloudflare, which IP-rate-limits with HTTP 429
// (error 1015) and an unhelpful "Retry-After: 0" header — so we ignore that
// header and back off on a fixed schedule instead. Callers are responsible for
// pacing the overall request rate (see tools/product-images.js); this just
// rides out a transient throttle rather than aborting.
async function getProductImageUrl(productId) {
    const sleep = (ms) => new Promise(r => setTimeout(r, ms));
    for (let attempt = 0; ; attempt++) {
        try {
            const res = await mintsoftClient.get(`/Product/${productId}`, {
                params: { ...mintsoftClient.defaults.params },
            });
            // A throttled response is HTML, not a product object — treat a
            // missing/!object body as a retryable miss rather than a value.
            if (res.data && typeof res.data === 'object') {
                // Mintsoft sometimes stores ImageURL with leading/trailing whitespace.
                return (res.data.ImageURL || '').trim();
            }
            if (attempt >= 6) throw new Error(`No product object for ID ${productId}`);
        } catch (err) {
            const status = err.response?.status;
            if (status !== 429 || attempt >= 6) throw err;
        }
        await sleep(Math.min(60000, 5000 * 2 ** attempt));
    }
}

async function getLocations(warehouseId) {
    if (!warehouseId) throw new Error('warehouseId is required');
    const res = await mintsoftClient.get(`/Warehouse/${warehouseId}/Location/All`, {
        params: { ...mintsoftClient.defaults.params, IncludeUnAssigned: true },
    });
    if (!res.data) return [];
    const list = Array.isArray(res.data) ? res.data : [res.data];
    return list.map(l => ({
        locationId: l.ID,
        warehouseId: l.WarehouseId,
        name: l.Name || '',
        locationName: l.LocationName || l.Name || '',
        pickSequence: l.PickSequence ?? null,
        locationTypeId: l.LocationTypeId ?? null,
    }));
}

// Names from GET /ASN/GoodsInTypes — Mintsoft expects the string name on
// PUT /ASN, otherwise the UI renders the raw enum ordinal.
const GOODS_IN_TYPE_NAMES = [
    'TwentyFtContainer',
    'FortyFtContainer',
    'Pallet',
    'Carton',
    'FortyFtContainerHC',
    'FortyFiveFtContainer',
    'FortyFiveFtContainerHC',
];

async function createAsn({ warehouseId, poReference, supplier, quantity, items, goodsInType = 3 }) {
    const goodsInTypeName = typeof goodsInType === 'string'
        ? goodsInType
        : GOODS_IN_TYPE_NAMES[goodsInType];
    if (!goodsInTypeName) throw new Error(`Unknown goodsInType: ${goodsInType}`);

    const body = {
        WarehouseId: warehouseId,
        POReference: poReference || '',
        Supplier: supplier || '',
        Quantity: quantity,
        GoodsInType: goodsInTypeName,
        EstimatedDelivery: new Date().toISOString(),
        Items: items.map(it => ({
            ProductId: it.productId,
            SKU: it.sku || '',
            Quantity: it.quantity,
        })),
    };
    // Admin API keys must scope ASNs to a client.
    if (MINTSOFT_CLIENT_ID) body.ClientId = MINTSOFT_CLIENT_ID;

    // PUT /ASN returns a ToolkitResult: { ID, Success, Message, ... }
    // The full ASN (with Items[].ID) only comes back from GET /ASN/{id}.
    const createRes = await mintsoftClient.put('/ASN', body);
    const result = createRes?.data;
    if (!result) throw new Error('Mintsoft ASN creation returned no data');
    if (result.Success === false) {
        throw new Error(`Mintsoft ASN creation rejected: ${result.Message || 'no message'}`);
    }
    const asnId = result.ID;
    if (!asnId) throw new Error('Mintsoft ASN creation returned no ID');

    // ASNs land in `Draft` state, which the main UI list filters out.
    // /Confirm promotes them to `Confirmed`/`Awaiting Delivery` so they're
    // visible and ready for goods-in. Always required — a Draft ASN can't
    // be received against, and leaving one behind clutters the system.
    const confirmRes = await mintsoftClient.get(`/ASN/${asnId}/Confirm`);
    if (confirmRes?.data?.Success === false) {
        throw new Error(`Mintsoft ASN ${asnId} confirm rejected: ${confirmRes.data.Message || 'no message'}`);
    }

    // Fetch the persisted ASN so we have the per-item IDs needed for /Items/Receive
    const detailRes = await mintsoftClient.get(`/ASN/${asnId}`);
    if (!detailRes?.data) throw new Error(`Mintsoft ASN ${asnId} fetch returned no data`);
    return detailRes.data;
}

// ── Stock movements (the UI's "Book Stock" form) ───────────────────────────
// POST /Warehouse/StockMovement?Action=<n> with a BookStockRequest body. The
// Action enum is 20 unnamed integers in Mintsoft's swagger; these are the ones
// we use, identified empirically on 2026-07-29 (the API echoes the action name
// in its response Message — full map in the commit that added this).
//
// Quarantining moves units OUT of the sellable pool: StockLevel and OnHand both
// drop by the quantity and InQuarantine rises by it, which is what
// getProductStock() already nets out via `available = level - quarantine`.
const STOCK_ACTION = { QUARANTINE: 7, UN_QUARANTINE: 8 };

// BatchNo AND ExpiryDate together identify WHICH units to move — Mintsoft holds
// stock as batch+expiry storage items, so sending the batch alone finds nothing
// when the units carry an expiry and the movement is refused with "not enough
// could be found in the selected location". Live-verified 2026-07-29 against
// ASN-received stock: same product, location and batch, refused without
// ExpiryDate, accepted with it. Both must mirror what the goods-in used.
async function moveStock(action, { productId, warehouseId, locationId, quantity, batchNo, expiryDate, comment }) {
    const res = await mintsoftClient.post('/Warehouse/StockMovement', {
        ProductId: productId,
        WarehouseId: warehouseId,
        LocationId: locationId,
        Quantity: quantity,
        BatchNo: batchNo || null,
        ExpiryDate: expiryDate || null,
        Comment: comment || '',
    }, { params: { ...mintsoftClient.defaults.params, Action: action } });

    const result = res?.data;
    // Mintsoft answers 200 with Success:false for domain failures (e.g. "not
    // enough stock in the selected location"), so the status code alone is not
    // enough to tell whether the units actually moved.
    if (!result || result.Success === false) {
        throw new Error(`Mintsoft stock movement (Action ${action}) rejected: ${result?.Message || 'no message'}`);
    }
    return result;
}

const quarantineStock = (params) => moveStock(STOCK_ACTION.QUARANTINE, params);
const unQuarantineStock = (params) => moveStock(STOCK_ACTION.UN_QUARANTINE, params);

async function receiveAsnItems(asnId, allocations) {
    // Complete: false records the allocation without auto-promoting to DELIVERED.
    // Then MarkAwaitingPutAway (→ AWAITINGPUTAWAY) and MarkPutAwayComplete
    // (→ BOOKEDIN) walk the ASN to BOOKEDIN. Verified empirically — none of
    // /BookIn, /BookInPartial, /PartBook land at BOOKEDIN; /BookIn jumps to
    // DELIVERED, /BookInPartial → BOOKEDIN-PARTIAL, /PartBook → PARTIALLYBOOKED.
    const body = allocations.map(a => ({
        ASNItemId: a.asnItemId,
        ProductId: a.productId,
        Quantity: a.quantity,
        LocationId: a.locationId,
        BatchNo: a.batchNo || null,
        ExpiryDate: a.expiryDate || null,
        Complete: false,
    }));

    const res = await mintsoftClient.post(`/ASN/${asnId}/Items/Receive`, body);

    const markRes = await mintsoftClient.get(`/ASN/${asnId}/MarkAwaitingPutAway`);
    if (markRes?.data?.Success === false) {
        throw new Error(`Mintsoft ASN ${asnId} MarkAwaitingPutAway rejected: ${markRes.data.Message || 'no message'}`);
    }

    const completeRes = await mintsoftClient.get(`/ASN/${asnId}/MarkPutAwayComplete`);
    if (completeRes?.data?.Success === false) {
        throw new Error(`Mintsoft ASN ${asnId} MarkPutAwayComplete rejected: ${completeRes.data.Message || 'no message'}`);
    }

    return res.data;
}

module.exports = { getProductsByJfCode, getProductStock, getProductDetails, getProductCartons, createAsn, receiveAsnItems, getWarehouses, getLocations, getProductImageUrl, quarantineStock, unQuarantineStock, STOCK_ACTION };