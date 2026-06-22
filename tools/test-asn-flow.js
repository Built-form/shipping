'use strict';
// Walks an ASN through the create -> confirm -> receive -> book-in flow and
// prints the Mintsoft status after each step. Useful for figuring out which
// endpoint lands the ASN at the desired status (e.g. BOOKEDIN).
//
// Usage:
//   node tools/test-asn-flow.js <jfCode> <warehouseId> <locationId> <qty> [--step=BookIn|BookInPartial|PartBook|MarkAwaitingPutAway|MarkPutAwayComplete|none] [--complete=true|false]
//
// Examples:
//   node tools/test-asn-flow.js JF1112 1 123 1
//   node tools/test-asn-flow.js JF1112 1 123 1 --step=BookIn --complete=false
//   node tools/test-asn-flow.js JF1112 1 123 1 --step=none --complete=true

require('dotenv').config();
const axios = require('axios');

const MINTSOFT_API_KEY = process.env.MINTSOFT_API_KEY;
const MINTSOFT_CLIENT_ID = process.env.MINTSOFT_CLIENT_ID
    ? Number(process.env.MINTSOFT_CLIENT_ID)
    : null;

if (!MINTSOFT_API_KEY) {
    console.error('MINTSOFT_API_KEY is not configured.');
    process.exit(1);
}

const client = axios.create({
    baseURL: 'https://api.mintsoft.co.uk/api',
    timeout: 20000,
    headers: { 'Content-Type': 'application/json' },
    params: { APIKey: MINTSOFT_API_KEY },
});

const positional = process.argv.slice(2).filter(a => !a.startsWith('--'));
const flags = Object.fromEntries(
    process.argv.slice(2)
        .filter(a => a.startsWith('--'))
        .map(a => a.replace(/^--/, '').split('='))
);

const [jfCode, warehouseIdRaw, locationIdRaw, qtyRaw] = positional;
if (!jfCode || !warehouseIdRaw || !locationIdRaw || !qtyRaw) {
    console.error('Usage: node tools/test-asn-flow.js <jfCode> <warehouseId> <locationId> <qty> [--step=...] [--complete=...]');
    process.exit(1);
}

const warehouseId = Number(warehouseIdRaw);
const locationId = Number(locationIdRaw);
const qty = Number(qtyRaw);
const step = flags.step ?? 'BookIn';
const completeFlag = flags.complete === 'true';

const VALID_STEPS = ['BookIn', 'BookInPartial', 'PartBook', 'MarkAwaitingPutAway', 'MarkPutAwayComplete', 'none'];
const stepList = step === 'none' ? [] : step.split(',');
for (const s of stepList) {
    if (!VALID_STEPS.includes(s)) {
        console.error(`--step entries must be one of: ${VALID_STEPS.join(', ')}`);
        process.exit(1);
    }
}

async function fetchStatus(asnId) {
    const res = await client.get(`/ASN/${asnId}`);
    const s = res.data?.ASNStatus?.Name ?? `(unknown, statusId=${res.data?.ASNStatusId})`;
    return s;
}

async function findProduct(code) {
    const suffixes = ['_QC', '_READY', '_pp', '_LABELLED', '_PACKED', '_IFU', '_FRANCE', ''];
    const skus = [code, ...suffixes.map(s => code + s)];
    const res = await client.get('/Product/Search', {
        params: { ...client.defaults.params, Search: code, IncludeBundles: false, IncludeDiscontinued: false },
    });
    const list = Array.isArray(res.data) ? res.data : [res.data];
    const matched = list.find(p => p?.ID && skus.includes(p.SKU));
    if (!matched) throw new Error(`No Mintsoft product matches JF code "${code}"`);
    return { productId: matched.ID, sku: matched.SKU };
}

(async () => {
    console.log(`[test-asn] jfCode=${jfCode} warehouseId=${warehouseId} locationId=${locationId} qty=${qty} step=${step} complete=${completeFlag}`);

    const { productId, sku } = await findProduct(jfCode);
    console.log(`[test-asn] resolved productId=${productId} sku=${sku}`);

    // 1. Create ASN
    const createBody = {
        WarehouseId: warehouseId,
        POReference: `TEST-${jfCode}-${Date.now()}`,
        Supplier: 'asn-flow-test',
        Quantity: qty,
        GoodsInType: 'Carton',
        EstimatedDelivery: new Date().toISOString(),
        Items: [{ ProductId: productId, SKU: sku, Quantity: qty }],
    };
    if (MINTSOFT_CLIENT_ID) createBody.ClientId = MINTSOFT_CLIENT_ID;
    const createRes = await client.put('/ASN', createBody);
    if (createRes.data?.Success === false) {
        throw new Error(`Create rejected: ${createRes.data.Message}`);
    }
    const asnId = createRes.data.ID;
    console.log(`[test-asn] 1. created ASN ${asnId}                  status=${await fetchStatus(asnId)}`);

    // 2. Confirm
    await client.get(`/ASN/${asnId}/Confirm`);
    console.log(`[test-asn] 2. /Confirm                              status=${await fetchStatus(asnId)}`);

    // 3. Receive
    const detail = await client.get(`/ASN/${asnId}`);
    const asnItemId = detail.data.Items?.[0]?.ID;
    if (!asnItemId) throw new Error('Could not find Items[0].ID on freshly created ASN');
    const recvRes = await client.post(`/ASN/${asnId}/Items/Receive`, [{
        ASNItemId: asnItemId,
        ProductId: productId,
        Quantity: qty,
        LocationId: locationId,
        BatchNo: null,
        ExpiryDate: null,
        Complete: completeFlag,
    }]);
    if (recvRes.data?.Success === false) {
        throw new Error(`Receive rejected: ${recvRes.data.Message}`);
    }
    console.log(`[test-asn] 3. /Items/Receive Complete=${completeFlag}        status=${await fetchStatus(asnId)}`);

    // 4..N. Optional book-in steps (chained, comma-separated)
    let n = 4;
    for (const s of stepList) {
        const bookRes = await client.get(`/ASN/${asnId}/${s}`);
        if (bookRes.data?.Success === false) {
            throw new Error(`/${s} rejected: ${bookRes.data.Message}`);
        }
        console.log(`[test-asn] ${n}. /${s.padEnd(20)}              status=${await fetchStatus(asnId)}`);
        n++;
    }

    console.log(`[test-asn] done. ASN ${asnId} final status=${await fetchStatus(asnId)}`);
})().catch(err => {
    console.error('[test-asn] ERROR:', err.response?.data ?? err.message);
    process.exit(1);
});
