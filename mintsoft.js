const axios = require('axios');
const log = require('./logger');
require('dotenv').config();

const MINTSOFT_API_KEY = process.env.MINTSOFT_API_KEY;

if (!MINTSOFT_API_KEY) {
    log.warn('MINTSOFT_API_KEY is not configured.');
}

const mintsoftClient = axios.create({
    baseURL: 'https://api.mintsoft.co.uk/api',
    timeout: 15000,
    headers: { 'Content-Type': 'application/json' },
    params: { APIKey: MINTSOFT_API_KEY }
});

const SKU_SUFFIXES = ['_QC', '_READY', '_pp','_LABELLED','_PACKED','_IFU','_FRANCE',''];

async function getProductsByJfCode(jfCode) {
    if (!jfCode) throw new Error('JF code is required');

    const skusToCheck = [jfCode, ...SKU_SUFFIXES.map(s => jfCode + s)];

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
    const matched = results.filter(p => p?.ID && skusToCheck.includes(p.SKU));

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

        const available = availableLevel - quarantine;
        return { warehouseId, stockLevel: totalStock, available, allocated, quarantine };
    });
}

module.exports = { getProductsByJfCode, getProductStock };