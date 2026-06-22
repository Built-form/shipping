'use strict';
// Probe one ASIN across every country's downloaded reports + the live inventory
// summaries API, to see where Amazon thinks the physical stock actually is.
//
//   node tools/probe-asin.js Hangerworld B001AG78X0

require('dotenv').config();
const { getPool, closePool } = require('../src/db');
const {
    getEndpoint, getTokenManager, getMarketplace,
    downloadReport, parseTsvReport, fetchInventorySummaries,
} = require('../src/services/amazon-stock-shared');

const account = process.argv[2];
const asin = (process.argv[3] || '').toUpperCase();
if (!account || !asin) {
    console.error('usage: node tools/probe-asin.js <account> <ASIN>');
    process.exit(1);
}

const COUNTRIES = ['UK', 'DE', 'FR', 'ES', 'IT'];

(async () => {
    const conn = await getPool().getConnection();
    let jobs;
    try {
        const [rows] = await conn.query(
            `SELECT country, report_type, document_id
             FROM amazon_report_jobs
             WHERE batch_date = CURDATE() AND account = ?
               AND country IN ('UK','DE','FR','ES','IT')
               AND report_type IN ('active_listings','health')
               AND document_id IS NOT NULL`,
            [account]
        );
        jobs = rows;
    } finally { conn.release(); }

    const tm = getTokenManager(account);
    const endpoint = getEndpoint();

    for (const country of COUNTRIES) {
        console.log(`\n========== ${country} ==========`);

        const active = jobs.find(j => j.country === country && j.report_type === 'active_listings');
        if (active) {
            const tsv = await downloadReport(tm, endpoint, active.document_id);
            const parsed = parseTsvReport(tsv);
            const hits = parsed.filter(r => {
                const a1 = (r['asin1'] || r['asin'] || '').trim();
                const pid = (r['product-id'] || '').trim();
                return a1 === asin || pid === asin;
            });
            console.log(`active_listings rows for ${asin}: ${hits.length}`);
            for (const h of hits) {
                console.log(`  sku=${h['seller-sku'] || h['sku'] || ''} fnsku=${h['fulfillment-channel-sku'] || h['fnsku'] || ''} qty=${h['quantity'] || ''} price=${h['price'] || ''} status=${h['status'] || ''}`);
            }
        }

        const health = jobs.find(j => j.country === country && j.report_type === 'health');
        if (health) {
            const tsv = await downloadReport(tm, endpoint, health.document_id);
            const parsed = parseTsvReport(tsv);
            const hits = parsed.filter(r => (r['asin'] || '').trim() === asin);
            console.log(`health rows for ${asin}: ${hits.length}`);
            for (const h of hits) {
                console.log(`  sku=${h['sku']} fnsku=${h['fnsku']} available=${h['available']} reserved=${h['Total Reserved Quantity']} ibw=${h['inbound-working']} ibs=${h['inbound-shipped']} ibr=${h['inbound-received']}`);
            }
        }

        // Live API summaries for this marketplace
        try {
            const { marketplaceId } = getMarketplace(country);
            const summaries = await fetchInventorySummaries(tm, endpoint, marketplaceId, `${account}/${country}`, account);
            const hits = summaries.filter(s => (s.asin || '').trim() === asin);
            console.log(`API summaries (marketplace=${country}) rows for ${asin}: ${hits.length}`);
            for (const h of hits) {
                const d = h.inventoryDetails || {};
                console.log(`  sellerSku=${h.sellerSku} fnSku=${h.fnSku} ful=${d.fulfillableQuantity} res=${d.reservedQuantity?.totalReservedQuantity} ibw=${d.inboundWorkingQuantity} ibs=${d.inboundShippedQuantity} ibr=${d.inboundReceivingQuantity} cond=${h.condition}`);
            }
        } catch (err) {
            console.log(`API summaries error: ${err.message}`);
        }
    }

    await closePool();
})().catch(err => { console.error(err); process.exit(1); });
