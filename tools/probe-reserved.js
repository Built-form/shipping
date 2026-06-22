'use strict';
// Investigate whether our captured `reserved` includes the FC-transfer
// (transshipment) portion. Compares, for one SKU:
//   - the raw HEALTH report row (every reserved/transship/pending column)
//   - the live FBA Inventory API reservedQuantity breakdown
//   - what we stored in amazon_stock_country_snapshots
//
//   node tools/probe-reserved.js COT1-ADPT-15X15-10
//   node tools/probe-reserved.js COT1-ADPT-15X15-10 JFA UK
require('dotenv').config();
const { getPool, closePool } = require('../src/db');
const {
    getEndpointForCountry, getRegionForCountry, getTokenManager, getMarketplace,
    downloadReport, parseTsvReport, fetchInventorySummaries,
} = require('../src/services/amazon-stock-shared');

const targetSku = (process.argv[2] || '').trim();
let account = process.argv[3] || null;
let country = (process.argv[4] || '').toUpperCase() || null;
if (!targetSku) {
    console.error('usage: node tools/probe-reserved.js <SKU> [account] [country]');
    process.exit(1);
}

(async () => {
    const conn = await getPool().getConnection();
    let asin, dateRan;
    try {
        // Locate the SKU in active_listings (latest date) to resolve asin/account/country.
        const [loc] = await conn.query(
            `SELECT al.asin, al.company, al.country, al.date_ran, al.fnsku
             FROM amazon_active_listings al
             JOIN (SELECT MAX(date_ran) d FROM amazon_active_listings WHERE sku = ?) m
               ON al.date_ran = m.d
             WHERE al.sku = ?
             ${account ? 'AND al.company = ?' : ''}
             ${country ? 'AND al.country = ?' : ''}
             ORDER BY al.country`,
            account && country ? [targetSku, targetSku, account, country]
              : account ? [targetSku, targetSku, account]
              : country ? [targetSku, targetSku, country]
              : [targetSku, targetSku]
        );
        if (loc.length === 0) {
            console.error(`No active_listings row found for sku=${targetSku}`);
            process.exit(2);
        }
        console.log('Listing matches:');
        for (const r of loc) {
            console.log(`  company=${r.company} country=${r.country} asin=${r.asin} fnsku=${r.fnsku} date=${String(r.date_ran).slice(0,10)}`);
        }
        const pick = loc[0];
        asin = pick.asin; account = pick.company; country = pick.country; dateRan = String(pick.date_ran).slice(0,10);
        console.log(`\nUsing → account=${account} country=${country} asin=${asin}\n`);

        // What we stored.
        const [snap] = await conn.query(
            `SELECT date_ran, sku, fnsku, fulfillable, reserved,
                    inbound_working, inbound_shipped, inbound_receiving
             FROM amazon_stock_country_snapshots
             WHERE asin = ? AND company = ? AND country = ?
             ORDER BY date_ran DESC LIMIT 3`,
            [asin, account, country]
        );
        console.log('Stored country snapshots (latest 3):');
        for (const r of snap) {
            console.log(`  ${String(r.date_ran).slice(0,10)} sku=${r.sku} fnsku=${r.fnsku} ful=${r.fulfillable} reserved=${r.reserved} ibw=${r.inbound_working} ibs=${r.inbound_shipped} ibr=${r.inbound_receiving}`);
        }

        // Health report doc id for today (or latest available).
        const [jobs] = await conn.query(
            `SELECT document_id, batch_date FROM amazon_report_jobs
             WHERE account = ? AND country = ? AND report_type = 'health'
               AND document_id IS NOT NULL
             ORDER BY batch_date DESC LIMIT 1`,
            [account, country]
        );
        var healthDoc = jobs[0]?.document_id || null;
        var healthDate = jobs[0]?.batch_date ? String(jobs[0].batch_date).slice(0,10) : null;
    } finally { conn.release(); }

    const endpoint = getEndpointForCountry(country);
    const tm = getTokenManager(account, getRegionForCountry(country));

    // ── Health report: dump every reserved/transship/pending column ──
    if (healthDoc) {
        console.log(`\n===== HEALTH report (${healthDate}, doc=${healthDoc}) =====`);
        const tsv = await downloadReport(tm, endpoint, healthDoc);
        const rows = parseTsvReport(tsv);
        const hits = rows.filter(r => (r['asin'] || '').trim() === asin);
        console.log(`rows for ${asin}: ${hits.length}`);
        if (hits.length) {
            const allKeys = Object.keys(hits[0]);
            const relKeys = allKeys.filter(k => /reserv|transship|pending|fc.?proc|available/i.test(k));
            console.log(`(reserved/transship/pending/available columns present: ${JSON.stringify(relKeys)})`);
            for (const h of hits) {
                const view = {};
                for (const k of relKeys) view[k] = h[k];
                console.log(`  sku=${h['sku']} fnsku=${h['fnsku']} → ${JSON.stringify(view)}`);
            }
        }
    } else {
        console.log('\n(no health report document on file)');
    }

    // ── Live API: full reservedQuantity breakdown ──
    console.log(`\n===== LIVE FBA Inventory API (marketplace=${country}) =====`);
    const { marketplaceId } = getMarketplace(country);
    const summaries = await fetchInventorySummaries(tm, endpoint, marketplaceId, `${account}/${country}`, account);
    const hits = summaries.filter(s => (s.asin || '').trim() === asin);
    console.log(`rows for ${asin}: ${hits.length}`);
    for (const h of hits) {
        const d = h.inventoryDetails || {};
        const rq = d.reservedQuantity || {};
        console.log(`  sellerSku=${h.sellerSku} fnSku=${h.fnSku} ful=${d.fulfillableQuantity}`);
        console.log(`    reserved: total=${rq.totalReservedQuantity} customerOrder=${rq.pendingCustomerOrderQuantity} transshipment(FC transfer)=${rq.pendingTransshipmentQuantity} fcProcessing=${rq.fcProcessingQuantity}`);
    }

    await closePool();
})().catch(err => { console.error(err); process.exit(1); });
