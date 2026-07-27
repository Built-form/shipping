'use strict';
// READ-ONLY impact analysis for the proposed FNSKU-level merge fix.
//
// Replays writeSnapshots' country-snapshot aggregation across EVERY ASIN in
// today's reports, twice:
//   OLD = current code (API fallback skips any ASIN the health report saw)
//   NEW = proposed fix (merge API-only local FNSKU pools onto the ASIN)
// and prints only the ASINs where the two differ, with per-field deltas.
// Writes nothing to the DB.
//
//   node tools/sim-fnsku-merge.js            # all accounts/countries
//   node tools/sim-fnsku-merge.js JFA        # one account
//   node tools/sim-fnsku-merge.js JFA UK     # one account+country
require('dotenv').config();
const { getPool, closePool } = require('../src/db');
const S = require('../src/services/amazon-stock-shared');

const onlyAccount = process.argv[2];
const onlyCountry = (process.argv[3] || '').toUpperCase();
const num = v => parseInt(v || '0', 10) || 0;
const FIELDS = ['fulfillable', 'inbound_working', 'inbound_shipped', 'inbound_receiving', 'reserved'];

// Faithful replica of writeSnapshots' byAsin aggregation. `mode` flips only the
// API-fallback merge rule; the health branch + locality + echo-drop are identical.
function aggregate(mode, { countryCode, listingRows, reportRows, apiSummaries, deFnskusByAsin }) {
    const activeAsins = new Set(), activeSkus = new Set();
    const skuToAsin = new Map();
    for (const item of apiSummaries) {
        const sku = (item.sellerSku || '').trim(), asin = (item.asin || '').trim();
        if (sku && asin && !skuToAsin.has(sku)) skuToAsin.set(sku, asin);
    }
    const apiReservedByFnsku = new Map();
    for (const item of apiSummaries) {
        const fnsku = (item.fnSku || '').trim();
        if (!fnsku || apiReservedByFnsku.has(fnsku)) continue;
        const r = item.inventoryDetails?.reservedQuantity?.totalReservedQuantity;
        if (typeof r === 'number') apiReservedByFnsku.set(fnsku, r);
    }
    for (const row of listingRows) {
        const t = String(row['product-id-type'] || '').trim();
        const sku = (row['seller-sku'] || row['sku'] || '').trim();
        let asin = (row['asin1'] || row['asin'] || (t === '1' ? row['product-id'] : '') || '').trim();
        if (!asin && sku) asin = skuToAsin.get(sku) || '';
        if (!asin) continue;
        activeAsins.add(asin);
        if (sku) activeSkus.add(sku);
    }

    const byAsin = {};
    // ── Health branch (identical in both modes) ──
    for (const row of reportRows) {
        const asin = row['asin'] || '';
        if (!asin || !activeAsins.has(asin)) continue;
        const fnsku = (row['fnsku'] || '').trim(), sku = row['sku'] || '';
        if (!activeSkus.has(sku)) continue;
        if (S.isPanEuPooled(countryCode) && fnsku && deFnskusByAsin?.get(asin)?.has(fnsku)) continue;
        if (!byAsin[asin]) byAsin[asin] = mkRow(true);
        const a = byAsin[asin];
        const qtyKey = fnsku ? `f:${fnsku}` : (sku ? `s:${sku}` : `r:${a.seen_qty_keys.size}`);
        if (!a.seen_qty_keys.has(qtyKey)) {
            a.seen_qty_keys.add(qtyKey);
            a.fulfillable       += num(row['available']);
            a.inbound_working   += num(row['inbound-working']);
            a.inbound_shipped   += num(row['inbound-shipped']);
            a.inbound_receiving += num(row['inbound-received']);
            const total = num(row['Total Reserved Quantity']);
            const comp = num(row['Reserved Customer Order']) + num(row['Reserved FC Transfer']) + num(row['Reserved FC Processing']);
            const live = fnsku ? apiReservedByFnsku.get(fnsku) : undefined;
            a.reserved += Math.max(total, comp, live || 0);
        }
        if (fnsku) a.seen_fnskus.add(fnsku);
        if (sku) a.seen_skus.add(sku);
    }

    // ── API fallback branch ──
    for (const item of apiSummaries) {
        const asin = item.asin || '';
        if (!asin) continue;
        if (mode === 'old' && byAsin[asin] && byAsin[asin]._fromHealth) continue; // OLD all-or-nothing
        if (!activeAsins.has(asin)) continue;
        const sku = (item.sellerSku || '').trim();
        if (!sku || !activeSkus.has(sku)) continue;
        const fnsku = (item.fnSku || '').trim();
        if (S.isPanEuPooled(countryCode) && fnsku && deFnskusByAsin?.get(asin)?.has(fnsku)) continue;
        const d = item.inventoryDetails || {};
        if (!byAsin[asin]) byAsin[asin] = mkRow(false);
        const a = byAsin[asin];
        if (mode === 'old') {
            if (fnsku && !a.seen_fnskus.has(fnsku)) {
                a.seen_fnskus.add(fnsku);
                a.fulfillable       += d.fulfillableQuantity || 0;
                a.inbound_working   += d.inboundWorkingQuantity || 0;
                a.inbound_shipped   += d.inboundShippedQuantity || 0;
                a.inbound_receiving += d.inboundReceivingQuantity || 0;
                a.reserved          += d.reservedQuantity?.totalReservedQuantity || 0;
            }
        } else {
            const qtyKey = fnsku ? `f:${fnsku}` : (sku ? `s:${sku}` : null);
            if (qtyKey && !a.seen_qty_keys.has(qtyKey)) {
                a.seen_qty_keys.add(qtyKey);
                a.fulfillable       += d.fulfillableQuantity || 0;
                a.inbound_working   += d.inboundWorkingQuantity || 0;
                a.inbound_shipped   += d.inboundShippedQuantity || 0;
                a.inbound_receiving += d.inboundReceivingQuantity || 0;
                a.reserved          += d.reservedQuantity?.totalReservedQuantity || 0;
            }
            if (fnsku) a.seen_fnskus.add(fnsku);
        }
        if (sku) a.seen_skus.add(sku);
    }
    return byAsin;
}
function mkRow(fromHealth) {
    return { _fromHealth: fromHealth, seen_qty_keys: new Set(), seen_fnskus: new Set(), seen_skus: new Set(),
        fulfillable: 0, inbound_working: 0, inbound_shipped: 0, inbound_receiving: 0, reserved: 0 };
}

async function loadReports(conn, account, country) {
    const [jobs] = await conn.query(
        `SELECT report_type, document_id FROM amazon_report_jobs
         WHERE batch_date = CURDATE() AND account = ? AND country = ?
           AND report_type IN ('active_listings','health') AND document_id IS NOT NULL`,
        [account, country]
    );
    const active = jobs.find(j => j.report_type === 'active_listings')?.document_id;
    const health = jobs.find(j => j.report_type === 'health')?.document_id;
    if (!active || !health) return null;
    const tm = S.getTokenManager(account, S.getRegionForCountry(country));
    const ep = S.getEndpointForCountry(country);
    const listingRows = S.parseTsvReport(await S.downloadReport(tm, ep, active));
    const reportRows = S.parseTsvReport(await S.downloadReport(tm, ep, health));
    const { marketplaceId } = S.getMarketplace(country);
    const apiSummaries = await S.fetchInventorySummaries(tm, ep, marketplaceId, `${account}/${country}`, account);
    return { listingRows, reportRows, apiSummaries };
}

(async () => {
    const conn = await getPool().getConnection();
    const summary = [];
    let totalDelta = { asins: 0 };
    for (const f of FIELDS) totalDelta[f] = 0;
    try {
        for (const account of S.ACCOUNT_NAMES) {
            if (onlyAccount && account !== onlyAccount) continue;
            const countries = S.getAccountCountries(account);
            // Process DE first to build the echo-drop set for FR/ES/IT.
            const ordered = [...countries].sort((a, b) => (a === 'DE' ? -1 : b === 'DE' ? 1 : 0));
            const deFnskusByAsin = new Map();
            for (const country of ordered) {
                if (onlyCountry && country !== onlyCountry && country !== 'DE') continue;
                let data;
                try { data = await loadReports(conn, account, country); }
                catch (e) { console.log(`  ${account}/${country}: load error ${e.message}`); continue; }
                if (!data) { continue; }

                const oldB = aggregate('old', { countryCode: country, deFnskusByAsin, ...data });
                const newB = aggregate('new', { countryCode: country, deFnskusByAsin, ...data });

                if (country === 'DE') { // record DE's counted FNSKUs for echo drop
                    for (const [asin, a] of Object.entries(newB)) {
                        if (a.seen_fnskus.size) deFnskusByAsin.set(asin, new Set(a.seen_fnskus));
                    }
                }
                if (onlyCountry && country !== onlyCountry) continue; // DE only needed for the set

                const asins = new Set([...Object.keys(oldB), ...Object.keys(newB)]);
                for (const asin of asins) {
                    const o = oldB[asin] || mkRow(false), n = newB[asin] || mkRow(false);
                    const delta = {}; let changed = false;
                    for (const f of FIELDS) { delta[f] = n[f] - o[f]; if (delta[f] !== 0) changed = true; }
                    if (!changed) continue;
                    summary.push({ account, country, asin, delta,
                        addedFnskus: [...n.seen_fnskus].filter(x => !o.seen_fnskus.has(x)) });
                    totalDelta.asins++;
                    for (const f of FIELDS) totalDelta[f] += delta[f];
                }
            }
        }
    } finally { conn.release(); await closePool(); }

    summary.sort((a, b) => (b.delta.inbound_shipped + b.delta.inbound_working + b.delta.inbound_receiving)
                         - (a.delta.inbound_shipped + a.delta.inbound_working + a.delta.inbound_receiving));
    console.log(`\n==== ASINs whose snapshot changes under the fix: ${summary.length} ====\n`);
    for (const s of summary) {
        const d = s.delta;
        const parts = FIELDS.filter(f => d[f] !== 0).map(f => `${f} ${d[f] > 0 ? '+' : ''}${d[f]}`);
        console.log(`${s.account}/${s.country} ${s.asin}  ${parts.join('  ')}   +pool[${s.addedFnskus.join(',')}]`);
    }
    console.log(`\n==== Totals across all changed ASINs ====`);
    console.log(`  ASINs changed: ${totalDelta.asins}`);
    for (const f of FIELDS) if (totalDelta[f] !== 0) console.log(`  ${f}: ${totalDelta[f] > 0 ? '+' : ''}${totalDelta[f]}`);
})().catch(e => { console.error(e); process.exit(1); });
