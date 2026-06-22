'use strict';
// READ-ONLY simulation. Replays the writeSnapshots dedup for one ASIN in one
// pooled country (FR/ES/IT) under three scenarios, to test whether "updating
// the pan_eu cache" actually restores the zeroed EU stock — WITHOUT writing
// anything to the DB.
//
//   A  current        — ASIN is in the pan_eu cache → FNSKU-alone dedup
//   B  cache updated   — ASIN removed from pan_eu    → needs BOTH sku AND fnsku
//   C  locality fix    — count only this country's own pool; drop DE-owned echoes
//
//   node tools/sim-dedup.js JFA B096B7B614 FR
require('dotenv').config();
const { getPool, closePool } = require('../src/db');
const {
    getEndpoint, getTokenManager, downloadReport, parseTsvReport, isPanEuPooled,
} = require('../src/services/amazon-stock-shared');

const account = process.argv[2];
const asin = (process.argv[3] || '').toUpperCase();
const country = (process.argv[4] || 'FR').toUpperCase();
if (!account || !asin) {
    console.error('usage: node tools/sim-dedup.js <account> <ASIN> [country=FR]');
    process.exit(1);
}
const num = v => parseInt(v || '0', 10) || 0;

(async () => {
    const conn = await getPool().getConnection();
    let deHealthDoc, cHealthDoc, deActiveSkus, cActiveSkus, panEuHas;
    try {
        const [jobs] = await conn.query(
            `SELECT country, document_id FROM amazon_report_jobs
             WHERE batch_date=CURDATE() AND account=? AND report_type='health'
               AND country IN ('DE', ?) AND document_id IS NOT NULL`,
            [account, country]
        );
        deHealthDoc = jobs.find(j => j.country === 'DE')?.document_id;
        cHealthDoc = jobs.find(j => j.country === country)?.document_id;

        const skusFor = async (ctry) => {
            const [r] = await conn.query(
                `SELECT DISTINCT sku FROM amazon_active_listings
                 WHERE asin=? AND company=? AND country=?
                   AND date_ran=(SELECT MAX(date_ran) FROM amazon_active_listings
                                 WHERE asin=? AND company=? AND country=?)`,
                [asin, account, ctry, asin, account, ctry]
            );
            return new Set(r.map(x => x.sku).filter(Boolean));
        };
        deActiveSkus = await skusFor('DE');
        cActiveSkus = await skusFor(country);

        const [pe] = await conn.query(
            `SELECT result_cache FROM amazon_report_jobs
             WHERE country='ALL' AND report_type='pan_eu' AND account=?
               AND batch_date=(SELECT MAX(batch_date) FROM amazon_report_jobs
                               WHERE country='ALL' AND report_type='pan_eu' AND account=?)`,
            [account, account]
        );
        let list = []; try { list = JSON.parse(pe[0]?.result_cache || '[]') || []; } catch {}
        panEuHas = new Set(list).has(asin);
    } finally { conn.release(); }

    if (!deHealthDoc || !cHealthDoc) {
        console.error(`missing health doc (DE=${deHealthDoc} ${country}=${cHealthDoc})`);
        await closePool(); process.exit(1);
    }

    const tm = getTokenManager(account);
    const ep = getEndpoint();
    const deRows = parseTsvReport(await downloadReport(tm, ep, deHealthDoc)).filter(r => (r['asin'] || '') === asin);
    const cRows = parseTsvReport(await downloadReport(tm, ep, cHealthDoc)).filter(r => (r['asin'] || '') === asin);

    // DE accumulated sets (DE is never deduped) = what writeSnapshots stores for DE.
    const deFnskus = new Set(), deSkus = new Set(), deLocalFnskus = new Set();
    for (const r of deRows) {
        const f = (r['fnsku'] || '').trim(), s = r['sku'] || '';
        if (f) deFnskus.add(f);
        if (s) deSkus.add(s);
        if (f && deActiveSkus.has(s)) deLocalFnskus.add(f); // DE's own physically-local pool
    }

    console.log(`\nASIN ${asin}  account ${account}  country ${country}`);
    console.log(`pan_eu cache currently contains ${asin}: ${panEuHas}`);
    console.log(`DE active SKUs: [${[...deActiveSkus]}]   DE-owned FNSKUs: [${[...deLocalFnskus]}]`);
    console.log(`${country} active SKUs: [${[...cActiveSkus]}]`);
    console.log(`\n${country} health rows for ${asin}:`);
    for (const r of cRows) console.log(`  sku=${(r['sku'] || '').padEnd(18)} fnsku=${(r['fnsku'] || '').padEnd(12)} available=${r['available']}`);

    const simulate = (label, keep) => {
        let total = 0; const seen = new Set(); const kept = [];
        for (const r of cRows) {
            const f = (r['fnsku'] || '').trim(), s = r['sku'] || '';
            if (!keep(s, f)) continue;
            const key = f ? `f:${f}` : (s ? `s:${s}` : `r:${seen.size}`);
            if (seen.has(key)) continue; seen.add(key);
            total += num(r['available']); kept.push(`${s}/${f}=${num(r['available'])}`);
        }
        console.log(`[${label}]  ${country} fulfillable = ${total}   (kept: ${kept.join(', ') || 'none'})`);
    };

    console.log('');
    // A current: ASIN in pan_eu → FNSKU-alone echo test.
    simulate('A current (pan-eu)   ', (s, f) =>
        !(isPanEuPooled(country) && f && deFnskus.has(f) && (true || (s && deSkus.has(s)))));
    // B cache updated: ASIN NOT pan-eu → require BOTH sku AND fnsku match DE.
    simulate('B cache updated      ', (s, f) =>
        !(isPanEuPooled(country) && f && deFnskus.has(f) && (false || (s && deSkus.has(s)))));
    // C locality fix: keep only pools whose SKU is locally listed AND whose FNSKU
    // is not one of DE's own physical pools.
    simulate('C locality fix       ', (s, f) => cActiveSkus.has(s) && !deLocalFnskus.has(f));

    await closePool();
})().catch(e => { console.error(e); process.exit(1); });
