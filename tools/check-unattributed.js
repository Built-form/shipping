'use strict';
// READ-ONLY. Finds FBA stock the DE health report shows under (asin,sku) pairs
// that are NOT an active listing in ANY of the account's marketplaces — i.e.
// the units the old code dumped into DE that the locality rule no longer
// attributes anywhere. Quantifies the post-fix total dip.
//   node tools/check-unattributed.js JFA
require('dotenv').config();
const { getPool, closePool } = require('../src/db');
const { getEndpoint, getTokenManager, downloadReport, parseTsvReport } = require('../src/services/amazon-stock-shared');
const account = process.argv[2] || 'JFA';
const num = v => parseInt(v || '0', 10) || 0;

(async () => {
    let deDoc, alRows;
    {
        const conn = await getPool().getConnection();
        try {
            const [d] = await conn.query(
                `SELECT document_id FROM amazon_report_jobs
                 WHERE batch_date=CURDATE() AND account=? AND country='DE' AND report_type='health' AND document_id IS NOT NULL`, [account]);
            deDoc = d[0]?.document_id;
            [alRows] = await conn.query(
                `SELECT asin, sku FROM amazon_active_listings
                 WHERE company=? AND date_ran=(SELECT MAX(date_ran) FROM amazon_active_listings WHERE company=?)`, [account, account]);
        } finally { conn.release(); }
    }
    const activeAnywhere = new Set(alRows.map(r => `${r.asin}|${r.sku}`));
    const deParsed = parseTsvReport(await downloadReport(getTokenManager(account), getEndpoint(), deDoc));

    const lost = new Map(); let total = 0;
    for (const r of deParsed) {
        const asin = (r['asin'] || '').trim(), sku = r['sku'] || '', av = num(r['available']);
        if (!asin || av <= 0) continue;
        if (activeAnywhere.has(`${asin}|${sku}`)) continue;
        total += av;
        lost.set(`${asin}|${sku}`, (lost.get(`${asin}|${sku}`) || 0) + av);
    }
    const arr = [...lost.entries()].sort((a, b) => b[1] - a[1]);
    console.log(`\n${account}: DE-reported FBA stock under SKUs with NO active listing in any marketplace`);
    console.log(`= ${total} units across ${arr.length} (asin,sku) pairs (the post-fix dip)\n`);
    console.log('ASIN|SKU                                  units');
    for (const [k, v] of arr.slice(0, 30)) console.log(`  ${k.padEnd(38)} ${v}`);
    await closePool();
})().catch(e => { console.error(e); process.exit(1); });
