'use strict';
// Read-only projection: for each marketplace's latest health report, compare the
// OLD reserved (Total Reserved Quantity) vs the NEW reserved
// (max(Total, customer+transfer+processing)) and report how much would change.
//
//   node tools/project-reserved-impact.js JFA UK DE FR ES IT
require('dotenv').config();
const { getPool, closePool } = require('../src/db');
const {
    getEndpointForCountry, getRegionForCountry, getTokenManager,
    downloadReport, parseTsvReport,
} = require('../src/services/amazon-stock-shared');

const account = process.argv[2] || 'JFA';
const countries = process.argv.slice(3);
if (!countries.length) { console.error('usage: node tools/project-reserved-impact.js <account> <country...>'); process.exit(1); }
const num = (v) => parseInt((v || '0').toString().replace(/[^0-9-]/g, ''), 10) || 0;

(async () => {
    for (const country of countries) {
        const conn = await getPool().getConnection();
        let doc, date;
        try {
            const [jobs] = await conn.query(
                `SELECT document_id, batch_date FROM amazon_report_jobs
                 WHERE account = ? AND country = ? AND report_type = 'health' AND document_id IS NOT NULL
                 ORDER BY batch_date DESC LIMIT 1`, [account, country]);
            doc = jobs[0]?.document_id; date = jobs[0]?.batch_date ? String(jobs[0].batch_date).slice(0,10) : null;
        } finally { conn.release(); }
        if (!doc) { console.log(`${account}/${country}: no health doc\n`); continue; }

        const tm = getTokenManager(account, getRegionForCountry(country));
        const tsv = await downloadReport(tm, getEndpointForCountry(country), doc);
        const rows = parseTsvReport(tsv);

        let oldTotal = 0, newTotal = 0, changedSkus = 0, biggestDelta = 0, biggestSku = '';
        for (const r of rows) {
            const oldR = num(r['Total Reserved Quantity']);
            const comp = num(r['Reserved Customer Order']) + num(r['Reserved FC Transfer']) + num(r['Reserved FC Processing']);
            const newR = Math.max(oldR, comp);
            oldTotal += oldR; newTotal += newR;
            if (newR !== oldR) {
                changedSkus++;
                const d = newR - oldR;
                if (d > biggestDelta) { biggestDelta = d; biggestSku = r['sku']; }
            }
        }
        console.log(`${account}/${country} (${date}): ${rows.length} rows`);
        console.log(`  reserved total: ${oldTotal} -> ${newTotal}  (+${newTotal - oldTotal} units across ${changedSkus} SKUs)`);
        console.log(`  biggest single change: ${biggestSku} +${biggestDelta}\n`);
    }
    await closePool();
})().catch(e => { console.error(e); process.exit(1); });
