'use strict';
// Scan a full health report and check whether `Total Reserved Quantity`
// always equals FC Transfer + FC Processing + Customer Order. Flags any row
// where the total does NOT account for the FC-transfer component.
//
//   node tools/scan-reserved.js JFA UK
require('dotenv').config();
const { getPool, closePool } = require('../src/db');
const {
    getEndpointForCountry, getRegionForCountry, getTokenManager,
    downloadReport, parseTsvReport,
} = require('../src/services/amazon-stock-shared');

const account = process.argv[2] || 'JFA';
const country = (process.argv[3] || 'UK').toUpperCase();
const num = (v) => parseInt((v || '0').toString().replace(/[^0-9-]/g, ''), 10) || 0;

(async () => {
    const conn = await getPool().getConnection();
    let doc, date;
    try {
        const [jobs] = await conn.query(
            `SELECT document_id, batch_date FROM amazon_report_jobs
             WHERE account = ? AND country = ? AND report_type = 'health' AND document_id IS NOT NULL
             ORDER BY batch_date DESC LIMIT 1`, [account, country]);
        doc = jobs[0]?.document_id; date = jobs[0]?.batch_date ? String(jobs[0].batch_date).slice(0,10) : null;
    } finally { conn.release(); }
    if (!doc) { console.error('no health doc'); process.exit(2); }

    const tm = getTokenManager(account, getRegionForCountry(country));
    const tsv = await downloadReport(tm, getEndpointForCountry(country), doc);
    const rows = parseTsvReport(tsv);
    console.log(`${account}/${country} health ${date}: ${rows.length} rows`);
    console.log(`columns: ${JSON.stringify(Object.keys(rows[0] || {}))}\n`);

    let withTransfer = 0, mismatchExclTransfer = 0, mismatchOther = 0, exact = 0;
    const samples = [];
    for (const r of rows) {
        const total = num(r['Total Reserved Quantity']);
        const fcT = num(r['Reserved FC Transfer']);
        const fcP = num(r['Reserved FC Processing']);
        const cust = num(r['Reserved Customer Order']);
        if (fcT === 0 && fcP === 0 && cust === 0 && total === 0) continue;
        if (fcT > 0) withTransfer++;
        const sum = fcT + fcP + cust;
        if (total === sum) { exact++; continue; }
        // total doesn't match the 3-way sum
        if (total === fcP + cust && fcT > 0) {
            // total excludes FC transfer specifically — this is the user's concern
            mismatchExclTransfer++;
            if (samples.length < 20) samples.push({ kind: 'EXCLUDES-FC-TRANSFER', sku: r['sku'], total, fcT, fcP, cust });
        } else {
            mismatchOther++;
            if (samples.length < 20) samples.push({ kind: 'other', sku: r['sku'], total, fcT, fcP, cust });
        }
    }
    console.log(`rows with any reserved: ${exact + mismatchExclTransfer + mismatchOther}`);
    console.log(`  rows with FC transfer > 0:        ${withTransfer}`);
    console.log(`  total == FCt+FCp+cust (exact):    ${exact}`);
    console.log(`  total == FCp+cust (EXCLUDES FCt): ${mismatchExclTransfer}  <-- user's concern`);
    console.log(`  other mismatch:                   ${mismatchOther}`);
    if (samples.length) {
        console.log('\nsamples:');
        for (const s of samples) console.log(`  [${s.kind}] sku=${s.sku} total=${s.total} fcTransfer=${s.fcT} fcProc=${s.fcP} cust=${s.cust}`);
    }
    await closePool();
})().catch(e => { console.error(e); process.exit(1); });
