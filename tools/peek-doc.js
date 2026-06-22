'use strict';
require('dotenv').config();
const { getPool } = require('../src/db');
const {
    getEndpoint, getTokenManager, downloadReport, parseTsvReport,
} = require('../src/services/amazon-stock-shared');

const account = process.argv[2] || 'JFA';
const country = process.argv[3] || 'ES';
const reportType = process.argv[4] || 'active_listings';
const filter = (process.argv[5] || '').trim().toUpperCase();

(async () => {
    const conn = await getPool().getConnection();
    let docId, batchDate;
    try {
        const [rows] = await conn.query(
            `SELECT batch_date, document_id FROM amazon_report_jobs
             WHERE batch_date = CURDATE() AND account = ? AND country = ? AND report_type = ?`,
            [account, country, reportType]
        );
        if (!rows[0]) { console.error('no row'); process.exit(1); }
        docId = rows[0].document_id;
        batchDate = rows[0].batch_date;
        console.log(`batch_date=${batchDate} docId=${docId}`);
    } finally { conn.release(); }

    const tokenManager = getTokenManager(account);
    const tsv = await downloadReport(tokenManager, getEndpoint(), docId);
    console.log(`raw bytes: ${Buffer.byteLength(tsv, 'utf8')}`);
    console.log(`raw lines: ${tsv.split('\n').length}`);
    console.log('--- first 600 chars ---');
    console.log(tsv.slice(0, 600));
    console.log('--- end ---');
    const parsed = parseTsvReport(tsv);
    console.log(`parsed rows: ${parsed.length}`);
    if (parsed[0]) console.log('first row keys:', Object.keys(parsed[0]));
    if (parsed[0]) console.log('first row:', parsed[0]);

    if (filter) {
        console.log(`\n--- rows matching "${filter}" (sku/asin/fnsku/product-id substring) ---`);
        const matches = parsed.filter(r =>
            Object.values(r).some(v => String(v || '').toUpperCase().includes(filter))
        );
        console.log(`matches: ${matches.length}`);
        for (const r of matches) console.log(r);

        // Also raw-line scan in case parsing dropped it
        const rawHits = tsv.split('\n').filter(l => l.toUpperCase().includes(filter));
        console.log(`\n--- raw TSV lines containing "${filter}": ${rawHits.length} ---`);
        for (const l of rawHits) console.log(l);
    }
    process.exit(0);
})().catch(err => { console.error(err); process.exit(1); });
