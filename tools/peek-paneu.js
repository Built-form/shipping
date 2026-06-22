'use strict';
require('dotenv').config();
const { getPool } = require('../src/db');
const {
    getEndpoint, getTokenManager, downloadReport, parseTsvReport,
} = require('../src/services/amazon-stock-shared');

const account = process.argv[2] || 'JFA';
const filterAsin = (process.argv[3] || '').toUpperCase();

(async () => {
    const conn = await getPool().getConnection();
    let docId;
    try {
        const [rows] = await conn.query(
            `SELECT document_id FROM amazon_report_jobs
             WHERE batch_date = CURDATE() AND account = ? AND report_type = 'pan_eu'`,
            [account]
        );
        docId = rows[0].document_id;
    } finally { conn.release(); }

    const tm = getTokenManager(account);
    const tsv = await downloadReport(tm, getEndpoint(), docId);
    const parsed = parseTsvReport(tsv);
    console.log(`total rows: ${parsed.length}`);
    if (parsed[0]) console.log('columns:', Object.keys(parsed[0]));

    if (filterAsin) {
        const matches = parsed.filter(r => (r['ASIN'] || '').trim().toUpperCase() === filterAsin);
        console.log(`\nrows for ${filterAsin}: ${matches.length}`);
        for (const r of matches) console.log(r);
    } else {
        // distribution of Enrol values
        const enrolCounts = {};
        for (const r of parsed) {
            const k = (r['Enrol'] || '<empty>').trim();
            enrolCounts[k] = (enrolCounts[k] || 0) + 1;
        }
        console.log('\nEnrol value distribution:', enrolCounts);
        console.log('\nfirst 3 rows:', parsed.slice(0, 3));
    }
    process.exit(0);
})().catch(err => { console.error(err); process.exit(1); });
