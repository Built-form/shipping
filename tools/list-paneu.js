'use strict';
// READ-ONLY. Lists every ASIN an account flags as Pan-EU (Enrol='Y' in the
// latest GET_PAN_EU_OFFER_STATUS report — i.e. what lands in our pan_eu cache),
// with its Pan-EU status + expiry, mapped to its JF code. Flags lapsed ones
// (Enrolment ended + expiry in the past) — those are the ones wrongly driving
// the FNSKU-alone dedup.
//   node tools/list-paneu.js JFA
require('dotenv').config();
const { getPool, closePool } = require('../src/db');
const { getEndpoint, getTokenManager, downloadReport, parseTsvReport } = require('../src/services/amazon-stock-shared');

const account = process.argv[2] || 'JFA';

(async () => {
    let docId;
    {
        const conn = await getPool().getConnection();
        try {
            const [rows] = await conn.query(
                `SELECT document_id FROM amazon_report_jobs
                 WHERE account=? AND country='ALL' AND report_type='pan_eu' AND document_id IS NOT NULL
                 ORDER BY batch_date DESC LIMIT 1`, [account]);
            docId = rows[0]?.document_id;
        } finally { conn.release(); }
    }
    if (!docId) { console.error('no pan_eu document for ' + account); await closePool(); process.exit(1); }

    const parsed = parseTsvReport(await downloadReport(getTokenManager(account), getEndpoint(), docId));

    const byAsin = new Map();
    for (const r of parsed) {
        if ((r['Enrol'] || '').trim().toUpperCase() !== 'Y') continue;
        const asin = (r['ASIN'] || '').trim();
        if (!asin || byAsin.has(asin)) continue;
        byAsin.set(asin, {
            asin,
            status: (r['Pan-EU status'] || '').trim(),
            expires: (r['Date Pan-EU expires'] || '').trim(),
            title: (r['Title'] || '').trim(),
        });
    }
    const asins = [...byAsin.keys()];

    const jfByAsin = new Map();
    {
        const conn = await getPool().getConnection();
        try {
            if (asins.length) {
                const [lc] = await conn.query(
                    `SELECT asin, jf_code FROM landed_costs
                     WHERE asin IN (${asins.map(() => '?').join(',')}) AND jf_code IS NOT NULL AND jf_code <> ''`,
                    asins);
                for (const r of lc) if (!jfByAsin.has(r.asin)) jfByAsin.set(r.asin, r);
            }
        } finally { conn.release(); }
    }

    const now = new Date();
    const list = [...byAsin.values()].map(v => {
        const exp = new Date(v.expires);
        const expValid = !isNaN(exp);
        const lapsed = v.status.toLowerCase().includes('ended') && expValid && exp < now;
        return {
            ...v,
            jf: jfByAsin.get(v.asin)?.jf_code || '',
            name: v.title,
            lapsed,
        };
    });
    list.sort((a, b) => (Number(b.lapsed) - Number(a.lapsed)) || a.jf.localeCompare(b.jf) || a.asin.localeCompare(b.asin));

    const counts = {};
    for (const x of list) counts[x.status || '(blank)'] = (counts[x.status || '(blank)'] || 0) + 1;
    const lapsedCount = list.filter(x => x.lapsed).length;
    const noJf = list.filter(x => !x.jf).length;

    console.log(`\n${account}: ${list.length} ASINs flagged Pan-EU (Enrol='Y') in latest report`);
    console.log('By Pan-EU status:', counts);
    console.log(`Lapsed (Enrolment ended + expiry in past): ${lapsedCount}`);
    console.log(`Without a JF-code mapping in landed_costs: ${noJf}\n`);

    console.log('JFCODE     ASIN         STATUS            EXPIRES                          NAME');
    for (const x of list) {
        console.log(
            `${(x.jf || '-').padEnd(10)} ${x.asin.padEnd(12)} ${(x.status || '-').padEnd(17)} ${(x.expires || '-').padEnd(32)} ${(x.name || '').slice(0, 44)}${x.lapsed ? '  <STALE>' : ''}`
        );
    }

    // Write the lapsed subset (the ones wrongly driving the dedup) to CSV.
    const fs = require('fs');
    const csvEsc = s => `"${String(s ?? '').replace(/"/g, '""')}"`;
    const lapsed = list.filter(x => x.lapsed);
    const csvPath = `paneu-lapsed-${account}.csv`;
    const csv = ['jf_code,asin,pan_eu_status,expires,title']
        .concat(lapsed.map(x => [x.jf, x.asin, x.status, x.expires, x.name].map(csvEsc).join(',')))
        .join('\n');
    fs.writeFileSync(csvPath, csv);
    console.log(`\nWrote ${lapsed.length} lapsed rows to ${csvPath} (${lapsed.filter(x => x.jf).length} with a JF code)`);

    await closePool();
})().catch(e => { console.error(e); process.exit(1); });
