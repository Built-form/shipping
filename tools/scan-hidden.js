'use strict';
// READ-ONLY. Quantifies the FR/ES/IT stock currently HIDDEN by the dedup across
// the lapsed Pan-EU ASINs in paneu-lapsed-<account>.csv. Uses ONE DE health
// download (DE's report enumerates every MCI pool's quantity) + stored
// active_listings + current country_snapshots, applies the locality rule, and
// diffs "what each country should show" vs "what it shows now". Writes nothing.
//   node tools/scan-hidden.js JFA
require('dotenv').config();
const fs = require('fs');
const { getPool, closePool } = require('../src/db');
const { getEndpoint, getTokenManager, downloadReport, parseTsvReport } = require('../src/services/amazon-stock-shared');

const account = process.argv[2] || 'JFA';
const POOLED = ['FR', 'ES', 'IT'];
const num = v => parseInt(v || '0', 10) || 0;

(async () => {
    const csvPath = `paneu-lapsed-${account}.csv`;
    if (!fs.existsSync(csvPath)) { console.error(`${csvPath} not found — run list-paneu.js first`); process.exit(1); }
    const rows = fs.readFileSync(csvPath, 'utf8').trim().split('\n').slice(1)
        .map(l => { const m = l.match(/^"([^"]*)","([^"]*)"/); return m ? { jf: m[1], asin: m[2] } : null; })
        .filter(Boolean);
    const asins = [...new Set(rows.map(r => r.asin))];
    const jfByAsin = new Map(rows.map(r => [r.asin, r.jf]));

    let deDoc, alRows, snapRows;
    {
        const conn = await getPool().getConnection();
        try {
            const [d] = await conn.query(
                `SELECT document_id FROM amazon_report_jobs
                 WHERE batch_date=CURDATE() AND account=? AND country='DE' AND report_type='health' AND document_id IS NOT NULL`,
                [account]);
            deDoc = d[0]?.document_id;
            const inC = asins.map(() => '?').join(',');
            [alRows] = await conn.query(
                `SELECT asin, country, sku FROM amazon_active_listings
                 WHERE company=? AND asin IN (${inC})
                   AND date_ran=(SELECT MAX(date_ran) FROM amazon_active_listings WHERE company=?)`,
                [account, ...asins, account]);
            [snapRows] = await conn.query(
                `SELECT asin, country, fulfillable FROM amazon_stock_country_snapshots
                 WHERE company=? AND asin IN (${inC})
                   AND date_ran=(SELECT MAX(date_ran) FROM amazon_stock_country_snapshots WHERE company=?)`,
                [account, ...asins, account]);
        } finally { conn.release(); }
    }
    if (!deDoc) { console.error('no DE health doc today'); await closePool(); process.exit(1); }

    const activeSkus = new Map();
    for (const r of alRows) {
        const k = `${r.asin}|${r.country}`;
        if (!activeSkus.has(k)) activeSkus.set(k, new Set());
        if (r.sku) activeSkus.get(k).add(r.sku);
    }
    const skusFor = (a, c) => activeSkus.get(`${a}|${c}`) || new Set();
    const curStock = new Map();
    for (const r of snapRows) curStock.set(`${r.asin}|${r.country}`, num(r.fulfillable));

    const deParsed = parseTsvReport(await downloadReport(getTokenManager(account), getEndpoint(), deDoc));
    const poolsByAsin = new Map();
    for (const r of deParsed) {
        const asin = (r['asin'] || '').trim(); if (!asin) continue;
        if (!poolsByAsin.has(asin)) poolsByAsin.set(asin, []);
        poolsByAsin.get(asin).push({ sku: r['sku'] || '', fnsku: (r['fnsku'] || '').trim(), avail: num(r['available']) });
    }

    const results = [];
    for (const asin of asins) {
        const pools = poolsByAsin.get(asin) || [];
        if (!pools.length) continue;
        const deSkus = skusFor(asin, 'DE');
        const deLocalFnskus = new Set(pools.filter(p => deSkus.has(p.sku)).map(p => p.fnsku).filter(Boolean));
        const row = { asin, jf: jfByAsin.get(asin) || '', hidden: {}, totalHidden: 0 };
        for (const c of POOLED) {
            const cSkus = skusFor(asin, c);
            const seen = new Set(); let trueStock = 0;
            for (const p of pools) {
                if (!cSkus.has(p.sku)) continue;            // not this country's own pool
                if (p.fnsku && deLocalFnskus.has(p.fnsku)) continue; // DE-owned echo (single-pool)
                const key = p.fnsku ? `f:${p.fnsku}` : `s:${p.sku}`;
                if (seen.has(key)) continue; seen.add(key);
                trueStock += p.avail;
            }
            const cur = curStock.get(`${asin}|${c}`) || 0;
            row.hidden[c] = { trueStock, cur, hidden: trueStock - cur };
            if (trueStock - cur > 0) row.totalHidden += trueStock - cur;
        }
        results.push(row);
    }

    results.sort((a, b) => b.totalHidden - a.totalHidden);
    const affected = results.filter(r => r.totalHidden > 0);
    const sum = c => affected.reduce((s, r) => s + Math.max(0, r.hidden[c]?.hidden || 0), 0);

    console.log(`\n${account}: scanned ${asins.length} lapsed ASINs (${results.length} had DE pool rows)`);
    console.log(`ASINs with hidden FR/ES/IT stock: ${affected.length}`);
    console.log(`Total hidden units — FR ${sum('FR')}   ES ${sum('ES')}   IT ${sum('IT')}   = ${sum('FR') + sum('ES') + sum('IT')}\n`);
    console.log('JFCODE   ASIN          FR     ES     IT   total');
    for (const r of affected) {
        const f = r.hidden.FR?.hidden || 0, e = r.hidden.ES?.hidden || 0, i = r.hidden.IT?.hidden || 0;
        console.log(`${(r.jf || '-').padEnd(8)} ${r.asin.padEnd(12)} ${String(f).padStart(5)} ${String(e).padStart(6)} ${String(i).padStart(6)} ${String(r.totalHidden).padStart(7)}`);
    }
    await closePool();
})().catch(e => { console.error(e); process.exit(1); });
