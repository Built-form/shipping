'use strict';
// Full SKU -> FNSKU listing, per company, per region.
// Usage: node tools/list-fnskus.js [company] [country]   (filters optional)
//        node tools/list-fnskus.js JFA UK
// Writes a CSV next to the repo and prints a summary.
require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { getPool, closePool } = require('../src/db');

const fCompany = (process.argv[2] || '').trim();
const fCountry = (process.argv[3] || '').trim().toUpperCase();
const splitCsv = v => (v || '').split(',').map(s => s.trim()).filter(Boolean);
const csvCell = v => {
    const s = v == null ? '' : String(v);
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
};

(async () => {
    const conn = await getPool().getConnection();
    try {
        // Latest listing rows per company/country (every marketplace SKU).
        const [al] = await conn.query(
            `SELECT al.company, al.country, al.asin, al.sku, al.product_name, al.price
             FROM amazon_active_listings al
             JOIN (SELECT company, country, MAX(date_ran) d FROM amazon_active_listings
                   GROUP BY company, country) m
               ON m.company=al.company AND m.country=al.country AND al.date_ran=m.d
             ORDER BY al.company, al.country, al.sku`);

        // Latest inventory snapshot per company/country -> fnsku pools + qty by asin.
        const [cs] = await conn.query(
            `SELECT a.company, a.country, a.asin, a.sku, a.fnsku,
                    a.fulfillable, a.reserved
             FROM amazon_stock_country_snapshots a
             JOIN (SELECT company, country, MAX(date_ran) d FROM amazon_stock_country_snapshots
                   GROUP BY company, country) m
               ON m.company=a.company AND m.country=a.country AND a.date_ran=m.d`);

        // key company|country|asin -> { fnskus:Set, positional:Map(sku->fnsku), ful, res }
        const pool = new Map();
        for (const r of cs) {
            const key = `${r.company}|${r.country}|${r.asin}`;
            if (!pool.has(key)) pool.set(key, { fnskus: new Set(), positional: new Map(), ful: 0, res: 0, hasRow: true });
            const p = pool.get(key);
            const skus = splitCsv(r.sku), fnskus = splitCsv(r.fnsku);
            for (const f of fnskus) p.fnskus.add(f);
            for (let i = 0; i < Math.min(skus.length, fnskus.length); i++) p.positional.set(skus[i], fnskus[i]);
            p.ful += Number(r.fulfillable) || 0;
            p.res += Number(r.reserved) || 0;
        }

        const rows = [];
        for (const r of al) {
            if (fCompany && r.company !== fCompany) continue;
            if (fCountry && r.country !== fCountry) continue;
            const p = pool.get(`${r.company}|${r.country}|${r.asin}`);
            let fnsku = '';
            if (p) {
                if (p.positional.has(r.sku)) fnsku = p.positional.get(r.sku);
                else if (p.fnskus.size === 1) fnsku = [...p.fnskus][0];
                else if (p.fnskus.size > 1) fnsku = [...p.fnskus].join(' | ');
            }
            rows.push({
                company: r.company, country: r.country, asin: r.asin, sku: r.sku,
                fnsku, fulfillable: p ? p.ful : '', reserved: p ? p.res : '',
                stock_state: !p ? 'no_pool' : (p.ful > 0 ? 'in_stock' : 'zero_pool'),
                price: r.price, product_name: r.product_name,
            });
        }

        const header = ['company','country','asin','sku','fnsku','fulfillable','reserved','stock_state','price','product_name'];
        const out = [header.join(',')]
            .concat(rows.map(r => header.map(h => csvCell(r[h])).join(',')))
            .join('\n');
        const fname = `fnskus-by-region${fCompany ? '-' + fCompany : ''}${fCountry ? '-' + fCountry : ''}.csv`;
        const fpath = path.join(process.cwd(), fname);
        fs.writeFileSync(fpath, out);

        // Summary
        const noFnsku = rows.filter(r => !r.fnsku).length;
        const noPool = rows.filter(r => r.stock_state === 'no_pool').length;
        const zero = rows.filter(r => r.stock_state === 'zero_pool').length;
        console.log(`Wrote ${rows.length} SKU rows -> ${fpath}`);
        console.log(`  with FNSKU:        ${rows.length - noFnsku}`);
        console.log(`  no FNSKU on file:  ${noFnsku}  (no inventory pool ever seen)`);
        console.log(`  stock_state: in_stock=${rows.length - noPool - zero}  zero_pool=${zero}  no_pool=${noPool}`);
    } finally { conn.release(); await closePool(); }
})().catch(e => { console.error(e); process.exit(1); });
