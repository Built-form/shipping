'use strict';
// READ-ONLY. Per-country fulfillable totals for an account's latest snapshot,
// plus optional per-ASIN breakdowns. Used to compare before/after a re-run.
//   node tools/peek-account-stock.js JFA [ASIN ...]
require('dotenv').config();
const { getPool, closePool } = require('../src/db');
const account = process.argv[2] || 'JFA';
const asins = process.argv.slice(3);
(async () => {
    const conn = await getPool().getConnection();
    try {
        const [tot] = await conn.query(
            `SELECT country, COUNT(*) asins, SUM(fulfillable) ful, SUM(reserved) res
             FROM amazon_stock_country_snapshots
             WHERE company=? AND date_ran=(SELECT MAX(date_ran) FROM amazon_stock_country_snapshots WHERE company=?)
             GROUP BY country ORDER BY FIELD(country,'DE','UK','FR','ES','IT','US')`, [account, account]);
        console.log(`\n${account} latest snapshot — per country:`);
        let g = 0;
        for (const r of tot) { console.log(`  ${r.country.padEnd(4)} asins=${String(r.asins).padStart(4)} fulfillable=${String(r.ful).padStart(7)} reserved=${r.res}`); g += Number(r.ful); }
        console.log(`  TOTAL fulfillable=${g}`);
        for (const a of asins) {
            const [rows] = await conn.query(
                `SELECT country, fulfillable, reserved, sku, fnsku FROM amazon_stock_country_snapshots
                 WHERE company=? AND asin=? AND date_ran=(SELECT MAX(date_ran) FROM amazon_stock_country_snapshots WHERE company=? AND asin=?)
                 ORDER BY FIELD(country,'DE','UK','FR','ES','IT')`, [account, a, account, a]);
            console.log(`\n  ${a}:`);
            if (!rows.length) console.log('    (no rows)');
            for (const r of rows) console.log(`    ${r.country.padEnd(4)} ful=${String(r.fulfillable).padStart(5)} res=${String(r.reserved).padStart(4)} sku=${(r.sku || '').padEnd(20)} fnsku=${r.fnsku}`);
        }
    } finally { conn.release(); await closePool(); }
})().catch(e => { console.error(e); process.exit(1); });
