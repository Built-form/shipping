'use strict';
require('dotenv').config();
const { getPool, closePool } = require('../src/db');
const frag = (process.argv[2] || '').trim();
(async () => {
    const conn = await getPool().getConnection();
    try {
        const [al] = await conn.query(
            `SELECT company, country, asin, sku, fnsku, MAX(date_ran) d
             FROM amazon_active_listings
             WHERE sku LIKE ? OR asin LIKE ?
             GROUP BY company, country, asin, sku, fnsku
             ORDER BY company, country LIMIT 50`,
            [`%${frag}%`, `%${frag}%`]
        );
        console.log(`active_listings matches for "${frag}": ${al.length}`);
        for (const r of al) console.log(`  ${r.company}/${r.country} asin=${r.asin} sku=${r.sku} fnsku=${r.fnsku} last=${String(r.d).slice(0,10)}`);

        const [sn] = await conn.query(
            `SELECT company, country, asin, sku, fnsku, MAX(date_ran) d
             FROM amazon_stock_country_snapshots
             WHERE sku LIKE ? OR asin LIKE ?
             GROUP BY company, country, asin, sku, fnsku
             ORDER BY company, country LIMIT 50`,
            [`%${frag}%`, `%${frag}%`]
        );
        console.log(`\nsnapshot matches for "${frag}": ${sn.length}`);
        for (const r of sn) console.log(`  ${r.company}/${r.country} asin=${r.asin} sku=${r.sku} fnsku=${r.fnsku} last=${String(r.d).slice(0,10)}`);
    } finally { conn.release(); await closePool(); }
})().catch(e => { console.error(e); process.exit(1); });
