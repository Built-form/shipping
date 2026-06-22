'use strict';
require('dotenv').config();
const { getPool, closePool } = require('../src/db');

(async () => {
    const conn = await getPool().getConnection();
    try {
        const [rows] = await conn.query(
            `SELECT a.country, a.date_ran, a.fnsku, a.sku, a.fulfillable, a.reserved,
                    a.inbound_working, a.inbound_shipped, a.inbound_receiving
             FROM amazon_stock_country_snapshots a
             JOIN (
                 SELECT country, MAX(date_ran) AS latest
                 FROM amazon_stock_country_snapshots
                 WHERE asin = 'B003IWY2CO' AND company = 'Hangerworld'
                 GROUP BY country
             ) ld ON ld.country = a.country AND a.date_ran = ld.latest
             WHERE a.asin = 'B003IWY2CO' AND a.company = 'Hangerworld'
             ORDER BY a.country`
        );
        console.log('Per-country latest snapshot for B003IWY2CO (Hangerworld) — what orders.js feeds the UI:');
        for (const r of rows) {
            console.log(`  ${r.country}  date=${String(r.date_ran).slice(0,10)}  sku=${(r.sku||'').padEnd(28)} fnsku=${(r.fnsku||'').padEnd(12)} ful=${r.fulfillable} res=${r.reserved} ibw=${r.inbound_working} ibs=${r.inbound_shipped} ibr=${r.inbound_receiving}`);
        }

        console.log('\nRecent rows for FR/ES/IT (chronological), to see when they were last written:');
        const [rows2] = await conn.query(
            `SELECT country, date_ran, fnsku, sku, fulfillable, reserved
             FROM amazon_stock_country_snapshots
             WHERE asin = 'B003IWY2CO' AND company = 'Hangerworld'
               AND country IN ('FR','ES','IT')
             ORDER BY country, date_ran DESC
             LIMIT 30`
        );
        for (const r of rows2) {
            console.log(`  ${r.country}  ${String(r.date_ran).slice(0,10)}  sku=${(r.sku||'').padEnd(28)} fnsku=${(r.fnsku||'').padEnd(12)} ful=${r.fulfillable} res=${r.reserved}`);
        }

        console.log('\nDE row(s) for sku/fnsku reference (for dedup matching):');
        const [rows3] = await conn.query(
            `SELECT date_ran, sku, fnsku, fulfillable, reserved
             FROM amazon_stock_country_snapshots
             WHERE asin = 'B003IWY2CO' AND company = 'Hangerworld' AND country = 'DE'
             ORDER BY date_ran DESC
             LIMIT 5`
        );
        for (const r of rows3) {
            console.log(`  DE  ${String(r.date_ran).slice(0,10)}  sku=${r.sku} fnsku=${r.fnsku} ful=${r.fulfillable} res=${r.reserved}`);
        }
    } finally {
        conn.release();
        await closePool();
    }
})().catch(err => { console.error(err); process.exit(1); });
