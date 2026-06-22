'use strict';
// DB-only (fast): show reserved before/after for a (account,country) by comparing
// the two most recent date_ran. Highlights SKUs where reserved jumped — i.e. where
// FC-transfer units are now included.
//   node tools/verify-reserved-jump.js JFA DE
require('dotenv').config();
const { getPool, closePool } = require('../src/db');
const account = process.argv[2] || 'JFA';
const country = (process.argv[3] || 'DE').toUpperCase();
(async () => {
    const conn = await getPool().getConnection();
    try {
        const [dates] = await conn.query(
            `SELECT DISTINCT date_ran FROM amazon_stock_country_snapshots
             WHERE company = ? AND country = ? ORDER BY date_ran DESC LIMIT 2`, [account, country]);
        if (dates.length < 2) { console.log('not enough dates'); return; }
        const today = String(dates[0].date_ran).slice(0,10), prev = String(dates[1].date_ran).slice(0,10);
        const [rows] = await conn.query(
            `SELECT t.sku, t.asin, p.reserved AS prev_reserved, t.reserved AS today_reserved,
                    (t.reserved - p.reserved) AS delta
             FROM amazon_stock_country_snapshots t
             JOIN amazon_stock_country_snapshots p
               ON p.asin = t.asin AND p.company = t.company AND p.country = t.country
             WHERE t.company = ? AND t.country = ? AND t.date_ran = ? AND p.date_ran = ?
             ORDER BY delta DESC LIMIT 15`, [account, country, today, prev]);
        console.log(`${account}/${country}: reserved ${prev} -> ${today} (top increases)`);
        const [tot] = await conn.query(
            `SELECT SUM(reserved) s FROM amazon_stock_country_snapshots
             WHERE company=? AND country=? AND date_ran=?`, [account, country, today]);
        const [totp] = await conn.query(
            `SELECT SUM(reserved) s FROM amazon_stock_country_snapshots
             WHERE company=? AND country=? AND date_ran=?`, [account, country, prev]);
        console.log(`  total reserved: ${totp[0].s} -> ${tot[0].s}\n`);
        for (const r of rows) console.log(`  +${String(r.delta).padStart(5)}  ${r.asin}  ${(r.sku||'').padEnd(28)} ${r.prev_reserved} -> ${r.today_reserved}`);
    } finally { conn.release(); await closePool(); }
})().catch(e => { console.error(e.message); process.exit(1); });
