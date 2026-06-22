'use strict';
// Build a CSV of the jfpro.products jfcodes that are NOT already in
// jfa.mintsoft_carton_sizes (the "missing" products), each with its Mintsoft
// image URL.
//
// These rows have no stored Mintsoft product_id, so for each jfcode we resolve
// the Mintsoft product via SKU search (getProductsByJfCode), then fetch its
// ImageURL. Paced to stay under Mintsoft's Cloudflare rate-limit.
//
// Output CSV columns: jfcode, name, status, product_id, sku, image_url
//
// Usage:
//   node tools/extra-product-images.js                    # all 278 missing, -> extra-product-images.csv
//   node tools/extra-product-images.js --live             # only status='Live'
//   node tools/extra-product-images.js --limit 5          # first N (quick test)
//   node tools/extra-product-images.js --csv out.csv      # custom output path
//   node tools/extra-product-images.js --concurrency 1    # tune parallelism (default 2)
require('dotenv').config();
const fs = require('fs');
const { getPool, closePool } = require('../src/db');
const { getProductsByJfCode, getProductImageUrl } = require('../src/services/mintsoft');

const args = process.argv.slice(2);
const has = (f) => args.includes(f);
const val = (f, d) => { const i = args.indexOf(f); return i !== -1 ? args[i + 1] : d; };

const liveOnly = has('--live');
const limit = Number(val('--limit', 0)) || 0;
const concurrency = Math.max(1, Number(val('--concurrency', 2)) || 2);
const csvPath = val('--csv', 'extra-product-images.csv');

const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const csvCell = (v) => {
    const s = String(v ?? '');
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
};

// getProductsByJfCode has no built-in 429 handling; wrap it so a transient
// Cloudflare throttle backs off instead of dropping the product.
async function resolveProduct(jfcode) {
    for (let attempt = 0; ; attempt++) {
        try {
            return await getProductsByJfCode(jfcode);
        } catch (err) {
            if (err.response?.status === 429 && attempt < 6) {
                await sleep(Math.min(60000, 5000 * 2 ** attempt));
                continue;
            }
            throw err;
        }
    }
}

(async () => {
    const pool = getPool();
    const conn = await pool.getConnection();
    let products;
    try {
        const [rows] = await conn.query(
            `SELECT TRIM(p.jfcode) AS jfcode, p.name, p.status
               FROM jfpro.products p
              WHERE TRIM(COALESCE(p.jfcode,'')) <> ''
                AND TRIM(p.jfcode) NOT IN (
                      SELECT TRIM(jf_code) FROM jfa.mintsoft_carton_sizes WHERE jf_code IS NOT NULL
                    )
                ${liveOnly ? "AND p.status = 'Live'" : ''}
              ORDER BY p.jfcode` + (limit ? ` LIMIT ${limit}` : '')
        );
        products = rows;
    } finally {
        conn.release();
    }

    const results = [];
    let done = 0;
    for (let i = 0; i < products.length; i += concurrency) {
        const wave = products.slice(i, i + concurrency);
        await Promise.all(wave.map(async (p) => {
            const row = { jfcode: p.jfcode, name: p.name, status: p.status, product_id: '', sku: '', image_url: '' };
            try {
                const matches = await resolveProduct(p.jfcode);
                // Prefer the SKU that equals the jfcode exactly (the base product),
                // otherwise take the first match.
                const best = matches.find(m => m.sku.toUpperCase() === p.jfcode.toUpperCase()) || matches[0];
                if (best) {
                    row.product_id = best.productId;
                    row.sku = best.sku;
                    row.image_url = await getProductImageUrl(best.productId);
                }
            } catch (err) {
                process.stderr.write(`\n${p.jfcode}: ${err.message}\n`);
            }
            results.push(row);
        }));
        done += wave.length;
        process.stderr.write(`Resolved ${done}/${products.length}...\r`);
        if (i + concurrency < products.length) await sleep(250);
    }
    process.stderr.write('\n');

    results.sort((a, b) => String(a.jfcode).localeCompare(String(b.jfcode)));

    const header = 'jfcode,name,status,product_id,sku,image_url';
    const lines = results.map(r =>
        [r.jfcode, r.name, r.status, r.product_id, r.sku, r.image_url].map(csvCell).join(',')
    );
    fs.writeFileSync(csvPath, [header, ...lines].join('\n') + '\n');

    const withImg = results.filter(r => r.image_url).length;
    const noMatch = results.filter(r => !r.product_id).length;
    console.log(`Wrote ${results.length} rows to ${csvPath}`);
    console.log(`  ${withImg} have an image URL`);
    console.log(`  ${results.length - withImg - noMatch} matched a Mintsoft product but have no image`);
    console.log(`  ${noMatch} had no Mintsoft product match`);

    await closePool();
})().catch(err => { console.error(err.message); process.exit(1); });
