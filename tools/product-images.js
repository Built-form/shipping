'use strict';
// Fetch the Mintsoft image URL for each product we already track in
// jfa.mintsoft_carton_sizes (561 rows), rather than paging the whole 34k-product
// catalogue (which trips Mintsoft's Cloudflare IP rate-limit).
//
// Output: TSV  jf_code  sku  product_id  image_url
//
// Usage:
//   node tools/product-images.js                 # all rows, TSV to stdout
//   node tools/product-images.js --with-image    # only products that have an image
//   node tools/product-images.js --missing       # only products with NO image
//   node tools/product-images.js --limit 10      # first N (handy for a quick test)
//   node tools/product-images.js --csv out.csv   # also write a CSV
//   node tools/product-images.js --concurrency 2 # tune request parallelism (default 3)
require('dotenv').config();
const fs = require('fs');
const { getPool, closePool } = require('../src/db');
const { getProductImageUrl } = require('../src/services/mintsoft');

const args = process.argv.slice(2);
const has = (f) => args.includes(f);
const val = (f, d) => { const i = args.indexOf(f); return i !== -1 ? args[i + 1] : d; };

const withImageOnly = has('--with-image');
const missingOnly = has('--missing');
const limit = Number(val('--limit', 0)) || 0;
const concurrency = Math.max(1, Number(val('--concurrency', 3)) || 3);
const csvPath = val('--csv', null);

const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const csvCell = (v) => {
    const s = String(v ?? '');
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
};

(async () => {
    const pool = getPool();
    const conn = await pool.getConnection();
    let products;
    try {
        const [rows] = await conn.query(
            `SELECT jf_code, sku, product_id
               FROM jfa.mintsoft_carton_sizes
              WHERE product_id IS NOT NULL
              ORDER BY jf_code` + (limit ? ` LIMIT ${limit}` : '')
        );
        products = rows;
    } finally {
        conn.release();
    }

    const results = [];
    let done = 0;
    // Process in small concurrent waves with a pause between them: enough
    // parallelism to be quick, paced low enough to stay under the Cloudflare
    // rate-limit. getProductImageUrl() itself backs off if we still get throttled.
    for (let i = 0; i < products.length; i += concurrency) {
        const wave = products.slice(i, i + concurrency);
        await Promise.all(wave.map(async (p) => {
            let imageUrl = '';
            try {
                imageUrl = await getProductImageUrl(p.product_id);
            } catch (err) {
                process.stderr.write(`\n${p.jf_code} (id ${p.product_id}): ${err.message}\n`);
            }
            results.push({ jf_code: p.jf_code, sku: p.sku, product_id: p.product_id, image_url: imageUrl });
        }));
        done += wave.length;
        process.stderr.write(`Fetched ${done}/${products.length}...\r`);
        if (i + concurrency < products.length) await sleep(200);
    }
    process.stderr.write('\n');

    results.sort((a, b) => String(a.jf_code).localeCompare(String(b.jf_code)));
    let out = results;
    if (withImageOnly) out = out.filter(r => r.image_url);
    if (missingOnly) out = out.filter(r => !r.image_url);

    for (const r of out) {
        console.log(`${r.jf_code}\t${r.sku}\t${r.product_id}\t${r.image_url}`);
    }
    const withImg = results.filter(r => r.image_url).length;
    console.log(`\n(${out.length} shown; ${withImg}/${results.length} have an image URL)`);

    if (csvPath) {
        const header = 'jf_code,sku,product_id,image_url';
        const lines = out.map(r => [r.jf_code, r.sku, r.product_id, r.image_url].map(csvCell).join(','));
        fs.writeFileSync(csvPath, [header, ...lines].join('\n') + '\n');
        console.log(`Wrote ${out.length} rows to ${csvPath}`);
    }

    await closePool();
})().catch(err => { console.error(err.message); process.exit(1); });
