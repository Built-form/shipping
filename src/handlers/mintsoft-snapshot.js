require('dotenv').config();
const log = require('../lib/logger');
const { getPool } = require('../db');
const { getProductsByJfCode, getProductStock } = require('../services/mintsoft');
// Note: Database table creation should ideally be handled by a migration script
// and not within the Lambda handler for production environments.
// ── JF codes + ASINs loaded from landed_costs table ──────────────────────────
async function loadJfCodes(conn) {
    const [rows] = await conn.execute(
        `SELECT lc.jf_code, lc.asin, MAX(ss.created_at) as last_updated
         FROM landed_costs lc
         LEFT JOIN stock_snapshots ss ON ss.jf_code = lc.jf_code
         WHERE lc.jf_code IS NOT NULL AND lc.jf_code != '' AND lc.jf_code != 'NULL'
         GROUP BY lc.jf_code, lc.asin
         ORDER BY last_updated ASC, RAND()`
    );
    return rows.map(r => ({ jfCode: r.jf_code.trim(), asin: (r.asin || '').trim() }));
}

// ── Ensure table exists ────────────────────────────────────────────────────────
async function ensureTable(conn) {
    // Assuming 'stock_snapshots' table is managed by migrations.
    const migrations = [
        `ALTER TABLE stock_snapshots ADD COLUMN asin VARCHAR(50) NOT NULL DEFAULT '' AFTER jf_code`,
        `ALTER TABLE stock_snapshots ADD COLUMN warehouse_id INT NOT NULL DEFAULT 0 AFTER product_id`,
        `ALTER TABLE stock_snapshots ADD COLUMN allocated INT NOT NULL DEFAULT 0 AFTER available`,
        `ALTER TABLE stock_snapshots ADD COLUMN quarantine INT NOT NULL DEFAULT 0 AFTER allocated`,
    ];
    for (const sql of migrations) {
        try { await conn.execute(sql); } catch (e) {
            if (!e.message.includes('Duplicate column')) throw e;
        }
    }
}

// ── Retry helper ───────────────────────────────────────────────────────────────
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 2000;

async function withRetry(fn, label) {
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        try {
            return await fn();
        } catch (err) {
            if (attempt === MAX_RETRIES) throw err;
            log.warn(`[${label}] Attempt ${attempt}/${MAX_RETRIES} failed: ${err.message} — retrying in ${RETRY_DELAY_MS}ms...`);
            await new Promise(r => setTimeout(r, RETRY_DELAY_MS * attempt));
        }
    }
}

// ── Main handler ───────────────────────────────────────────────────────────────
const handler = async () => {
    if (!process.env.MINTSOFT_API_KEY) {
        throw new Error('MINTSOFT_API_KEY is not configured');
    }

    const dateRan = new Date().toISOString().slice(0, 10); // YYYY-MM-DD
    const db = await getPool().getConnection();

    try {
        await ensureTable(db);

        const jfCodes = await loadJfCodes(db);
        if (jfCodes.length === 0) {
            log.warn('No JF codes found in landed_costs — nothing to snapshot');
            return { statusCode: 200, body: 'No JF codes found' };
        }

        log.info(`Mintsoft snapshot ${dateRan} — ${jfCodes.length} codes`);

        const results = { success: [], failed: [] };

        // Fetch all stock data in parallel (batches of 10 to avoid rate limits)
        const BATCH_SIZE = 10;
        for (let i = 0; i < jfCodes.length; i += BATCH_SIZE) {
            const batch = jfCodes.slice(i, i + BATCH_SIZE);

            const settled = await Promise.allSettled(
                batch.map(async ({ jfCode, asin }) => {
                    return withRetry(async () => {
                        const products = await getProductsByJfCode(jfCode);
                        const allStocks = [];
                        for (const { productId, sku } of products) {
                            const warehouseStocks = await getProductStock(productId);
                            allStocks.push({ sku, productId, warehouseStocks });
                        }
                        return { jfCode, asin, stocks: allStocks };
                    }, jfCode);
                })
            );

            // Write results to DB
            for (const result of settled) {
                if (result.status === 'fulfilled') {
                    const { jfCode, asin, stocks } = result.value;

                    for (const { sku, productId, warehouseStocks } of stocks) {
                        for (const { warehouseId, stockLevel, available, allocated, quarantine } of warehouseStocks) {
                            const [existing] = await db.execute(
                                `SELECT id FROM stock_snapshots WHERE date_ran = ? AND sku = ? AND warehouse_id = ? LIMIT 1`,
                                [dateRan, sku, warehouseId]
                            );

                            if (existing.length > 0) {
                                await db.execute(
                                    `UPDATE stock_snapshots
                                     SET jf_code = ?, asin = ?, product_id = ?, stock_level = ?, available = ?, allocated = ?, quarantine = ?
                                     WHERE id = ?`,
                                    [jfCode, asin, productId, stockLevel, available, allocated, quarantine, existing[0].id]
                                );
                            } else {
                                await db.execute(
                                    `INSERT INTO stock_snapshots
                                        (date_ran, jf_code, asin, sku, product_id, warehouse_id, stock_level, available, allocated, quarantine)
                                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                                    [dateRan, jfCode, asin, sku, productId, warehouseId, stockLevel, available, allocated, quarantine]
                                );
                            }
                        }
                    }

                    log.info(`[${jfCode}] OK`);
                    results.success.push({ jfCode, stocks });
                } else {
                    const { jfCode } = batch[settled.indexOf(result)];
                    log.error(`[${jfCode}] Failed: ${result.reason?.message}`);
                    results.failed.push({ jfCode, error: result.reason?.message });
                }
            }
        }

        log.info(`Snapshot complete — ${results.success.length} OK, ${results.failed.length} failed`);
        if (results.failed.length > 0) {
            log.warn('Failed JF codes:', results.failed.map(f => f.jfCode).join(', '));
        }
        return { statusCode: 200, body: JSON.stringify(results) };

    } catch (err) {
        log.error('Fatal error during snapshot', err);
        throw err;
    } finally {
        db.release();
    }
};

exports.handler = handler;

// Allow direct execution: node src/handlers/mintsoft-snapshot.js
if (require.main === module) {
    handler().then(result => {
        log.info('Result:', JSON.stringify(result, null, 2));
        process.exit(0);
    }).catch(err => {
        log.error('Unhandled error:', err.message, err.stack);
        process.exit(1);
    });
}
