'use strict';

// Re-runs the auto-check on an existing invoice using Gemini Pro instead of
// Flash, inserts a fresh check row, and prints a side-by-side comparison
// against the most-recent Flash check on the same invoice.
//
// Usage: node tools/fire-pro-check.js [invoiceId] [model]
//   defaults: invoiceId=5, model=gemini-3.1-pro-preview

require('dotenv').config();
const { getPool, closePool } = require('../src/db');
const { compare } = require('../src/services/po-invoice-check');

const INVOICE_ID = process.argv[2] ? Number(process.argv[2]) : 5;
const MODEL = process.argv[3] || 'gemini-3.1-pro-preview';

(async () => {
    if (!process.env.GEMINI_API_KEY) {
        console.error('Missing GEMINI_API_KEY in .env');
        process.exit(1);
    }
    const pool = getPool();
    const conn = await pool.getConnection();
    try {
        const [invRows] = await conn.query(
            `SELECT id, purchase_order_id, filename, file_size
               FROM purchase_order_invoices
              WHERE id = ? AND deleted_at IS NULL`,
            [INVOICE_ID]
        );
        if (!invRows.length) throw new Error(`Invoice ${INVOICE_ID} not found.`);
        const invoice = invRows[0];
        console.log(`Invoice ${invoice.id} (PO ${invoice.purchase_order_id}): ${invoice.filename}, ${invoice.file_size} bytes`);
        console.log(`Running check with model: ${MODEL}\n`);

        const t0 = Date.now();
        let checkId, success = true, errMsg = null;
        try {
            const out = await compare(conn, {
                purchaseOrderId: invoice.purchase_order_id,
                invoiceId: invoice.id,
                model: MODEL,
            });
            const [ins] = await conn.execute(
                `INSERT INTO purchase_order_invoice_checks
                    (purchase_order_invoice_id, status, verdict, discrepancy_count, result_json,
                     model_used, input_tokens, output_tokens, total_tokens,
                     triggered_by, triggered_by_email)
                 VALUES (?, 'succeeded', ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                [
                    invoice.id,
                    out.check?.overallVerdict || null,
                    out.check?.discrepancies?.length ?? 0,
                    JSON.stringify(out.check ?? null),
                    out.modelUsed,
                    out.usage?.promptTokenCount ?? null,
                    out.usage?.candidatesTokenCount ?? null,
                    out.usage?.totalTokenCount ?? null,
                    'manual',
                    'test@built-form.co.uk',
                ]
            );
            checkId = ins.insertId;
        } catch (err) {
            success = false;
            errMsg = err.message;
            const [ins] = await conn.execute(
                `INSERT INTO purchase_order_invoice_checks
                    (purchase_order_invoice_id, status, error_code, error_message,
                     model_used, triggered_by, triggered_by_email)
                 VALUES (?, 'failed', ?, ?, ?, ?, ?)`,
                [invoice.id, err.code || null, String(err.message).slice(0, 4000),
                 MODEL, 'manual', 'test@built-form.co.uk']
            );
            checkId = ins.insertId;
        }
        const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
        console.log(`${success ? 'Succeeded' : 'Failed'} in ${elapsed}s (check id=${checkId})`);
        if (!success) console.log(`Error: ${errMsg}\n`);

        // Comparison — pull all checks on this invoice, group by model
        const [allChecks] = await conn.query(
            `SELECT id, status, verdict, discrepancy_count, model_used,
                    input_tokens, output_tokens, total_tokens, created_at,
                    error_code, error_message,
                    TIMESTAMPDIFF(SECOND, created_at, NOW()) AS age_seconds
               FROM purchase_order_invoice_checks
              WHERE purchase_order_invoice_id = ?
              ORDER BY created_at DESC, id DESC`,
            [invoice.id]
        );
        console.log(`\nAll checks on invoice ${invoice.id} (${allChecks.length} total):`);
        console.table(allChecks.map(r => ({
            id: r.id,
            model: r.model_used,
            status: r.status,
            verdict: r.verdict,
            count: r.discrepancy_count,
            inTok: r.input_tokens,
            outTok: r.output_tokens,
            totalTok: r.total_tokens,
            createdAt: r.created_at,
        })));

        // Side-by-side compare: latest per distinct model
        const seenModels = new Map();
        for (const r of allChecks) {
            if (r.status !== 'succeeded') continue;
            if (!seenModels.has(r.model_used)) seenModels.set(r.model_used, r);
        }
        if (seenModels.size >= 2) {
            console.log('\nSide-by-side latest succeeded per model:');
            const cols = ['model', 'verdict', 'discrepancyCount', 'inputTokens', 'outputTokens', 'totalTokens'];
            const rows = [...seenModels.values()].map(r => ({
                model: r.model_used,
                verdict: r.verdict,
                discrepancyCount: r.discrepancy_count,
                inputTokens: r.input_tokens,
                outputTokens: r.output_tokens,
                totalTokens: r.total_tokens,
            }));
            console.table(rows, cols);
        }

        console.log(`\nLeft in DB. To inspect:`);
        console.log(`  SELECT * FROM purchase_order_invoice_checks WHERE purchase_order_invoice_id = ${invoice.id};`);
    } catch (err) {
        console.error('Fatal:', err.message);
        if (err.code) console.error('Code:', err.code);
        process.exitCode = 1;
    } finally {
        conn.release();
        await closePool();
    }
})();
