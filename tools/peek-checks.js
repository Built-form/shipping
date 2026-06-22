'use strict';

// Side-by-side dump of all succeeded checks on a given invoice — Flash vs
// Pro etc. Zooms in on whether each model caught the missing line and how
// it described each discrepancy.
//
// Usage: node tools/peek-checks.js [invoiceId]   (default 5)

require('dotenv').config();
const { getPool, closePool } = require('../src/db');

const INVOICE_ID = process.argv[2] ? Number(process.argv[2]) : 5;

(async () => {
    const pool = getPool();
    const conn = await pool.getConnection();
    try {
        const [rows] = await conn.query(
            `SELECT id, model_used, status, verdict, discrepancy_count,
                    result_json, total_tokens, created_at
               FROM purchase_order_invoice_checks
              WHERE purchase_order_invoice_id = ?
                AND status = 'succeeded'
              ORDER BY created_at ASC, id ASC`,
            [INVOICE_ID]
        );
        if (!rows.length) { console.log('No succeeded checks for invoice', INVOICE_ID); return; }

        for (const r of rows) {
            const result = typeof r.result_json === 'string' ? JSON.parse(r.result_json) : r.result_json;
            console.log('═══════════════════════════════════════════════════════════════');
            console.log(`Check id=${r.id}  model=${r.model_used}  tokens=${r.total_tokens}`);
            console.log(`Verdict: ${result.overallVerdict}  (${r.discrepancy_count} discrepancies)`);
            console.log('───────────────────────────────────────────────────────────────');
            console.log('Summary:');
            console.log(`  matched lines:      ${result.summary.matchedLines}`);
            console.log(`  mismatches:         ${result.summary.mismatches}`);
            console.log(`  missing on invoice: ${result.summary.missingOnInvoice}`);
            console.log(`  extra on invoice:   ${result.summary.extraOnInvoice}`);

            console.log('\nMissing-on-invoice findings:');
            const missing = (result.discrepancies || []).filter(d => d.type === 'missing_on_invoice');
            if (missing.length === 0) {
                console.log('  (none flagged)');
            } else {
                missing.forEach((d, i) => {
                    console.log(`  ${i + 1}. sku=${d.sku ?? '—'}  po=${d.po ?? '—'}  pi=${d.pi ?? '—'}`);
                    console.log(`     ${d.description}`);
                });
            }

            console.log('\nAll discrepancies (compact):');
            (result.discrepancies || []).forEach((d, i) => {
                const po = d.po ?? '—';
                const pi = d.pi ?? '—';
                console.log(`  ${String(i + 1).padStart(2)}. [${String(d.type).padEnd(24)}] sku=${String(d.sku || '—').padEnd(8)} po=${String(po).slice(0,30).padEnd(30)} pi=${String(pi).slice(0,30)}`);
            });

            console.log('\nVerdict explanation:');
            console.log(`  ${result.verdictExplanation}`);
            console.log('');
        }
    } catch (err) {
        console.error('Fatal:', err.message);
    } finally {
        conn.release();
        await closePool();
    }
})();
