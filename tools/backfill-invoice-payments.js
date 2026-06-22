'use strict';
// Backfill payment instructions for PIs that were uploaded/checked before the
// Gemini check started extracting payment terms. Those invoices have a
// succeeded check row but NO row in purchase_order_invoice_payments, so their
// `payment` field comes back null in GET /purchase-orders/:id/invoices.
//
// For each such invoice this re-runs the same compare() the /check endpoint
// uses, records a fresh succeeded check row (provenance + refreshed
// latestCheck), and upserts the extracted payment instructions via the shared
// upsertInvoicePaymentTerms. payment_status is owned by the operator, so an
// existing arranged/paid status is never touched (the upsert leaves it out).
//
// Safe by default: prints the candidate list and exits. Pass --apply to call
// Gemini (one billed call per invoice) and write rows.
//
// Usage:
//   node tools/backfill-invoice-payments.js                 # dry-run, list all candidates
//   node tools/backfill-invoice-payments.js --apply         # backfill every candidate
//   node tools/backfill-invoice-payments.js --po 307 --apply # one PO only
//   node tools/backfill-invoice-payments.js --limit 5 --apply

require('dotenv').config();
const log = require('../src/lib/logger');
const { getPool, closePool } = require('../src/db');
const { compare, upsertInvoicePaymentTerms } = require('../src/services/po-invoice-check');

function parseArgs(argv) {
    const args = { apply: false, poId: null, limit: null };
    for (let i = 0; i < argv.length; i++) {
        const a = argv[i];
        if (a === '--apply') args.apply = true;
        else if (a === '--po') args.poId = Number(argv[++i]);
        else if (a === '--limit') args.limit = Number(argv[++i]);
        else { console.error(`Unknown argument: ${a}`); process.exit(1); }
    }
    if (args.poId != null && !Number.isInteger(args.poId)) { console.error('--po needs an integer PO id'); process.exit(1); }
    if (args.limit != null && !(Number.isInteger(args.limit) && args.limit > 0)) { console.error('--limit needs a positive integer'); process.exit(1); }
    return args;
}

// PIs on live POs that already passed a check but have no payment row yet.
async function findCandidates(conn, { poId, limit }) {
    const where = ['inv.deleted_at IS NULL', "chk.status = 'succeeded'", 'pay.id IS NULL'];
    const params = [];
    if (poId != null) { where.push('inv.purchase_order_id = ?'); params.push(poId); }
    const sql = `
        SELECT inv.id AS invoice_id, inv.purchase_order_id, inv.filename, po.po_number
          FROM purchase_order_invoices inv
          JOIN purchase_orders po
            ON po.id = inv.purchase_order_id AND po.deleted_at IS NULL
          JOIN purchase_order_invoice_checks chk
            ON chk.purchase_order_invoice_id = inv.id
         LEFT JOIN purchase_order_invoice_payments pay
            ON pay.purchase_order_invoice_id = inv.id
         WHERE ${where.join(' AND ')}
         GROUP BY inv.id, inv.purchase_order_id, inv.filename, po.po_number
         ORDER BY inv.id
         ${limit != null ? 'LIMIT ?' : ''}`;
    if (limit != null) params.push(limit);
    const [rows] = await conn.query(sql, params);
    return rows;
}

// Mirror recordInvoiceCheck()'s success path from src/handlers/orders.js:
// insert a succeeded check row, then upsert the payment terms keyed to it.
async function backfillOne(conn, inv) {
    const purchaseOrderId = inv.purchase_order_id;
    const invoiceId = inv.invoice_id;

    const out = await compare(conn, { purchaseOrderId, invoiceId });
    const verdict = out.check?.overallVerdict || null;
    const discrepancyCount = Array.isArray(out.check?.discrepancies) ? out.check.discrepancies.length : null;
    const resultJson = JSON.stringify(out.check ?? null);
    const modelUsed = out.modelUsed || null;
    const inputTokens = out.usage?.promptTokenCount ?? null;
    const outputTokens = out.usage?.candidatesTokenCount ?? null;
    const totalTokens = out.usage?.totalTokenCount ?? null;

    const [ins] = await conn.query(
        `INSERT INTO purchase_order_invoice_checks
            (purchase_order_invoice_id, status, verdict, discrepancy_count, result_json,
             model_used, input_tokens, output_tokens, total_tokens,
             triggered_by, triggered_by_email)
         VALUES (?, 'succeeded', ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [invoiceId, verdict, discrepancyCount, resultJson, modelUsed,
         inputTokens, outputTokens, totalTokens, 'backfill', null]
    );

    await upsertInvoicePaymentTerms(conn, {
        invoiceId,
        purchaseOrderId,
        checkId: ins.insertId,
        paymentTerms: out.check?.paymentTerms,
        modelUsed,
    });

    const pt = out.check?.paymentTerms || {};
    return { verdict, paymentType: pt.paymentType, amountDue: pt.amountDueNow, currency: pt.currency };
}

(async () => {
    const args = parseArgs(process.argv.slice(2));
    const pool = getPool();
    const conn = await pool.getConnection();
    try {
        const candidates = await findCandidates(conn, args);
        if (!candidates.length) {
            log.info('No invoices need backfilling — every checked PI already has a payment row.');
            return;
        }

        log.info(`${candidates.length} PI(s) have a succeeded check but no payment row:`);
        for (const c of candidates) {
            log.info(`  invoice ${c.invoice_id}  PO ${c.po_number} (#${c.purchase_order_id})  ${c.filename}`);
        }

        if (!args.apply) {
            log.info('');
            log.info('Dry run. Re-run with --apply to extract payment terms (one Gemini call each).');
            return;
        }

        let ok = 0, failed = 0;
        for (const c of candidates) {
            const label = `invoice ${c.invoice_id} (PO ${c.po_number})`;
            try {
                const r = await backfillOne(conn, c);
                ok++;
                const amt = r.amountDue != null ? `${r.amountDue} ${r.currency || ''}`.trim() : 'no amount';
                log.info(`  ✓ ${label}: ${r.paymentType || 'unknown'} / ${amt} [${r.verdict || 'no verdict'}]`);
            } catch (e) {
                failed++;
                log.error(`  ✗ ${label}: ${e.code || ''} ${e.message}`);
            }
        }
        log.info(`Done. ${ok} backfilled, ${failed} failed, ${candidates.length} total.`);
    } finally {
        conn.release();
        await closePool();
    }
})().catch(err => { console.error(err); process.exit(1); });
