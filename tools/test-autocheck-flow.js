'use strict';

// Smoke test for auto-fire-on-upload check flow.
// Generates a synthetic PI, inserts an invoice row, runs the check, queries
// purchase_order_invoice_checks to verify the row landed. Soft-deletes the
// test invoice afterwards.

require('dotenv').config();
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const { v4: uuidv4 } = require('uuid');
const PDFDocument = require('pdfkit');
const { getPool, closePool } = require('../src/db');
const { compare } = require('../src/services/po-invoice-check');

const PO_ID = 2;
const PO_BUCKET = process.env.PO_DOCS_BUCKET;
const REGION = process.env.AWS_REGION || 'eu-north-1';
const s3 = new S3Client({ region: REGION });
const publicS3Url = key =>
    `https://${PO_BUCKET}.s3.${REGION}.amazonaws.com/${key.split('/').map(encodeURIComponent).join('/')}`;

function rowToJson(r) {
    if (!r) return null;
    return {
        id: r.id,
        invoiceId: r.purchase_order_invoice_id,
        status: r.status,
        verdict: r.verdict,
        discrepancyCount: r.discrepancy_count,
        triggeredBy: r.triggered_by,
        modelUsed: r.model_used,
        totalTokens: r.total_tokens,
        createdAt: r.created_at,
    };
}

async function recordCheck(conn, invoiceId) {
    try {
        const out = await compare(conn, { purchaseOrderId: PO_ID, invoiceId });
        const [ins] = await conn.execute(
            `INSERT INTO purchase_order_invoice_checks
                (purchase_order_invoice_id, status, verdict, discrepancy_count, result_json,
                 model_used, input_tokens, output_tokens, total_tokens,
                 triggered_by, triggered_by_email)
             VALUES (?, 'succeeded', ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            [
                invoiceId,
                out.check?.overallVerdict || null,
                out.check?.discrepancies?.length ?? 0,
                JSON.stringify(out.check ?? null),
                out.modelUsed,
                out.usage?.promptTokenCount ?? null,
                out.usage?.candidatesTokenCount ?? null,
                out.usage?.totalTokenCount ?? null,
                'auto_upload',
                'test@built-form.co.uk',
            ]
        );
        const [rb] = await conn.query(`SELECT * FROM purchase_order_invoice_checks WHERE id = ?`, [ins.insertId]);
        return rowToJson(rb[0]);
    } catch (err) {
        const [ins] = await conn.execute(
            `INSERT INTO purchase_order_invoice_checks
                (purchase_order_invoice_id, status, error_code, error_message,
                 triggered_by, triggered_by_email)
             VALUES (?, 'failed', ?, ?, ?, ?)`,
            [invoiceId, err.code || null, String(err.message).slice(0, 4000),
             'auto_upload', 'test@built-form.co.uk']
        );
        const [rb] = await conn.query(`SELECT * FROM purchase_order_invoice_checks WHERE id = ?`, [ins.insertId]);
        console.warn('Check failed but row recorded:', err.message);
        return rowToJson(rb[0]);
    }
}

(async () => {
    if (!process.env.GEMINI_API_KEY) {
        console.error('Missing GEMINI_API_KEY in .env');
        process.exit(1);
    }
    const pool = getPool();
    const conn = await pool.getConnection();
    let invoiceId = null;
    try {
        await conn.execute(`
            CREATE TABLE IF NOT EXISTS purchase_order_invoice_checks (
                id INT NOT NULL AUTO_INCREMENT,
                purchase_order_invoice_id INT NOT NULL,
                status ENUM('succeeded', 'failed') NOT NULL,
                verdict VARCHAR(32) NULL,
                discrepancy_count INT NULL,
                result_json JSON NULL,
                model_used VARCHAR(64) NULL,
                input_tokens INT NULL,
                output_tokens INT NULL,
                total_tokens INT NULL,
                error_code VARCHAR(64) NULL,
                error_message TEXT NULL,
                triggered_by VARCHAR(32) NULL,
                triggered_by_email VARCHAR(255) NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                KEY idx_invoice_id (purchase_order_invoice_id),
                KEY idx_created_at (created_at)
            )
        `);

        const [poRows] = await conn.query(
            `SELECT id, po_number, supplier, currency FROM purchase_orders WHERE id = ? AND deleted_at IS NULL`,
            [PO_ID]
        );
        const po = poRows[0];
        const [lineRows] = await conn.query(
            `SELECT jf_code, asin, product_name, SUM(quantity) AS qty, unit_price
               FROM orders
              WHERE purchase_order_id = ? AND deleted_at IS NULL
              GROUP BY jf_code, asin, product_name, unit_price
              ORDER BY product_name
              LIMIT 5`,
            [PO_ID]
        );
        const lines = lineRows.map(r => ({
            sku: r.jf_code || r.asin || null,
            description: r.product_name || '',
            quantity: Number(r.qty),
            unitPrice: r.unit_price != null ? Number(r.unit_price) : 0,
        }));
        lines.splice(1, 1); // drop one to create a discrepancy

        // Build minimal PI PDF
        const pdfBuffer = await new Promise((resolve, reject) => {
            const doc = new PDFDocument({ size: 'A4', margin: 50 });
            const chunks = [];
            doc.on('data', c => chunks.push(c));
            doc.on('end', () => resolve(Buffer.concat(chunks)));
            doc.on('error', reject);
            doc.fontSize(18).font('Helvetica-Bold').text('PROFORMA INVOICE', 50, 60);
            doc.fontSize(10).font('Helvetica').text(`PO Reference: ${po.po_number}`, 50, 100);
            doc.text(`From: ${po.supplier}`, 50, 116);
            let y = 160;
            for (const l of lines) {
                doc.text(`${l.sku || ''}  ${(l.description || '').slice(0, 40)}  qty=${l.quantity}  unit=${l.unitPrice}`, 50, y);
                y += 14;
            }
            doc.font('Helvetica-Bold').text(`TOTAL: ${po.currency || 'USD'} 0.00`, 380, y + 20);
            doc.end();
        });

        const token = uuidv4();
        const filename = `AUTOCHECK-${po.po_number}.pdf`;
        const s3Key = `invoices/${token}/${filename}`;
        await s3.send(new PutObjectCommand({
            Bucket: PO_BUCKET, Key: s3Key, Body: pdfBuffer,
            ContentType: 'application/pdf',
        }));
        const url = publicS3Url(s3Key);
        const [ins] = await conn.execute(
            `INSERT INTO purchase_order_invoices
                (purchase_order_id, filename, s3_key, public_url, content_type, file_size, notes, uploaded_by_email)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
            [PO_ID, filename, s3Key, url, 'application/pdf', pdfBuffer.length,
             'Auto-check smoke test', 'test@built-form.co.uk']
        );
        invoiceId = ins.insertId;
        console.log(`Uploaded test invoice id=${invoiceId} (${pdfBuffer.length} bytes)\n`);

        console.log('Auto-firing check…');
        const t0 = Date.now();
        const row = await recordCheck(conn, invoiceId);
        console.log(`Completed in ${((Date.now() - t0) / 1000).toFixed(1)}s\n`);
        console.log('Check row:');
        console.table([row]);

        const [history] = await conn.query(
            `SELECT id, status, verdict, discrepancy_count, triggered_by, total_tokens, created_at
               FROM purchase_order_invoice_checks
              WHERE purchase_order_invoice_id = ?
              ORDER BY created_at DESC, id DESC`,
            [invoiceId]
        );
        console.log(`\nGET /api/v1/purchase-order-invoices/${invoiceId}/checks would return ${history.length} row(s):`);
        console.table(history);
    } catch (err) {
        console.error('Fatal:', err.message);
        process.exitCode = 1;
    } finally {
        if (invoiceId) {
            try {
                await conn.execute(`UPDATE purchase_order_invoices SET deleted_at = NOW() WHERE id = ?`, [invoiceId]);
                console.log(`\n(cleanup: soft-deleted invoice id=${invoiceId})`);
            } catch (e) { console.warn('Cleanup failed:', e.message); }
        }
        conn.release();
        await closePool();
    }
})();
