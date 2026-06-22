'use strict';

// Like tools/test-autocheck-flow.js but DOES NOT clean up — leaves the
// invoice row + check row in the DB so the user can inspect via the GET
// endpoints or in MySQL.

require('dotenv').config();
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const { v4: uuidv4 } = require('uuid');
const PDFDocument = require('pdfkit');
const { getPool, closePool } = require('../src/db');
const { compare } = require('../src/services/po-invoice-check');

const PO_ID = process.argv[2] ? Number(process.argv[2]) : 2;
const PO_BUCKET = process.env.PO_DOCS_BUCKET;
const REGION = process.env.AWS_REGION || 'eu-north-1';
const s3 = new S3Client({ region: REGION });
const publicS3Url = key =>
    `https://${PO_BUCKET}.s3.${REGION}.amazonaws.com/${key.split('/').map(encodeURIComponent).join('/')}`;

(async () => {
    if (!process.env.GEMINI_API_KEY) {
        console.error('Missing GEMINI_API_KEY in .env');
        process.exit(1);
    }
    const pool = getPool();
    const conn = await pool.getConnection();
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
        if (!poRows.length) throw new Error(`PO ${PO_ID} not found.`);
        const po = poRows[0];

        const [lineRows] = await conn.query(
            `SELECT jf_code, asin, product_name, SUM(quantity) AS qty, unit_price
               FROM orders
              WHERE purchase_order_id = ? AND deleted_at IS NULL
              GROUP BY jf_code, asin, product_name, unit_price
              ORDER BY product_name
              LIMIT 6`,
            [PO_ID]
        );
        const lines = lineRows.map(r => ({
            sku: r.jf_code || r.asin || null,
            description: r.product_name || '',
            quantity: Number(r.qty),
            unitPrice: r.unit_price != null ? Number(r.unit_price) : 0,
        }));
        // Deliberate discrepancy: drop one line so the check has something to flag.
        const dropped = lines.splice(1, 1)[0];

        const pdfBuffer = await new Promise((resolve, reject) => {
            const doc = new PDFDocument({ size: 'A4', margin: 50 });
            const chunks = [];
            doc.on('data', c => chunks.push(c));
            doc.on('end', () => resolve(Buffer.concat(chunks)));
            doc.on('error', reject);
            doc.fontSize(20).font('Helvetica-Bold').text('PROFORMA INVOICE', 50, 60);
            doc.fontSize(10).font('Helvetica');
            doc.text(`PO Reference: ${po.po_number}`, 50, 100);
            doc.text(`From: ${po.supplier}`, 50, 116);
            doc.text(`Currency: ${po.currency || 'USD'}`, 50, 132);
            doc.text(`Payment Terms: 50% deposit, 50% before shipment`, 50, 148);
            let y = 190;
            for (const l of lines) {
                doc.text(
                    `${l.sku || ''}  ${(l.description || '').slice(0, 40)}  qty=${l.quantity}  unit=${l.unitPrice}`,
                    50, y
                );
                y += 14;
            }
            doc.font('Helvetica-Bold').text(`TOTAL: ${po.currency || 'USD'} 0.00`, 380, y + 20);
            doc.end();
        });

        const token = uuidv4();
        const filename = `KEEPER-${po.po_number}-${Date.now()}.pdf`;
        const s3Key = `invoices/${token}/${filename}`;
        await s3.send(new PutObjectCommand({
            Bucket: PO_BUCKET, Key: s3Key, Body: pdfBuffer,
            ContentType: 'application/pdf',
            ContentDisposition: `inline; filename="${filename}"`,
        }));
        const url = publicS3Url(s3Key);

        const [insInv] = await conn.execute(
            `INSERT INTO purchase_order_invoices
                (purchase_order_id, filename, s3_key, public_url, content_type, file_size, notes, uploaded_by_email)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
            [PO_ID, filename, s3Key, url, 'application/pdf', pdfBuffer.length,
             `Test invoice — dropped line "${dropped?.description?.slice(0, 40) || ''}"`,
             'test@built-form.co.uk']
        );
        const invoiceId = insInv.insertId;
        console.log(`\nInserted invoice id=${invoiceId} (PO ${po.po_number})`);
        console.log(`  url: ${url}\n`);

        console.log('Auto-firing check…');
        const t0 = Date.now();
        let checkId, status, verdict, discrepancyCount;
        try {
            const out = await compare(conn, { purchaseOrderId: PO_ID, invoiceId });
            const [insChk] = await conn.execute(
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
            checkId = insChk.insertId;
            status = 'succeeded';
            verdict = out.check?.overallVerdict;
            discrepancyCount = out.check?.discrepancies?.length ?? 0;
        } catch (err) {
            const [insChk] = await conn.execute(
                `INSERT INTO purchase_order_invoice_checks
                    (purchase_order_invoice_id, status, error_code, error_message,
                     triggered_by, triggered_by_email)
                 VALUES (?, 'failed', ?, ?, ?, ?)`,
                [invoiceId, err.code || null, String(err.message).slice(0, 4000),
                 'auto_upload', 'test@built-form.co.uk']
            );
            checkId = insChk.insertId;
            status = 'failed';
        }
        const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
        console.log(`Check ${status} in ${elapsed}s\n`);

        console.log('────────── Rows left in DB (NOT cleaned up) ──────────');
        console.log(`  purchase_order_invoices.id        = ${invoiceId}`);
        console.log(`  purchase_order_invoice_checks.id  = ${checkId}`);
        console.log(`  status                            = ${status}`);
        console.log(`  verdict                           = ${verdict || '(n/a)'}`);
        console.log(`  discrepancy_count                 = ${discrepancyCount ?? '(n/a)'}`);
        console.log('\nInspect via SQL:');
        console.log(`  SELECT * FROM purchase_order_invoices WHERE id = ${invoiceId};`);
        console.log(`  SELECT * FROM purchase_order_invoice_checks WHERE id = ${checkId};`);
        console.log('\nInspect via API:');
        console.log(`  GET /api/v1/purchase-orders/${PO_ID}/invoices                       ← list with latestCheck`);
        console.log(`  GET /api/v1/purchase-order-invoices/${invoiceId}/checks                ← full check history`);
        console.log('\nTo clean up later:');
        console.log(`  UPDATE purchase_order_invoices SET deleted_at = NOW() WHERE id = ${invoiceId};`);
        console.log(`  DELETE FROM purchase_order_invoice_checks WHERE id = ${checkId};`);
    } catch (err) {
        console.error('Fatal:', err.message);
        process.exitCode = 1;
    } finally {
        conn.release();
        await closePool();
    }
})();
