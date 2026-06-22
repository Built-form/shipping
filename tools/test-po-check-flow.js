'use strict';

// End-to-end test for POST /purchase-orders/:poId/invoices/:invoiceId/check.
//
// 1. Load PO 2 (INTCO-08) + its 16 line items from DB.
// 2. Generate a "supplier" proforma invoice PDF with DELIBERATE discrepancies
//    so we can verify the check actually catches them:
//      a) Inflate the unit price on the first line by 10%.
//      b) Drop the second line entirely (missing_on_invoice).
//      c) Slightly mis-spell the supplier name ("INTCO Medical Inc.").
//      d) Add a $50 "Handling" shipping line not on the PO.
//      e) Reference the wrong PO number in the header ("INTCO-7" instead of "INTCO-08").
// 3. Upload it to S3 under the invoices/ prefix.
// 4. Insert a purchase_order_invoices row.
// 5. Run the compare() service directly (skips HTTP/auth layer).
// 6. Print the discrepancy report so we can eyeball what Gemini caught.
// 7. Soft-delete the test invoice row so it doesn't pollute real data.

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

async function buildTestInvoicePdf({ supplierName, invoiceNumber, poRefOnInvoice, currency, lines, extraShipping }) {
    return await new Promise((resolve, reject) => {
        const doc = new PDFDocument({ size: 'A4', margin: 50 });
        const chunks = [];
        doc.on('data', c => chunks.push(c));
        doc.on('end', () => resolve(Buffer.concat(chunks)));
        doc.on('error', reject);

        doc.fontSize(22).font('Helvetica-Bold').text('PROFORMA INVOICE', 50, 60);
        doc.fontSize(10).font('Helvetica-Bold');
        doc.text('Invoice No:', 50, 110, { continued: true })
            .font('Helvetica').text(`   ${invoiceNumber}`);
        doc.font('Helvetica-Bold')
            .text('PO Reference:', 50, 125, { continued: true })
            .font('Helvetica').text(`   ${poRefOnInvoice}`);
        doc.font('Helvetica-Bold')
            .text('Currency:', 50, 140, { continued: true })
            .font('Helvetica').text(`   ${currency}`);
        doc.font('Helvetica-Bold')
            .text('Payment Terms:', 50, 155, { continued: true })
            .font('Helvetica').text('   50% deposit, 50% before shipment');

        doc.fontSize(10).font('Helvetica-Bold').text('From:', 50, 195);
        doc.font('Helvetica').text(supplierName, 50, 210);
        doc.text('Address line 1, Address line 2', 50, 222);
        doc.text('Some City, China', 50, 234);

        doc.font('Helvetica-Bold').text('To:', 320, 195);
        doc.font('Helvetica').text('JFA Medical Ltd', 320, 210);
        doc.text('Unit B Prestige House, Cornford Road', 320, 222);
        doc.text('Blackpool, FY4 4QQ, United Kingdom', 320, 234);

        // Line items table
        let y = 290;
        doc.lineWidth(0.5).rect(50, y, 495, 24).fillAndStroke('#e6e6e6', '#000');
        doc.fillColor('#000').font('Helvetica-Bold').fontSize(10);
        doc.text('SKU', 55, y + 7, { width: 80 });
        doc.text('Description', 140, y + 7, { width: 220 });
        doc.text('Qty', 360, y + 7, { width: 40, align: 'right' });
        doc.text('Unit', 405, y + 7, { width: 60, align: 'right' });
        doc.text('Line Total', 470, y + 7, { width: 70, align: 'right' });
        y += 24;

        let subtotal = 0;
        for (const l of lines) {
            doc.lineWidth(0.3).rect(50, y, 495, 22).stroke('#888');
            doc.font('Helvetica').fontSize(9).fillColor('#000');
            doc.text(l.sku || '', 55, y + 7, { width: 80 });
            doc.text((l.description || '').slice(0, 60), 140, y + 7, { width: 220 });
            doc.text(String(l.quantity), 360, y + 7, { width: 40, align: 'right' });
            doc.text(l.unitPrice.toFixed(4), 405, y + 7, { width: 60, align: 'right' });
            doc.text(l.lineTotal.toFixed(2), 470, y + 7, { width: 70, align: 'right' });
            subtotal += l.lineTotal;
            y += 22;
            if (y > 720) { doc.addPage(); y = 60; }
        }

        const shipping = extraShipping || 0;
        const total = subtotal + shipping;
        y += 14;
        doc.font('Helvetica-Bold').fontSize(10);
        doc.text('Subtotal:', 380, y, { continued: true, width: 100 });
        doc.font('Helvetica').text(`   ${currency} ${subtotal.toFixed(2)}`);
        y += 16;
        if (shipping > 0) {
            doc.font('Helvetica-Bold').text('Handling / Shipping:', 380, y, { continued: true });
            doc.font('Helvetica').text(`   ${currency} ${shipping.toFixed(2)}`);
            y += 16;
        }
        doc.font('Helvetica-Bold').fontSize(12);
        doc.text('TOTAL:', 380, y, { continued: true });
        doc.font('Helvetica-Bold').text(`   ${currency} ${total.toFixed(2)}`);

        doc.end();
    });
}

(async () => {
    if (!process.env.GEMINI_API_KEY) {
        console.error('Missing GEMINI_API_KEY in .env');
        process.exit(1);
    }
    const pool = getPool();
    const conn = await pool.getConnection();
    let insertedId = null;
    try {
        // Load PO
        const [poRows] = await conn.query(
            `SELECT id, po_number, supplier, currency, shipping_total
               FROM purchase_orders WHERE id = ? AND deleted_at IS NULL`,
            [PO_ID]
        );
        if (!poRows.length) throw new Error(`PO ${PO_ID} not found.`);
        const po = poRows[0];
        const [lineRows] = await conn.query(
            `SELECT jf_code, asin, product_name, SUM(quantity) AS qty, unit_price
               FROM orders
              WHERE purchase_order_id = ? AND deleted_at IS NULL
              GROUP BY jf_code, asin, product_name, unit_price
              ORDER BY product_name`,
            [PO_ID]
        );
        const lines = lineRows.map(r => ({
            sku: r.jf_code || r.asin || null,
            description: r.product_name || '',
            quantity: Number(r.qty),
            unitPrice: r.unit_price != null ? Number(r.unit_price) : 0,
        })).map(l => ({ ...l, lineTotal: Number((l.quantity * l.unitPrice).toFixed(4)) }));

        if (lines.length < 3) throw new Error('PO has too few lines to build a meaningful test.');

        console.log(`PO ${po.po_number} (${po.supplier}, ${po.currency}) — ${lines.length} lines\n`);

        // Inject deliberate discrepancies
        const injected = JSON.parse(JSON.stringify(lines));
        // a) Inflate line 0 unit price by 10%
        const origUnit = injected[0].unitPrice;
        injected[0].unitPrice = Number((origUnit * 1.10).toFixed(4));
        injected[0].lineTotal = Number((injected[0].quantity * injected[0].unitPrice).toFixed(2));
        // b) Drop line 1
        const dropped = injected.splice(1, 1)[0];
        // c) Inject extra shipping/handling
        const extraShipping = 50;
        // d/e) Mis-spelled supplier + wrong PO ref
        const wrongSupplier = (po.supplier || 'Supplier') + ' Inc.';
        const wrongPoRef = po.po_number.replace(/\d+/, m => String(Math.max(1, Number(m) - 1)));

        console.log('Injected discrepancies:');
        console.log(`  a) Line 0 (${injected[0].sku}) unit price: ${origUnit} → ${injected[0].unitPrice}`);
        console.log(`  b) Dropped line: ${dropped.sku} — "${dropped.description.slice(0,40)}" qty=${dropped.quantity}`);
        console.log(`  c) Added handling: ${po.currency} ${extraShipping}`);
        console.log(`  d) Supplier on invoice: "${wrongSupplier}" (PO: "${po.supplier}")`);
        console.log(`  e) PO ref on invoice: "${wrongPoRef}" (real: "${po.po_number}")\n`);

        // Build the PDF
        const pdfBuffer = await buildTestInvoicePdf({
            supplierName: wrongSupplier,
            invoiceNumber: 'TEST-INV-' + Date.now(),
            poRefOnInvoice: wrongPoRef,
            currency: po.currency || 'USD',
            lines: injected,
            extraShipping,
        });
        console.log(`Generated test invoice PDF: ${pdfBuffer.length} bytes`);

        // Upload to S3 + insert invoice row
        const token = uuidv4();
        const filename = `TEST-INV-${po.po_number}.pdf`;
        const s3Key = `invoices/${token}/${filename}`;
        await s3.send(new PutObjectCommand({
            Bucket: PO_BUCKET, Key: s3Key, Body: pdfBuffer,
            ContentType: 'application/pdf',
            ContentDisposition: `inline; filename="${filename}"`,
        }));
        const url = publicS3Url(s3Key);

        const [ins] = await conn.execute(
            `INSERT INTO purchase_order_invoices
                (purchase_order_id, filename, s3_key, public_url, content_type, file_size, notes, uploaded_by_email)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
            [PO_ID, filename, s3Key, url, 'application/pdf', pdfBuffer.length,
             'Synthetic test invoice with deliberate discrepancies', 'test@built-form.co.uk']
        );
        insertedId = ins.insertId;
        console.log(`Inserted invoice id=${insertedId}, url=${url}\n`);

        // Call the service
        console.log('Calling compare()…');
        const result = await compare(conn, { purchaseOrderId: PO_ID, invoiceId: insertedId });
        console.log('\n──── Result ────');
        console.log(`Verdict:   ${result.check.overallVerdict}`);
        console.log(`Model:     ${result.modelUsed}`);
        console.log(`Tokens:    in=${result.usage?.promptTokenCount ?? '?'} out=${result.usage?.candidatesTokenCount ?? '?'} total=${result.usage?.totalTokenCount ?? '?'}`);
        console.log(`\nSummary:`);
        console.log(`  matched lines:      ${result.check.summary.matchedLines}`);
        console.log(`  mismatches:         ${result.check.summary.mismatches}`);
        console.log(`  missing on invoice: ${result.check.summary.missingOnInvoice}`);
        console.log(`  extra on invoice:   ${result.check.summary.extraOnInvoice}`);
        console.log(`\nHeader:`);
        console.log(`  supplierMatches:     ${result.check.header.supplierMatches}  — ${result.check.header.supplierNotes}`);
        console.log(`  poNumberReferenced:  ${result.check.header.poNumberReferenced}`);
        console.log(`  currencyMatches:     ${result.check.header.currencyMatches}`);
        console.log(`  paymentTermsMatch:   ${result.check.header.paymentTermsMatch}  — ${result.check.header.paymentTermsNotes}`);
        console.log(`\nTotals:`);
        console.log(`  PO grand total:  ${result.check.totals.poGrandTotal}`);
        console.log(`  PI grand total:  ${result.check.totals.piGrandTotal}`);
        console.log(`  Delta:           ${result.check.totals.delta}`);
        console.log(`  Notes: ${result.check.totals.notes}`);
        console.log(`\nDiscrepancies (${result.check.discrepancies.length}):`);
        result.check.discrepancies.forEach((d, i) => {
            console.log(`  ${i + 1}. [${d.type}] sku=${d.sku ?? '—'} po=${d.po ?? '—'} pi=${d.pi ?? '—'}`);
            console.log(`     ${d.description}`);
        });
        console.log(`\nVerdict explanation:`);
        console.log(`  ${result.check.verdictExplanation}`);
    } catch (err) {
        console.error('Fatal:', err.message);
        if (err.code) console.error('Code:', err.code);
        if (err.cause) console.error('Cause:', err.cause.message);
        process.exitCode = 1;
    } finally {
        if (insertedId) {
            try {
                await conn.execute(
                    `UPDATE purchase_order_invoices SET deleted_at = NOW() WHERE id = ?`,
                    [insertedId]
                );
                console.log(`\n(cleanup: soft-deleted test invoice id=${insertedId})`);
            } catch (e) { console.warn('Cleanup failed:', e.message); }
        }
        conn.release();
        await closePool();
    }
})();
