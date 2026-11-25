'use strict';

// One-off smoke test for the /purchase-order-documents/:id/email endpoint.
// Picks the most-recent purchase_order_documents row, fetches the PDF from
// S3, and sends it through SES exactly the way the handler does
// (src/services/email-send.js). Run with:
//
//   node tools/send-po-test.js <recipient1,recipient2,...> [documentId]
//
// Recipients are comma-separated. Document id is optional — defaults to
// the latest row. Needs SES_FROM_ARN (and AWS credentials that may
// ses:SendEmail from it) in the environment.

require('dotenv').config();
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const { getPool, closePool } = require('../src/db');
const { sendEmail, isConfigured } = require('../src/services/email-send');

const recipientArg = process.argv[2];
const explicitDocId = process.argv[3] ? Number(process.argv[3]) : null;

if (!recipientArg) {
    console.error('Usage: node tools/send-po-test.js <recipient1,recipient2,...> [documentId]');
    process.exit(1);
}
const recipients = recipientArg.split(',').map(s => s.trim()).filter(Boolean);
if (!isConfigured()) {
    console.error('Missing or malformed SES_FROM_ARN in env (an email-address identity ARN).');
    process.exit(1);
}
if (!process.env.PO_DOCS_BUCKET) {
    console.error('Missing PO_DOCS_BUCKET in env.');
    process.exit(1);
}

const s3 = new S3Client({ region: process.env.AWS_REGION || 'eu-north-1' });

const PO_EMAIL_BODY_HTML = [
    '<div style="font-family:Helvetica,Arial,sans-serif;font-size:14px;color:#222;line-height:1.5;">',
    '<p>Greetings,</p>',
    '<p>Please see attached PO. Please send back a PI. I will send artwork shortly.</p>',
    '<p>Thank you.</p>',
    '<p>Kind Regards,<br>Operations Team.<br>JFA Medical Ltd.</p>',
    '</div>',
].join('');

(async () => {
    const pool = getPool();
    const conn = await pool.getConnection();
    try {
        const sql = explicitDocId
            ? `SELECT d.id, d.purchase_order_id, d.version, d.s3_key, d.public_url, d.file_size,
                      po.po_number
                 FROM purchase_order_documents d
                 JOIN purchase_orders po ON po.id = d.purchase_order_id
                WHERE d.id = ? AND po.deleted_at IS NULL`
            : `SELECT d.id, d.purchase_order_id, d.version, d.s3_key, d.public_url, d.file_size,
                      po.po_number
                 FROM purchase_order_documents d
                 JOIN purchase_orders po ON po.id = d.purchase_order_id
                WHERE po.deleted_at IS NULL
                ORDER BY d.generated_at DESC
                LIMIT 1`;
        const [rows] = await conn.query(sql, explicitDocId ? [explicitDocId] : []);
        if (!rows.length) {
            console.error('No purchase_order_documents row found.');
            process.exit(1);
        }
        const doc = rows[0];
        console.log('Picked document:', {
            id: doc.id,
            purchaseOrderId: doc.purchase_order_id,
            poNumber: doc.po_number,
            version: doc.version,
            s3Key: doc.s3_key,
            fileSize: doc.file_size,
        });

        console.log('Fetching PDF from S3…');
        const obj = await s3.send(new GetObjectCommand({
            Bucket: process.env.PO_DOCS_BUCKET,
            Key: doc.s3_key,
        }));
        const pdfBytes = Buffer.from(await obj.Body.transformToByteArray());
        console.log(`  ${pdfBytes.length} bytes`);

        const safePoNumber = String(doc.po_number).replace(/[^A-Za-z0-9._-]/g, '_');
        const filename = `${safePoNumber}-v${doc.version}.pdf`;
        const subject = String(doc.po_number);
        const htmlBody = PO_EMAIL_BODY_HTML;

        console.log(`Sending through SES (from=${process.env.SES_FROM_ARN}, to=${recipients.join(', ')})…`);
        const sent = await sendEmail({ kind: 'test.po',
            to: recipients, subject, html: htmlBody,
            attachments: [{ bytes: pdfBytes, filename, contentType: 'application/pdf' }],
        });
        console.log('SES result:', JSON.stringify(sent));
        if (!sent.ok) process.exitCode = 1;
    } catch (err) {
        console.error('Fatal:', err.message);
        process.exitCode = 1;
    } finally {
        conn.release();
        await closePool();
    }
})();
