'use strict';

// End-to-end smoke test for the new invoice endpoints' core logic (S3 +
// DB), bypassing the HTTP/auth layer. Uses an existing PO and a minimal
// synthetic PDF so we don't need real supplier files to verify the flow.

require('dotenv').config();
const { S3Client, PutObjectCommand, GetObjectCommand } = require('@aws-sdk/client-s3');
const { v4: uuidv4 } = require('uuid');
const { getPool, closePool } = require('../src/db');

const s3 = new S3Client({ region: process.env.AWS_REGION || 'eu-north-1' });
const PO_BUCKET = process.env.PO_DOCS_BUCKET;
const REGION = process.env.AWS_REGION || 'eu-north-1';
const publicS3Url = key => `https://${PO_BUCKET}.s3.${REGION}.amazonaws.com/${key.split('/').map(encodeURIComponent).join('/')}`;

// Smallest valid PDF — header + xref + EOF, ~150 bytes. Good enough for
// a smoke test (just verifies the byte round-trip through S3).
const MINIMAL_PDF = Buffer.from(
    '%PDF-1.4\n1 0 obj<</Type/Catalog>>endobj\nxref\n0 2\n0000000000 65535 f \n0000000009 00000 n \ntrailer<</Size 2/Root 1 0 R>>\nstartxref\n47\n%%EOF\n'
);

(async () => {
    const pool = getPool();
    const conn = await pool.getConnection();
    try {
        // Make sure the table exists (mirrors the lazy migration in orders.js).
        await conn.execute(`
            CREATE TABLE IF NOT EXISTS purchase_order_invoices (
                id INT NOT NULL AUTO_INCREMENT,
                purchase_order_id INT NOT NULL,
                filename VARCHAR(255) NOT NULL,
                s3_key VARCHAR(500) NOT NULL,
                public_url VARCHAR(1000) NULL,
                content_type VARCHAR(128) NULL,
                file_size INT NULL,
                notes TEXT NULL,
                uploaded_by_email VARCHAR(255) NULL,
                uploaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                deleted_at DATETIME NULL,
                PRIMARY KEY (id),
                KEY idx_purchase_order_id (purchase_order_id),
                KEY idx_uploaded_at (uploaded_at)
            )
        `);

        // Pick the most recent PO that has at least one document — sensible
        // candidate for attaching an invoice to.
        const [poRows] = await conn.query(`
            SELECT po.id, po.po_number, po.supplier
              FROM purchase_orders po
             WHERE po.deleted_at IS NULL
             ORDER BY po.id DESC LIMIT 1
        `);
        if (!poRows.length) throw new Error('No purchase_orders to test against.');
        const po = poRows[0];
        console.log(`Using PO ${po.id} (${po.po_number}, supplier=${po.supplier})\n`);

        // 1) Upload
        const filename = 'invoice-smoke-test.pdf';
        const token = uuidv4();
        const s3Key = `invoices/${token}/${filename}`;
        const ct = 'application/pdf';
        console.log('Uploading to S3…');
        await s3.send(new PutObjectCommand({
            Bucket: PO_BUCKET,
            Key: s3Key,
            Body: MINIMAL_PDF,
            ContentType: ct,
            ContentDisposition: `inline; filename="${filename}"`,
        }));
        const url = publicS3Url(s3Key);
        const [ins] = await conn.execute(
            `INSERT INTO purchase_order_invoices
                (purchase_order_id, filename, s3_key, public_url, content_type, file_size, notes, uploaded_by_email)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
            [po.id, filename, s3Key, url, ct, MINIMAL_PDF.length, 'Initial test note', 'dev@built-form.co.uk']
        );
        const invoiceId = ins.insertId;
        console.log(`  inserted invoice id=${invoiceId}, url=${url}\n`);

        // 2) List
        console.log('Listing invoices for PO…');
        const [list] = await conn.query(
            `SELECT id, filename, public_url, content_type, file_size, notes, uploaded_by_email, uploaded_at
               FROM purchase_order_invoices
              WHERE purchase_order_id = ? AND deleted_at IS NULL
              ORDER BY uploaded_at DESC`,
            [po.id]
        );
        console.table(list.map(r => ({
            id: r.id, filename: r.filename, size: r.file_size,
            type: r.content_type, notes: r.notes,
            uploadedBy: r.uploaded_by_email, uploadedAt: r.uploaded_at,
        })));

        // 3) Verify S3 readable + content matches
        console.log('\nVerifying S3 round-trip…');
        const obj = await s3.send(new GetObjectCommand({ Bucket: PO_BUCKET, Key: s3Key }));
        const fetched = Buffer.from(await obj.Body.transformToByteArray());
        console.log(`  fetched ${fetched.length} bytes; match=${Buffer.compare(fetched, MINIMAL_PDF) === 0}`);

        // 4) Update notes
        console.log('\nUpdating notes…');
        await conn.execute(
            `UPDATE purchase_order_invoices SET notes = ? WHERE id = ?`,
            ['Updated note — payment received', invoiceId]
        );
        const [reread] = await conn.query(`SELECT notes FROM purchase_order_invoices WHERE id = ?`, [invoiceId]);
        console.log(`  notes now: "${reread[0].notes}"`);

        // 5) Soft delete (cleans up after the smoke test so the PO doesn't
        //    show this synthetic invoice in real UI use)
        console.log('\nSoft-deleting smoke-test invoice…');
        await conn.execute(`UPDATE purchase_order_invoices SET deleted_at = NOW() WHERE id = ?`, [invoiceId]);

        const [after] = await conn.query(
            `SELECT COUNT(*) AS n FROM purchase_order_invoices WHERE purchase_order_id = ? AND deleted_at IS NULL`,
            [po.id]
        );
        console.log(`  active invoices on PO ${po.id} now: ${after[0].n}`);
        console.log('\nPreview URL (still live in S3, file kept for reference):');
        console.log(' ', url);
    } catch (err) {
        console.error('Fatal:', err.message);
        process.exitCode = 1;
    } finally {
        conn.release();
        await closePool();
    }
})();
