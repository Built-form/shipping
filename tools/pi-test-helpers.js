'use strict';

// Shared fixtures for the supplier-portal PI suites that run against the TEST
// stage (tools/test-pi-check-flow.js, tools/test-pi-submissions.js,
// tools/test-pi-import-flow.js). Not unit tests: they need the test database,
// the portal's test bucket and AWS credentials.
//
// jportal.pi_submissions rows are created by the portal, and this app's login
// may not INSERT or DELETE them (see docs/pi-submissions-frontend.md), so the
// fixtures are written through a separate connection: FIXTURE_DB_USER /
// FIXTURE_DB_PASSWORD (a login with INSERT + DELETE on <jportal>.pi_submissions
// on TEST), falling back to DB_USER / DB_PASSWORD.

require('dotenv').config();
const crypto = require('crypto');
const mysql = require('mysql2/promise');
const PDFDocument = require('pdfkit');
const { S3Client, PutObjectCommand, DeleteObjectCommand, HeadObjectCommand } = require('@aws-sdk/client-s3');
const { jportalSchema } = require('../src/lib/jportal');

const REGION = process.env.AWS_REGION || 'eu-north-1';
const s3 = new S3Client({ region: REGION });

// Refuses (exit 2) unless the database and the bucket are the test ones.
function refuseUnlessTest() {
    const host = process.env.DB_HOST || '';
    const db = process.env.DB_NAME || '';
    if (!/test/i.test(host) && !/test/i.test(db)) {
        console.error(`Refusing to run: ${host}/${db} does not look like the TEST database.`);
        process.exit(2);
    }
    if (!process.env.PI_UPLOADS_BUCKET) process.env.PI_UPLOADS_BUCKET = 'jportal-pi-uploads-test';
    if (!/-test$/.test(process.env.PI_UPLOADS_BUCKET)) {
        console.error(`Refusing to run: PI_UPLOADS_BUCKET=${process.env.PI_UPLOADS_BUCKET} is not the test bucket.`);
        process.exit(2);
    }
    return { host, db, bucket: process.env.PI_UPLOADS_BUCKET };
}

let fixtureConn = null;
async function fixtures() {
    if (!fixtureConn) {
        fixtureConn = await mysql.createConnection({
            host: process.env.DB_HOST, port: process.env.DB_PORT || 3306, database: process.env.DB_NAME,
            user: process.env.FIXTURE_DB_USER || process.env.DB_USER,
            password: process.env.FIXTURE_DB_PASSWORD || process.env.DB_PASSWORD,
            ssl: { rejectUnauthorized: false },
        });
    }
    return fixtureConn;
}
async function closeFixtures() {
    if (fixtureConn) { await fixtureConn.end(); fixtureConn = null; }
}

// A one-page PDF with these lines of text.
function makePdf(lines) {
    return new Promise((resolve, reject) => {
        const doc = new PDFDocument({ size: 'A4', margin: 50 });
        const chunks = [];
        doc.on('data', c => chunks.push(c));
        doc.on('end', () => resolve(Buffer.concat(chunks)));
        doc.on('error', reject);
        for (const line of lines) doc.fontSize(11).text(line);
        doc.end();
    });
}

// A PI for `po` (loadPoForCheck output): its lines and totals, quoting `quotePo`.
function piLines(po, { quotePo = po.poNumber, bank = null } = {}) {
    const out = [
        'PROFORMA INVOICE', `Seller: ${po.supplier}`, `Buyer: ${po.buyer || 'JFA Medical Ltd'}`,
        `PO number: ${quotePo}`, `Currency: ${po.currency}`, '',
        'SKU | Description | Qty | Unit price | Amount',
    ];
    for (const l of po.lines) out.push(`${l.sku || '-'} | ${l.description} | ${l.quantity} | ${l.unitPrice.toFixed(4)} | ${l.lineTotal.toFixed(2)}`);
    out.push('', `Total: ${po.grandTotal.toFixed(2)} ${po.currency}`, 'Payment: 100% before shipment.');
    if (bank) {
        out.push('', 'Bank details:', `Beneficiary: ${bank.beneficiaryName}`, `Bank: ${bank.bankName}`,
            `Account number: ${bank.accountNumber}`, `SWIFT: ${bank.swiftBic}`);
    }
    return out;
}

const newKey = (ext = 'pdf') => `pi/${crypto.randomUUID()}.${ext}`;

// Puts bytes in the portal bucket, tagged as the portal's signed upload would
// be. → the object's ETag, as S3 returns it (quotes included).
async function putPortalObject(key, bytes, contentType) {
    const out = await s3.send(new PutObjectCommand({
        Bucket: process.env.PI_UPLOADS_BUCKET, Key: key, Body: bytes, ContentType: contentType, Tagging: 'state=draft',
    }));
    return out.ETag;
}
async function deletePortalObject(key) {
    try { await s3.send(new DeleteObjectCommand({ Bucket: process.env.PI_UPLOADS_BUCKET, Key: key })); } catch { /* best effort */ }
}
async function portalObjectExists(key) {
    try { await s3.send(new HeadObjectCommand({ Bucket: process.env.PI_UPLOADS_BUCKET, Key: key })); return true; } catch { return false; }
}

// A PO on the test database with lines, and the supplier id jportal would
// snapshot for it (a live supplier row named exactly po.supplier).
async function pickPo(pool, { excludeIds = [] } = {}) {
    const [rows] = await pool.query(
        `SELECT po.id, po.po_number, po.supplier, s.id AS supplier_id
           FROM purchase_orders po
           JOIN suppliers s ON s.deleted_at IS NULL
                           AND CONVERT(s.name USING utf8mb4) COLLATE utf8mb4_0900_bin = CONVERT(po.supplier USING utf8mb4) COLLATE utf8mb4_0900_bin
          WHERE po.deleted_at IS NULL
            AND EXISTS (SELECT 1 FROM orders o WHERE o.purchase_order_id = po.id AND o.deleted_at IS NULL)
            ${excludeIds.length ? `AND po.id NOT IN (${excludeIds.map(() => '?').join(', ')})` : ''}
            AND NOT EXISTS (SELECT 1 FROM ${jportalSchema()}.pi_submissions p WHERE p.purchase_order_id = po.id AND p.status = 'submitted')
          ORDER BY po.id DESC LIMIT 1`,
        excludeIds
    );
    if (!rows.length) throw new Error('No live PO with lines and a matching supplier on the test database.');
    return rows[0];
}

// Inserts a pi_submissions row as jportal would (create, then whatever state
// the test needs). → id.
async function insertSubmission(fields) {
    const conn = await fixtures();
    const f = {
        user_id: 999999, submitted_by_email: 'pi-test@example.invalid', filename: 'PI-test.pdf',
        content_type: 'application/pdf', file_size: 1, check_status: 'awaiting_upload', status: 'draft', ...fields,
    };
    for (const k of ['result_json', 'po_snapshot_json']) if (f[k] && typeof f[k] !== 'string') f[k] = JSON.stringify(f[k]);
    const cols = Object.keys(f);
    const [ins] = await conn.query(
        `INSERT INTO ${jportalSchema()}.pi_submissions (${cols.join(', ')}) VALUES (${cols.map(() => '?').join(', ')})`,
        cols.map(c => f[c])
    );
    return ins.insertId;
}

async function readSubmission(pool, id) {
    const [rows] = await pool.query(`SELECT * FROM ${jportalSchema()}.pi_submissions WHERE id = ?`, [id]);
    return rows[0] || null;
}

// Removes submissions and what an import made of them (invoices, checks,
// payment rows, audit rows). Imported files in the PO bucket are left (the
// nightly live→test mirror deletes strays).
async function cleanupSubmissions(pool, ids) {
    if (!ids.length) return;
    const conn = await fixtures();
    const [subs] = await pool.query(`SELECT id, s3_key, imported_invoice_id FROM ${jportalSchema()}.pi_submissions WHERE id IN (?)`, [ids]);
    const [invs] = await pool.query(`SELECT id, purchase_order_id FROM purchase_order_invoices WHERE pi_submission_id IN (?)`, [ids]);
    const invIds = invs.map(i => i.id);
    if (invIds.length) {
        await pool.query(`DELETE FROM purchase_order_invoice_checks WHERE purchase_order_invoice_id IN (?)`, [invIds]);
        await pool.query(`DELETE FROM purchase_order_invoice_payments WHERE purchase_order_invoice_id IN (?)`, [invIds]);
        await pool.query(`DELETE FROM purchase_order_invoices WHERE id IN (?)`, [invIds]);
        await pool.query(
            `DELETE FROM audit_log WHERE entity_type = 'purchase_order' AND entity_id IN (?)
                AND JSON_EXTRACT(after_json, '$.invoiceId') IN (?)`,
            [[...new Set(invs.map(i => i.purchase_order_id))], invIds]
        );
    }
    await pool.query(`DELETE FROM audit_log WHERE entity_type = 'pi_submission' AND entity_id IN (?)`, [ids]);
    await conn.query(`DELETE FROM ${jportalSchema()}.pi_submissions WHERE id IN (?)`, [ids]);
    for (const s of subs) await deletePortalObject(s.s3_key);
}

// A check result in RESPONSE_SCHEMA's shape, as piCheck stores it.
function checkResult({ verdict = 'pass', discrepancies = [], bank = null, poNumber = 'PO' } = {}) {
    return {
        summary: { matchedLines: 1, mismatches: discrepancies.length, missingOnInvoice: 0, extraOnInvoice: 0 },
        header: {
            supplierMatches: true, supplierNotes: '', poNumberReferenced: true, currencyMatches: true,
            paymentTermsMatch: true, paymentTermsNotes: '',
        },
        totals: { poGrandTotal: 100, piGrandTotal: 100, delta: 0, notes: '' },
        discrepancies,
        paymentTerms: {
            paymentType: 'full', amountDueNow: 100, currency: 'USD', depositPercentage: null, invoiceTotal: 100,
            dueDate: null, dueTerms: '100% before shipment', beneficiaryName: null, bankName: null, bankAddress: null,
            accountNumber: null, iban: null, swiftBic: null, intermediaryBank: null, paymentReference: poNumber, rawText: null,
            ...(bank || {}),
        },
        otherCharges: [],
        overallVerdict: verdict,
        verdictExplanation: 'test fixture',
    };
}

module.exports = {
    s3, refuseUnlessTest, fixtures, closeFixtures, makePdf, piLines, newKey,
    putPortalObject, deletePortalObject, portalObjectExists, pickPo,
    insertSubmission, readSubmission, cleanupSubmissions, checkResult,
};
