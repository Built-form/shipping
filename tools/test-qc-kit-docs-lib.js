'use strict';

// Unit tests for src/lib/qc-kit-docs.js — QC inspection kits read from JFPRO
// (view qc_kit_documents): the ?jfCodes= parser, grouping view rows by
// JF code with de-duplication, kit status for kit_missing, and the documents
// (and file names) attached to the inspector email. No database.
//   node --test tools/test-qc-kit-docs-lib.js

const test = require('node:test');
const assert = require('node:assert/strict');
const K = require('../src/lib/qc-kit-docs');

// A view row (snake_case, as mysql2 returns it).
const row = (over = {}) => ({
    jf_code: 'JF0208', product_id: 12, product_name: 'Widget', document_id: 501, document_name: 'QC plan',
    version_id: 9001, version_number: 3, s3_key: 'documents/501/v3/qc-plan.pdf', filename: 'qc-plan.pdf',
    mime_type: 'application/pdf', size_bytes: 120000, uploaded_by: 'ops@example.com',
    uploaded_at: new Date('2026-10-20T09:15:00Z'), attached_via: 'product', ...over,
});

// ── ?jfCodes= ───────────────────────────────────────────────────────────────

test('parseJfCodesParam: normalised, _FQC stripped, blanks dropped, de-duplicated, request order', () => {
    assert.deepEqual(K.parseJfCodesParam(' jf0208 ,JF0101_fqc,, JF0208_FQC ,jf0101,JF9'), { codes: ['JF0208', 'JF0101', 'JF9'] });
    assert.deepEqual(K.parseJfCodesParam(['JF2,jf1', 'JF2']), { codes: ['JF2', 'JF1'] }, 'a repeated parameter');
});

test('parseJfCodesParam: missing, empty, not text, or over 500 codes is an error', () => {
    for (const bad of [undefined, null, '', ' , ,', [], ['', ' '], { a: 'JF1' }, ['JF1', { a: 1 }], 7]) {
        assert.ok(K.parseJfCodesParam(bad).error, JSON.stringify(bad));
    }
    const codes = n => Array.from({ length: n }, (_, i) => `JF${i}`).join(',');
    assert.equal(K.parseJfCodesParam(codes(500)).codes.length, 500);
    assert.match(K.parseJfCodesParam(codes(501)).error, /At most 500/);
    assert.equal(K.parseJfCodesParam(`${codes(500)},JF0_FQC,jf1`).codes.length, 500, 'counted after normalising');
    assert.equal(K.MAX_KIT_CODES, 500);
});

// ── rows → kits ─────────────────────────────────────────────────────────────

test('docFromRow: the QcKitDocument fields (camelCase, ISO time) plus the internal s3Key', () => {
    assert.deepEqual(K.docFromRow(row({ size_bytes: '120000', document_id: '501' })), {
        documentId: 501, versionId: 9001, version: 3, name: 'QC plan', filename: 'qc-plan.pdf',
        mimeType: 'application/pdf', sizeBytes: 120000, uploadedBy: 'ops@example.com',
        uploadedAt: '2026-10-20T09:15:00.000Z', attachedVia: 'product', s3Key: 'documents/501/v3/qc-plan.pdf',
    });
    const bare = K.docFromRow(row({ document_name: null, mime_type: null, size_bytes: null, uploaded_by: null, uploaded_at: '2026-10-20 09:15:00', attached_via: 'product_group' }));
    assert.equal(bare.name, 'qc-plan.pdf', 'a nameless document is called by its file name');
    assert.equal(bare.mimeType, null);
    assert.equal(bare.sizeBytes, null);
    assert.equal(bare.uploadedBy, null);
    assert.equal(bare.uploadedAt, '2026-10-20T09:15:00.000Z', "MySQL 'YYYY-MM-DD HH:MM:SS' is UTC");
    assert.equal(bare.attachedVia, 'product_group');
    assert.equal(K.docFromRow(row({ uploaded_at: null })).uploadedAt, null);
});

test('kitsFromRows: one kit per requested code in order, even without documents', () => {
    const kits = K.kitsFromRows(['jf0999', 'JF0208_FQC', 'JF0208'], [row(), row({ jf_code: 'JF7777', document_id: 1 })]);
    assert.deepEqual(kits.map(k => k.jfCode), ['JF0999', 'JF0208'], 'codes normalised and de-duplicated; rows of other codes ignored');
    assert.deepEqual(kits[0], { jfCode: 'JF0999', productId: null, productName: null, documents: [] });
    assert.equal(kits[1].productId, 12);
    assert.equal(kits[1].productName, 'Widget');
    assert.deepEqual(kits[1].documents.map(d => d.documentId), [501]);
    assert.deepEqual(K.kitsFromRows([], [row()]), []);
    assert.equal(K.kitsFromRows(['JF0208'], [row({ jf_code: ' jf0208 ' })])[0].documents.length, 1, 'row codes are normalised too');
});

test('kitsFromRows: a document reachable directly and through groups appears once, as product', () => {
    for (const order of [['product_group', 'product', 'product_group'], ['product', 'product_group'], ['product_group', 'product_group', 'product']]) {
        const rows = order.map(via => row({ attached_via: via }));
        const docs = K.kitsFromRows(['JF0208'], rows)[0].documents;
        assert.equal(docs.length, 1, order.join());
        assert.equal(docs[0].attachedVia, 'product', order.join());
    }
    const viaGroups = K.kitsFromRows(['JF0208'], [row({ attached_via: 'product_group' }), row({ attached_via: 'product_group' })])[0].documents;
    assert.deepEqual(viaGroups.map(d => d.attachedVia), ['product_group'], 'two groups, one document');
});

test('kitsFromRows: documents sort by name (case-insensitive), then documentId', () => {
    const rows = [
        row({ document_id: 30, document_name: 'beta' }),
        row({ document_id: 20, document_name: 'Alpha' }),
        row({ document_id: 10, document_name: 'beta' }),
        row({ document_id: 40, document_name: 'alpha 2' }),
    ];
    assert.deepEqual(K.kitsFromRows(['JF0208'], rows)[0].documents.map(d => d.documentId), [20, 40, 10, 30]);
});

test('kitsFromRows: with several JFPRO products on one code, the lowest product id names the kit', () => {
    const kit = K.kitsFromRows(['JF0208'], [
        row({ product_id: 40, product_name: 'Later', document_id: 1 }),
        row({ product_id: 12, product_name: 'First', document_id: 2 }),
    ])[0];
    assert.equal(kit.productId, 12);
    assert.equal(kit.productName, 'First');
    assert.equal(kit.documents.length, 2);
});

test('kitStatusFromRows: docs or none per requested code (for kit_missing)', () => {
    assert.deepEqual(K.kitStatusFromRows(['JF1', 'jf2_fqc', 'JF3', ''], [{ jf_code: 'JF2' }, { jf_code: 'JF9' }]),
        { JF1: 'none', JF2: 'docs', JF3: 'none' });
    assert.deepEqual(K.kitStatusFromRows([], []), {});
});

// ── the inspector email ─────────────────────────────────────────────────────

test('kitDocumentsToSend: every code\'s documents, each document once across codes', () => {
    const rows = [
        row({ jf_code: 'JF1', document_id: 1, document_name: 'Own 1' }),
        row({ jf_code: 'JF1', document_id: 9, document_name: 'Group plan', attached_via: 'product_group' }),
        row({ jf_code: 'JF2', document_id: 9, document_name: 'Group plan', attached_via: 'product_group' }),
        row({ jf_code: 'JF2', document_id: 2, document_name: 'Another' }),
        row({ jf_code: 'JF3', document_id: 3, document_name: 'Not on the visit' }),
    ];
    const docs = K.kitDocumentsToSend(['JF1', 'JF2_FQC', 'JF2'], rows);
    assert.deepEqual(docs.map(d => [d.documentId, d.jfCodes]), [[9, ['JF1', 'JF2']], [1, ['JF1']], [2, ['JF2']]]);
    assert.equal(docs[0].s3Key, 'documents/501/v3/qc-plan.pdf', 'the key to read the bytes from');
    assert.deepEqual(K.kitDocumentsToSend(['JF4'], rows), []);
    assert.deepEqual(K.kitDocumentsToSend([], rows), []);
});

test('attachmentNames: the file name, made unique with the JF code (then the document id)', () => {
    const d = (documentId, filename, jfCodes = ['JF1']) => ({ documentId, filename, jfCodes });
    assert.deepEqual(K.attachmentNames([d(1, 'plan.pdf'), d(2, 'spec.pdf')]), ['plan.pdf', 'spec.pdf']);
    assert.deepEqual(K.attachmentNames([d(1, 'QC.pdf', ['JF1']), d(2, 'qc.pdf', ['JF2']), d(3, 'other.pdf')]),
        ['JF1_QC.pdf', 'JF2_qc.pdf', 'other.pdf'], 'a clash (any case) is prefixed with the JF code');
    assert.deepEqual(K.attachmentNames([d(1, 'QC.pdf', ['JF1']), d(2, 'QC.pdf', ['JF1'])]),
        ['JF1_QC.pdf', '2_JF1_QC.pdf'], 'then with the document id');
    assert.deepEqual(K.attachmentNames([d(5, 'a"b\r\n.pdf'), d(6, '  '), d(7, null)]), ['ab.pdf', 'document-6', 'document-7'],
        'quotes and line breaks removed; a blank name falls back to the document id');
    assert.deepEqual(K.attachmentNames([]), []);
});

test('sentKitDocument / kitDocumentsFromJson: the qc_visit_sends.kit_documents_json round trip', () => {
    const doc = K.docFromRow(row());
    const stored = K.sentKitDocument(doc);
    assert.deepEqual(stored, { documentId: 501, versionId: 9001, version: 3, filename: 'qc-plan.pdf' });
    assert.deepEqual(K.kitDocumentsFromJson([stored]), [stored], 'mysql2 returns JSON columns parsed');
    assert.deepEqual(K.kitDocumentsFromJson(JSON.stringify([stored])), [stored], 'or as text');
    assert.deepEqual(K.kitDocumentsFromJson(Buffer.from(JSON.stringify([stored]))), [stored]);
    assert.deepEqual(K.kitDocumentsFromJson([{ documentId: '7', versionId: null, filename: 3 }, null, 'x']),
        [{ documentId: 7, versionId: null, version: null, filename: '3' }]);
    assert.equal(K.kitDocumentsFromJson(null), null);
    assert.equal(K.kitDocumentsFromJson(undefined), null);
    assert.equal(K.kitDocumentsFromJson('not json'), null);
    assert.equal(K.kitDocumentsFromJson({ documentId: 1 }), null);
    assert.deepEqual(K.kitDocumentsFromJson([]), []);
});

test('fillKitProducts: names the JFPRO product of a kit without documents, lowest id wins', () => {
    const kits = [
        { jfCode: 'JF1', productId: 7, productName: 'Seven', documents: [] },
        { jfCode: 'JF2', productId: null, productName: null, documents: [] },
        { jfCode: 'JF3', productId: null, productName: null, documents: [] },
    ];
    const out = K.fillKitProducts(kits, [
        { id: 9, name: 'Other one', jfcode: 'JF1' },
        { id: 12, name: 'Later', jfcode: 'jf2 ' },
        { id: 4, name: 'Gloves', jfcode: 'JF2' },
    ]);
    assert.deepEqual(out.map(k => [k.jfCode, k.productId, k.productName]), [['JF1', 7, 'Seven'], ['JF2', 4, 'Gloves'], ['JF3', null, null]]);
    assert.equal(kits[1].productId, null); // inputs untouched
    assert.ok(K.KIT_PRODUCTS_SQL.includes('FROM jfpro.products WHERE jfcode IN (?)'));
});

test('the SQL reads the view with one array parameter', () => {
    for (const sql of [K.KIT_DOCUMENTS_SQL, K.KIT_CODES_WITH_DOCUMENTS_SQL]) {
        assert.match(sql, /FROM qc_kit_documents/);
        assert.equal(sql.split('?').length - 1, 1);
    }
});
