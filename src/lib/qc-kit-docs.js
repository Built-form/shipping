'use strict';

// QC inspection kits from JFPRO — the pure part of GET /api/v1/qc-kits
// (src/services/qc-kit-routes.js), the kit_missing warning
// (src/services/qc-visit-routes.js) and the inspector email's kit
// attachments (src/services/qc-visit-comms-routes.js).
//
// A product's kit is its JFPRO documents with category 'qa' whose Plan of
// Action stage is "China Agent — factory QC" (tag qa_type 'poa_china', or no
// qa_type tag), attached to the product directly or to a product group it
// belongs to. Users add and edit them in JFPRO; shipping stores no kit. The
// view qc_kit_documents
// (migration src/db/migrate/2026-10-28_20_qc_kit_documents_view.sql) has one row per
// (product, attachment path, document) with the document's current version;
// the helpers below group its rows by JF code and de-duplicate them.
//
// No DB, no AWS: tested by tools/test-qc-kit-docs-lib.js.

const { normJfCode } = require('./qc-visits');

const MAX_KIT_CODES = 500;

// Every view row for a list of normalised codes (one parameter: the array).
const KIT_DOCUMENTS_SQL = `SELECT jf_code, product_id, product_name, document_id, document_name, version_id,
       version_number, s3_key, filename, mime_type, size_bytes, uploaded_by, uploaded_at, attached_via
  FROM qc_kit_documents
 WHERE jf_code IN (?)`;
// The codes (of a normalised list) that have at least one kit document.
const KIT_CODES_WITH_DOCUMENTS_SQL = 'SELECT DISTINCT jf_code FROM qc_kit_documents WHERE jf_code IN (?)';
// JFPRO products of codes that have no kit documents yet, so ShipLine can
// attach a first upload to the product (jfcode is unique in practice; the
// collation is case-insensitive).
const KIT_PRODUCTS_SQL = 'SELECT id, name, jfcode FROM jfpro.products WHERE jfcode IN (?)';

function isoOf(v) {
    if (v == null || v === '') return null;
    if (v instanceof Date) return Number.isNaN(v.getTime()) ? null : v.toISOString();
    const s = String(v);
    // MySQL 'YYYY-MM-DD HH:MM:SS' is UTC in this app.
    const t = Date.parse(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}/.test(s) ? `${s.replace(' ', 'T')}Z` : s);
    return Number.isNaN(t) ? null : new Date(t).toISOString();
}

const REQUIRED = 'jfCodes is required: a comma-separated list of JF codes.';

// ?jfCodes=A,B (or the parameter repeated) → { codes }: normalised with
// normJfCode (trimmed, upper case, _FQC stripped), blanks dropped,
// de-duplicated, in request order. { error } when it is missing, empty, not
// text, or names more than MAX_KIT_CODES codes.
function parseJfCodesParam(raw) {
    const parts = Array.isArray(raw) ? raw : [raw];
    if (raw == null || parts.some(p => typeof p !== 'string')) return { error: REQUIRED };
    const codes = [...new Set(parts.flatMap(p => p.split(',')).map(normJfCode).filter(Boolean))];
    if (!codes.length) return { error: REQUIRED };
    if (codes.length > MAX_KIT_CODES) return { error: `At most ${MAX_KIT_CODES} JF codes per request.` };
    return { codes };
}

// One view row → a QcKitDocument without its links (the route presigns `url`
// and `downloadUrl` from s3Key, which never leaves the server).
function docFromRow(r) {
    const documentId = Number(r.document_id);
    const filename = r.filename ? String(r.filename) : `document-${documentId}`;
    return {
        documentId,
        versionId: Number(r.version_id),
        version: Number(r.version_number),
        name: r.document_name ? String(r.document_name) : filename,
        filename,
        mimeType: r.mime_type || null,
        sizeBytes: r.size_bytes == null ? null : Number(r.size_bytes),
        uploadedBy: r.uploaded_by || null,
        uploadedAt: isoOf(r.uploaded_at),
        attachedVia: r.attached_via === 'product' ? 'product' : 'product_group',
        s3Key: r.s3_key == null ? null : String(r.s3_key),
    };
}

const byName = (a, b) => a.name.localeCompare(b.name, 'en', { sensitivity: 'base' })
    || (a.name < b.name ? -1 : (a.name > b.name ? 1 : 0))
    || a.documentId - b.documentId;

// View rows → one kit per code of `codes` (normalised, de-duplicated, in
// order), even a code with no documents:
//   { jfCode, productId|null, productName|null, documents: doc[] }
// A document reachable directly and through a group (or two groups) appears
// once; 'product' wins. Documents sort by name, then documentId. productId
// and productName come from the view rows, so they are null for a code with
// no kit documents; with several JFPRO products on one code the lowest id wins.
function kitsFromRows(codes, rows) {
    const list = [...new Set((codes || []).map(normJfCode).filter(Boolean))];
    const byCode = new Map(list.map(c => [c, { productId: null, productName: null, docs: new Map() }]));
    for (const r of rows || []) {
        const k = byCode.get(normJfCode(r.jf_code));
        if (!k) continue;
        const pid = r.product_id == null ? null : Number(r.product_id);
        if (pid != null && (k.productId == null || pid < k.productId)) {
            k.productId = pid;
            k.productName = r.product_name == null ? null : String(r.product_name);
        }
        const d = docFromRow(r);
        const prev = k.docs.get(d.documentId);
        if (!prev || (prev.attachedVia !== 'product' && d.attachedVia === 'product')) k.docs.set(d.documentId, d);
    }
    return list.map((c) => {
        const k = byCode.get(c);
        return { jfCode: c, productId: k.productId, productName: k.productName, documents: [...k.docs.values()].sort(byName) };
    });
}

// { [normJfCode]: 'docs' | 'none' } for the kit_missing warning, from the
// { jf_code } rows of KIT_CODES_WITH_DOCUMENTS_SQL (or KIT_DOCUMENTS_SQL).
function kitStatusFromRows(codes, rows) {
    const withDocs = new Set((rows || []).map(r => normJfCode(r.jf_code)).filter(Boolean));
    const out = {};
    for (const c of (codes || []).map(normJfCode).filter(Boolean)) out[c] = withDocs.has(c) ? 'docs' : 'none';
    return out;
}

// The kit documents to email for the given JF codes: each code's documents
// (codes in order, each kit by name), every document once even when several
// codes share it (a product-group document). Each entry is a docFromRow
// object plus jfCodes, the codes it covers.
function kitDocumentsToSend(codes, rows) {
    const out = new Map();
    for (const kit of kitsFromRows(codes, rows)) {
        for (const d of kit.documents) {
            const prev = out.get(d.documentId);
            if (prev) {
                if (!prev.jfCodes.includes(kit.jfCode)) prev.jfCodes.push(kit.jfCode);
            } else {
                out.set(d.documentId, { ...d, jfCodes: [kit.jfCode] });
            }
        }
    }
    return [...out.values()];
}

// Attachment file names for kitDocumentsToSend entries, in the same order:
// the document's filename (quotes and line breaks removed), unique within the
// email (case-insensitively). A name used by more than one document is
// prefixed with the first JF code it covers ("JF0208_QC plan.pdf"); one that
// still clashes is prefixed with the document id too.
function attachmentNames(docs) {
    const clean = d => String(d.filename || '').replace(/["\r\n]/g, '').trim() || `document-${d.documentId}`;
    const key = s => s.toLowerCase();
    const counts = new Map();
    for (const d of docs) {
        const k = key(clean(d));
        counts.set(k, (counts.get(k) || 0) + 1);
    }
    const used = new Set();
    return docs.map((d) => {
        let name = clean(d);
        if (counts.get(key(name)) > 1 && d.jfCodes && d.jfCodes[0]) name = `${d.jfCodes[0]}_${name}`;
        if (used.has(key(name))) name = `${d.documentId}_${name}`;
        used.add(key(name));
        return name;
    });
}

// The qc_visit_sends.kit_documents_json entry of an attached document.
const sentKitDocument = d => ({ documentId: d.documentId, versionId: d.versionId, version: d.version, filename: d.filename });

// qc_visit_sends.kit_documents_json (a JSON column: parsed by mysql2, or text)
// → [{ documentId, versionId, version, filename }], or null when NULL.
function kitDocumentsFromJson(v) {
    if (v == null) return null;
    let list = v;
    if (typeof v === 'string' || Buffer.isBuffer(v)) {
        try { list = JSON.parse(String(v)); } catch { return null; }
    }
    if (!Array.isArray(list)) return null;
    const num = x => (x == null ? null : Number(x));
    return list.filter(x => x && typeof x === 'object').map(x => ({
        documentId: num(x.documentId),
        versionId: num(x.versionId),
        version: num(x.version),
        filename: x.filename == null ? null : String(x.filename),
    }));
}

// Fills productId / productName on kits that have none (no documents yet)
// from KIT_PRODUCTS_SQL rows: the lowest product id per normalised code, as
// kitsFromRows does. Returns new kit objects; kits that already name a
// product are unchanged.
function fillKitProducts(kits, productRows) {
    const best = new Map();
    for (const r of productRows || []) {
        const code = normJfCode(r.jfcode);
        const id = Number(r.id);
        if (!code || !Number.isInteger(id) || id <= 0) continue;
        const prev = best.get(code);
        if (!prev || id < prev.id) best.set(code, { id, name: r.name == null ? null : String(r.name) });
    }
    return (kits || []).map((k) => {
        if (k.productId != null) return k;
        const p = best.get(normJfCode(k.jfCode));
        return p ? { ...k, productId: p.id, productName: p.name } : k;
    });
}

module.exports = {
    MAX_KIT_CODES,
    KIT_PRODUCTS_SQL,
    fillKitProducts,
    KIT_DOCUMENTS_SQL,
    KIT_CODES_WITH_DOCUMENTS_SQL,
    parseJfCodesParam,
    docFromRow,
    kitsFromRows,
    kitStatusFromRows,
    kitDocumentsToSend,
    attachmentNames,
    sentKitDocument,
    kitDocumentsFromJson,
};
