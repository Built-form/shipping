const crypto = require('crypto');

// Fully ambiguity-free alphabet: drop the confusable digits 0 and 1 AND the
// letters that look like them (I, L, O). Nothing left can be misread as
// anything else. Easy to read off an email and type on a phone (audience:
// suppliers in China). Codes are matched case-insensitively and ignoring
// spaces/dashes (see normalizeCode), so a supplier can type loosely.
const PORTAL_CODE_ALPHABET = '23456789ABCDEFGHJKMNPQRSTUVWXYZ';

// Normalise a code for comparison and storage: uppercase, and drop anything
// that isn't a letter or digit. So "mhtr-kqxp", "MHTR KQXP" and "mhtrkqxp" all
// resolve to the same code as the stored "MHTRKQXP".
function normalizeCode(s) {
    return String(s == null ? '' : s).toUpperCase().replace(/[^0-9A-Z]/g, '');
}

// 8 chars over a 32-symbol alphabet ≈ 1.1e12 combinations — not guessable by
// brute force against a low-concurrency public endpoint, but short enough to
// share. The code is always used together with a specific supplierId + PO
// number, so it is one factor of a scoped lookup, not a standalone secret.
function generatePortalCode(length = 8) {
    const bytes = crypto.randomBytes(length);
    let out = '';
    for (let i = 0; i < length; i++) {
        out += PORTAL_CODE_ALPHABET[bytes[i] % PORTAL_CODE_ALPHABET.length];
    }
    return out;
}

// Constant-time string comparison for access-code checks on the public
// endpoint. Returns false (rather than throwing) when either side is empty or
// the lengths differ, so callers can treat it as a plain boolean.
function safeCompareCode(a, b) {
    const ab = Buffer.from(String(a == null ? '' : a), 'utf8');
    const bb = Buffer.from(String(b == null ? '' : b), 'utf8');
    if (ab.length === 0 || ab.length !== bb.length) return false;
    return crypto.timingSafeEqual(ab, bb);
}

// Idempotent feature setup, safe to call on every cold start from any handler:
// give every live supplier that still lacks a code a freshly generated one.
//
// portal_code now lives on jfpro.suppliers — the unified single source of truth.
// jfa.suppliers is a VIEW over it (see 2026-06-30_suppliers_views.sql), so we
// read/write jfpro.suppliers directly: you cannot ALTER or UPDATE through the
// view, and the column already exists (added by 2026-06-30_jfpro_suppliers_add_columns.sql).
// Returns the number of suppliers that were assigned a new code (0 in steady state).
async function ensureSupplierPortalCodes(conn) {
    const [rows] = await conn.query(
        `SELECT id FROM jfpro.suppliers
          WHERE COALESCE(is_deleted, 0) = 0 AND (portal_code IS NULL OR portal_code = '')`
    );
    for (const r of rows) {
        await conn.query('UPDATE jfpro.suppliers SET portal_code = ? WHERE id = ?', [generatePortalCode(), r.id]);
    }
    return rows.length;
}

module.exports = {
    PORTAL_CODE_ALPHABET,
    generatePortalCode,
    normalizeCode,
    safeCompareCode,
    ensureSupplierPortalCodes,
};
