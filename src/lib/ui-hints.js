'use strict';

// Info bubbles — pure rules behind /api/v1/ui-hints (routes:
// src/services/ui-hint-routes.js; schema: src/db/migrate/2026-11-10_10_shipline_ui_hints.sql).
//
// A hint is a short plain-text explanation a ShipLine user writes on a caption
// (a column head, a field label, a section heading) for everyone to read, with
// one optional link to another ShipLine page. It hangs on a key the page
// builds from where the caption sits:
//
//   key   := scope ('/' segment){1,4}         payments.ready/col.supplier
//   scope := part ('.' part){0,4}             view . tab . sub-tab
//   part  := [a-z0-9][a-z0-9_-]{0,59}
//
// The last segment carries a kind prefix (col. field. stat. tab. section.
// stage.); the page decides those, the server only checks the grammar. The
// grammar, the limits and the delete rule are the ones JFPRO's info bubbles
// use (JFPRO/api/ui-hints-rules.js), so the two apps read the same way.
//
// ShipLine keeps a copy of docs/frontend/ui-hints-contract.json (built from
// this file by tools/gen-ui-hints-contract.js) and tests its client rules
// against it; change a rule here, regenerate, and copy the file across.
//
// Who: everyone signed in reads; admin and standard users write; the author or
// an admin deletes. Accountants, warehouse and exec accounts read only.

const TABLE = 'shipline_ui_hints';
const AUDIT_ENTITY = 'ui_hint';

const HINT_ROLES = Object.freeze(['admin', 'standard']);

const HINT_KEY_MAX_LEN = 160;   // the column width
const HINT_BODY_MAX_LEN = 600;
const LINK_PATH_MAX_LEN = 200;
const LINK_LABEL_MAX_LEN = 80;
const ROW_VERSION_MAX = 2 ** 31 - 1;

const HINT_KEY_RE = /^[a-z0-9][a-z0-9_-]{0,59}(\.[a-z0-9][a-z0-9_-]{0,59}){0,4}(\/[a-z0-9][a-z0-9_.-]{0,60}){1,4}$/;

// The first path segment of every ShipLine view (ShipLine/src/nav.ts PATHS),
// plus `info`, the page that lists the hints. A hint's link may only point at
// one of these. Old paths (/qc, /supplier-changes) still open in ShipLine but
// are not offered: a new link names the page as it is called today.
const LINK_ROOTS = Object.freeze([
    'bridge', 'purchase-orders', 'manifest', 'draft', 'warehouse', 'delivered', 'qc-visits',
    'supplier-portal', 'shipments', 'payments', 'audit-log', 'email-receipts', 'users', 'info',
]);

const REFUSALS = Object.freeze({
    STALE: Object.freeze({ status: 409, code: 'STALE', error: 'This hint was changed by someone else. Reload and try again.' }),
    OCCUPIED: Object.freeze({ status: 409, code: 'OCCUPIED', error: 'A hint already exists here; reload to edit it.' }),
    OCCUPIED_MOVE: Object.freeze({ status: 409, code: 'OCCUPIED_MOVE', error: 'That place already has a hint.' }),
    DELETED: Object.freeze({ status: 404, code: 'DELETED', error: 'This hint was deleted.' }),
    NOT_AUTHOR: Object.freeze({ status: 403, code: 'NOT_AUTHOR', error: 'Only the author or an admin can delete this hint.' }),
});

const cannotWrite = userType => ({
    status: 403,
    code: 'CANNOT_WRITE_HINTS',
    error: `Your role (${userType || 'none'}) cannot change hints — admin and standard users do.`,
});

const canWriteHints = userType => typeof userType === 'string' && HINT_ROLES.includes(userType);

// `row` is the locked DB row. The stored email is compared case-insensitively.
function canDeleteHint(userType, userEmail, row) {
    if (userType === 'admin') return true;
    if (!canWriteHints(userType)) return false;
    if (typeof userEmail !== 'string' || !row || typeof row.created_by !== 'string') return false;
    return row.created_by.toLowerCase() === userEmail.toLowerCase();
}

const normEmail = v => String(v || 'unknown').trim().toLowerCase();

// { key } or { error }. The key is taken exactly as sent: the page builds it.
function parseKey(value, field = 'key') {
    if (typeof value !== 'string' || !value) return { error: `${field} is required.` };
    if (value.length > HINT_KEY_MAX_LEN) return { error: `${field} cannot exceed ${HINT_KEY_MAX_LEN} characters.` };
    if (!HINT_KEY_RE.test(value)) return { error: `${field} must look like view.tab/section/kind.name (lowercase letters, digits, - _ .).` };
    return { key: value };
}

// { body } or { error }. Plain text: line breaks kept (CRLF → LF), the rest is
// shown as text by the page, never as HTML.
function parseBody(value) {
    if (typeof value !== 'string') return { error: 'body is required and must be text.' };
    const body = value.replace(/\r\n?/g, '\n').trim();
    if (!body) return { error: 'body cannot be empty.' };
    if (body.length > HINT_BODY_MAX_LEN) return { error: `body cannot exceed ${HINT_BODY_MAX_LEN} characters.` };
    return { body };
}

const LINK_CHARSET_RE = /^[A-Za-z0-9\-._~/?=&%]*$/;
const BAD_ESCAPE_RE = /%(?![0-9A-Fa-f]{2})/;
// eslint-disable-next-line no-control-regex
const CONTROL_RE = /[\u0000-\u001f\u007f]/;

// A same-origin ShipLine path. Checked on the raw string and once more on its
// single decoding; the raw string is what is stored and followed.
function parseLinkPath(raw) {
    if (typeof raw !== 'string') return { error: 'linkPath must be text.' };
    if (raw.length > LINK_PATH_MAX_LEN) return { error: `linkPath cannot exceed ${LINK_PATH_MAX_LEN} characters.` };
    if (!raw.startsWith('/') || raw.startsWith('//')) return { error: 'linkPath must be a ShipLine path starting with /.' };
    if (!LINK_CHARSET_RE.test(raw)) return { error: 'linkPath contains characters that are not allowed.' };
    if (BAD_ESCAPE_RE.test(raw)) return { error: 'linkPath has a malformed % escape.' };
    let decoded;
    try {
        decoded = decodeURIComponent(raw);
    } catch {
        return { error: 'linkPath has a malformed % escape.' };
    }
    if (decoded.includes('\\') || decoded.includes('//') || CONTROL_RE.test(decoded)) {
        return { error: 'linkPath is not a plain ShipLine path.' };
    }
    const segments = decoded.split('?')[0].split('/').slice(1);
    if (segments.some(s => s === '.' || s === '..')) return { error: 'linkPath may not contain . or .. segments.' };
    if (!LINK_ROOTS.includes(segments[0])) return { error: 'linkPath must point at a ShipLine page.' };
    return { linkPath: raw };
}

// { linkPath, linkLabel } (both null when there is no link) or { error }.
function parseLink({ linkPath, linkLabel } = {}) {
    const hasPath = linkPath !== undefined && linkPath !== null && linkPath !== '';
    const hasLabel = linkLabel !== undefined && linkLabel !== null && String(linkLabel).trim() !== '';
    if (!hasPath && !hasLabel) return { linkPath: null, linkLabel: null };
    if (!hasPath) return { error: 'linkLabel needs a linkPath.' };
    if (!hasLabel) return { error: 'linkPath needs a linkLabel.' };
    const p = parseLinkPath(linkPath);
    if (p.error) return p;
    if (typeof linkLabel !== 'string') return { error: 'linkLabel must be text.' };
    const label = linkLabel.trim();
    if (label.length > LINK_LABEL_MAX_LEN) return { error: `linkLabel cannot exceed ${LINK_LABEL_MAX_LEN} characters.` };
    return { linkPath: p.linkPath, linkLabel: label };
}

// { rowVersion } or { error }. Numeric strings pass (DELETE sends it in the
// query string).
function parseRowVersion(value) {
    if (value === undefined || value === null || value === '' || typeof value === 'boolean') return { error: 'rowVersion is required.' };
    const n = Number(value);
    if (!Number.isInteger(n) || n < 0 || n > ROW_VERSION_MAX) return { error: 'rowVersion must be a whole number, 0 or more.' };
    return { rowVersion: n };
}

function parseId(value) {
    const n = Number(value);
    if (value === undefined || value === null || typeof value === 'boolean' || !Number.isInteger(n) || n <= 0) {
        return { error: 'id must be a positive whole number.' };
    }
    return { id: n };
}

// PUT body → { value: { key, id, rowVersion, body, linkPath, linkLabel } } or
// { error }. rowVersion 0 creates (no id); 1 or more updates the row `id`.
function parsePutBody(raw) {
    const b = raw && typeof raw === 'object' ? raw : {};
    const k = parseKey(b.key);
    if (k.error) return k;
    const rv = parseRowVersion(b.rowVersion);
    if (rv.error) return rv;
    let id = null;
    if (rv.rowVersion > 0) {
        const i = parseId(b.id);
        if (i.error) return { error: `${i.error} An update names the hint it changes.` };
        id = i.id;
    }
    const body = parseBody(b.body);
    if (body.error) return body;
    const link = parseLink({ linkPath: b.linkPath, linkLabel: b.linkLabel });
    if (link.error) return link;
    return { value: { key: k.key, id, rowVersion: rv.rowVersion, body: body.body, linkPath: link.linkPath, linkLabel: link.linkLabel } };
}

// POST …/move body → { value: { rowVersion, toKey } } or { error }.
function parseMoveBody(raw) {
    const b = raw && typeof raw === 'object' ? raw : {};
    const rv = parseRowVersion(b.rowVersion);
    if (rv.error) return rv;
    if (rv.rowVersion === 0) return { error: 'rowVersion must be the hint\'s current version (1 or more).' };
    const to = parseKey(b.toKey, 'toKey');
    if (to.error) return to;
    return { value: { rowVersion: rv.rowVersion, toKey: to.key } };
}

// Rows are born at row_version 1, so rowVersion 0 can only mean "create". An
// existing row is named by id AND version: a key deleted and recreated gets a
// new id, which turns an old editor's save into a 404 rather than an overwrite.
function planUpsert(existingRow, rowVersion, id) {
    if (rowVersion === 0) return existingRow ? 'occupied' : 'insert';
    if (!existingRow || Number(existingRow.id) !== Number(id)) return 'missing';
    if (Number(existingRow.row_version) !== rowVersion) return 'stale';
    return 'update';
}

const sameContent = (row, v) =>
    row.body === v.body && (row.link_path ?? null) === v.linkPath && (row.link_label ?? null) === v.linkLabel;

// Exactly what a person changes — no timestamps or versions — so an audit
// diff only ever shows the text, the link or the place.
const hintSnapshot = row => ({
    key: row.hint_key,
    body: row.body,
    linkPath: row.link_path ?? null,
    linkLabel: row.link_label ?? null,
});

const iso = v => (v == null ? null : v.toISOString ? v.toISOString() : String(v));

function rowToJson(r) {
    return {
        id: Number(r.id),
        key: r.hint_key,
        body: r.body,
        linkPath: r.link_path ?? null,
        linkLabel: r.link_label ?? null,
        createdBy: r.created_by,
        updatedBy: r.updated_by,
        updatedByName: r.updated_by_name || null,
        createdAt: iso(r.created_at),
        updatedAt: iso(r.updated_at),
        rowVersion: Number(r.row_version),
    };
}

module.exports = {
    TABLE, AUDIT_ENTITY, HINT_ROLES,
    HINT_KEY_MAX_LEN, HINT_BODY_MAX_LEN, LINK_PATH_MAX_LEN, LINK_LABEL_MAX_LEN, ROW_VERSION_MAX,
    HINT_KEY_RE, LINK_ROOTS, REFUSALS,
    cannotWrite, canWriteHints, canDeleteHint, normEmail,
    parseKey, parseBody, parseLink, parseRowVersion, parseId, parsePutBody, parseMoveBody,
    planUpsert, sameContent, hintSnapshot, rowToJson,
};
