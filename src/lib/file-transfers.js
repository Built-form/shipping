'use strict';

// ── Files sent between a supplier and our team, through the supplier portal ──
// A shared folder with a record (portal README section 18, this app's
// docs/file-transfers-frontend.md). One row of jportal.file_transfers per file,
// sent one way:
//   from_supplier  a supplier's user uploaded it on the portal's Files page; it
//                  waits here (SUPPLIER PORTAL > Files) to be opened and
//                  downloaded. Nothing reads it: no check, no import.
//   to_supplier    a staff member sends it from ShipLine: a row and a presigned
//                  PUT into the portal's bucket (files/staff/<uuid>.<ext>), the
//                  browser PUTs the bytes, `uploaded` pins the ETag and publishes
//                  the row, and the supplier's active portal users are emailed
//                  through Front that it is there.
// Any kind of file but a program (by extension, and by its first bytes), up to
// 250 MiB.
//
// Who writes what on jportal.file_transfers: the portal creates and moves its
// from_supplier rows; this app (admin on the server) creates and moves the
// to_supplier rows, writes notified_at / notify_error, and records staff views
// of a supplier's file in jportal.file_transfer_views (viewer_kind 'staff').
// Never a from_supplier row's status.
//
// Pure: no database, no clock, no network. Constants shared with jportal (its
// src/lib/fileTransfers.js — keep in step): the limit, the blocked list, the
// key patterns, the statuses.

const { DECIDER_ROLES, canDecide, positiveInt, parseRowVersion } = require('./pi-submissions');

/** 250 MiB, as the portal. */
const MAX_FILE_BYTES = 250 * 1024 * 1024;
const NOTE_MAX = 2000;
const FILENAME_MAX = 255;

const DIRECTIONS = ['from_supplier', 'to_supplier'];
const STATUSES = ['draft', 'sent', 'withdrawn', 'discarded'];
/** What staff are ever shown: never a draft (the portal's or ours) or a discarded row. */
const VISIBLE_STATUSES = ['sent', 'withdrawn'];

/** The portal's list, word for word (src/lib/fileTransfers.js BLOCKED_EXTENSIONS). */
const BLOCKED_EXTENSIONS = [
    'exe', 'msi', 'msp', 'mst', 'com', 'scr', 'pif', 'cpl', 'dll', 'sys', 'drv', 'ocx',
    'bat', 'cmd', 'ps1', 'psm1', 'psd1', 'vbs', 'vbe', 'js', 'jse', 'wsf', 'wsh', 'hta', 'sct', 'reg', 'inf', 'lnk', 'url',
    'jar', 'jnlp', 'sh', 'bash', 'zsh', 'csh', 'ksh', 'run', 'bin', 'elf', 'appimage',
    'app', 'dmg', 'pkg', 'command', 'action', 'workflow', 'apk', 'ipa', 'deb', 'rpm', 'gadget', 'msc', 'scf', 'vb', 'ws',
];
const BLOCKED = new Set(BLOCKED_EXTENSIONS);
const EXT_RE = /\.([A-Za-z0-9]{1,10})$/;

function extensionOf(filename) {
    if (typeof filename !== 'string') return null;
    const m = EXT_RE.exec(filename.trim());
    return m ? m[1].toLowerCase() : null;
}

/** The extension a key is stored under: the file's own, else `bin`. */
function storageExtension(filename) {
    const ext = extensionOf(filename);
    return ext && !BLOCKED.has(ext) ? ext : 'bin';
}

const isBlockedExtension = filename => {
    const ext = extensionOf(filename);
    return ext !== null && BLOCKED.has(ext);
};

function startsWith(bytes, signature) {
    if (!bytes || bytes.length < signature.length) return false;
    for (let i = 0; i < signature.length; i++) if (bytes[i] !== signature[i]) return false;
    return true;
}

/** The portal's rule for a program's first bytes: MZ, ELF, Mach-O (any), a shebang. */
function sniffExecutable(bytes) {
    if (!bytes || typeof bytes.length !== 'number') return false;
    return startsWith(bytes, [0x4d, 0x5a])
        || startsWith(bytes, [0x7f, 0x45, 0x4c, 0x46])
        || startsWith(bytes, [0xfe, 0xed, 0xfa, 0xce])
        || startsWith(bytes, [0xfe, 0xed, 0xfa, 0xcf])
        || startsWith(bytes, [0xce, 0xfa, 0xed, 0xfe])
        || startsWith(bytes, [0xcf, 0xfa, 0xed, 0xfe])
        || startsWith(bytes, [0xca, 0xfe, 0xba, 0xbe])
        || startsWith(bytes, [0x23, 0x21]);
}

/**
 * Why a file cannot be sent at all, from the request alone: FILE_EMPTY,
 * FILE_TOO_LARGE, FILE_BLOCKED (its extension), or null.
 */
function fileRefusal({ filename, fileSize }) {
    const size = Number(fileSize);
    if (!Number.isFinite(size) || size <= 0) return 'FILE_EMPTY';
    if (size > MAX_FILE_BYTES) return 'FILE_TOO_LARGE';
    if (isBlockedExtension(filename)) return 'FILE_BLOCKED';
    return null;
}

const CONTENT_TYPE_RE = /^[a-z0-9!#$&^_.+-]{1,60}\/[a-z0-9!#$&^_.+-]{1,60}$/;
const OCTET_STREAM = 'application/octet-stream';

/** The browser's content type when it is one plain media type, else octet-stream. */
function normalizeContentType(value) {
    if (typeof value !== 'string') return OCTET_STREAM;
    const type = value.trim().toLowerCase();
    return CONTENT_TYPE_RE.test(type) ? type : OCTET_STREAM;
}

// The display name as the portal keeps it: trimmed, no control or format
// characters, 1..255 characters. → { value } or { error }.
const INVISIBLE_RE = /[\p{Cc}\p{Cf}]/u;
function parseFilename(raw) {
    if (typeof raw !== 'string') return { error: 'filename must be a string.' };
    const name = raw.trim();
    if (!name) return { error: 'filename is required.' };
    if (name.length > FILENAME_MAX) return { error: `filename must be at most ${FILENAME_MAX} characters.` };
    if (INVISIBLE_RE.test(name)) return { error: 'filename contains characters that are not allowed.' };
    return { value: name };
}

// A note sent with the file: optional, trimmed, at most NOTE_MAX, line breaks
// allowed. → { value } or { error }.
const NOTE_INVISIBLE_RE = /[^\P{Cc}\n\r\t]|\p{Cf}/u;
function parseTransferNote(raw) {
    if (raw === undefined || raw === null) return { value: null };
    if (typeof raw !== 'string') return { error: 'note must be a string.' };
    const note = raw.trim();
    if (!note) return { value: null };
    if (note.length > NOTE_MAX) return { error: `note must be at most ${NOTE_MAX} characters.` };
    if (NOTE_INVISIBLE_RE.test(note)) return { error: 'note contains characters that are not allowed.' };
    return { value: note };
}

const UUID = '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}';
const UUID_RE = new RegExp(`^${UUID}$`);
const EXT = '[a-z0-9]{1,10}';
const FILES_IN_KEY_RE = new RegExp(`^files/in/${UUID}\\.${EXT}$`);
const FILES_SUP_KEY_RE = new RegExp(`^files/sup/(\\d{1,19})/${UUID}\\.${EXT}$`);
const FILES_STAFF_KEY_RE = new RegExp(`^files/staff/${UUID}\\.${EXT}$`);

/** `files/staff/<uuid>.<ext>`: the one key this app ever writes. Nothing typed is in it. */
function newStaffKey(uuid, filename) {
    if (typeof uuid !== 'string' || !UUID_RE.test(uuid)) throw new TypeError('newStaffKey: not a lower-case uuid');
    const key = `files/staff/${uuid}.${storageExtension(filename)}`;
    if (!FILES_STAFF_KEY_RE.test(key)) throw new Error('newStaffKey: built a key outside the key pattern');
    return key;
}

const isSupKey = key => typeof key === 'string' && FILES_SUP_KEY_RE.test(key);
const isStaffKey = key => typeof key === 'string' && FILES_STAFF_KEY_RE.test(key);
const isInKey = key => typeof key === 'string' && FILES_IN_KEY_RE.test(key);

/**
 * The key this app may read for a row: a supplier file's FINAL key once the
 * portal published it (never its staging key, which may still be rewritten),
 * a staff file's own key once pinned. Null when there is nothing to read.
 */
function readableKey(row) {
    if (!row) return null;
    if (row.direction === 'from_supplier') {
        return row.final_s3_key && row.final_etag && isSupKey(row.final_s3_key) ? { key: row.final_s3_key, etag: row.final_etag } : null;
    }
    return row.s3_key && row.s3_etag && isStaffKey(row.s3_key) ? { key: row.s3_key, etag: row.s3_etag } : null;
}

// A name S3 and a browser agree on for the download, from the display name:
// NFKC, ASCII letters, digits, dot, dash and underscore, the extension kept;
// '' when nothing is left (the route then falls back to file-<id>.<ext>).
function safeDownloadName(filename) {
    const ext = extensionOf(filename);
    let stem = String(filename == null ? '' : filename).normalize('NFKC').trim().replace(/\.[A-Za-z0-9]{1,10}$/, '');
    stem = stem.replace(/[^A-Za-z0-9._-]+/g, '_').replace(/_{2,}/g, '_').replace(/^[._-]+|[._-]+$/g, '');
    if (!/[A-Za-z0-9]/.test(stem)) return '';
    stem = stem.slice(0, 120);
    return ext ? `${stem}.${ext}` : stem;
}

// RFC 5987 for the real (possibly Chinese) name beside the ASCII one.
const encodeRfc5987 = value => encodeURIComponent(value).replace(/['()*]/g, c => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);
function contentDisposition(filename, fallback) {
    const ascii = safeDownloadName(filename) || fallback;
    const full = typeof filename === 'string' && filename.trim() ? filename.trim() : ascii;
    return `attachment; filename="${ascii.replace(/["\\]/g, '_')}"; filename*=UTF-8''${encodeRfc5987(full)}`;
}

// ── Lists ──────────────────────────────────────────────────────────────────

const LIST_LIMIT_DEFAULT = 100;
const LIST_LIMIT_MAX = 500;

/**
 * GET /file-transfers query → { value: { statuses, direction, supplierId,
 * unopened, limit, offset } } or { error }.
 *   status: open (= sent, the default), history (= withdrawn), sent, withdrawn,
 *           comma-separated, or all (both).
 *   direction: from_supplier | to_supplier | (absent: both).
 *   unopened: 1 → only supplier files no staff member has opened yet.
 */
function parseListQuery(q = {}) {
    const status = q.status === undefined || q.status === '' ? 'open' : String(q.status);
    let statuses;
    if (status === 'all') statuses = VISIBLE_STATUSES;
    else if (status === 'open') statuses = ['sent'];
    else if (status === 'history') statuses = ['withdrawn'];
    else {
        statuses = status.split(',').map(s => s.trim()).filter(Boolean);
        if (!statuses.length || statuses.some(s => !VISIBLE_STATUSES.includes(s))) {
            return { error: `status must be open, history, all, or one or more of ${VISIBLE_STATUSES.join(', ')}.` };
        }
    }
    const value = { statuses: [...new Set(statuses)], direction: null, supplierId: null, unopened: false, limit: LIST_LIMIT_DEFAULT, offset: 0 };
    if (q.direction !== undefined && q.direction !== '') {
        if (!DIRECTIONS.includes(String(q.direction))) return { error: `direction must be one of ${DIRECTIONS.join(', ')}.` };
        value.direction = String(q.direction);
    }
    if (q.supplierId !== undefined && q.supplierId !== '') {
        const id = positiveInt(q.supplierId);
        if (!id) return { error: 'supplierId must be a positive integer.' };
        value.supplierId = id;
    }
    if (q.unopened !== undefined && q.unopened !== '') {
        value.unopened = q.unopened === '1' || q.unopened === 'true' || q.unopened === true;
    }
    if (q.limit !== undefined && q.limit !== '') {
        const n = positiveInt(q.limit);
        if (!n) return { error: 'limit must be a positive integer.' };
        value.limit = Math.min(n, LIST_LIMIT_MAX);
    }
    if (q.offset !== undefined && q.offset !== '') {
        const n = Number(q.offset);
        if (!Number.isSafeInteger(n) || n < 0) return { error: 'offset must be a non-negative integer.' };
        value.offset = n;
    }
    return { value };
}

/**
 * POST /file-transfers body → { value: { supplierId, filename, contentType,
 * fileSize, note } } or { error, code }.
 */
function parseCreateBody(body = {}) {
    const supplierId = positiveInt(body.supplierId);
    if (!supplierId) return { error: 'supplierId is required: the supplier the file is for.', code: 'BAD_FIELD' };
    const name = parseFilename(body.filename);
    if (name.error) return { error: name.error, code: 'BAD_FIELD' };
    const fileSize = positiveInt(body.fileSize);
    if (!fileSize) return { error: 'fileSize is required: the size of the file in bytes.', code: 'BAD_FIELD' };
    const refused = fileRefusal({ filename: name.value, fileSize });
    if (refused === 'FILE_TOO_LARGE') return { error: `Files can be at most ${MAX_FILE_BYTES} bytes (250 MB).`, code: 'FILE_TOO_LARGE' };
    if (refused === 'FILE_BLOCKED') return { error: 'Programs, scripts and installers cannot be sent; zip the file if you must.', code: 'FILE_BLOCKED' };
    const note = parseTransferNote(body.note);
    if (note.error) return { error: note.error, code: 'BAD_FIELD' };
    return { value: { supplierId, filename: name.value, contentType: normalizeContentType(body.contentType), fileSize, note: note.value } };
}

// ── Response shapes ────────────────────────────────────────────────────────

const iso = v => (v == null ? null : v.toISOString ? v.toISOString() : String(v));

/**
 * A row as ShipLine sees it (every column but the keys and ETags, which it
 * never needs). `seen` is whether the OTHER side has opened the file: a staff
 * member for a supplier's file, anyone at the supplier for ours; `seenAt` the
 * first time, `viewers` how many people. `views` (the detail) lists them.
 */
function transferToJson(row, { views = null } = {}) {
    const fromSupplier = row.direction === 'from_supplier';
    const seenAt = fromSupplier ? row.staff_first_viewed_at_iso : row.supplier_first_viewed_at_iso;
    return {
        id: Number(row.id),
        direction: row.direction,
        status: row.status,
        supplierId: Number(row.supplier_id),
        supplierKey: `s:${Number(row.supplier_id)}`,
        supplierName: row.live_supplier_name || row.supplier_name || null,
        supplierExists: Number(row.supplier_exists) === 1,
        filename: row.filename,
        contentType: row.content_type,
        fileSize: Number(row.file_size),
        note: row.note || null,
        sentBy: row.sender_email || null,
        sentAt: row.sent_at_iso || null,
        withdrawnAt: row.withdrawn_at_iso || null,
        withdrawnBy: row.withdrawn_by || null,
        seen: Boolean(seenAt),
        seenAt: seenAt || null,
        viewers: Number(fromSupplier ? row.staff_viewers : row.supplier_viewers) || 0,
        notifiedAt: fromSupplier ? null : (row.notified_at_iso || null),
        notifyError: fromSupplier ? null : (row.notify_error || null),
        rowVersion: Number(row.row_version),
        createdAt: row.created_at_iso || null,
        ...(views ? { views } : {}),
    };
}

function viewToJson(v) {
    return {
        kind: v.viewer_kind,
        email: v.viewer_email,
        firstViewedAt: iso(v.first_viewed_at),
        lastViewedAt: iso(v.last_viewed_at),
        count: Number(v.view_count) || 0,
    };
}

// ── The email to the supplier ──────────────────────────────────────────────

const escapeHtml = s => String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');

const TEMPLATE_KEY = 'file_transfer_portal';

/**
 * The tokens for the "a file is waiting for you" email (template
 * file_transfer_portal). Everything in `body` is HTML-escaped (staff typed the
 * note and chose the file); `subject` is plain text. Body tokens: {supplierName}
 * {filename} {note} {noteHtml} {portalLinkHtml}.
 */
function buildEmailVars({ row, portalUrl }) {
    const supplierName = String(row.live_supplier_name || row.supplier_name || '').replace(/[\r\n]+/g, ' ');
    const text = row.note ? String(row.note) : '';
    const base = portalUrl ? String(portalUrl).replace(/\/+$/, '') : '';
    const href = base ? `${base}/files` : '';
    const portalLinkHtml = href
        ? `<p><a href="${escapeHtml(href)}">Download it in the supplier portal / 在供应商门户中下载</a></p>`
        : '';
    return {
        subject: { supplierName, filename: String(row.filename || '').replace(/[\r\n]+/g, ' ') },
        body: {
            supplierName: escapeHtml(supplierName),
            filename: escapeHtml(row.filename || ''),
            note: escapeHtml(text),
            noteHtml: text
                ? `<p><strong>Message from our team / 我方留言：</strong><br>${escapeHtml(text).replace(/\r?\n/g, '<br>')}</p>`
                : '',
            portalLinkHtml,
        },
    };
}

module.exports = {
    MAX_FILE_BYTES,
    NOTE_MAX,
    FILENAME_MAX,
    DIRECTIONS,
    STATUSES,
    VISIBLE_STATUSES,
    BLOCKED_EXTENSIONS,
    DECIDER_ROLES,
    LIST_LIMIT_DEFAULT,
    LIST_LIMIT_MAX,
    TEMPLATE_KEY,
    OCTET_STREAM,
    canDecide,
    positiveInt,
    parseRowVersion,
    extensionOf,
    storageExtension,
    isBlockedExtension,
    sniffExecutable,
    fileRefusal,
    normalizeContentType,
    parseFilename,
    parseTransferNote,
    newStaffKey,
    isSupKey,
    isStaffKey,
    isInKey,
    readableKey,
    safeDownloadName,
    contentDisposition,
    parseListQuery,
    parseCreateBody,
    transferToJson,
    viewToJson,
    buildEmailVars,
};
