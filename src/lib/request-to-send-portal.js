'use strict';

// A Request to send (a draft_container_documents row of type 'supplier-quote':
// one supplier's packing list for one container) goes to the supplier as a LINK
// to the supplier portal, not as attachments, whenever the portal can show it
// to them (docs/request-to-send-portal.md). The pure rules only, no database:
// which way a document goes out, the portal link, what the email said about
// the departure, and the fixed portal block under the editable template body.
// The SQL is in src/services/request-to-send-portal.js; the route is
// POST /api/v1/draft-container-documents/:id/email in src/handlers/orders.js.
//
// The PO send and the signed-PI send work the same way (src/lib/po-portal-email.js),
// with one difference: those are always a link. This one falls back to the old
// email, PDF and CSV attached, when the portal would show the supplier nothing.

const { FONT, INK, MUTED, escapeHtml, noteBox } = require('./email-layout');
const drift = require('./request-to-send-drift');
const { blockHtml } = require('./po-portal-email');
const S = require('./shipments');

// ── What a supplier is told a container is called ────────────────────────────
// A draft's name is internal. It carries the SPA's stamp, whatever the team
// typed after it (other suppliers' names, a departure date long out of date:
// 'DRAFT-SEA-260923-141617 - 333 Sunmed Kingphar Foreu ETD 13 Oct'), and it is
// what draft_container_documents.draft_container_name stores. None of that is
// for a supplier. They are shown the container's NUMBER and nothing else: on
// the PDF, in its file name, in the email and on the portal.

/**
 * The reference a supplier sees for a document's container: the booked
 * reference when the name is one ('328', '121. Air Freight'), else the number
 * the draft's name reserves ('… - 333 Sunmed …' -> '333', '… - 126. Air
 * Freight' -> '126. Air Freight'; S.parseNameHint, the rule booking itself
 * uses). null when the name holds no number: then nothing of it is shown.
 * The portal has the same rule (portal src/lib/sentDocuments.js supplierRefOf).
 */
function supplierRef(name) {
    const booked = S.parseReference(name);
    if (booked && booked.known) return booked.reference;
    const hint = S.parseNameHint(name);
    return hint ? hint.reference : null;
}

// What an email calls the container where a template says {draftContainerName}.
const emailName = name => supplierRef(name) || 'shipment';

// As an HTML editor writes a name's & < >.
const editorEscaped = s => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

/**
 * `text` (a subject, or an HTML body) with every occurrence of the container's
 * internal name replaced by what a supplier is told instead (emailName). The
 * send dialog fills {draftContainerName} itself, and a ShipLine from before
 * 2026-11-13 fills it with the whole name: the server is where that is undone,
 * whichever screen the email was written on.
 */
function hideInternalName(text, name) {
    const internal = String(name == null ? '' : name).trim();
    if (text == null || text === '' || !internal) return text;
    const shown = emailName(internal);
    if (shown === internal) return text;
    let out = String(text).split(editorEscaped(internal)).join(editorEscaped(shown));
    out = out.split(internal).join(shown);
    return out;
}

/**
 * What a generated Request to send prints after "Ref:" and the base name of
 * its PDF and CSV files: 'request-to-send-<supplier's letters>-<number>-v<n>'.
 * With no number in the container's name the reference is left empty and the
 * file is named without one.
 */
function requestNames({ name, supplierName, version }) {
    const ref = supplierRef(name) || '';
    const supplierSlug = String(supplierName || '').replace(/[^A-Za-z]/g, '') || 'supplier';
    const safeRef = ref.replace(/[^A-Za-z0-9._-]/g, '_');
    return {
        ref,
        fileBase: ['request-to-send', supplierSlug, safeRef, `v${version}`].filter(Boolean).join('-'),
    };
}

// The stored template the portal email starts from (category 'Supplier Quote',
// so ShipLine offers it in the Request to send dialog). Seeded by
// 2026-11-13_20_seed_request_to_send_portal_template.js.
const TEMPLATE_KEY = 'request_to_send_portal';
// The same for a new version of a request the supplier was already sent
// (requestUpdate). Seeded by 2026-11-13_40_seed_request_to_send_update_template.js.
const UPDATE_TEMPLATE_KEY = 'request_to_send_update_portal';

const DELIVERIES = ['portal', 'attached'];

// email_receipts.email_type. A Request to send has its own since 2026-11-13,
// whichever way it goes out; before that it was logged as the forwarder's
// 'draft_container_quote', which every other draft-container document still is.
const EMAIL_TYPE = 'request_to_send';
const emailTypeFor = docType => (docType === 'supplier-quote' ? EMAIL_TYPE : 'draft_container_quote');

// The portal's Requests to send page, opened on this document's row (portal:
// web/src/screens/RequestsToSendScreen.tsx). Never the PDF's own address. A
// signed-out visitor is sent to sign in and brought back here.
const requestPortalUrl = (base, documentId) =>
    `${base}/requests-to-send?doc=${encodeURIComponent(documentId)}`;

/**
 * How a draft-container document is delivered, and why it is not a link when it
 * is not. The portal lists a Request to send only to users linked to a live
 * supplier named EXACTLY as the document's supplier (portal README section 10),
 * so without such a supplier the link would open an empty page:
 *   not_request_to_send  a forwarder quote or a plain quote (they go to a forwarder)
 *   portal_url_unset     SUPPLIER_PORTAL_URL is not configured on this server
 *   portal_unavailable   no portal database here
 *   no_supplier          no live supplier has the document's exact supplier name
 */
function deliveryFor({ docType, supplier, portalBase, supplierMatched, portalAvailable }) {
    const attached = reason => ({ delivery: 'attached', reason });
    if (docType !== 'supplier-quote') return attached('not_request_to_send');
    if (!portalBase) return attached('portal_url_unset');
    if (!String(supplier == null ? '' : supplier).trim()) return attached('no_supplier');
    if (!portalAvailable) return attached('portal_unavailable');
    if (!supplierMatched) return attached('no_supplier');
    return { delivery: 'portal', reason: null };
}

/**
 * The `delivery` a caller says it showed the sender. Absent = a ShipLine from
 * before this existed: its wording says "attached", so the email goes out
 * attached and nothing is checked. → { ok, requested } | { ok: false, error }.
 */
function requestedDelivery(value) {
    if (value === undefined || value === null || value === '') return { ok: true, requested: null };
    if (DELIVERIES.includes(value)) return { ok: true, requested: value };
    return { ok: false, error: '`delivery` must be "portal" or "attached".' };
}

const REASON_TEXT = {
    not_request_to_send: 'it is not a Request to send',
    portal_url_unset: 'the supplier portal address is not configured',
    portal_unavailable: 'the supplier portal is not available here',
    no_supplier: 'no supplier on the portal has this exact name',
};

/**
 * null when the email can go out the way the sender was shown; otherwise the
 * 409 body. The wording of the email depends on the way it goes out ("on the
 * portal" against "attached"), so a change between opening the dialog and
 * pressing Send is handed back to the sender, never sent as written.
 */
function deliveryConflict(requested, actual) {
    if (!requested || requested === actual.delivery) return null;
    const error = actual.delivery === 'attached'
        ? `This request can no longer go out as a portal link (${REASON_TEXT[actual.reason] || actual.reason}): it would be sent with the PDF and CSV attached. Check the wording and send again.`
        : 'This supplier is on the portal now: the request goes out as a link to the portal, with nothing attached. Check the wording and send again.';
    return { code: 'DELIVERY_CHANGED', error, delivery: actual.delivery, reason: actual.reason, allowedNext: [actual.delivery] };
}

const PORT_MAX = 128;   // draft_container_document_sends.request_port
const DAY_RE = /^(\d{4})-(\d{2})-(\d{2})$/;

// A real calendar day written YYYY-MM-DD, from its parts (no Date parsing of
// the string, so no time zone can move it).
function isCalendarDay(s) {
    const m = DAY_RE.exec(s);
    if (!m) return false;
    const [y, mo, d] = [Number(m[1]), Number(m[2]), Number(m[3])];
    // A day that does not exist rolls over into another month (the 31st of
    // November is the 1st of December), and a year under 100 is read as 19xx.
    const date = new Date(Date.UTC(y, mo - 1, d));
    return date.getUTCFullYear() === y && date.getUTCMonth() === mo - 1;
}

/**
 * What the email told the supplier about the departure, as the send dialog
 * filled it in: `fields: { etd: 'YYYY-MM-DD', port: 'Shanghai' }`, both
 * optional. Stored on the send row and shown on the portal beside the PDF, so
 * the page says what the email said. → { ok, etd, port } | { ok: false, error }.
 */
function parseSentFields(fields) {
    if (fields === undefined || fields === null) return { ok: true, etd: null, port: null };
    if (typeof fields !== 'object' || Array.isArray(fields)) return { ok: false, error: '`fields` must be an object.' };

    let etd = null;
    if (fields.etd !== undefined && fields.etd !== null && fields.etd !== '') {
        if (typeof fields.etd !== 'string' || !isCalendarDay(fields.etd)) {
            return { ok: false, error: '`fields.etd` must be a date written YYYY-MM-DD.' };
        }
        etd = fields.etd;
    }

    let port = null;
    if (fields.port !== undefined && fields.port !== null && fields.port !== '') {
        if (typeof fields.port !== 'string') return { ok: false, error: '`fields.port` must be text.' };
        const clean = fields.port.replace(/\s+/g, ' ').trim();
        if (clean.length > PORT_MAX) return { ok: false, error: `\`fields.port\` is longer than ${PORT_MAX} characters.` };
        port = clean || null;
    }
    return { ok: true, etd, port };
}

const MONTHS = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];

/** 'YYYY-MM-DD' as { en: '22 November 2026', zh: '2026年11月22日' }, or null. */
function formatEtd(etd) {
    if (typeof etd !== 'string' || !isCalendarDay(etd)) return null;
    const [, y, mo, d] = DAY_RE.exec(etd);
    return { en: `${Number(d)} ${MONTHS[Number(mo) - 1]} ${y}`, zh: `${y}年${Number(mo)}月${Number(d)}日` };
}

/**
 * The fixed portal block under the template body: what is waiting on the
 * portal, the button to it, and how to get an account. English and Chinese.
 * `etd` and `port` are what the sender filled in (parseSentFields); a line is
 * only printed for the ones that were given. `preapproved` = every recipient
 * was granted access to this document's supplier.
 */
function requestPortalBlockHtml({ version, etd, port, url, registerUrl, preapproved }) {
    const v = version != null ? ` (version ${escapeHtml(version)})` : '';
    const vZh = version != null ? `（第 ${escapeHtml(version)} 版）` : '';
    const day = formatEtd(etd);
    const en = [];
    const zh = [];
    if (day) { en.push(`ETD <strong>${escapeHtml(day.en)}</strong>`); zh.push(`预计开船日 <strong>${escapeHtml(day.zh)}</strong>`); }
    if (port) { en.push(`port of departure <strong>${escapeHtml(port)}</strong>`); zh.push(`起运港 <strong>${escapeHtml(port)}</strong>`); }
    const facts = en.length ? `<br>${en.join(' · ')}` : '';
    const factsZh = zh.length ? `<br>${zh.join(' · ')}` : '';
    return blockHtml({
        leadHtml: `Your <strong>packing list (Request to send)</strong>${v} is on the supplier portal.${facts}<br>Sign in, open <strong>Requests to send</strong>, and download the PDF or the CSV.`
            + `<br><br>您的<strong>装箱单（发货通知）</strong>${vZh}已发布在供应商门户。${factsZh}<br>请登录门户，打开<strong>“发货通知”</strong>，下载 PDF 或 CSV。`,
        url,
        regUrl: registerUrl,
        preapproved,
        seeEn: 'it',
        seeZh: '该文件',
        footHtml: 'No file is attached to this email: the packing list is kept on the portal, so you always download the current version. / 本邮件不附带文件：装箱单保存在门户中，您下载的始终是最新版本。',
    });
}

/** supplier_users.decided_note for a recipient this send pre-approved. */
const preapprovalNote = ({ ref, version, actor }) =>
    `Pre-approved from ShipLine: Request to send ${ref} v${version} emailed by ${actor || 'ShipLine'}`;

/**
 * How one send went out and what it said, from its draft_container_document_sends
 * row (request_etd already read as 'YYYY-MM-DD' text). A row from before the
 * columns existed went out attached and said nothing we kept.
 */
// ── A new version of a request the supplier already has ─────────────────────
// A supplier works from the last packing list they were sent. When the
// container changes and the request is generated and sent again, the email
// must say so: that this version replaces the earlier one, and what a supplier
// acts on that is different (docs/request-to-send-portal.md, "Updates").

/**
 * Whether `doc` is an update, and of what. `previous` is the latest EARLIER
 * version of the same request (same container and supplier) that was emailed
 * to anyone: { version, lines_json }, or null when this is the first they are
 * sent (the SQL is resolveUpdate in src/services/request-to-send-portal.js).
 * → null (not an update), or { replacesVersion, changes }:
 *   changes  what differs between the lines the two PDFs were printed from
 *            (their lines_json snapshots), by the same rule that flags a request
 *            STALE in ShipLine (src/lib/request-to-send-drift.js): an item added
 *            or removed, its units, its lot. [] when none of that changed
 *            (a carton size or a weight did). null when either version has no
 *            snapshot, so what changed is not known.
 */
function requestUpdate(previous, doc) {
    if (!previous) return null;
    const replacesVersion = Number(previous.version);
    const then = drift.parseRequestLines(previous.lines_json);
    const now = drift.parseRequestLines(doc && doc.lines_json);
    if (!then || !now) return { replacesVersion, changes: null };
    return { replacesVersion, changes: drift.requestDrift(then.lines, now.lines).changes };
}

const CHANGE_KINDS = ['units', 'lot', 'added', 'removed'];
const CHANGE_KEYS = ['kind', 'item', 'lot', 'lotWas', 'unitsWas', 'unitsNow'];

/**
 * An update as a send row keeps it (request_update, JSON) and as it is read
 * back, from the column or from an object: { replacesVersion, changes } with
 * only the keys and the kinds of change this code knows. Anything that is not
 * an update → null.
 */
function storedUpdate(value) {
    let v = value;
    if (typeof v === 'string') {
        try { v = JSON.parse(v); } catch { return null; }
    }
    if (!v || typeof v !== 'object' || Array.isArray(v)) return null;
    if (!Number.isInteger(v.replacesVersion) || v.replacesVersion < 1) return null;
    const changes = Array.isArray(v.changes)
        ? v.changes
            .filter(c => c && typeof c === 'object' && CHANGE_KINDS.includes(c.kind))
            .map(c => Object.fromEntries(CHANGE_KEYS.filter(k => k in c).map(k => [k, c[k]])))
        : null;
    return { replacesVersion: v.replacesVersion, changes };
}

const count = v => Number(v).toLocaleString('en-GB');

/** One change as a supplier reads it: { en, zh }, plain text. */
function changeText(c) {
    const lotEn = c.lot ? ` (lot ${c.lot})` : '';
    const lotZh = c.lot ? `（批号 ${c.lot}）` : '';
    switch (c.kind) {
        case 'units':
            return {
                en: `${c.item}${lotEn}: ${count(c.unitsWas)} → ${count(c.unitsNow)} units`,
                zh: `${c.item}${lotZh}：数量 ${count(c.unitsWas)} → ${count(c.unitsNow)}`,
            };
        case 'lot': {
            const moved = c.unitsWas != null && c.unitsNow != null;
            return {
                en: `${c.item}: lot ${c.lotWas ?? 'none'} → ${c.lot ?? 'none'}${moved ? `, ${count(c.unitsWas)} → ${count(c.unitsNow)} units` : ''}`,
                zh: `${c.item}：批号 ${c.lotWas ?? '无'} → ${c.lot ?? '无'}${moved ? `，数量 ${count(c.unitsWas)} → ${count(c.unitsNow)}` : ''}`,
            };
        }
        case 'added':
            return { en: `${c.item}${lotEn}: added, ${count(c.unitsNow)} units`, zh: `${c.item}${lotZh}：新增，数量 ${count(c.unitsNow)}` };
        default:
            return { en: `${c.item}${lotEn}: removed (was ${count(c.unitsWas)} units)`, zh: `${c.item}${lotZh}：已取消（原数量 ${count(c.unitsWas)}）` };
    }
}

// As many changes as an email lists; the rest are counted.
const MAX_CHANGES = 30;

/**
 * The fixed block an update's email carries under its body, whichever way it
 * goes out: this is version N, it replaces version M, and what changed.
 * English and Chinese. `update` is requestUpdate's answer.
 */
function updateBlockHtml({ version, replacesVersion, changes }) {
    const v = escapeHtml(version);
    const was = escapeHtml(replacesVersion);
    const p = html => `<p style="margin:0 0 8px;font-family:${FONT};font-size:14px;line-height:1.55;color:${INK};">${html}</p>`;
    const parts = [
        p(`<strong>This is version ${v} of this request.</strong> It replaces version ${was}: please work from this one.<br>`
            + `<strong>这是本通知的第 ${v} 版。</strong>它取代第 ${was} 版，请以本版为准。`),
    ];
    if (Array.isArray(changes) && changes.length) {
        const shown = changes.slice(0, MAX_CHANGES).map(changeText);
        parts.push(p('What changed / 变更内容：'));
        parts.push(
            `<ul style="margin:0 0 8px;padding-left:20px;font-family:${FONT};font-size:14px;line-height:1.5;color:${INK};">`
            + shown.map(t => `<li style="margin:0 0 4px;">${escapeHtml(t.en)}<br><span style="color:${MUTED};">${escapeHtml(t.zh)}</span></li>`).join('')
            + '</ul>'
        );
        const more = changes.length - shown.length;
        if (more > 0) parts.push(p(`… and ${more} more: see the packing list. / 另有 ${more} 项，详见装箱单。`));
    } else if (Array.isArray(changes)) {
        parts.push(p(`The items, quantities and lots are the same as in version ${was}; other details were updated.<br>`
            + `产品、数量和批号与第 ${was} 版相同，其他信息有更新。`));
    }
    return noteBox('Updated request / 通知已更新', parts.join(''));
}

function sendFactsOf(row) {
    const r = row || {};
    return {
        delivery: r.delivery === 'portal' ? 'portal' : 'attached',
        etd: typeof r.request_etd === 'string' && isCalendarDay(r.request_etd) ? r.request_etd : null,
        port: typeof r.request_port === 'string' && r.request_port.trim() ? r.request_port : null,
    };
}

module.exports = {
    TEMPLATE_KEY,
    UPDATE_TEMPLATE_KEY,
    requestUpdate,
    storedUpdate,
    changeText,
    updateBlockHtml,
    MAX_CHANGES,
    DELIVERIES,
    EMAIL_TYPE,
    emailTypeFor,
    supplierRef,
    emailName,
    hideInternalName,
    requestNames,
    PORT_MAX,
    requestPortalUrl,
    deliveryFor,
    requestedDelivery,
    deliveryConflict,
    parseSentFields,
    formatEtd,
    requestPortalBlockHtml,
    preapprovalNote,
    sendFactsOf,
};
