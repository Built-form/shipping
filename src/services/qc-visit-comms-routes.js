'use strict';

// /api/v1/qc-visits/:id/{sends,email-preview,email,ics} — emailing a QC visit
// to the factory (date proposal) or the inspector (assignment with the
// products' QC documents), the cancellation email to either, and the visit's
// calendar file.
//
// Sending mirrors POST /api/v1/quality-assurance-documents/:id/email in
// orders.js: the subject/body come from an email template (qc_visit_factory,
// qc_visit_inspector, qc_visit_cancel — src/lib/email-template-defaults.js)
// and can be overridden per send; the message goes to Front as multipart
// (channels/${FRONT_CHANNEL_ID}/messages) with a confirm-receipt link
// (email_type 'qc_visit'), and each send is a qc_visit_sends row the receipt
// is linked to. Attachments: the kit documents of the visit's products
// (inspector proposals only): each JF code's current China factory QC
// documents in JFPRO (view qc_kit_documents, src/lib/qc-kit-docs.js),
// read from the JFPRO documents bucket (JFPRO_DOCS_BUCKET); <ref>.ics; and an
// optional QA document PDF covering one of the visit's orders. What
// the factory receives (body, {lines}, .ics) never shows another supplier's
// PO number — see linesFor.
//
// Registered from orders.js; authed through the greedy /api/v1/{proxy+} route.

const { GetObjectCommand } = require('@aws-sdk/client-s3');
const QV = require('../lib/qc-visits');
const KD = require('../lib/qc-kit-docs');
const A = require('../lib/qc-availability');
// "Include availability link" on proposals (docs/qc-availability.md).
const { emailAvailabilityLink, availabilityEnabled, AvailabilityFail } = require('./qc-availability-routes');
const { buildIcs } = require('../lib/ics');
const {
    apiBaseUrlFromReq,
    createEmailReceipt,
    appendReceiptLink,
    linkReceiptToSend,
} = require('../lib/email-receipt');

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const AUDIENCES = ['factory', 'inspector'];
const KINDS = ['proposal', 'cancel'];
const MAX_RECIPIENTS = 20;
// Total size of everything attached to one email. The QA PDF and the .ics
// always go; kit documents fill what is left and the rest are skipped (listed
// in the response's skippedAttachments). Front caps a message at 25 MB.
const MAX_ATTACH_BYTES = 20 * 1024 * 1024;
const CHINA_OFFSET_HOURS = 8; // Asia/Shanghai is fixed +08:00, no DST
const ICS_UID_DOMAIN = 'built-form.co.uk';
const ICS_ORGANIZER_FALLBACK = `noreply@${ICS_UID_DOMAIN}`;
const DEFAULT_ESCALATION = 'Message us on WeChat or email while still at the factory if the defect rate exceeds the AQL limit, before leaving.';

const TEMPLATE_KEYS = {
    factory: 'qc_visit_factory',
    inspector: 'qc_visit_inspector',
    cancel: 'qc_visit_cancel',
};

const iso = v => v?.toISOString?.() ?? v ?? null;

function esc(s) {
    return String(s == null ? '' : s)
        .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}

const str = v => (v == null ? '' : String(v).trim());
const hm = v => (v ? String(v).slice(0, 5) : null);                 // TIME 'HH:MM:SS' → 'HH:MM'
const ymdOf = v => (v instanceof Date ? v.toISOString().slice(0, 10) : (v ? String(v).slice(0, 10) : null));
const safeFile = s => String(s || '').replace(/[^A-Za-z0-9._-]+/g, '_');

function parseJsonArray(v) {
    if (Array.isArray(v)) return v;
    if (typeof v === 'string') {
        try { const p = JSON.parse(v); return Array.isArray(p) ? p : [p]; } catch { return [v]; }
    }
    return [];
}

// ── Dates and times ─────────────────────────────────────────────────────────

const DAY_FMT = new Intl.DateTimeFormat('en-GB', { timeZone: 'UTC', weekday: 'short', day: 'numeric', month: 'short', year: 'numeric' });
const DAY_SHORT_FMT = new Intl.DateTimeFormat('en-GB', { timeZone: 'UTC', weekday: 'short', day: 'numeric', month: 'short' });
const UK_FMT = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Europe/London', year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', hourCycle: 'h23', timeZoneName: 'short',
});

function ymdToUtcDate(ymd) {
    const [y, m, d] = String(ymd).split('-').map(Number);
    return new Date(Date.UTC(y, m - 1, d));
}

// 'YYYY-MM-DD' → 'Thu 12 Nov 2026'
function fmtVisitDate(ymd) {
    if (!ymd) return null;
    return DAY_FMT.format(ymdToUtcDate(ymd)).replace(/,/g, '');
}

function fmtDayShort(ymd) {
    return DAY_SHORT_FMT.format(ymdToUtcDate(ymd)).replace(/,/g, '');
}

// China wall clock (visit date + HH:MM, fixed +08:00) → UTC instant.
function chinaInstant(ymd, hhmm) {
    const [y, m, d] = String(ymd).split('-').map(Number);
    const [h, mi] = String(hhmm).split(':').map(Number);
    return new Date(Date.UTC(y, m - 1, d, h - CHINA_OFFSET_HOURS, mi));
}

// The UK wall clock at an instant, with Europe/London's offset AT THAT
// INSTANT (GMT or BST), so a December visit booked in October prints GMT.
function ukAt(instant) {
    const parts = {};
    for (const p of UK_FMT.formatToParts(instant)) parts[p.type] = p.value;
    return {
        ymd: `${parts.year}-${parts.month}-${parts.day}`,
        hm: `${parts.hour}:${parts.minute}`,
        zone: parts.timeZoneName || 'UK',
    };
}

// '09:30–12:00 China time' / 'from 09:30 China time' / null (no time set).
function fmtChinaWindow(start, end) {
    if (!start) return null;
    return end ? `${start}–${end} China time` : `from ${start} China time`;
}

// The same window in UK time for the visit's own date, e.g.
// '01:30–04:00 UK time (GMT)', '23:00–01:00 …' across midnight spelled with
// days, or '… on Wed 11 Nov' when the whole window falls on the UK day before.
function fmtUkWindow(ymd, start, end) {
    if (!ymd || !start) return null;
    const s = ukAt(chinaInstant(ymd, start));
    if (!end) {
        return `from ${s.hm} UK time (${s.zone})${s.ymd !== ymd ? ` on ${fmtDayShort(s.ymd)}` : ''}`;
    }
    const e = ukAt(chinaInstant(ymd, end));
    const zone = s.zone === e.zone ? s.zone : `${s.zone}/${e.zone}`; // the window spans a UK clock change
    if (s.ymd !== e.ymd) {
        return `${fmtDayShort(s.ymd)} ${s.hm} – ${fmtDayShort(e.ymd)} ${e.hm} UK time (${zone})`;
    }
    return `${s.hm}–${e.hm} UK time (${zone})${s.ymd !== ymd ? ` on ${fmtDayShort(s.ymd)}` : ''}`;
}

// ── Template variables ─────────────────────────────────────────────────────

function factoryAddressOf(v) {
    return [v.factory_address, v.factory_city, v.factory_province].map(str).filter(Boolean).join(', ');
}

function factoryContactOf(v) {
    const name = str(v.factory_contact_name);
    const title = str(v.factory_contact_title);
    const bits = [];
    if (name) bits.push(title ? `${name} (${title})` : name);
    if (str(v.factory_contact_phone)) bits.push(`Tel ${str(v.factory_contact_phone)}`);
    if (str(v.factory_contact_wechat)) bits.push(`WeChat ${str(v.factory_contact_wechat)}`);
    if (str(v.factory_contact_email)) bits.push(str(v.factory_contact_email));
    return bits.join(' · ');
}

function lineText(l) {
    const qty = Number(l.quantity_to_inspect) || 0;
    const parts = [
        str(l.jf_code) || '—',
        str(l.product_name) || '—',
        ...(l.hide_po ? [] : [str(l.po_number) || '—']),
        `${qty.toLocaleString('en-GB')} ${qty === 1 ? 'unit' : 'units'}`,
    ];
    return parts.join(' — ')
        + (l.moved_in_label ? ' (moved in)' : '')
        + (l.sample_method === 'destructive' ? ' (destructive sample)' : '');
}

// The lines as an audience may see them. The inspector sees everything. The
// factory must not learn other suppliers' PO numbers, so a line whose order
// belongs to another supplier (moved in) prints without its PO number. With
// no audience (the downloadable .ics, which may be forwarded to anyone) no
// line prints its PO number.
function linesFor(visit, lines, audience) {
    if (audience === 'inspector') return lines;
    const host = QV.normSupplier(visit.supplier_name || '');
    return lines.map((l) => {
        const movedIn = QV.normSupplier(l.supplier || '') !== host;
        if (audience === 'factory' && !movedIn) return l;
        return { ...l, po_number: null, hide_po: true, moved_in_label: audience === 'factory' };
    });
}

function linesHtml(lines) {
    if (!lines.length) return '<p>—</p>';
    return '<ul style="margin:0 0 12px;padding-left:20px;">'
        + lines.map(l => `<li>${esc(lineText(l))}</li>`).join('')
        + '</ul>';
}

// The products list as a spreadsheet (what {lines} lists, one row a line), for
// inspectors who want it in Excel. A UTF-8 BOM so Excel reads Chinese product
// names correctly; CRLF rows. A line printed without its PO number (linesFor)
// has an empty PO cell.
const CSV_HEADER = ['JF Code', 'Product', 'PO Number', 'Units', 'Sample', 'Note'];
function csvCell(v) {
    const s = v == null ? '' : String(v);
    // A leading = + - @ would run as a formula in Excel.
    const safe = /^[=+\-@]/.test(s) ? `'${s}` : s;
    return /[",\r\n]/.test(safe) ? `"${safe.replace(/"/g, '""')}"` : safe;
}
function linesCsv(lines) {
    const rows = lines.map(l => [
        str(l.jf_code),
        str(l.product_name),
        l.hide_po ? '' : str(l.po_number),
        Number(l.quantity_to_inspect) || 0,
        l.sample_method === 'destructive' ? 'Destructive' : '',
        l.moved_in_label ? 'Moved in' : '',
    ]);
    return '﻿' + [CSV_HEADER, ...rows].map(r => r.map(csvCell).join(',')).join('\r\n') + '\r\n';
}

// Values for the template tokens. `text` is for the subject (plain text);
// `html` is for the body (escaped; {lines} is an HTML list).
function buildVars(visit, lines) {
    const ymd = ymdOf(visit.visit_date);
    const start = hm(visit.window_start);
    const end = hm(visit.window_end);
    const deadline = visit.report_deadline_hours != null ? Number(visit.report_deadline_hours) : 24;
    const state = visit.expected_production_state;
    const labels = QV.PRODUCTION_STATE_LABELS || {};
    const text = {
        ref: visit.ref || QV.formatRef(visit.id),
        supplier: str(visit.supplier_name),
        factory: str(visit.factory_label) || str(visit.supplier_name),
        factoryAddress: factoryAddressOf(visit) || '—',
        factoryContact: factoryContactOf(visit) || 'to be confirmed',
        visitDate: fmtVisitDate(ymd) || 'date to be agreed',
        window: fmtChinaWindow(start, end) || 'time to be agreed',
        windowUk: fmtUkWindow(ymd, start, end) || '',
        inspector: str(visit.inspector_name) || 'to be confirmed',
        inspectorCompany: str(visit.inspector_company),
        productionState: (state && labels[state]) || 'not specified',
        reportDeadline: `${deadline} ${deadline === 1 ? 'hour' : 'hours'}`,
        escalation: str(visit.escalation_note) || DEFAULT_ESCALATION,
    };
    const html = {};
    for (const [k, v] of Object.entries(text)) html[k] = esc(v);
    html.lines = linesHtml(lines);
    text.lines = lines.map(lineText).join('; ');
    return { text, html, ymd, start, end };
}

// ── Calendar ───────────────────────────────────────────────────────────────

function icsContent(visit, lines, { sequence, cancel, organizerEmail, attendees }) {
    const { text, ymd, start, end } = buildVars(visit, lines);
    const description = [
        `QC inspection ${text.ref}`,
        `Supplier: ${text.supplier}`,
        `Factory: ${text.factory}`,
        `Address: ${text.factoryAddress}`,
        `Factory contact: ${text.factoryContact}`,
        `Inspector: ${text.inspector}${text.inspectorCompany ? ` (${text.inspectorCompany})` : ''}`,
        `Production state expected: ${text.productionState}`,
        'Products:',
        ...lines.map(l => `- ${lineText(l)}`),
    ].join('\n');
    return buildIcs({
        uid: `qcvisit-${visit.id}@${ICS_UID_DOMAIN}`,
        sequence,
        method: cancel ? 'CANCEL' : 'REQUEST',
        summary: `${cancel ? 'Cancelled: ' : ''}QC inspection ${text.ref} – ${text.factory}`,
        description,
        location: [text.factory, factoryAddressOf(visit)].filter(Boolean).join(', '),
        date: ymd,
        start,
        end: start ? end : null,
        tz: visit.tz || 'Asia/Shanghai',
        organizerEmail: organizerEmail || ICS_ORGANIZER_FALLBACK,
        attendees,
    });
}

// ── Rows → JSON ─────────────────────────────────────────────────────────────

function sendToJson(r) {
    return {
        id: r.id,
        audience: r.audience,
        kind: r.kind || 'proposal',
        kitDocuments: KD.kitDocumentsFromJson(r.kit_documents_json),
        sentTo: parseJsonArray(r.sent_to).map(String),
        subject: r.subject || null,
        frontMessageUid: r.front_message_uid || null,
        frontConversationId: r.front_conversation_id || null,
        sentByEmail: r.sent_by_email || null,
        sentAt: iso(r.sent_at),
    };
}

function registerQcVisitCommsRoutes(app, deps) {
    const {
        withConnection, s3, poBucket, jfproDocsBucket, recordAudit, auditLogSchemaReady, log, getEmailTemplate, renderTemplate,
    } = deps;

    const fail = (res, status, error, code, payload) => res.status(status).json({ error, code, ...(payload || {}) });
    const visitIdOf = req => Number(req.params.id);

    async function loadVisit(conn, id) {
        const [rows] = await conn.query(`SELECT * FROM qc_visits WHERE id = ?`, [id]);
        return rows[0] || null;
    }

    async function loadLines(conn, visitId) {
        const [rows] = await conn.query(
            `SELECT l.id, l.order_id, l.quantity_to_inspect, l.sample_method,
                    o.jf_code, o.product_name, o.po_number, o.supplier
               FROM qc_visit_lines l
               LEFT JOIN orders o ON o.id = l.order_id
              WHERE l.visit_id = ?
              ORDER BY o.po_number, o.jf_code, l.id`,
            [visitId]
        );
        return rows;
    }

    async function getObjectBytes(key, bucket = poBucket) {
        const obj = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
        return Buffer.from(await obj.Body.transformToByteArray());
    }

    // The kit documents of the visit's products (src/lib/qc-kit-docs.js
    // kitDocumentsToSend): every line's JF code (_FQC lines use the parent
    // product's), each document once.
    async function loadKitDocuments(conn, lines) {
        const codes = [...new Set(lines.map(l => QV.normJfCode(l.jf_code || '')).filter(Boolean))];
        if (!codes.length) return [];
        const [rows] = await conn.query(KD.KIT_DOCUMENTS_SQL, [codes]);
        return KD.kitDocumentsToSend(codes, rows);
    }

    // availabilityUrl: the availability link this email carries (null: none);
    // availabilityPlaceholder: render the "will be created when sent" text
    // instead (preview of a link that does not exist yet).
    async function renderEmail(conn, visit, lines, audience, kind, { availabilityUrl = null, availabilityPlaceholder = false } = {}) {
        const key = kind === 'cancel' ? TEMPLATE_KEYS.cancel : TEMPLATE_KEYS[audience];
        const tpl = await getEmailTemplate(conn, key);
        const vars = buildVars(visit, lines);
        // {availabilityLink}: '' when the email carries no link, so the token
        // never prints literally.
        const av = A.availabilityVars(availabilityUrl, audience, { placeholder: availabilityPlaceholder });
        vars.text.availabilityLink = av.text;
        vars.html.availabilityLink = av.html;
        const fallbackSubject = kind === 'cancel'
            ? `Cancelled: QC inspection ${vars.text.ref}`
            : `QC inspection ${vars.text.ref}`;
        return {
            subject: renderTemplate(tpl.subject, vars.text) || fallbackSubject,
            body: renderTemplate(tpl.bodyHtml, vars.html) || '',
            vars,
        };
    }

    function suggestedTo(visit, audience) {
        const raw = audience === 'factory' ? visit.factory_contact_email : visit.inspector_email;
        return String(raw || '').split(/[;,\s]+/).map(s => s.trim()).filter(s => EMAIL_RE.test(s));
    }

    function attendeesFor(visit, emails) {
        const named = new Map();
        if (str(visit.factory_contact_email)) named.set(str(visit.factory_contact_email).toLowerCase(), str(visit.factory_contact_name) || null);
        if (str(visit.inspector_email)) named.set(str(visit.inspector_email).toLowerCase(), str(visit.inspector_name) || null);
        return emails.map(email => ({ name: named.get(email.toLowerCase()) || null, email }));
    }

    // GET /api/v1/qc-visits/:id/sends — every email sent for the visit, newest first.
    app.get('/api/v1/qc-visits/:id(\\d+)/sends', async (req, res) => {
        try {
            const id = visitIdOf(req);
            const out = await withConnection(async (conn) => {
                const visit = await loadVisit(conn, id);
                if (!visit) return null;
                const [rows] = await conn.query(
                    `SELECT * FROM qc_visit_sends WHERE visit_id = ? ORDER BY sent_at DESC, id DESC`,
                    [id]
                );
                return rows.map(sendToJson);
            });
            if (!out) return fail(res, 404, `QC visit ${id} not found.`, 'NOT_FOUND');
            res.json({ data: out });
        } catch (error) {
            log.error('[GET /qc-visits/:id/sends]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // GET /api/v1/qc-visits/:id/email-preview?audience=factory|inspector&kind=proposal|cancel
    //     [&includeAvailabilityLink=1]
    // The template rendered for this visit, the suggested recipients, and
    // availabilityEnabled (SUPPLIER_PORTAL_URL is set and, for a proposal, the
    // jportal table is reachable; availabilityUnavailable: true when it is
    // missing). With
    // includeAvailabilityLink (1/true/yes; proposals only) the body carries the
    // party's open availability link, or the placeholder when sending would
    // create one. The preview never creates a link.
    app.get('/api/v1/qc-visits/:id(\\d+)/email-preview', async (req, res) => {
        try {
            const id = visitIdOf(req);
            const audience = String(req.query.audience || '');
            const kind = String(req.query.kind || 'proposal');
            if (!AUDIENCES.includes(audience)) return fail(res, 400, 'audience must be factory or inspector.', 'BAD_FIELD', { field: 'audience' });
            if (!KINDS.includes(kind)) return fail(res, 400, 'kind must be proposal or cancel.', 'BAD_FIELD', { field: 'kind' });
            const includeAvailability = kind === 'proposal'
                && ['1', 'true', 'yes'].includes(String(req.query.includeAvailabilityLink || '').toLowerCase());
            const out = await withConnection(async (conn) => {
                const visit = await loadVisit(conn, id);
                if (!visit) return null;
                const lines = linesFor(visit, await loadLines(conn, id), audience);
                // Proposals always look the link up (read-only) so that
                // availabilityEnabled is false when the portal URL is unset or
                // the jportal table is missing, ticked or not; nothing about
                // availability (link or placeholder) is shown then.
                const look = kind === 'proposal' ? await emailAvailabilityLink(conn, visit, audience, { create: false, log }) : null;
                const enabled = look ? look.enabled : availabilityEnabled();
                const av = includeAvailability && look && look.enabled ? look : null;
                const rendered = await renderEmail(conn, visit, lines, audience, kind, {
                    availabilityUrl: av ? av.url : null, availabilityPlaceholder: !!av && !av.url,
                });
                const body = av ? A.appendAvailabilityHtml(rendered.body, av.url, audience) : rendered.body;
                return {
                    subject: rendered.subject, body, to: suggestedTo(visit, audience),
                    availabilityEnabled: enabled,
                    ...(look && look.unavailable ? { availabilityUnavailable: true } : {}),
                };
            });
            if (!out) return fail(res, 404, `QC visit ${id} not found.`, 'NOT_FOUND');
            res.json(out);
        } catch (error) {
            log.error('[GET /qc-visits/:id/email-preview]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // GET /api/v1/qc-visits/:id/ics[?cancel=1] — the visit as a calendar file.
    // SEQUENCE is the number of emails already sent for the visit, so a
    // downloaded copy never ranks below an invite that went out by email.
    // The download has no audience and may be forwarded to the factory, so
    // its products list no PO numbers (JF code — product — quantity).
    app.get('/api/v1/qc-visits/:id(\\d+)/ics', async (req, res) => {
        try {
            const id = visitIdOf(req);
            const cancel = ['1', 'true', 'yes'].includes(String(req.query.cancel || '').toLowerCase());
            const out = await withConnection(async (conn) => {
                const visit = await loadVisit(conn, id);
                if (!visit) return { notFound: true };
                if (!ymdOf(visit.visit_date)) return { noDate: true };
                const lines = linesFor(visit, await loadLines(conn, id), null);
                const [[{ n }]] = await conn.query(`SELECT COUNT(*) AS n FROM qc_visit_sends WHERE visit_id = ?`, [id]);
                const emails = [...suggestedTo(visit, 'factory'), ...suggestedTo(visit, 'inspector')];
                const ics = icsContent(visit, lines, {
                    sequence: Number(n) || 0,
                    cancel,
                    organizerEmail: req.userEmail,
                    attendees: attendeesFor(visit, emails),
                });
                return { ics, ref: visit.ref || QV.formatRef(visit.id) };
            });
            if (out.notFound) return fail(res, 404, `QC visit ${id} not found.`, 'NOT_FOUND');
            if (out.noDate) {
                return fail(res, 409, 'Set the visit date first.', 'BLOCKED', {
                    blockers: [{ key: 'missing_field', level: 'block', orderId: null, message: 'Set the visit date first.', meta: { field: 'visitDate' } }],
                });
            }
            res.set('Content-Type', `text/calendar; charset=utf-8; method=${cancel ? 'CANCEL' : 'REQUEST'}`);
            res.set('Content-Disposition', `attachment; filename="${safeFile(out.ref)}.ics"`);
            res.status(200).send(out.ics);
        } catch (error) {
            log.error('[GET /qc-visits/:id/ics]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // GET /api/v1/qc-visits/:id/products.csv?audience=inspector|factory
    // The products list as a CSV (`<ref>-products.csv`), the same rows the
    // email's {lines} prints for that audience (default inspector: every PO
    // number shown) and the same file attachProductList sends.
    app.get('/api/v1/qc-visits/:id(\\d+)/products.csv', async (req, res) => {
        try {
            const id = visitIdOf(req);
            const audience = req.query.audience == null || req.query.audience === '' ? 'inspector' : String(req.query.audience);
            if (!AUDIENCES.includes(audience)) return fail(res, 400, 'audience must be factory or inspector.', 'BAD_FIELD', { field: 'audience' });
            const out = await withConnection(async (conn) => {
                const visit = await loadVisit(conn, id);
                if (!visit) return null;
                return {
                    csv: linesCsv(linesFor(visit, await loadLines(conn, id), audience)),
                    ref: visit.ref || QV.formatRef(visit.id),
                };
            });
            if (!out) return fail(res, 404, `QC visit ${id} not found.`, 'NOT_FOUND');
            res.set('Content-Type', 'text/csv; charset=utf-8');
            res.set('Content-Disposition', `attachment; filename="${safeFile(out.ref)}-products.csv"`);
            res.status(200).send(out.csv);
        } catch (error) {
            log.error('[GET /qc-visits/:id/products.csv]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // POST /api/v1/qc-visits/:id/email
    // Body: { audience, kind?='proposal', to[], subject?, body?,
    //         attachKitFiles?, attachIcs?, attachQaDocumentId?, includeAvailabilityLink?,
    //         attachProductList? }
    // attachProductList (proposals only): the products list as
    // `<ref>-products.csv`, the rows that audience's {lines} shows.
    // attachBrief, sent by older clients, is ignored (there is no brief).
    // includeAvailabilityLink (proposals only): reuse the party's open
    // availability link or create one (default window, recipient to[0]) before
    // anything is fetched or sent; a posted body still holding the preview's
    // placeholder gets the URL, a body with neither gets the link appended.
    app.post('/api/v1/qc-visits/:id(\\d+)/email', async (req, res) => {
        try {
            if (!QV.canEdit(req.userType)) return fail(res, 403, 'Read only.', 'READ_ONLY');
            await auditLogSchemaReady;

            const id = visitIdOf(req);
            const b = req.body || {};
            const audience = String(b.audience || '');
            const kind = b.kind == null ? 'proposal' : String(b.kind);
            if (!AUDIENCES.includes(audience)) return fail(res, 400, 'audience must be factory or inspector.', 'BAD_FIELD', { field: 'audience' });
            if (!KINDS.includes(kind)) return fail(res, 400, 'kind must be proposal or cancel.', 'BAD_FIELD', { field: 'kind' });
            const toAddresses = [...new Set((Array.isArray(b.to) ? b.to : [b.to])
                .filter(x => typeof x === 'string')
                .map(x => x.trim())
                .filter(Boolean))];
            if (toAddresses.length === 0 || toAddresses.length > MAX_RECIPIENTS || toAddresses.some(a => !EMAIL_RE.test(a))) {
                return fail(res, 400, `Give 1 to ${MAX_RECIPIENTS} valid email addresses in \`to\`.`, 'BAD_FIELD', { field: 'to' });
            }
            if (b.subject != null && typeof b.subject !== 'string') return fail(res, 400, 'subject must be text.', 'BAD_FIELD', { field: 'subject' });
            if (b.body != null && typeof b.body !== 'string') return fail(res, 400, 'body must be text.', 'BAD_FIELD', { field: 'body' });
            let qaDocId = null;
            if (b.attachQaDocumentId != null && b.attachQaDocumentId !== '') {
                qaDocId = Number(b.attachQaDocumentId);
                if (!Number.isInteger(qaDocId) || qaDocId <= 0) return fail(res, 400, 'attachQaDocumentId must be a document id.', 'BAD_FIELD', { field: 'attachQaDocumentId' });
            }
            // A cancellation carries only the calendar cancellation (and a QA
            // document if asked); the kit documents belong to proposals, and
            // only the inspector's.
            const attachKitFiles = kind === 'proposal' && audience === 'inspector' && b.attachKitFiles === true;
            const attachIcs = b.attachIcs !== false; // CONTRACT-GAP: default not specified; the invite is on unless turned off
            const includeAvailability = kind === 'proposal' && b.includeAvailabilityLink === true;
            const attachProductList = kind === 'proposal' && b.attachProductList === true;

            if (!process.env.FRONT_API_TOKEN || !process.env.FRONT_CHANNEL_ID) {
                return fail(res, 500, 'Front is not configured (FRONT_API_TOKEN, FRONT_CHANNEL_ID).', 'FRONT_NOT_CONFIGURED');
            }
            if (!poBucket) return fail(res, 500, 'PO_DOCS_BUCKET env var not configured.', 'NO_BUCKET');
            if (attachKitFiles && !jfproDocsBucket) return fail(res, 500, 'JFPRO_DOCS_BUCKET env var not configured.', 'NO_BUCKET');

            const result = await withConnection(async (conn) => {
                const visit = await loadVisit(conn, id);
                if (!visit) return { status: 404, error: `QC visit ${id} not found.`, code: 'NOT_FOUND' };

                const terminal = (QV.TERMINAL_STATUSES || ['COMPLETED', 'CANCELLED']).includes(visit.status);
                if (kind === 'proposal' && terminal) {
                    return { status: 409, error: `This visit is ${String(visit.status).toLowerCase()}.`, code: 'TERMINAL' };
                }
                if (kind === 'cancel' && visit.status !== 'CANCELLED') {
                    const message = 'Cancel the visit before sending a cancellation.';
                    return { status: 409, error: message, code: 'BLOCKED', payload: { blockers: [{ key: 'bad_transition', level: 'block', orderId: null, message }] } };
                }
                if (kind === 'proposal' && !ymdOf(visit.visit_date)) {
                    const message = 'Set the visit date before sending.';
                    return { status: 409, error: message, code: 'BLOCKED', payload: { blockers: [{ key: 'missing_field', level: 'block', orderId: null, message, meta: { field: 'visitDate' } }] } };
                }

                // The availability link first, in its own transaction: a 409 /
                // 503 here stops the send before any attachment is fetched.
                let availability = null;
                if (includeAvailability) {
                    try {
                        availability = await emailAvailabilityLink(conn, visit, audience, {
                            create: true, to: toAddresses, userEmail: req.userEmail || null, recordAudit, log,
                        });
                    } catch (e) {
                        if (e instanceof AvailabilityFail) return { status: e.status, error: e.error, code: e.code, payload: e.payload };
                        throw e;
                    }
                }

                const ref = visit.ref || QV.formatRef(visit.id);
                const lines = await loadLines(conn, id);
                const shownLines = linesFor(visit, lines, audience);
                const attachments = [];   // { bytes, filename, type }
                const skippedAttachments = [];
                let usedBytes = 0;

                if (qaDocId != null) {
                    const [docs] = await conn.query(
                        `SELECT id, ref, version, s3_key, order_ids FROM quality_assurance_documents WHERE id = ? AND deleted_at IS NULL`,
                        [qaDocId]
                    );
                    if (!docs.length) return { status: 404, error: `Quality assurance document ${qaDocId} not found.`, code: 'NOT_FOUND', payload: { field: 'attachQaDocumentId' } };
                    const doc = docs[0];
                    // The document must cover at least one of this visit's orders.
                    const visitOrderIds = new Set(lines.map(l => Number(l.order_id)));
                    if (!parseJsonArray(doc.order_ids).map(Number).some(n => visitOrderIds.has(n))) {
                        return { status: 400, error: `Quality assurance document ${qaDocId} is not for this visit's orders.`, code: 'BAD_FIELD', payload: { field: 'attachQaDocumentId' } };
                    }
                    const bytes = await getObjectBytes(doc.s3_key);
                    attachments.push({ bytes, filename: `${safeFile(doc.ref || `QA-${doc.id}`)}-v${doc.version}.pdf`, type: 'application/pdf' });
                    usedBytes += bytes.length;
                }

                const [[{ n: priorSends }]] = await conn.query(
                    `SELECT COUNT(*) AS n FROM qc_visit_sends WHERE visit_id = ? AND audience = ?`,
                    [id, audience]
                );
                if (attachIcs) {
                    if (!ymdOf(visit.visit_date)) {
                        skippedAttachments.push({ filename: `${safeFile(ref)}.ics`, reason: 'The visit has no date.' });
                    } else {
                        const ics = icsContent(visit, shownLines, {
                            sequence: Number(priorSends) || 0,
                            cancel: kind === 'cancel',
                            organizerEmail: req.userEmail,
                            attendees: attendeesFor(visit, toAddresses),
                        });
                        const bytes = Buffer.from(ics, 'utf8');
                        attachments.push({
                            bytes,
                            filename: `${safeFile(ref)}.ics`,
                            type: `text/calendar; charset=utf-8; method=${kind === 'cancel' ? 'CANCEL' : 'REQUEST'}`,
                        });
                        usedBytes += bytes.length;
                    }
                }

                if (attachProductList) {
                    const bytes = Buffer.from(linesCsv(shownLines), 'utf8');
                    attachments.push({ bytes, filename: `${safeFile(ref)}-products.csv`, type: 'text/csv; charset=utf-8' });
                    usedBytes += bytes.length;
                }

                const sentKitDocuments = [];
                if (attachKitFiles) {
                    const docs = await loadKitDocuments(conn, lines);
                    const names = KD.attachmentNames(docs);
                    for (const [i, d] of docs.entries()) {
                        const filename = names[i];
                        if (d.sizeBytes != null && usedBytes + d.sizeBytes > MAX_ATTACH_BYTES) {
                            skippedAttachments.push({ filename, reason: 'Over the 20 MB attachment limit.' });
                            continue;
                        }
                        let bytes;
                        try {
                            bytes = await getObjectBytes(d.s3Key, jfproDocsBucket);
                        } catch (err) {
                            log.error('[qc-visits/email] kit document read failed', { visitId: id, documentId: d.documentId, versionId: d.versionId, err });
                            skippedAttachments.push({ filename, reason: 'The file could not be read.' });
                            continue;
                        }
                        if (usedBytes + bytes.length > MAX_ATTACH_BYTES) {
                            skippedAttachments.push({ filename, reason: 'Over the 20 MB attachment limit.' });
                            continue;
                        }
                        attachments.push({ bytes, filename, type: d.mimeType || 'application/octet-stream' });
                        usedBytes += bytes.length;
                        sentKitDocuments.push(KD.sentKitDocument(d));
                    }
                }

                const rendered = await renderEmail(conn, visit, shownLines, audience, kind, {
                    availabilityUrl: availability ? availability.url : null,
                });
                // One line (no header injection), at most 255 characters; stored the same.
                const finalSubject = String((typeof b.subject === 'string' && b.subject.trim()) ? b.subject : rendered.subject)
                    .replace(/[\r\n]+/g, ' ').trim().slice(0, 255);
                let htmlBody = (typeof b.body === 'string' && b.body.trim()) ? b.body : rendered.body;
                // With a link: older availability URLs and the placeholder →
                // the current URL; neither present → paragraph appended; never
                // both. Without one: the preview's placeholder is stripped so
                // it never reaches the recipient.
                htmlBody = availability
                    ? A.appendAvailabilityHtml(htmlBody, availability.url, audience)
                    : A.stripAvailabilityPlaceholderHtml(htmlBody);

                const receiptToken = await createEmailReceipt(conn, {
                    emailType: 'qc_visit', sentTo: toAddresses, subject: finalSubject,
                });
                const bodyWithReceipt = appendReceiptLink(htmlBody, receiptToken, apiBaseUrlFromReq(req));

                const form = new FormData();
                for (const addr of toAddresses) form.append('to[]', addr);
                form.append('subject', finalSubject);
                form.append('body', bodyWithReceipt);
                form.append('body_format', 'html');
                form.append('options[archive]', 'false');
                for (const a of attachments) {
                    form.append('attachments[]', new Blob([a.bytes], { type: a.type }), a.filename);
                }

                const frontUrl = `https://api2.frontapp.com/channels/${encodeURIComponent(process.env.FRONT_CHANNEL_ID)}/messages`;
                const resp = await fetch(frontUrl, {
                    method: 'POST',
                    headers: {
                        Authorization: `Bearer ${process.env.FRONT_API_TOKEN}`,
                        Accept: 'application/json',
                    },
                    body: form,
                });
                const respText = await resp.text();
                if (!resp.ok) {
                    log.error('[qc-visits/email] Front API error', { status: resp.status, body: respText.slice(0, 500) });
                    return {
                        status: 502, error: 'Front rejected the message.', code: 'FRONT_FAILED',
                        payload: { frontStatus: resp.status, frontBody: respText.slice(0, 500) },
                    };
                }
                let parsed = null;
                try { parsed = JSON.parse(respText); } catch { /* 202 with empty body is fine */ }
                const conversationUrl = parsed?._links?.related?.conversation || '';
                const frontConversationId = conversationUrl ? conversationUrl.split('/').pop() : null;
                const frontMessageUid = parsed?.message_uid || parsed?.id || null;

                const attachedNames = attachments.map(a => a.filename);
                // Front has sent the email. From here a database failure must not
                // answer 500: the user would press SEND again and the recipient
                // would get it twice. Log it and answer 201 with recorded: false.
                let send;
                try {
                    const [ins] = await conn.query(
                        `INSERT INTO qc_visit_sends
                            (visit_id, audience, kind, sent_to, kit_documents_json, subject, front_message_uid, front_conversation_id, sent_by_email)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                        [id, audience, kind, JSON.stringify(toAddresses),
                            sentKitDocuments.length ? JSON.stringify(sentKitDocuments) : null,
                            finalSubject, frontMessageUid, frontConversationId, req.userEmail || null]
                    );
                    await linkReceiptToSend(conn, receiptToken, 'qc_visit_sends', ins.insertId);
                    const [sendRows] = await conn.query(`SELECT * FROM qc_visit_sends WHERE id = ?`, [ins.insertId]);
                    send = sendToJson(sendRows[0]);
                } catch (recordErr) {
                    log.error('[qc-visits/email] sent through Front but not recorded', {
                        visitId: id, audience, kind, sentTo: toAddresses, frontMessageUid, frontConversationId,
                        error: recordErr.message, code: recordErr.code,
                    });
                    return {
                        ok: true,
                        send: {
                            id: null, audience, kind, kitDocuments: sentKitDocuments.length ? sentKitDocuments : null,
                            sentTo: toAddresses, subject: finalSubject, frontMessageUid, frontConversationId,
                            sentByEmail: req.userEmail || null, sentAt: new Date().toISOString(),
                            attachments: attachedNames, skippedAttachments,
                            availabilityLinkId: availability ? availability.linkId : null,
                            recorded: false,
                            warning: 'The email was sent, but it could not be recorded on the visit. Do not send it again.',
                        },
                    };
                }

                await recordAudit(conn, {
                    entityType: 'qc_visit', entityId: id, action: 'email_sent',
                    before: null,
                    after: {
                        ref, sendId: send.id, audience, kind, sentTo: toAddresses, subject: finalSubject,
                        kitDocuments: send.kitDocuments, attachments: attachedNames, skippedAttachments,
                        frontMessageUid, frontConversationId,
                        ...(availability ? { availabilityLinkId: availability.linkId } : {}),
                    },
                    userEmail: req.userEmail,
                });

                return {
                    ok: true,
                    send: { ...send, attachments: attachedNames, skippedAttachments, availabilityLinkId: availability ? availability.linkId : null },
                };
            });

            if (!result.ok) return fail(res, result.status, result.error, result.code, result.payload);
            res.status(201).json(result.send);
        } catch (error) {
            log.error('[POST /qc-visits/:id/email]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });
}

module.exports = {
    registerQcVisitCommsRoutes,
    // exported for tests
    _internals: { buildVars, fmtVisitDate, fmtChinaWindow, fmtUkWindow, lineText, linesFor, linesHtml, sendToJson, esc },
};
