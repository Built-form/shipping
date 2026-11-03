'use strict';

// QC visit schedule links — the pure rules (docs/qc-schedule-link.md).
//
// The inspector's email carries one no-login link per visit instead of the
// products' QC documents as attachments. The page behind it (the public
// qcScheduleApi Lambda, src/handlers/qc-schedule.js) lists every product to
// inspect with its files, always as the visit is NOW, and highlights what
// changed since the last email to the inspector. Staff see the same diff per
// party (factory, inspector) in ShipLine as a "products changed since the
// last email" banner with SEND UPDATE.
//
// This module holds everything that needs no database: the token and URL,
// the lines snapshot stored on each send (qc_visit_sends.lines_snapshot_json)
// and the diff against it, the email paragraph for the link and the {changes}
// list, and the page's HTML. The SQL is in src/services/qc-schedule-routes.js.

const crypto = require('crypto');

// /api/v1/qc-schedule/<token> on the public API host (PUBLIC_API_BASE_URL, or
// the request's host, as the confirm-receipt links).
const SCHEDULE_PATH = '/api/v1/qc-schedule';
// 32 random bytes as base64url: 43 characters. The token opens files, so it
// is long, unlike the 5-character availability tokens.
const TOKEN_RE = /^[A-Za-z0-9_-]{43}$/;
const AUDIENCES = ['inspector'];
// Shown in an email preview when sending would create the link.
const SCHEDULE_PLACEHOLDER = '(schedule link will be created when sent)';
// The fields of a line whose change the inspector must hear about.
const CHANGE_FIELDS = ['quantityToInspect', 'sampleMethod', 'note'];
const SAMPLE_LABELS = { full: 'Full', aql: 'AQL', custom: 'Custom', destructive: 'Destructive' };

const newToken = () => crypto.randomBytes(32).toString('base64url');
const isToken = t => typeof t === 'string' && TOKEN_RE.test(t);

function scheduleUrl(base, token) {
    const b = String(base || '').trim().replace(/\/+$/, '');
    if (!b || !token) return null;
    return `${b}${SCHEDULE_PATH}/${token}`;
}

function esc(s) {
    return String(s == null ? '' : s)
        .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}

const str = v => (v == null ? '' : String(v).trim());
const numOr = (v, d = 0) => (v == null || v === '' || Number.isNaN(Number(v)) ? d : Number(v));

// ── Lines snapshot and diff ─────────────────────────────────────────────────

// A line row, from either loader: the comms route's (jf_code, …) or the visit
// routes' (o_jf_code, …).
const col = (r, k) => (r[k] !== undefined ? r[k] : r[`o_${k}`]);

/**
 * The visit's lines as stored on a send: one entry per line, by orderId.
 *   rows: qc_visit_lines joined to orders.
 * → [{ orderId, jfCode, productName, poNumber, lotNumber, supplier,
 *      quantityToInspect, sampleMethod, note }]
 */
function linesSnapshot(rows) {
    return (rows || []).map(r => ({
        orderId: Number(r.order_id),
        jfCode: str(col(r, 'jf_code')) || null,
        productName: str(col(r, 'product_name')) || null,
        poNumber: str(col(r, 'po_number')) || null,
        lotNumber: str(col(r, 'lot_number')) || null,
        supplier: str(col(r, 'supplier')) || null,
        quantityToInspect: numOr(r.quantity_to_inspect),
        sampleMethod: r.sample_method || 'full',
        note: str(r.note) || null,
    })).sort((a, b) => a.orderId - b.orderId);
}

// qc_visit_sends.lines_snapshot_json (parsed by mysql2, or text) → the
// snapshot, or null when NULL / unreadable (an email sent before snapshots).
function parseSnapshot(v) {
    if (v == null) return null;
    let list = v;
    if (typeof v === 'string' || Buffer.isBuffer(v)) {
        try { list = JSON.parse(String(v)); } catch { return null; }
    }
    if (!Array.isArray(list)) return null;
    return list.filter(x => x && typeof x === 'object' && Number.isInteger(Number(x.orderId))).map(x => ({
        orderId: Number(x.orderId),
        jfCode: x.jfCode == null ? null : String(x.jfCode),
        productName: x.productName == null ? null : String(x.productName),
        poNumber: x.poNumber == null ? null : String(x.poNumber),
        lotNumber: x.lotNumber == null ? null : String(x.lotNumber),
        supplier: x.supplier == null ? null : String(x.supplier),
        quantityToInspect: numOr(x.quantityToInspect),
        sampleMethod: x.sampleMethod || 'full',
        note: x.note == null ? null : String(x.note),
    }));
}

/**
 * What changed between the lines an email showed (`before`) and the lines now.
 * Lines are matched by orderId.
 * → { added: Line[], removed: Line[], changed: (Line & { before })[], count }
 *   changed[].before holds only the CHANGE_FIELDS that differ, as they were.
 */
function diffLines(before, now) {
    const prev = new Map((before || []).map(l => [l.orderId, l]));
    const cur = new Map((now || []).map(l => [l.orderId, l]));
    const added = [];
    const changed = [];
    for (const l of cur.values()) {
        const p = prev.get(l.orderId);
        if (!p) { added.push(l); continue; }
        const was = {};
        for (const f of CHANGE_FIELDS) if ((p[f] ?? null) !== (l[f] ?? null)) was[f] = p[f] ?? null;
        if (Object.keys(was).length) changed.push({ ...l, before: was });
    }
    const removed = [...prev.values()].filter(l => !cur.has(l.orderId));
    return { added, removed, changed, count: added.length + removed.length + changed.length };
}

// "2 added, 1 removed, 1 changed" ('' when nothing changed).
function changeSummary(diff) {
    if (!diff) return '';
    const parts = [];
    if (diff.added.length) parts.push(`${diff.added.length} added`);
    if (diff.removed.length) parts.push(`${diff.removed.length} removed`);
    if (diff.changed.length) parts.push(`${diff.changed.length} changed`);
    return parts.join(', ');
}

// A snapshot line as an audience may see it: the factory never learns another
// supplier's PO number (the comms route's linesFor rule). `hostSupplier` is
// the visit's normalised supplier; `norm` the normaliser (qc-visits normSupplier).
function lineForAudience(l, audience, hostSupplier, norm) {
    if (audience === 'inspector') return l;
    const movedIn = norm(l.supplier || '') !== hostSupplier;
    return audience === 'factory' && !movedIn ? l : { ...l, poNumber: null };
}

const units = n => `${Number(n || 0).toLocaleString('en-GB')} ${Number(n) === 1 ? 'unit' : 'units'}`;

function changeLineText(l) {
    return [str(l.jfCode) || '—', str(l.productName) || '—', ...(l.poNumber ? [l.poNumber] : [])].join(' — ');
}

function changedFieldsText(l) {
    const out = [];
    const b = l.before || {};
    if ('quantityToInspect' in b) out.push(`${units(b.quantityToInspect)} → ${units(l.quantityToInspect)}`);
    if ('sampleMethod' in b) out.push(`sampling ${SAMPLE_LABELS[b.sampleMethod] || b.sampleMethod} → ${SAMPLE_LABELS[l.sampleMethod] || l.sampleMethod}`);
    if ('note' in b) out.push(l.note ? `note: ${l.note}` : 'note removed');
    return out.join('; ');
}

/**
 * The {changes} token of the update email: an HTML list of the added,
 * removed and changed lines (already filtered for the audience), or a
 * sentence when there is nothing to list.
 */
function changesHtml(diff) {
    if (!diff || !diff.count) return '<p>No product changes since our last email.</p>';
    const li = [
        ...diff.added.map(l => `<li><strong>Added:</strong> ${esc(changeLineText(l))} — ${esc(units(l.quantityToInspect))}</li>`),
        ...diff.changed.map(l => `<li><strong>Changed:</strong> ${esc(changeLineText(l))} — ${esc(changedFieldsText(l))}</li>`),
        ...diff.removed.map(l => `<li><strong>Removed:</strong> ${esc(changeLineText(l))}</li>`),
    ];
    return `<ul style="margin:0 0 12px;padding-left:20px;">${li.join('')}</ul>`;
}

function changesText(diff) {
    if (!diff || !diff.count) return 'No product changes since our last email.';
    return [
        ...diff.added.map(l => `Added: ${changeLineText(l)} — ${units(l.quantityToInspect)}`),
        ...diff.changed.map(l => `Changed: ${changeLineText(l)} — ${changedFieldsText(l)}`),
        ...diff.removed.map(l => `Removed: ${changeLineText(l)}`),
    ].join('; ');
}

// ── Email: the link paragraph ───────────────────────────────────────────────

const LINK_INTRO = 'All products to inspect, with their QC documents and files, are here (always up to date):';

function scheduleLinkHtml(url) {
    const target = url ? `<a href="${esc(url)}">${esc(url)}</a>` : `<em>${esc(SCHEDULE_PLACEHOLDER)}</em>`;
    return `<p>${esc(LINK_INTRO)}<br>${target}</p>`;
}

// The {scheduleLink} token's values: { text, html }; both '' without a link.
function scheduleLinkVars(url, { placeholder = false } = {}) {
    if (!url && !placeholder) return { text: '', html: '' };
    return { text: url || SCHEDULE_PLACEHOLDER, html: scheduleLinkHtml(url) };
}

const escRe = s => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
// Any schedule link in a body (an older, rotated one in a posted preview).
const LINK_RE = new RegExp(`(?:https?://[^\\s"'<>]*?)?${escRe(SCHEDULE_PATH)}/[A-Za-z0-9_-]{43}(?![A-Za-z0-9_-])`, 'g');
const PLACEHOLDERS = [`<em>${esc(SCHEDULE_PLACEHOLDER)}</em>`, esc(SCHEDULE_PLACEHOLDER), SCHEDULE_PLACEHOLDER];

/**
 * The HTML body with the schedule link in it, whatever the template or the
 * staff-edited body did: an older link or the preview placeholder becomes
 * the URL; a body with neither gets the paragraph appended.
 */
function appendScheduleLinkHtml(html, url) {
    let body = html == null ? '' : String(html);
    if (!url) return body;
    body = body.replace(LINK_RE, esc(url));
    const anchor = `<a href="${esc(url)}">${esc(url)}</a>`;
    for (const ph of PLACEHOLDERS) if (body.includes(ph)) body = body.split(ph).join(anchor);
    if (body.includes(url) || body.includes(esc(url))) return body;
    return `${body}${scheduleLinkHtml(url)}`;
}

// A body sent WITHOUT the link: the preview's placeholder never reaches anyone.
function stripSchedulePlaceholderHtml(html) {
    let body = html == null ? '' : String(html);
    body = body.split(scheduleLinkHtml(null)).join('');
    for (const ph of PLACEHOLDERS) body = body.split(ph).join('');
    return body;
}

// ── The page ────────────────────────────────────────────────────────────────

function fmtBytes(n) {
    if (n == null || Number.isNaN(Number(n))) return '';
    const b = Number(n);
    if (b < 1024) return `${b} B`;
    if (b < 1024 * 1024) return `${(b / 1024).toFixed(0)} KB`;
    return `${(b / (1024 * 1024)).toFixed(1)} MB`;
}

const STAMP_FMT = new Intl.DateTimeFormat('en-GB', { timeZone: 'UTC', day: 'numeric', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit', hourCycle: 'h23' });
const fmtStamp = iso => (iso ? `${STAMP_FMT.format(new Date(iso)).replace(/,/g, '')} UTC` : '');

const PAGE_CSS = `
:root{--bg:#f6f6f4;--card:#fff;--ink:#1d1d1b;--ink2:#55554f;--ink3:#8a8a83;--rule:#e2e2dc;--accent:#1f5fa8;--new:#1e7f4f;--chg:#b26a00;--del:#b03a2e;--newbg:#e9f6ef;--chgbg:#fff4e0;--delbg:#fbeceb}
@media (prefers-color-scheme:dark){:root{--bg:#141413;--card:#1d1d1b;--ink:#ecece8;--ink2:#b5b5ad;--ink3:#86867f;--rule:#33332f;--accent:#7fb0ea;--new:#6fcf9b;--chg:#f0b45a;--del:#f08a80;--newbg:#16291f;--chgbg:#2e2412;--delbg:#2f1a18}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:15px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,"PingFang SC","Microsoft YaHei",sans-serif}
main{max-width:860px;margin:0 auto;padding:20px 16px 48px}
h1{font-size:22px;margin:0 0 4px}h2{font-size:15px;margin:28px 0 10px;text-transform:uppercase;letter-spacing:.06em;color:var(--ink2)}
.sub{color:var(--ink2);margin:0 0 16px}.card{background:var(--card);border:1px solid var(--rule);border-radius:6px;padding:14px 16px;margin-bottom:12px}
dl{display:grid;grid-template-columns:max-content 1fr;gap:4px 14px;margin:0}dt{color:var(--ink3)}dd{margin:0;overflow-wrap:anywhere}
.banner{border-radius:6px;padding:12px 14px;margin:0 0 16px;border:1px solid var(--rule)}
.banner.chg{background:var(--chgbg);border-color:var(--chg)}.banner.del{background:var(--delbg);border-color:var(--del)}.banner.info{background:var(--card)}
.code{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-weight:600}
.prod.is-new{border-left:4px solid var(--new)}.prod.is-chg{border-left:4px solid var(--chg)}
.tag{display:inline-block;font-size:11px;font-weight:700;letter-spacing:.06em;padding:1px 6px;border-radius:3px;margin-left:6px;vertical-align:middle}
.tag.new{color:var(--new);background:var(--newbg)}.tag.chg{color:var(--chg);background:var(--chgbg)}.tag.del{color:var(--del);background:var(--delbg)}
table{width:100%;border-collapse:collapse;margin-top:8px;font-size:14px}th{text-align:left;font-weight:500;color:var(--ink3);font-size:12px;border-bottom:1px solid var(--rule);padding:4px 6px}
td{padding:6px;border-bottom:1px solid var(--rule);vertical-align:top}td.n{text-align:right;white-space:nowrap}tr.is-new td{background:var(--newbg)}tr.is-chg td{background:var(--chgbg)}
.was{color:var(--ink3);text-decoration:line-through;margin-right:4px}
ul.files{list-style:none;margin:10px 0 0;padding:0}ul.files li{padding:6px 0;border-top:1px dashed var(--rule);display:flex;gap:8px;flex-wrap:wrap;align-items:baseline}
ul.files a{color:var(--accent);font-weight:500;overflow-wrap:anywhere}.meta{color:var(--ink3);font-size:12px}.none{color:var(--ink3);font-size:13px;margin-top:8px}
.removed li{color:var(--ink2)}a.btn{display:inline-block;color:var(--accent);border:1px solid var(--accent);border-radius:4px;padding:5px 12px;text-decoration:none;font-size:13px}
footer{margin-top:32px;color:var(--ink3);font-size:12px}
@media (max-width:560px){dl{grid-template-columns:1fr}dt{margin-top:6px}table,thead,tbody,tr,td,th{font-size:13px}}
`;

function pageShell(title, inner) {
    return '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        + '<meta name="viewport" content="width=device-width,initial-scale=1">'
        + '<meta name="robots" content="noindex,nofollow"><meta name="referrer" content="no-referrer">'
        + `<title>${esc(title)}</title><style>${PAGE_CSS}</style></head><body><main>${inner}</main></body></html>`;
}

// "Link not recognised", "no longer active" …
function renderNoticePage(title, message) {
    return pageShell(title, `<h1>${esc(title)}</h1><p class="sub">${esc(message)}</p>`);
}

function fileItem(f, { since } = {}) {
    const isNew = since && f.addedAt && f.addedAt > since;
    const meta = [f.kind, f.version ? `v${f.version}` : null, fmtBytes(f.size)].filter(Boolean).join(' · ');
    return `<li><a href="${esc(f.href)}" target="_blank" rel="noopener noreferrer">${esc(f.name)}</a>`
        + `${isNew ? '<span class="tag new">NEW</span>' : ''}`
        + `${meta ? `<span class="meta">${esc(meta)}</span>` : ''}`
        + `${f.note ? `<span class="meta">— ${esc(f.note)}</span>` : ''}</li>`;
}

function aqlText(l) {
    const a = l.aql;
    if (!a || (a.level == null && a.critical == null && a.major == null && a.minor == null)) return '';
    return `AQL ${a.level || '—'} · ${a.critical ?? '—'} / ${a.major ?? '—'} / ${a.minor ?? '—'}`;
}

function lineRow(l) {
    const b = l.before || {};
    const qty = 'quantityToInspect' in b
        ? `<span class="was">${esc(Number(b.quantityToInspect).toLocaleString('en-GB'))}</span>${esc(Number(l.quantityToInspect).toLocaleString('en-GB'))}`
        : esc(Number(l.quantityToInspect).toLocaleString('en-GB'));
    const sampling = [
        'sampleMethod' in b ? `<span class="was">${esc(SAMPLE_LABELS[b.sampleMethod] || b.sampleMethod)}</span>` : '',
        esc(SAMPLE_LABELS[l.sampleMethod] || l.sampleMethod),
        aqlText(l) ? `<div class="meta">${esc(aqlText(l))}</div>` : '',
    ].join('');
    const tag = l.change === 'new' ? '<span class="tag new">NEW</span>' : l.change === 'changed' ? '<span class="tag chg">CHANGED</span>' : '';
    const cls = l.change === 'new' ? ' class="is-new"' : l.change === 'changed' ? ' class="is-chg"' : '';
    return `<tr${cls}><td>${esc(l.poNumber || '—')}${tag}${l.lotNumber ? `<div class="meta">lot ${esc(l.lotNumber)}</div>` : ''}</td>`
        + `<td class="n">${qty}</td><td>${sampling}</td><td>${esc(l.note || '')}${'note' in b ? '<div class="meta">note updated</div>' : ''}</td></tr>`;
}

/**
 * The schedule page. `m` (built by src/handlers/qc-schedule.js; every value
 * plain text, escaped here):
 *   { ref, status, statusNotice, supplier, factory, factoryAddress,
 *     factoryContact, visitDate, window, windowUk, inspector, productionState,
 *     reportDeadline, escalation, lastEmailAt (ISO | null), diff (| null),
 *     products: [{ jfCode, productName, change, lines: [line & { change, before? }],
 *                  files: [{ name, href, kind, version?, size?, note?, addedAt? }] }],
 *     visitFiles: [file], csvHref }
 */
function renderSchedulePage(m) {
    const out = [];
    out.push(`<h1>QC inspection ${esc(m.ref)}</h1>`);
    out.push(`<p class="sub">${esc(m.factory)} · ${esc(m.visitDate)} · ${esc(m.window)}${m.windowUk ? ` <span class="meta">(${esc(m.windowUk)})</span>` : ''}</p>`);

    if (m.statusNotice) out.push(`<div class="banner del"><strong>${esc(m.statusNotice)}</strong></div>`);
    if (m.diff && m.diff.count && m.lastEmailAt) {
        out.push(`<div class="banner chg"><strong>Updated since our email of ${esc(fmtStamp(m.lastEmailAt))}:</strong> ${esc(changeSummary(m.diff))}. Changes are marked below.</div>`);
    }

    out.push('<div class="card"><dl>');
    const row = (k, v) => out.push(`<dt>${esc(k)}</dt><dd>${esc(v)}</dd>`);
    row('Date', m.visitDate);
    row('Time', m.window + (m.windowUk ? ` (${m.windowUk})` : ''));
    row('Supplier', m.supplier);
    row('Factory', m.factory);
    row('Address', m.factoryAddress);
    row('Factory contact', m.factoryContact);
    row('Inspector', m.inspector);
    row('Production state expected', m.productionState);
    row('Report', `Full report with photos within ${m.reportDeadline} of finishing`);
    row('Escalation', m.escalation);
    out.push('</dl></div>');

    const unitTotal = m.products.reduce((s, p) => s + p.lines.reduce((t, l) => t + Number(l.quantityToInspect || 0), 0), 0);
    out.push(`<h2>Products to inspect · ${m.products.length} · ${esc(units(unitTotal))}</h2>`);
    if (m.csvHref) out.push(`<p><a class="btn" href="${esc(m.csvHref)}">Download products list (.csv)</a></p>`);
    if (!m.products.length) out.push('<p class="none">No products on this visit.</p>');
    for (const p of m.products) {
        const cls = p.change === 'new' ? ' is-new' : p.change === 'changed' ? ' is-chg' : '';
        const tag = p.change === 'new' ? '<span class="tag new">NEW</span>' : p.change === 'changed' ? '<span class="tag chg">CHANGED</span>' : '';
        out.push(`<section class="card prod${cls}">`);
        out.push(`<div><span class="code">${esc(p.jfCode || '—')}</span>${tag}</div>`);
        out.push(`<div>${esc(p.productName || '—')}</div>`);
        out.push('<table><thead><tr><th>PO</th><th class="n">Units</th><th>Sampling</th><th>Note</th></tr></thead><tbody>');
        for (const l of p.lines) out.push(lineRow(l));
        out.push('</tbody></table>');
        if (p.files.length) {
            out.push(`<ul class="files">${p.files.map(f => fileItem(f, { since: m.lastEmailAt })).join('')}</ul>`);
        } else {
            out.push('<p class="none">No QC documents for this product.</p>');
        }
        out.push('</section>');
    }

    if (m.diff && m.diff.removed.length) {
        out.push('<h2>Removed since our last email</h2><div class="card banner del"><ul class="removed">');
        for (const l of m.diff.removed) out.push(`<li><span class="tag del">REMOVED</span> ${esc(changeLineText(l))}</li>`);
        out.push('</ul><p class="meta">Do not inspect these.</p></div>');
    }

    out.push(`<h2>Files for the whole visit · ${m.visitFiles.length}</h2>`);
    out.push(m.visitFiles.length
        ? `<div class="card"><ul class="files">${m.visitFiles.map(f => fileItem(f, { since: m.lastEmailAt })).join('')}</ul></div>`
        : '<p class="none">None.</p>');

    out.push(`<footer>This page always shows the visit as it is now. File links open a fresh copy each time. Questions: reply to our email.</footer>`);
    return pageShell(`QC inspection ${m.ref}`, out.join(''));
}

module.exports = {
    SCHEDULE_PATH,
    AUDIENCES,
    SCHEDULE_PLACEHOLDER,
    CHANGE_FIELDS,
    newToken,
    isToken,
    scheduleUrl,
    linesSnapshot,
    parseSnapshot,
    diffLines,
    changeSummary,
    lineForAudience,
    changesHtml,
    changesText,
    scheduleLinkHtml,
    scheduleLinkVars,
    appendScheduleLinkHtml,
    stripSchedulePlaceholderHtml,
    renderSchedulePage,
    renderNoticePage,
};
