'use strict';

// Auto-import supplier QC / inspection reports out of Front.
//
// Our inspection agent (AQI, always from @aqiservice.com) emails the purchasing
// inbox a QC report per pre-shipment inspection. The report arrives in one of
// two shapes:
//
//   Variant B — the report PDF is a direct attachment on the inbound Front
//               message (e.g. "PSI20260612-….pdf"). Body says "Report: In
//               attached".
//   Variant A — the report is a link to a jianguoyun (坚果云 / Nutstore) public
//               share folder: body says "Report: https://www.jianguoyun.com/p/…".
//               Those links expire (~2 weeks), which is the whole reason to pull
//               them automatically and promptly.
//
// This module finds those emails, fetches the report PDF (Front attachment, or
// jianguoyun folder → zip → extract PDF), de-dupes against anything already
// imported, uploads the PDF to the SAME S3 bucket/prefix the manual QC upload
// endpoint uses (so auto + manual reports sit alongside each other), and inserts
// a `qc_reports` row tagged with its origin. Analysis (Gemini extraction +
// order matching) is the existing pipeline — see analyzeAndPersist below, which
// mirrors the background runner in src/handlers/orders.js.
//
// Read paths proven against the live APIs before this was written:
//   Front : GET /conversations/search/{query}  (query "from:<domain> after:<ts>")
//           GET /conversations/{id}/messages
//           GET <attachment.url>               (bytes, same bearer token)
//   jgy   : GET /p/{hash}                       (HTML, holds `var PageInfo`)
//           GET /d/ajax/dirops/pubDIRLink?k=&dn=  -> { url } (zip of the folder)
//           GET /d/ajax/fileops/pubFileLink?k=&name= -> { url } (single file)

const os = require('os');
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const { pipeline } = require('stream/promises');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const { v4: uuidv4 } = require('uuid');
const log = require('../lib/logger');
const { analyzeQcReport } = require('./qc-report-check');

const FRONT_API = 'https://api2.frontapp.com';
const SUPPLIER_DOMAIN = (process.env.QC_FRONT_DOMAIN || 'aqiservice.com').toLowerCase();

const PO_BUCKET = process.env.PO_DOCS_BUCKET;
const PO_BUCKET_REGION = process.env.AWS_REGION || 'eu-north-1';
const s3 = new S3Client({ region: PO_BUCKET_REGION });

// Matches the manual upload endpoint (orders.js) so filenames key identically.
const SAFE_FILENAME_RE = /[^A-Za-z0-9._-]+/g;
const BROWSER_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)';

function publicS3Url(s3Key) {
    return `https://${PO_BUCKET}.s3.${PO_BUCKET_REGION}.amazonaws.com/${s3Key}`;
}

// ── Front ────────────────────────────────────────────────────────────────────
function frontHeaders() {
    if (!process.env.FRONT_API_TOKEN) {
        const e = new Error('FRONT_API_TOKEN is not configured.');
        e.code = 'NOT_CONFIGURED';
        throw e;
    }
    return { Authorization: `Bearer ${process.env.FRONT_API_TOKEN}`, Accept: 'application/json' };
}

async function frontGetJson(url) {
    const resp = await fetch(url, { headers: frontHeaders() });
    if (!resp.ok) {
        const body = await resp.text().catch(() => '');
        const e = new Error(`Front GET ${url} -> ${resp.status} ${body.slice(0, 200)}`);
        e.status = resp.status;
        throw e;
    }
    return resp.json();
}

// Front search wants the query in the path. We only ever build it from a fixed
// domain + numeric timestamp, so the sole character needing escaping is the
// space between operators — leave ':' literal (the API rejects %3A here).
async function* searchConversations(query) {
    let url = `${FRONT_API}/conversations/search/${query.replace(/ /g, '%20')}`;
    while (url) {
        const data = await frontGetJson(url);
        for (const c of data._results || []) yield c;
        url = data._pagination && data._pagination.next;
    }
}

async function listMessages(conversationId) {
    const out = [];
    let url = `${FRONT_API}/conversations/${encodeURIComponent(conversationId)}/messages`;
    while (url) {
        const data = await frontGetJson(url);
        for (const m of data._results || []) out.push(m);
        url = data._pagination && data._pagination.next;
    }
    return out;
}

function messageFromEmail(m) {
    const fromR = (m.recipients || []).find(r => r.role === 'from');
    return (fromR && fromR.handle) || (m.author && m.author.email) || null;
}

function emailInDomain(email, domain) {
    if (!email) return false;
    const d = String(email).split('@').pop().toLowerCase();
    return d === domain || d.endsWith('.' + domain);
}

// Plain text for link-scraping: prefer the message's `text`, else strip tags.
function messageText(m) {
    if (m.text && m.text.trim()) return m.text;
    return String(m.body || '').replace(/<[^>]+>/g, ' ');
}

// Readable body for storage: collapse runs of whitespace, cap length.
function messageBodySnippet(m) {
    return messageText(m).replace(/[ \t]+/g, ' ').replace(/\n{3,}/g, '\n\n').trim().slice(0, 4000);
}

// Email subjects all start "Re: <Name> QC Documents (Report)" — strip the reply
// prefix so the distinct part (the product/customer name) becomes the title.
function cleanSubject(s) {
    return String(s || '').replace(/^\s*(re|fwd?)\s*[:：]\s*/i, '').replace(/\s+/g, ' ').trim().slice(0, 255);
}

function frontWebUrl(conversationId) {
    return `https://app.frontapp.com/open/${conversationId}`;
}

// Only the report email threads — excludes the agent's invoice threads, which
// also come from @aqiservice.com and may carry an (invoice) PDF attachment.
function looksLikeQcThread(subject) {
    const s = String(subject || '');
    return /\bqc\b|report/i.test(s) && !/invoice/i.test(s);
}

function isPdfAttachment(a) {
    return a && (a.content_type === 'application/pdf' || /\.pdf$/i.test(a.filename || ''));
}

// Pull the URL that follows the "Report:" label, scoped to before the separate
// "Photos link:" so we never mistake the photos folder for the report.
function extractReportLink(text) {
    const t = String(text || '');
    const rep = t.match(/Report\s*[:：]/i);
    if (!rep) return null;
    let slice = t.slice(rep.index + rep[0].length);
    const photos = slice.match(/Photos?\s*link\s*[:：]/i);
    if (photos) slice = slice.slice(0, photos.index);
    const url = slice.match(/https?:\/\/[^\s"'<>)]+/);
    return url ? url[0] : null;
}

// ── jianguoyun (坚果云 / Nutstore) public share ────────────────────────────────
function jgyHash(shareUrl) {
    const m = String(shareUrl).match(/jianguoyun\.com\/p\/([A-Za-z0-9_-]+)/);
    return m ? m[1] : null;
}

async function httpGetText(url) {
    const resp = await fetch(url, { headers: { 'User-Agent': BROWSER_UA } });
    if (!resp.ok) throw new Error(`GET ${url} -> ${resp.status}`);
    return resp.text();
}

async function jgyAjaxJson(url, refererHash) {
    const resp = await fetch(url, {
        headers: {
            'User-Agent': BROWSER_UA,
            'X-Requested-With': 'XMLHttpRequest',
            Referer: `https://www.jianguoyun.com/p/${refererHash}`,
        },
    });
    const body = await resp.text();
    let parsed;
    try { parsed = JSON.parse(body); } catch { throw new Error(`jianguoyun non-JSON from ${url}: ${body.slice(0, 160)}`); }
    if (parsed && parsed.errorCode) {
        throw new Error(`jianguoyun ${parsed.errorCode}: ${parsed.detailMsg || ''}`.trim());
    }
    return parsed;
}

// The /p/{hash} page embeds `var PageInfo = { … }`. We only need a few flags;
// pull them with targeted regexes rather than eval'ing supplier-controlled HTML.
async function jgyPageInfo(hash) {
    const html = await httpGetText(`https://www.jianguoyun.com/p/${hash}`);
    const str = k => { const m = html.match(new RegExp(k + "\\s*:\\s*'((?:[^'\\\\]|\\\\.)*)'")); return m ? m[1] : null; };
    const bool = k => { const m = html.match(new RegExp(k + '\\s*:\\s*(true|false)')); return m ? m[1] === 'true' : null; };
    return {
        name: str('name'),
        isdir: bool('isdir'),
        needsPassword: bool('needsPassword'),
        isrevoked: bool('isrevoked'),
        isdeleted: bool('isdeleted'),
        canNotAccess: bool('canNotAccess'),
    };
}

// Guard against a folder that bundles videos/photos: buffering hundreds of MB
// (then inflating) OOM-kills the Lambda/process, which can't be caught. Real
// report zips run ~50 MB, so cap well above that and fail loudly instead.
const MAX_DOWNLOAD_BYTES = 150 * 1024 * 1024;
async function httpGetBuffer(url) {
    const resp = await fetch(url, { headers: { 'User-Agent': BROWSER_UA } });
    if (!resp.ok) throw new Error(`GET ${url} -> ${resp.status}`);
    const declared = Number(resp.headers.get('content-length') || 0);
    if (declared > MAX_DOWNLOAD_BYTES) {
        throw new Error(`download too large (${(declared / 1e6).toFixed(0)} MB > ${(MAX_DOWNLOAD_BYTES / 1e6) | 0} MB cap)`);
    }
    const buf = Buffer.from(await resp.arrayBuffer());
    if (buf.length > MAX_DOWNLOAD_BYTES) {
        throw new Error(`download too large (${(buf.length / 1e6).toFixed(0)} MB)`);
    }
    return buf;
}

// Minimal ZIP reader (no dependency): walk the central directory for accurate
// sizes — jianguoyun streams the zip with data descriptors, so the per-file
// LOCAL headers carry 0 sizes and can't be trusted. Returns the first PDF entry.
function extractFirstPdfFromZip(buf) {
    const EOCD_SIG = 0x06054b50, CEN_SIG = 0x02014b50, LOC_SIG = 0x04034b50;
    let eocd = -1;
    for (let i = buf.length - 22; i >= 0 && i >= buf.length - 22 - 65535; i--) {
        if (buf.readUInt32LE(i) === EOCD_SIG) { eocd = i; break; }
    }
    if (eocd < 0) throw new Error('not a zip (no EOCD record)');
    const count = buf.readUInt16LE(eocd + 10);
    let p = buf.readUInt32LE(eocd + 16);
    const extras = [];
    for (let i = 0; i < count; i++) {
        if (buf.readUInt32LE(p) !== CEN_SIG) break;
        const method = buf.readUInt16LE(p + 10);
        const compSize = buf.readUInt32LE(p + 20);
        const uncompSize = buf.readUInt32LE(p + 24);
        const nameLen = buf.readUInt16LE(p + 28);
        const extraLen = buf.readUInt16LE(p + 30);
        const commentLen = buf.readUInt16LE(p + 32);
        const localOff = buf.readUInt32LE(p + 42);
        const name = buf.slice(p + 46, p + 46 + nameLen).toString('utf8');
        p += 46 + nameLen + extraLen + commentLen;
        if (!/\.pdf$/i.test(name)) continue;
        if (extras.length) { extras.push(name); continue; }

        if (buf.readUInt32LE(localOff) !== LOC_SIG) throw new Error('bad local header in zip');
        const lNameLen = buf.readUInt16LE(localOff + 26);
        const lExtraLen = buf.readUInt16LE(localOff + 28);
        const dataStart = localOff + 30 + lNameLen + lExtraLen;
        const comp = buf.slice(dataStart, dataStart + compSize);
        let bytes;
        if (method === 0) bytes = comp;
        else if (method === 8) bytes = zlib.inflateRawSync(comp);
        else throw new Error(`unsupported zip compression method ${method}`);
        if (uncompSize && bytes.length !== uncompSize) {
            throw new Error(`zip size mismatch for ${name}: got ${bytes.length} expected ${uncompSize}`);
        }
        extras.push(name); // mark that we took one
        return { name: name.split('/').pop(), bytes, others: extras.slice(1) };
    }
    return null;
}

// Resolves a jianguoyun "Report:" share link to the report PDF bytes. Handles a
// shared folder (the common case — zip it and pull the PDF) and a directly
// shared single PDF.
async function fetchJianguoyunReport(shareUrl) {
    const hash = jgyHash(shareUrl);
    if (!hash) throw new Error(`not a jianguoyun share URL: ${shareUrl}`);
    const info = await jgyPageInfo(hash);
    if (info.isrevoked || info.isdeleted || info.canNotAccess) throw new Error('jianguoyun share is revoked/deleted/inaccessible');
    if (info.needsPassword) throw new Error('jianguoyun share is password-protected');

    if (info.isdir) {
        const link = await jgyAjaxJson(
            `https://www.jianguoyun.com/d/ajax/dirops/pubDIRLink?k=${hash}&dn=${encodeURIComponent(info.name || 'Report')}`,
            hash
        );
        if (!link.url) throw new Error('jianguoyun pubDIRLink returned no url');
        const zip = await httpGetBuffer(link.url);
        const pdf = extractFirstPdfFromZip(zip);
        if (!pdf) throw new Error('no PDF found inside jianguoyun folder');
        if (pdf.others && pdf.others.length) {
            log.warn('[front-qc-import] jianguoyun folder had multiple PDFs; took first', { hash, took: pdf.name, ignored: pdf.others });
        }
        return { bytes: pdf.bytes, filename: pdf.name };
    }

    // Single shared file.
    const link = await jgyAjaxJson(
        `https://www.jianguoyun.com/d/ajax/fileops/pubFileLink?k=${hash}&name=${encodeURIComponent(info.name || 'Report.pdf')}`,
        hash
    );
    if (!link.url) throw new Error('jianguoyun pubFileLink returned no url');
    const bytes = await httpGetBuffer(link.url);
    const filename = info.name && /\.pdf$/i.test(info.name) ? info.name : `${info.name || 'Report'}.pdf`;
    return { bytes, filename };
}

// ── Discovery ─────────────────────────────────────────────────────────────────
// Reduce a qualifying inbound supplier message to the report source(s) it
// carries. Front PDF attachment(s) win; otherwise the "Report:" link in the body.
function reportSourcesFromMessage(m) {
    const sources = [];
    const pdfs = (m.attachments || []).filter(isPdfAttachment);
    if (pdfs.length) {
        for (const a of pdfs) {
            sources.push({
                kind: 'front-attachment',
                sourceKey: `front:${a.id}`,
                filename: a.filename || `${a.id}.pdf`,
                fetch: async () => ({ bytes: await httpGetBufferFront(a.url), filename: a.filename || `${a.id}.pdf` }),
            });
        }
        return sources;
    }
    const link = extractReportLink(messageText(m));
    if (link && jgyHash(link)) {
        const hash = jgyHash(link);
        sources.push({
            kind: 'jianguoyun',
            sourceKey: `jianguoyun:${hash}`,
            filename: null,
            link,
            fetch: async () => fetchJianguoyunReport(link),
        });
    }
    return sources;
}

async function httpGetBufferFront(url) {
    const resp = await fetch(url, { headers: frontHeaders() });
    if (!resp.ok) throw new Error(`Front attachment download ${url} -> ${resp.status}`);
    return Buffer.from(await resp.arrayBuffer());
}

// ── Schema (additive, idempotent) ─────────────────────────────────────────────
// qc_reports already exists (created by orders.js). We only add origin columns.
async function ensureSchema(conn) {
    const addCol = async (sql) => {
        try { await conn.query(sql); }
        catch (e) { if (e && (e.errno === 1060 || e.errno === 1061)) return; throw e; } // dup column / dup key
    };
    await addCol(`ALTER TABLE qc_reports ADD COLUMN source VARCHAR(32) NULL`);
    await addCol(`ALTER TABLE qc_reports ADD COLUMN source_ref VARCHAR(255) NULL`);
    await addCol(`ALTER TABLE qc_reports ADD COLUMN source_meta JSON NULL`);
    await addCol(`ALTER TABLE qc_reports ADD COLUMN source_received_at DATETIME NULL`);
    await addCol(`ALTER TABLE qc_reports ADD UNIQUE KEY uk_source_ref (source_ref)`);
}

// Hard-deletes everything THIS importer previously created (rows + their order
// links) so a `--reset` re-fetch starts clean. A plain soft-delete wouldn't do:
// the row lingers, so the UNIQUE(source_ref) + the dedup lookup would make the
// re-import skip it. Only ever touches source='front-%' rows — never manual
// uploads.
async function resetFrontImports(conn) {
    await ensureSchema(conn);
    const [rows] = await conn.query(`SELECT id FROM qc_reports WHERE source LIKE 'front-%'`);
    const ids = rows.map(r => r.id);
    if (!ids.length) return { deleted: 0, ids: [] };
    await conn.query(`DELETE FROM order_qc_reports WHERE qc_report_id IN (?)`, [ids]);
    await conn.query(`DELETE FROM qc_reports WHERE id IN (?)`, [ids]);
    return { deleted: ids.length, ids };
}

// ── Import ────────────────────────────────────────────────────────────────────
// Searches Front for supplier QC emails in the last `sinceDays`, fetches each
// report PDF not already imported, stores it in S3 and inserts a qc_reports row.
// Does NOT analyze (call analyzeAndPersist on each returned row for that).
async function importQcReportsFromFront(conn, { sinceDays = 14, dryRun = false } = {}) {
    if (!PO_BUCKET) {
        const e = new Error('PO_DOCS_BUCKET is not configured.');
        e.code = 'NOT_CONFIGURED';
        throw e;
    }
    await ensureSchema(conn);

    const cutoffMs = Date.now() - sinceDays * 86400000;
    const query = `from:${SUPPLIER_DOMAIN} after:${Math.floor(cutoffMs / 1000)}`;

    const results = { query, sinceDays, found: [], imported: [], importedRows: [], skipped: [], failed: [] };
    const seen = new Set();

    for await (const convo of searchConversations(query)) {
        if (!looksLikeQcThread(convo.subject)) {
            results.skipped.push({ conversationId: convo.id, subject: convo.subject, reason: 'not-a-qc-thread' });
            continue;
        }
        const messages = await listMessages(convo.id);
        for (const m of messages) {
            if (!m.is_inbound) continue;
            const from = messageFromEmail(m);
            if (!emailInDomain(from, SUPPLIER_DOMAIN)) continue;
            if ((m.created_at || 0) * 1000 < cutoffMs) continue;

            for (const src of reportSourcesFromMessage(m)) {
                if (seen.has(src.sourceKey)) continue;
                seen.add(src.sourceKey);

                const subjectClean = cleanSubject(convo.subject);
                const receivedAt = m.created_at ? new Date(m.created_at * 1000) : null;
                const meta = {
                    conversationId: convo.id, messageId: m.id, subject: convo.subject,
                    from, date: new Date((m.created_at || 0) * 1000).toISOString(),
                    kind: src.kind, sourceKey: src.sourceKey, link: src.link || null,
                };
                results.found.push(meta);

                // Provenance stored on the report so the email it came from
                // (who/what/when) shows alongside it — the reports are otherwise
                // indistinguishable (Gemini titles them all "Pre-shipment …").
                const sourceMeta = {
                    from, subject: convo.subject || null, subjectClean,
                    receivedAt: meta.date, conversationId: convo.id, messageId: m.id,
                    frontUrl: frontWebUrl(convo.id), link: src.link || null,
                    body: messageBodySnippet(m),
                };
                const metaJson = JSON.stringify(sourceMeta);

                const [existing] = await conn.query(
                    `SELECT id, status FROM qc_reports WHERE source_ref = ? OR source_ref LIKE ? LIMIT 1`,
                    [src.sourceKey, src.sourceKey + '#%']
                );
                if (existing.length) {
                    // Re-run enriches rows we previously imported (backfill the
                    // email content + subject-title) without re-fetching.
                    if (!dryRun) {
                        await conn.query(
                            `UPDATE qc_reports
                                SET source_meta = ?, source_received_at = ?, report_title = COALESCE(NULLIF(?, ''), report_title)
                              WHERE id = ? AND source LIKE 'front-%'`,
                            [metaJson, receivedAt, subjectClean, existing[0].id]
                        );
                    }
                    results.skipped.push({ ...meta, reason: 'already-imported', existingId: existing[0].id, status: existing[0].status });
                    continue;
                }
                if (dryRun) { results.imported.push({ ...meta, dryRun: true }); continue; }

                log.info('[front-qc-import] fetching report', { kind: src.kind, subject: meta.subject });
                try {
                    const { bytes, filename } = await src.fetch();
                    if (!bytes || !bytes.length) throw new Error('empty file');
                    const safe = (filename || 'report.pdf').replace(SAFE_FILENAME_RE, '_').slice(0, 200);
                    const token = uuidv4();
                    const s3Key = `qc-reports/${token}/${safe}`;
                    await s3.send(new PutObjectCommand({
                        Bucket: PO_BUCKET, Key: s3Key, Body: bytes, ContentType: 'application/pdf',
                    }));
                    const [ins] = await conn.query(
                        `INSERT INTO qc_reports
                            (filename, s3_key, public_url, content_type, file_size, status, source, source_ref, report_title, source_meta, source_received_at, uploaded_by_email)
                         VALUES (?, ?, ?, ?, ?, 'uploaded', ?, ?, ?, ?, ?, ?)`,
                        [safe, s3Key, publicS3Url(s3Key), 'application/pdf', bytes.length,
                         src.kind === 'jianguoyun' ? 'front-jianguoyun' : 'front-attachment',
                         src.sourceKey, subjectClean || null, metaJson, receivedAt, `front-import:${SUPPLIER_DOMAIN}`]
                    );
                    const [rows] = await conn.query(`SELECT * FROM qc_reports WHERE id = ?`, [ins.insertId]);
                    results.imported.push({ ...meta, id: ins.insertId, filename: safe, fileSize: bytes.length, s3Key });
                    results.importedRows.push(rows[0]);
                } catch (e) {
                    results.failed.push({ ...meta, error: e.message });
                    log.error('[front-qc-import] fetch/store failed', { sourceKey: src.sourceKey, error: e.message });
                }
            }
        }
    }
    return results;
}

// ── Analyze + persist ─────────────────────────────────────────────────────────
// Runs the existing Gemini extraction + order matching for one report row and
// writes the order_qc_reports links + report summary. Mirrors the background
// runner in src/handlers/orders.js (POST /qc-reports/:id/analyze).
// A useful, collision-free report name: the email subject (e.g. "Sunmed QC
// Documents (Report)") + the inspection date Gemini extracted — because the same
// product line gets inspected repeatedly (two "Sunmed" reports in one fortnight).
// Rebuilt from the stable subjectClean in source_meta every analysis, so it's
// idempotent and never reverts to Gemini's generic "Pre-shipment Inspection
// Report". Manual uploads (no source_meta) keep Gemini's title.
function reportDisplayTitle(reportRow, out) {
    let meta = reportRow.source_meta;
    if (meta && typeof meta === 'string') { try { meta = JSON.parse(meta); } catch { meta = null; } }
    const base = meta && meta.subjectClean ? String(meta.subjectClean).trim() : null;
    if (!base) return out.report.title || null;
    const date = out.report.inspectionDate ? String(out.report.inspectionDate).trim() : null;
    return (date ? `${base} — ${date}` : base).slice(0, 255);
}

async function analyzeAndPersist(conn, reportRow, { model, userEmail = 'front-qc-import' } = {}) {
    await conn.query(
        `UPDATE qc_reports SET status = 'processing', error_message = NULL, analyzed_at = NULL WHERE id = ?`,
        [reportRow.id]
    );
    try {
        const out = await analyzeQcReport(conn, {
            s3Key: reportRow.s3_key, contentType: reportRow.content_type, model,
        });

        await conn.query(`DELETE FROM order_qc_reports WHERE qc_report_id = ?`, [reportRow.id]);
        for (const mItem of out.matched) {
            await conn.query(
                `INSERT INTO order_qc_reports
                    (qc_report_id, order_id, jf_code, lot_number, qc_result, result_detail, match_method)
                 VALUES (?, ?, ?, ?, ?, ?, ?)
                 ON DUPLICATE KEY UPDATE
                    jf_code = VALUES(jf_code), lot_number = VALUES(lot_number),
                    qc_result = VALUES(qc_result), result_detail = VALUES(result_detail),
                    match_method = VALUES(match_method)`,
                [reportRow.id, mItem.orderId, mItem.jfCode, mItem.lotNumber, mItem.qcResult,
                 mItem.resultDetail ? String(mItem.resultDetail).slice(0, 1000) : null, mItem.matchMethod]
            );
        }

        await conn.query(
            `UPDATE qc_reports
                SET status = 'succeeded', supplier = ?, report_title = COALESCE(NULLIF(?, ''), ?), inspection_date = ?,
                    model_used = ?, item_count = ?, matched_count = ?, file_size = ?,
                    result_json = ?, error_message = NULL, analyzed_at = NOW()
              WHERE id = ?`,
            [out.report.supplier, reportDisplayTitle(reportRow, out), out.report.title, out.report.inspectionDate,
             out.modelUsed, out.items.length, out.matchedItemCount, out.fileSize,
             JSON.stringify({ matched: out.matched, unmatched: out.unmatched, items: out.items }),
             reportRow.id]
        );

        await conn.query(
            `INSERT INTO audit_log (entity_type, entity_id, action, before_json, after_json, user_email)
             VALUES ('qc_report', ?, 'qc_report_analyzed', NULL, ?, ?)`,
            [reportRow.id,
             JSON.stringify({ itemCount: out.items.length, matchedOrders: out.matched.length, unmatched: out.unmatched.length, modelUsed: out.modelUsed, source: 'front-import' }),
             userEmail]
        );
        return out;
    } catch (e) {
        await conn.query(
            `UPDATE qc_reports SET status = 'failed', error_message = ? WHERE id = ?`,
            [String(e.message).slice(0, 4000), reportRow.id]
        ).catch(() => {});
        throw e;
    }
}

module.exports = {
    importQcReportsFromFront,
    analyzeAndPersist,
    resetFrontImports,
    // exported for unit-poking / reuse
    extractReportLink,
    extractFirstPdfFromZip,
    fetchJianguoyunReport,
    reportSourcesFromMessage,
    looksLikeQcThread,
};
