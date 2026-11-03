// ── QC visit schedule page — PUBLIC API ─────────────────────────────────────
// The no-login page behind the link in the inspector's QC visit email
// (docs/qc-schedule-link.md): every product to inspect, grouped by JF code,
// with its files (the JFPRO kit documents of the code and the visit's uploads
// for that product), the visit's own files, and what changed since the last
// email to the inspector. Always the visit as it is now.
//
// A tiny isolated Lambda like email-receipt.js: no JWT authorizer, the public
// surface is the two routes below, none of the authed routes are reachable.
// The 43-character token in the path IS the capability; an unknown or
// malformed token reads exactly like a missing one. Files are never linked
// directly: each click goes through /files/<id>, which checks the file still
// belongs to the visit and answers a 302 to a fresh 5-minute presigned URL, so
// a link copied from the page dies with the schedule link.
//
// Routes (unauthenticated — throttled per route in serverless.yml):
//   GET /api/v1/qc-schedule/{token}               → the page (HTML)
//   GET /api/v1/qc-schedule/{token}/files/{file}  → 302 to the file
//        file: k<versionId> (JFPRO kit document version), a<attachmentId>
//              (visit upload), products.csv (the products list)
const serverless = require('serverless-http');
require('dotenv').config();
const express = require('express');
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const { getPool } = require('../db');
const log = require('../lib/logger');
const S = require('../lib/qc-schedule');
const QV = require('../lib/qc-visits');
const KD = require('../lib/qc-kit-docs');
const { viewResponseParams } = require('../lib/qc-file-types');
const { findLinkByToken, recordOpen, lastLineSends } = require('../services/qc-schedule-routes');
const { buildVars, linesFor, linesCsv } = require('../services/qc-visit-comms-routes');

const app = express();
const pool = getPool();
const s3 = new S3Client({ region: process.env.AWS_REGION || 'eu-north-1' });
const PO_BUCKET = process.env.PO_DOCS_BUCKET;
const JFPRO_BUCKET = process.env.JFPRO_DOCS_BUCKET;
const FILE_URL_SECONDS = 300;

async function withConnection(fn) {
    const conn = await pool.getConnection();
    try {
        return await fn(conn);
    } finally {
        conn.release();
    }
}

// Every response: never cached, never indexed, and the token never leaks in a
// Referer to the S3 host or anywhere else.
app.use((req, res, next) => {
    res.set('Cache-Control', 'no-store');
    res.set('X-Robots-Tag', 'noindex, nofollow');
    res.set('Referrer-Policy', 'no-referrer');
    res.set('X-Content-Type-Options', 'nosniff');
    res.set('Content-Security-Policy', "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'");
    next();
});

const html = (res, status, body) => res.status(status).type('html').send(body);
const notRecognised = res => html(res, 404, S.renderNoticePage('Link not recognised', 'This link is not recognised. Please use the link in our latest email.'));

const iso = v => {
    if (v == null) return null;
    if (v instanceof Date) return Number.isNaN(v.getTime()) ? null : v.toISOString();
    const t = Date.parse(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}/.test(String(v)) ? `${String(v).replace(' ', 'T')}Z` : String(v));
    return Number.isNaN(t) ? null : new Date(t).toISOString();
};
const n = v => (v == null || v === '' ? null : Number(v));

/**
 * The link, its visit and the visit's lines, or { status, page } to answer
 * instead: unknown token 404, rotated link 410.
 */
async function loadContext(conn, token) {
    const link = await findLinkByToken(conn, token);
    if (!link) return { status: 404 };
    if (link.revoked_at) {
        return { status: 410, page: S.renderNoticePage('Link no longer active', 'This link has been replaced. Please use the link in our latest email.') };
    }
    const [vr] = await conn.query(`SELECT * FROM qc_visits WHERE id = ?`, [link.visit_id]);
    if (!vr.length) return { status: 404 };
    const [lines] = await conn.query(
        `SELECT l.id, l.order_id, l.quantity_to_inspect, l.sample_method, l.note,
                l.aql_level, l.aql_critical, l.aql_major, l.aql_minor,
                o.jf_code, o.product_name, o.po_number, o.lot_number, o.supplier
           FROM qc_visit_lines l
           LEFT JOIN orders o ON o.id = l.order_id
          WHERE l.visit_id = ?
          ORDER BY o.jf_code, o.po_number, l.id`,
        [link.visit_id]
    );
    const codes = [...new Set(lines.map(l => QV.normJfCode(l.jf_code || '')).filter(Boolean))];
    return { link, visit: vr[0], lines, codes };
}

async function kitRows(conn, codes) {
    if (!codes.length) return [];
    const [rows] = await conn.query(KD.KIT_DOCUMENTS_SQL, [codes]);
    return rows;
}

// ── GET /api/v1/qc-schedule/:token ──────────────────────────────────────────
app.get('/api/v1/qc-schedule/:token', async (req, res) => {
    const token = req.params.token;
    if (!S.isToken(token)) return notRecognised(res);
    try {
        const page = await withConnection(async (conn) => {
            const ctx = await loadContext(conn, token);
            if (ctx.status) return ctx;
            const { link, visit, lines, codes } = ctx;
            try { await recordOpen(conn, link.id); } catch (e) { log.warn('[qc-schedule] open not recorded', { linkId: link.id, error: e.message }); }

            const ref = visit.ref || QV.formatRef(visit.id);
            if (visit.status === 'CANCELLED') {
                return { status: 200, page: S.renderNoticePage(`QC inspection ${ref} cancelled`, 'This inspection has been cancelled. Please disregard it and remove it from your calendar.') };
            }

            const { text } = buildVars(visit, linesFor(visit, lines, 'inspector'));
            const last = (await lastLineSends(conn, visit.id)).inspector;
            const before = last ? S.parseSnapshot(last.lines_snapshot_json) : null;
            const diff = before ? S.diffLines(before, S.linesSnapshot(lines)) : null;
            const added = new Set((diff ? diff.added : []).map(l => l.orderId));
            const changedBy = new Map((diff ? diff.changed : []).map(l => [l.orderId, l.before]));

            const kits = new Map(KD.kitsFromRows(codes, await kitRows(conn, codes)).map(k => [k.jfCode, k]));
            const [atts] = await conn.query(
                `SELECT id, jf_code, filename, file_size, note, created_at FROM qc_visit_attachments
                  WHERE visit_id = ? AND deleted_at IS NULL ORDER BY created_at, id`,
                [visit.id]
            );
            const href = id => `./${token}/files/${id}`;
            const upload = a => ({ name: a.filename, href: href(`a${a.id}`), kind: 'File', size: n(a.file_size), note: a.note || null, addedAt: iso(a.created_at) });

            // One product per JF code (_FQC sample lines join their parent's
            // product); a line without a code is a product of its own.
            const groups = new Map();
            for (const l of lines) {
                const code = QV.normJfCode(l.jf_code || '');
                const key = code || `#${l.order_id}`;
                if (!groups.has(key)) groups.set(key, { jfCode: code || null, productName: l.product_name || null, lines: [] });
                const orderId = Number(l.order_id);
                const was = changedBy.get(orderId);
                groups.get(key).lines.push({
                    poNumber: l.po_number || null,
                    lotNumber: l.lot_number || null,
                    quantityToInspect: Number(l.quantity_to_inspect) || 0,
                    sampleMethod: l.sample_method || 'full',
                    aql: { level: l.aql_level || null, critical: n(l.aql_critical), major: n(l.aql_major), minor: n(l.aql_minor) },
                    note: l.note || null,
                    change: added.has(orderId) ? 'new' : was ? 'changed' : null,
                    ...(was ? { before: was } : {}),
                });
            }
            const products = [...groups.values()].map((g) => {
                const kit = g.jfCode ? kits.get(g.jfCode) : null;
                const files = [
                    ...(kit ? kit.documents : []).map(d => ({
                        name: d.name, href: href(`k${d.versionId}`), kind: 'QC document', version: d.version, size: d.sizeBytes, addedAt: d.uploadedAt,
                    })),
                    ...atts.filter(a => g.jfCode && QV.normJfCode(a.jf_code || '') === g.jfCode).map(upload),
                ];
                const change = g.lines.every(l => l.change === 'new') ? 'new'
                    : g.lines.some(l => l.change) ? 'changed' : null;
                return { ...g, productName: g.productName || (kit && kit.productName) || null, files, change };
            });

            const statusNotice = visit.status === 'COMPLETED' ? 'This inspection is marked completed.'
                : visit.status === 'DRAFT' ? 'This visit is on hold and its details may change. We will email you when it is confirmed.'
                    : null;

            return {
                status: 200,
                page: S.renderSchedulePage({
                    ref, status: visit.status, statusNotice,
                    supplier: text.supplier, factory: text.factory, factoryAddress: text.factoryAddress,
                    factoryContact: text.factoryContact, visitDate: text.visitDate, window: text.window,
                    windowUk: text.windowUk, inspector: text.inspector + (text.inspectorCompany ? ` (${text.inspectorCompany})` : ''),
                    productionState: text.productionState, reportDeadline: text.reportDeadline, escalation: text.escalation,
                    lastEmailAt: last ? iso(last.sent_at) : null,
                    diff: diff && diff.count ? diff : null,
                    products,
                    // Uploads for the whole visit. One whose product has left the visit is not shown.
                    visitFiles: atts.filter(a => !a.jf_code).map(upload),
                    csvHref: href('products.csv'),
                }),
            };
        });
        if (page.status === 404) return notRecognised(res);
        return html(res, page.status, page.page);
    } catch (error) {
        log.error('[GET /qc-schedule/:token]', error);
        return html(res, 500, S.renderNoticePage('Something went wrong', 'The page could not be loaded. Please try again in a moment.'));
    }
});

// ── GET /api/v1/qc-schedule/:token/files/:file ──────────────────────────────
app.get('/api/v1/qc-schedule/:token/files/:file', async (req, res) => {
    const { token, file } = req.params;
    if (!S.isToken(token)) return notRecognised(res);
    try {
        const out = await withConnection(async (conn) => {
            const ctx = await loadContext(conn, token);
            if (ctx.status) return ctx;
            const { visit, lines, codes } = ctx;
            if (visit.status === 'CANCELLED') return { status: 404 };

            if (file === 'products.csv') {
                return { csv: linesCsv(linesFor(visit, lines, 'inspector')), ref: visit.ref || QV.formatRef(visit.id) };
            }
            let m = /^k(\d{1,10})$/.exec(file);
            if (m) {
                // A current kit document version of one of the visit's products.
                if (!codes.length || !JFPRO_BUCKET) return { status: 404 };
                const [rows] = await conn.query(
                    `SELECT s3_key, filename, mime_type FROM qc_kit_documents WHERE version_id = ? AND jf_code IN (?) LIMIT 1`,
                    [Number(m[1]), codes]
                );
                if (!rows.length || !rows[0].s3_key) return { status: 404 };
                return { bucket: JFPRO_BUCKET, key: rows[0].s3_key, name: rows[0].filename || 'document', type: rows[0].mime_type };
            }
            m = /^a(\d{1,10})$/.exec(file);
            if (m) {
                if (!PO_BUCKET) return { status: 404 };
                const [rows] = await conn.query(
                    `SELECT jf_code, s3_key, filename, content_type FROM qc_visit_attachments
                      WHERE id = ? AND visit_id = ? AND deleted_at IS NULL`,
                    [Number(m[1]), visit.id]
                );
                const a = rows[0];
                // A product file whose product has left the visit is gone with it.
                if (!a || (a.jf_code && !codes.includes(QV.normJfCode(a.jf_code)))) return { status: 404 };
                return { bucket: PO_BUCKET, key: a.s3_key, name: a.filename || 'file', type: a.content_type };
            }
            return { status: 404 };
        });
        if (out.status === 410) return html(res, 410, out.page);
        if (out.status) return html(res, 404, S.renderNoticePage('File not found', 'This file is no longer part of the inspection. Please reload the page.'));
        if (out.csv != null) {
            const safe = String(out.ref).replace(/[^A-Za-z0-9._-]+/g, '_');
            res.set('Content-Type', 'text/csv; charset=utf-8');
            res.set('Content-Disposition', `attachment; filename="${safe}-products.csv"`);
            return res.status(200).send(out.csv);
        }
        const url = await getSignedUrl(s3, new GetObjectCommand({
            Bucket: out.bucket, Key: out.key, ...viewResponseParams(out.name, out.type),
        }), { expiresIn: FILE_URL_SECONDS });
        return res.redirect(302, url);
    } catch (error) {
        log.error('[GET /qc-schedule/:token/files/:file]', error);
        return html(res, 500, S.renderNoticePage('Something went wrong', 'The file could not be opened. Please try again in a moment.'));
    }
});

app.use((req, res) => notRecognised(res));

// ── Serverless export ───────────────────────────────────────────────────────
const serverlessApp = serverless(app);

module.exports.handler = async (event, context) => {
    context.callbackWaitsForEmptyEventLoop = false;
    return await serverlessApp(event, context);
};

if (require.main === module) {
    const PORT = process.env.QC_SCHEDULE_PORT || 3004;
    app.listen(PORT, () => {
        log.info(`QC schedule page running on http://localhost:${PORT}`);
    });
}
