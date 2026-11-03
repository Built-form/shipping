'use strict';

// /api/v1/qc-visits/:id/schedule-link — the inspector's no-login schedule
// page link (docs/qc-schedule-link.md), and the SQL shared with the email
// route (qc-visit-comms-routes.js), the visit detail's "changed since the
// last email" (qc-visit-routes.js) and the public page
// (src/handlers/qc-schedule.js). The pure rules are in src/lib/qc-schedule.js.
//
// One live link per (visit, audience): created on first use (the first email
// with "Include schedule link", or POST here) and then reused by every later
// email, so the inspector keeps one URL for the life of the visit. Rotating
// revokes it ('rotated') and issues a new one; the old URL then shows "no
// longer active". Creation is serialised per visit by the qc_visits row lock.
// The token is never written to the audit log.
//
// Registered from orders.js beside the other QC route modules; authed through
// the greedy /api/v1/{proxy+} route.

const S = require('../lib/qc-schedule');
const QV = require('../lib/qc-visits');
const { apiBaseUrlFromReq } = require('../lib/email-receipt');

const SEND_KINDS_WITH_LINES = ['proposal', 'update'];

async function inTx(conn, fn) {
    await conn.beginTransaction();
    try {
        const out = await fn();
        await conn.commit();
        return out;
    } catch (e) {
        try { await conn.rollback(); } catch { /* the original error matters */ }
        throw e;
    }
}

const iso = v => {
    if (v == null) return null;
    if (v instanceof Date) return Number.isNaN(v.getTime()) ? null : v.toISOString();
    const s = String(v);
    const t = Date.parse(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}/.test(s) ? `${s.replace(' ', 'T')}Z` : s);
    return Number.isNaN(t) ? null : new Date(t).toISOString();
};

async function liveLinkOf(conn, visitId, audience) {
    const [rows] = await conn.query(
        `SELECT * FROM qc_visit_links WHERE visit_id = ? AND audience = ? AND revoked_at IS NULL
          ORDER BY id DESC LIMIT 1`,
        [visitId, audience]
    );
    return rows[0] || null;
}

async function insertLink(conn, visitId, audience, userEmail) {
    // 32 random bytes: a clash is not expected; retried anyway, as availability does.
    for (let attempt = 1; ; attempt++) {
        try {
            const [ins] = await conn.query(
                `INSERT INTO qc_visit_links (visit_id, audience, token, created_by_email) VALUES (?, ?, ?, ?)`,
                [visitId, audience, S.newToken(), userEmail || null]
            );
            const [rows] = await conn.query(`SELECT * FROM qc_visit_links WHERE id = ?`, [ins.insertId]);
            return rows[0];
        } catch (err) {
            if (err && err.code === 'ER_DUP_ENTRY' && attempt < 5) continue;
            throw err;
        }
    }
}

/**
 * The visit's live link for `audience`, or a new one (own transaction, the
 * visit row locked so two sends cannot both create one).
 * → { row, created } — or null when the visit does not exist.
 */
async function getOrCreateLink(conn, visitId, audience, { userEmail = null, recordAudit = null, via = null } = {}) {
    return inTx(conn, async () => {
        const [v] = await conn.query(`SELECT id FROM qc_visits WHERE id = ? FOR UPDATE`, [visitId]);
        if (!v.length) return null;
        const live = await liveLinkOf(conn, visitId, audience);
        if (live) return { row: live, created: false };
        const row = await insertLink(conn, visitId, audience, userEmail);
        if (recordAudit) {
            await recordAudit(conn, {
                entityType: 'qc_visit', entityId: Number(visitId), action: 'schedule_link_created',
                before: null, after: { linkId: Number(row.id), audience, ...(via ? { via } : {}) }, userEmail,
            });
        }
        return { row, created: true };
    });
}

// Revoke the live link ('rotated') and issue a new one. → the new row, or null (no visit).
async function rotateLink(conn, visitId, audience, { userEmail = null, recordAudit = null } = {}) {
    return inTx(conn, async () => {
        const [v] = await conn.query(`SELECT id FROM qc_visits WHERE id = ? FOR UPDATE`, [visitId]);
        if (!v.length) return null;
        const [old] = await conn.query(
            `SELECT id FROM qc_visit_links WHERE visit_id = ? AND audience = ? AND revoked_at IS NULL`,
            [visitId, audience]
        );
        const oldIds = old.map(r => Number(r.id));
        if (oldIds.length) {
            await conn.query(
                `UPDATE qc_visit_links SET revoked_at = CURRENT_TIMESTAMP, revoke_reason = 'rotated', revoked_by_email = ?
                  WHERE id IN (?) AND revoked_at IS NULL`,
                [userEmail || null, oldIds]
            );
        }
        const row = await insertLink(conn, visitId, audience, userEmail);
        if (recordAudit) {
            await recordAudit(conn, {
                entityType: 'qc_visit', entityId: Number(visitId), action: 'schedule_link_rotated',
                before: oldIds.length ? { linkIds: oldIds } : null, after: { linkId: Number(row.id), audience }, userEmail,
            });
        }
        return row;
    });
}

async function findLinkByToken(conn, token) {
    if (!S.isToken(token)) return null;
    const [rows] = await conn.query(`SELECT * FROM qc_visit_links WHERE token = ? LIMIT 1`, [token]);
    return rows[0] || null;
}

async function recordOpen(conn, linkId) {
    await conn.query(
        `UPDATE qc_visit_links
            SET open_count = open_count + 1, last_opened_at = CURRENT_TIMESTAMP,
                first_opened_at = COALESCE(first_opened_at, CURRENT_TIMESTAMP)
          WHERE id = ?`,
        [linkId]
    );
}

/**
 * The newest proposal / update email per audience that recorded its lines.
 * → { factory: row | null, inspector: row | null }. A party whose newest such
 * email predates snapshots (lines_snapshot_json NULL) gets that row anyway,
 * so callers can tell "never emailed" from "emailed, but nothing to compare".
 */
async function lastLineSends(conn, visitId) {
    const [rows] = await conn.query(
        `SELECT id, audience, kind, sent_at, lines_snapshot_json
           FROM qc_visit_sends
          WHERE visit_id = ? AND kind IN (?)
          ORDER BY sent_at DESC, id DESC`,
        [visitId, SEND_KINDS_WITH_LINES]
    );
    const out = { factory: null, inspector: null };
    for (const r of rows) if (r.audience in out && !out[r.audience]) out[r.audience] = r;
    return out;
}

/**
 * Per party, what changed in the visit's lines since the last email that
 * listed them. `lineRows`: the visit's current qc_visit_lines joined to orders.
 * → { factory: Changes | null, inspector: Changes | null } where null means
 * never emailed and Changes is
 *   { sendId, sentAt, kind, comparable, added, removed, changed, count }
 * comparable false: that email predates snapshots, so nothing is listed.
 */
async function sendChanges(conn, visitId, lineRows) {
    const last = await lastLineSends(conn, visitId);
    const now = S.linesSnapshot(lineRows);
    const out = {};
    for (const p of ['factory', 'inspector']) {
        const r = last[p];
        if (!r) { out[p] = null; continue; }
        const before = S.parseSnapshot(r.lines_snapshot_json);
        const base = { sendId: Number(r.id), sentAt: iso(r.sent_at), kind: r.kind };
        out[p] = before
            ? { ...base, comparable: true, ...S.diffLines(before, now) }
            : { ...base, comparable: false, added: [], removed: [], changed: [], count: 0 };
    }
    return out;
}

function linkToJson(row, base) {
    if (!row) return null;
    return {
        id: Number(row.id),
        audience: row.audience,
        url: row.revoked_at ? null : S.scheduleUrl(base, row.token),
        createdByEmail: row.created_by_email || null,
        createdAt: iso(row.created_at),
        firstOpenedAt: iso(row.first_opened_at),
        lastOpenedAt: iso(row.last_opened_at),
        openCount: Number(row.open_count) || 0,
    };
}

function registerQcScheduleRoutes(app, deps) {
    const { withConnection, recordAudit, auditLogSchemaReady, log } = deps;

    const fail = (res, status, error, code, payload) => res.status(status).json({ error, code, ...(payload || {}) });
    const audienceOf = v => (v == null || v === '' ? 'inspector' : String(v));

    // GET /api/v1/qc-visits/:id/schedule-link[?audience=inspector]
    // → { link: Link | null }. Creates nothing.
    app.get('/api/v1/qc-visits/:id(\\d+)/schedule-link', async (req, res) => {
        try {
            const id = Number(req.params.id);
            const audience = audienceOf(req.query.audience);
            if (!S.AUDIENCES.includes(audience)) return fail(res, 400, 'audience must be inspector.', 'BAD_FIELD', { field: 'audience' });
            const out = await withConnection(async (conn) => {
                const [v] = await conn.query(`SELECT id FROM qc_visits WHERE id = ?`, [id]);
                if (!v.length) return null;
                return { link: linkToJson(await liveLinkOf(conn, id, audience), apiBaseUrlFromReq(req)) };
            });
            if (!out) return fail(res, 404, `QC visit ${id} not found.`, 'NOT_FOUND');
            res.json(out);
        } catch (error) {
            log.error('[GET /qc-visits/:id/schedule-link]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // POST /api/v1/qc-visits/:id/schedule-link  { audience?, rotate? }
    // Without rotate: the live link, or a new one (201). With rotate: true the
    // live link stops working and a new one is issued (201); email the
    // inspector again so they have it.
    app.post('/api/v1/qc-visits/:id(\\d+)/schedule-link', async (req, res) => {
        try {
            if (!QV.canEdit(req.userType)) return fail(res, 403, 'Read only.', 'READ_ONLY');
            await auditLogSchemaReady;
            const id = Number(req.params.id);
            const b = req.body || {};
            const audience = audienceOf(b.audience);
            if (!S.AUDIENCES.includes(audience)) return fail(res, 400, 'audience must be inspector.', 'BAD_FIELD', { field: 'audience' });
            const userEmail = req.userEmail || null;
            const out = await withConnection(async (conn) => {
                if (b.rotate === true) {
                    const row = await rotateLink(conn, id, audience, { userEmail, recordAudit });
                    return row ? { row, created: true } : null;
                }
                return getOrCreateLink(conn, id, audience, { userEmail, recordAudit, via: 'staff' });
            });
            if (!out) return fail(res, 404, `QC visit ${id} not found.`, 'NOT_FOUND');
            res.status(out.created ? 201 : 200).json({ link: linkToJson(out.row, apiBaseUrlFromReq(req)) });
        } catch (error) {
            log.error('[POST /qc-visits/:id/schedule-link]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });
}

module.exports = {
    registerQcScheduleRoutes,
    liveLinkOf,
    getOrCreateLink,
    rotateLink,
    findLinkByToken,
    recordOpen,
    lastLineSends,
    sendChanges,
    linkToJson,
};
