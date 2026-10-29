'use strict';

// /api/v1/qc-visits/:id/availability, /availability-links — the no-login
// availability links a QC visit's factory and inspector answer in the
// supplier portal (docs/qc-availability.md).
//
// The rows live in jportal's schema, <jportal>.qc_availability_requests
// (owned and migrated by jportal; JPORTAL_DB_SCHEMA, default `jportal`).
// Shipping's login needs SELECT, INSERT and the column-level
// UPDATE (revoked_at, revoke_reason, proposed_date, row_version) on it,
// granted by hand (docs/qc-availability.md). Shipping inserts a request,
// revokes it, and moves its proposed_date; it never writes the answer
// columns (available_days, none_available, answered_*, answer_count,
// answer_ip), which are jportal's. Every shipping UPDATE bumps row_version so
// the portal's optimistic answer write sees the change and re-reads.
//
// No locking read (SELECT … FOR UPDATE) on the jportal table: the column-level
// grant may not allow one. Updates are conditional instead
// (`WHERE … AND revoked_at IS NULL`). Creation is serialised per visit by the
// qc_visits row lock.
//
// An environment without the table (or without the grant) answers GET with
// `unavailable: true` and the writes with 503 PORTAL_UNAVAILABLE; the
// lifecycle hook (syncAvailabilityLinks) logs and carries on.
//
// The pure rules are in src/lib/qc-availability.js. Registered from
// orders.js beside the other QC route modules; authed through the greedy
// /api/v1/{proxy+} route. syncAvailabilityLinks is also called from the
// visit writes (qc-visit-routes.js) and the supplier portal's confirm
// (src/handlers/supplier-portal.js); createOrReuseLink / emailAvailabilityLink
// from the email route (qc-visit-comms-routes.js).

const A = require('../lib/qc-availability');
const { jportalSchema, isSourceMissing } = require('../lib/jportal');
const QV = require('../lib/qc-visits');
const defaultLog = require('../lib/logger');

const PORTAL_UNAVAILABLE_MESSAGE = 'The supplier portal is not available in this environment yet.';
const SAVEPOINT = 'qc_availability_sync';

// Read on every call: an invalid JPORTAL_DB_SCHEMA is a 500 on these routes,
// not a failed import that would take the whole Lambda down.
const table = () => `${jportalSchema()}.qc_availability_requests`;

// The portal's base URL (the link host), '' when unset.
const portalUrlOf = (env = process.env) => String((env && env.SUPPLIER_PORTAL_URL) || '').trim();
const availabilityEnabled = (env = process.env) => portalUrlOf(env) !== '';

class AvailabilityFail extends Error {
    constructor(status, code, error, payload) {
        super(error);
        this.status = status;
        this.code = code;
        this.error = error;
        this.payload = payload || null;
    }
}

const portalUnavailable = () => new AvailabilityFail(503, 'PORTAL_UNAVAILABLE', PORTAL_UNAVAILABLE_MESSAGE);

// Run fn against the jportal table; its absence becomes 503 PORTAL_UNAVAILABLE.
async function portalCall(fn) {
    try {
        return await fn();
    } catch (e) {
        if (isSourceMissing(e)) throw portalUnavailable();
        throw e;
    }
}

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

const clip = (v, n) => (typeof v === 'string' && v.trim() ? v.trim().slice(0, n) : null);
const failBody = f => ({ error: f.error, code: f.code, ...(f.payload || {}) });

async function loadLinkRows(conn, visitId) {
    const [rows] = await conn.query(
        `SELECT * FROM ${table()} WHERE visit_id = ? ORDER BY created_at DESC, id DESC`,
        [visitId]
    );
    return rows;
}

async function loadLinkRow(conn, id) {
    const [rows] = await conn.query(`SELECT * FROM ${table()} WHERE id = ?`, [id]);
    return rows[0] || null;
}

// The party's newest link that can still be answered (open or answered), or null.
async function liveLinkOf(conn, visitId, party, now) {
    const [rows] = await conn.query(
        `SELECT * FROM ${table()}
          WHERE visit_id = ? AND party = ? AND revoked_at IS NULL
          ORDER BY created_at DESC, id DESC`,
        [visitId, party]
    );
    return rows.find(r => A.isLive(r, now)) || null;
}

/**
 * A new availability request for one party of a visit, or (reuse: true) the
 * party's open / answered one when it has one (returned before the input is
 * validated: only a new link is checked against the window rules). Creating
 * revokes the party's
 * earlier unrevoked requests ('superseded') and inserts the new one.
 * Run it inside a transaction (the caller's): the revoke and the insert must
 * land together. Throws AvailabilityFail: 409 PORTAL_URL_UNSET / TERMINAL /
 * NO_DATE, 400 BAD_FIELD, 503 PORTAL_UNAVAILABLE.
 *   visit: the qc_visits row.
 *   opts.input: the POST body ({ windowStartDate?, windowEndDate?,
 *               recipientName?, recipientEmail?, preferredLang?, expiresInDays? }).
 * → { row, created, supersededIds }
 */
async function createOrReuseLink(conn, visit, party, opts = {}) {
    const {
        input = {}, reuse = false, userEmail = null, recordAudit = null,
        portalUrl = portalUrlOf(), today = QV.chinaToday(), now = new Date(), via = null,
    } = opts;
    if (!portalUrl) {
        throw new AvailabilityFail(409, 'PORTAL_URL_UNSET', 'The supplier portal address (SUPPLIER_PORTAL_URL) is not configured, so availability links cannot be made.');
    }
    if (QV.TERMINAL_STATUSES.includes(visit.status)) {
        throw new AvailabilityFail(409, 'TERMINAL', `This visit is ${String(visit.status).toLowerCase()}: it no longer needs availability.`, { status: visit.status });
    }
    if (!QV.ymdOf(visit.visit_date)) {
        throw new AvailabilityFail(409, 'NO_DATE', 'Set the visit date first: the link proposes it.');
    }
    const partyKey = typeof party === 'string' ? party.trim().toLowerCase() : party;
    if (!A.PARTIES.includes(partyKey)) {
        throw new AvailabilityFail(400, 'BAD_FIELD', 'party must be factory or inspector.', { field: 'party' });
    }

    return portalCall(async () => {
        // A live link is reused as it is, before the input is validated: its
        // window was valid when made and the lifecycle sync keeps it so (a
        // visit date already in the past must not refuse it).
        if (reuse) {
            const live = await liveLinkOf(conn, visit.id, partyKey, now);
            if (live) return { row: live, created: false, supersededIds: [] };
        }
        const v = A.validateLinkInput({ ...(input || {}), party: partyKey }, visit, today);
        if (v.error) throw new AvailabilityFail(400, v.error.code, v.error.message, { field: v.error.field });
        const cols = v.value;
        const [prev] = await conn.query(
            `SELECT id FROM ${table()} WHERE visit_id = ? AND party = ? AND revoked_at IS NULL`,
            [visit.id, cols.party]
        );
        const supersededIds = prev.map(r => Number(r.id));
        if (supersededIds.length) {
            await conn.query(
                `UPDATE ${table()}
                    SET revoked_at = CURRENT_TIMESTAMP, revoke_reason = 'superseded', row_version = row_version + 1
                  WHERE id IN (?) AND revoked_at IS NULL`,
                [supersededIds]
            );
        }
        const [ins] = await conn.query(
            `INSERT INTO ${table()}
                (token, visit_id, visit_ref, party, recipient_name, recipient_email,
                 supplier_name, factory_label, factory_city,
                 proposed_date, window_start_date, window_end_date, preferred_lang,
                 expires_at, created_by_email, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP + INTERVAL ? DAY, ?, CURRENT_TIMESTAMP)`,
            [
                A.newToken(), Number(visit.id), String(visit.ref || QV.formatRef(visit.id)).slice(0, 16), cols.party,
                cols.recipient_name, cols.recipient_email,
                clip(visit.supplier_name, 255), clip(visit.factory_label, 100), clip(visit.factory_city, 100),
                cols.proposed_date, cols.window_start_date, cols.window_end_date, cols.preferred_lang,
                cols.expiresInDays, clip(userEmail, 255),
            ]
        );
        const row = await loadLinkRow(conn, ins.insertId);
        if (recordAudit) {
            // Never the token: the audit log is readable by every staff user.
            await recordAudit(conn, {
                entityType: 'qc_visit', entityId: Number(visit.id), action: 'availability_link_created',
                before: null,
                after: {
                    linkId: Number(ins.insertId), party: cols.party,
                    windowStartDate: cols.window_start_date, windowEndDate: cols.window_end_date,
                    proposedDate: cols.proposed_date, expiresInDays: cols.expiresInDays,
                    ...(supersededIds.length ? { supersededLinkIds: supersededIds } : {}),
                    ...(via ? { via } : {}),
                },
                userEmail,
            });
        }
        return { row, created: true, supersededIds };
    });
}

/**
 * Open links follow the visit (plan §5, lib linkSyncPatch): a move into
 * CONFIRMED / COMPLETED / CANCELLED revokes them with that reason; a new
 * visit date outside a link's window revokes it ('date_moved'); one inside
 * moves its proposed_date. Call it on the visit write's connection, inside
 * its transaction, after the qc_visits UPDATE, whenever status or visit_date
 * may have changed: a rollback of the visit write then undoes this too.
 *   before / after: the qc_visits row before the write and the patched
 *   values (rows or { status, visitDate }; missing fields mean unchanged).
 * It never fails the visit write: its statements run under a SAVEPOINT, and
 * any error (the jportal table missing — logged at info — or anything else —
 * logged at warn) rolls back to it and returns { skipped: true }. Only an
 * error that already ended the transaction (a deadlock, which InnoDB rolls
 * back whole) is rethrown, since the visit write is lost anyway.
 * Audits availability_links_revoked { linkIds, reason } on the visit.
 * → { revoked: [{ id, reason }], moved: [{ id, proposedDate }], skipped? }
 */
async function syncAvailabilityLinks(conn, opts = {}) {
    const {
        visitId, before, after: afterIn, userEmail = null, recordAudit = null,
        log = defaultLog, now = new Date(), savepoint = true,
    } = opts;
    const b = A.visitState(before);
    const a0 = A.visitState(afterIn);
    // Fields the write did not touch are unchanged.
    const hasStatus = afterIn && afterIn.status !== undefined;
    const hasDate = afterIn && (afterIn.visit_date !== undefined || afterIn.visitDate !== undefined);
    const after = { status: hasStatus ? a0.status : b.status, visitDate: hasDate ? a0.visitDate : b.visitDate };
    const result = { revoked: [], moved: [] };
    if (!visitId || !A.visitChanged(b, after)) return result;

    if (savepoint) await conn.query(`SAVEPOINT ${SAVEPOINT}`);
    try {
        const [rows] = await conn.query(
            `SELECT * FROM ${table()} WHERE visit_id = ? AND revoked_at IS NULL`,
            [visitId]
        );
        const actions = A.linkSyncPatch(rows, { before: b, after }, now);
        for (const act of actions) {
            if (act.action === 'revoke') {
                const [r] = await conn.query(
                    `UPDATE ${table()}
                        SET revoked_at = CURRENT_TIMESTAMP, revoke_reason = ?, row_version = row_version + 1
                      WHERE id = ? AND revoked_at IS NULL`,
                    [act.reason, act.id]
                );
                if (r && r.affectedRows) result.revoked.push({ id: act.id, reason: act.reason });
            } else if (act.action === 'move') {
                const [r] = await conn.query(
                    `UPDATE ${table()}
                        SET proposed_date = ?, row_version = row_version + 1
                      WHERE id = ? AND revoked_at IS NULL`,
                    [act.proposedDate, act.id]
                );
                if (r && r.affectedRows) result.moved.push({ id: act.id, proposedDate: act.proposedDate });
            }
        }
        if (recordAudit) {
            const byReason = new Map();
            for (const r of result.revoked) {
                if (!byReason.has(r.reason)) byReason.set(r.reason, []);
                byReason.get(r.reason).push(r.id);
            }
            for (const [reason, linkIds] of byReason) {
                await recordAudit(conn, {
                    entityType: 'qc_visit', entityId: Number(visitId), action: 'availability_links_revoked',
                    before: null, after: { linkIds, reason }, userEmail,
                });
            }
        }
        if (savepoint) await conn.query(`RELEASE SAVEPOINT ${SAVEPOINT}`);
        return result;
    } catch (err) {
        if (savepoint) {
            try {
                await conn.query(`ROLLBACK TO SAVEPOINT ${SAVEPOINT}`);
            } catch {
                throw err; // the transaction is gone (deadlock): nothing to keep
            }
        } else if (!isSourceMissing(err)) {
            throw err;
        }
        if (isSourceMissing(err)) {
            log.info('[qc-availability] portal table unavailable; links not synced', { visitId, errno: err.errno });
        } else {
            log.warn('[qc-availability] link sync failed; the visit write goes ahead', { visitId, error: err.message });
        }
        return { revoked: [], moved: [], skipped: true };
    }
}

/**
 * The availability link for a proposal email (qc-visit-comms-routes.js).
 *   create: false (preview) — the party's open / answered link if it has
 *           one, else url null (the preview shows the placeholder). Creates
 *           nothing; an unset portal URL or a missing table is url null with
 *           enabled: false (and unavailable: true for the table).
 *   create: true (send) — in its own transaction, lock and re-read the
 *           visit, then reuse the party's live link or create one with the
 *           default window, recipient_email = to[0], recipient_name from the
 *           factory contact / inspector (before the email goes out). Throws
 *           AvailabilityFail (404 NOT_FOUND, 409 PORTAL_URL_UNSET / TERMINAL /
 *           NO_DATE, 400 BAD_FIELD, 503 PORTAL_UNAVAILABLE).
 * → { url, linkId, created, enabled?, unavailable? }
 */
async function emailAvailabilityLink(conn, visit, audience, opts = {}) {
    const {
        create = false, to = [], userEmail = null, recordAudit = null,
        portalUrl = portalUrlOf(), now = new Date(), log = defaultLog,
    } = opts;
    if (!create) {
        // enabled: false when no link could be sent (portal URL unset, or the
        // table missing — then also unavailable: true); the preview then
        // shows neither a link nor the placeholder.
        if (!portalUrl) return { url: null, linkId: null, created: false, enabled: false };
        try {
            const live = await liveLinkOf(conn, visit.id, audience, now);
            return live
                ? { url: A.linkUrl(portalUrl, live.token), linkId: Number(live.id), created: false, enabled: true }
                : { url: null, linkId: null, created: false, enabled: true };
        } catch (e) {
            if (!isSourceMissing(e)) throw e;
            log.info('[qc-availability] portal table unavailable (preview)', { visitId: visit.id, errno: e.errno });
            return { url: null, linkId: null, created: false, enabled: false, unavailable: true };
        }
    }
    const recipientEmail = Array.isArray(to) && to.length ? clip(to[0], 255) : null;
    const out = await inTx(conn, async () => {
        // The visit row locked and read afresh, as POST /availability-links
        // and the visit writes do: creation is serialised per visit, and
        // TERMINAL / NO_DATE are checked against the current row.
        const [rows] = await conn.query(`SELECT * FROM qc_visits WHERE id = ? FOR UPDATE`, [visit.id]);
        if (!rows.length) throw new AvailabilityFail(404, 'NOT_FOUND', `Visit ${visit.id} not found.`);
        const fresh = rows[0];
        const recipientName = clip(audience === 'factory' ? fresh.factory_contact_name : fresh.inspector_name, 255);
        return createOrReuseLink(conn, fresh, audience, {
            input: { recipientName, recipientEmail },
            reuse: true, userEmail, recordAudit, portalUrl, now, via: 'email',
        });
    });
    return { url: A.linkUrl(portalUrl, out.row.token), linkId: Number(out.row.id), created: out.created };
}

// ── routes ──────────────────────────────────────────────────────────────────

function registerQcAvailabilityRoutes(app, deps) {
    const { withConnection, recordAudit, auditLogSchemaReady } = deps;
    const log = deps.log || defaultLog;

    const route = (label, fn) => async (req, res) => {
        try {
            await fn(req, res);
        } catch (e) {
            if (res.headersSent) return;
            if (e instanceof AvailabilityFail) return res.status(e.status).json(failBody(e));
            if (isSourceMissing(e)) return res.status(503).json(failBody(portalUnavailable()));
            log.error(`[${label}]`, e);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    };

    // The same predicate as the other QC routes (EDITOR_ROLES).
    function refuseReadOnly(req) {
        if (!QV.canEdit(req.userType)) throw new AvailabilityFail(403, 'READ_ONLY', 'Read only.');
    }

    async function loadVisit(conn, id, { lock = false } = {}) {
        const [rows] = await conn.query(`SELECT * FROM qc_visits WHERE id = ?${lock ? ' FOR UPDATE' : ''}`, [id]);
        if (!rows.length) throw new AvailabilityFail(404, 'NOT_FOUND', `Visit ${id} not found.`);
        return rows[0];
    }

    // ── GET /api/v1/qc-visits/:id/availability ──────────────────────────────
    // { enabled, links: Link[] (newest first), collated } — or, without the
    // jportal table, { enabled, unavailable: true, links: [], collated: null }.
    app.get('/api/v1/qc-visits/:id(\\d+)/availability', route('GET /qc-visits/:id/availability', async (req, res) => {
        const id = Number(req.params.id);
        const portalUrl = portalUrlOf();
        const enabled = portalUrl !== '';
        const now = new Date();
        const out = await withConnection(async (conn) => {
            await loadVisit(conn, id);
            let rows;
            try {
                rows = await loadLinkRows(conn, id);
            } catch (e) {
                if (!isSourceMissing(e)) throw e;
                log.info('[qc-availability] portal table unavailable', { visitId: id, errno: e.errno });
                return { enabled, unavailable: true, links: [], collated: null };
            }
            return {
                enabled,
                links: rows.map(r => A.linkToJson(r, { portalUrl, now })),
                collated: A.collate(rows, now),
            };
        });
        res.json(out);
    }));

    // ── POST /api/v1/qc-visits/:id/availability-links ───────────────────────
    // { party, windowStartDate?, windowEndDate?, recipientName?, recipientEmail?,
    //   preferredLang?, expiresInDays? } → 201 Link. Revokes the party's
    // earlier link ('superseded') in the same transaction.
    app.post('/api/v1/qc-visits/:id(\\d+)/availability-links', route('POST /qc-visits/:id/availability-links', async (req, res) => {
        refuseReadOnly(req);
        await auditLogSchemaReady;
        const id = Number(req.params.id);
        const b = req.body || {};
        const portalUrl = portalUrlOf();
        const now = new Date();
        const row = await withConnection(conn => inTx(conn, async () => {
            const visit = await loadVisit(conn, id, { lock: true });
            const out = await createOrReuseLink(conn, visit, b.party, {
                input: b, reuse: false, userEmail: req.userEmail || null, recordAudit, portalUrl, now,
            });
            return out.row;
        }));
        res.status(201).json(A.linkToJson(row, { portalUrl, now }));
    }));

    // ── POST /api/v1/qc-visits/:id/availability-links/:linkId/revoke ────────
    // → 200 Link (revoke_reason 'staff'). Revoking a revoked link is a no-op.
    app.post('/api/v1/qc-visits/:id(\\d+)/availability-links/:linkId(\\d+)/revoke', route('POST /qc-visits/:id/availability-links/:linkId/revoke', async (req, res) => {
        refuseReadOnly(req);
        await auditLogSchemaReady;
        const id = Number(req.params.id);
        const linkId = Number(req.params.linkId);
        const portalUrl = portalUrlOf();
        const now = new Date();
        const row = await withConnection(conn => inTx(conn, async () => {
            await loadVisit(conn, id);
            return portalCall(async () => {
                const link = await loadLinkRow(conn, linkId);
                if (!link || Number(link.visit_id) !== id) {
                    throw new AvailabilityFail(404, 'NOT_FOUND', `Availability link ${linkId} not found on this visit.`);
                }
                if (link.revoked_at != null) return link;
                const [r] = await conn.query(
                    `UPDATE ${table()}
                        SET revoked_at = CURRENT_TIMESTAMP, revoke_reason = 'staff', row_version = row_version + 1
                      WHERE id = ? AND revoked_at IS NULL`,
                    [linkId]
                );
                if (r && r.affectedRows && recordAudit) {
                    await recordAudit(conn, {
                        entityType: 'qc_visit', entityId: id, action: 'availability_link_revoked',
                        before: null, after: { linkId, party: link.party, reason: 'staff' },
                        userEmail: req.userEmail || null,
                    });
                }
                return loadLinkRow(conn, linkId);
            });
        }));
        res.json(A.linkToJson(row, { portalUrl, now }));
    }));
}

module.exports = {
    registerQcAvailabilityRoutes,
    createOrReuseLink,
    syncAvailabilityLinks,
    emailAvailabilityLink,
    availabilityEnabled,
    portalUrlOf,
    AvailabilityFail,
    PORTAL_UNAVAILABLE_MESSAGE,
};
