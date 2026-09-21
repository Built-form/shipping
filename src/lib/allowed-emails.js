'use strict';

// This app's OWN user allowlist — the table the auth middleware in
// src/handlers/orders.js checks every request against, plus the CRUD routes
// that manage it.
//
// Why a new table: `jfa.allowed_emails` is shared. joshdex's API (perp.js) runs
// against the same schema, creates that table itself and exposes its own
// /allowed-emails CRUD, so neither app could add or remove a user without
// changing who can log into the other. `shipping_allowed_emails` is seeded with
// a copy of those rows once and is ours from then on; `allowed_emails` is left
// exactly as it is for joshdex. Same move JFPRO made when it stopped reading
// this schema's table through a view.

const express = require('express');
const log = require('./logger');
const { recordAudit } = require('./audit');

const TABLE = 'shipping_allowed_emails';
const LEGACY_TABLE = 'allowed_emails';

// A user's `type` — their role. The set of roles is NOT hardcoded: it's
// whatever distinct values the table currently holds (see listUserTypes), so
// introducing a role is assigning it to someone, never a deploy. Writes are
// therefore validated on SHAPE rather than membership — a fixed allowlist would
// make a new role impossible to create without shipping code, which is exactly
// what this avoids.
//
// Only these two are structural and so are always offered even when no row
// carries them: 'standard' is the default, 'admin' is the role that may manage
// this allowlist (and the other admin-gated routes).
const BASE_USER_TYPES = ['standard', 'admin'];
const DEFAULT_USER_TYPE = 'standard';
const USER_TYPE_MAX_LEN = 32;   // matches the column

const EMAIL_MAX_LEN = 255;
const DISPLAY_NAME_MAX_LEN = 255;

// ── Seed ──────────────────────────────────────────────────────────────────
// The table itself is schema (src/db/migrate/, applied by deploy.sh). The
// seed below runs from migration 2026-09-21_13_seed_allowed_emails.js.

/**
 * First run only: seed the table from the shared `allowed_emails`.
 *
 * The seed is gated on the table being EMPTY, so a later run never re-copies —
 * otherwise removing a user here would resurrect them from the shared table.
 * Nothing is written to `allowed_emails`; joshdex keeps it untouched.
 *
 * If there is nothing to copy (fresh schema, no legacy table), the
 * comma-separated `bootstrapAdminEmails` (BOOTSTRAP_ADMIN_EMAILS) seeds the
 * first admins — without it a fresh database would 401 everyone, including
 * whoever needs to add user #1.
 */
async function seedAllowedEmails(conn, { bootstrapAdminEmails = '' } = {}) {
    const [[{ c: count }]] = await conn.query(`SELECT COUNT(*) AS c FROM ${TABLE}`);
    if (count > 0) return;

    const [legacy] = await conn.query(
        `SELECT TABLE_NAME FROM information_schema.TABLES
          WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?`,
        [LEGACY_TABLE]
    );
    if (legacy.length > 0) {
        const [result] = await conn.query(`
            INSERT IGNORE INTO ${TABLE} (email, type, created_at)
            SELECT LOWER(email), COALESCE(NULLIF(type, ''), '${DEFAULT_USER_TYPE}'), created_at
              FROM ${LEGACY_TABLE}
        `);
        log.info(`[allowed-emails] seeded ${result.affectedRows} user(s) from ${LEGACY_TABLE}`);
    }

    const [[{ c: seeded }]] = await conn.query(`SELECT COUNT(*) AS c FROM ${TABLE}`);
    if (seeded > 0) return;

    const bootstrap = String(bootstrapAdminEmails || '')
        .split(',').map(e => e.trim().toLowerCase()).filter(Boolean);
    for (const email of bootstrap) {
        await conn.query(
            `INSERT IGNORE INTO ${TABLE} (email, type) VALUES (?, 'admin')`,
            [email]
        );
    }
    if (bootstrap.length) {
        log.info(`[allowed-emails] seeded ${bootstrap.length} bootstrap admin(s)`);
    } else {
        log.warn(`[allowed-emails] ${TABLE} is EMPTY and BOOTSTRAP_ADMIN_EMAILS is unset — every authed request will 401.`);
    }
}

/**
 * The auth middleware's lookup. Returns the user's type, or null when the
 * address isn't on the allowlist (→ 401).
 */
async function lookupUserType(pool, email) {
    const conn = await pool.getConnection();
    try {
        const [rows] = await conn.query(
            `SELECT type FROM ${TABLE} WHERE email = ?`,
            [String(email).toLowerCase()]
        );
        if (rows.length === 0) return null;
        return rows[0].type || DEFAULT_USER_TYPE;
    } finally {
        conn.release();
    }
}

// ── Validation ────────────────────────────────────────────────────────────

// Emails are stored and compared lower-cased — the JWT claim the middleware
// looks up is lower-cased too, so anything else would silently never match. The
// shape check is deliberately loose (Google is the real authority on whether an
// address exists); it only rejects obvious typos.
function validateEmail(value) {
    if (typeof value !== 'string') return { error: 'email is required and must be a string.' };
    const email = value.trim().toLowerCase();
    if (!email) return { error: 'email cannot be empty.' };
    if (email.length > EMAIL_MAX_LEN) return { error: `email cannot exceed ${EMAIL_MAX_LEN} characters.` };
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) return { error: 'email is not a valid address.' };
    return { email };
}

// Shape-only, deliberately: the roles in use are read back out of the table
// (listUserTypes), so rejecting anything not already present would mean a role
// could never be introduced without a deploy. The cost is that a typo creates a
// role rather than erroring — the UI is where that gets caught, by offering the
// existing roles as the default choice and free text only as a deliberate act.
function validateUserType(value) {
    if (value === undefined || value === null || value === '') return { type: DEFAULT_USER_TYPE };
    const type = String(value).trim().toLowerCase();
    if (type.length > USER_TYPE_MAX_LEN) {
        return { error: `type cannot exceed ${USER_TYPE_MAX_LEN} characters.` };
    }
    if (!/^[a-z0-9][a-z0-9_-]*$/.test(type)) {
        return { error: 'type must be lower-case letters, digits, hyphens or underscores (e.g. "warehouse").' };
    }
    return { type };
}

// The roles currently in use, plus the two structural ones, for a UI dropdown.
// Read fresh every call — that's the whole point: assign someone 'accountant'
// and it appears here immediately, no deploy.
async function listUserTypes(conn) {
    const [rows] = await conn.query(
        `SELECT DISTINCT type FROM ${TABLE} WHERE type IS NOT NULL AND type <> ''`
    );
    const types = new Set(BASE_USER_TYPES);
    for (const r of rows) types.add(String(r.type).toLowerCase());
    return [...types].sort();
}

function validateDisplayName(value) {
    if (value === undefined || value === null) return { displayName: null };
    const displayName = String(value).trim();
    if (!displayName) return { displayName: null };
    if (displayName.length > DISPLAY_NAME_MAX_LEN) {
        return { error: `displayName cannot exceed ${DISPLAY_NAME_MAX_LEN} characters.` };
    }
    return { displayName };
}

function rowToUser(r) {
    return {
        id: r.id,
        email: r.email,
        displayName: r.display_name || null,
        type: r.type || DEFAULT_USER_TYPE,
        createdAt: r.created_at?.toISOString?.() ?? r.created_at ?? null,
        updatedAt: r.updated_at?.toISOString?.() ?? r.updated_at ?? null,
    };
}

const USER_COLS = 'id, email, display_name, type, created_at, updated_at';

async function loadUser(conn, email) {
    const [rows] = await conn.query(
        `SELECT ${USER_COLS} FROM ${TABLE} WHERE email = ?`,
        [email]
    );
    return rows[0] ? rowToUser(rows[0]) : null;
}

// How many admins are left besides `exceptEmail`. Guards the two ways an admin
// can lock everyone out of user management: removing the last admin, or
// demoting them to a type that can't reach these routes.
async function otherAdminCount(conn, exceptEmail) {
    const [rows] = await conn.query(
        `SELECT COUNT(*) AS c FROM ${TABLE} WHERE type = 'admin' AND email <> ?`,
        [exceptEmail]
    );
    return Number(rows[0]?.c) || 0;
}

// ── Routes ────────────────────────────────────────────────────────────────

/**
 * Mounts the allowlist CRUD at /api/v1/users.
 *
 * Everything except GET /me is admin-only: this list decides who can reach the
 * API at all, so handing it to standard users would let anyone grant themselves
 * anything. Changes take effect on the caller's very next request — the auth
 * middleware reads this table per request and holds no cache.
 */
function registerUserRoutes(app, pool) {
    const router = express.Router();

    const requireAdmin = (req, res) => {
        if (req.userType === 'admin') return true;
        res.status(403).json({ error: 'Admin access required.' });
        return false;
    };

    // GET /api/v1/users/me — the caller's own record. Open to every allowlisted
    // user (the frontend needs its own type to decide what to render), so it
    // must stay ABOVE /:email. In local dev the row may not exist — fall back to
    // what the middleware short-circuited to rather than 404ing the whole app.
    router.get('/me', async (req, res) => {
        try {
            const conn = await pool.getConnection();
            try {
                const me = await loadUser(conn, req.userEmail);
                if (me) return res.json(me);
                res.json({
                    id: null, email: req.userEmail, displayName: null,
                    type: req.userType || DEFAULT_USER_TYPE, createdAt: null, updatedAt: null,
                });
            } finally {
                conn.release();
            }
        } catch (error) {
            log.error('[GET /users/me]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // GET /api/v1/users?type=admin&q=jfa — list, both filters optional.
    router.get('/', async (req, res) => {
        if (!requireAdmin(req, res)) return;
        try {
            const where = [];
            const params = [];
            if (req.query.type !== undefined) {
                const t = validateUserType(req.query.type);
                if (t.error) return res.status(400).json({ error: t.error });
                where.push('type = ?');
                params.push(t.type);
            }
            if (req.query.q) {
                where.push('(email LIKE ? OR display_name LIKE ?)');
                params.push(`%${req.query.q}%`, `%${req.query.q}%`);
            }
            const conn = await pool.getConnection();
            try {
                const [rows] = await conn.query(
                    `SELECT ${USER_COLS} FROM ${TABLE}
                     ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
                     ORDER BY email ASC`,
                    params
                );
                res.json({ data: rows.map(rowToUser) });
            } finally {
                conn.release();
            }
        } catch (error) {
            log.error('[GET /users]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // GET /api/v1/users/:email
    router.get('/:email', async (req, res) => {
        if (!requireAdmin(req, res)) return;
        try {
            const e = validateEmail(req.params.email);
            if (e.error) return res.status(400).json({ error: e.error });
            const conn = await pool.getConnection();
            try {
                const user = await loadUser(conn, e.email);
                if (!user) return res.status(404).json({ error: 'User not found.' });
                res.json(user);
            } finally {
                conn.release();
            }
        } catch (error) {
            log.error('[GET /users/:email]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // POST /api/v1/users  Body: { email, type?, displayName? }
    // Grants access. The unique key on email is the authority on duplicates.
    router.post('/', async (req, res) => {
        if (!requireAdmin(req, res)) return;
        try {
            const e = validateEmail(req.body?.email);
            if (e.error) return res.status(400).json({ error: e.error });
            const t = validateUserType(req.body?.type);
            if (t.error) return res.status(400).json({ error: t.error });
            const d = validateDisplayName(req.body?.displayName ?? req.body?.display_name);
            if (d.error) return res.status(400).json({ error: d.error });

            const conn = await pool.getConnection();
            try {
                const [result] = await conn.query(
                    `INSERT INTO ${TABLE} (email, display_name, type) VALUES (?, ?, ?)`,
                    [e.email, d.displayName, t.type]
                );
                const created = await loadUser(conn, e.email);
                await recordAudit(conn, {
                    entityType: 'user', entityId: result.insertId, action: 'create',
                    before: null, after: created, userEmail: req.userEmail,
                });
                res.status(201).json(created);
            } finally {
                conn.release();
            }
        } catch (error) {
            if (error.code === 'ER_DUP_ENTRY') {
                return res.status(409).json({ error: 'That user already has access.' });
            }
            log.error('[POST /users]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // PATCH /api/v1/users/:email  Body: { type?, displayName? }
    // The email IS the identity (it's what the JWT claim matches), so it can't
    // be changed here — remove the user and add the new address instead.
    const updateUser = async (req, res) => {
        if (!requireAdmin(req, res)) return;
        try {
            const e = validateEmail(req.params.email);
            if (e.error) return res.status(400).json({ error: e.error });

            const conn = await pool.getConnection();
            try {
                const before = await loadUser(conn, e.email);
                if (!before) return res.status(404).json({ error: 'User not found.' });

                const bodyEmail = req.body?.email;
                if (bodyEmail !== undefined && String(bodyEmail).trim().toLowerCase() !== e.email) {
                    return res.status(400).json({
                        error: 'email cannot be changed — remove this user and add the new address.',
                    });
                }

                const sets = [];
                const params = [];
                if (req.body?.type !== undefined) {
                    const t = validateUserType(req.body.type);
                    if (t.error) return res.status(400).json({ error: t.error });
                    // Demoting the last admin would leave nobody able to manage
                    // the allowlist — including re-promoting anyone.
                    if (before.type === 'admin' && t.type !== 'admin'
                        && await otherAdminCount(conn, e.email) === 0) {
                        return res.status(409).json({ error: 'Cannot demote the last admin.' });
                    }
                    sets.push('type = ?');
                    params.push(t.type);
                }
                const rawName = req.body?.displayName !== undefined ? req.body.displayName : req.body?.display_name;
                if (rawName !== undefined) {
                    const d = validateDisplayName(rawName);
                    if (d.error) return res.status(400).json({ error: d.error });
                    sets.push('display_name = ?');
                    params.push(d.displayName);
                }
                if (sets.length === 0) {
                    return res.status(400).json({ error: 'Nothing to update: provide type and/or displayName.' });
                }

                params.push(e.email);
                await conn.query(`UPDATE ${TABLE} SET ${sets.join(', ')} WHERE email = ?`, params);

                const after = await loadUser(conn, e.email);
                await recordAudit(conn, {
                    entityType: 'user', entityId: before.id, action: 'update',
                    before, after, userEmail: req.userEmail,
                });
                res.json(after);
            } finally {
                conn.release();
            }
        } catch (error) {
            log.error('[PATCH /users/:email]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    };
    router.patch('/:email', updateUser);
    // PUT is accepted as an alias — the update is a full-field merge either way,
    // and frontends reach for PUT often enough that 405ing them is just friction.
    router.put('/:email', updateUser);

    // DELETE /api/v1/users/:email — revokes access immediately (that email's
    // next request 401s). Two lockout guards: an admin can't remove themselves
    // by accident, and the last admin can't be removed at all.
    router.delete('/:email', async (req, res) => {
        if (!requireAdmin(req, res)) return;
        try {
            const e = validateEmail(req.params.email);
            if (e.error) return res.status(400).json({ error: e.error });

            const conn = await pool.getConnection();
            try {
                const before = await loadUser(conn, e.email);
                if (!before) return res.status(404).json({ error: 'User not found.' });
                if (e.email === req.userEmail) {
                    return res.status(409).json({ error: 'You cannot remove your own access.' });
                }
                if (before.type === 'admin' && await otherAdminCount(conn, e.email) === 0) {
                    return res.status(409).json({ error: 'Cannot remove the last admin.' });
                }

                await conn.query(`DELETE FROM ${TABLE} WHERE email = ?`, [e.email]);
                await recordAudit(conn, {
                    entityType: 'user', entityId: before.id, action: 'delete',
                    before, after: null, userEmail: req.userEmail,
                });
                res.json({ message: 'Access removed.', email: e.email });
            } finally {
                conn.release();
            }
        } catch (error) {
            log.error('[DELETE /users/:email]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // GET /api/v1/user-types — the roles in use, read live from the table so a
    // new one needs no deploy. Mounted on `app` rather than the router so it
    // can't be shadowed by /users/:email. Open to any allowlisted user: it's a
    // list of role names, and the UI needs it before it knows the viewer's own.
    app.get('/api/v1/user-types', async (req, res) => {
        try {
            const conn = await pool.getConnection();
            try {
                res.json({ data: await listUserTypes(conn) });
            } finally {
                conn.release();
            }
        } catch (error) {
            log.error('[GET /user-types]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    app.use('/api/v1/users', router);
}

module.exports = {
    TABLE,
    BASE_USER_TYPES,
    DEFAULT_USER_TYPE,
    seedAllowedEmails,
    lookupUserType,
    listUserTypes,
    registerUserRoutes,
};
