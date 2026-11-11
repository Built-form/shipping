'use strict';

// /api/v1/ui-hints — ShipLine's info bubbles: short explanations a person
// writes on a caption (a column head, a field label, a section heading) for
// everyone to read. Rules and the key grammar: src/lib/ui-hints.js. Schema:
// src/db/migrate/2026-11-10_10_shipline_ui_hints.sql (no cold-start DDL).
// Frontend guide: docs/ui-hints-frontend.md.
//
// Keys contain '/', and behind the HTTP API an encoded %2F arrives as a real
// slash (see draftNameFromPath in orders.js), so a key always travels in the
// body and an existing row is addressed by its numeric id.
//
// Concurrency: a row is born at row_version 1, rowVersion 0 means "create",
// and every change to an existing row names { id, rowVersion }: a transaction
// locks the row, checks it, then runs a statement guarded on the version, so
// two people saving at once get one success and one 409 STALE that carries
// the current hint. The pool has one connection: nothing here checks out a
// second one while holding it.
//
// Registered from orders.js; authed through the greedy /api/v1/{proxy+} route.

const L = require('../lib/ui-hints');
const { TABLE: USERS_TABLE } = require('../lib/allowed-emails');

const T = L.TABLE;
const SELECT = `SELECT h.*, u.display_name AS updated_by_name
                  FROM ${T} h
                  LEFT JOIN ${USERS_TABLE} u ON u.email = h.updated_by`;

function registerUiHintRoutes(app, { withConnection, recordAudit, auditLogSchemaReady, log }) {
    const refuse = (res, r) => res.status(r.status).json({
        error: r.error, code: r.code, ...(r.current ? { current: r.current } : {}),
    });
    const badField = error => ({ status: 400, code: 'BAD_FIELD', error });

    const load = async (conn, column, value) => {
        const [rows] = await conn.query(`${SELECT} WHERE ${column} = ?`, [value]);
        return rows[0] || null;
    };
    // The lock reads the hint table alone: a join would lock the editor's
    // user row too.
    const lock = async (conn, column, value) => {
        const [rows] = await conn.query(`SELECT * FROM ${T} WHERE ${column} = ? FOR UPDATE`, [value]);
        return rows[0] || null;
    };
    const currentJson = async (conn, id) => {
        const row = await load(conn, 'h.id', id);
        return row ? L.rowToJson(row) : null;
    };
    const conflict = async (conn, refusal, id) => ({ fail: { ...refusal, current: await currentJson(conn, id) } });

    // fn(conn) returns { fail } (rolled back) or { ok: { status, body } }
    // (committed). Anything thrown rolls back and goes on to the 500.
    const inTransaction = fn => withConnection(async conn => {
        await conn.beginTransaction();
        try {
            const out = await fn(conn);
            if (out.fail) await conn.rollback();
            else await conn.commit();
            return out;
        } catch (e) {
            await conn.rollback().catch(() => {});
            throw e;
        }
    });

    const finish = (res, out) => (out.fail ? refuse(res, out.fail) : res.status(out.ok.status).json(out.ok.body));
    const internal = (res, label, error) => {
        log.error(label, error);
        res.status(500).json({ error: 'An internal error occurred.' });
    };

    // GET /api/v1/ui-hints — every hint, for anyone signed in. 200 { data }
    app.get('/api/v1/ui-hints', async (req, res) => {
        try {
            const rows = await withConnection(async conn => {
                const [r] = await conn.query(`${SELECT} ORDER BY h.hint_key`);
                return r;
            });
            res.json({ data: rows.map(L.rowToJson) });
        } catch (error) {
            internal(res, '[GET /ui-hints]', error);
        }
    });

    // PUT /api/v1/ui-hints
    //   create: { key, rowVersion: 0, body, linkPath?, linkLabel? }  → 201 { hint }
    //   update: { key, id, rowVersion, body, linkPath?, linkLabel? }  → 200 { hint, unchanged? }
    app.put('/api/v1/ui-hints', async (req, res) => {
        try {
            if (!L.canWriteHints(req.userType)) return refuse(res, L.cannotWrite(req.userType));
            const parsed = L.parsePutBody(req.body);
            if (parsed.error) return refuse(res, badField(parsed.error));
            const v = parsed.value;
            const by = L.normEmail(req.userEmail);
            await auditLogSchemaReady;
            const out = await inTransaction(async conn => {
                const existing = await lock(conn, 'hint_key', v.key);
                const plan = L.planUpsert(existing, v.rowVersion, v.id);
                if (plan === 'occupied') return conflict(conn, L.REFUSALS.OCCUPIED, existing.id);
                if (plan === 'missing') return { fail: L.REFUSALS.DELETED };
                if (plan === 'stale') return conflict(conn, L.REFUSALS.STALE, existing.id);

                if (plan === 'insert') {
                    let result;
                    try {
                        [result] = await conn.query(
                            `INSERT INTO ${T} (hint_key, body, link_path, link_label, created_by, updated_by) VALUES (?, ?, ?, ?, ?, ?)`,
                            [v.key, v.body, v.linkPath, v.linkLabel, by, by],
                        );
                    } catch (e) {
                        // Someone created the same place between our lock (a
                        // gap, nothing to lock) and the insert.
                        if (e && e.code === 'ER_DUP_ENTRY') {
                            const row = await load(conn, 'h.hint_key', v.key);
                            return { fail: { ...L.REFUSALS.OCCUPIED, ...(row ? { current: L.rowToJson(row) } : {}) } };
                        }
                        throw e;
                    }
                    const row = await load(conn, 'h.id', result.insertId);
                    await recordAudit(conn, { entityType: L.AUDIT_ENTITY, entityId: row.id, action: 'create', before: null, after: L.hintSnapshot(row), userEmail: req.userEmail });
                    return { ok: { status: 201, body: { hint: L.rowToJson(row) } } };
                }

                // plan === 'update'
                if (L.sameContent(existing, v)) {
                    return { ok: { status: 200, body: { hint: await currentJson(conn, existing.id), unchanged: true } } };
                }
                const [upd] = await conn.query(
                    `UPDATE ${T} SET body = ?, link_path = ?, link_label = ?, updated_by = ?, row_version = row_version + 1
                      WHERE id = ? AND hint_key = ? AND row_version = ?`,
                    [v.body, v.linkPath, v.linkLabel, by, v.id, v.key, v.rowVersion],
                );
                if (!upd || upd.affectedRows === 0) return conflict(conn, L.REFUSALS.STALE, existing.id);
                const row = await load(conn, 'h.id', v.id);
                await recordAudit(conn, { entityType: L.AUDIT_ENTITY, entityId: row.id, action: 'update', before: L.hintSnapshot(existing), after: L.hintSnapshot(row), userEmail: req.userEmail });
                return { ok: { status: 200, body: { hint: L.rowToJson(row) } } };
            });
            finish(res, out);
        } catch (error) {
            internal(res, '[PUT /ui-hints]', error);
        }
    });

    // POST /api/v1/ui-hints/:id/move  Body: { rowVersion, toKey }  → 200 { hint }
    // The same text, on another caption (a caption was renamed or moved).
    app.post('/api/v1/ui-hints/:id(\\d+)/move', async (req, res) => {
        try {
            if (!L.canWriteHints(req.userType)) return refuse(res, L.cannotWrite(req.userType));
            const id = Number(req.params.id);
            const parsed = L.parseMoveBody(req.body);
            if (parsed.error) return refuse(res, badField(parsed.error));
            const v = parsed.value;
            const by = L.normEmail(req.userEmail);
            await auditLogSchemaReady;
            const out = await inTransaction(async conn => {
                const existing = await lock(conn, 'id', id);
                if (!existing) return { fail: L.REFUSALS.DELETED };
                if (Number(existing.row_version) !== v.rowVersion) return conflict(conn, L.REFUSALS.STALE, id);
                if (existing.hint_key === v.toKey) return { fail: badField('toKey is where the hint already is.') };
                let upd;
                try {
                    [upd] = await conn.query(
                        `UPDATE ${T} SET hint_key = ?, updated_by = ?, row_version = row_version + 1
                          WHERE id = ? AND row_version = ?`,
                        [v.toKey, by, id, v.rowVersion],
                    );
                } catch (e) {
                    if (e && e.code === 'ER_DUP_ENTRY') return { fail: L.REFUSALS.OCCUPIED_MOVE };
                    throw e;
                }
                if (!upd || upd.affectedRows === 0) return conflict(conn, L.REFUSALS.STALE, id);
                const row = await load(conn, 'h.id', id);
                await recordAudit(conn, { entityType: L.AUDIT_ENTITY, entityId: id, action: 'update', before: L.hintSnapshot(existing), after: L.hintSnapshot(row), userEmail: req.userEmail });
                return { ok: { status: 200, body: { hint: L.rowToJson(row) } } };
            });
            finish(res, out);
        } catch (error) {
            internal(res, '[POST /ui-hints/:id/move]', error);
        }
    });

    // DELETE /api/v1/ui-hints/:id?rowVersion=n  → 200 { removed }
    // The author, or an admin.
    app.delete('/api/v1/ui-hints/:id(\\d+)', async (req, res) => {
        try {
            if (!L.canWriteHints(req.userType)) return refuse(res, L.cannotWrite(req.userType));
            const id = Number(req.params.id);
            const rv = L.parseRowVersion(req.query && req.query.rowVersion);
            if (rv.error) return refuse(res, badField(rv.error));
            if (rv.rowVersion === 0) return refuse(res, badField('rowVersion must be the hint\'s current version (1 or more).'));
            await auditLogSchemaReady;
            const out = await inTransaction(async conn => {
                const existing = await lock(conn, 'id', id);
                if (!existing) return { fail: L.REFUSALS.DELETED };
                if (Number(existing.row_version) !== rv.rowVersion) return conflict(conn, L.REFUSALS.STALE, id);
                if (!L.canDeleteHint(req.userType, req.userEmail, existing)) return { fail: L.REFUSALS.NOT_AUTHOR };
                const removed = L.rowToJson(await load(conn, 'h.id', id));
                const [del] = await conn.query(`DELETE FROM ${T} WHERE id = ? AND row_version = ?`, [id, rv.rowVersion]);
                if (!del || del.affectedRows === 0) return conflict(conn, L.REFUSALS.STALE, id);
                await recordAudit(conn, { entityType: L.AUDIT_ENTITY, entityId: id, action: 'delete', before: L.hintSnapshot(existing), after: null, userEmail: req.userEmail });
                return { ok: { status: 200, body: { removed } } };
            });
            finish(res, out);
        } catch (error) {
            internal(res, '[DELETE /ui-hints/:id]', error);
        }
    });
}

module.exports = { registerUiHintRoutes };
