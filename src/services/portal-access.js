'use strict';

// The supplier portal's side of sending a PO (docs/po-portal-send.md): before
// the "your PO is on the portal" email goes out, every supplier recipient is
// pre-approved as a portal user for the PO's supplier, so registering with that
// address needs no approval; and the portal's record of who opened which
// emailed PDF, read back for ShipLine's SEEN tick.
//
// Both live in jportal's schema (JPORTAL_DB_SCHEMA, default `jportal`):
//   - supplier_users / user_supplier_access, which JFPRO's Portal users page
//     also writes (portal README section 4). A pre-provisioned row is unbound
//     (cognito_sub NULL) and `active`; the portal binds it on the first
//     sign-in with that verified email and keeps its status.
//   - po_document_views, written by the portal (README section 10, "Views").
// Shipping's login is `admin` on both stages, so there are no grants to keep.
// An environment without the schema or a table answers as unavailable
// (SOURCE_MISSING) instead of failing the send.
//
// The pure rules are in src/lib/po-portal-email.js.

const { jportalSchema, isSourceMissing } = require('../lib/jportal');
const P = require('../lib/po-portal-email');

const NOTE_MAX = 500;      // supplier_users.decided_note
const ISO_UTC = "'%Y-%m-%dT%H:%i:%sZ'";

// Two sends that each invite a new address deadlock under REPEATABLE READ:
// each `SELECT … FOR UPDATE` of a missing email takes a gap lock on
// uniq_email, gap locks don't conflict with each other, and then both INSERTs
// wait on the other's gap. With few portal users most new addresses share one
// gap, so it is not only the same address twice (tools/smoke-po-portal-preapproval.js
// reproduced it every time for the same address, and for different ones).
// READ COMMITTED takes no gap locks for those reads; a real clash on the same
// email then waits and lands in the 1062 branch below. A deadlock that still
// happens (two sends listing the same new addresses in opposite orders) has
// rolled the whole transaction back, so it is simply run again.
const ER_LOCK_DEADLOCK = 1213;
const MAX_ATTEMPTS = 3;

/**
 * Pre-approves `emails` on the portal and links them to every supplier on the
 * PO: the rows named exactly as the PO's own supplier (the portal's access
 * rule for the PO itself, so it is visible to them) and as each of its lines'
 * suppliers (`lineSuppliers`; older POs' lines often name a different row of
 * the same company). Per address: no row -> created `active` and unbound
 * ('invited'); `pending` -> `active` ('approved'); `active` -> left as is
 * ('already_active'); `rejected` / `disabled` -> left as is ('blocked', no
 * supplier added). Every non-blocked address gets the supplier rows it lacks.
 * One transaction; commits before the email is sent.
 *
 * Every recipient is pre-approved, staff included (P.provisionList).
 *
 * Returns { status, suppliers, poSupplierMatched, users } where
 * status is 'ok', 'none' (no recipients), 'no_supplier' (no live
 * supplier has the exact name of the PO or any of its lines, so nobody is
 * linked) or 'unavailable' (no portal schema here). poSupplierMatched is
 * false when only line suppliers matched: recipients are linked to those, but
 * the PO itself will not show for them until its supplier name is fixed.
 */
async function preapprovePortalUsers(conn, { recipients, poSupplier, lineSuppliers = [], actorEmail, note }) {
    const empty = status => ({ status, suppliers: [], poSupplierMatched: false, users: [] });
    const provision = P.provisionList(recipients);
    if (!provision.length) return empty('none');

    const poName = poSupplier == null ? '' : String(poSupplier);
    const names = [...new Set([poName, ...lineSuppliers.map(n => (n == null ? '' : String(n)))].filter(Boolean))];
    if (!names.length) return empty('no_supplier');
    const BIN = 'COLLATE utf8mb4_0900_bin';
    const [suppliers] = await conn.query(
        `SELECT s.id, s.name
           FROM suppliers s
          WHERE s.deleted_at IS NULL
            AND CONVERT(s.name USING utf8mb4) ${BIN}
                IN (${names.map(() => `CONVERT(? USING utf8mb4) ${BIN}`).join(', ')})
          ORDER BY s.id`,
        names
    );
    if (!suppliers.length) return empty('no_supplier');
    // utf8mb4_0900_bin is a plain code-point comparison, as === is.
    const poSupplierMatched = suppliers.some(s => s.name === poName);

    const ctx = {
        jp: jportalSchema(),
        actor: actorEmail || 'shipline',
        decidedNote: String(note || '').slice(0, NOTE_MAX) || null,
    };
    for (let attempt = 1; ; attempt++) {
        try {
            const users = await provisionOnce(conn, provision, suppliers, ctx);
            return {
                status: 'ok',
                suppliers: suppliers.map(s => ({ id: s.id, name: s.name })),
                poSupplierMatched,
                users,
            };
        } catch (e) {
            if (isSourceMissing(e)) return empty('unavailable');
            if (e.errno === ER_LOCK_DEADLOCK && attempt < MAX_ATTEMPTS) continue;
            throw e;
        }
    }
}

// One attempt, in one READ COMMITTED transaction; rolled back on any error.
async function provisionOnce(conn, provision, suppliers, { jp, actor, decidedNote }) {
    const users = [];
    try {
        // Applies to the next transaction only, so it goes right before BEGIN:
        // any statement in between (even a plain SELECT, which autocommits as
        // its own transaction) would use it up.
        await conn.query('SET TRANSACTION ISOLATION LEVEL READ COMMITTED');
        await conn.beginTransaction();
        for (const email of provision) {
            const lock = async () => (await conn.query(
                `SELECT id, status FROM ${jp}.supplier_users WHERE email = ? FOR UPDATE`, [email]
            ))[0][0] || null;
            let row = await lock();
            let outcome = P.outcomeFor(row);
            let userId = row ? Number(row.id) : null;

            if (outcome === 'invited') {
                try {
                    const [ins] = await conn.query(
                        `INSERT INTO ${jp}.supplier_users
                            (email, status, approved_by, approved_at, decided_note, created_by)
                         VALUES (?, 'active', ?, UTC_TIMESTAMP(), ?, ?)`,
                        [email, actor, decidedNote, actor]
                    );
                    userId = Number(ins.insertId);
                } catch (e) {
                    // The supplier registered this address a moment ago: take their row.
                    if (e.errno !== 1062) throw e;
                    row = await lock();
                    outcome = P.outcomeFor(row);
                    userId = row ? Number(row.id) : null;
                }
            }
            if (outcome === 'approved') {
                // row_version, so an admin with this request open in JFPRO gets a conflict.
                await conn.query(
                    `UPDATE ${jp}.supplier_users
                        SET status = 'active', approved_by = ?, approved_at = UTC_TIMESTAMP(),
                            decided_note = ?, row_version = row_version + 1
                      WHERE id = ? AND status = 'pending'`,
                    [actor, decidedNote, userId]
                );
            }

            let suppliersAdded = 0;
            if (P.GRANTED_OUTCOMES.has(outcome) && userId) {
                const [r] = await conn.query(
                    `INSERT IGNORE INTO ${jp}.user_supplier_access (user_id, supplier_id, supplier_name, created_by)
                     VALUES ${suppliers.map(() => '(?, ?, ?, ?)').join(', ')}`,
                    suppliers.flatMap(s => [userId, s.id, s.name, actor])
                );
                suppliersAdded = r.affectedRows;
            }
            users.push({ email, outcome, suppliersAdded });
        }
        await conn.commit();
    } catch (e) {
        await conn.rollback().catch(() => {});
        throw e;
    }
    return users;
}

/**
 * Who opened each PO document on the portal: Map(documentId -> [{ email,
 * firstViewedAt, lastViewedAt, count }]), first viewer first. Documents nobody
 * opened have no entry. Empty when the portal table is not here.
 */
async function loadPoDocumentViews(conn, documentIds) {
    const out = new Map();
    const ids = [...new Set((documentIds || []).filter(Boolean))];
    if (!ids.length) return out;
    let rows;
    try {
        [rows] = await conn.query(
            `SELECT purchase_order_document_id, user_email, view_count,
                    DATE_FORMAT(first_viewed_at, ${ISO_UTC}) AS first_viewed_at,
                    DATE_FORMAT(last_viewed_at, ${ISO_UTC}) AS last_viewed_at
               FROM ${jportalSchema()}.po_document_views
              WHERE purchase_order_document_id IN (${ids.map(() => '?').join(',')})
              ORDER BY first_viewed_at, id`,
            ids
        );
    } catch (e) {
        if (isSourceMissing(e)) return out;
        throw e;
    }
    for (const r of rows) {
        const key = Number(r.purchase_order_document_id);
        if (!out.has(key)) out.set(key, []);
        out.get(key).push({
            email: r.user_email,
            firstViewedAt: r.first_viewed_at,
            lastViewedAt: r.last_viewed_at,
            count: Number(r.view_count),
        });
    }
    return out;
}

module.exports = { preapprovePortalUsers, loadPoDocumentViews };
