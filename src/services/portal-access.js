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

/**
 * Pre-approves `emails` on the portal for the supplier rows named exactly as
 * the PO's supplier (the portal's own access rule, so the PO is visible to
 * them). Per address: no row -> created `active` and unbound ('invited');
 * `pending` -> `active` ('approved'); `active` -> left as is
 * ('already_active'); `rejected` / `disabled` -> left as is ('blocked', no
 * supplier added). Every non-blocked address gets the supplier rows it lacks.
 * One transaction; commits before the email is sent.
 *
 * Returns { status, suppliers, users, skipped } where status is 'ok', 'none'
 * (no supplier recipients), 'no_supplier' (no live supplier has the PO's
 * exact name, so nobody could see it) or 'unavailable' (no portal schema here).
 */
async function preapprovePortalUsers(conn, { recipients, poSupplier, actorEmail, note }) {
    const [staffRows] = await conn.query('SELECT email FROM shipping_allowed_emails');
    const { provision, skipped } = P.splitRecipients(recipients, staffRows.map(r => r.email));
    if (!provision.length) return { status: 'none', suppliers: [], users: [], skipped };

    const [suppliers] = await conn.query(
        `SELECT s.id, s.name
           FROM suppliers s
          WHERE s.deleted_at IS NULL
            AND CONVERT(s.name USING utf8mb4) COLLATE utf8mb4_0900_bin
              = CONVERT(? USING utf8mb4) COLLATE utf8mb4_0900_bin
          ORDER BY s.id`,
        [poSupplier == null ? '' : String(poSupplier)]
    );
    if (!suppliers.length) return { status: 'no_supplier', suppliers: [], users: [], skipped };

    const jp = jportalSchema();
    const actor = actorEmail || 'shipline';
    const decidedNote = String(note || '').slice(0, NOTE_MAX) || null;
    const users = [];
    try {
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
        if (isSourceMissing(e)) return { status: 'unavailable', suppliers: [], users: [], skipped };
        throw e;
    }
    return {
        status: 'ok',
        suppliers: suppliers.map(s => ({ id: s.id, name: s.name })),
        users,
        skipped,
    };
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
