'use strict';

// Let the supplier portal tell when a Request to send is done
// (docs/request-to-send-portal.md, "When a request is done"): which shipment
// the document belongs to and how far that shipment has got (its stored
// stage, and which shipment each order is on: the portal already reads an
// order's status), and whether a packing list and a commercial invoice are
// filed for it and under which supplier's name. Existence only: NOT the files,
// what was read from them, amounts, notes or the uploader.
//
// A file of its own, after 2026-11-13_11 (the CSV address, ETD and port), which
// test had already applied when these were added: an applied migration is
// never run again. The portal's db/grants/jportal_app.sql carries the same
// statements, and its own deploy applies that file, so on a stage where the
// portal was deployed since, this changes nothing. Only where the portal's
// login exists (a GRANT to a missing account is error 1133). GRANT is
// idempotent, so a re-run changes nothing.

const PORTAL_USER = 'jportal_app';
const PORTAL_HOST = '%';

const GRANTS = [
    ['draft_container_documents', 'shipment_id'],
    ['shipments', 'id, reference, stage'],
    ['orders', 'shipment_id'],
    ['packing_lists', 'container_kind, container_number, supplier_name, deleted_at'],
    ['shipment_payment_documents', 'shipment_id, supplier_name, doc_kind, deleted_at'],
];

exports.up = async (conn, { log }) => {
    const [users] = await conn.query(
        'SELECT 1 FROM mysql.user WHERE user = ? AND host = ?',
        [PORTAL_USER, PORTAL_HOST]
    );
    if (!users.length) {
        log(`no '${PORTAL_USER}'@'${PORTAL_HOST}' on this server; no grants to apply`);
        return;
    }
    const [[{ schema }]] = await conn.query('SELECT DATABASE() AS `schema`');
    const to = `'${PORTAL_USER}'@'${PORTAL_HOST}'`;
    for (const [table, columns] of GRANTS) {
        await conn.query(`GRANT SELECT (${columns}) ON \`${schema}\`.${table} TO ${to}`);
    }
    log(`${to} granted the Request to send progress columns on ${schema}`);
};
