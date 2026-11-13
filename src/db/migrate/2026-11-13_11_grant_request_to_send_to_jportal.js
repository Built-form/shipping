'use strict';

// Let the supplier portal read what its Requests to send page needs
// (docs/request-to-send-portal.md). The portal's db/grants/jportal_app.sql
// carries the same statements; this applies them on deploy so the columns and
// their reader land together. Only where the portal's login exists on this
// server (a GRANT to a missing account is error 1133). GRANT is idempotent, so
// a re-run changes nothing.
//
// 1. Beside each emailed packing list: the CSV's address, and the ETD and port
//    of departure the email gave (2026-11-13_10).
// 2. Whether a request is done ("When a request is done" in the doc): which
//    shipment the document belongs to and how far that shipment has got (its
//    stored stage, and which shipment each order is on: the portal already
//    reads an order's status), and whether a packing list and a commercial
//    invoice are filed for it and under which supplier's name. Existence only.
//
// Column grants add to the ones the portal already holds. Still NOT granted:
// on a send, sent_to, subject, sent_by_email and delivery (who an email went
// to, who sent it and how are staff information); on a packing list or an
// invoice, the file, what was read from it, amounts, notes and the uploader.

const PORTAL_USER = 'jportal_app';
const PORTAL_HOST = '%';

const GRANTS = [
    ['draft_container_documents', 'csv_public_url, shipment_id'],
    ['draft_container_document_sends', 'request_etd, request_port'],
    ['shipments', 'id, reference, stage'],
    ['orders', 'shipment_id'],
    ['packing_lists', 'container_kind, container_number, supplier_name, deleted_at'],
    ['shipment_payment_documents', 'shipment_id, supplier_name, doc_kind, deleted_at'],
];

exports.GRANTS = GRANTS;

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
    log(`${to} granted the Request to send columns on ${schema}`);
};
