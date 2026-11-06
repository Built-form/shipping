'use strict';

// POST /api/v1/shipment-payment-documents/:id/refile — move a balance invoice
// between the two places an invoice is read in:
//   commercial  on a booked shipment, filed under a supplier on it: read
//               against the box (does it belong here, does it agree with it);
//   qc          on the supplier alone, no shipment: a QC invoice, every line
//               read as a QC line (the Payments page's "Upload QC invoice").
// The place decides the read (lib/payment-documents.js readPlaceOf,
// shipment-payment-extract.js), so an invoice filed in the wrong place — a QC
// invoice imported from the supplier portal on a shipment, a goods invoice
// uploaded through "Upload QC invoice", a commercial invoice on the wrong box
// — is put right by moving it (user, 2026-11-06: "if anyone accidentally
// assigns it the wrong type there is a way to switch easily"), never by
// deleting it and uploading it again. The file, the document id and its audit
// trail stay; the read runs again in the new place, after the response, as a
// staff upload's does. Rules: lib/payment-documents.js (parseRefileBody,
// refileRefusal, refilePlace). Frontend guide: docs/shipment-payments-frontend.md.
//
// In one transaction: lock the row; for a commercial invoice resolve the
// shipment (SHIPMENT_NOT_FOUND, MERGED, NOT_BOOKED — the staff upload's
// rules) and the name it is filed under (the body's supplierName when it is
// on the shipment, else the document's own name when that is on the
// shipment, else 422 SUPPLIER_NOT_ON_SHIPMENT with who is, unless force); for
// a QC invoice the body's supplierName or the document's own; count the
// current sign-offs keyed to the document (payment_reviews qc:<id>: refused
// without force, DOCUMENT_SIGNED_OFF — two people's work would no longer
// apply); the duplicate guard in the new place (DUPLICATE_DOCUMENT unless
// force); UPDATE the row's place and reset its read; audit 'refile'; and, for
// a document imported from the supplier portal, move the ci_submissions row's
// confirmed_* snapshot with it (best effort: the portal schema may be
// unreachable on a database without the grants, which is logged, never a
// failed refile).
//
// Registered from orders.js next to the other document routes; authed through
// the greedy /api/v1/{proxy+} route.

const L = require('../lib/payment-documents');
const S = require('../lib/shipments');
const SP = require('./shipment-payments');
const C = require('./ci-check');
const { suppliersOnShipment } = require('./ci-submission-routes');
const { jportalSchema } = require('../lib/jportal');

const isDeadlock = err => Boolean(err) && (err.code === 'ER_LOCK_DEADLOCK' || err.errno === 1213);
const isLockTimeout = err => Boolean(err) && (err.code === 'ER_LOCK_WAIT_TIMEOUT' || err.errno === 1205);
const CONFLICT = { status: 409, code: 'CONFLICT', error: 'Someone changed this document at the same moment; reload and try again.' };

function registerPaymentDocumentRefileRoutes(app, deps) {
    const {
        withConnection, pool, ready, recordAudit, shipmentPaymentDocumentRowToJson, runShipmentPaymentExtraction, log,
    } = deps;

    const refuse = (res, r) => res.status(r.status).json({ error: r.error, code: r.code, ...(r.payload || r.extra || {}) });

    // The shipment a commercial invoice goes on: live and booked, with the POs
    // on board (the supplier it is filed under comes from them).
    const resolveShipment = async (conn, { shipmentId, shipmentReference }) => {
        const r = await SP.resolveShipment(conn, { id: shipmentId, reference: shipmentReference });
        if (r.notFound) return { fail: { status: 404, code: 'SHIPMENT_NOT_FOUND', error: 'Shipment not found.' } };
        if (r.merged) {
            return { fail: { status: 409, code: 'MERGED', error: `That shipment was merged into ${r.mergedIntoId}.`, payload: { mergedIntoId: r.mergedIntoId } } };
        }
        if (!S.isBookedStage(r.row.stage)) {
            return { fail: { status: 422, code: 'NOT_BOOKED', error: 'Invoices are filed on a shipment once it is booked.', payload: { stage: r.row.stage } } };
        }
        const cand = await C.loadShipmentCandidate(conn, r.row.id);
        if (!cand) return { fail: { status: 404, code: 'SHIPMENT_NOT_FOUND', error: 'Shipment not found.' } };
        return { shipment: cand.shipment, memberPos: cand.memberPos };
    };

    // Current sign-offs keyed to the document (a QC invoice's payment key).
    const signOffsOf = async (conn, documentId) => {
        try {
            const [[n]] = await conn.query(
                `SELECT COUNT(*) AS n FROM payment_reviews WHERE payment_key = ? AND revoked_at IS NULL`, [`qc:${documentId}`]
            );
            return Number(n.n) || 0;
        } catch (e) {
            if (e && e.errno === 1146) return 0; // no sign-offs table yet
            throw e;
        }
    };

    // The same file (name and size) already in the new place, other than this document.
    const duplicateOf = async (conn, doc, place) => {
        const [dupes] = await conn.query(
            `SELECT id FROM shipment_payment_documents
              WHERE deleted_at IS NULL AND id <> ? AND filename = ? AND file_size = ?
                AND ${place.shipmentId != null ? 'shipment_id = ?' : 'shipment_id IS NULL AND supplier_key = ?'}
              ORDER BY id LIMIT 1`,
            [doc.id, doc.filename, doc.file_size, place.shipmentId != null ? place.shipmentId : SP.supplierKey(place.supplierName)]
        );
        return dupes.length ? Number(dupes[0].id) : null;
    };

    // fn(conn) in a transaction; a returned { fail } rolls back. One deadlock retry.
    const inTransaction = async (fn) => {
        const once = () => withConnection(async (conn) => {
            await conn.beginTransaction();
            try {
                const out = await fn(conn);
                if (out && out.fail) await conn.rollback();
                else await conn.commit();
                return out;
            } catch (e) {
                try { await conn.rollback(); } catch (rollbackError) { log.warn('[refile] rollback failed', rollbackError.message); }
                throw e;
            }
        });
        try {
            return await once();
        } catch (e) {
            if (isLockTimeout(e)) return { fail: CONFLICT };
            if (!isDeadlock(e)) throw e;
        }
        try {
            return await once();
        } catch (e) {
            if (isDeadlock(e) || isLockTimeout(e)) return { fail: CONFLICT };
            throw e;
        }
    };

    const startExtraction = (req, documentId) => {
        if (req.lambdaContext) req.lambdaContext.callbackWaitsForEmptyEventLoop = true;
        const userEmail = req.userEmail || null;
        (async () => {
            try {
                const out = await runShipmentPaymentExtraction(pool, { documentId, userEmail });
                if (out && out.failed) log.warn('[shipment-payment-documents/refile] extraction failed', { documentId, code: out.failed });
            } catch (e) {
                log.warn('[shipment-payment-documents/refile] extraction threw', { documentId, error: e.message });
            }
        })();
    };

    // POST /api/v1/shipment-payment-documents/:id/refile
    // Body: { kind: 'commercial' | 'qc', shipmentId | shipmentReference (commercial), supplierName?, force? }
    // 202 { document, refiled: { from: { kind, shipmentId, shipmentReference, supplierName }, to: { … } } }
    // 400 BAD_FIELD · 403 CANNOT_REFILE · 404 NOT_FOUND | SHIPMENT_NOT_FOUND
    // 409 NO_CHANGE | DOCUMENT_IN_USE | DOCUMENT_SIGNED_OFF | DUPLICATE_DOCUMENT | MERGED | CONFLICT
    // 422 NOT_AN_INVOICE | NOT_REFILABLE | NOT_BOOKED | SUPPLIER_NOT_ON_SHIPMENT
    app.post('/api/v1/shipment-payment-documents/:id(\\d+)/refile', async (req, res) => {
        try {
            const id = Number(req.params.id);
            const parsed = L.parseRefileBody(req.body);
            if (parsed.error) return refuse(res, { status: 400, code: 'BAD_FIELD', error: parsed.error });
            const body = parsed.value;
            if (!L.REFILE_ROLES.includes(String(req.userType))) {
                return refuse(res, L.refileRefusal({ userType: req.userType, doc: { doc_kind: 'balance_invoice' }, kind: body.kind, shipment: { id: 0 }, supplierName: 'x' }));
            }
            await ready();

            const out = await inTransaction(async (conn) => {
                const [rows] = await conn.query(`SELECT * FROM shipment_payment_documents WHERE id = ? AND deleted_at IS NULL FOR UPDATE`, [id]);
                const doc = rows[0];
                if (!doc) return { fail: { status: 404, code: 'NOT_FOUND', error: `Document ${id} not found.` } };

                let shipment = null;
                let supplierName = body.supplierName || doc.supplier_name || null;
                let onShipment = null;
                if (body.kind === 'commercial') {
                    const r = await resolveShipment(conn, body);
                    if (r.fail) return r;
                    shipment = r.shipment;
                    const suppliers = suppliersOnShipment(r.memberPos, doc.supplier_name ? [doc.supplier_name] : []);
                    const find = name => suppliers.find(s => SP.supplierKey(s.supplierName) === SP.supplierKey(name));
                    const wanted = body.supplierName || doc.supplier_name || null;
                    const there = wanted ? find(wanted) : null;
                    if (there) {
                        supplierName = there.supplierName; // the PO's own spelling
                        onShipment = true;
                    } else if (body.force && wanted) {
                        supplierName = wanted;
                        onShipment = false;
                    } else {
                        return { fail: {
                            status: 422, code: 'SUPPLIER_NOT_ON_SHIPMENT',
                            error: `${wanted || 'That supplier'} has no purchase orders on ${shipment.reference || shipment.id}. Pick a supplier on it, or send force: true to file it under that name anyway.`,
                            payload: { suppliers },
                        } };
                    }
                }

                const signOffs = await signOffsOf(conn, id);
                const refusal = L.refileRefusal({ userType: req.userType, doc, kind: body.kind, shipment, supplierName, signOffs, force: body.force });
                if (refusal) return { fail: refusal };

                const place = L.refilePlace({ kind: body.kind, shipment, supplierName });
                const duplicateId = await duplicateOf(conn, doc, place);
                if (duplicateId && !body.force) {
                    return { fail: {
                        status: 409, code: 'DUPLICATE_DOCUMENT',
                        error: `${doc.filename} is already ${place.shipmentId != null ? 'on that shipment' : 'a QC invoice of that supplier'}.`,
                        payload: { existingId: duplicateId },
                    } };
                }

                const from = {
                    kind: L.readPlaceOf(doc) === 'qc' ? 'qc' : 'commercial',
                    shipmentId: doc.shipment_id ?? null, shipmentReference: doc.shipment_reference ?? null, supplierName: doc.supplier_name ?? null,
                };
                const to = { kind: body.kind, ...place };
                // The read is reset: its old fit and check were against the old place.
                await conn.query(
                    `UPDATE shipment_payment_documents
                        SET shipment_id = ?, shipment_reference = ?, supplier_name = ?, supplier_key = ?,
                            extract_status = 'processing', extract_error = NULL
                      WHERE id = ?`,
                    [place.shipmentId, place.shipmentReference, place.supplierName, SP.supplierKey(place.supplierName), id]
                );
                await recordAudit(conn, {
                    entityType: 'shipment_payment_document', entityId: id, action: 'refile',
                    before: from,
                    after: { ...to, onShipment, force: body.force, signOffs, duplicateOf: duplicateId },
                    userEmail: req.userEmail,
                });

                // A document imported from the supplier portal: the portal row's
                // "filed on" snapshot follows the document (the supplier sees it).
                let portalRows = 0;
                try {
                    const [upd] = await conn.query(
                        `UPDATE ${jportalSchema()}.ci_submissions
                            SET confirmed_shipment_id = ?, confirmed_shipment_reference = ?, row_version = row_version + 1
                          WHERE document_id = ? AND status = 'imported'`,
                        [place.shipmentId, place.shipmentReference == null ? null : String(place.shipmentReference).slice(0, 100), id]
                    );
                    portalRows = upd.affectedRows || 0;
                } catch (e) {
                    log.warn('[shipment-payment-documents/refile] portal row not updated', { id, error: e.message });
                }

                const [readback] = await conn.query(`SELECT * FROM shipment_payment_documents WHERE id = ?`, [id]);
                return { document: shipmentPaymentDocumentRowToJson(readback[0]), refiled: { from, to }, portalRows };
            });
            if (out.fail) return refuse(res, out.fail);

            res.status(202).json({ document: out.document, refiled: out.refiled, portalRows: out.portalRows });
            startExtraction(req, id);
        } catch (error) {
            log.error('[POST /shipment-payment-documents/:id/refile]', error);
            if (!res.headersSent) res.status(500).json({ error: 'An internal error occurred.' });
        }
    });
}

module.exports = { registerPaymentDocumentRefileRoutes };
