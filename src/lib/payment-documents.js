'use strict';

// Uploaded payment documents (shipment_payment_documents): a supplier's
// invoice, or a proof of payment. Pure rules, no database.

const { DECIDER_ROLES, positiveInt } = require('./pi-submissions');

const email = v => String(v ?? '').trim().toLowerCase();
// shipment-payments.js supplierKey, without the service: the Payments page
// groups by this.
const supplierKey = v => String(v ?? '').trim().replace(/\s+/g, ' ').toLowerCase();
const isObj = v => v !== null && typeof v === 'object' && !Array.isArray(v);

// Who may delete an upload. An admin, always. Otherwise only whoever uploaded
// a proof of payment that no payment uses: Record payment files the proof
// just before it saves the payment, and takes it back when the payment is
// refused — a proof exists only with its payment (user, 2026-10-02: a file
// left as "proof to apply" after a payment that was never recorded, which
// nobody could delete from the page). A proof a payment uses is that
// payment's evidence, and an invoice may have produced a balance record:
// those stay admin-only.
// null when allowed, else { status, code, error }.
function deleteRefusal({ userType, userEmail, doc }) {
    if (userType === 'admin') return null;
    const mine = !!email(userEmail) && email(userEmail) === email(doc.uploaded_by_email);
    const unusedProof = doc.doc_kind === 'remittance' && doc.supplier_payment_id == null && doc.payment_id == null;
    if (mine && unusedProof) return null;
    return {
        status: 403, code: 'ADMIN_ONLY',
        error: 'Admin access required — only an admin, or whoever uploaded a proof that no payment uses, can delete it.',
    };
}

// Where an upload was filed decides how it is read (user, 2026-11-02): on a
// shipment it is that container's invoice; on the supplier alone through
// "Upload QC invoice", a QC invoice; on the balance of a PO's combined /
// repackaged goods (no container will ever bill them), their balance invoice,
// the PO named. A proof of payment on the supplier stays a proof.
// 'shipment' | 'repackaged' | 'qc' | 'supplier'.
function readPlaceOf(doc) {
    if (doc.shipment_id != null) return 'shipment';
    if (doc.doc_kind !== 'balance_invoice') return 'supplier';
    return doc.purchase_order_id != null ? 'repackaged' : 'qc';
}

// An upload naming a PO: the supplier's balance invoice for goods on it that
// are marked Combined / Repackaged, with no shipment. `po` is the live PO row
// ({ id, poNumber }) or null; `repackagedLines` how many of its live lines
// are REPACKAGED. null when allowed, else { status, code, error }.
function repackagedUploadRefusal({ docKind, hasShipment, purchaseOrderId, po, repackagedLines }) {
    const id = Number(purchaseOrderId);
    if (!Number.isInteger(id) || id <= 0) return { status: 400, code: 'BAD_FIELD', error: 'purchaseOrderId must be a purchase order id.' };
    if (hasShipment) return { status: 400, code: 'BAD_FIELD', error: 'An invoice for combined / repackaged goods names a purchase order, not a shipment — send one or the other.' };
    if (docKind !== 'balance_invoice') return { status: 400, code: 'BAD_FIELD', error: 'Only a balance invoice is filed against a purchase order\'s combined / repackaged goods.' };
    if (!po) return { status: 404, code: 'NOT_FOUND', error: `Purchase order ${id} not found.` };
    if (!(repackagedLines > 0)) {
        return { status: 422, code: 'NOT_REPACKAGED', error: `${po.poNumber} has no goods marked Combined / Repackaged — upload its invoice on the container it ships in.` };
    }
    return null;
}

// ── Refiling: a commercial invoice ↔ a QC invoice ──────────────────────────
// The place decides how an invoice is read (readPlaceOf), so one filed in the
// wrong place — a QC invoice imported from the supplier portal on a shipment,
// a goods invoice uploaded through "Upload QC invoice", a commercial invoice
// on the wrong box — is put right by moving it, not by deleting it and
// uploading it again (user, 2026-11-06: "if anyone accidentally assigns it the
// wrong type there is a way to switch easily"). Moving it starts the usual
// read again in its new place; the file, its id and its audit trail stay.
//   commercial  on a booked shipment, filed under a supplier on it
//   qc          on the supplier alone, no shipment
const REFILE_KINDS = ['commercial', 'qc'];
// Who may: the supplier-portal import deciders (admin and standard).
const REFILE_ROLES = DECIDER_ROLES;

// POST /shipment-payment-documents/:id/refile body
// { kind, shipmentId | shipmentReference (commercial only), supplierName?, force? }
// → { value } or { error }
function parseRefileBody(body) {
    const b = isObj(body) ? body : {};
    if (!REFILE_KINDS.includes(b.kind)) return { error: `kind must be commercial or qc.` };
    let shipmentId = null;
    let shipmentReference = null;
    const idGiven = b.shipmentId !== undefined && b.shipmentId !== null && b.shipmentId !== '';
    const refGiven = typeof b.shipmentReference === 'string' && b.shipmentReference.trim();
    if (b.kind === 'qc') {
        if (idGiven || refGiven) return { error: 'A QC invoice is filed on the supplier, not on a shipment: leave shipmentId and shipmentReference out.' };
    } else {
        if (idGiven) {
            shipmentId = positiveInt(b.shipmentId);
            if (!shipmentId) return { error: 'shipmentId must be a positive integer.' };
        }
        if (refGiven) shipmentReference = b.shipmentReference.trim().slice(0, 100);
        if (!shipmentId && !shipmentReference) return { error: 'shipmentId or shipmentReference is required for a commercial invoice.' };
    }
    let supplierName = null;
    if (b.supplierName !== undefined && b.supplierName !== null && b.supplierName !== '') {
        if (typeof b.supplierName !== 'string' || !b.supplierName.trim()) return { error: 'supplierName must be a non-empty string.' };
        supplierName = b.supplierName.trim().slice(0, 255);
    }
    if (b.force !== undefined && b.force !== null && typeof b.force !== 'boolean') return { error: 'force must be true or false.' };
    return { value: { kind: b.kind, shipmentId, shipmentReference, supplierName, force: b.force === true } };
}

/**
 * Why `doc` cannot be refiled as asked. `shipment` is the resolved live
 * shipment ({ id, reference }) for a commercial invoice; `supplierName` the
 * name it will be filed under; `signOffs` how many current payment sign-offs
 * are keyed to it (qc:<id>). null when allowed, else { status, code, error,
 * payload? }.
 */
function refileRefusal({ userType, doc, kind, shipment = null, supplierName = null, signOffs = 0, force = false }) {
    if (!REFILE_ROLES.includes(String(userType))) {
        return { status: 403, code: 'CANNOT_REFILE', error: `Your role (${userType || 'none'}) cannot refile invoices — admin and standard users can.` };
    }
    if (doc.doc_kind !== 'balance_invoice') {
        return { status: 422, code: 'NOT_AN_INVOICE', error: 'Only a balance invoice is refiled; a proof of payment stays with its supplier.' };
    }
    if (readPlaceOf(doc) === 'repackaged') {
        return { status: 422, code: 'NOT_REFILABLE', error: 'This invoice is filed on the combined / repackaged goods of a purchase order; it is neither a shipment\'s nor a QC invoice.' };
    }
    if (doc.supplier_payment_id != null || doc.payment_id != null) {
        return {
            status: 409, code: 'DOCUMENT_IN_USE',
            error: 'This invoice is evidence for a recorded payment, so it stays where it is.',
            payload: { supplierPaymentId: doc.supplier_payment_id ?? null, paymentId: doc.payment_id ?? null },
        };
    }
    if (kind === 'commercial' && !shipment) return { status: 400, code: 'BAD_FIELD', error: 'A commercial invoice is filed on a shipment.' };
    if (!supplierName || !String(supplierName).trim()) return { status: 400, code: 'BAD_FIELD', error: 'The invoice is filed under a supplier: supplierName is required.' };
    const sameSupplier = supplierKey(supplierName) === supplierKey(doc.supplier_name);
    const samePlace = kind === 'qc'
        ? doc.shipment_id == null
        : Number(doc.shipment_id) === Number(shipment.id);
    if (sameSupplier && samePlace) {
        return { status: 409, code: 'NO_CHANGE', error: kind === 'qc' ? 'This is already a QC invoice under that supplier.' : 'This invoice is already on that shipment under that supplier.' };
    }
    if (Number(signOffs) > 0 && !force) {
        return {
            status: 409, code: 'DOCUMENT_SIGNED_OFF',
            error: `${signOffs} sign-off${Number(signOffs) === 1 ? '' : 's'} on this invoice would no longer apply. Send force: true to refile it anyway.`,
            payload: { signOffs: Number(signOffs) },
        };
    }
    return null;
}

// The place the row takes: { shipmentId, shipmentReference, supplierName }.
function refilePlace({ kind, shipment, supplierName }) {
    if (kind === 'qc') return { shipmentId: null, shipmentReference: null, supplierName };
    return { shipmentId: Number(shipment.id), shipmentReference: shipment.reference || String(shipment.id), supplierName };
}

module.exports = {
    deleteRefusal, readPlaceOf, repackagedUploadRefusal,
    REFILE_KINDS, REFILE_ROLES, parseRefileBody, refileRefusal, refilePlace, supplierKey,
};
