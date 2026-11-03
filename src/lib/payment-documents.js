'use strict';

// Uploaded payment documents (shipment_payment_documents): a supplier's
// invoice, or a proof of payment. Pure rules, no database.

const email = v => String(v ?? '').trim().toLowerCase();

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

module.exports = { deleteRefusal, readPlaceOf, repackagedUploadRefusal };
