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

module.exports = { deleteRefusal };
