-- Bank-detail verification on a PI's payment row (src/lib/bank-verification.js).
--
-- A PI that came through the supplier portal (purchase_order_invoices
-- .pi_submission_id set) is payable only once someone has verified its bank
-- details, or they match the supplier's last verified ones:
--   bank_verification_status   'verified' | 'needs_verification'; NULL = not
--                              applicable (staff-uploaded and legacy PIs)
--   bank_fingerprint           sha256 of the normalised details now on the row
--                              ('' when none were read)
--   bank_verified_fingerprint  sha256 of the details someone verified
--   bank_verified_json         those details (BankDetails), so the dialog can
--                              show what was verified next to what is there now
--   bank_verified_by / _at / bank_verification_method / _note
--                              who, when, how (phone_known_number,
--                              existing_email_thread, other, matches_previous)
-- Every write of new details recomputes the status from the two fingerprints,
-- so a re-check that reads different details makes the PI unverified again.
-- Gated where money moves: PUT …/invoices/:invoiceId/payment-status
-- (arranged / paid) and supplier-payment lines on a PI (orders.js).
-- One clause per ALTER, so a re-run skips what landed.

ALTER TABLE purchase_order_invoice_payments ADD COLUMN bank_verification_status VARCHAR(24) NULL;
ALTER TABLE purchase_order_invoice_payments ADD COLUMN bank_fingerprint CHAR(64) NULL;
ALTER TABLE purchase_order_invoice_payments ADD COLUMN bank_verified_fingerprint CHAR(64) NULL;
ALTER TABLE purchase_order_invoice_payments ADD COLUMN bank_verified_json JSON NULL;
ALTER TABLE purchase_order_invoice_payments ADD COLUMN bank_verified_by VARCHAR(255) NULL;
ALTER TABLE purchase_order_invoice_payments ADD COLUMN bank_verified_at TIMESTAMP NULL;
ALTER TABLE purchase_order_invoice_payments ADD COLUMN bank_verification_method VARCHAR(32) NULL;
ALTER TABLE purchase_order_invoice_payments ADD COLUMN bank_verification_note VARCHAR(1000) NULL;
