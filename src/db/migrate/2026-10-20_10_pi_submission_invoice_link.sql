-- Supplier-portal proforma invoices (/api/v1/pi-submissions —
-- src/services/pi-submission-routes.js, rules in src/lib/pi-submissions.js).
--
-- A PI imported from the supplier portal records which submission it came
-- from: jportal.pi_submissions.id (another schema, so no FK). NULL for every
-- staff-uploaded and legacy PI. Unique, so one submission can never be
-- imported twice: the import's INSERT answers 409 CONFLICT on a duplicate.
-- Also what bank verification keys on: a PI with pi_submission_id is payable
-- only once its bank details are verified (src/lib/bank-verification.js).
-- One clause per ALTER, so a re-run skips what landed.

ALTER TABLE purchase_order_invoices ADD COLUMN pi_submission_id BIGINT UNSIGNED NULL;
ALTER TABLE purchase_order_invoices ADD UNIQUE KEY uk_pi_submission (pi_submission_id);
