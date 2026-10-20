-- Packing lists imported from the supplier portal (/api/v1/pl-submissions —
-- src/services/pl-submission-routes.js, results in src/services/pl-results.js).
--
-- source: 'staff' (uploaded in ShipLine, every row so far) or 'portal'
-- (imported from a supplier's jportal.pl_submissions row). portal_submission_id
-- is that row's id: another schema, so no FK; NULL for staff uploads.
--
-- One submission is imported at most once AT A TIME: delete is a soft delete
-- (packing-list-routes.js DELETE /:id), so a plain unique key on
-- portal_submission_id would block re-importing a submission after its PL was
-- deleted. The key is on a generated column that is NULL once the row is
-- deleted. VIRTUAL, so adding it is an instant column add plus an index
-- build, not a table rebuild. One clause per ALTER, so a re-run skips what
-- landed.

ALTER TABLE packing_lists ADD COLUMN source VARCHAR(16) NOT NULL DEFAULT 'staff';
ALTER TABLE packing_lists ADD COLUMN portal_submission_id BIGINT UNSIGNED NULL;
ALTER TABLE packing_lists ADD COLUMN live_portal_submission_id BIGINT UNSIGNED
    GENERATED ALWAYS AS (IF(deleted_at IS NULL, portal_submission_id, NULL)) VIRTUAL;
ALTER TABLE packing_lists ADD UNIQUE KEY uk_live_portal_submission (live_portal_submission_id);
