-- QC visit schedule links (docs/qc-schedule-link.md): instead of attaching the
-- products' QC documents to the inspector's email, the email carries a
-- no-login link to a page (served by the public qcScheduleApi Lambda,
-- src/handlers/qc-schedule.js) listing every product to inspect with its
-- files: the JFPRO kit documents of each JF code and the visit's own uploads,
-- either for one product (qc_visit_attachments.jf_code) or the whole visit.
-- The page always shows the visit as it is now; changes since the last email
-- to that party are highlighted from that email's lines snapshot.
--
-- qc_visit_links: one per (visit, audience) at a time (enforced in code:
--   src/services/qc-schedule-routes.js). Only audience 'inspector' is issued
--   today. token is 43 base64url characters (32 random bytes): it is the whole
--   permission, unlike the 5-character availability tokens, because it opens
--   files. Never written to the audit log. revoked_at set: the link is dead
--   (rotated by staff, reason 'rotated'). Cancelled / completed visits keep
--   their link; the page says so.
-- qc_visit_attachments.jf_code: the product an upload belongs to
--   (normJfCode, src/lib/qc-visits.js), NULL for the whole visit.
-- qc_visit_sends.lines_snapshot_json: the visit's lines when the email went
--   out ([{ orderId, jfCode, productName, poNumber, lotNumber, supplier,
--   quantityToInspect, sampleMethod }], src/lib/qc-schedule.js linesSnapshot),
--   NULL on emails sent before this existed. kind may now be 'update' too.
-- qc_visit_sends.schedule_link_id: the qc_visit_links row the email carried.
--
-- No foreign keys, no ENUMs, UTC timestamps.

CREATE TABLE IF NOT EXISTS qc_visit_links (
    id                    INT          NOT NULL AUTO_INCREMENT,
    visit_id              INT          NOT NULL,                     -- qc_visits.id
    audience              VARCHAR(16)  NOT NULL DEFAULT 'inspector', -- inspector (factory reserved)
    token                 VARCHAR(64)  NOT NULL,
    created_by_email      VARCHAR(255) NULL,
    created_at            TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    revoked_at            TIMESTAMP    NULL,
    revoke_reason         VARCHAR(32)  NULL,                         -- rotated
    revoked_by_email      VARCHAR(255) NULL,
    first_opened_at       TIMESTAMP    NULL,
    last_opened_at        TIMESTAMP    NULL,
    open_count            INT          NOT NULL DEFAULT 0,
    PRIMARY KEY (id),
    UNIQUE KEY uq_qc_visit_links_token (token),
    KEY idx_qc_visit_links_visit (visit_id, audience)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

ALTER TABLE qc_visit_attachments ADD COLUMN jf_code VARCHAR(64) NULL AFTER visit_id;

ALTER TABLE qc_visit_sends ADD COLUMN lines_snapshot_json JSON NULL AFTER kit_documents_json;

ALTER TABLE qc_visit_sends ADD COLUMN schedule_link_id INT NULL AFTER lines_snapshot_json;
