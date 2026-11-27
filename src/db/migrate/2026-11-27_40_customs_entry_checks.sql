-- Customs-entry checks (docs/customs-entry-check.md): one row per AI report on a
-- customs entry or its draft, saying whether it matches what we have on file
-- for the container (src/services/customs-entry-check.js).
--
-- A row is ONE RUN on ONE FILE. Running it again makes a new row: the reports
-- of a document are its history, and the newest is the one shown.
--   source      where the file is:
--                 'upload'    a forwarder's upload on the supplier portal, still
--                             in the portal's bucket (forwarder_upload_id). The
--                             same row stands once the upload is accepted and
--                             filed: the filed document names its upload.
--                 'document'  a filed shipment_forwarder_documents row that came
--                             from no upload (forwarder_document_id)
--                 'staff'     a file one of our team uploaded to be checked
--   bucket      'portal' (PI_UPLOADS_BUCKET) or 'ours' (PO_DOCS_BUCKET): where
--               s3_key is read from.
--   status      processing | succeeded | failed
--   verdict     match | same_result | incomplete | differences | no_basis |
--               unreadable (src/lib/customs-entry-compare.js)
--   basis_kind  what it was compared with: 'sheet' (the clearance instructions
--               sent, clearance_id), 'live' (the container's lines, JFPRO and
--               the packing list as they were at the run) or 'none'
--   extract_json  the entry as the model read it, untouched
--   report_json   the report as it was shown
--
-- No FKs, UTC, as the tables around it. CREATE TABLE IF NOT EXISTS: safe to run
-- twice.
--
-- Rollback (once no deployed code reads it): DROP TABLE customs_entry_checks.
-- Nothing else refers to it.

CREATE TABLE IF NOT EXISTS customs_entry_checks (
    id INT NOT NULL AUTO_INCREMENT,
    shipment_id INT NOT NULL,
    source VARCHAR(16) NOT NULL,
    forwarder_upload_id INT NULL,
    forwarder_document_id INT NULL,
    kind VARCHAR(32) NULL,
    filename VARCHAR(200) NOT NULL,
    bucket VARCHAR(16) NOT NULL,
    s3_key VARCHAR(500) NOT NULL,
    content_type VARCHAR(100) NULL,
    file_size INT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'processing',
    verdict VARCHAR(16) NULL,
    mrn VARCHAR(32) NULL,
    basis_kind VARCHAR(16) NULL,
    clearance_id INT NULL,
    finding_count INT NULL,
    model_read VARCHAR(64) NULL,
    model_judge VARCHAR(64) NULL,
    extract_json JSON NULL,
    report_json JSON NULL,
    error_code VARCHAR(40) NULL,
    error_message TEXT NULL,
    requested_by_email VARCHAR(255) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    analyzed_at DATETIME NULL,
    PRIMARY KEY (id),
    KEY idx_customs_check_shipment (shipment_id, id),
    KEY idx_customs_check_upload (forwarder_upload_id, id),
    KEY idx_customs_check_document (forwarder_document_id, id)
);
