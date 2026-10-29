-- QC visit emails sent through Front (src/services/qc-visit-comms-routes.js):
-- one row per proposal or cancellation sent to the factory or the inspector.
-- The email receipt (email_receipts, email_type 'qc_visit') links back here
-- with send_table 'qc_visit_sends'. sent_to is the JSON array of addresses.
-- kit_documents_json lists the JFPRO kit documents attached to an inspector
-- proposal (view qc_kit_documents, bytes from the JFPRO documents
-- bucket): [{ documentId, versionId, version, filename }], or NULL when none
-- were attached.
--
-- No foreign keys, no ENUMs, UTC timestamps.

CREATE TABLE IF NOT EXISTS qc_visit_sends (
    id                    INT          NOT NULL AUTO_INCREMENT,
    visit_id              INT          NOT NULL,                     -- qc_visits.id
    audience              VARCHAR(16)  NOT NULL,                     -- factory | inspector
    kind                  VARCHAR(16)  NOT NULL DEFAULT 'proposal',  -- proposal | cancel
    sent_to               JSON         NOT NULL,
    kit_documents_json    JSON         NULL,                         -- [{documentId, versionId, version, filename}] (JFPRO)
    subject               VARCHAR(500) NULL,
    front_message_uid     VARCHAR(255) NULL,
    front_conversation_id VARCHAR(255) NULL,
    sent_by_email         VARCHAR(255) NULL,
    sent_at               TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY idx_qc_visit_sends_visit (visit_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
