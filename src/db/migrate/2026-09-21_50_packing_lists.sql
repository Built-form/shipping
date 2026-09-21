-- Packing-list check: one row per uploaded supplier packing list, against one
-- container and one supplier group (see src/services/packing-list-check.js
-- for the 's:<id>' / 'n:<name>' keys). The container is either
--   container_kind 'booked': container_number = orders.container_number, or
--   container_kind 'draft':  draft_container_id = draft_containers.id (stable
--                            across renames), container_name = the draft name
--                            at upload, container_number = the number it reserves.
-- extract_json is what Gemini read; comparison_json is the comparison as it
-- stood at analysis time (GET /packing-lists/:id recomputes it live).
-- s3_key is unique: one uploaded object backs one packing list (the upload
-- route also checks, to answer 409 DUPLICATE_UPLOAD rather than fail).
-- Routes: src/services/packing-list-routes.js (no cold-start DDL).

CREATE TABLE IF NOT EXISTS packing_lists (
    id INT NOT NULL AUTO_INCREMENT,
    container_kind VARCHAR(16) NOT NULL DEFAULT 'booked',
    container_number VARCHAR(100) NULL,
    draft_container_id INT NULL,
    container_name VARCHAR(100) NULL,
    supplier_key VARCHAR(300) NOT NULL,
    supplier_name VARCHAR(255) NULL,
    filename VARCHAR(200) NOT NULL,
    s3_key VARCHAR(500) NOT NULL,
    content_type VARCHAR(100) NULL,
    file_size INT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'processing',
    model_used VARCHAR(64) NULL,
    invoice_number VARCHAR(100) NULL,
    document_date VARCHAR(32) NULL,
    supplier_printed VARCHAR(255) NULL,
    row_count INT NULL,
    discrepancy_count INT NULL,
    verdict VARCHAR(16) NULL,
    extract_json JSON NULL,
    comparison_json JSON NULL,
    error_message TEXT NULL,
    uploaded_by_email VARCHAR(255) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    analyzed_at DATETIME NULL,
    deleted_at DATETIME NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_s3_key (s3_key),
    KEY idx_container (container_number),
    KEY idx_draft_container (draft_container_id),
    KEY idx_created_at (created_at)
);
