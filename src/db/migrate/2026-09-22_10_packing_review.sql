-- Packing review (src/services/packing-review.js): several packing lists per
-- supplier per container, corrected versions, line sign-offs and a
-- container-level approval.
--
-- packing_lists: a corrected upload replaces its predecessor. The new row
-- carries replaces_id and version = predecessor + 1; the old row gets
-- superseded_by_id and drops out of the live check. Deleting the replacement
-- puts the predecessor back.
ALTER TABLE packing_lists ADD COLUMN version INT NOT NULL DEFAULT 1;
ALTER TABLE packing_lists ADD COLUMN replaces_id INT NULL;
ALTER TABLE packing_lists ADD COLUMN superseded_by_id INT NULL;
ALTER TABLE packing_lists ADD COLUMN superseded_at DATETIME NULL;
ALTER TABLE packing_lists ADD KEY idx_superseded_by (superseded_by_id);

-- A person accepting one line's difference for one supplier in one container.
-- Keyed by the line (product code + PO), not by a document, so it survives a
-- re-read or a replaced packing list. `fingerprint` is the line's status and
-- non-informational differences at signing: if the line changes, the sign-off
-- is shown as stale and the line is outstanding again. Rows are never edited;
-- a re-sign revokes the old row (revoked_reason 'superseded') and adds a new one.
CREATE TABLE IF NOT EXISTS packing_list_sign_offs (
    id INT NOT NULL AUTO_INCREMENT,
    container_kind VARCHAR(16) NOT NULL,
    container_number VARCHAR(100) NULL,
    draft_container_id INT NULL,
    container_name VARCHAR(100) NULL,
    supplier_key VARCHAR(300) NOT NULL,
    line_key VARCHAR(255) NOT NULL,
    jf_code VARCHAR(50) NULL,
    po_number VARCHAR(100) NULL,
    line_status VARCHAR(16) NULL,
    fingerprint CHAR(40) NOT NULL,
    differences_json JSON NULL,
    note VARCHAR(1000) NULL,
    signed_by_email VARCHAR(255) NULL,
    signed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    revoked_at DATETIME NULL,
    revoked_by_email VARCHAR(255) NULL,
    revoked_reason VARCHAR(255) NULL,
    PRIMARY KEY (id),
    KEY idx_container_supplier (container_number, supplier_key),
    KEY idx_draft_supplier (draft_container_id, supplier_key)
);

-- "Packed to our satisfaction", for the whole container, whatever the checks
-- say. snapshot_json is every supplier's check state at approval (verdict,
-- outstanding, document ids, per-line fingerprints) so a later change to the
-- goods or the paperwork is reported as "approved, but changed since". One
-- active (withdrawn_at IS NULL) approval per container.
CREATE TABLE IF NOT EXISTS packing_approvals (
    id INT NOT NULL AUTO_INCREMENT,
    container_kind VARCHAR(16) NOT NULL,
    container_number VARCHAR(100) NULL,
    draft_container_id INT NULL,
    container_name VARCHAR(100) NULL,
    note VARCHAR(1000) NULL,
    snapshot_json JSON NULL,
    approved_by_email VARCHAR(255) NULL,
    approved_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    withdrawn_at DATETIME NULL,
    withdrawn_by_email VARCHAR(255) NULL,
    withdrawn_note VARCHAR(1000) NULL,
    PRIMARY KEY (id),
    KEY idx_container (container_number),
    KEY idx_draft (draft_container_id)
);
