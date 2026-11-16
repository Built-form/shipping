-- shipment_telex_releases: the telex-released bill of lading a supplier sends
-- once a container's balance is paid (docs/telex-releases.md;
-- src/lib/telex-releases.js, src/services/telex-release-routes.js). One per
-- supplier per shipment, as a packing list or an invoice is; more than one row
-- is allowed (a corrected copy), the newest is the one shown first.
--
-- Its OWN table, deliberately not a doc_kind of shipment_payment_documents:
-- that table's readers treat anything that is not a proof of payment as an
-- invoice (read, checked, offered to pay), so a telex release there would have
-- to be kept out of some thirty places, and out of every one written later.
--
--   source                'staff' (filed from ShipLine) or 'portal' (uploaded by
--                         the supplier on the portal and accepted here).
--   portal_submission_id  the portal's telex_submissions row it was accepted
--                         from. Unique: accepting twice files once.
--   s3_key / public_url   the PO bucket, telex-releases/<uuid>/<name>.
--
-- Soft delete, no FKs, as the tables around it.

CREATE TABLE IF NOT EXISTS shipment_telex_releases (
    id INT NOT NULL AUTO_INCREMENT,
    shipment_id INT NOT NULL,
    shipment_reference VARCHAR(100) NULL,
    supplier_name VARCHAR(255) NOT NULL,
    filename VARCHAR(200) NOT NULL,
    s3_key VARCHAR(512) NOT NULL,
    public_url VARCHAR(1024) NOT NULL,
    content_type VARCHAR(100) NOT NULL,
    file_size INT NOT NULL,
    source VARCHAR(16) NOT NULL DEFAULT 'staff',
    portal_submission_id BIGINT NULL,
    notes VARCHAR(500) NULL,
    uploaded_by_email VARCHAR(255) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at DATETIME NULL,
    deleted_by_email VARCHAR(255) NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uniq_portal_submission (portal_submission_id),
    KEY idx_shipment (shipment_id, deleted_at)
);
