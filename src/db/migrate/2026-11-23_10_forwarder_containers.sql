-- A forwarder's container page on the supplier portal
-- (docs/forwarder-containers.md): everything shipping's own schema needs for
-- it, in one file, so the three slices of the build after it are code only.
--
-- 1. shipments.loading_date: the expected loading date. Nothing held one
--    before (the only loading_date was on a forwarder's quoted option). Lives
--    on the shipment only, as bl_number and booking_ref do.
--
-- 2. shipment_forwarder_documents: a forwarder's bill of lading file, accepted
--    from the portal. Its OWN table: it is not a telex release (that table is
--    one per SUPPLIER, by exact supplier name), not an invoice
--    (shipment_payment_documents: anything there that is not a proof is read
--    and offered to pay), and not a generated document
--    (draft_container_documents). shipments.bl_number is not touched by it.
--      forwarder_upload_id  the portal's forwarder_uploads row it was accepted
--                           from. Unique: accepting twice files once.
--      s3_key / public_url  the PO bucket, forwarder-documents/<random>/<name>:
--                           the random part is made once per upload and never
--                           sent to the portal.
--
-- 3. forwarder_upload_id on shipment_payment_documents and container_photos:
--    the portal upload an accepted invoice or loading photo came from (unique,
--    the same double-accept guard), and forwarder_id on the invoice, so
--    ShipLine can put it in that forwarder's place by id, not only by name.
--
-- 4. freight_container_notices: one row per email asking a forwarder for a
--    container's details (POST /shipments/:id/forwarder-notice).
--      send_key  made by ShipLine's dialog and sent with every attempt.
--                Unique: a key that has a row is reported as it stands and is
--                never emailed again, except one Front refused.
--      status    sending (written before Front is called) | sent | failed
--
-- No FKs, soft deletes, UTC, as the tables around them. tools/migrate.js
-- tolerates "column exists" (1060) and "duplicate key name" (1061), so this is
-- safe to run twice. One clause per ALTER: a failed run can leave half a file
-- applied, and a re-run must finish the rest.
--
-- Rollback (once no deployed code reads them): DROP TABLE
-- shipment_forwarder_documents, freight_container_notices; the added columns
-- are nullable and ignored by older code.

ALTER TABLE shipments ADD COLUMN loading_date DATE NULL AFTER origin_port;

CREATE TABLE IF NOT EXISTS shipment_forwarder_documents (
    id INT NOT NULL AUTO_INCREMENT,
    shipment_id INT NOT NULL,
    shipment_reference VARCHAR(100) NULL,
    forwarder_id INT NOT NULL,
    forwarder_name VARCHAR(255) NULL,
    kind VARCHAR(32) NOT NULL DEFAULT 'bill_of_lading',
    filename VARCHAR(200) NOT NULL,
    s3_key VARCHAR(512) NOT NULL,
    public_url VARCHAR(1024) NOT NULL,
    content_type VARCHAR(100) NOT NULL,
    file_size INT NOT NULL,
    forwarder_upload_id BIGINT NULL,
    uploaded_by_email VARCHAR(255) NULL,
    accepted_by_email VARCHAR(255) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at DATETIME NULL,
    deleted_by_email VARCHAR(255) NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uniq_forwarder_upload (forwarder_upload_id),
    KEY idx_shipment (shipment_id, deleted_at)
);

ALTER TABLE shipment_payment_documents ADD COLUMN forwarder_upload_id BIGINT NULL;

ALTER TABLE shipment_payment_documents ADD COLUMN forwarder_id INT NULL;

ALTER TABLE shipment_payment_documents ADD UNIQUE KEY uniq_spd_forwarder_upload (forwarder_upload_id);

ALTER TABLE container_photos ADD COLUMN forwarder_upload_id BIGINT NULL;

ALTER TABLE container_photos ADD UNIQUE KEY uniq_container_photos_forwarder_upload (forwarder_upload_id);

CREATE TABLE IF NOT EXISTS freight_container_notices (
    id INT NOT NULL AUTO_INCREMENT,
    shipment_id INT NOT NULL,
    shipment_reference VARCHAR(100) NULL,
    forwarder_id INT NOT NULL,
    send_key CHAR(36) NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'sending',
    sent_to JSON NULL,
    subject VARCHAR(500) NULL,
    front_message_uid VARCHAR(255) NULL,
    front_conversation_id VARCHAR(255) NULL,
    sent_by_email VARCHAR(255) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    sent_at DATETIME NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uniq_send_key (send_key),
    KEY idx_shipment_forwarder (shipment_id, forwarder_id)
);
