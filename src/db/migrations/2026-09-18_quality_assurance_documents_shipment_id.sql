-- Shipments entity, rollout step 1: quality_assurance_documents.shipment_id.
--
-- Stamped from the draft the QA sheet was tagged with. Untagged QA sheets
-- belong to no draft and stay NULL by design.
--
-- Apply after 2026-09-18_shipments_tables.sql.

SET SESSION lock_wait_timeout = 5;

ALTER TABLE quality_assurance_documents
    ADD COLUMN shipment_id INT NULL,
    ADD KEY idx_shipment_id (shipment_id);
