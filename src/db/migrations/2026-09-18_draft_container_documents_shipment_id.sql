-- Shipments entity, rollout step 1: draft_container_documents.shipment_id.
--
-- The shipment a quote or forwarder document was generated for, stamped once
-- and never moved to a later generation of the same draft name, so a
-- shipment's documents survive renames and span generations.
--
-- Apply after 2026-09-18_shipments_tables.sql.

SET SESSION lock_wait_timeout = 5;

ALTER TABLE draft_container_documents
    ADD COLUMN shipment_id INT NULL,
    ADD KEY idx_shipment_id (shipment_id);
