-- Shipments entity: shipment_id on the tables that point at a shipment (was
-- cold-start DDL in src/services/shipment-sync.js, and the four
-- src/db/migrations/2026-09-18_*_shipment_id.sql files).
--
-- orders.shipment_id is set only by the sync primitives, never by clients (it
-- is deliberately not in UPDATABLE_FIELDS). One clause per ALTER, so a re-run
-- after a lock-wait timeout skips what landed and finishes the rest.

ALTER TABLE orders ADD COLUMN shipment_id INT NULL;
ALTER TABLE orders ADD KEY idx_shipment_id (shipment_id);

ALTER TABLE draft_containers ADD COLUMN shipment_id INT NULL;
ALTER TABLE draft_containers ADD KEY idx_shipment_id (shipment_id);

ALTER TABLE draft_container_documents ADD COLUMN shipment_id INT NULL;
ALTER TABLE draft_container_documents ADD KEY idx_shipment_id (shipment_id);

ALTER TABLE quality_assurance_documents ADD COLUMN shipment_id INT NULL;
ALTER TABLE quality_assurance_documents ADD KEY idx_shipment_id (shipment_id);
