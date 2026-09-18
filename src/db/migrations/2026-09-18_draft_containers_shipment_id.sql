-- Shipments entity, rollout step 1: draft_containers.shipment_id.
--
-- The LATEST shipment of this draft name. A registry row belongs to a name and
-- is reused forever, a shipment to one generation of that name, so an open
-- draft's shipment is always found by shipments.open_key = 'D:<name>' and this
-- pointer is only followed when no open shipment exists.
--
-- Apply after 2026-09-18_shipments_tables.sql.

SET SESSION lock_wait_timeout = 5;

ALTER TABLE draft_containers
    ADD COLUMN shipment_id INT NULL,
    ADD KEY idx_shipment_id (shipment_id);
