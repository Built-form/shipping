-- Shipments entity, rollout step 1: orders.shipment_id.
--
-- The booked shipment an order travels in. Written ONLY by the shadow sync
-- (src/services/shipment-sync.js) and the backfill, never by a client: it is
-- deliberately not in UPDATABLE_FIELDS. Every shadow UPDATE of it pins
-- last_updated, so no order looks edited. Soft-deleted orders keep theirs as
-- history.
--
-- Apply after 2026-09-18_shipments_tables.sql. If the ALTER times out on the
-- metadata lock, re-run this file alone in a quieter moment.

SET SESSION lock_wait_timeout = 5;

ALTER TABLE orders
    ADD COLUMN shipment_id INT NULL,
    ADD KEY idx_shipment_id (shipment_id);
