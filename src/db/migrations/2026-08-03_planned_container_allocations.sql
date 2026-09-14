-- Planned containers — an independent twin of draft_container_allocations.
--
-- Same planning shape as draft containers: a user-named container holding
-- order lines with per-line allocated quantities. Deliberately allocations
-- only — planned containers have no documents/sends tables, so there is no
-- PDF generation or forwarder email side to this feature.
--
-- Created at runtime by orders.js (plannedContainerAllocationsSchemaReady) and
-- by carton-scan.js (whose order reads go through ORDER_SELECT, which
-- aggregates planned container names). This file documents the canonical shape
-- and lets the schema be applied ahead of a deploy. Idempotent.

CREATE TABLE IF NOT EXISTS planned_container_allocations (
    id                     INT NOT NULL AUTO_INCREMENT,
    order_id               INT NOT NULL,                  -- orders.id (no hard FK, matches the rest of the schema)
    planned_container_name VARCHAR(100) NOT NULL,         -- free-text, user-named; the container is implicit in these rows
    allocated              INT NOT NULL DEFAULT 0,        -- units of the order assigned to this container
    created_at             TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at             TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    -- One line per order per container; changing the quantity is an UPDATE.
    UNIQUE KEY uk_order_planned (order_id, planned_container_name),
    KEY idx_planned_name (planned_container_name),        -- list one container's lines
    KEY idx_order_id (order_id)                           -- list an order's containers / ORDER_SELECT subquery
);
