-- Shipments entity, rollout step 1: the three new tables. Idempotent.
--
-- A shipment is one physical movement of goods (a sea container, an air
-- consignment, a truck) through PLANNED, DRAFT, BOOKED, IN_TRANSIT, ARRIVED,
-- CLOSED or CANCELLED. Through step 3 it is a derived, rebuildable shadow of
-- the legacy columns and tables (orders.container_number, the draft and planned
-- allocation tables, draft_containers), kept in step by fail-soft hooks in
-- src/handlers/orders.js and src/services/shipment-sync.js.
--
-- Hand-apply BEFORE deploying the code that uses it, in a quiet window:
--   node tools/sql.js -f src/db/migrations/2026-09-18_shipments_tables.sql
-- then the four ..._shipment_id.sql files (one ALTERed table each, so a
-- timed-out ALTER never strands the others behind a Duplicate column error).
-- orders.js creates the same tables at cold start (shipmentsSchemaReady) for a
-- fresh environment. Repo conventions: no FKs, no charset or engine clauses,
-- VARCHAR enums validated in code (src/lib/shipments.js).
--
-- Rules the schema relies on (enforced in code):
--   Unique keys are released on exit. A move to CANCELLED, a soft delete and a
--   merge loser all set reference = NULL and open_key = NULL, and every finder
--   filters deleted_at IS NULL AND merged_into_id IS NULL.
--   reference is TRIM(orders.container_number), compared SQL-side (the key is
--   case-insensitive).

SET SESSION lock_wait_timeout = 5;

CREATE TABLE IF NOT EXISTS shipments (
    id               INT NOT NULL AUTO_INCREMENT,
    reference        VARCHAR(100) NULL,       -- TRIM(orders.container_number), e.g. '308' or '121. Air Freight'
    reference_seq    INT NULL,                -- numeric part when it matches a known sequence, NULL = quarantined
    name             VARCHAR(100) NULL,       -- human label, equals the legacy draft or planned name while those exist
    open_key         VARCHAR(110) NULL,       -- 'D:<name>' or 'P:<name>' while DRAFT or PLANNED, else NULL
    mode             VARCHAR(8) NULL,         -- SEA, AIR or ROAD, NULL = unknown (needs_review)
    mode_source      VARCHAR(16) NOT NULL DEFAULT 'default',  -- the sync never overwrites 'user'
    stage            VARCHAR(16) NOT NULL DEFAULT 'DRAFT',    -- stored stage, written only by shipment-level actions
    tracking_ref     VARCHAR(255) NULL,       -- stored TRIMmed, the only link to containers and air_shipments
    booking_ref      VARCHAR(100) NULL,
    bl_number        VARCHAR(100) NULL,
    forwarder        VARCHAR(255) NULL,
    vessel_name      VARCHAR(255) NULL,
    origin_port      VARCHAR(255) NULL,
    etd              DATE NULL,
    eta              DATE NULL,
    ata              DATE NULL,
    notes            TEXT NULL,
    needs_review     TINYINT(1) NOT NULL DEFAULT 0,
    review_note      VARCHAR(255) NULL,
    origin           VARCHAR(16) NOT NULL DEFAULT 'api',      -- api, legacy_route or backfill
    source_draft_id  INT NULL,                -- draft_containers.id this shipment came from
    merged_into_id   INT NULL,                -- survivor of a merge (this row is then soft-deleted)
    created_by_email VARCHAR(255) NULL,
    created_at       TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    booked_at        DATETIME NULL,
    departed_at      DATETIME NULL,
    arrived_at       DATETIME NULL,
    closed_at        DATETIME NULL,
    cancelled_at     DATETIME NULL,
    cancelled_reason VARCHAR(32) NULL,
    deleted_at       DATETIME NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_reference (reference),
    UNIQUE KEY uk_open_key (open_key),
    KEY idx_tracking_ref (tracking_ref),      -- non-unique: ISO boxes are reused across voyages
    KEY idx_stage_mode (stage, mode),
    KEY idx_mode_seq (mode, reference_seq),
    KEY idx_source_draft (source_draft_id)
);

-- The manifest. PLANNED and DRAFT lines mirror the legacy allocation rows one
-- to one (including rows on soft-deleted orders, which reads hide like legacy
-- does). BOOKED and later lines equal the live orders with shipment_id = id,
-- at orders.quantity.
CREATE TABLE IF NOT EXISTS shipment_lines (
    id               INT NOT NULL AUTO_INCREMENT,
    shipment_id      INT NOT NULL,
    order_id         INT NOT NULL,
    quantity         INT NOT NULL DEFAULT 0,
    created_by_email VARCHAR(255) NULL,
    created_at       TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_shipment_order (shipment_id, order_id),
    KEY idx_order_id (order_id)
);

-- One unresolved row per (site, key_kind, key_value): a repeat failure bumps
-- occurrences. Resolving a row NULLs dedup_key, so the next failure for the
-- same key starts a fresh row.
CREATE TABLE IF NOT EXISTS shipment_sync_failures (
    id               INT NOT NULL AUTO_INCREMENT,
    site             VARCHAR(64) NOT NULL,
    key_kind         VARCHAR(16) NOT NULL,
    key_value        VARCHAR(255) NOT NULL,
    error            VARCHAR(500) NULL,
    dedup_key        VARCHAR(340) NULL,       -- 'site|kind|value' while unresolved, NULL once resolved
    occurrences      INT NOT NULL DEFAULT 1,
    created_at       TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_at     DATETIME NULL,
    resolved_at      DATETIME NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_dedup (dedup_key),
    KEY idx_unresolved (resolved_at, created_at)
);
