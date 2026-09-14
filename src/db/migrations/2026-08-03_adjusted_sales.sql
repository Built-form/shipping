-- Manually adjusted sales figures, one live row per (asin, country).
--
-- An ops override of the sales number for an ASIN in a given marketplace, held
-- at the same grain as amazon_stock_country_snapshots (UK/DE/FR/IT/ES/US, plus
-- EU) with 'ALL' reserved for a single cross-market figure. It is a CURRENT
-- value, not a series: changes overwrite the row and the history lives in
-- audit_log under entity_type 'adjusted_sales'.
--
-- Nothing else in the API consumes the figure yet — it is written and read back
-- through /api/v1/adjusted-sales only.
--
-- Created at runtime by orders.js (adjustedSalesSchemaReady). This file
-- documents the canonical shape and lets the schema be applied ahead of a
-- deploy. Idempotent.

CREATE TABLE IF NOT EXISTS adjusted_sales (
    id               INT NOT NULL AUTO_INCREMENT,
    asin             VARCHAR(20) NOT NULL,
    country          VARCHAR(8) NOT NULL,          -- UK/DE/FR/IT/ES/US/EU, or ALL for cross-market
    adjusted_sales   DECIMAL(12,2) NOT NULL,       -- >= 0; 0 means "assume no sales here"
    note             VARCHAR(500) NULL,            -- optional free-text reason for the adjustment
    created_by_email VARCHAR(255) NULL,
    updated_by_email VARCHAR(255) NULL,
    created_at       TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted_at       DATETIME NULL,                -- soft delete; the row keeps its id and audit trail
    PRIMARY KEY (id),
    -- Spans soft-deleted rows too, so re-creating a deleted key REVIVES that
    -- row rather than inserting a second one (handled in the POST/PUT routes).
    UNIQUE KEY uk_asin_country (asin, country),
    KEY idx_asin (asin)                            -- GET /adjusted-sales/:asin
);
