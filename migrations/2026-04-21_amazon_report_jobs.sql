-- Tracks SP-API report lifecycle for the requester/collector split.
--
-- Rows inserted by the requester in status='REQUESTED'.
-- Collector polls status → flips to DONE (with document_id) or FAILED.
-- For pan_eu rows: collector also downloads + parses and caches the
-- resulting ASIN list into result_cache so country units can read it
-- without re-downloading.
-- For active_listings + health rows: collector processes them as a pair
-- per (account, country), then flips both to PROCESSED.

CREATE TABLE IF NOT EXISTS amazon_report_jobs (
    id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    batch_date    DATE            NOT NULL,
    account       VARCHAR(64)     NOT NULL,
    country       VARCHAR(8)      NOT NULL,        -- 'ALL' for account-scoped reports (pan_eu)
    report_type   VARCHAR(32)     NOT NULL,        -- 'pan_eu' | 'active_listings' | 'health'
    marketplace_id VARCHAR(32)    NULL,
    report_id     VARCHAR(64)     NULL,
    document_id   VARCHAR(128)    NULL,
    status        ENUM('REQUESTED','DONE','PROCESSED','FAILED') NOT NULL DEFAULT 'REQUESTED',
    result_cache  LONGTEXT        NULL,            -- JSON blob; used by pan_eu to cache ASIN list
    claim_token   VARCHAR(64)     NULL,
    claimed_at    DATETIME        NULL,
    requested_at  DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    polled_at     DATETIME        NULL,
    completed_at  DATETIME        NULL,
    processed_at  DATETIME        NULL,
    error         TEXT            NULL,
    UNIQUE KEY uniq_batch_job (batch_date, account, country, report_type),
    KEY idx_status_batch (status, batch_date),
    KEY idx_claim (claim_token, claimed_at)
);
