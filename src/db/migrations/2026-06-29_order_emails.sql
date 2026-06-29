-- Front email → order index (feature: frontEmailIndex Lambda).
-- Created at runtime by ensureSchema() in src/services/front-email-index.js and
-- (order_emails only) by orders.js; this file documents the canonical shape and
-- lets the schema be applied manually. All idempotent.

-- The searchable link table: one row per (conversation, order). Append-only
-- facts — a link is never auto-removed even if the order later changes.
CREATE TABLE IF NOT EXISTS order_emails (
    id BIGINT NOT NULL AUTO_INCREMENT,
    order_id          BIGINT NOT NULL,                  -- orders.id (no hard FK, matches the rest of the schema)
    conversation_id   VARCHAR(100) NOT NULL,            -- Front conversation id (cnv_…)
    subject           VARCHAR(512) NULL,
    preview           VARCHAR(1000) NULL,               -- snippet of the thread text
    from_email        VARCHAR(255) NULL,                -- sender of the latest message
    participants      JSON NULL,                        -- [emails] seen on the thread
    direction         VARCHAR(16) NULL,                 -- inbound | outbound (latest message)
    front_url         VARCHAR(255) NULL,                -- app.frontapp.com/open/<id>
    match_tier        VARCHAR(16) NOT NULL DEFAULT 'product', -- strong | batch | product
    match_basis       JSON NULL,                        -- [{type:'container'|'po'|'awb'|'jf_code'|'asin'|'lot'|'supplier'|'product', value}]
    source            VARCHAR(16) NOT NULL DEFAULT 'rule',    -- rule | gemini
    confidence        DECIMAL(4,3) NULL,                -- gemini-fallback confidence (rule matches: NULL)
    -- NB: the full body is NOT stored here (a thread links to many orders). It
    -- lives ONCE on front_email_index.body_full; the API JOINs it by conversation_id.
    message_count     INT NOT NULL DEFAULT 0,
    first_message_at  DATETIME NULL,
    last_message_at   DATETIME NULL,
    created_at        TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_convo_order (conversation_id, order_id),
    KEY idx_order_recent (order_id, last_message_at),
    KEY idx_convo (conversation_id)
);

-- Per-conversation provenance + dedup ledger, and the single home for the FULL
-- thread body. Lets re-runs skip threads with no new activity, records the
-- no-identifier residual (ambiguous / gemini-used), and FULLTEXT-indexes the body
-- for fast keyword search.
CREATE TABLE IF NOT EXISTS front_email_index (
    id BIGINT NOT NULL AUTO_INCREMENT,
    conversation_id   VARCHAR(100) NOT NULL,
    subject           VARCHAR(512) NULL,
    from_email        VARCHAR(255) NULL,
    front_url         VARCHAR(255) NULL,
    body_full         MEDIUMTEXT NULL,                  -- the FULL thread text (every message), stored ONCE per conversation
    message_count     INT NOT NULL DEFAULT 0,
    last_message_at   DATETIME NULL,
    last_message_ms   BIGINT NULL,                      -- epoch ms of the last message; drives the dedup-skip (TZ-independent)
    matched_order_count INT NOT NULL DEFAULT 0,
    order_ids         JSON NULL,
    ambiguous         TINYINT(1) NOT NULL DEFAULT 0,
    gemini_used       TINYINT(1) NOT NULL DEFAULT 0,
    gemini_confidence DECIMAL(4,3) NULL,
    signals           JSON NULL,
    scanned_at        TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_convo (conversation_id),
    KEY idx_last_msg (last_message_at),
    FULLTEXT KEY ftx_body (subject, body_full)          -- fast `MATCH(subject, body_full) AGAINST(...)` keyword search
);

-- Single-row incremental watermark (epoch SECONDS of the last fully-drained run).
CREATE TABLE IF NOT EXISTS front_email_scan_state (
    id TINYINT NOT NULL,
    last_run_ts BIGINT NULL,
    updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);
