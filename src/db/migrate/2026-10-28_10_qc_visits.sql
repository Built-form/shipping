-- QC Visits (plan: ShipLine docs/plans/qc-visits.md; routes in
-- src/services/qc-visit-routes.js, pure rules in src/lib/qc-visits.js):
-- arranging a factory QC visit in China. Which PO lines, how many units, at
-- which factory, by which inspector, on which day, and who has confirmed the
-- slot. What the inspector finds stays in the QC reports flow.
--
-- qc_factories: one or more sites per supplier (suppliers in JFPRO carry no
--   address). Keyed by the free-text supplier name like orders.supplier;
--   supplier_code is a snapshot of suppliers.code so a renamed supplier can be
--   re-homed. (supplier_name, label) is unique among LIVE rows, enforced in
--   code: soft delete makes a UNIQUE key wrong.
-- qc_inspectors: internal staff or agencies (seeded with AQI by
--   2026-10-28_11_qc_inspectors_seed.js). Deactivated, never deleted.
-- qc_visits: the visit. ref is QCV_<id, 5 digits>, set right after the insert.
--   The factory_* and inspector_* columns are snapshots copied when the id is
--   set, editable afterwards. visit_date and window_* are China wall-clock
--   (tz). updated_at has millisecond precision: it is the optimistic
--   concurrency token every write compares (409 STALE).
--   *_confirmed_at are denormalised from qc_visit_confirmations.
--   wasted_trip is derived from cancel_category (factory_not_ready,
--   goods_not_at_site).
-- qc_visit_lines: one order per visit; po_quantity_snapshot is
--   orders.quantity when the line was last written (po_shrank warning).
-- qc_visit_moves: goods moved from another supplier so one stop covers them.
-- qc_visit_confirmations: the log of proposals, confirmations, declines and
--   counter-proposals per party; the latest non-proposed row drives state.
-- qc_visit_attachments: free-form files on a visit, PO docs bucket under
--   qc-visits/<uuid>/<filename>.
--
-- Status, kind, channel and method columns are VARCHAR validated in
-- src/lib/qc-visits.js. No foreign keys, no ENUMs, UTC timestamps.

CREATE TABLE IF NOT EXISTS qc_factories (
    id                    INT          NOT NULL AUTO_INCREMENT,
    supplier_name         VARCHAR(255) NOT NULL,
    supplier_code         VARCHAR(64)  NULL,
    label                 VARCHAR(100) NOT NULL,
    address               VARCHAR(500) NULL,
    city                  VARCHAR(100) NULL,
    province              VARCHAR(100) NULL,
    contact_name          VARCHAR(255) NULL,
    contact_title         VARCHAR(255) NULL,
    contact_email         VARCHAR(255) NULL,
    contact_wechat        VARCHAR(255) NULL,
    contact_phone         VARCHAR(255) NULL,
    is_default            TINYINT(1)   NOT NULL DEFAULT 0,
    notes                 TEXT         NULL,
    created_by_email      VARCHAR(255) NULL,
    updated_by_email      VARCHAR(255) NULL,
    created_at            TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted_at            TIMESTAMP    NULL,
    deleted_by_email      VARCHAR(255) NULL,
    PRIMARY KEY (id),
    KEY idx_qc_factories_supplier (supplier_name),
    KEY idx_qc_factories_code (supplier_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS qc_inspectors (
    id                    INT          NOT NULL AUTO_INCREMENT,
    name                  VARCHAR(255) NOT NULL,
    kind                  VARCHAR(16)  NOT NULL DEFAULT 'agency',    -- internal | agency
    company               VARCHAR(255) NULL,
    email                 VARCHAR(255) NULL,
    wechat                VARCHAR(255) NULL,
    phone                 VARCHAR(255) NULL,
    base_city             VARCHAR(100) NULL,
    notes                 TEXT         NULL,
    active                TINYINT(1)   NOT NULL DEFAULT 1,
    created_by_email      VARCHAR(255) NULL,
    updated_by_email      VARCHAR(255) NULL,
    created_at            TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_qc_inspectors_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS qc_visits (
    id                        INT          NOT NULL AUTO_INCREMENT,
    ref                       VARCHAR(16)  NULL,                     -- QCV_00001, set right after insert
    status                    VARCHAR(16)  NOT NULL DEFAULT 'DRAFT', -- DRAFT | PROPOSED | CONFIRMED | COMPLETED | CANCELLED
    supplier_name             VARCHAR(255) NOT NULL,                 -- the host factory's supplier
    factory_id                INT          NULL,                     -- qc_factories.id
    factory_label             VARCHAR(100) NULL,
    factory_address           VARCHAR(500) NULL,
    factory_city              VARCHAR(100) NULL,
    factory_province          VARCHAR(100) NULL,
    factory_contact_name      VARCHAR(255) NULL,
    factory_contact_title     VARCHAR(255) NULL,
    factory_contact_email     VARCHAR(255) NULL,
    factory_contact_wechat    VARCHAR(255) NULL,
    factory_contact_phone     VARCHAR(255) NULL,
    inspector_id              INT          NULL,                     -- qc_inspectors.id
    inspector_name            VARCHAR(255) NULL,
    inspector_company         VARCHAR(255) NULL,
    inspector_email           VARCHAR(255) NULL,
    inspector_wechat          VARCHAR(255) NULL,
    inspector_phone           VARCHAR(255) NULL,
    visit_date                DATE         NULL,                     -- a day in China
    window_start              TIME         NULL,                     -- China wall-clock
    window_end                TIME         NULL,
    tz                        VARCHAR(40)  NOT NULL DEFAULT 'Asia/Shanghai',
    expected_production_state VARCHAR(32)  NULL,                     -- PRODUCTION_STATES in src/lib/qc-visits.js
    report_deadline_hours     SMALLINT     NULL DEFAULT 24,
    escalation_note           VARCHAR(500) NULL,
    notes                     TEXT         NULL,
    factory_confirmed_at      TIMESTAMP    NULL,
    inspector_confirmed_at    TIMESTAMP    NULL,
    proposed_at               TIMESTAMP    NULL,
    confirmed_at              TIMESTAMP    NULL,
    completed_at              TIMESTAMP    NULL,
    cancelled_at              TIMESTAMP    NULL,
    cancel_category           VARCHAR(32)  NULL,                     -- CANCEL_CATEGORIES in src/lib/qc-visits.js
    wasted_trip               TINYINT(1)   NOT NULL DEFAULT 0,
    cancel_reason             VARCHAR(500) NULL,
    cloned_from_visit_id      INT          NULL,
    created_by_email          VARCHAR(255) NULL,
    updated_by_email          VARCHAR(255) NULL,
    created_at                TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at                TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3), -- ms precision: the STALE token
    PRIMARY KEY (id),
    UNIQUE KEY uq_qc_visits_ref (ref),
    KEY idx_qc_visits_status_date (status, visit_date),
    KEY idx_qc_visits_supplier (supplier_name),
    KEY idx_qc_visits_inspector_date (inspector_id, visit_date),
    KEY idx_qc_visits_factory (factory_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS qc_visit_lines (
    id                    INT          NOT NULL AUTO_INCREMENT,
    visit_id              INT          NOT NULL,                     -- qc_visits.id
    order_id              INT          NOT NULL,                     -- orders.id
    quantity_to_inspect   INT          NOT NULL,
    po_quantity_snapshot  INT          NOT NULL,                     -- orders.quantity when last written
    sample_method         VARCHAR(32)  NOT NULL DEFAULT 'full',      -- full | aql | custom | destructive
    aql_level             VARCHAR(32)  NULL,
    aql_critical          DECIMAL(4,2) NULL,
    aql_major             DECIMAL(4,2) NULL,
    aql_minor             DECIMAL(4,2) NULL,
    note                  VARCHAR(500) NULL,
    created_at            TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_qc_visit_lines (visit_id, order_id),
    KEY idx_qc_visit_lines_order (order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS qc_visit_moves (
    id                    INT          NOT NULL AUTO_INCREMENT,
    visit_id              INT          NOT NULL,                     -- qc_visits.id
    order_id              INT          NOT NULL,                     -- orders.id
    from_supplier_name    VARCHAR(255) NOT NULL,
    to_factory_id         INT          NULL,                         -- qc_factories.id
    move_by               DATE         NULL,
    arranged_by           VARCHAR(255) NULL,
    note                  VARCHAR(500) NULL,
    confirmed_at          TIMESTAMP    NULL,
    created_at            TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_qc_visit_moves (visit_id, order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS qc_visit_confirmations (
    id                    INT          NOT NULL AUTO_INCREMENT,
    visit_id              INT          NOT NULL,                     -- qc_visits.id
    party                 VARCHAR(16)  NOT NULL,                     -- factory | inspector
    kind                  VARCHAR(16)  NOT NULL,                     -- proposed | confirmed | declined | counter
    channel               VARCHAR(16)  NOT NULL,                     -- email | wechat | phone | portal | other
    on_date               DATE         NULL,
    counter_date          DATE         NULL,
    counter_window_start  TIME         NULL,
    counter_window_end    TIME         NULL,
    by_name               VARCHAR(255) NULL,
    note                  VARCHAR(500) NULL,
    recorded_by_email     VARCHAR(255) NULL,
    created_at            TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    KEY idx_qc_visit_confirmations_visit (visit_id, party, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS qc_visit_attachments (
    id                    INT          NOT NULL AUTO_INCREMENT,
    visit_id              INT          NOT NULL,                     -- qc_visits.id
    filename              VARCHAR(200) NOT NULL,
    s3_key                VARCHAR(500) NOT NULL,
    content_type          VARCHAR(100) NULL,
    file_size             INT          NULL,
    note                  VARCHAR(255) NULL,
    uploaded_by_email     VARCHAR(255) NULL,
    created_at            TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at            TIMESTAMP    NULL,
    deleted_by_email      VARCHAR(255) NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_qc_visit_attachments_key (s3_key),
    KEY idx_qc_visit_attachments_visit (visit_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
