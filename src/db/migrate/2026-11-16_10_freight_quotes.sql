-- Freight quotes: asking freight forwarders for sailings and prices, booking
-- one, and sending the container's contents (docs/freight-quotes.md).
--
-- Staff pick forwarders and a date range in ShipLine and send a request
-- (POST /api/v1/freight-quotes). Each forwarder gets its own row and its own
-- no-login link into the supplier portal, <portal>/freight/<token>, where it
-- lists its options: loading date, sailing date, transit time, port A, port B,
-- service, container size, price. A forwarder contact who signs in to the
-- portal sees the same requests. Staff compare the options in ShipLine, request
-- a booking on one, and the forwarder confirms it on the same page.
--
-- All five tables live HERE, as the artwork confirmation tables do
-- (2026-11-11_10): shipping owns the feature and the portal reads and writes
-- under column grants (2026-11-16_11, mirrored in jportal's
-- db/grants/jportal_app.sql).
--
-- WHO WRITES WHAT
--   forwarders, forwarder_contacts      shipping only.
--   freight_quote_requests              shipping inserts the row and writes the
--                                       revoke columns; the portal writes the
--                                       answer columns (answer_count … answer_ip).
--                                       Both bump row_version.
--   freight_quote_options               INSERT only, by the portal (a forwarder's
--                                       save) or by shipping (staff typing in an
--                                       emailed answer). Each save is a new
--                                       answer_no; the current options are those
--                                       with answer_no = the request's answer_count.
--   freight_bookings                    shipping inserts and cancels; the portal
--                                       writes the response columns (confirm or
--                                       decline). Both bump row_version.
--
-- No foreign keys, no ENUMs, UTC timestamps, as the other tables.

CREATE TABLE IF NOT EXISTS forwarders (
    id               INT           NOT NULL AUTO_INCREMENT,
    name             VARCHAR(255)  NOT NULL,
    notes            VARCHAR(1000) NULL,
    created_by_email VARCHAR(255)  NULL,
    created_at       TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted_at       DATETIME      NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS forwarder_contacts (
    id           INT          NOT NULL AUTO_INCREMENT,
    forwarder_id INT          NOT NULL,                  -- forwarders.id
    name         VARCHAR(255) NULL,
    email        VARCHAR(255) NOT NULL,                  -- trimmed, lower-case: how the portal matches a signed-in user
    created_at   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at   DATETIME     NULL,
    -- One live row per address per forwarder; a removed contact can be added again.
    live_email   VARCHAR(255) GENERATED ALWAYS AS (IF(deleted_at IS NULL, email, NULL)) STORED,
    PRIMARY KEY (id),
    UNIQUE KEY uq_forwarder_contacts_live (forwarder_id, live_email),
    KEY idx_forwarder_contacts_email (email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS freight_quote_requests (
    id                    INT           NOT NULL AUTO_INCREMENT,
    token                 CHAR(48)      NOT NULL,            -- 48 hex; the link's whole permission
    batch_id              CHAR(36)      NOT NULL,            -- one send to several forwarders shares it
    forwarder_id          INT           NOT NULL,            -- forwarders.id
    forwarder_name        VARCHAR(255)  NULL,                -- snapshot, shown on the page
    window_start_date     DATE          NOT NULL,            -- sailings asked for: first day
    window_end_date       DATE          NOT NULL,            -- last day
    origin                VARCHAR(255)  NULL,                -- where the goods are, as staff typed it
    destination           VARCHAR(255)  NULL,
    container_size        VARCHAR(8)    NOT NULL,            -- 20GP | 40GP | 40HQ: the size asked for
    services              JSON          NOT NULL,            -- the services asked for: ["door_to_door", …]
    staff_note            VARCHAR(1000) NULL,
    sent_to               JSON          NULL,                -- recipients; never shown on the portal
    subject               VARCHAR(500)  NULL,
    front_message_uid     VARCHAR(128)  NULL,
    front_conversation_id VARCHAR(128)  NULL,
    created_by_email      VARCHAR(255)  NULL,                -- staff sender; never shown on the portal
    created_at            TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    sent_at               DATETIME      NULL,                -- set once Front has taken the email
    expires_at            DATETIME      NOT NULL,
    revoked_at            DATETIME      NULL,
    revoke_reason         VARCHAR(32)   NULL,                -- staff | send_failed | filled | forwarder_removed
    -- The answer: written by the portal (or by shipping for a staff-entered one).
    answer_count          INT           NOT NULL DEFAULT 0,  -- saves so far; also the current answer_no
    answered_at           DATETIME      NULL,
    answered_by_name      VARCHAR(255)  NULL,
    answered_by_email     VARCHAR(255)  NULL,                -- a signed-in portal user, or the staff member
    answer_source         VARCHAR(16)   NULL,                -- link | portal | staff
    answer_note           VARCHAR(1000) NULL,
    declined              TINYINT(1)    NOT NULL DEFAULT 0,  -- "we cannot quote"
    answer_ip             VARCHAR(45)   NULL,                -- written by the portal, never read back by it
    row_version           INT           NOT NULL DEFAULT 1,  -- optimistic lock; bumped by every write of either app
    updated_at            TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_freight_requests_token (token),
    -- A retried send never makes a second row (or email) for the same forwarder.
    UNIQUE KEY uq_freight_requests_batch (batch_id, forwarder_id),
    KEY idx_freight_requests_forwarder (forwarder_id),
    KEY idx_freight_requests_window (window_end_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS freight_quote_options (
    id             INT           NOT NULL AUTO_INCREMENT,
    request_id     INT           NOT NULL,                   -- freight_quote_requests.id
    answer_no      INT           NOT NULL,                   -- which save this row belongs to
    line_no        INT           NOT NULL,                   -- order on the page, from 1
    loading_date   DATE          NOT NULL,
    sailing_date   DATE          NOT NULL,
    transit_days   INT           NOT NULL,
    port_from      VARCHAR(100)  NOT NULL,                   -- port A
    port_to        VARCHAR(100)  NOT NULL,                   -- port B
    service        VARCHAR(16)   NOT NULL,                   -- door_to_door | door_to_port | port_to_port | port_to_door
    container_size VARCHAR(8)    NOT NULL,                   -- may differ from the size asked for
    price          DECIMAL(12,2) NOT NULL,                   -- all-in, for this container
    currency       CHAR(3)       NOT NULL,                   -- USD | GBP | EUR | CNY
    carrier        VARCHAR(100)  NULL,
    valid_until    DATE          NULL,
    note           VARCHAR(500)  NULL,
    created_at     TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_freight_options_line (request_id, answer_no, line_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS freight_bookings (
    id                    INT           NOT NULL AUTO_INCREMENT,
    request_id            INT           NOT NULL,            -- freight_quote_requests.id
    option_id             INT           NOT NULL,            -- freight_quote_options.id
    status                VARCHAR(16)   NOT NULL DEFAULT 'requested', -- requested | confirmed | declined | cancelled
    requested_by_email    VARCHAR(255)  NULL,
    requested_at          DATETIME      NOT NULL,
    request_note          VARCHAR(1000) NULL,
    sent_to               JSON          NULL,                -- recipients; never shown on the portal
    subject               VARCHAR(500)  NULL,
    front_message_uid     VARCHAR(128)  NULL,
    front_conversation_id VARCHAR(128)  NULL,
    shipment_id           INT           NULL,                -- shipments.id of the attached draft container
    draft_container_name  VARCHAR(255)  NULL,                -- its name when attached
    -- The response: written by the portal (or by shipping when staff mark it).
    booking_ref           VARCHAR(100)  NULL,
    response_note         VARCHAR(1000) NULL,
    responded_at          DATETIME      NULL,
    responded_by_name     VARCHAR(255)  NULL,
    responded_by_email    VARCHAR(255)  NULL,
    response_source       VARCHAR(16)   NULL,                -- link | portal | staff
    response_ip           VARCHAR(45)   NULL,
    cancelled_at          DATETIME      NULL,
    cancelled_by_email    VARCHAR(255)  NULL,
    cancel_reason         VARCHAR(32)   NULL,                -- staff | send_failed
    -- The contents email: when, to whom, and the lines it listed.
    contents_sent_at      DATETIME      NULL,
    contents_sent_to      JSON          NULL,
    contents_json         JSON          NULL,
    row_version           INT           NOT NULL DEFAULT 1,
    created_at            TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    -- At most one live booking (requested or confirmed) per request.
    live_request_id       INT GENERATED ALWAYS AS (IF(status IN ('requested', 'confirmed'), request_id, NULL)) STORED,
    PRIMARY KEY (id),
    UNIQUE KEY uq_freight_bookings_live (live_request_id),
    KEY idx_freight_bookings_request (request_id),
    KEY idx_freight_bookings_shipment (shipment_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
