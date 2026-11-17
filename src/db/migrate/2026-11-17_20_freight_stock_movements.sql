-- Freight quotes (docs/freight-quotes.md): stock movements as rows of their
-- own, each with its own price.
--
-- A stock movement is goods to be collected from a factory, with their volume
-- and weight. Until now a request carried them as JSON (`stock_movements`,
-- 2026-11-16_30): a factory, a port or consolidation point, CBM and kg, listed
-- for information and priced inside the sailing's all-in price. Three things
-- change:
--   - there is no "to": the forwarder knows where it consolidates;
--   - each movement is priced on its own line by the forwarder (price and
--     currency), apart from the price of the sailing;
--   - staff can add a movement to a request after it was sent, after it was
--     quoted and after it was booked, and ask for its price then.
-- So a movement is a row here. 2026-11-17_22 copies the JSON of requests
-- already sent into this table; the JSON column is no longer read or written.
--
-- WHO WRITES WHAT
--   shipping  inserts the row (with the request, or added later), removes it
--             (deleted_at), and writes the price columns when staff type in a
--             price the forwarder gave by email.
--   portal    writes the price columns (price … price_ip) and bumps
--             row_version: the forwarder's own price, from the link or signed
--             in. Never the movement itself.
--
-- `add_key` is the call that added the row: the send's batch_id for movements
-- sent with the request, a new id for each later addition. With line_no it is
-- unique per request, so a retried call never adds (or emails) twice.
--
-- No foreign keys, no ENUMs, UTC timestamps, as the other tables.

CREATE TABLE IF NOT EXISTS freight_stock_movements (
    id               INT           NOT NULL AUTO_INCREMENT,
    request_id       INT           NOT NULL,                -- freight_quote_requests.id
    add_key          CHAR(36)      NOT NULL,                -- the call that added it
    line_no          INT           NOT NULL,                -- order within that call, from 1
    factory          VARCHAR(255)  NOT NULL,                -- where the goods are collected from
    cbm              DECIMAL(8,3)  NOT NULL,
    weight_kg        DECIMAL(10,2) NOT NULL,
    created_by_email VARCHAR(255)  NULL,                    -- staff; never shown on the portal
    created_at       TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at       DATETIME      NULL,                    -- removed by staff
    deleted_by_email VARCHAR(255)  NULL,
    -- The price: written by the portal (or by shipping for a staff-entered one).
    price            DECIMAL(12,2) NULL,                    -- NULL = not priced yet
    currency         CHAR(3)       NULL,                    -- USD | GBP | EUR | CNY
    price_note       VARCHAR(500)  NULL,
    priced_at        DATETIME      NULL,
    priced_by_name   VARCHAR(255)  NULL,
    priced_by_email  VARCHAR(255)  NULL,                    -- a signed-in portal user, or the staff member
    price_source     VARCHAR(16)   NULL,                    -- link | portal | staff
    price_ip         VARCHAR(45)   NULL,                    -- written by the portal, never read back by it
    row_version      INT           NOT NULL DEFAULT 1,      -- optimistic lock; bumped by every write of either app
    updated_at       TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_freight_movements_add (request_id, add_key, line_no),
    KEY idx_freight_movements_request (request_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
