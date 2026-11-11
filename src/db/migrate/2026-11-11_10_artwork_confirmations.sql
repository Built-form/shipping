-- Artwork confirmation by the supplier, per PO line (docs/artwork-confirmations.md).
--
-- Staff email a supplier from ShipLine asking them to confirm final artwork
-- (POST /api/v1/purchase-orders/:id/artwork-confirmation/email). The email
-- carries a no-login link into the supplier portal, <portal>/artwork/<token>,
-- where the supplier ticks each item whose artwork they have confirmed; a
-- signed-in portal user ticks the same items on the order's page.
--
-- Both tables live HERE, beside the PO, as po_deposit_receipts does
-- (2026-10-27_10): every reader is on this schema whatever server the portal
-- runs on. The portal reads the request and inserts confirmations under column
-- grants (2026-11-11_11, mirrored in jportal's db/grants/jportal_app.sql), then
-- invokes jportal-artwork-confirmed-<env> (src/handlers/artwork-confirmed.js),
-- which writes orders.artwork_confirmed_by_supplier (2026-11-11_05). The portal
-- never writes orders.
--
-- orders.artwork_confirmed_by_supplier is the one truth for "confirmed by the
-- supplier" (artwork_confirmed_date stays staff's own field). A confirmation
-- row is the supplier's word, applied once (applied_at); staff clearing the
-- confirmation in ShipLine makes the line confirmable again.
--
-- No foreign keys, no ENUMs, UTC timestamps, as the other tables.

CREATE TABLE IF NOT EXISTS artwork_confirmation_requests (
    id                    INT          NOT NULL AUTO_INCREMENT,
    token                 CHAR(48)     NOT NULL,             -- 48 hex; the link's whole permission
    purchase_order_id     INT          NOT NULL,             -- purchase_orders.id
    po_number             VARCHAR(64)  NULL,                 -- snapshot, shown on the page
    supplier_name         VARCHAR(255) NULL,                 -- snapshot, shown on the page
    order_ids             JSON         NOT NULL,             -- the lines asked about: [orders.id, …]
    sent_to               JSON         NULL,                 -- recipients; never shown on the portal
    subject               VARCHAR(500) NULL,
    front_message_uid     VARCHAR(128) NULL,
    front_conversation_id VARCHAR(128) NULL,
    created_by_email      VARCHAR(255) NULL,                 -- staff sender; never shown on the portal
    created_at            TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at            DATETIME     NOT NULL,
    revoked_at            DATETIME     NULL,
    revoke_reason         VARCHAR(32)  NULL,                 -- staff | send_failed
    PRIMARY KEY (id),
    UNIQUE KEY uq_artwork_requests_token (token),
    KEY idx_artwork_requests_po (purchase_order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS artwork_confirmations (
    id                 INT          NOT NULL AUTO_INCREMENT,
    order_id           INT          NOT NULL,                -- orders.id (the PO line)
    purchase_order_id  INT          NOT NULL,                -- purchase_orders.id
    request_id         INT          NULL,                    -- artwork_confirmation_requests.id for a link tick
    source             VARCHAR(16)  NOT NULL,                -- link | portal
    supplier_id        INT          NULL,                    -- the portal's ids, signed-in ticks only
    user_id            INT          NULL,
    confirmed_by_email VARCHAR(255) NULL,                    -- signed-in tick: the portal user's email
    confirmed_by_name  VARCHAR(255) NULL,                    -- link tick: the name typed on the page, if any
    confirmed_on       DATE         NOT NULL,                -- UTC date of the tick
    created_at         TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    applied_at         DATETIME     NULL,                    -- written by shipping only
    -- At most one confirmation waiting to be applied per line.
    pending_order_id   INT GENERATED ALWAYS AS (IF(applied_at IS NULL, order_id, NULL)) STORED,
    PRIMARY KEY (id),
    UNIQUE KEY uq_artwork_confirmations_pending (pending_order_id),
    KEY idx_artwork_confirmations_po (purchase_order_id),
    KEY idx_artwork_confirmations_order (order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
