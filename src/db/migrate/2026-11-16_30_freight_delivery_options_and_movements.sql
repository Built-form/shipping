-- Freight quotes (docs/freight-quotes.md): delivery options staff keep
-- themselves, and the stock movements of a request.
--
-- DELIVERY OPTIONS. What a request asks to be quoted ("Door to door",
-- "Door to port, we collect from Felixstowe", …) was a fixed list of four
-- codes. It is now a list staff keep in ShipLine (FREIGHT > DELIVERY OPTIONS):
-- free text, added, reworded and removed there. A request stores the TEXT of
-- the ones chosen in its `services` column, as it stood when sent, and each
-- option the forwarder offers names one of them in `service`. So rewording or
-- removing a list entry never changes a request already sent. Requests from
-- before this migration hold the four old codes; every reader still knows them.
-- The four are seeded as the first entries (2026-11-16_32).
--
-- STOCK MOVEMENTS. A request may carry one or more: goods to be moved from a
-- factory to the port or a consolidation point, each with its volume and
-- weight, so the forwarder can price the collections. Kept on the request as
-- JSON, as sent:
--   [ { "from": "Acme Medical, Ningbo", "to": "Ningbo port", "cbm": 12.5, "weightKg": 2400 } ]
-- NULL = none. The supplier portal shows them on the quote page (2026-11-16_31
-- grants the column).
--
-- tools/migrate.js tolerates "column exists" (1060), and MODIFY to the same
-- definition is a no-op, so this is safe to re-run.

CREATE TABLE IF NOT EXISTS freight_delivery_options (
    id               INT          NOT NULL AUTO_INCREMENT,
    label            VARCHAR(100) NOT NULL,                 -- free text, as staff typed it
    created_by_email VARCHAR(255) NULL,
    created_at       TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted_at       DATETIME     NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

ALTER TABLE freight_quote_requests ADD COLUMN stock_movements JSON NULL AFTER staff_note;

-- Was VARCHAR(16): one of the four codes. Now the text of a delivery option.
ALTER TABLE freight_quote_options MODIFY COLUMN service VARCHAR(100) NOT NULL;
