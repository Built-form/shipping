-- Ready-date reminders (docs/ready-date-reminders.md).
--
-- A supplier is emailed every working day while they owe us a ready date
-- (src/services/ready-date-reminders.js, Lambda sendReadyDateReminders):
--   the ESTIMATED date, from a week after staff confirm final artwork on a PO
--   line and once the PO's deposit is paid or not required
--   the ACTUAL date, once the line's estimated ready date has passed
--
-- One email per SUPPLIER per day, covering all of their purchase orders, so a
-- supplier with many open POs gets one email, not one per PO. This table holds
-- one row per chase of one supplier: from the first email until none of the
-- lines it asked about is still waiting.
--   supplier_key        the supplier's name reduced to letters and digits, lower
--                       case (the same company is often typed two ways)
--   purchase_order_ids  the POs named in the latest email
--   order_ids           every line asked about during this chase
--   send_count          emails sent so far in this chase
--   last_sent_at        when the latest went, UTC. Also the claim that stops a
--                       second email within the day: a run takes the supplier
--                       by moving it forward, under a 20 hour minimum gap
--   closed_at           no line of order_ids is waiting any more. NULL = open
--   open_supplier_key   generated: supplier_key while open. Unique, so a
--                       supplier has at most one open chase
-- The emails themselves are listed on ShipLine's Email Receipts page:
-- email_receipts, email_type 'ready_date_reminder', send_table
-- 'ready_date_reminders', send_id = this id. The first email of a chase is the
-- row, each later one a row of email_receipt_reminders under it.
--
-- Written by shipping only. No foreign keys, no ENUMs, UTC timestamps, as the
-- other tables.

CREATE TABLE IF NOT EXISTS ready_date_reminders (
    id                    INT          NOT NULL AUTO_INCREMENT,
    supplier_key          VARCHAR(255) NOT NULL,
    supplier_name         VARCHAR(255) NULL,
    purchase_order_ids    JSON         NOT NULL,
    order_ids             JSON         NOT NULL,
    sent_to               JSON         NULL,
    subject               VARCHAR(500) NULL,
    send_count            INT          NOT NULL DEFAULT 0,
    first_sent_at         DATETIME     NULL,
    last_sent_at          DATETIME     NOT NULL,
    front_message_uid     VARCHAR(128) NULL,
    front_conversation_id VARCHAR(128) NULL,
    closed_at             DATETIME     NULL,
    created_at            TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    open_supplier_key     VARCHAR(255) GENERATED ALWAYS AS (IF(closed_at IS NULL, supplier_key, NULL)) STORED,
    PRIMARY KEY (id),
    UNIQUE KEY uq_ready_date_reminders_open (open_supplier_key),
    KEY idx_ready_date_reminders_supplier (supplier_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
