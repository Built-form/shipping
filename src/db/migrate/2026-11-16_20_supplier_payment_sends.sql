-- supplier_payment_sends: every "Send to supplier" of a recorded payment
-- (docs/payment-proof-send.md; src/services/payment-notices.js). One row per
-- email asked for: a transfer is told to its supplier as one or two notices
-- (notice_kind 'deposit' / 'balance'), each its own email.
--
-- A send log like the other *_sends tables, with what a money email needs:
--   send_key      the client's key for one click. Unique: a retried request
--                 finds its row and is answered from it, never sent twice.
--   request_hash  what that click asked for (recipients, subject, body,
--                 delivery, what the dialog showed): the same key with other
--                 content is refused.
--   status        'sending' (the row is written BEFORE Front is called),
--                 'sent', 'failed', or 'unknown' (Front did not answer: a
--                 person checks Front and says which; it is never retried).
--   deadline_at   after this, a row still 'sending' is read as 'unknown'.
--   supplier_name the transfer's supplier_name when this was sent. The
--                 portal shows a payment only while a 'sent' row carries the
--                 transfer's CURRENT name (portal_payments_v), so a payment
--                 moved to another supplier is not shown to them unsent.
--   delivery      'portal' (a link) or 'attached' (the proof on the email).
--
-- NO copy of the payment's figures: the portal reads the payment live. Not
-- granted to the portal's login: the views carry "was it sent".
-- No FKs, VARCHAR enums validated in code (src/lib/payment-notices.js).

CREATE TABLE IF NOT EXISTS supplier_payment_sends (
    id INT NOT NULL AUTO_INCREMENT,
    supplier_payment_id INT NOT NULL,
    notice_kind VARCHAR(16) NOT NULL,
    delivery VARCHAR(16) NOT NULL,
    supplier_name VARCHAR(255) NOT NULL,
    send_key VARCHAR(64) NOT NULL,
    request_hash CHAR(64) NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'sending',
    deadline_at DATETIME NOT NULL,
    sent_to JSON NOT NULL,
    subject VARCHAR(500) NULL,
    receipt_token VARCHAR(128) NULL,
    front_message_uid VARCHAR(128) NULL,
    front_conversation_id VARCHAR(64) NULL,
    send_error VARCHAR(500) NULL,
    sent_by_email VARCHAR(255) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    sent_at DATETIME NULL,
    resolved_at DATETIME NULL,
    resolved_by_email VARCHAR(255) NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uniq_send_key (send_key),
    KEY idx_payment (supplier_payment_id, notice_kind),
    KEY idx_sent_at (sent_at)
);
