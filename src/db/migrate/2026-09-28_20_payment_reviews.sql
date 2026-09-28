-- Payment sign-offs (/api/v1/payment-reviews, /api/v1/payment-assignees —
-- src/services/payment-review-routes.js, rules in src/lib/payment-reviews.js).
--
-- ShipLine's Payments flow page works a payment out from the terms and the
-- goods on board, so there is no payment row to hang a sign-off on. It is
-- keyed instead by the key the page builds:
--   deposit:<purchase order id>
--   balance:<currency>:<container>|<supplier key>
-- and carries the figure the reviewer saw (amount + currency): once the page's
-- figure moves, that review no longer counts and the payment is reviewed again.
--
-- Rows are never edited. Withdrawing sets revoked_at ('withdrawn'); reviewing
-- a changed figure revokes the reviewer's old row ('superseded') and adds one.
-- Two different people must hold a current review; the assignee is never one
-- of them. No FKs, VARCHAR enums validated in code.
CREATE TABLE IF NOT EXISTS payment_reviews (
    id INT NOT NULL AUTO_INCREMENT,
    payment_key VARCHAR(255) NOT NULL,
    kind VARCHAR(16) NOT NULL,
    supplier_name VARCHAR(255) NULL,
    po_numbers VARCHAR(1000) NULL,
    container_ref VARCHAR(100) NULL,
    currency CHAR(3) NOT NULL,
    amount DECIMAL(14,2) NOT NULL,
    due_date DATE NULL,
    reviewed_by_email VARCHAR(255) NOT NULL,
    reviewed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    revoked_at DATETIME NULL,
    revoked_by_email VARCHAR(255) NULL,
    revoked_reason VARCHAR(32) NULL,
    PRIMARY KEY (id),
    KEY idx_payment_key (payment_key),
    KEY idx_revoked_at (revoked_at)
);

-- One assignee per payment: an accountant (shipping_allowed_emails.type =
-- 'accountant' when set). Unassigning deletes the row; the audit log keeps
-- the history (entity_type 'payment_assignee').
CREATE TABLE IF NOT EXISTS payment_assignees (
    id INT NOT NULL AUTO_INCREMENT,
    payment_key VARCHAR(255) NOT NULL,
    assignee_email VARCHAR(255) NOT NULL,
    assigned_by_email VARCHAR(255) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_payment_key (payment_key)
);
