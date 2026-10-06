-- Custom due dates on ShipLine's Payments flow page
-- (/api/v1/payment-due-dates — src/services/payment-due-date-routes.js,
-- rules in src/lib/payment-due-dates.js).
--
-- The page works each payment's due date out from the terms, the company
-- rules and the goods' dates; a person can set one by hand instead, and clear
-- it to go back to the derived date. A payment has no row of its own, so the
-- date hangs on the key the page builds (as sign-offs do):
--   deposit:<purchase order id>
--   balance:<currency>:<container>|<supplier key>
--   item:<row id>                      one row of a payment; beats the payment's
-- One row per key: setting again updates it, clearing deletes it. The audit
-- log keeps the history (entity_type 'payment_due_date'). No FKs.
CREATE TABLE IF NOT EXISTS payment_due_dates (
    id INT NOT NULL AUTO_INCREMENT,
    target_key VARCHAR(255) NOT NULL,
    due_date DATE NOT NULL,
    note VARCHAR(500) NULL,
    set_by_email VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_target_key (target_key)
);
