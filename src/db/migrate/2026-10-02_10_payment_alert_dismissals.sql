-- Dismissed "Needs attention" lines on ShipLine's Payments flow page
-- (/api/v1/payment-alert-dismissals — src/services/payment-alert-routes.js,
-- rules in src/lib/payment-alerts.js).
--
-- The page works its alerts out from the data each time, so an alert has no
-- row of its own. An admin dismissing one is keyed by the key the page builds
-- from what the alert says (kind, PO, container, supplier and a hash of its
-- wording): when the wording or the figure changes the key changes, and the
-- alert comes back. What it said when it was dismissed is kept beside the key.
--
-- Rows are never deleted. Restoring sets restored_at; dismissing again adds a
-- row. No FKs; kind is the page's vocabulary and is not validated here.
CREATE TABLE IF NOT EXISTS payment_alert_dismissals (
    id INT NOT NULL AUTO_INCREMENT,
    alert_key VARCHAR(255) NOT NULL,
    kind VARCHAR(40) NOT NULL,
    currency CHAR(3) NULL,
    po_number VARCHAR(100) NULL,
    shipment_reference VARCHAR(100) NULL,
    supplier_name VARCHAR(255) NULL,
    detail VARCHAR(1000) NULL,
    amount DECIMAL(14,2) NULL,
    note VARCHAR(500) NULL,
    dismissed_by_email VARCHAR(255) NOT NULL,
    dismissed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    restored_at DATETIME NULL,
    restored_by_email VARCHAR(255) NULL,
    PRIMARY KEY (id),
    KEY idx_alert_key (alert_key),
    KEY idx_restored_at (restored_at)
);
