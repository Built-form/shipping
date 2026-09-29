-- Extra charges and credits on payments (/api/v1/payment-extras —
-- src/services/payment-extra-routes.js, rules in src/lib/payment-extras.js).
--
-- Money a supplier bills that is not goods (mould, handling, samples, …) or a
-- credit they give, added by a person with a reason. It rides with a payment
-- ShipLine's Payments flow page already works out — a PO's deposit, or one
-- supplier's balance in one container — and is signed off and paid with it.
-- Never part of the PO value. amount is signed: a credit is negative.
--
-- A transfer pays one through a supplier_payment_lines row with
-- target_kind = 'extra' (VARCHAR(16), no ALTER needed); settling sets status
-- 'paid' and settled_by_payment_id, and deleting or editing that transfer puts
-- it back to 'open'. 'paid' with no settled_by_payment_id = marked paid by hand.
-- No FKs, VARCHAR enums validated in code; soft delete.
CREATE TABLE IF NOT EXISTS payment_extras (
    id INT NOT NULL AUTO_INCREMENT,
    supplier_name VARCHAR(255) NOT NULL,
    supplier_key VARCHAR(255) NOT NULL,
    currency CHAR(3) NOT NULL,
    amount DECIMAL(14,2) NOT NULL,
    kind VARCHAR(16) NOT NULL,
    description VARCHAR(255) NULL,
    rides_with VARCHAR(16) NOT NULL,
    purchase_order_id INT NULL,
    shipment_id INT NULL,
    shipment_reference VARCHAR(64) NULL,
    due_date DATE NULL,
    source_kind VARCHAR(24) NULL,
    source_id INT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'open',
    paid_on DATE NULL,
    settled_by_payment_id INT NULL,
    note TEXT NULL,
    created_by_email VARCHAR(255) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by_email VARCHAR(255) NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted_at DATETIME NULL,
    deleted_by_email VARCHAR(255) NULL,
    PRIMARY KEY (id),
    KEY idx_purchase_order (purchase_order_id),
    KEY idx_shipment (shipment_id),
    KEY idx_supplier_currency (supplier_key, currency),
    KEY idx_settled_by (settled_by_payment_id)
);
