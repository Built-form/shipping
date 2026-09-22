-- Supplier payments (/api/v1/supplier-payments): one bank transfer to a
-- supplier, applied to the things it settled — balances on several
-- shipments, a deposit PI, a deposit on a PO with no PI yet — each for an
-- amount, so one transfer can cover four obligations and a partial payment is
-- representable. See the "Supplier payments" section of src/handlers/orders.js.
-- No FKs, VARCHAR enums validated in code.

CREATE TABLE IF NOT EXISTS supplier_payments (
    id INT NOT NULL AUTO_INCREMENT,
    supplier_name VARCHAR(255) NOT NULL,
    supplier_key VARCHAR(255) NOT NULL,
    amount DECIMAL(14,2) NOT NULL,
    currency CHAR(3) NOT NULL,
    paid_on DATE NOT NULL,
    bank_ref VARCHAR(255) NULL,
    note VARCHAR(2000) NULL,
    source VARCHAR(16) NOT NULL DEFAULT 'manual',
    created_by_email VARCHAR(255) NULL,
    updated_by_email VARCHAR(255) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted_at DATETIME NULL,
    PRIMARY KEY (id),
    KEY idx_supplier_key (supplier_key),
    KEY idx_paid_on (paid_on)
);

-- One line per obligation the transfer was applied to. target_kind is
-- 'balance' (shipment_payments.id), 'pi' (purchase_order_invoice_payments.id)
-- or 'po_deposit' (purchase_orders.id — a deposit paid before any PI was
-- filed; the line itself is the record of it). Replaced wholesale on edit.
CREATE TABLE IF NOT EXISTS supplier_payment_lines (
    id INT NOT NULL AUTO_INCREMENT,
    payment_id INT NOT NULL,
    target_kind VARCHAR(16) NOT NULL,
    target_id INT NOT NULL,
    amount DECIMAL(14,2) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY idx_payment (payment_id),
    KEY idx_target (target_kind, target_id)
);

-- Which transfer settled an obligation, so deleting the transfer can put it
-- back to pending and nothing else.
ALTER TABLE shipment_payments ADD COLUMN settled_by_payment_id INT NULL;
ALTER TABLE purchase_order_invoice_payments ADD COLUMN settled_by_payment_id INT NULL;

-- A proof of payment is to a supplier, not to one shipment: it may be
-- uploaded from the Payments page with no shipment at all, and once applied
-- it belongs to the transfer.
ALTER TABLE shipment_payment_documents MODIFY shipment_id INT NULL;
ALTER TABLE shipment_payment_documents MODIFY shipment_reference VARCHAR(100) NULL;
ALTER TABLE shipment_payment_documents ADD COLUMN supplier_payment_id INT NULL;
ALTER TABLE shipment_payment_documents ADD KEY idx_supplier_payment (supplier_payment_id);
