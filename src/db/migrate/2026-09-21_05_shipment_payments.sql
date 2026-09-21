-- Shipment balance payments (/api/v1/shipment-payments): one record per
-- shipment x supplier (per invoice, when a supplier bills in parts) with a
-- per-PO split underneath it, and the documents behind them. See the
-- "Shipment balance payments" section of src/handlers/orders.js. Was
-- cold-start DDL in orders.js. No FKs, VARCHAR enums validated in code.

CREATE TABLE IF NOT EXISTS shipment_payments (
    id INT NOT NULL AUTO_INCREMENT,
    shipment_id INT NOT NULL,
    shipment_reference VARCHAR(100) NOT NULL,
    supplier_name VARCHAR(255) NOT NULL,
    supplier_key VARCHAR(255) NOT NULL,
    kind VARCHAR(16) NOT NULL DEFAULT 'balance',
    amount DECIMAL(14,2) NOT NULL,
    currency CHAR(3) NOT NULL,
    invoice_number VARCHAR(100) NULL,
    invoice_date DATE NULL,
    due_date DATE NULL,
    invoice_total DECIMAL(14,2) NULL,
    deposit_deducted DECIMAL(14,2) NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'pending',
    paid_on DATE NULL,
    bank_ref VARCHAR(255) NULL,
    note VARCHAR(2000) NULL,
    source VARCHAR(16) NOT NULL DEFAULT 'manual',
    created_by_email VARCHAR(255) NULL,
    updated_by_email VARCHAR(255) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted_at DATETIME NULL,
    PRIMARY KEY (id),
    KEY idx_shipment (shipment_id),
    KEY idx_reference (shipment_reference),
    KEY idx_supplier_key (supplier_key),
    KEY idx_status (status)
);

-- Replaced wholesale on every write, so no updated_at: the parent's
-- updated_at is bumped instead and drives the page's polling.
CREATE TABLE IF NOT EXISTS shipment_payment_allocations (
    id INT NOT NULL AUTO_INCREMENT,
    payment_id INT NOT NULL,
    purchase_order_id INT NULL,
    po_ref VARCHAR(100) NOT NULL,
    amount DECIMAL(14,2) NOT NULL,
    source VARCHAR(16) NOT NULL DEFAULT 'manual',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY idx_payment (payment_id),
    KEY idx_purchase_order (purchase_order_id)
);

-- The paper behind a record: the supplier's balance invoice, or a remittance
-- advice proving we paid it. extract_status tracks the background read.
CREATE TABLE IF NOT EXISTS shipment_payment_documents (
    id INT NOT NULL AUTO_INCREMENT,
    shipment_id INT NOT NULL,
    shipment_reference VARCHAR(100) NOT NULL,
    supplier_name VARCHAR(255) NULL,
    supplier_key VARCHAR(255) NULL,
    payment_id INT NULL,
    doc_kind VARCHAR(20) NOT NULL DEFAULT 'balance_invoice',
    filename VARCHAR(200) NOT NULL,
    s3_key VARCHAR(500) NOT NULL,
    public_url VARCHAR(1000) NULL,
    content_type VARCHAR(100) NULL,
    file_size INT NULL,
    notes VARCHAR(2000) NULL,
    extract_status VARCHAR(20) NOT NULL DEFAULT 'none',
    extract_json JSON NULL,
    model_used VARCHAR(64) NULL,
    extract_error TEXT NULL,
    extracted_at TIMESTAMP NULL,
    uploaded_by_email VARCHAR(255) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at DATETIME NULL,
    PRIMARY KEY (id),
    KEY idx_shipment (shipment_id),
    KEY idx_payment (payment_id),
    KEY idx_extract_status (extract_status)
);
