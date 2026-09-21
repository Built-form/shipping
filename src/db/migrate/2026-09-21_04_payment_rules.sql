-- Payment rules: the company's own payment policy layered over a supplier's
-- terms (see the "Payment rules" section of src/handlers/orders.js). One
-- 'default' rule (supplier_name '') plus per-supplier overrides keyed by the
-- lower-cased JFPRO supplier name. Was cold-start DDL in orders.js.

CREATE TABLE IF NOT EXISTS payment_rules (
    id INT NOT NULL AUTO_INCREMENT,
    scope ENUM('default', 'supplier') NOT NULL,
    supplier_name VARCHAR(255) NOT NULL DEFAULT '',
    supplier_label VARCHAR(255) NULL,
    deposit_pct DECIMAL(6,3) NULL,
    deposit_trigger VARCHAR(32) NULL,
    deposit_grace_days INT NOT NULL DEFAULT 0,
    balance_trigger VARCHAR(32) NULL,
    balance_document_type VARCHAR(64) NULL,
    balance_offset_days INT NULL,
    balance_grace_days INT NOT NULL DEFAULT 0,
    notes VARCHAR(500) NULL,
    updated_by_email VARCHAR(255) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_scope_supplier (scope, supplier_name)
);

-- Added 2026-09-18: deposit timing relative to its trigger, and the lead-time
-- estimates the Payments page uses to date an event that has not happened yet
-- (one JSON document, validated by parsePaymentRuleEstimates). The TEST
-- database also carries seven est_*_days columns from an earlier cut the same
-- day: unused, safe to drop.
ALTER TABLE payment_rules ADD COLUMN deposit_offset_days INT NULL;
ALTER TABLE payment_rules ADD COLUMN estimates_json TEXT NULL;
