-- Seed the buyer-side companies a PO is issued for, so the company dropdown
-- and the PO PDF's Customer Details block work on a fresh database (was
-- purchaseOrdersSchemaReady in src/handlers/orders.js). Legal names and
-- addresses from Companies House; both share the Cornford Road premises.
-- INSERT IGNORE: existing rows (and UI edits to them) are left alone.

INSERT IGNORE INTO companies (name, address_lines, country) VALUES
    ('JFA Medical Ltd', JSON_ARRAY('Unit B, Prestige House', 'Cornford Road', 'Blackpool', 'Lancashire', 'FY4 4QQ'), 'UNITED KINGDOM'),
    ('Hangerworld Ltd', JSON_ARRAY('Unit B, Prestige House', 'Cornford Road', 'Blackpool', 'Lancashire', 'FY4 4QQ'), 'UNITED KINGDOM');
