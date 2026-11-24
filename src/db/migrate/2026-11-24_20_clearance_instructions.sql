-- Clearance instructions (docs/clearance-instructions.md): the sheet our team
-- send to the forwarder that handles a container's customs, generated from what
-- we hold instead of typed by hand into a spreadsheet.
--
-- 1. shipment_clearance_instructions: one row per email that carried a sheet
--    (POST /shipments/:id/forwarder-notice with `clearance`). Written in the
--    transaction that claims the send, before Front is called, so what was sent
--    is on record whatever Front answers.
--      notice_id   the freight_container_notices row of that email. Unique: one
--                  sheet per email; a send Front refused is sent again over the
--                  same row. Whether it went is that row's status.
--      version     the container's 1st, 2nd, ... sheet.
--      rows_json   the rows as they were checked and sent.
--      csv         the file itself. Kept here, not in a bucket: it is a few
--                  kilobytes of text, and the supplier portal serves it to the
--                  forwarder from the view of 2026-11-24_21 with no new
--                  permission on any bucket.
--
-- 2. clearance_duty_rates: the duty percentage each commodity code last went
--    with, offered the next time that code is on a sheet. Nothing here knows a
--    tariff: it is what our team typed. A whole-number percent, as JFPRO holds
--    duty (2.50 = 2.5%).
--
-- No FKs, UTC, as the tables around them. CREATE TABLE IF NOT EXISTS: safe to
-- run twice.
--
-- Rollback (once no deployed code reads them): DROP VIEW
-- portal_forwarder_clearance_v; DROP TABLE shipment_clearance_instructions,
-- clearance_duty_rates. Nothing else refers to them.

CREATE TABLE IF NOT EXISTS shipment_clearance_instructions (
    id INT NOT NULL AUTO_INCREMENT,
    shipment_id INT NOT NULL,
    forwarder_id INT NOT NULL,
    notice_id INT NOT NULL,
    version INT NOT NULL,
    title VARCHAR(100) NOT NULL,
    filename VARCHAR(200) NOT NULL,
    container_cost_usd DECIMAL(12,2) NOT NULL,
    total_cbm DECIMAL(10,2) NOT NULL,
    rows_json JSON NOT NULL,
    csv MEDIUMTEXT NOT NULL,
    created_by_email VARCHAR(255) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_clearance_notice (notice_id),
    KEY idx_clearance_shipment (shipment_id, id),
    KEY idx_clearance_forwarder (forwarder_id, shipment_id)
);

CREATE TABLE IF NOT EXISTS clearance_duty_rates (
    commodity_code VARCHAR(10) NOT NULL,
    duty_percent DECIMAL(5,2) NOT NULL,
    updated_by_email VARCHAR(255) NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (commodity_code)
);
