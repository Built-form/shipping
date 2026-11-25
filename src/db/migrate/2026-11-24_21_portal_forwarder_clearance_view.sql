-- What the supplier portal may read of the CLEARANCE INSTRUCTIONS sent to a
-- forwarder (docs/clearance-instructions.md): the sheets that have gone, each
-- with its file, by container and by the forwarder it was sent to.
--
-- Only a sheet whose email is marked SENT: one Front refused, or never answered
-- for, is not shown (the forwarder may never have had it). Never the rows as
-- data, who sent it, or to which addresses: the file is what was sent, and it
-- holds nothing its forwarder was not emailed.
--
-- The portal shows a row only to a contact of that forwarder who can see that
-- container (its own join on portal_forwarder_containers_v, by shipment and
-- forwarder): this view alone gives nobody anything, it has no grant until
-- 2026-11-24_22.
--
-- SQL SECURITY DEFINER and CREATE OR REPLACE, as 2026-11-16_21: the portal's
-- login reads the view and gains no right on the tables under it.

CREATE OR REPLACE SQL SECURITY DEFINER VIEW portal_forwarder_clearance_v AS
SELECT c.id,
       c.shipment_id,
       c.forwarder_id,
       c.version,
       c.filename,
       c.csv,
       n.sent_at
  FROM shipment_clearance_instructions c
  JOIN freight_container_notices n ON n.id = c.notice_id
 WHERE n.status = 'sent';
