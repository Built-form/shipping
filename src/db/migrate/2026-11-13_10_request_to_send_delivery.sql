-- draft_container_document_sends: how an email went out, and what a Request to
-- send's email said about the departure (docs/request-to-send-portal.md).
--
-- delivery: 'portal' = the email carried a link to the supplier portal and no
--   attachment (a Request to send whose supplier is on the portal);
--   'attached' = the PDF and its CSV were attached (every forwarder quote, and
--   a Request to send the portal could not show). NULL = a send from before
--   this column: attached.
-- request_etd / request_port: the ETD and the port of departure the sender
--   filled into the email (ShipLine's send dialog passes them as `fields`).
--   What the email said, kept as it was said: the portal shows them beside the
--   PDF, so they are never recomputed from the shipment. NULL = the email gave
--   none. Only a Request to send (type 'supplier-quote') stores them.
--
-- The supplier portal reads request_etd and request_port (2026-11-13_11 grants
-- them); delivery is for staff only.
--
-- The code that writes and reads these answers a server without them as empty
-- (src/services/request-to-send-portal.js), so this may run after the deploy.
-- tools/migrate.js tolerates "column exists" (1060), so it is safe to re-run.

ALTER TABLE draft_container_document_sends ADD COLUMN delivery VARCHAR(16) NULL AFTER sent_by_email;

ALTER TABLE draft_container_document_sends ADD COLUMN request_etd DATE NULL AFTER delivery;

ALTER TABLE draft_container_document_sends ADD COLUMN request_port VARCHAR(128) NULL AFTER request_etd;
