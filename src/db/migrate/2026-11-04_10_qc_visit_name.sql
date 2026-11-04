-- QC visits: a free-text name beside the ref (QCV_00012), so a visit that
-- gathers several products or suppliers on one trip can be called what ops
-- call it ("Ningbo trip w/c 10 Nov"). Optional; shown next to the ref in
-- ShipLine's lists, calendar and pop-up, and searched. Set on create (POST
-- /qc-visits) or later (PATCH /qc-visits/:id); validated to 120 characters in
-- src/lib/qc-visits.js validateVisitFields. Copied by a clone.
--
-- tools/migrate.js tolerates "column exists" (1060), so this is safe to re-run.

ALTER TABLE qc_visits ADD COLUMN name VARCHAR(120) NULL AFTER ref;
