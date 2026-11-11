-- orders.artwork_confirmed_by_supplier: the day the SUPPLIER confirmed a line's
-- final artwork on the supplier portal (docs/artwork-confirmations.md). NULL =
-- not confirmed by them.
--
-- A field of its own, apart from artwork_confirmed_date, which stays what staff
-- record in ShipLine (and what the payment rules' "after artwork is confirmed"
-- trigger and the move-into-production gate read). Written only by
-- jportal-artwork-confirmed-<env> (src/handlers/artwork-confirmed.js) when it
-- applies a supplier's tick, and cleared by staff from ShipLine's ARTWORK panel
-- (POST …/artwork-confirmation/lines/:orderId/clear) so the supplier can
-- confirm again. Returned on every order as `artworkConfirmedBySupplier`.
--
-- tools/migrate.js tolerates "column exists" (1060), so this is safe to re-run.

ALTER TABLE orders ADD COLUMN artwork_confirmed_by_supplier DATE NULL AFTER artwork_confirmed_date;
