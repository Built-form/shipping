-- Payment rules: delivered air freight (src/lib/payment-rules.js). Air goods
-- are released before their balance is paid — it goes with a later transfer —
-- so the Payments flow page (ShipLine) keeps air delivered on or after
-- air_owed_from as owed until a payment is recorded, due by air_limit_days
-- after delivery. air_owed_from is company-wide (the default rule only);
-- air_limit_days can differ per supplier (null inherits the default's, then 60).
-- Both null = today's behaviour: air counts as paid once delivered.
ALTER TABLE payment_rules ADD COLUMN air_owed_from DATE NULL;
ALTER TABLE payment_rules ADD COLUMN air_limit_days INT NULL;
