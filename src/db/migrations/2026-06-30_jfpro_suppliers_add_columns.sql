-- Phase 1 of the supplier unification. Adds the shipping-only columns onto
-- jfpro.suppliers so JFPro can be the single source of truth and jfa.suppliers
-- can become a clean single-table passthrough VIEW (Phase 3). All columns are
-- additive + nullable so existing JFPro reads/writes are unaffected.
--
-- Fields JFPro already has are REUSED via the Phase 3 view, not duplicated here:
--   default_incoterms      <- jfpro.suppliers.shippingTerms
--   default_payment_terms  <- jfpro.suppliers.paymentTerms
--   contact_name           <- jfpro.suppliers.contactPerson
--   contact_email          <- jfpro.suppliers.email
--   deleted_at             <- (is_deleted ? updated_at : NULL)
-- `country_code` (char2) is kept distinct from jfpro's full-name countryOfOrigin.
--
-- Also adds provenance columns (real columns, not customFields):
--   source  -> 'shipsline' on the shipping-only suppliers merged in, so they can
--              be filtered/ignored later as legacy. NULL on native JFPro rows.
--   jfa_id  -> the original jfa.suppliers.id (set on inserted AND merged rows).
--
-- RUN ONCE, as admin:
--   node tools/sql.js -f src/db/migrations/2026-06-30_jfpro_suppliers_add_columns.sql
-- (Re-running errors on "Duplicate column" — that is expected, it is not idempotent.)
-- REQUIRES JFPro-team sign-off (it mutates a JFPro-owned table).

ALTER TABLE jfpro.suppliers
  ADD COLUMN portal_code      VARCHAR(32) NULL,
  ADD COLUMN code             VARCHAR(64) NULL,
  ADD COLUMN country_code     CHAR(2)     NULL,
  ADD COLUMN default_currency CHAR(3)     NULL,
  ADD COLUMN source           VARCHAR(32) NULL,
  ADD COLUMN jfa_id           INT         NULL,
  ADD KEY idx_suppliers_portal_code (portal_code),
  ADD KEY idx_suppliers_source (source);
