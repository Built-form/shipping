-- Auto-assign a supplier portal access code the moment a new jfpro.suppliers row
-- is created (e.g. from the JFPro app, manual insert, or import) — so the public
-- supplier "ready date" portal works for a new supplier immediately, instead of
-- waiting for a shipping Lambda cold start to backfill it via ensureSupplierPortalCodes.
-- That helper stays as a backstop for any pre-existing code-less rows.
--
-- Pure-SQL 8-char generator over the same unambiguous alphabet as
-- src/lib/portal-code.js (no confusable 0/1/I/L/O). Randomness is RAND(), not
-- crypto.randomBytes — acceptable here: the code is one factor of a name + PO
-- scoped lookup, not a standalone secret, and is never enumerated to the public.
--
-- Single-statement trigger body (no BEGIN/END), so it carries no internal
-- semicolons and runs cleanly through tools/sql.js. Idempotent (DROP IF EXISTS).
--
-- Run as admin:
--   node tools/sql.js -f src/db/migrations/2026-06-30_jfpro_portal_code_trigger.sql

DROP TRIGGER IF EXISTS jfpro.suppliers_portal_code_bi;

CREATE DEFINER = 'admin'@'%' TRIGGER jfpro.suppliers_portal_code_bi
BEFORE INSERT ON jfpro.suppliers
FOR EACH ROW
SET NEW.portal_code = IF(
  NEW.portal_code IS NULL OR NEW.portal_code = '',
  CONCAT(
    SUBSTRING('23456789ABCDEFGHJKMNPQRSTUVWXYZ', FLOOR(RAND() * 31) + 1, 1),
    SUBSTRING('23456789ABCDEFGHJKMNPQRSTUVWXYZ', FLOOR(RAND() * 31) + 1, 1),
    SUBSTRING('23456789ABCDEFGHJKMNPQRSTUVWXYZ', FLOOR(RAND() * 31) + 1, 1),
    SUBSTRING('23456789ABCDEFGHJKMNPQRSTUVWXYZ', FLOOR(RAND() * 31) + 1, 1),
    SUBSTRING('23456789ABCDEFGHJKMNPQRSTUVWXYZ', FLOOR(RAND() * 31) + 1, 1),
    SUBSTRING('23456789ABCDEFGHJKMNPQRSTUVWXYZ', FLOOR(RAND() * 31) + 1, 1),
    SUBSTRING('23456789ABCDEFGHJKMNPQRSTUVWXYZ', FLOOR(RAND() * 31) + 1, 1),
    SUBSTRING('23456789ABCDEFGHJKMNPQRSTUVWXYZ', FLOOR(RAND() * 31) + 1, 1)
  ),
  NEW.portal_code
);
