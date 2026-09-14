-- This app's OWN user allowlist.
--
-- `allowed_emails` in this schema is SHARED: joshdex's API (perp.js) runs
-- against jfa too, creates that table itself and exposes its own
-- /allowed-emails CRUD, so neither app could add or remove a user without
-- changing who can log into the other. This table is seeded once with a copy of
-- those rows and is ShipLine's from then on; `allowed_emails` is left untouched
-- for joshdex.
--
-- Checked by the auth middleware in src/handlers/orders.js on every request
-- (no cache — a grant or revoke lands on that user's next request) and managed
-- through /api/v1/users. `id` is a surrogate: email is the real identity (it's
-- what the Google JWT claim matches), but audit_log.entity_id is an INT, so
-- user changes need an integer key to be audited like every other entity.
--
-- Created at runtime by src/lib/allowed-emails.js (ensureAllowedEmailsSchema).
-- This file documents the canonical shape and lets the schema be applied ahead
-- of a deploy. Idempotent.

CREATE TABLE IF NOT EXISTS shipping_allowed_emails (
    id           INT NOT NULL AUTO_INCREMENT,
    email        VARCHAR(255) NOT NULL,        -- stored lower-cased; matched against the JWT email claim
    display_name VARCHAR(255) NULL,
    -- Free-form, not an enum: the roles on offer are SELECT DISTINCT over this
    -- column (served by /api/v1/user-types), so adding one is assigning it to a
    -- user, not a deploy. 'standard' (default) and 'admin' (the gate on the
    -- admin-only routes) are the only two the code treats specially.
    type         VARCHAR(32) NOT NULL DEFAULT 'standard',
    created_at   TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_email (email)
);

-- One-time seed from the shared table. The code runs this only when
-- shipping_allowed_emails is EMPTY, so a later cold start never re-copies —
-- otherwise removing a user here would resurrect them from `allowed_emails`.
INSERT IGNORE INTO shipping_allowed_emails (email, type, created_at)
SELECT LOWER(email), COALESCE(NULLIF(type, ''), 'standard'), created_at
  FROM allowed_emails;
