-- Replace the static jfa.mintsoft_carton_sizes table as the carton-data source
-- with a live view over jfpro.products (the authoritative product catalogue).
--
-- Context: nothing writes to jfa.mintsoft_carton_sizes anymore (the Mintsoft
-- sync was retired); it had carton_weight = 0.000 on every row. jfpro.products
-- holds real grossCartonWeightKg, unitsPerCarton, carton L/W/H and a precomputed
-- cartonCbm. jfcode is effectively unique in products (805/805 distinct), so the
-- jf_code join does not fan out. Cross-DB read jfa -> jfpro is permitted.
--
-- The view re-exposes products under the OLD mintsoft_carton_sizes column names
-- and keys so the existing app SELECTs (orders.js: forwarder/supplier/draft
-- loaders, the /mintsoft/carton-sizes listing, and the two asin-keyed lookups)
-- are a drop-in repoint of FROM mintsoft_carton_sizes -> FROM product_carton_sizes.
--
-- Caveats baked in (see also docs):
--   * Unit-level weight/height/width/depth have no source in products -> 0.
--     (amazon_item_*_cm are surfaced where present but are mostly NULL.)
--   * product_id (a Mintsoft id) has no equivalent -> NULL.
--   * sku has no clean equivalent -> aliased to jfcode (matches legacy rows
--     where sku == jf_code).
--   * The three carton dims are the same SET as the legacy row but may be
--     labelled differently (length<->height, height<->depth); CBM is identical.
--   * Old jf_codes absent from products (≈38, mostly End-Of-Line) are dropped
--     by design — all live products are expected to exist in jfpro.products.
--
-- Rollback: DROP VIEW jfa.product_carton_sizes; and revert the orders.js repoint.
--
-- DEFINER is pinned to 'admin'@'%' with SQL SECURITY DEFINER so the cross-DB
-- read of jfpro.products always runs with admin's privileges, regardless of
-- which user the app authenticates as through the RDS Proxy. This matches the
-- view already live on the instance; re-applying this file reproduces it
-- deterministically. (Requires running the migration as an account allowed to
-- SET the definer, i.e. admin.)

CREATE OR REPLACE DEFINER = 'admin'@'%' SQL SECURITY DEFINER VIEW jfa.product_carton_sizes AS
SELECT
  p.id                                                        AS id,
  p.jfcode                                                    AS jf_code,
  COALESCE(p.asin, '')                                        AS asin,
  p.jfcode                                                    AS sku,
  NULL                                                        AS product_id,
  CAST(0 AS DECIMAL(10,3))                                    AS weight,
  CAST(COALESCE(p.amazon_item_height_cm, 0) AS DECIMAL(10,3)) AS height,
  CAST(COALESCE(p.amazon_item_width_cm, 0)  AS DECIMAL(10,3)) AS width,
  CAST(COALESCE(p.amazon_item_length_cm, 0) AS DECIMAL(10,3)) AS depth,
  CAST(COALESCE(p.unitsPerCarton, 0) AS SIGNED)               AS carton_qty,
  CAST(COALESCE(NULLIF(p.grossCartonWeightKg, 0), p.netCartonWeightKg, 0) AS DECIMAL(10,3)) AS carton_weight,
  CAST(COALESCE(p.cartonHeightCm, 0) AS DECIMAL(10,3))        AS carton_height,
  CAST(COALESCE(p.cartonWidthCm, 0)  AS DECIMAL(10,3))        AS carton_width,
  CAST(COALESCE(p.cartonLengthCm, 0) AS DECIMAL(10,3))        AS carton_depth,
  p.cartonCbm                                                 AS carton_cbm,
  p.created_at                                                AS created_at,
  p.updated_at                                                AS updated_at
FROM jfpro.products p
WHERE TRIM(COALESCE(p.jfcode, '')) <> '';
