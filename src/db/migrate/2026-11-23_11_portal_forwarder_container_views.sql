-- What the portal may read of a CONTAINER, for a freight forwarder's contact
-- (docs/forwarder-containers.md): one new view, and one more column on the
-- payment lines view so a payment can be shown on the container it pays.
--
-- A view for the same reason as 2026-11-20_21: it holds only the containers a
-- forwarder may see and, of a container, only what a forwarder is told. The
-- portal's login gets SELECT on it (2026-11-23_12) and no new right on
-- shipments, orders or freight_bookings.
--
-- WHO SEES A ROW is decided by forwarder id, never by a name. The view lists
-- EVERY forwarder's containers: the portal must join it as
--   contact (the signed-in address) -> a row with one of that contact's
--   forwarder ids -> a submission with the SAME shipment id AND forwarder id.
--
-- TWO KINDS OF ROW, one per container and forwarder:
--
-- link_source 'booking' | 'container'  (a FULL row: the forwarder may send its
--   number, dates and files). The rule of src/lib/forwarder-containers.js
--   ownerOf, which the routes apply to rows they have locked:
--     - a real container: not deleted, not merged away, with a reference, and
--       a STORED stage from BOOKED on;
--     - its forwarder is the live booking's (requested or confirmed; at most
--       one per container, uq_freight_bookings_live_shipment), else the pair
--       on the container itself;
--     - a live booking must be CONFIRMED: one still `requested` shows the
--       container to nobody, a forwarder picked by hand included;
--     - the forwarder is on the list and not removed.
--   Of the container: `stage`, `ecn`, `etd`, `eta` and `vessel_name` follow
--   the API's rule for a booked container (lib/shipments.js rowToShipment,
--   shipment-sync.js shipmentSelect): the member orders win over the stored
--   copies, and the stage is the later of stored and what the orders justify.
--   `arrived_on`: shipments.ata, else the latest member arrived_date.
--   `finished`: more than 14 days after that, on this server's UTC date, so
--   the portal and shipping never disagree about it.
--
-- link_source 'payment'  (a PAYMENT-ONLY row): a real container, and a
--   forwarder with no full row for it, that was SENT a payment with a charge on
--   it (portal_forwarder_payment_lines_v: sent payments only, failing closed).
--   Every column but the ids, reference, mode and link_source is NULL: a
--   forwarder paid on a container learns nothing of it beyond the number its
--   payment already tells them. Every write the portal makes refuses it.
--
-- NOT in this view, for anyone: suppliers, lines, quantities, prices, notes.
--
-- SQL SECURITY DEFINER and CREATE OR REPLACE, as 2026-11-16_21.
--
-- Rollback (once no deployed portal reads it): DROP VIEW
-- portal_forwarder_containers_v. portal_forwarder_payment_lines_v stays as it
-- is here: one more column harms no older reader (they name their columns).

-- The charges of a sent forwarder payment (2026-11-20_21), with the container
-- each is on: the charge's own shipment id, else the live container whose
-- number the charge carries (references are unique).
CREATE OR REPLACE SQL SECURITY DEFINER VIEW portal_forwarder_payment_lines_v AS
SELECT
  l.id          AS line_id,
  l.payment_id  AS payment_id,
  l.amount      AS amount,
  ex.kind       AS extra_kind,
  COALESCE(sh.reference, ex.shipment_reference) AS shipment_reference,
  COALESCE(ex.shipment_id,
           (SELECT MIN(s2.id) FROM shipments s2
             WHERE s2.deleted_at IS NULL AND s2.merged_into_id IS NULL
               AND CONVERT(s2.reference USING utf8mb4) COLLATE utf8mb4_0900_bin
                 = CONVERT(ex.shipment_reference USING utf8mb4) COLLATE utf8mb4_0900_bin)) AS shipment_id
FROM supplier_payment_lines l
JOIN portal_forwarder_payments_v p ON p.payment_id = l.payment_id
JOIN payment_extras ex ON l.target_kind = 'extra' AND ex.id = l.target_id
LEFT JOIN shipments sh ON sh.id = ex.shipment_id;

CREATE OR REPLACE SQL SECURITY DEFINER VIEW portal_forwarder_containers_v AS
SELECT
  s.id                                            AS shipment_id,
  IF(bk.id IS NULL, s.forwarder_id, fr.forwarder_id) AS forwarder_id,
  IF(bk.id IS NULL, 'container', 'booking')       AS link_source,
  s.reference                                     AS reference,
  s.mode                                          AS mode,
  ELT(GREATEST(FIELD(s.stage, 'BOOKED','IN_TRANSIT','ARRIVED','CLOSED'),
               FIELD(CASE WHEN COALESCE(m.member_count, 0) = 0 THEN ''
                          WHEN m.n_terminal = m.member_count THEN 'CLOSED'
                          WHEN m.n_arrived + m.n_terminal > 0 THEN 'ARRIVED'
                          WHEN m.n_transit > 0 THEN 'IN_TRANSIT'
                          ELSE 'BOOKED' END, 'BOOKED','IN_TRANSIT','ARRIVED','CLOSED')),
      'BOOKED','IN_TRANSIT','ARRIVED','CLOSED')   AS stage,
  COALESCE(IF(s.mode = 'AIR', COALESCE(m.member_awb, m.member_ext), COALESCE(m.member_ext, m.member_awb)),
           NULLIF(TRIM(s.tracking_ref), ''))      AS ecn,
  s.loading_date                                  AS loading_date,
  COALESCE(m.member_etd, s.etd)                   AS etd,
  COALESCE(m.member_eta, s.eta)                   AS eta,
  COALESCE(s.ata, m.member_arrived)               AS arrived_on,
  (COALESCE(s.ata, m.member_arrived) IS NOT NULL
     AND UTC_DATE() > DATE_ADD(COALESCE(s.ata, m.member_arrived), INTERVAL 14 DAY)) AS finished,
  COALESCE(m.member_vessel, NULLIF(TRIM(s.vessel_name), '')) AS vessel_name,
  NULLIF(TRIM(s.origin_port), '')                 AS origin_port,
  COALESCE(NULLIF(TRIM(bk.booking_ref), ''), NULLIF(TRIM(s.booking_ref), '')) AS booking_ref,
  opt.sailing_date                                AS sailing_date,
  opt.port_from                                   AS port_from,
  opt.port_to                                     AS port_to,
  opt.container_size                              AS container_size,
  bk.request_id                                   AS freight_request_id
FROM shipments s
LEFT JOIN freight_bookings bk ON bk.shipment_id = s.id AND bk.status IN ('requested', 'confirmed')
LEFT JOIN freight_quote_requests fr ON fr.id = bk.request_id
LEFT JOIN freight_quote_options opt ON opt.id = bk.option_id
JOIN forwarders f ON f.id = IF(bk.id IS NULL, s.forwarder_id, fr.forwarder_id) AND f.deleted_at IS NULL
LEFT JOIN (
    SELECT o.shipment_id,
           COUNT(*) AS member_count,
           SUM(o.status IN ('RECEIVED', 'PARTIALLY_RECEIVED', 'DESTROYED')) AS n_terminal,
           SUM(o.status = 'ARRIVED_AT_WAREHOUSE') AS n_arrived,
           SUM(o.status IN ('ON_SEA', 'ON_AIR')) AS n_transit,
           MAX(o.eta) AS member_eta,
           MAX(o.estimated_departure_date) AS member_etd,
           MAX(o.arrived_date) AS member_arrived,
           MAX(NULLIF(TRIM(o.vessel_name), '')) AS member_vessel,
           MAX(NULLIF(TRIM(o.external_container_number), '')) AS member_ext,
           MAX(NULLIF(TRIM(o.awb_number), '')) AS member_awb
      FROM orders o
     WHERE o.deleted_at IS NULL AND o.shipment_id IS NOT NULL
     GROUP BY o.shipment_id
) m ON m.shipment_id = s.id
WHERE s.deleted_at IS NULL AND s.merged_into_id IS NULL
  AND NULLIF(TRIM(s.reference), '') IS NOT NULL
  AND s.stage IN ('BOOKED', 'IN_TRANSIT', 'ARRIVED', 'CLOSED')
  AND (bk.id IS NULL OR bk.status = 'confirmed')

UNION ALL

SELECT
  s.id            AS shipment_id,
  paid.forwarder_id AS forwarder_id,
  'payment'       AS link_source,
  s.reference     AS reference,
  s.mode          AS mode,
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
FROM shipments s
JOIN (SELECT DISTINCT pl.shipment_id, pp.forwarder_id
        FROM portal_forwarder_payment_lines_v pl
        JOIN portal_forwarder_payments_v pp ON pp.payment_id = pl.payment_id
       WHERE pl.shipment_id IS NOT NULL) paid ON paid.shipment_id = s.id
WHERE s.deleted_at IS NULL AND s.merged_into_id IS NULL
  AND NULLIF(TRIM(s.reference), '') IS NOT NULL
  AND s.stage IN ('BOOKED', 'IN_TRANSIT', 'ARRIVED', 'CLOSED')
  AND NOT EXISTS (
        SELECT 1
          FROM shipments s3
          LEFT JOIN freight_bookings bk3 ON bk3.shipment_id = s3.id AND bk3.status IN ('requested', 'confirmed')
          LEFT JOIN freight_quote_requests fr3 ON fr3.id = bk3.request_id
          JOIN forwarders f3 ON f3.id = IF(bk3.id IS NULL, s3.forwarder_id, fr3.forwarder_id) AND f3.deleted_at IS NULL
         WHERE s3.id = s.id
           AND (bk3.id IS NULL OR bk3.status = 'confirmed')
           AND IF(bk3.id IS NULL, s3.forwarder_id, fr3.forwarder_id) = paid.forwarder_id);
