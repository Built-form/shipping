-- THE FIX for the forwarder screens failing on production only (2026-11-25):
-- "Illegal mix of collations (utf8mb4_0900_ai_ci,COERCIBLE) and
-- (utf8mb4_unicode_ci,COERCIBLE) for operation '='" on every read of
-- portal_forwarder_containers_v. The forwarder screens never worked on
-- production: the view of 2026-11-23 failed the second way below, the view of
-- 2026-11-24 the first.
--
-- Why (src/lib/view-collations.js has the whole story): a view keeps the
-- collation of the session that created it. This runner connects direct
-- (utf8mb4_unicode_ci); the live Lambdas connect through the RDS Proxy, whose
-- sessions run under the server default (utf8mb4_0900_ai_ci). TEST connects
-- direct, so it never showed.
--
-- The two views of 2026-11-24_11, unchanged in what they answer, with every
-- string made safe under either session:
--
--   1. The comparison that broke every read:
--          DATE_FORMAT(o.delivery_date, '%H:%i') = '00:00'
--      (the session's collation against the view's) is now
--          HOUR(o.delivery_date) = 0 AND MINUTE(o.delivery_date) = 0
--      Numbers have no collation. Same answer on all 587 dated orders of TEST.
--   2. Every column that was text made only of literals (duty, source,
--      link_source, and stage: ELT over four literals) is wrapped in
--      CONVERT(… USING utf8mb4), in EVERY branch of a UNION (one bare branch
--      undoes the others). Such a column clashes with a literal in the
--      CALLER's query (link_source IN ('booking')), which is how the
--      2026-11-23 view failed. CONVERT's answer is utf8mb4_0900_ai_ci in every
--      session and behaves like a real column: a caller's literal yields to it.
--
-- The values are the same text as before. link_source, stage, duty and source
-- now report utf8mb4_0900_ai_ci, the collation of every table here.
--
-- Nothing else differs from 2026-11-24_11: tools/test-view-collations-lib.js
-- rebuilds this file's statements from that one and compares them.
--
-- CREATE OR REPLACE keeps the grants (the portal's SELECT on
-- portal_forwarder_containers_v). No new right for anyone.
--
-- Checked after every --apply by the runner (view collation check), which
-- fails a deploy when a view cannot be read under either collation.
--
-- Rollback: none needed. To go back, re-run the two statements of
-- 2026-11-24_11 by hand (production then fails again as before).

CREATE OR REPLACE SQL SECURITY DEFINER VIEW forwarder_duty_holders_v AS
SELECT
  s.id                                               AS shipment_id,
  CONVERT('freight' USING utf8mb4)                   AS duty,
  IF(bk.id IS NULL, s.forwarder_id, fr.forwarder_id) AS forwarder_id,
  CONVERT(IF(bk.id IS NULL, 'container', 'booking') USING utf8mb4) AS source,
  bk.id                                              AS booking_id
FROM shipments s
LEFT JOIN freight_bookings bk ON bk.shipment_id = s.id AND bk.status IN ('requested', 'confirmed')
                             AND bk.scope IN ('port_to_port', 'end_to_end')
LEFT JOIN freight_quote_requests fr ON fr.id = bk.request_id
JOIN forwarders f ON f.id = IF(bk.id IS NULL, s.forwarder_id, fr.forwarder_id) AND f.deleted_at IS NULL
WHERE s.deleted_at IS NULL AND s.merged_into_id IS NULL
  AND NULLIF(TRIM(s.reference), '') IS NOT NULL
  AND s.stage IN ('BOOKED', 'IN_TRANSIT', 'ARRIVED', 'CLOSED')
  AND (bk.id IS NULL OR bk.status = 'confirmed')

UNION ALL

SELECT
  s.id,
  CONVERT('delivery' USING utf8mb4),
  IF(bk.id IS NULL, s.delivery_forwarder_id, fr.forwarder_id),
  CONVERT(IF(bk.id IS NULL, 'container', 'booking') USING utf8mb4),
  bk.id
FROM shipments s
LEFT JOIN freight_bookings bk ON bk.shipment_id = s.id AND bk.status IN ('requested', 'confirmed')
                             AND bk.scope IN ('end_to_end', 'delivery')
LEFT JOIN freight_quote_requests fr ON fr.id = bk.request_id
JOIN forwarders f ON f.id = IF(bk.id IS NULL, s.delivery_forwarder_id, fr.forwarder_id) AND f.deleted_at IS NULL
WHERE s.deleted_at IS NULL AND s.merged_into_id IS NULL
  AND NULLIF(TRIM(s.reference), '') IS NOT NULL
  AND s.stage IN ('BOOKED', 'IN_TRANSIT', 'ARRIVED', 'CLOSED')
  AND (bk.id IS NULL OR bk.status = 'confirmed')

UNION ALL

SELECT
  s.id,
  CONVERT('customs' USING utf8mb4),
  s.customs_forwarder_id,
  CONVERT('container' USING utf8mb4),
  NULL
FROM shipments s
JOIN forwarders f ON f.id = s.customs_forwarder_id AND f.deleted_at IS NULL
WHERE s.deleted_at IS NULL AND s.merged_into_id IS NULL
  AND NULLIF(TRIM(s.reference), '') IS NOT NULL
  AND s.stage IN ('BOOKED', 'IN_TRANSIT', 'ARRIVED', 'CLOSED');

CREATE OR REPLACE SQL SECURITY DEFINER VIEW portal_forwarder_containers_v AS
SELECT
  c.shipment_id                                   AS shipment_id,
  a.forwarder_id                                  AS forwarder_id,
  CONVERT(IF(a.does_freight = 1, a.freight_source, 'duty') USING utf8mb4) AS link_source,
  c.reference                                     AS reference,
  c.mode                                          AS mode,
  c.stage                                         AS stage,
  c.ecn                                           AS ecn,
  c.loading_date                                  AS loading_date,
  c.etd                                           AS etd,
  c.eta                                           AS eta,
  c.arrived_on                                    AS arrived_on,
  (c.arrived_on IS NOT NULL
     AND UTC_DATE() > DATE_ADD(GREATEST(c.arrived_on, COALESCE(c.delivery_day, c.arrived_on)), INTERVAL 14 DAY)
     AND NOT (c.has_delivery_holder = 1 AND c.delivery_day IS NULL AND c.stage <> 'CLOSED')) AS finished,
  c.vessel_name                                   AS vessel_name,
  c.origin_port                                   AS origin_port,
  IF(a.does_freight = 1, c.freight_booking_ref, NULL) AS booking_ref,
  c.sailing_date                                  AS sailing_date,
  c.port_from                                     AS port_from,
  c.port_to                                       AS port_to,
  c.container_size                                AS container_size,
  IF(a.does_freight = 1, c.freight_request_id, NULL) AS freight_request_id,
  a.does_freight                                  AS does_freight,
  a.does_delivery                                 AS does_delivery,
  a.does_customs                                  AS does_customs,
  c.delivery_date                                 AS delivery_date
FROM (
    SELECT h.shipment_id, h.forwarder_id,
           MAX(h.duty = 'freight')  AS does_freight,
           MAX(h.duty = 'delivery') AS does_delivery,
           MAX(h.duty = 'customs')  AS does_customs,
           MAX(IF(h.duty = 'freight', h.source, NULL)) AS freight_source
      FROM forwarder_duty_holders_v h
     GROUP BY h.shipment_id, h.forwarder_id
) a
JOIN (
    SELECT
      s.id        AS shipment_id,
      s.reference AS reference,
      s.mode      AS mode,
      CONVERT(ELT(GREATEST(FIELD(s.stage, 'BOOKED','IN_TRANSIT','ARRIVED','CLOSED'),
                   FIELD(CASE WHEN COALESCE(m.member_count, 0) = 0 THEN ''
                              WHEN m.n_terminal = m.member_count THEN 'CLOSED'
                              WHEN m.n_arrived + m.n_terminal > 0 THEN 'ARRIVED'
                              WHEN m.n_transit > 0 THEN 'IN_TRANSIT'
                              ELSE 'BOOKED' END, 'BOOKED','IN_TRANSIT','ARRIVED','CLOSED')),
          'BOOKED','IN_TRANSIT','ARRIVED','CLOSED') USING utf8mb4) AS stage,
      COALESCE(IF(s.mode = 'AIR', COALESCE(m.member_awb, m.member_ext), COALESCE(m.member_ext, m.member_awb)),
               NULLIF(TRIM(s.tracking_ref), ''))      AS ecn,
      s.loading_date                                  AS loading_date,
      COALESCE(m.member_etd, s.etd)                   AS etd,
      COALESCE(m.member_eta, s.eta)                   AS eta,
      COALESCE(s.ata, m.member_arrived)               AS arrived_on,
      COALESCE(m.member_vessel, NULLIF(TRIM(s.vessel_name), '')) AS vessel_name,
      NULLIF(TRIM(s.origin_port), '')                 AS origin_port,
      COALESCE(NULLIF(TRIM(bk.booking_ref), ''), NULLIF(TRIM(s.booking_ref), '')) AS freight_booking_ref,
      opt.sailing_date                                AS sailing_date,
      opt.port_from                                   AS port_from,
      opt.port_to                                     AS port_to,
      opt.container_size                              AS container_size,
      bk.request_id                                   AS freight_request_id,
      m.member_delivery                               AS delivery_date,
      m.member_delivery_day                           AS delivery_day,
      COALESCE(dh.has_delivery, 0)                    AS has_delivery_holder
    FROM shipments s
    LEFT JOIN freight_bookings bk ON bk.shipment_id = s.id AND bk.status = 'confirmed'
                                 AND bk.scope IN ('port_to_port', 'end_to_end')
    LEFT JOIN freight_quote_options opt ON opt.id = bk.option_id
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
               MAX(NULLIF(TRIM(o.awb_number), '')) AS member_awb,
               IF(COUNT(IF(o.delivery_date >= '1000-01-01 00:00:00', 1, NULL)) = COUNT(*)
                    AND COUNT(DISTINCT IF(o.delivery_date >= '1000-01-01 00:00:00',
                          IF(HOUR(o.delivery_date) = 0 AND MINUTE(o.delivery_date) = 0, DATE_FORMAT(o.delivery_date, '%Y-%m-%d'), DATE_FORMAT(o.delivery_date, '%Y-%m-%dT%H:%i')),
                          NULL)) = 1,
                  MAX(IF(o.delivery_date >= '1000-01-01 00:00:00',
                         IF(HOUR(o.delivery_date) = 0 AND MINUTE(o.delivery_date) = 0, DATE_FORMAT(o.delivery_date, '%Y-%m-%d'), DATE_FORMAT(o.delivery_date, '%Y-%m-%dT%H:%i')),
                         NULL)),
                  NULL) AS member_delivery,
               MAX(IF(o.delivery_date >= '1000-01-01 00:00:00', DATE(o.delivery_date), NULL)) AS member_delivery_day
          FROM orders o
         WHERE o.deleted_at IS NULL AND o.shipment_id IS NOT NULL
         GROUP BY o.shipment_id
    ) m ON m.shipment_id = s.id
    LEFT JOIN (
        SELECT h2.shipment_id, MAX(h2.duty = 'delivery') AS has_delivery
          FROM forwarder_duty_holders_v h2
         GROUP BY h2.shipment_id
    ) dh ON dh.shipment_id = s.id
    WHERE s.deleted_at IS NULL AND s.merged_into_id IS NULL
      AND NULLIF(TRIM(s.reference), '') IS NOT NULL
      AND s.stage IN ('BOOKED', 'IN_TRANSIT', 'ARRIVED', 'CLOSED')
) c ON c.shipment_id = a.shipment_id

UNION ALL

SELECT
  s.id            AS shipment_id,
  paid.forwarder_id AS forwarder_id,
  CONVERT('payment' USING utf8mb4) AS link_source,
  s.reference     AS reference,
  s.mode          AS mode,
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
  0, 0, 0, NULL
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
          FROM forwarder_duty_holders_v h3
         WHERE h3.shipment_id = s.id
           AND h3.forwarder_id = paid.forwarder_id);
