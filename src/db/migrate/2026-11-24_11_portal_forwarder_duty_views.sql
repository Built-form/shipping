-- What the portal may read of a CONTAINER, now that a container has three
-- duties and may have a forwarder for each (docs/forwarder-containers.md,
-- 2026-11-24). Replaces portal_forwarder_containers_v of 2026-11-23_11. A view
-- replaced with CREATE OR REPLACE keeps its grants: the portal's login needs no
-- new one, and gains no right on anything under it.
--
-- THE RULE, held twice as before: here, and in src/lib/forwarder-containers.js
-- dutiesOf, which the routes apply to rows they have locked.
-- tools/test-portal-forwarder-container-view.js holds the two together.
--
--   freight    the live booking that covers freight (scope port_to_port or
--              end_to_end), else shipments.forwarder_id
--   delivery   the live booking that covers delivery (scope end_to_end), else
--              shipments.delivery_forwarder_id
--   customs    shipments.customs_forwarder_id (picked by hand only)
--   For each: a covering booking must be CONFIRMED (one still requested leaves
--   THAT duty with nobody, the pick included), the holder must be on the list
--   and not removed, and the container must be real (not deleted, not merged
--   away, with a reference, a STORED stage from BOOKED on).
--
-- forwarder_duty_holders_v is that rule as rows: one per container and duty
-- that has a holder. It is NOT granted to the portal.
--
-- portal_forwarder_containers_v: ONE ROW PER CONTAINER AND FORWARDER, as before.
--
--   link_source 'booking' | 'container'   the row of the FREIGHT holder, and only
--     that one: exactly what those two words meant until now. A portal build
--     from before this migration treats only these as rows it may write for, so
--     it can never write for the two kinds of holder it does not know.
--   link_source 'duty'   a forwarder that holds delivery and/or customs and not
--     freight. Full for a portal build that reads the duty flags, read-only for
--     an older one.
--   link_source 'payment'   as before: a forwarder that holds nothing on the
--     container and was SENT a payment with a charge on it. Every column but the
--     ids, reference, mode and link_source is NULL (the flags are 0).
--
--   does_freight, does_delivery, does_customs   0 or 1: what this forwarder
--     holds. What it may send follows from them (FIELD_DUTY, KIND_DUTY).
--   delivery_date   the delivery date the container holds, as text to the
--     minute: 'YYYY-MM-DD' when the stored time is 00:00, else
--     'YYYY-MM-DDTHH:MM'. It is the member orders' delivery_date when EVERY
--     order holds the same one, and NULL when none does or they differ.
--   booking_ref, freight_request_id   the FREIGHT booking's, on the freight
--     holder's row only: another company is never handed a link to that quote.
--   sailing_date, port_from, port_to, container_size   the confirmed freight
--     booking's facts, on every full row: which port, what size.
--   finished   arrived, and more than 14 days after the later of the arrival
--     and the latest delivery day held, on this server's UTC date. While a
--     delivery holder exists and no order holds a delivery date, not finished
--     until the container is CLOSED (every order received). One answer for
--     every row of the container.
--
-- New columns are added at the END: every reader names its columns.
--
-- NOT in this view, for anyone: suppliers, lines, quantities, prices, notes.
--
-- Rollback: none needed. The view is additive for every older reader (shipping,
-- the portal), and the flags are 1, 0, 0 on every row while no delivery pick,
-- customs pick or end-to-end booking exists.

CREATE OR REPLACE SQL SECURITY DEFINER VIEW forwarder_duty_holders_v AS
SELECT
  s.id                                               AS shipment_id,
  'freight'                                          AS duty,
  IF(bk.id IS NULL, s.forwarder_id, fr.forwarder_id) AS forwarder_id,
  IF(bk.id IS NULL, 'container', 'booking')          AS source,
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
  'delivery',
  IF(bk.id IS NULL, s.delivery_forwarder_id, fr.forwarder_id),
  IF(bk.id IS NULL, 'container', 'booking'),
  bk.id
FROM shipments s
LEFT JOIN freight_bookings bk ON bk.shipment_id = s.id AND bk.status IN ('requested', 'confirmed')
                             AND bk.scope = 'end_to_end'
LEFT JOIN freight_quote_requests fr ON fr.id = bk.request_id
JOIN forwarders f ON f.id = IF(bk.id IS NULL, s.delivery_forwarder_id, fr.forwarder_id) AND f.deleted_at IS NULL
WHERE s.deleted_at IS NULL AND s.merged_into_id IS NULL
  AND NULLIF(TRIM(s.reference), '') IS NOT NULL
  AND s.stage IN ('BOOKED', 'IN_TRANSIT', 'ARRIVED', 'CLOSED')
  AND (bk.id IS NULL OR bk.status = 'confirmed')

UNION ALL

SELECT
  s.id,
  'customs',
  s.customs_forwarder_id,
  'container',
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
  IF(a.does_freight = 1, a.freight_source, 'duty') AS link_source,
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
                          IF(DATE_FORMAT(o.delivery_date, '%H:%i') = '00:00', DATE_FORMAT(o.delivery_date, '%Y-%m-%d'), DATE_FORMAT(o.delivery_date, '%Y-%m-%dT%H:%i')),
                          NULL)) = 1,
                  MAX(IF(o.delivery_date >= '1000-01-01 00:00:00',
                         IF(DATE_FORMAT(o.delivery_date, '%H:%i') = '00:00', DATE_FORMAT(o.delivery_date, '%Y-%m-%d'), DATE_FORMAT(o.delivery_date, '%Y-%m-%dT%H:%i')),
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
  'payment'       AS link_source,
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
