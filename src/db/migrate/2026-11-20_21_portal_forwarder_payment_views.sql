-- What the portal may read of a payment sent to a FREIGHT FORWARDER
-- (docs/payment-proof-send.md, "A forwarder's payment"): three more views, and
-- the supplier's first view tightened so the two audiences can never meet.
--
-- Views for the same reason as 2026-11-16_21: they hold SENT payments only,
-- and of a payment only what its forwarder is told. The portal's login gets
-- SELECT on the three (2026-11-20_22) and no right on a payment table.
--
-- WHO SEES A ROW is decided by forwarder id, never by a name: the portal lists
-- these rows WHERE forwarder_id IN (the forwarders whose contact list holds
-- the signed-in address). A payment is a forwarder's when EVERY line of it is
-- a live forwarder charge (payment_extras, rides_with 'shipment') linked to
-- that one forwarder (payment_extras.forwarder_id), and it was sent to that
-- same forwarder (supplier_payment_sends.forwarder_id). Anything else keeps
-- the whole payment out: a line of any other kind, a deleted or missing
-- charge, a charge with a typed payee, charges of two forwarders, a forwarder
-- taken off the list.
--
-- Live, like the supplier's: an edit shows at once. A payment re-pointed at
-- another forwarder leaves the first one's page; pointed back, it shows again
-- without a new send (to the forwarder it was sent to, and to nobody else).
--
-- SQL SECURITY DEFINER and CREATE OR REPLACE, as 2026-11-16_21.
--
-- Rollback (once no deployed portal reads them): DROP VIEW
-- portal_forwarder_payment_proofs_v, portal_forwarder_payment_lines_v,
-- portal_forwarder_payments_v. portal_payments_v stays as it is here: its two
-- added conditions are right for older code too.

-- The supplier's view, with two conditions added (the rest is 2026-11-16_21's):
--   a send counts only when it was a supplier's part (deposit or balance), so
--   a send to a forwarder never lists a payment to a supplier who happens to
--   have a supplier record of exactly the forwarder's name;
--   the transfer holds no forwarder charge now, so an old supplier send does
--   not keep showing a payment that was later turned into a forwarder's.
CREATE OR REPLACE SQL SECURITY DEFINER VIEW portal_payments_v AS
SELECT
  sp.id             AS payment_id,
  sp.supplier_name  AS supplier_name,
  sp.amount         AS amount,
  sp.currency       AS currency,
  sp.paid_on        AS paid_on,
  (SELECT MAX(s.sent_at) FROM supplier_payment_sends s
    WHERE s.supplier_payment_id = sp.id AND s.status = 'sent'
      AND s.notice_kind IN ('deposit', 'balance')
      AND CONVERT(s.supplier_name USING utf8mb4) COLLATE utf8mb4_0900_bin
        = CONVERT(sp.supplier_name USING utf8mb4) COLLATE utf8mb4_0900_bin) AS last_sent_at
FROM supplier_payments sp
WHERE sp.deleted_at IS NULL
  AND NOT EXISTS (SELECT 1 FROM supplier_payment_lines fl
                    JOIN payment_extras fx ON fx.id = fl.target_id
                   WHERE fl.payment_id = sp.id AND fl.target_kind = 'extra' AND fx.rides_with = 'shipment')
  AND EXISTS (SELECT 1 FROM supplier_payment_sends s
               WHERE s.supplier_payment_id = sp.id AND s.status = 'sent'
                 AND s.notice_kind IN ('deposit', 'balance')
                 AND CONVERT(s.supplier_name USING utf8mb4) COLLATE utf8mb4_0900_bin
                   = CONVERT(sp.supplier_name USING utf8mb4) COLLATE utf8mb4_0900_bin);

-- A live transfer that is one forwarder's and was sent to that forwarder.
--
-- `own`: one row per transfer whose EVERY line is a live forwarder charge of
-- one forwarder. The aggregate runs over all the transfer's lines; the LEFT
-- JOIN finds a charge only for a line of kind 'extra' (a line of another kind
-- whose target id happens to equal a charge's id does not join), so
-- COUNT(*) = COUNT(ex.forwarder_id) fails as soon as one line is anything
-- else, deleted, missing or not linked, and MIN = MAX fails for two forwarders.
-- A transfer with no lines has no row.
--
-- `revision`: a hash of exactly what the forwarder is shown, so that the
-- portal and ShipLine agree on whether an acknowledgement is of the payment as
-- it stands. In it: the forwarder, amount, currency, paid date; every charge
-- (its own id, container, kind, amount: the charge's id, not the line's, which
-- changes on every save); every proof the proofs view lists (id, file name,
-- address). Each value is written as its length, a colon and its text, and a
-- NULL as '~', so no value can run into the next. Each row is hashed and the
-- rows are added up (order does not matter, two equal rows do not cancel, and
-- no GROUP_CONCAT length limit applies); lines and proofs are added up apart.
CREATE OR REPLACE SQL SECURITY DEFINER VIEW portal_forwarder_payments_v AS
SELECT
  sp.id             AS payment_id,
  own.forwarder_id  AS forwarder_id,
  sp.amount         AS amount,
  sp.currency       AS currency,
  sp.paid_on        AS paid_on,
  (SELECT MAX(s.sent_at) FROM supplier_payment_sends s
    WHERE s.supplier_payment_id = sp.id AND s.status = 'sent'
      AND s.notice_kind = 'forwarder' AND s.forwarder_id = own.forwarder_id) AS last_sent_at,
  SHA2(CONCAT(
    IFNULL(CONCAT(CHAR_LENGTH(CAST(own.forwarder_id AS CHAR)), ':', CAST(own.forwarder_id AS CHAR)), '~'),
    IFNULL(CONCAT(CHAR_LENGTH(CAST(sp.amount AS CHAR)), ':', CAST(sp.amount AS CHAR)), '~'),
    IFNULL(CONCAT(CHAR_LENGTH(CONVERT(sp.currency USING utf8mb4)), ':', CONVERT(sp.currency USING utf8mb4)), '~'),
    IFNULL(CONCAT(CHAR_LENGTH(DATE_FORMAT(sp.paid_on, '%Y-%m-%d')), ':', DATE_FORMAT(sp.paid_on, '%Y-%m-%d')), '~'),
    'L',
    (SELECT CONCAT(COUNT(*), ':', COALESCE(SUM(CAST(CONV(LEFT(SHA2(CONCAT(
              IFNULL(CONCAT(CHAR_LENGTH(CAST(ex2.id AS CHAR)), ':', CAST(ex2.id AS CHAR)), '~'),
              IFNULL(CONCAT(CHAR_LENGTH(CONVERT(COALESCE(sh2.reference, ex2.shipment_reference) USING utf8mb4)), ':', CONVERT(COALESCE(sh2.reference, ex2.shipment_reference) USING utf8mb4)), '~'),
              IFNULL(CONCAT(CHAR_LENGTH(CONVERT(ex2.kind USING utf8mb4)), ':', CONVERT(ex2.kind USING utf8mb4)), '~'),
              IFNULL(CONCAT(CHAR_LENGTH(CAST(l2.amount AS CHAR)), ':', CAST(l2.amount AS CHAR)), '~')
            ), 256), 15), 16, 10) AS UNSIGNED)), 0))
       FROM supplier_payment_lines l2
       JOIN payment_extras ex2 ON l2.target_kind = 'extra' AND ex2.id = l2.target_id
       LEFT JOIN shipments sh2 ON sh2.id = ex2.shipment_id
      WHERE l2.payment_id = sp.id),
    'P',
    (SELECT CONCAT(COUNT(*), ':', COALESCE(SUM(CAST(CONV(LEFT(SHA2(CONCAT(
              IFNULL(CONCAT(CHAR_LENGTH(CAST(d2.id AS CHAR)), ':', CAST(d2.id AS CHAR)), '~'),
              IFNULL(CONCAT(CHAR_LENGTH(CONVERT(d2.filename USING utf8mb4)), ':', CONVERT(d2.filename USING utf8mb4)), '~'),
              IFNULL(CONCAT(CHAR_LENGTH(CONVERT(d2.public_url USING utf8mb4)), ':', CONVERT(d2.public_url USING utf8mb4)), '~')
            ), 256), 15), 16, 10) AS UNSIGNED)), 0))
       FROM shipment_payment_documents d2
      WHERE d2.supplier_payment_id = sp.id AND d2.deleted_at IS NULL AND d2.doc_kind = 'remittance'
        AND CONVERT(d2.supplier_name USING utf8mb4) COLLATE utf8mb4_0900_bin
          = CONVERT(sp.supplier_name USING utf8mb4) COLLATE utf8mb4_0900_bin)
  ), 256)           AS revision
FROM supplier_payments sp
JOIN (SELECT l.payment_id, MIN(ex.forwarder_id) AS forwarder_id
        FROM supplier_payment_lines l
        LEFT JOIN payment_extras ex
               ON l.target_kind = 'extra' AND ex.id = l.target_id
              AND ex.deleted_at IS NULL AND ex.rides_with = 'shipment'
       GROUP BY l.payment_id
      HAVING COUNT(*) = COUNT(ex.forwarder_id) AND MIN(ex.forwarder_id) = MAX(ex.forwarder_id)) own
  ON own.payment_id = sp.id
JOIN forwarders f ON f.id = own.forwarder_id AND f.deleted_at IS NULL
WHERE sp.deleted_at IS NULL
  AND EXISTS (SELECT 1 FROM supplier_payment_sends s
               WHERE s.supplier_payment_id = sp.id AND s.status = 'sent'
                 AND s.notice_kind = 'forwarder' AND s.forwarder_id = own.forwarder_id);

-- The charges of those transfers, as their forwarder is told them: the amount,
-- what kind of charge, and the container. NOT here: the description or note
-- typed on a charge, its payee's name, a PO, an invoice (the user, 2026-11-20).
CREATE OR REPLACE SQL SECURITY DEFINER VIEW portal_forwarder_payment_lines_v AS
SELECT
  l.id          AS line_id,
  l.payment_id  AS payment_id,
  l.amount      AS amount,
  ex.kind       AS extra_kind,
  COALESCE(sh.reference, ex.shipment_reference) AS shipment_reference
FROM supplier_payment_lines l
JOIN portal_forwarder_payments_v p ON p.payment_id = l.payment_id
JOIN payment_extras ex ON l.target_kind = 'extra' AND ex.id = l.target_id
LEFT JOIN shipments sh ON sh.id = ex.shipment_id;

-- The proofs of those transfers: a live document LINKED TO THAT PAYMENT
-- (supplier_payment_id: whose file it is), a remittance, and filed under
-- exactly the transfer's own name: the filing rule the send applies before an
-- email goes (src/lib/payment-notices.js), so the email never promises a proof
-- this view does not hold. No invoice is ever a row here.
CREATE OR REPLACE SQL SECURITY DEFINER VIEW portal_forwarder_payment_proofs_v AS
SELECT
  d.id                   AS document_id,
  d.supplier_payment_id  AS payment_id,
  d.filename             AS filename,
  d.public_url           AS public_url
FROM shipment_payment_documents d
JOIN portal_forwarder_payments_v p ON p.payment_id = d.supplier_payment_id
JOIN supplier_payments sp ON sp.id = d.supplier_payment_id
WHERE d.deleted_at IS NULL
  AND d.doc_kind = 'remittance'
  AND CONVERT(d.supplier_name USING utf8mb4) COLLATE utf8mb4_0900_bin
    = CONVERT(sp.supplier_name USING utf8mb4) COLLATE utf8mb4_0900_bin;
