# PO deposit receipts — the shared contract with the supplier portal

A supplier confirms on the portal that the deposit payment for a purchase order arrived.
The portal writes it into THIS schema — one row per PO in `jfa.po_deposit_receipts`
(`src/db/migrate/2026-10-27_10_po_deposit_receipts.sql`): `received_on`, the UTC date of
the tick, NULL when unticked, and `confirmed_by_email`, who ticked — and then invokes
`jportal-deposit-received-<env>` here, async, with `{ purchaseOrderId }`.

The row lives beside the PO, not in the portal's own schema, so that every reader is on
`jfa` whatever server the portal runs on. (It started life as
`jportal.po_deposit_receipts`, read across schemas; production has no `jportal` schema,
so ShipLine's payments endpoint failed there with "Unknown database 'jportal'".
`2026-10-27_20_copy_po_deposit_receipts.js` copied the rows over where that schema
exists; the portal's `2026-10-27_jportal_po_deposit_receipts_moved.sql` drops the old
table.)

Who writes what: the portal writes every column (its column grants are in jportal's
`db/grants/jportal_app.sql`; it needs UPDATE as well as SELECT for its `FOR UPDATE`
read). `supplier_id` and `user_id` are the portal's own ids, snapshots for its audit
trail (`jportal.audit_logs`, `entity_type 'po_deposit_receipt'`); nothing here joins on
them. Shipping only reads.

## Readers here

- `GET /api/v1/purchase-orders` and `GET /api/v1/purchase-orders/:id` (orders.js,
  `attachSupplierDepositReceipts`): every PO carries `supplierDepositReceipt:
  { receivedOn, email } | null`. ShipLine's collapsed row shows it as the RECEIVED
  lifecycle tick after PAID.
- `GET /api/v1/purchase-orders/:id/payments`: the same object as
  `supplierDepositReceipt`, the green "Supplier confirmed the deposit arrived" line at
  the top of ShipLine's Payments section.
- `src/handlers/deposit-received.js` (`jportal-deposit-received-<env>`):
  - re-reads the receipt — an unticked or missing row moves nothing;
  - moves the PO's live `SCHEDULED` / `PO_SENT` lines into `IN_PRODUCTION`, written exactly
    as an operator's move is: status only, one `audit_log` row per line (`entity_type
    'order'`, `action 'update'`, note "deposit received (supplier portal)"), under the
    supplier user's email;
  - is idempotent: lines at `IN_PRODUCTION` or beyond, deleted lines and `DESTROYED`
    samples are left alone.

All three tolerate the table not existing yet (errno 1146: the tick is simply absent),
nothing else.

The portal's invoke is fire-and-forget (a failed start is only logged there), so staff can
always make the same move in ShipLine by hand.

## The other direction

How the portal decides a deposit was SENT (read-only, its grants are in jportal's
`db/grants/jportal_app.sql`): a `supplier_payment_lines` row of kind `po_deposit` on the
PO, or of kind `pi` on a `purchase_order_invoice_payments` row of `payment_type`
`deposit`/`full`, each joined to a live `supplier_payments` transfer — plus a deposit/full
invoice payment whose `payment_status` is `paid` with no transfer recorded (sent, no
date). Staff record all of those in ShipLine; the portal never writes them.

## Deploying

The portal and shipping must be on the same MySQL server for the portal's write to land
where shipping reads (test: both on `explorer-test`). Shipping's migrations create the
table on deploy; the portal's grants are applied by hand as admin, once per server:

```sql
GRANT SELECT ON jfa.po_deposit_receipts TO 'jportal_app'@'%';
GRANT INSERT (purchase_order_id, supplier_id, user_id, confirmed_by_email, received_on)
             ON jfa.po_deposit_receipts TO 'jportal_app'@'%';
GRANT UPDATE (supplier_id, user_id, confirmed_by_email, received_on)
             ON jfa.po_deposit_receipts TO 'jportal_app'@'%';
```

(Use the real schema name if `SHIPPING_DB_SCHEMA` on the portal differs from `jfa`.)
