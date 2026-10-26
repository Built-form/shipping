# PO deposit receipts — the shared contract with the supplier portal

A supplier confirms on the portal that the deposit payment for a purchase order arrived.
The portal records it (one row per PO in `jportal.po_deposit_receipts`: `received_on`, the
UTC date of the tick, NULL when unticked) and then invokes
`jportal-deposit-received-<env>` here, async, with `{ purchaseOrderId }`.

This side (`src/handlers/deposit-received.js`):

- re-reads the receipt — an unticked or missing row moves nothing;
- moves the PO's live `SCHEDULED` / `PO_SENT` lines into `IN_PRODUCTION`, written exactly
  as an operator's move is: status only, one `audit_log` row per line (`entity_type
  'order'`, `action 'update'`), under the supplier user's email;
- is idempotent: lines at `IN_PRODUCTION` or beyond, deleted lines and `DESTROYED`
  samples are left alone.

The portal's invoke is fire-and-forget (a failed start is only logged there), so staff can
always make the same move in ShipLine by hand.

How the portal decides a deposit was SENT (read-only, its grants are in jportal's
`db/grants/jportal_app.sql`): a `supplier_payment_lines` row of kind `po_deposit` on the
PO, or of kind `pi` on a `purchase_order_invoice_payments` row of `payment_type`
`deposit`/`full`, each joined to a live `supplier_payments` transfer — plus a deposit/full
invoice payment whose `payment_status` is `paid` with no transfer recorded (sent, no
date). Staff record all of those in ShipLine; the portal never writes them.

ShipLine shows the supplier's confirmation on the PO's payments
(`GET /api/v1/purchase-orders/:id/payments`, `supplierDepositReceipt`).

## Grants

Shipping's login (`DB_USER` of `shipping/<env>`) only reads the receipt:

```sql
GRANT SELECT (purchase_order_id, received_on, confirmed_by_email, updated_at)
      ON jportal.po_deposit_receipts TO '<shipping login>'@'%';
```

(Use the test schema name on test if `JPORTAL_DB_SCHEMA` differs.) No grant on
`jportal.audit_logs`: the portal keeps its own trail there (`entity_type
'po_deposit_receipt'`), and shipping's is its `audit_log`.
