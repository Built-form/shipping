# Dismissed "Needs attention" lines — frontend guide

Routes: `src/services/payment-alert-routes.js` · rules: `src/lib/payment-alerts.js`
· schema: `src/db/migrate/2026-10-02_10_payment_alert_dismissals.sql` (applied by
`bash deploy.sh`; the Lambdas run no DDL) · tests: `tools/test-payment-alerts-lib.js`
(unit, in `npm run test:unit`) and `tools/test-payment-alerts.js` (HTTP, TEST database).

ShipLine's Payments flow page works its "Needs attention" lines out in the
browser each time, so an alert has no row. An admin can dismiss one (user,
2026-10-02); this API keeps who dismissed which alert, when, and what it said.

## The key

The page builds it from what the alert is about and what it says
(`alertKey` in `ShipLine/src/components/payments/paymentsFind.ts`):

```
<kind>|<currency>|<purchase order id>|<CONTAINER>|<supplier>|<hash of its wording and figure>
```

When the wording or the figure changes the key changes, so **the alert comes
back on its own**. The server treats the key as an opaque string of at most
255 characters. A dismissal whose alert no longer exists hides nothing.

## Routes

| Route | Who | Returns |
| --- | --- | --- |
| `GET /api/v1/payment-alert-dismissals` | everyone | `{ data: Dismissal[] }` — the ones still standing |
| `POST /api/v1/payment-alert-dismissals` | admin | `201 { dismissal }`; `200` with the existing row when that key is already dismissed |
| `DELETE /api/v1/payment-alert-dismissals/:id` | admin | `200 { dismissal }` — restored; the row stays, marked |

POST body: `{ alertKey, kind, currency?, poNumber?, shipmentReference?, supplierName?, detail?, amount?, note? }`.
Only `alertKey` and `kind` are required; the rest records what the alert said.

Refusals: `403 CANNOT_DISMISS` (not an admin) · `400 BAD_FIELD` · `404 NOT_FOUND`
· `409 NOT_ACTIVE` (already restored).

```ts
interface Dismissal {
  id: number; alertKey: string; kind: string; currency: string | null;
  poNumber: string | null; shipmentReference: string | null; supplierName: string | null;
  detail: string | null; amount: number | null; note: string | null;
  dismissedByEmail: string; dismissedByName: string | null; dismissedAt: string;
  restoredAt: string | null; restoredByEmail: string | null;
}
```

## Audit

`audit_log` rows, `entity_type` `payment_alert_dismissal` (create on dismiss,
update on restore).

## On the page

A dismissed line leaves the Needs attention list and its count and sits in a
closed "Dismissed" group at the bottom of the tab, with who dismissed it; an
admin restores it there. An API build without these routes shows every alert
and offers no Dismiss button. The alerts shown inside a payment's pop-up are
not filtered.
