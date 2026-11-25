# Sample orders

A purchase order has a **kind**: `stock` (every PO before 2026-11) or `sample`. A sample PO buys a few units of a product to look at, not to sell. It is sent to the supplier through the portal like any PO, but it has no container, no production stages and no Mintsoft booking.

Its lines are ordinary rows of `orders`, marked `order_kind = 'sample'`. That is deliberate: board cards, the portal's change requests, JFPRO's sample page, the QC strip, the invoice check and payment lines are all keyed on `orders.id`. The price is that every reader and writer of order rows needs a rule for samples. This page is that rule set.

Code: `src/lib/sample-orders.js` (the rules, pure), `src/services/sample-orders.js` (the database side and the write guard), `src/services/sample-orders-check.js` (the activation check). Tests: `tools/test-sample-orders-lib.js`, `tools/test-sample-orders-guard.js`, `tools/test-sample-orders-check.js`, plus the sample cases in `tools/test-po-change-requests-lib.js` and `tools/test-po-pdf-layout.js`.

## Data

Migration `2026-11-25_20_sample_orders.js` (additive; every existing row reads as stock):

| Column | Meaning |
|---|---|
| `purchase_orders.kind` | `stock` or `sample`. Set by `POST /purchase-orders`, never changed. |
| `orders.order_kind` | The same word on each line, stamped by the server from the line's PO. Never taken from a request body. |
| `orders.courier_name`, `orders.courier_tracking` | How a sample travels. Never `awb_number`: ShipsGo's air sync and JFPRO's landed-cost import key on that. |
| `orders.sample_received_qty` | How many arrived. The ordered quantity, and so the bill, is not touched. |
| `orders.sample_status_changed_at` | When the line's status last changed (UTC). Judges a stale "mark as sent". |
| `orders.sample_ever_sent` | 1 from the first time the line reaches `ON_AIR`. Never cleared. |
| `orders.sample_lot_key` | Generated, **unique**: the sample code a sample line carries. One line per code, for good. NULL for stock lines and for a sample line deleted without ever being sent. |

`2026-11-25_21_sample_lots_view.sql` creates `sample_lots_v` over JFPRO's lots (`type = 'sample'`) and their product links. It needs JFPRO's "samples are lots" schema on the same server. `2026-11-25_22` grants the portal `SELECT (kind)` on `purchase_orders`, the only grant this feature adds.

A database without these columns (a TEST rebuilt from a production that does not have them yet) holds no sample. `sampleOrders.schemaReady()` detects that, and every helper then answers "stock": the stock paths keep working, unchanged.

## The rules for a sample line

One validator, `validateSampleWrite(before, after, ctx)`, looks at the **complete resulting row**. Every write path calls it through `sampleOrders.guardWrite`.

- It belongs to a sample PO and never moves to another. Its kind never changes.
- No `container_number`, `awb_number`, `external_container_number` or `shipment_id`. No ASIN is stored.
- JF code and lot are required. The lot must be a sample lot of that product in `sample_lots_v`; a failed lookup refuses the write (503 `SAMPLE_LOT_UNCHECKED`). Both can be corrected until the line is first sent, then never.
- Quantity is a whole number, 1 to 50 (a typo guard).
- **No price means a free sample**: the server stores 0, so "free" is a stated value. A negative or non-numeric price is refused.
- Courier and tracking: 64 characters each.

States, one step at a time; each needs everything the earlier ones needed:

| State | Needs | Reached by |
|---|---|---|
| `SCHEDULED` | PO, product, sample lot, quantity | create |
| `PO_SENT` | PO number, PO date, supplier | create, or from `SCHEDULED` |
| `ON_AIR` | sent date (`shipped_date`) | from `PO_SENT` |
| `ARRIVED_AT_WAREHOUSE` | arrived date | from `ON_AIR` |
| `RECEIVED` | received quantity | from `ARRIVED_AT_WAREHOUSE`, **only** by `POST /orders/:id/sample-received` |
| `DESTROYED` | a reason | from `ON_AIR` or `ARRIVED_AT_WAREHOUSE`, admin only |

An admin can correct one step back (`RECEIVED` to `ARRIVED`, `ARRIVED` to `ON_AIR`, `ON_AIR` to `PO_SENT`, `DESTROYED` to `ON_AIR`). Each correction clears exactly what its step had set. Nothing moves back to `SCHEDULED`.

A refusal is `{ error, code, allowedNext?, missing? }`, 409 for a rule and 400 for a malformed value.

### Edits that change the bill

A sample PO is **committed** once its PDF has been emailed to the supplier or money is recorded on it (a `po_deposit` transfer line, a PI marked paid, a transfer applied to one of its PIs). From then on:

- quantity, unit price, adding or deleting a line, and the PO's shipping total: admin only, with `reason` in the body (kept in `audit_log` as action `sample_reason`);
- supplier and currency: frozen for everyone.

A line that was ever sent cannot be deleted. A sample PO is deleted only with no live line and no recorded money, and only as a soft delete.

### Stock lines

The guard does two things to a stock line and nothing else: it cannot be put on a sample PO, and it cannot **take** a sample lot (409 `SAMPLE_LOT_ON_STOCK`). A stock line that already carries a sticker code keeps it.

## Routes

| Route | For a sample |
|---|---|
| `POST /purchase-orders` | `kind: 'sample'` creates one, only where sample orders are armed (409 `SAMPLES_DISABLED`). |
| `PUT`, `DELETE /purchase-orders/:id` | The committed-PO rules above. |
| `POST /orders` | A line on a sample PO is a sample line. One transaction: the shared lock on the lot's rows in JFPRO, the insert, the unique key (409 `SAMPLE_LOT_IN_USE`). |
| `PUT /orders/:id`, `PATCH /orders/:id/status` | Through the guard, on a locked row. `courierName` and `courierTracking` are accepted for a sample line. |
| `POST /orders/:id/sample-received` | `{ quantity, note? }` and an `Idempotency-Key`. Sets `RECEIVED`. No Mintsoft call. A key seen before returns its first answer and never applies again, even after a correction. |
| `POST /orders/:id/receive`, `/not-received`, `/split`, `POST /containers/pack`, shipment booking, the AI suggestion approve | Refused: 409 `SAMPLE_NOT_STOCK`. |
| `GET /orders`, `GET /purchase-orders` | Sample rows only with `?samples=1`. Any other caller gets stock rows and stock POs only. With the flag, the answer carries `capabilities.sampleOrders`. |
| `POST /po-change-requests/:id/apply` | Applies a supplier's "mark as sent" (below). |

## "Mark as sent" from the portal

The supplier's one action on a sample order. The portal stages `requested_json = { markSent: { sentOn, courier?, tracking? } }` in its own `po_change_requests`, with `base_json = { status, jfCode, quantity, seenAt }`: what the supplier was looking at. Staff apply it in ShipLine; it moves the line from `PO_SENT` to `ON_AIR` with the date and the courier.

`planApply` in `src/lib/po-change-requests.js` blocks it when: the line is not a sample (`sample_only`); the line is no longer `PO_SENT` (`sent_from`); the date is not a date, is after tomorrow UTC, or is before the PO date; the courier fields are not short text; or the request is **stale**: status, product or quantity differ from `base`, `seenAt` is missing, or the page was loaded before the line's last status change. A sample line takes no ready date and no QC move (`not_for_sample`).

Courier details staff entered first are kept unless the apply sends `useSupplierCourier: true`. Any change staff make to a sample line's status, product, lot or quantity supersedes a pending request on it, in the same transaction.

## Who reads order rows, and what they do with a sample

| Reader | Policy |
|---|---|
| `stock-snapshots/sum`, `/sum/all-asins` (Cashboard's stock figures) | excluded (`sampleOrders.stockOnlySql`) |
| AI status suggestions, ready-date reminders, the old public ready-date page | excluded |
| Daily alerts | excluded, except "arrived, not received" |
| Make webhook on `PO_SENT` | not fired |
| Deposit-received and artwork-confirmed Lambdas | a sample PO's lines are not moved; the supplier's tick is still recorded |
| Artwork confirmation email | refused (409 `SAMPLE_NO_ARTWORK`) |
| QC visit candidates, ShipsGo sync, shipment shadow | cannot meet a sample: it is never `IN_PRODUCTION` or `READY_FOR_QC` and holds no AWB, container or shipment (the validator, and the activation check, hold that) |
| PO PDF | headed SAMPLE ORDER; one line per sample code. A sample with no price is not priced on the paper (the user, 2026-11-25: "exclude the price column dont say free"): when no line has a price the table has no Unit Price and Line Price columns, and with no shipping charge either there is no totals box; when some lines have a price the columns stay and an unpriced line's two cells are empty. Nothing prints "free", and nothing prints a 0 for a sample's price |
| Portal payment views | included: a sample's payment shows like any PO payment |

## Switching it on

Nothing can create a sample order until this stage's row is in `app_migrations`: `sample_orders_on:test`, or `sample_orders_on:dev` for production (the production stage is named `dev`). The server reads `APP_STAGE` and arms only on its own stage's row, so the row a TEST rebuild copies from production never arms TEST. The row is read fresh, uncached, on the two routes that create a sample PO or a sample line.

```
node tools/check-sample-orders.js --stage test --jflow-user <login> --jflow-health <url>          # report
node tools/check-sample-orders.js --stage test --jflow-user <login> --jflow-health <url> --arm    # then switch on
node tools/check-sample-orders.js --stage test --disarm
```

`--jflow-health` is JFlow's own health route for that stage (`https://<its api>/api/v1/health`). It answers `paymentsFlow.sampleOrders: true` once JFlow's payments model has been re-pinned to a ShipLine commit that holds the sample rule; JFlow asks the model itself, so the answer is about the code that is deployed. The route sits behind JFlow's sign-in: put a signed-in Google ID token in the `JFLOW_TOKEN` environment variable before running the check. It is sent as a bearer token and never printed. Or give `--jflow-function <name>` in place of `--jflow-health` (for TEST, `jflow-test-jflowApi`): the same route asked of JFlow's Lambda directly with `aws lambda invoke`, which needs no token, only the rights a deploy already needs.

`--arm` writes only when every check passed and none was left unanswered: the columns, the unique key and the view exist; the portal's login can read `kind`; JFlow's login can read both kind columns and its health route reports the sample payment rule; no line's kind differs from its PO's; no sample line has a container, AWB or shipment; every sample's lot is a sample of its product; every sample is in a sample state.

Order of release: shipping (migrations, then deploy, not armed), JFPRO's identity guard, the portal's deploy, ShipLine, JFlow's re-pin, then the check and `--arm`. After every TEST rebuild from production: re-apply the migrations if production does not have them yet, then run the check and arm TEST again.

Once a sample row exists there is no rolling back below this release in shipping, the portal, JFlow or JFPRO: an older reader counts the row as stock. `--disarm` stops new samples and is the only retreat.

## Not built

- The activation check does not yet run on a schedule; it is the tool above.
- A sample cannot ride inside a production container.
- Stock orders that already carry sticker codes stay stock rows.
