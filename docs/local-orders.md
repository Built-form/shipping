# Local orders

A purchase order of kind **`local`** buys stock from a supplier who delivers it to the warehouse themselves (a UK supplier with a van, say). It is stock: it is booked into Mintsoft, counts in Cashboard's figures, and is paid like any PO. But it has none of the import pipeline: no port, no production or QC stage, no container, air shipment or shipment. ShipLine keeps the record so the goods can be booked in (the user, 2026-11-27: "these will be record only on shipline so they can be booked in on mintsoft").

It is the third value of the kind that sample orders added (`docs/sample-orders.md`): `purchase_orders.kind` and `orders.order_kind`, stamped by the server from the line's PO.

Code: `src/lib/local-orders.js` (the rules, pure); the local branch of `guardWrite` in `src/services/sample-orders.js`. Tests: `tools/test-local-orders-lib.js`, `tools/test-local-orders-guard.js`. ShipLine: `src/components/shared/localOrders.ts` (the same rules, so the UI offers only what the server takes).

## Data

Migration `2026-11-27_30_local_orders.js` widens the two kind CHECKs to `('stock', 'sample', 'local')`. Nothing else is added. It needs `2026-11-25_20` first. A database without it holds no local order: `POST /purchase-orders` with `kind: 'local'` answers 409 `LOCAL_ORDERS_UNAVAILABLE`, and `GET /orders?samples=1` says `capabilities.localOrders: false`, so ShipLine offers no "+ NEW LOCAL ORDER" button.

There is no switch to arm. A local order is stock to every reader that does not know the word: JFlow reads any kind but `sample` as stock, and so does the portal.

## The rules for a local line

`validateLocalWrite(before, after, { po })`, run by `guardWrite` on every write:

- It belongs to a local PO and never moves to another. A stock line cannot be put on a local PO (409 `LOCAL_PO_REQUIRED`).
- Made at `SCHEDULED` or `PO_SENT`. After that it holds only `SCHEDULED`, `PO_SENT`, `ON_AIR`, `ARRIVED_AT_WAREHOUSE`, `PARTIALLY_RECEIVED` or `RECEIVED`. Any other status is 409 `LOCAL_NOT_SHIPPED`, with `allowedNext`.
- No `container_number`, `external_container_number`, `awb_number` or `shipment_id` (409 `LOCAL_NOT_SHIPPED`, with `field`).
- No sample's code as its lot, as for any stock line (409 `SAMPLE_LOT_ON_STOCK`).
- No port is asked for, and no lot, MFG or expiry date before booking in.

The path is PO Sent, then On Air/Road if staff want to show it is on its way (optional; the user, 2026-11-27: "allow local orders to go to on road column"), then Arrived at Warehouse (when the supplier delivers), then Received (by booking into Mintsoft). On Air/Road is `ON_AIR`, the same column air freight uses: for a local line it holds no AWB and no container, and ShipLine opens no Pack dialog for it and merges nothing when it comes back off it. ShipLine also lets staff move it one step back (Arrived to On Air/Road or PO Sent, On Air/Road to PO Sent), if it was moved by mistake. No admin can move a local line to any other stage. The server enforces the set of stages, not the order of the moves, as for stock.

## Booking in

`POST /orders/:id/receive` books a local line into Mintsoft like any stock line, with one difference: **lot and expiry are optional** (`missingForBookIn`). The receive dialog still shows both and lets staff fill them in, in case the product carries them (the user: "allow none but ask at booking in just in case product has it"). A receipt with no batch is stored with `batch_no` NULL, and Mintsoft is sent `BatchNo: null`. Untested against a batch-tracked Mintsoft product: if Mintsoft refuses one with no batch, the receive answers 502 `MINTSOFT_CALL_FAILED` and nothing is stored.

## Who reads local rows, and what they do with one

| Reader | Policy |
|---|---|
| `GET /orders`, `GET /purchase-orders` | included, with `orderKind` / `kind` `'local'` |
| `stock-snapshots/sum`, `/sum/all-asins` (Cashboard) | included (`sampleOrders.notSampleSql`); a local line on the road counts in the on-air figures, as any `ON_AIR` line does |
| Payments page, JFlow | included as any PO; goods in no container, so a line on the road or landed counts as departed and the balance falls due on the supplier's terms |
| Ready-date reminders, AI status suggestions from email, old ready-date page | excluded (`stockOnlySql`, `= 'stock'`): they ask a supplier about production |
| Daily alerts | as stock; a local line never reaches the transit or QC alerts' stages |
| Make webhook on `PO_SENT` | not fired |
| PO send (`POST /purchase-order-documents/:id/email`), artwork confirmation email | refused, 409 `LOCAL_NO_SEND`: both email a portal link. GEN PDF and download still work |
| Deposit-received and artwork-confirmed Lambdas | a local PO's lines are not moved |
| Pack, split, shipment booking, approving an email suggestion | refused, 409 `LOCAL_NOT_SHIPPED` |
| Supplier portal | **not changed** (the user: "no need to implement portal stuff"). It lists a local PO as a stock PO to the supplier's contacts who already have portal access. A supplier tick there moves nothing (above) |

## Release order

shipping first (migration, then deploy), then ShipLine. No jportal or JFlow change. Run `node tools/migrate.js --stage <stage> --apply` for the migration; it adds no view, so `--check-views` is not needed for it.
