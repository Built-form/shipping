# Shipment balance payments — frontend guide

A **deposit** is a fact about a purchase order and lives on the PO: its PI, the
Gemini-extracted instructions, the `paymentStatus` chips, the proof files. See
[invoice-payments-frontend.md](invoice-payments-frontend.md).

A **balance** is not. It is settled per shipment, for the lines that actually
travelled in that box. One PO spread over a dozen containers pays a dozen
times; one container holding eight suppliers has eight payables. So a balance
record is keyed **shipment × supplier**, with a per-PO split underneath it.

```
GET    /api/v1/shipment-payments             → { data, documents, shipments }   (any allowlisted user)
GET    /api/v1/shipment-payments/context     → the record form's data
POST   /api/v1/shipment-payments             → 201 record
PUT    /api/v1/shipment-payments/:id         → 200 record
DELETE /api/v1/shipment-payments/:id         → 204                              (admin only)
POST   /api/v1/shipment-payments/relink      → repair pointers after a re-seed  (admin only)
```

Errors are `{ error, code, …details }`. `error` is the sentence to show; `code`
is what to branch on.

## The record

```json
{
  "id": 12,
  "shipmentId": 61, "shipmentReference": "308", "link": "ok", "mergedIntoId": null,
  "supplierName": "Suzhou Sunmed Co., Ltd", "supplierKey": "suzhou sunmed co., ltd",
  "kind": "balance",
  "amount": 34400.02, "currency": "USD",
  "invoiceNumber": "SM-BAL-308", "invoiceDate": null, "dueDate": "2026-10-05",
  "invoiceTotal": null, "depositDeducted": null,
  "status": "pending", "paidOn": null, "bankRef": null, "note": null,
  "source": "manual",
  "allocations": [
    { "id": 1, "purchaseOrderId": 299, "poRef": "PO_00299J", "poNumber": "PO_00299J", "amount": 9758.62, "source": "share" }
  ],
  "allocatedTotal": 34400.02, "unallocated": 0, "fullyAllocated": true,
  "documents": [],
  "createdByEmail": "…", "updatedByEmail": null, "createdAt": "…", "updatedAt": "…"
}
```

- `status` is the same vocabulary as a PI's: `pending | arranged | paid | skipped`.
  It is **owned by the operator**. Nothing the server does — including reading a
  supplier's invoice — ever sets `paid`; that is a deliberate act.
- `amount` is what this document asks for. `allocations` split it across the
  purchase orders on that shipment; `unallocated` is the part not yet attributed
  (allowed while pending, refused for `paid`).
- `source` is `manual` or `extracted` (phase 2, from an uploaded invoice).
- `shipmentReference` is exactly what orders carry as `containerNumber`, so the
  page can join records to its container × supplier payables without another
  fetch. `shipments` in the response is a side-map of summaries keyed by id.

### `link` — the shipment pointer

The shipments entity is still a rebuildable shadow: a re-seed renumbers it and a
merge moves a loser's orders to the survivor. Every record therefore stores the
reference beside the id, and every read reports:

| `link` | meaning | what the UI should do |
|---|---|---|
| `ok` | the shipment resolves | nothing |
| `merged` | it was merged into `mergedIntoId` (also in `shipments`) | show the survivor |
| `missing` | the id is gone (re-seed) | offer an admin `POST /relink` |

`PUT` still works on a `missing` record for everything except the split — a
re-seed must never block marking something paid. Editing `allocations` needs the
shipment to resolve and answers `409 SHIPMENT_UNRESOLVED` otherwise.

## `GET /api/v1/shipment-payments`

Query (all optional): `status` (CSV of the four, or `open` = pending + arranged)
· `shipmentId` · `shipmentReference` · `supplier` (substring of the normalised
key) · `purchaseOrderId` (records with an allocation to it) · `updatedSince`
(ISO; allocation writes bump the parent's `updatedAt`, so this is a safe poll
cursor). No pagination — a few hundred small rows.

Returns `{ data: [record], documents: [document], shipments: { id: summary } }`.
`documents` is **every live document**, including ones not yet attached to a
record (extraction running or failed), so an upload is visible immediately.

## `GET /api/v1/shipment-payments/context?shipmentId=|shipmentReference=`

What the "record a balance" form needs:

```json
{ "shipment": { "id": 61, "reference": "308", "stage": "BOOKED", "mode": "SEA", "etd": "…", "eta": "…", "mergedIntoId": null },
  "purchaseOrders": [
    { "id": 299, "poNumber": "PO_00299J", "supplier": "Suzhou Sunmed Co., Ltd", "currency": "USD",
      "lineCount": 2, "unitsInShipment": 4200, "valueInShipment": 9758.62,
      "deposit": { "amountDue": 2927.59, "currency": "USD", "paymentStatus": "paid", "dueDate": "2026-07-02", "depositPercentage": 30 } }
  ],
  "existingPaymentIds": [12] }
```

`valueInShipment` is what that PO has **on board** — the basis for the default
split, and the number to pre-fill each allocation with. `404 NOT_FOUND` /
`409 MERGED` / `422 NOT_BOOKED`.

## `POST /api/v1/shipment-payments`

```json
{ "shipmentId": 61,                  // or "shipmentReference": "308"
  "supplierName": "Suzhou Sunmed Co., Ltd",
  "amount": 34400.02, "currency": "USD",
  "invoiceNumber": "SM-BAL-308", "invoiceDate": null, "dueDate": "2026-10-05",
  "invoiceTotal": null, "depositDeducted": null, "bankRef": null, "note": null,
  "allocations": [{ "purchaseOrderId": 299, "amount": 9758.62 }],
  "allocate": { "mode": "share", "purchaseOrderIds": [299, 297] },
  "force": false }
```

- Either `allocations` (explicit) or `allocate` (split by value share). With
  neither, the record is unallocated and the page shows the remainder.
  `allocate.purchaseOrderIds` defaults to every member PO of that supplier.
  A share split is computed in whole cents, so the parts always sum back to
  `amount` exactly.
- An allocation may name a `poRef` string instead of a `purchaseOrderId` (a
  reference we could not resolve); it counts as unallocated on the page.
- The record always lands `pending`.

Refusals: `404 NOT_FOUND` · `409 MERGED { mergedIntoId }` · `422 NOT_BOOKED` ·
`422 PO_NOT_IN_SHIPMENT { purchaseOrderId }` · `422 OVER_ALLOCATED { allocated, amount }` ·
`409 DUPLICATE_INVOICE { existingId }` (same supplier + invoice number; pass
`force: true` to record it anyway) · `400 BAD_FIELD` / `BAD_ALLOCATIONS`.

`warnings[]` comes back on success and is advisory only — a second open balance
for the same supplier on the same shipment, or an allocation to a PO filed under
a different spelling of the supplier's name. **Supplier spelling is never a
refusal**: `orders.supplier` and `purchase_orders.supplier` routinely disagree
for the same company on the same shipment.

## `PUT /api/v1/shipment-payments/:id`

Any of `amount, currency, invoiceNumber, invoiceDate, dueDate, invoiceTotal,
depositDeducted, bankRef, note, status, paidOn, allocations`. The shipment is
not editable — that is what `relink` is for.

- `status: "paid"` requires the split to cover the whole invoice →
  `422 NOT_FULLY_ALLOCATED { allocated, amount }`.
- Moving **into** `paid` stamps `paidOn` with today unless one is sent; moving
  out of it clears `paidOn`.
- Lowering `amount` below the existing split → `422 OVER_ALLOCATED` (resend
  `allocations` with it).

## `POST /api/v1/shipment-payments/relink`

Admin. `{ "dryRun": true }` (the default) reports; `{ "dryRun": false }` writes.
Returns `{ dryRun, checked, relinked: [{ entity, id, from, to, reason }], unresolved: [...] }`.
Reasons: `merged`, `missing`, `deleted`, `reference_drift`. The same work is
`node tools/relink-shipment-payments.js [--apply]`.

## Documents — `/api/v1/shipment-payment-documents`

The paper behind a record: the supplier's balance invoice, or a remittance
advice proving we paid it. A balance invoice is **read in the background by
Gemini**, which fills in the figures and the per-PO split.

**Reading a document never marks anything paid.** An invoice asks for money;
the record it drafts lands `pending` and an operator flips it.

```
POST   /api/v1/shipment-payment-documents            → 201 { document }
POST   /api/v1/shipment-payment-documents/:id/extract → 202 { document, alreadyRunning }
GET    /api/v1/shipment-payment-documents            → { data: [document] }
DELETE /api/v1/shipment-payment-documents/:id        → 204   (admin only)
```

```json
{ "shipmentId": 74,                  // or "shipmentReference": "324"
  "supplierName": "Suzhou Sunmed Co., Ltd",
  "docKind": "balance_invoice",      // balance_invoice | remittance | other
  "filename": "balance-324.pdf", "contentType": "application/pdf",
  "dataBase64": "…",                 // the file, as the PI upload sends it
  "notes": null,
  "paymentId": 12,                   // optional: attach a remittance to a record
  "extract": true,                   // defaults true for balance_invoice, false otherwise
  "force": false }
```

API Gateway caps the request body at 6 MB, so a file tops out around 4.4 MB —
fine for the one- to three-page documents suppliers send. Refusals:
`404 NOT_FOUND` · `409 MERGED` · `422 NOT_BOOKED` ·
`422 PAYMENT_NOT_ON_SHIPMENT` · `409 DUPLICATE_DOCUMENT { existingId }` (same
filename and size already on the shipment; `force: true` overrides) ·
`413 TOO_LARGE` · `400 BAD_FIELD`.

The document carries its own read state, so polling the feed is enough:

| `extractStatus` | meaning |
|---|---|
| `none` | filed, not read (a remittance, or `extract: false`) |
| `processing` | being read now — poll |
| `succeeded` | `extracted` holds the result; `paymentId` names the record |
| `failed` | `extractError` says why; `POST …/extract` tries again |

`extracted` is the model's own output plus what we did with it:
`documentKind, supplierName, invoiceNumber, invoiceDate, dueDate, currency,
totalAmount, amountDueNow, depositDeducted, dueTerms, containerRefs, blRefs,
poRefs, lines[], bank{}, rawText, confidence`, then `matchedLines`,
`unmatchedLines`, `lineTotal`, `splitByShare`, `needsAllocation`, and
`duplicateOfPaymentId` / `skippedUpdate` when it declined to write.

**What a read does.** A balance or deposit invoice with a payable amount
creates a `pending` record (`source: "extracted"`) with allocations mapped from
the document's own references — suppliers write `PO00333J`, `PO 00297J`,
`PO-00299J` for the same three POs, so references match on letters+digits,
then on the digit core when that identifies exactly one PO on the shipment.
Anything it cannot map is kept verbatim as `poRef` with no id, and counts as
unallocated. A document naming only PO references (no line amounts) is split by
what each PO has on board. A remittance is filed and its bank details stored;
no record is created.

**A re-run never overwrites a person.** It refreshes a record only while that
record is still `pending` *and* `source: "extracted"`; otherwise it stores
`skippedUpdate: "operator_owned"` and leaves everything alone. A second
document for an invoice number already on file links to the existing record
rather than creating a duplicate (`duplicateOfPaymentId`).

A read stranded by a killed Lambda can be restarted after ten minutes;
`POST …/extract` answers `202 { alreadyRunning: true }` before that.

## Permissions

Reads and writes are open to any allowlisted user, exactly like the PI status
chips; `DELETE` and `relink` need `X-User-Type: admin`. ShipLine's accountant
role is read-only in the client. Every write is recorded in `audit_log` as
`entity_type = 'shipment_payment'` (`create`, `update`, `status`, `delete`,
`relink`).

## Testing

`npm run dev:local` (TEST database) then `node tools/test-shipment-payments.js`
— it picks a real booked shipment, exercises every route and refusal, and
removes its own rows. Under `LOCAL_USER_TYPE=standard` it asserts the 403s
instead.

Uploads cannot be exercised locally: `PO_DOCS_BUCKET` is set by
`serverless.yml` at deploy time, so a local `POST /shipment-payment-documents`
answers "PO_DOCS_BUCKET env var not configured" — the same limitation the PI
upload has. The reader itself (prompt, schema, reference matching, every DB
write and the re-run rules) is exercised against the real Gemini API and the
TEST database by calling `runShipmentPaymentExtraction` directly with the S3
fetch stubbed.
