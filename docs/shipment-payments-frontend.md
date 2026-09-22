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

GET    /api/v1/supplier-payments             → { data, documents }   transfers + proofs waiting to be applied
GET    /api/v1/supplier-payments/open-items  → what a transfer to a supplier can be applied to
POST   /api/v1/supplier-payments             → 201 transfer  (marks what its lines cover paid)
PUT    /api/v1/supplier-payments/:id         → 200 transfer
DELETE /api/v1/supplier-payments/:id         → 204                              (admin only)
```

**Paid is a fact about a transfer, not a flag on an invoice.** One bank
transfer to a supplier can cover balances on two containers and a deposit on a
third PO; a supplier payment (below) records that transfer once and applies it
line by line. Marking a balance paid by hand is still allowed — the page shows
it as `NO PROOF`.

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
  "appliedTotal": 0, "remaining": 34400.02, "settledByPaymentId": null,
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
- `appliedTotal` / `remaining` are what supplier payments have applied to it so
  far; a part payment leaves it `pending` with a remainder. `settledByPaymentId`
  names the transfer that completed it — null on a balance someone marked paid
  by hand, which the page flags `NO PROOF`.
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
`documents` is **every live upload on the shipments asked about**, with or
without a record — still being read, unreadable, a remittance, one that did not
match — so an upload is visible immediately and never silently disappears.
Only `shipmentId`, `shipmentReference` and `supplier` narrow it; the status and
PO filters describe records, which an upload without one cannot match. (Before
2026-09-21 the list was limited to shipments that already had a record, so an
upload on a box with no balance yet was invisible whenever any other box had
one.)

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
- `documentId` (optional) pins an upload the reader made no record from — a
  mismatch, or one with no amount — to the record being created ("record it
  anyway" on the page).

Refusals: `404 NOT_FOUND` · `409 MERGED { mergedIntoId }` · `422 NOT_BOOKED` ·
`422 DOCUMENT_NOT_ON_SHIPMENT` · `409 DOCUMENT_LINKED { paymentId }` ·
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
the record it drafts lands `pending` and an operator flips it. A remittance
proves a payment; the operator records the transfer (`POST /supplier-payments`
with `documentId`) and ticks what it covered.

```
POST   /api/v1/shipment-payment-documents              → 201 { document }
POST   /api/v1/shipment-payment-documents/:id/extract   → 202 { document, alreadyRunning }
GET    /api/v1/shipment-payment-documents              → { data: [document] }
DELETE /api/v1/shipment-payment-documents/:id          → 204   (admin only)
```

```json
{ "shipmentId": 74,                  // or "shipmentReference": "324" — or NEITHER for a proof of payment:
                                     // a bank confirmation is to a supplier, not a box (shipmentId comes back null)
  "supplierName": "Suzhou Sunmed Co., Ltd",
  "docKind": "balance_invoice",      // balance_invoice | remittance | other
  "filename": "balance-324.pdf", "contentType": "application/pdf",
  "dataBase64": "…",                 // the file, as the PI upload sends it
  "notes": null,
  "paymentId": 12,                   // optional: attach a remittance to a record
  "extract": true,                   // defaults true for balance_invoice and remittance, false for other
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
| `none` | filed, not read (`docKind: other`, or `extract: false`) |
| `processing` | being read now — poll |
| `succeeded` | `extracted` holds the result; `paymentId` names the record |
| `failed` | `extractError` says why; `POST …/extract` tries again |

`extracted` is the model's own output plus what we did with it:
`documentKind, supplierName, invoiceNumber, invoiceDate, dueDate, paymentDate,
currency, totalAmount, amountDueNow, depositDeducted, dueTerms, containerRefs,
blRefs, poRefs, lines[], bank{}, rawText, confidence`, then `fit`,
`allocations`, `matchedLines`, `unmatchedLines`, `lineTotal`, `splitByShare`,
`needsAllocation`, and `noPaymentCreated` / `duplicateOfPaymentId` /
`skippedUpdate` when it made or changed nothing, and `check` (below). Reads
from before 2026-09-21 have no `fit`, `paymentDate`, `allocations` or `check`
— re-run them.

**Does it agree with the box? — `check`** (invoices uploaded on a shipment).
The PO-vs-PI check asks "does the invoice match the order"; this asks "does it
match what is actually in the container": every invoice line against the
supplier's lines on board (the model maps each line to our `jfCode`; else PO +
product name; else the PO's only remaining line), comparing quantity, unit
price and line total, plus the amount asked for against what the terms imply
— goods on board less the deposit (`depositBasis`: `pi_percentage` from the
deposit PIs, else `invoice_deduction`, else `none`).

```json
{ "verdict": "differs",                 // match | differs | unverified
  "matchedLines": 1,
  "lines": [{ "poNumber": "PO_00298J", "jfCode": "JF0166", "productName": "…", "ourQty": 1720, "invoiceQty": 2600, "ourUnitPrice": 1.93, "invoiceUnitPrice": 1.2768, "ourTotal": 3319.6, "invoiceTotal": 3319.6,
              "issues": ["quantity 2,600 on the invoice, 1,720 in the container", "unit price 1.2768 on the invoice, 1.93 on the PO"] }],
  "missingOurLines": [{ "poNumber": "PO_00333J", "jfCode": "JF0945", "qty": 500, "total": 1700 }],
  "extraInvoiceLines": [{ "poRef": "PO00333J", "sku": null, "description": "Nitrile gloves M", "qty": 18400, "unitPrice": 1.258, "amount": 23147.2 }],
  "goodsOnBoard": 53494.62, "invoiceGoods": 47236.22,
  "depositBasis": "invoice_deduction", "depositExpected": 14170.87, "expectedBalance": 39323.75,
  "invoiceBalance": 33065.35, "balanceDelta": -6258.4, "balanceTolerance": 196.62 }
```

Tolerances as the PI check: 0.005 on a unit price, 0.50 on a line total,
max(1, 0.5 %) on the balance. `fit` and `check` are different questions: an
invoice can belong here (`fit: match`) and still not agree with the box.

**Does it belong here? — `fit`.** Every read is compared with the shipment and
the supplier it was uploaded against:

```json
{ "verdict": "mismatch",               // match | unconfirmed | mismatch
  "checks": [
    { "key": "supplier",  "ok": false, "text": "Issued by HANGZHOU BRIGHTSTAR HYGIENE CO.,LTD, not SUNMED." },
    { "key": "container", "ok": null,  "text": "Names no container or bill of lading." },
    { "key": "pos",       "ok": false, "text": "Names BSH-4471, BSH-4472 — not on this shipment." },
    { "key": "amount",    "ok": false, "text": "USD 12,915.00 is more than the USD 10,225.36 of goods this supplier has on board." } ],
  "onBoard": 10225.36, "currency": "USD" }
```

`ok: true` ties it here, `false` points somewhere else, `null` means nothing to
compare. Any `false` → `mismatch`; a container/B-L or PO reference that ties it
here with no `false` → `match`; otherwise `unconfirmed` (normal for a bank
remittance, which names only the payee). Checks: supplier name (legal-form words
ignored, so "MEDOFFICE SAGLIK ENDUSTRI" is MEDOFFICE); container / B/L against
the shipment's reference, carrier ref, B/L and booking ref (an 11-character
container number may carry a suffix; the 3-digit internal reference must match
whole; a container cannot be contradicted when the shipment holds no number);
PO references against the POs on board, including another supplier's PO; the
currency; and an amount above 105 % of what this supplier has on board.

**What a read does.**
- A **balance invoice** with a payable amount creates a `pending` record
  (`source: "extracted"`), filed under the supplier it was uploaded against
  (not the letterhead spelling), with allocations mapped from the document's
  own references — suppliers write `PO00333J`, `PO 00297J`, `PO-00299J` for the
  same three POs, so references match on letters+digits, then on the digit core
  when that identifies exactly one PO on the shipment. Anything it cannot map is
  kept verbatim as `poRef` with no id, and counts as unallocated. A document
  naming only PO references is split by what each PO has on board. Lines bill
  the goods while the payable is often the goods less the deposit, so when the
  lines add up to MORE than the amount due the split is scaled to it, each PO
  keeping its share of the lines, in whole cents (`scaledToAmount: true`);
  lines adding up to LESS are left as read and the gap stays unallocated.
  **Unless `fit.verdict` is `mismatch`**: then no record is made
  (`noPaymentCreated: "mismatch"`) and the page offers "record it anyway"
  (`POST /shipment-payments` with `documentId`). No amount →
  `noPaymentCreated: "no_amount"`.
- A **deposit invoice** makes nothing (`noPaymentCreated: "deposit_invoice"`):
  deposits belong on the purchase order.
- A **remittance** makes nothing on its own (`noPaymentCreated: "remittance"`);
  its amount, `paymentDate` and `bank.paymentReference` pre-fill the transfer
  it is applied with. The model decides the kind — a remittance uploaded as
  `balance_invoice` is still read as a remittance and refiled. An invoice
  uploaded with no shipment makes nothing (`noPaymentCreated: "no_shipment"`).

## Supplier payments — `/api/v1/supplier-payments`

One bank transfer to a supplier, applied to the obligations it settled. Each
line carries the amount applied, so one transfer covers four things and a part
payment is representable. Schema: `src/db/migrate/*_supplier_payments.sql`.

```json
{ "id": 7, "supplierName": "SUNMED", "supplierKey": "sunmed",
  "amount": 5800, "currency": "USD", "paidOn": "2026-09-19", "bankRef": "2026091911472006100482719930", "note": null, "source": "manual",
  "lines": [
    { "id": 1, "kind": "balance",    "targetId": 96,  "amount": 5000, "label": "308",       "shipmentId": 61, "shipmentReference": "308", "purchaseOrderId": null, "poNumber": null,      "paymentType": null,      "targetAmount": 5000,    "targetStatus": "paid", "targetMissing": false },
    { "id": 2, "kind": "pi",         "targetId": 124, "amount": 800,  "label": "PO_00393J", "shipmentId": null, "shipmentReference": null, "purchaseOrderId": 393, "poNumber": "PO_00393J", "paymentType": "full",    "targetAmount": 125319.46, "targetStatus": "pending", "targetMissing": false },
    { "id": 3, "kind": "po_deposit", "targetId": 84,  "amount": 0,    "label": "SUNMED-95", "shipmentId": null, "shipmentReference": null, "purchaseOrderId": 84,  "poNumber": "SUNMED-95", "paymentType": "deposit", "targetAmount": null,    "targetStatus": null,   "targetMissing": false }
  ],
  "linesTotal": 5800, "unexplained": 0,
  "documents": [ { "...": "the proof(s) of payment, same shape as any document" } ],
  "createdByEmail": "…", "updatedByEmail": null, "createdAt": "…", "updatedAt": "…" }
```

Line kinds: `balance` → `shipment_payments.id` · `pi` →
`purchase_order_invoice_payments.id` (a deposit, balance or full PI) ·
`po_deposit` → `purchase_orders.id`, a deposit paid before any PI was filed;
the line itself is the record of it (the page treats it as a stated paid
deposit and shrinks the projected one).

A fourth kind exists **on the way in only**: `container_balance` → a shipment
id, for a box nobody has recorded a balance for yet. Saving records that
balance (`balanceAmount`, default the amount paid; split by `allocations`
`[{ purchaseOrderId, amount }]`, default by what each of the supplier's POs has
on board) and the line becomes an ordinary `balance`. `409 BALANCE_EXISTS
{ balanceId, shipmentId }` when that shipment × supplier already has an open
balance — apply to it instead; `422 NOT_BOOKED` / `NO_MEMBER_POS` /
`PO_NOT_IN_SHIPMENT` as for a balance.

**Where it shows.** The proof follows the payment to everything it covered: a
balance carries `settlements[]` (`GET /shipment-payments`), a PI carries
`payment.settlements[]` (`GET /purchase-orders/:id/invoices`), and
`GET /purchase-orders/:id/payments` answers `{ data: [proof files], transfers:
[...] }` — every transfer that touched the PO (`kind: "po_deposit"` for a
deposit paid with no PI, `kind: "pi"` with `invoicePaymentId`). Each
settlement: `{ paymentId, paidOn, amount (applied here), paymentAmount,
currency, bankRef, note, documents: [{ id, filename, url }], also: [the
transfer's other lines] }`.

**Settling.** An obligation (`balance`, `pi`) is marked paid once the live
lines applied to it cover its amount within a bank charge — max(1, 1 %) — with
`paidOn` = the transfer's date and (for a balance) `bankRef` kept if already
set; `settled_by_payment_id` remembers which transfer completed it. Less than
that leaves it open with `appliedTotal` / `remaining`. Editing or deleting a
transfer reverts only what *that* transfer settled — a balance or PI someone
marked paid by hand is never touched. Audit: `supplier_payment` (`create` /
`update` / `delete`); each flip is audited on the obligation as before
(`shipment_payment` `status` with `via: "payment"`, `purchase_order`
`invoice_payment_status` with `via: "payment"`).

### `GET /api/v1/supplier-payments?supplier=&shipmentId=&updatedSince=`

`{ data: [transfer], documents: [document] }`. `documents` is every unapplied
proof of payment (a remittance by kind or by what the reader found, or any
upload with no shipment) — "proofs to apply" on the page. `shipmentId` keeps
transfers with a line on one of that shipment's balances.

### `GET /api/v1/supplier-payments/open-items?supplier=&currency=`

What a transfer to this supplier could be applied to — open balances and open
PIs, matched on the supplier's name however the PO spells it (see "Supplier
names" below), each with `amount`, `applied`, `remaining` — **and** what is
already paid (`open: false`, `paidOn`), listed so the operator sees it while
ticking. A balance carries `allocations[]`: each PO's part, `linesOnBoard` /
`linesTotal` and `unitsOnBoard` / `unitsTotal` (destroyed samples excluded),
and that PO's `deposit` (`{ source: "pi" | "payment", status, amount, paidOn }`,
null when nothing is on file). A balance-type PI carries the same for its PO;
a deposit or 100 % PI is the deposit and carries none.

```json
{ "supplier": "SUNMED", "currency": "USD", "items": [
  { "kind": "balance", "id": 96, "label": "308", "amount": 5000, "applied": 0, "remaining": 5000, "currency": "USD", "status": "pending", "dueDate": "2026-09-30", "invoiceNumber": "SM-BAL-308", "shipmentId": 61, "shipmentReference": "308", "purchaseOrderId": null, "poNumber": null, "paymentType": null, "supplierName": "SUNMED" },
  { "kind": "pi", "id": 124, "label": "PO_00393J", "amount": 125319.46, "applied": 0, "remaining": 125319.46, "currency": "USD", "status": "pending", "dueDate": null, "invoiceNumber": null, "shipmentId": null, "shipmentReference": null, "purchaseOrderId": 393, "poNumber": "PO_00393J", "paymentType": "full", "supplierName": "Suzhou Sunmed Co.,Ltd." } ] }
```

Deposits on POs with no PI, and balances for boxes with no record yet, are
**not** listed: only the Payments page knows the projected amounts, so it adds
them itself (`kind: "po_deposit"`, `id` = the PO; `kind: "container_balance"`,
`id` = the shipment, with the per-PO make-up from the goods on board). It also
annotates a PI spread over several boxes with how much of it each box holds,
so one box's share can be paid as a part payment.

### `POST /api/v1/supplier-payments`

```json
{ "supplierName": "SUNMED", "amount": 5800, "currency": "USD", "paidOn": "2026-09-19",
  "bankRef": "…", "note": null,
  "lines": [ { "kind": "balance", "id": 96, "amount": 5000 }, { "kind": "balance", "id": 97, "amount": 800 } ],
  "documentId": 75 }             // optional: the uploaded proof, attached to the transfer
```

Refusals: `400 BAD_FIELD` (no `paidOn`, bad currency…) · `400 BAD_LINES` (none,
unknown kind, the same target twice) · `404 TARGET_NOT_FOUND { kind, id }` ·
`422 CURRENCY_MISMATCH { kind, id, targetCurrency }` · `422 TARGET_NOT_OPEN`
(skipped) · `422 OVER_APPLIED { kind, id, remaining, applied }` (a line above
what is still owed) · `422 NOT_FULLY_ALLOCATED` (the line would settle a balance
not yet split across its POs) · `422 OVER_ALLOCATED { linesTotal, amount }`
(lines add up to more than was sent; less is fine and comes back as
`unexplained`) · `409 DOCUMENT_LINKED { paymentId }`. `warnings[]` on success:
a line filed under another spelling of the supplier, a deposit applied to a PO
that has a deposit PI (apply to the PI instead).

`PUT /:id` takes any header field and/or `lines` (replaced wholesale: what the
transfer had settled is reverted first, then the new lines applied).
`DELETE /:id` (admin) soft-deletes, reverts, and frees its proof to be applied
again.

### Supplier names

A balance is filed under the name the page shows (JFPRO's); a PO often carries
a short one ("SUNMED" vs "Suzhou Sunmed Co.,Ltd."). Open items and warnings
match names by their distinctive words: every word of the shorter name, legal
noise aside ("Co", "Ltd", "Sanayi"…), must appear in the longer. A shared city
name is not a match — Suzhou Sunmed and Suzhou Quanjuda are two suppliers.

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
`relink`) or `'supplier_payment'` (`create`, `update`, `delete`).

## Testing

`npm run dev:local` (TEST database) then `node tools/test-shipment-payments.js`
— it picks a real booked shipment, exercises every route and refusal, and
removes its own rows. Under `LOCAL_USER_TYPE=standard` it asserts the 403s
instead.

Uploads need S3 locally: `PO_DOCS_BUCKET` in `.env` plus working
`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (the default AWS credential
chain — the `SP_AWS_*` pair is the Selling Partner API's and is never read by
the S3 client). Without them a local `POST /shipment-payment-documents` answers
"PO_DOCS_BUCKET env var not configured", or a 500 with `CredentialsProviderError`
/ a signature mismatch in the server log — the same limitation the PI upload
has. The HTTP suite inserts document rows directly, so it needs neither. The reader itself (prompt, schema, reference matching, every DB
write and the re-run rules) is exercised against the real Gemini API and the
TEST database by calling `runShipmentPaymentExtraction` directly with the S3
fetch stubbed.
