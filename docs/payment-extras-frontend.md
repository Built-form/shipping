# Extra charges and credits — frontend contract

Money a supplier bills that is not goods (mould, tooling, handling, samples,
testing, freight, packaging, bank charges) and credits they give (a discount,
damaged goods) are added to a payment on ShipLine's Payments flow page by a
person, with a reason. What is owed is otherwise worked out from the terms ×
the goods on board, and an uploaded invoice never changes it by itself; an
extra is how the "extra bit" gets in.

Rules and shapes: `src/lib/payment-extras.js`. Routes:
`src/services/payment-extra-routes.js`. Schema:
`src/db/migrate/2026-09-29_10_payment_extras.sql`.

## The model

- An extra **rides with** a payment the page already works out:
  - `deposit` — a PO's deposit (`purchaseOrderId` required, no container);
  - `balance` — one supplier's balance in one container (`shipmentId` and/or
    `shipmentReference` required, `purchaseOrderId` optional).
  It is due, signed off and paid with that payment. When that payment is
  already paid, the extra's own `dueDate` applies (else it is due now).
- `amount` is **signed**: a credit is negative. `kind: "discount"` must be negative.
- Never part of the PO value: not split by the deposit %, never moves a
  container share. The page adds it on top.
- The sign-off keys are unchanged: an extra joins its payment's key, so adding
  or editing one changes that payment's figure and its sign-offs go stale.
- `status` is `open` or `paid`. A transfer settles it (see below); **mark paid**
  is for money that went out with no transfer recorded in ShipLine.

Who may write: `admin`, `standard`, `accountant` (403 `CANNOT_EDIT_EXTRAS`
otherwise). Reading is open to every allowlisted user.

## `GET /api/v1/payment-extras?supplier=&currency=&purchaseOrderId=&shipmentId=`

Every live extra, open and paid: `{ data: [extra] }`.

```json
{ "id": 7, "supplierName": "Suzhou Sunmed Co.,Ltd.", "supplierKey": "suzhou sunmed co.,ltd.", "currency": "USD",
  "amount": 500, "kind": "mould", "label": "PO_00395J mould cost", "description": "Mould for JF1375",
  "ridesWith": "deposit", "purchaseOrderId": 395, "poNumber": "PO_00395J", "shipmentId": null, "shipmentReference": null,
  "dueDate": null, "sourceKind": null, "sourceId": null,
  "status": "open", "paidOn": null, "settledByPaymentId": null, "applied": 0, "remaining": 500,
  "note": null, "createdByEmail": "…", "createdAt": "…", "updatedByEmail": null, "updatedAt": "…",
  "settlements": [] }
```

`applied` = what live transfers applied (signed); `remaining` = `amount −
applied` while open, 0 once paid. `settlements[]` as on a balance (each
transfer that touched it, its proofs, what else it covered).

## `POST /api/v1/payment-extras`

```json
{ "supplierName": "Suzhou Sunmed Co.,Ltd.", "currency": "USD", "amount": -120, "kind": "discount",
  "description": "Damaged cartons", "ridesWith": "balance", "shipmentId": 62, "shipmentReference": "301",
  "purchaseOrderId": null, "dueDate": null, "sourceKind": "shipment_document", "sourceId": 11, "note": null }
```

201 `{ ...extra, warnings }`. `warnings[]`: the PO is filed under another
spelling of the supplier (the page names suppliers as JFPRO does).

Refusals: `400 BAD_FIELD` · `404 NOT_FOUND` (the PO or shipment) ·
`422 CURRENCY_MISMATCH` (not the PO's currency) · `409 ALREADY_ADDED
{ extraId }` (the same `sourceKind` + `sourceId` + `amount` — a suggestion
added twice).

`sourceKind: "shipment_document"` + `sourceId` = the uploaded container invoice
it was read from (both or neither).

## `PUT /api/v1/payment-extras/:id`

Same body as POST; replaces the details. Once a transfer has applied money to
it, it must stay what that money paid: `409 EXTRA_HAS_PAYMENTS` for a new
currency or supplier, a charge turned credit (or back), or an amount below what
was applied. Raising a settled extra above what was applied opens it again;
bringing it back within what was applied settles it again (by the last
transfer that paid it).

## `DELETE /api/v1/payment-extras/:id`

Soft. `409 EXTRA_HAS_PAYMENTS` while any live transfer applies money to it —
edit or delete that transfer first. 204.

## `POST /api/v1/payment-extras/:id/status`  `{ "status": "paid" | "open", "paidOn"? }`

Paid by hand (default `paidOn` today), or open again. `409 SETTLED_BY_TRANSFER`
when a recorded transfer paid it — undo it through that transfer. Audited as
`payment_extra` `status` with `via: "hand"`.

## Paying one — `/api/v1/supplier-payments`

A line `{ "kind": "extra", "id": 7, "amount": 500 }`. A credit is used as a
negative line: `{ "kind": "extra", "id": 8, "amount": -120 }` next to what is
being paid — `amount` sent is the net. Refusals on top of the usual ones:
`422 SIGN_MISMATCH`, `422 OVER_APPLIED { remaining }`, `422 CREDIT_ALONE`.
Settles when |applied| ≥ |amount| − max(1, 1 %); editing or deleting the
transfer reopens what it settled. `open-items` lists open extras and credits.

## Suggestions from invoices

Both readers return `otherCharges: [{ description, amount, kind }]` — charges on
the document that are not goods, discounts negative, `kind` from the same list
as an extra:

- **Container invoices** (`GET /shipment-payments` documents, `extract_json`):
  also `check.otherCharges` / `check.chargesTotal`; the goods check
  (`invoiceGoods`, `goodsDelta`, `invoiceBalance`) and the "belongs here"
  amount check are net of them. The page offers "Add as extra charge"
  pre-filled with `sourceKind: "shipment_document"`, `sourceId`.
- **PIs** (`latestCheck.result.otherCharges`): the PI's own amount due and
  total already include them, and the page treats those as owed — so the page
  shows them as "already included in this PI's figures", never as an Add.
  Reads made before this field existed have none; re-run a container read to
  get them (safe: that reader never creates or changes a balance). Do not
  re-run PI checks in bulk: a re-check rewrites the PI's amount due and total.

## Counting a charge twice

A PO value can hold more than its lines: charges on the PI (mould, FOB,
pallets…) or, with no PI, the PO's shipping total. The terms already charge
for that money. The deposit takes its share, and the balance share is its own
row on the PO's first container. An extra for the same money would count it
twice, and the server cannot tell, so the page warns and never blocks (user,
2026-09-29). This lives in ShipLine's `extrasGuard.ts`:

- A balance that carries the charges says so under its extras, e.g. "PI
  charges above PO_00326H's lines — 255.00 on the PI, 178.50 of it in this
  payment". The share is the row's `owedInFull ?? amount`.
- A container-invoice charge is shown as "already in this payment", with no
  Add, when it matches one of these (max(1, 1 %)):
  - the whole charge, or its share in this payment;
  - one of the PI's own `otherCharges`, or its share.
  A credit never matches. A different charge that happens to match can still
  be added with "+ Add extra charge or credit".
- The extra-charge editor lists the same charges as a warning. On a deposit,
  it lists the PO's charges and the PI's own read charges.

Audit: `payment_extra` (`create`, `update`, `delete`, `status`); settling and
reverting by a transfer are `status` with `via: "payment"` / `"payment_reverted"`.
