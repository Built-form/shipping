# PO Invoice Payment Instructions — frontend guide

When a supplier **proforma/commercial invoice (PI)** is uploaded to a PO, the
backend already runs a Gemini PO-vs-PI check. That same check now **also
extracts the payment instructions** from the PDF — how much to pay now
(deposit/upfront or full), by when, and the beneficiary bank details — so they
can be shown to the operator and later actioned by a payment job (WorldFirst).

All paths share the same base URL + auth as the rest of the API: send the
Google JWT as `Authorization: Bearer <token>` on every request.

> One record **per invoice (PI)**. If a newer PI is uploaded to the same PO,
> it's a new invoice with its own payment record — they don't overwrite.

---

## Where the data appears

It's attached to each invoice in the existing invoices list — no new fetch:

### `GET /api/v1/purchase-orders/:id/invoices`

```json
{ "data": [
  {
    "id": 59,
    "filename": "PI$12015.36 JFA-1.pdf",
    "url": "https://…s3…/invoices/…",
    "uploadedAt": "2026-06-19T09:50:00.000Z",
    "latestCheck": { "status": "succeeded", "verdict": "minor_discrepancies", "…": "…" },

    "payment": {
      "id": 1,
      "invoiceId": 59,
      "purchaseOrderId": 307,
      "paymentType": "deposit",          // deposit | balance | full | other | unknown
      "amountDue": 3604.61,              // what to actually pay now
      "currency": "USD",
      "depositPercentage": 30,           // null if terms aren't a %
      "invoiceTotal": 12015.36,          // full invoice total, for context
      "dueDate": "2026-06-26",           // YYYY-MM-DD, or null if none stated
      "dueTerms": "30% T/T in advance, 70% B/L copy.",
      "beneficiaryName": "NINGBO TAPE INDUSTRIAL CO., LTD",
      "bankName": "SHANGHAI PUDONG DEVELOPMENT BANK, NINGBO BRANCH",
      "bankAddress": "NO. 21, JIANG XIA STREET, NINGBO, ZHEJIANG, CHINA",
      "accountNumber": "94150078814300001772",
      "iban": null,
      "swiftBic": "SPDBCNSH342",
      "intermediaryBank": "CITIBANK N.A., NEW YORK (SWIFT: CITIUS33)",
      "paymentReference": "NBTB260605",
      "rawTermsText": "PAYMENT: 30% T/T IN ADVANCE … (verbatim block from the PI)",
      "paymentStatus": "pending",        // pending | arranged | paid | skipped
      "modelUsed": "gemini-3-flash-preview",
      "createdAt": "2026-06-19T09:58:45.000Z",
      "updatedAt": "2026-06-19T09:58:45.000Z"
    }
  }
] }
```

### Important rendering rules

- **`payment` is `null` until the check finishes.** Upload kicks off the check
  in the background (`latestCheck.status: "processing"/"pending"`). Poll the
  invoices list (same as you already do for `latestCheck`); `payment` fills in
  when the check succeeds. If `latestCheck.status` is `failed`, `payment` stays
  `null`.
- **Every field is best-effort and may be `null`** — extraction never fails the
  check. Always show `rawTermsText` somewhere (tooltip / "show original") so the
  operator can sanity-check the parsed numbers before paying.
- **`amountDue` is the figure to pay now**, not the invoice total. For a deposit
  it's the deposit amount; show `depositPercentage` / `invoiceTotal` as context.
- **Treat bank details as display-only strings.** Don't reformat or "tidy"
  account numbers / IBAN / SWIFT — copy them verbatim.
- A re-check (manual `POST …/invoices/:invoiceId/check`) **refreshes the
  extracted fields but never resets `paymentStatus`** — a payment you've already
  marked `arranged`/`paid` stays that way.

---

## Updating the lifecycle — `PUT …/payment-status`

Flip the payment through its lifecycle (this is what the operator clicks, and
what the future WorldFirst job will call once a transfer is arranged/settled).
It only touches `payment_status`; the extracted terms are untouched.

```
PUT /api/v1/purchase-orders/:poId/invoices/:invoiceId/payment-status
```
**Body**
```json
{ "paymentStatus": "arranged" }     // pending | arranged | paid | skipped
```
**Response `200`** — the full updated `payment` object (same shape as above).

Errors: `400` (invalid `paymentStatus`), `404` (no payment record for that
invoice on that PO — e.g. the check hasn't produced one yet).

---

## Suggested UI

On the PO's invoices panel, for each invoice with a `payment`:

1. A **"Pay now" badge**: `{amountDue} {currency}` + a `paymentType` chip
   (`Deposit 30%` / `Balance` / `Full`), and `dueDate` (or `dueTerms` if no
   concrete date).
2. A **bank-details block** (beneficiary, bank, account/IBAN, SWIFT,
   correspondent bank, reference) with a copy-to-clipboard button per field and
   a "show original terms" reveal for `rawTermsText`.
3. A **status control** (pending → arranged → paid, or skipped) wired to
   `PUT …/payment-status`, optimistically updating from the returned object.

While `latestCheck.status` is `processing`/`pending`, show a spinner in the
payment area instead of "no payment info".
