# Signed PI (PI_signed) — frontend feature brief

A description you can hand to an AI/dev to build the UI. The backend is already
implemented; this is the API contract + suggested UX.

## What we're adding

Each **Purchase Order** can already have supplier invoices ("PI") uploaded. We've
added a parallel document stream: a **signed proforma invoice ("PI_signed")** — a
signed copy of the PI uploaded against the PO, which can then be **sent to the
supplier**.

> **Since 2026-11-12 the send is a link to the supplier portal, not an
> attachment**, exactly as the PO send is: see `po-portal-send.md`, "Signed PIs".
> Section 3 below describes the send as it is now.

It behaves like the existing invoice upload, with two differences:
- It lives in its **own list** on the PO (separate from invoices/payments).
- It runs **no AI checks** (invoices auto-run a Gemini PO-vs-PI check; signed PIs
  do not — it's just a stored, sendable file).

## API reference

All endpoints use the existing API base + the same Google JWT auth as the rest of
the app. A PO is identified by its numeric **id** in the path.

### 1. Upload a signed PI

`POST /api/v1/purchase-orders/:id/signed-pis`

```json
{
  "filename": "PI_signed_SUNMED-85.pdf",
  "contentType": "application/pdf",
  "dataBase64": "JVBERi0xLjQ...",
  "notes": "Signed by supplier 2026-05-29"
}
```

- `filename` (required), `dataBase64` (required) — the file as base64. Accepts raw
  base64 or a full `data:...;base64,...` string (FileReader output works directly).
- `contentType` (optional, defaults to `application/octet-stream`), `notes` (optional).
- Max file size **10 MB** → `413` if exceeded.

**201 response (`SignedPI`):**
```json
{
  "id": 12,
  "purchaseOrderId": 5,
  "filename": "PI_signed_SUNMED-85.pdf",
  "url": "https://.../signed-pis/<token>/PI_signed_SUNMED-85.pdf",
  "contentType": "application/pdf",
  "fileSize": 84211,
  "notes": "Signed by supplier 2026-05-29",
  "uploadedByEmail": "ops@built-form.co.uk",
  "uploadedAt": "2026-05-29T10:00:00.000Z"
}
```
- `url` is a public, directly-openable link.
- `404` if the PO doesn't exist.

### 2. List a PO's signed PIs

`GET /api/v1/purchase-orders/:id/signed-pis` → `{ "data": [ SignedPI, … ] }`

Newest first. Each row is a `SignedPI` (as above) plus its send history and who
opened it on the supplier portal:
```json
{
  "id": 12,
  "...": "...",
  "sends": [
    { "id": 3, "sentTo": ["supplier@example.com"], "subject": "Signed PI – PO_00005J",
      "sentByEmail": "ops@…", "sentAt": "2026-05-29T10:05:00.000Z",
      "frontMessageUid": "...", "frontConversationId": "..." }
  ],
  "views": [
    { "email": "supplier@example.com", "firstViewedAt": "2026-05-29T11:00:00Z",
      "lastViewedAt": "2026-05-30T08:00:00Z", "count": 2 }
  ]
}
```
`views` is `[]` until someone opens it (and wherever the portal's table is not
there). The `GET /orders` bundle's `signedPis` carry `sends` but not `views`.

### 3. Send a signed PI (a link to the supplier portal)

`POST /api/v1/purchase-order-signed-pis/:id/email`

```json
{ "to": "supplier@example.com" }
```
- **Nothing is attached.** The email (via Front) tells the supplier the signed PI
  is on the supplier portal, with a button to the order's page there
  (`${SUPPLIER_PORTAL_URL}/orders/<po id>?tab=pdfs`) and how to register. The
  signed PI is listed on that page once this send row exists.
- `to`: a single address or an array; each entry may hold several addresses
  (`,` `;` or new lines), `Name <a@b.com>` or `mailto:`. Anything that is still
  not one plain address → `400 { error, invalid: [...] }`, nothing sent.
- Every recipient is **pre-approved** on the portal for the PO's supplier (and its
  lines' suppliers) before the email goes out, as for the PO send.
- Optional `subject` / `body` overrides — omit to use the stored `purchase_order_signed_pi`
  email template (`body` is HTML). `{poNumber}` is filled in either way.
- **200 response:** `{ ok, sendId, signedPiId, purchaseOrderId, sentTo, subject, sentAt, frontMessageUid, frontConversationId, portalUrl, portalAccess }`.
  `portalAccess` is the PO send's: `{ status, suppliers, poSupplierMatched, users: [{ email, outcome, suppliersAdded }] }`.
- `404` if not found; `409 { code: "PORTAL_URL_UNSET" }` if the portal's address is
  not configured; `502 { error, frontStatus, frontBody }` if Front rejects.

### 4. Delete a signed PI

`DELETE /api/v1/purchase-order-signed-pis/:id` → `204` (soft delete; `404` if not found).

## Suggested UX

1. On the PO detail view, add a **"Signed PIs"** section alongside Invoices.
2. **Upload** — file picker → read as base64 → POST to `…/signed-pis`. Show the
   row immediately (no AI-check spinner, unlike invoices).
3. **List** — each signed PI shows filename, open/download (`url`), uploaded-by/at,
   notes, and a **"Send"** action.
4. **Send dialog** — collect one or more recipient emails (optionally let the user
   edit the subject/body — pre-fill by `GET /api/v1/email-templates/purchase_order_signed_pi`)
   → call the email endpoint → append the result to that row's `sends` history.
   Say in the dialog that the file is not attached, and after the send alert the
   sender only when `portalAccess` needs a look (as the PO send dialog does).
5. **Delete** — soft-delete with a confirm.

## Notes for the implementer

- No AI/verification step — the upload response is final.
- `to` accepts a string or array; every address is validated server-side.
- Email subject/body come from the editable `purchase_order_signed_pi` template;
  only send `subject`/`body` if the user customised them. The fixed portal block
  (button, registration, "no file is attached") is appended under the body by the
  server and is not part of the template.
- These endpoints require the backend to be deployed (`serverless deploy`); the
  routes already exist, so no further infra change is needed.
