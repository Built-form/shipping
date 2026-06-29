# Forwarder quote document — frontend feature brief

A description you can hand to an AI/dev to build the UI. The backend is already
implemented; this is the API contract + suggested UX.

## What we're adding

Draft containers already generate a **delivery quote request** PDF. We've added a
second flavour: a **forwarder quote** — a detailed, carton-level PDF a freight
forwarder can price a shipment from. It's the same generate/list/email flow as
the existing quote, just a different `type`.

So a draft container can now have **two independent document streams**:
- `quote` — the existing simple delivery quote request (unchanged)
- `forwarder-quote` — the new detailed carton-level quote

Each type has its **own version sequence** (forwarder-quote v1, v2… separate from
quote v1, v2…).

## What's in the forwarder-quote PDF

Landscape A4, one row per order allocated to the container, with a TOTAL row.
Columns:

| Column | Notes |
|---|---|
| SKU | product SKU |
| HW/JF Code | e.g. JF1292 |
| Ordered Units | units allocated to this container |
| Carton Weight (kg) | falls back to unit weight × units-per-carton when not set |
| Units per Carton | |
| Carton H / L / W (cm) | carton dimensions |
| Carton CBM | per-carton volume |
| No. of Cartons | ceil(units ÷ units-per-carton) |
| Total CBM | carton CBM × no. of cartons |
| Supplier | |
| Port | origin port |
| PO Number | |

TOTAL row sums Ordered Units, No. of Cartons, and Total CBM.

The frontend doesn't build the table — the backend renders the PDF. The UI just
triggers generation, lists versions, lets the user open/download, and optionally
emails a version to a forwarder.

## API reference

All endpoints use the existing API base + the same Google JWT auth as the rest of
the app. A draft container is identified by its **name** in the path (URL-encode
it — names contain spaces, e.g. `Shanghai%20Mixed%20Container%202`).

### 1. Generate a forwarder quote

`POST /api/v1/draft-containers/:name/generate`

```json
{ "type": "forwarder-quote", "comments": "Please quote DDP to our UK warehouse." }
```

- `type`: `"forwarder-quote"` for the new PDF. Omit it (or send `"quote"`) for the
  existing delivery quote. Any other value → `400`.
- `comments`: optional free text printed in a box under the table.
- `splitBySupplier`: **defaults to `true` for forwarder quotes.** The backend also
  generates one **forwarder-quote** PDF per distinct supplier in the container (same
  carton-level forwarder layout, filtered to that supplier, file named after the
  supplier) alongside the main combined PDF. Every PDF in the set is
  `type:"forwarder-quote"`. Send `"splitBySupplier": false` to get only the combined
  PDF. Ignored for `"quote"`.

**201 response** (split on — the default):
```json
{
  "documentId": 42,
  "draftContainerName": "Shanghai Mixed Container 2",
  "type": "forwarder-quote",
  "version": 1,
  "fileSize": 7642,
  "url": "https://.../draft-containers/<token>/forwarder-quote-Shanghai_Mixed_Container_2-v1.pdf",
  "batchId": "b1c2…",
  "supplierDocuments": [
    { "documentId": 43, "type": "forwarder-quote", "supplier": "Acme Mfg",
      "version": 1, "fileSize": 5120,
      "url": "https://.../forwarder-quote-AcmeMfg-Shanghai_Mixed_Container_2-v1.pdf" },
    { "documentId": 44, "type": "forwarder-quote", "supplier": "Bright Foam Co",
      "version": 1, "fileSize": 4880,
      "url": "https://.../forwarder-quote-BrightFoamCo-Shanghai_Mixed_Container_2-v1.pdf" }
  ]
}
```

- The top-level fields are always the **main combined** forwarder PDF (`supplier:null`).
- `batchId` is present whenever the split ran and is shared by the combined PDF and
  every entry in `supplierDocuments` — it's the key that the forwarder email groups
  on (§3) and that the UI should group on.
- `supplierDocuments` is present whenever the split ran (omitted when
  `splitBySupplier:false`). Each entry is a full document (`type:"forwarder-quote"`
  scoped to one supplier, own version sequence, openable `url`) and also appears in
  the documents list.
- `failedSuppliers` (array of names) is included only if a per-supplier PDF failed
  to render — the main PDF and the other suppliers still succeed.
- Suppliers with a blank supplier field on their orders get **no** per-supplier PDF
  (those lines still appear in the combined PDF).
- `url` is a public, directly-openable PDF link.
- `400 { "error": "Draft container \"…\" has no allocations." }` if the container
  has no orders allocated to it yet.

### 2. List a draft container's documents

`GET /api/v1/draft-containers/:name/documents` → `{ "data": [ Document, … ] }`

Each `Document`:
```json
{
  "id": 42,
  "draftContainerName": "Shanghai Mixed Container 2",
  "type": "forwarder-quote",
  "version": 1,
  "supplier": null,
  "batchId": "b1c2…",
  "fileSize": 7642,
  "url": "https://.../forwarder-quote-...-v1.pdf",
  "generatedByEmail": "ops@built-form.co.uk",
  "generatedAt": "2026-05-28T10:00:00.000Z",
  "sends": [
    { "id": 9, "sentTo": ["forwarder@example.com"], "subject": "…",
      "sentByEmail": "ops@…", "sentAt": "2026-05-28T10:05:00.000Z" }
  ]
}
```

The list contains **both** types — filter/group by `type` in the UI. `sends` is the
history of times that document was emailed.

**`batchId` ties together the PDFs from one split-generate run** — the combined
forwarder quote (`supplier:null`) and its per-supplier copies all share it
(`null` for non-split documents). **Group the forwarder quotes by `batchId`** to
show the exact set a forwarder email will send: emailing any document in a batch
attaches that whole batch (see §3), so the on-screen group and the sent set match.
Note that versions can differ *within* a batch (each supplier has its own version
counter), so don't group by version — group by `batchId`.

### 3. Email a document to a forwarder

`POST /api/v1/draft-container-documents/:id/email`

```json
{ "to": "forwarder@example.com" }
```
`to` accepts a single address or an array. Sends via Front and returns the send
record.

**Emailing a split forwarder quote attaches that document's whole batch.** When the
`:id` is a `forwarder-quote` that was generated with the per-supplier split, the
email carries the main combined PDF **plus every per-supplier forwarder-quote copy
that shares its `batchId`** (one Front message, multiple attachments — all
`type:"forwarder-quote"`, combined PDF first then suppliers A–Z). It is exactly the
set produced by that one generate run — **not** the latest version of each supplier
across runs. Only forwarder quotes from that batch are grouped; no other document
type or batch is ever mixed in. So group documents by `batchId` in the UI and the
on-screen group will equal what gets sent. Emailing any other document — a plain
`quote` (the Delivery Quote Request), a standalone `supplier-quote`, or a forwarder
quote generated with `splitBySupplier:false` — attaches just that one PDF.

The response gains an `attachments` array listing what was sent, and a send is
recorded against **each** attached document (so each shows this email in its own
`sends` history):
```json
{
  "ok": true,
  "documentId": 42,
  "sentTo": ["forwarder@example.com"],
  "subject": "Delivery Quote Request – Shanghai Mixed Container 2",
  "attachments": [
    { "documentId": 42, "type": "forwarder-quote", "supplier": null, "version": 1,
      "filename": "forwarder-quote-Shanghai_Mixed_Container_2-v1.pdf" },
    { "documentId": 43, "type": "forwarder-quote", "supplier": "Acme Mfg", "version": 1,
      "filename": "forwarder-quote-AcmeMfg-Shanghai_Mixed_Container_2-v1.pdf" }
  ]
}
```

## Suggested UX

1. **Two generate actions** on the draft-container detail view: keep the existing
   "Generate Quote", add **"Generate Forwarder Quote"** (calls the same endpoint
   with `type: "forwarder-quote"`). An optional comments field feeds `comments`.
2. **Documents list split by type** — two sections (or a type filter): "Quotes"
   and "Forwarder Quotes", with open/download (`url`), generated-by/at, and a "Send
   to forwarder" action. "Quotes" can be a flat version history; **"Forwarder
   Quotes" must be grouped by `batchId`** (combined + per-supplier copies) — see
   "Fixing the mixed-batch view" below.
3. **Send dialog** — collect one or more recipient emails → call the email
   endpoint → show the result in that document's `sends` history.
4. **Empty / error states** — if the container has no allocations, the generate
   call returns `400`; surface "Allocate orders to this container before
   generating a quote."

## Notes for the implementer

- Generation is synchronous: the POST returns once the PDF is built and stored,
  with the final `url`. No polling needed.
- Versions are per type — don't assume a single global version counter.
- "Ordered Units" = units **allocated to this container**, not the full PO order
  quantity. (Backend can switch this if needed — ask if the forwarder should see
  full order quantities instead.)
- These endpoints require the backend to be deployed (`serverless deploy`); the
  route already exists, so no infra change is needed.

## Fixing the mixed-batch view

**Symptom:** the forwarder-quote tree shows a set like `Combined v4 / MANFO v4 /
PACKRICH v2 / YOHO v1` — versions from different generate runs collapsed into one
group. That mix does **not** equal what a send attaches: the email sends exactly one
batch, never a latest-per-supplier mix.

**Cause:** the UI groups forwarder quotes by supplier (latest version per supplier),
which spans multiple `batchId`s.

**Rule:** group forwarder quotes by **`batchId`**, never by supplier or version. One
`batchId` = one split-generate run = one email = one on-screen group.

### Build the grouped view
```js
const forwarder = data.filter(d => d.type === 'forwarder-quote');

// One group per batch. Do NOT dedupe by supplier across batches.
const batches = new Map();   // batchId -> { batchId, combined, suppliers[], generatedAt }
const standalone = [];       // forwarder quotes with no batch (splitBySupplier:false)

for (const d of forwarder) {
  if (!d.batchId) { standalone.push(d); continue; }
  if (!batches.has(d.batchId)) batches.set(d.batchId, { batchId: d.batchId, combined: null, suppliers: [] });
  const g = batches.get(d.batchId);
  if (d.supplier === null) g.combined = d;   // the combined PDF
  else g.suppliers.push(d);                  // a per-supplier copy
}

// Within a batch: combined first, then suppliers A–Z (matches the email order)
for (const g of batches.values()) {
  g.suppliers.sort((a, b) => a.supplier.localeCompare(b.supplier));
  g.generatedAt = g.combined?.generatedAt ?? g.suppliers[0]?.generatedAt;
}

// Batches newest-first; the top one is the current quote to send
const groups = [...batches.values()].sort((a, b) => new Date(b.generatedAt) - new Date(a.generatedAt));
```

### Render
- Each batch is one card/row. Header = the **combined** PDF (`supplier:null`) — this
  is the set the email sends. Children = the per-supplier copies, A–Z.
- **Don't** put a single version on the group header — versions legitimately differ
  per supplier within a batch. Show version per child only, or label the group by
  `generatedAt` ("Run from <date>") instead.

### Send
`POST /api/v1/draft-container-documents/:id/email` with the `id` of **any** document
in the batch (use the combined PDF's `id`). The backend attaches that whole batch —
combined + every per-supplier copy sharing the `batchId`, all `type:"forwarder-quote"`,
combined first. Show the response's `attachments[]` as confirmation of what went out.

### Standalone (unchanged)
- `forwarder-quote` with `batchId:null` → generated `splitBySupplier:false`; emails
  only itself.
- `quote` (Delivery Quote Request) and standalone `supplier-quote` → email attaches
  just that one PDF.

### Data note (not a UI bug)
Within one batch you may see near-duplicate suppliers (e.g. `PACKRICH` and `Xiamen
Packrich Imp and Exp Co Ltd`). The split makes one copy per **distinct supplier
string** on the orders, and the order data uses inconsistent names. Grouping is
correct; merging them requires normalizing the orders' `supplier` values (a data
fix, not UI).
