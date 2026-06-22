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

**201 response:**
```json
{
  "documentId": 42,
  "draftContainerName": "Shanghai Mixed Container 2",
  "type": "forwarder-quote",
  "version": 1,
  "fileSize": 7642,
  "url": "https://.../draft-containers/<token>/forwarder-quote-Shanghai_Mixed_Container_2-v1.pdf"
}
```

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

### 3. Email a document to a forwarder

`POST /api/v1/draft-container-documents/:id/email`

```json
{ "to": "forwarder@example.com" }
```
`to` accepts a single address or an array. Works for either document type
(attaches the chosen PDF and sends via Front). Returns the send record.

## Suggested UX

1. **Two generate actions** on the draft-container detail view: keep the existing
   "Generate Quote", add **"Generate Forwarder Quote"** (calls the same endpoint
   with `type: "forwarder-quote"`). An optional comments field feeds `comments`.
2. **Documents list split by type** — two sections (or a type filter): "Quotes"
   and "Forwarder Quotes", each showing its own version history (v1, v2, …) with
   open/download (`url`), generated-by/at, and a "Send to forwarder" action.
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
