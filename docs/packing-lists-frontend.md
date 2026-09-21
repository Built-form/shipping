# Packing lists — frontend contract

Upload a supplier's packing list against a container, pick the supplier, and see every row compared with the lines we expect from that supplier **in that container**. Gemini (`gemini-3.8-flash`, Pro fallback) reads the PDF **without** seeing what we expect, and the comparison is done in code.

All routes need the usual Bearer token.

## What "expected" means: the container's lines, not the PO

A PO is often split across containers. The check always compares against this container's share of each line, never the whole PO:

| Container | Expected lines | Expected quantity |
|---|---|---|
| **Booked** (e.g. `324`) | Live orders with that `container_number` | `orders.quantity`. Booking a partial quantity splits the order row, so each container has its own row. |
| **Draft** (e.g. `DRAFT-SEA-260917-173825 - 328`) | That draft's allocation lines | The line's **allocated** quantity. The order row itself still holds the whole PO line, and one order can be split across two drafts. |

Planned containers are not supported.

## 1. Pick the container and supplier

`GET /api/v1/packing-lists/context?containerNumber=328` or `?draftContainerId=129`

`containerNumber` is free text. It is tried in this order:

1. A booked reference (`324`) or the carrier's box number (`MRKU4645188`).
2. An exact draft name.
3. The number an **open draft with lines** reserves in its name. `328` finds `DRAFT-SEA-260917-173825 - 328`, and `123` finds `… - 123. Airfreight urgent`.

A booked container wins over a draft that reserves the same number. To choose the draft instead, send `draftContainerId`, which is `draft_containers.id`. `GET /api/v1/draft-container-registry` returns that id, and it survives renames.

```json
{
  "container": { "kind": "draft", "containerNumber": "328", "draftContainerId": 129,
                 "draftName": "DRAFT-SEA-260917-173825 - 328", "label": "DRAFT-SEA-260917-173825 - 328" },
  "suppliers": [
    { "supplierKey": "s:11", "supplierName": "Roosin Medical Co., Ltd.",
      "names": ["…"], "poNumbers": ["…"], "orderCount": 11, "units": 21864 }
  ]
}
```

- `units` is this container's share (allocated units for a draft).
- A supplier is a company, not one spelling of its name: every name variant that shares a supplier-portal code appears in one entry.

| Error | Meaning |
|---|---|
| 404 `CONTAINER_NOT_FOUND` | No booked container or open draft matches. |
| 409 `AMBIGUOUS_CONTAINER` | Several open drafts reserve that number. `candidates` gives `[{ draftContainerId, draftName, lineCount, units, … }]`; let the user pick one and send its `draftContainerId`. |
| 422 `CONTAINER_EMPTY` | The draft has no lines. |

## 2. Upload

`POST /api/v1/packing-lists`

```json
{ "draftContainerId": 129, "supplierKey": "s:11",
  "filename": "PL-328.pdf", "contentType": "application/pdf",
  "dataBase64": "<base64, data: prefix allowed>" }
```

- Identify the container with `containerNumber` or `draftContainerId`, the same as in `/context`.
- Choose the supplier with `supplierKey` from `/context` (preferred), or `supplierId`, or `supplierName`. `"n:"` is the group for lines with no supplier name.
- **Files up to 4 MB:** send them inline as `dataBase64`. The limit comes from API Gateway's 6 MB request cap after base64 expansion; a bigger body gets a bare gateway 413 with no JSON.
- **Files over 4 MB (up to 18 MB):**
  1. Call `POST /api/v1/packing-lists/upload-url` with `{ filename, contentType }`. It returns `{ uploadUrl, s3Key, uploadHeaders }`.
  2. `PUT` the bytes to `uploadUrl` with `uploadHeaders`.
  3. POST as above with `s3Key` in place of `dataBase64`.

  Possible errors: 409 `NOT_UPLOADED` means the PUT hasn't landed yet; 409 `DUPLICATE_UPLOAD` means that `s3Key` is already registered.
- The response is `201 { packingList }` with `status: "processing"`, and the PDF is then read in the background (about 20–30 s).
- 422 `SUPPLIER_NOT_ON_CONTAINER` means that supplier has no lines in the container. The body includes `suppliers`. Send `force: true` to check anyway.
- The errors listed in section 1 apply here too.

`packingList` records the container as it was uploaded: `containerKind` (`booked` or `draft`), `containerNumber`, `draftContainerId` and `containerName`.

## 3. Poll for the result

Call `GET /api/v1/packing-lists/:id` every 3–5 s until `packingList.status` is `succeeded` or `failed`. When it is `failed`, `packingList.error` has the reason.

```json
{
  "packingList": { "id": 7, "containerKind": "draft", "draftContainerId": 129, "status": "succeeded",
                   "verdict": "differences", "discrepancyCount": 1, "…": "…" },
  "extracted": { "lines": [ /* every row as read */ ], "totalQuantity": 22024, "…": "…" },
  "comparisonIsLive": true,
  "comparison": {
    "container": { "kind": "draft", "draftContainerId": 129, "draftName": "…", "label": "…" },
    "summary": { "verdict": "differences", "matched": 10, "mismatched": 1, "missingFromPackingList": 0,
                 "notExpected": 0, "discrepancyCount": 1, "expectedUnits": 21864, "packedUnits": 22024 },
    "extractionCheck": { "printedTotalQuantity": 22024, "sumOfRowsQuantity": 22024, "ok": true, "…": "…" },
    "skippedLines": [],
    "lines": [
      { "status": "mismatch", "severity": "error", "matchMethod": "jf_po",
        "jfCode": "JF1053", "poNumbers": ["PO_00337J"],
        "expected": { "quantity": 340, "cartons": 34,
                      "orders": [ { "orderId": 1141, "quantity": 340, "orderQuantity": 500, "allocationId": 123, "…": "…" } ] },
        "packed":   { "quantity": 500, "cartons": 50, "unit": "PC", "rows": [ "…" ] },
        "differences": [ { "field": "quantity", "severity": "error", "expected": 340, "actual": 500,
                           "message": "Packed 500, expected 340 (+160)." } ],
        "elsewhere": [ { "kind": "draft", "where": "DRAFT-AIR-260916-154300 - 123. Airfreight urgent",
                         "orderId": 1141, "quantity": 160, "status": "READY", "…": "…" } ] }
    ]
  }
}
```

### How to render it

- Show `comparison.lines` as a table. It is already sorted: `missing`, then `unexpected`, then `mismatch`, then `match`.
- `status` values:
  - `match`: nothing is wrong. There may still be `info` differences.
  - `mismatch`: at least one `error` or `warning` difference was found.
  - `missing`: expected in this container but not on the packing list.
  - `unexpected`: on the packing list but not expected from this supplier in this container.
- Severities. Show each difference's `message` as its text.
  - `error` (red): `quantity`, `lot`, `expDate`, or a whole line (`line`).
  - `warning` (amber): `mfgDate`, `cartons`, `poNumber`.
  - `info` (grey): `cartonWeight`, `cartonDimensions`.
- **Split lines.**
  - `expected.orders[].quantity` is this container's share and `orderQuantity` is the whole order row. When they differ, show "340 of 500 in this container".
  - `elsewhere` appears on `unexpected` lines and on lines with a quantity error. It says where the rest of that product/PO line is:
    - `kind: "booked"`: `where` is the container number.
    - `kind: "draft"` or `"planned"`: `where` is the draft or planned container's name.
    - `kind: "unallocated"`: `where` is null and the stock is still at the factory.
  - For example: packed 500, expected 340, and the other 160 are in air draft 123.
- `skippedLines` lists draft lines whose order is already booked into a container (`bookedIn`). They are left out, so their units are not counted twice. If the list is non-empty, show a note.
- `matchMethod: "jf_only"` means the line matched on product code only, because the PO on the packing list didn't match. This always comes with a `poNumber` warning.
- If `extractionCheck.ok` is false, the rows Gemini read don't add up to the totals printed on the document. Show a "the reading may have missed a row" banner.
- Dates compare by month. QC-sample orders (`JF0197_FQC`) are grouped with their product.

### Live comparison

`GET /:id` recomputes the comparison against the container's lines **as they are now**. Fix an order or change an allocation, reload, and that difference disappears; the PDF is not read again.

A packing list uploaded against a draft follows that draft:

- **After a rename:** found through `draftContainerId`.
- **After booking:** compared against the booked container. `comparison.container.followedFrom` names the original draft; show "now compared against booked container 328".
- **If neither can be found:** the response falls back to the analysis-time comparison, with `comparisonIsLive: false` and a `liveUnavailableReason`.

Add `?snapshot=1` to always get the analysis-time comparison. The list view's `verdict` and `discrepancyCount` are also from analysis time.

## Other routes

| Route | Purpose |
|---|---|
| `GET /api/v1/packing-lists?containerNumber=328[&draftContainerId=129][&supplierKey=s:11]` | List, newest first. `containerNumber` matches the booked reference, a draft's reserved number, or its name. |
| `POST /api/v1/packing-lists/:id/analyze` | Read the PDF again. Optional body `{ "model": "gemini-3.1-pro-preview" }`. Returns 202; then poll. |
| `GET /api/v1/packing-lists/:id/download[?disposition=attachment]` | Returns `{ url }`, a presigned link valid for 5 minutes. Audited. |
| `DELETE /api/v1/packing-lists/:id` | Soft delete. Returns 204. |
