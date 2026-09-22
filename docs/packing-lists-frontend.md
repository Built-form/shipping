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
- The response is `201 { packingList }` with `status: "processing"`, and the PDF is then read in the background (about 20–30 s). Upload every document the supplier sent; they are checked together (section 4).
- **Replacing a document:** send `replacesId` (the old packing list's id). If the link could not be made, the response also carries `replaceRefused: { code, error }` and the upload was kept as an extra document.
- 422 `SUPPLIER_NOT_ON_CONTAINER` means that supplier has no lines in the container. The body includes `suppliers`. Send `force: true` to check anyway.
- The errors listed in section 1 apply here too.

`packingList` records the container as it was uploaded: `containerKind` (`booked` or `draft`), `containerNumber`, `draftContainerId` and `containerName`.

## 3. Poll for the result

Call `GET /api/v1/packing-lists/:id` every 3–5 s until `packingList.status` is `succeeded` or `failed`. When it is `failed`, `packingList.error` has the reason.

The `comparison` it returns is the **supplier's check** (every active document pooled, sign-offs applied — the same thing `GET /check` returns, section 4), plus `packingLists` (the active documents), `superseded`, `signOffs` and `approval`. The row's own `verdict` / `discrepancyCount` describe that document alone at reading time; do not show them as the state of the container.

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
- `matchMethod: "jf_only"` means the line matched on product code only, because the PO on the packing list didn't match. This always comes with a `poNumber` warning. `"no_code"` means the packing-list row printed no product code and was matched by PO or by a unique quantity; it carries a `productCode` warning — ask the user to confirm it is the same product.
- Every line has a stable `lineKey` (product code + PO) and a `fingerprint` (its status and non-info differences). Sign-offs are keyed by `lineKey` and hold while the `fingerprint` is unchanged. Lines carry `accepted` and `signOff` when signed (section 4).
- `packed.rows[].packingListId` says which document each row came from; show it when a supplier has more than one document. `documents[]` is the reading check per document; `extractionCheck.ok` is false if any document fails it.
- If `extractionCheck.ok` is false, the rows Gemini read don't add up to the totals printed on the document. Show a "the reading may have missed a row" banner.
- Dates compare by month. QC-sample orders (`JF0197_FQC`) are grouped with their product.

### Live comparison

`GET /:id` recomputes the comparison against the container's lines **as they are now**. Fix an order or change an allocation, reload, and that difference disappears; the PDF is not read again.

A packing list uploaded against a draft follows that draft:

- **After a rename:** found through `draftContainerId`.
- **After booking:** compared against the booked container. `comparison.container.followedFrom` names the original draft; show "now compared against booked container 328".
- **If neither can be found:** the response falls back to the analysis-time comparison, with `comparisonIsLive: false` and a `liveUnavailableReason`.

Add `?snapshot=1` to get what this document alone said at reading time.

## 4. The review: status, check, sign-offs, approval

The unit of comparison is the **supplier's check**: every active packing list that supplier has for the container (not deleted, not replaced), pooled and compared together — a container often ships under several invoices. Over the check sit two human decisions: a per-line **sign-off** and a container-level **approval**. All of it is computed from the lines as they are now; nothing is cached.

### Status — what a container screen shows

`GET /api/v1/packing-lists/status?containerNumber=324` (or `?draftContainerId=129`). One call.

```json
{
  "container": { "kind": "booked", "containerNumber": "323", "label": "323" },
  "overall": { "verdict": "incomplete", "suppliers": 3, "suppliersWithoutList": 1, "suppliersProcessing": 0, "suppliersFailed": 0,
               "outstanding": 2, "signedOff": 0, "mismatched": 0, "missingProducts": 1, "extraProducts": 1 },
  "approval": null,
  "suppliers": [
    { "supplierKey": "s:44", "supplierName": "Guilin Yoho Co., Ltd", "onBoard": true, "orderCount": 13, "units": 11332,
      "packingLists": [], "superseded": [], "processing": 0, "failed": 0,
      "check": { "verdict": "none", "discrepancyCount": null, "signedOff": null, "outstanding": null, "extractionOk": null, "summary": null, "duplicateInvoices": [] } },
    { "supplierKey": "s:98", "supplierName": "Dongguan Ruiying Technology Co., Ltd.", "onBoard": true, "orderCount": 1, "units": 10000,
      "packingLists": [ { "id": 3, "filename": "…", "version": 1, "status": "succeeded", "…": "…" } ],
      "check": { "verdict": "differences", "discrepancyCount": 2, "signedOff": 0, "outstanding": 2, "extractionOk": true, "summary": { "…": "…" }, "duplicateInvoices": [] } }
  ]
}
```

- `overall.verdict`: `approved` · `approved_stale` (approved, but something changed since) · `none` (no packing lists at all) · `incomplete` (a supplier has no list, or one could not be read) · `differences` (something outstanding) · `processing` · `complete` (every supplier matches or is fully signed off).
- `suppliers[].check.verdict`: `none` · `processing` · `failed` · `match` · `accepted` (every difference signed off) · `differences`. `outstanding` = differences not signed off.
- A supplier with `onBoard: false` has a packing list filed here but no lines in the container (a forced upload).
- Refresh it every ~30 s while a container screen is open, and every few seconds while any supplier has `processing > 0`. Editing an order or an allocation changes the next result.

### Check — one supplier in detail

`GET /api/v1/packing-lists/check?containerNumber=324&supplierKey=s:2` → `{ container, supplier, packingLists, superseded, processing, failed, signOffs, comparison, approval }`. `comparison` is as in section 3, pooled over the active documents with sign-offs applied. Render sign-off controls on every line with `status !== "match"`:

- no `signOff`: a **Sign off** button, with an optional note.
- `accepted: true`: show `signOff.signedByEmail`, `signedAt` and `note`, and an **Undo**.
- `signOff.stale: true`: the line changed since it was signed. Show the old sign-off with its `differences` (what was accepted then) and offer **Sign off again**. The line counts as outstanding.
- `signOff.moot: true`: the line now matches; nothing to show.

### Versions

Upload as many documents as the supplier sent; they are checked together. To correct one, POST the new file with `replacesId` (section 2): the new document gets `version = old + 1`, the old one gets `supersededById` and leaves the check but stays on file (`superseded[]`). Deleting a replacement puts the old version back. `comparison.duplicateInvoices` flags an invoice number that is on file twice (its rows count twice) — suggest deleting the newer copy and re-uploading it as a replacement.

### Sign-offs

| Route | Body / result |
|---|---|
| `POST /api/v1/packing-lists/sign-offs` | `{ containerNumber \| draftContainerId, supplierKey, lineKey, note? }` → `201 { signOff, created, comparison, signOffs }` (200 with `created: false` if already signed and unchanged). 404 `LINE_NOT_FOUND`, 409 `LINE_MATCHES`, 409 `NO_CHECK`. Re-signing a changed line retires the old sign-off. |
| `DELETE /api/v1/packing-lists/sign-offs/:id` | Take it back. 204. |

Sign-offs belong to the container + supplier + `lineKey`, not to a document: they survive a re-read and a replaced packing list, and follow a draft into its booked container. They hold only while the line's `fingerprint` (status + non-info differences) is unchanged.

### Approval

| Route | Body / result |
|---|---|
| `POST /api/v1/packing-lists/approvals` | `{ containerNumber \| draftContainerId, note? }` → `201 { approval }`. Allowed whatever the checks say; before sending, show what is still open (`overall.outstanding`, `suppliersWithoutList`) and ask for a note. 409 `ALREADY_APPROVED` while one is active. |
| `POST /api/v1/packing-lists/approvals/:id/withdraw` | `{ note? }` → `{ approval }`. |
| `GET /api/v1/packing-lists/approvals` | `{ data: [ …active approvals… ] }`, for badges across containers. |

`status.approval` carries `stale` and `changesSinceApproval[]` (`line_changed`, `line_added`, `line_removed`, `documents_changed`, `supplier_added`, `supplier_removed`, each with `supplierName` and, for lines, `jfCode` / `poNumbers`), plus `outstandingAtApproval` and `suppliersWithoutListAtApproval`. A later sign-off is not a change. Show a stale approval in amber with the list of changes; the remedy is withdraw, review, approve again.

## Other routes

| Route | Purpose |
|---|---|
| `GET /api/v1/packing-lists?containerNumber=328[&draftContainerId=129][&supplierKey=s:11]` | List, newest first. `containerNumber` matches the booked reference, a draft's reserved number, or its name. |
| `POST /api/v1/packing-lists/:id/analyze` | Read the PDF again. Optional body `{ "model": "gemini-3.1-pro-preview" }`. Returns 202; then poll. |
| `GET /api/v1/packing-lists/:id/download[?disposition=attachment]` | Returns `{ url }`, a presigned link valid for 5 minutes. Audited. |
| `DELETE /api/v1/packing-lists/:id` | Soft delete. Returns 204. Deleting a replacement puts the version it replaced back. |
