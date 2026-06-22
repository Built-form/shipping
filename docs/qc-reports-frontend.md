# QC Reports API — frontend guide

Upload a supplier **QC / inspection report** (PDF), store it in S3, run Gemini
extraction, and attach the report to every order whose `(jf_code, lot_number)`
the report inspected — each with a **pass / fail** verdict.

All paths are under the same base URL and auth as the rest of the API:
send the Google JWT as `Authorization: Bearer <token>` on every request.

## Why upload is a 4-step dance (read this first)

Real reports are 9–40 MB scans. Two AWS limits force the shape:

- **HTTP API caps the request body at 6 MB** → you can't POST the file to us as
  base64. Instead we hand you a short-lived **presigned S3 URL** and you `PUT`
  the bytes straight to S3.
- **HTTP API caps a request at 30 s**, but Gemini on a 40 MB scan takes 1–2 min
  → analysis runs in the **background**. You start it, get `202`, then **poll**.

```
1. POST /api/v1/qc-reports            -> { id, uploadUrl, uploadHeaders }
2. PUT  <uploadUrl>  (browser -> S3)  -> 200   (send the file bytes)
3. POST /api/v1/qc-reports/:id/analyze-> 202   (kicks off Gemini)
4. GET  /api/v1/qc-reports/:id  (poll)-> status: processing -> succeeded
```

## Enums

| Field | Values |
|---|---|
| report `status` | `awaiting_upload` → `processing` → `succeeded` \| `failed` |
| `qaStatus` (per item) | `pass` \| `fail` \| `unknown` (`unknown` = report gave no clear verdict, e.g. "Pending") |
| `matchMethod` | `exact` \| `normalised` |

---

## 1. `POST /api/v1/qc-reports`

Create the report record and get a presigned upload URL.

**Request**
```json
{ "filename": "sunmed-psi-2026-06.pdf", "contentType": "application/pdf" }
```
`contentType` is optional (defaults to `application/pdf`).

**Response `201`**
```json
{
  "id": 12,
  "filename": "sunmed-psi-2026-06.pdf",
  "status": "awaiting_upload",
  "url": "https://<bucket>.s3.eu-north-1.amazonaws.com/qc-reports/<uuid>/sunmed-psi-2026-06.pdf",
  "uploadUrl": "https://<bucket>.s3.eu-north-1.amazonaws.com/qc-reports/<uuid>/...&X-Amz-Signature=...",
  "s3Key": "qc-reports/<uuid>/sunmed-psi-2026-06.pdf",
  "uploadMethod": "PUT",
  "uploadHeaders": { "Content-Type": "application/pdf" },
  "expiresInSeconds": 600
}
```
Errors: `400` (filename missing), `500` (bucket not configured).

## 2. `PUT <uploadUrl>` — browser → S3 (not our API)

Upload the raw file bytes directly to S3.

```js
await fetch(resp.uploadUrl, {
  method: "PUT",
  headers: resp.uploadHeaders,   // MUST send exactly this Content-Type
  body: file,                    // the File/Blob
});
```
⚠️ **Gotchas**
- The `Content-Type` header must match `uploadHeaders` exactly, or S3 returns
  `403 SignatureDoesNotMatch`.
- Do **not** add `Authorization` — the signature is in the URL.
- The URL expires after `expiresInSeconds` (10 min). If it lapses, call step 1
  again for a fresh URL (same flow).

## 3. `POST /api/v1/qc-reports/:id/analyze`

Confirms the file landed in S3, then runs Gemini + order-matching in the
background. Returns immediately.

**Request** `{}` (optional `{ "model": "gemini-3.1-pro-preview" }` to override).

**Response `202`** — the report row with `status: "processing"`.

Errors: `404` (unknown id), `409` (file not in S3 yet — finish step 2 first).
Safe to call again to re-run; a re-run **replaces** that report's own links.

## 4. `GET /api/v1/qc-reports/:id` — poll for the result

Poll every ~3–5 s until `status` is `succeeded` or `failed`.

**Response `200`**
```json
{
  "id": 12,
  "filename": "sunmed-psi-2026-06.pdf",
  "status": "succeeded",
  "documentUrl": "https://<bucket>.s3...amazonaws.com/qc-reports/<uuid>/sunmed-psi-2026-06.pdf",
  "supplier": "Suzhou Sunmed Co.,Ltd",
  "reportTitle": "Pre-shipment Inspection Report",
  "inspectionDate": "Jun 01~02, 2026",
  "modelUsed": "gemini-3.1-pro-preview",
  "itemCount": 26,
  "matchedCount": 15,
  "fileSize": 40289405,
  "error": null,
  "createdAt": "2026-06-15T10:00:00.000Z",
  "analyzedAt": "2026-06-15T10:02:13.000Z",

  "orders": [
    {
      "orderId": 826,
      "jfCode": "JF0168",
      "lotNumber": "0168008",
      "qaStatus": "fail",
      "detail": "Shipping marks failed (instruction manual mismatch)",
      "matchMethod": "exact",
      "productName": "PLCS-WH2",
      "poNumber": "PO-1234",
      "orderStatus": "PO_SENT",
      "documentUrl": "https://<bucket>.s3...amazonaws.com/qc-reports/<uuid>/sunmed-psi-2026-06.pdf"
    }
  ],

  "unmatched": [
    {
      "jfCode": "JF0134",
      "lotNumber": "0134006",
      "qaStatus": "fail",
      "detail": "Packing check failed (carton weight)",
      "nearMiss": {
        "byJfCode": [],
        "byLot": [ { "orderId": 968, "jfCode": "HW0134", "lotNumber": "0134006", "status": "PO_SENT" } ]
      }
    }
  ]
}
```

- **`orders[]`** = the inspected items that matched an order ("attached"). Each
  carries the `documentUrl` (same PDF for all) and its `qaStatus`.
- **`unmatched[]`** = inspected items with no order at that exact jf_code+lot.
  `nearMiss.byLot` / `nearMiss.byJfCode` hint at close rows (e.g. the same lot
  under code `HW0134`, order 968) so the user can resolve manually.
- If `status` is `failed`, read `error` for the reason.

---

## `GET /api/v1/qc-reports` — list

```json
{ "data": [ { "id": 12, "filename": "...", "status": "succeeded",
             "supplier": "...", "reportTitle": "...", "inspectionDate": "...",
             "itemCount": 26, "matchedCount": 15,
             "createdAt": "...", "analyzedAt": "..." } ] }
```
Newest first, max 200. No per-order detail (use `GET /:id` for that).

## `DELETE /api/v1/qc-reports/:id` — soft-delete

Hides the report and drops it from the order-history view. Response
`{ "id": 12, "deleted": true }`; `404` if unknown.

## `GET /api/v1/orders/:id/qc-reports` — an order's QC history

Every report attached to one order, **newest analysis first**. A second report
covering the same item does **not** overwrite the first — both appear here, so
treat the top row as the current verdict.

```json
{ "data": [
  { "reportId": 12, "qaStatus": "fail", "detail": "...", "matchMethod": "exact",
    "jfCode": "JF0170", "lotNumber": "0170011",
    "documentUrl": "https://<bucket>.s3...amazonaws.com/...",
    "filename": "...", "supplier": "...", "reportTitle": "...",
    "inspectionDate": "...", "status": "succeeded", "analyzedAt": "..." }
] }
```

---

## Suggested UI flow

1. **Upload button** → file picker → POST (1) → PUT (2) → POST analyze (3) →
   show a spinner.
2. **Poll** GET (4); when `succeeded`, render a results table: matched
   `orders[]` (orderId · product · jf/lot · ✅/❌ `qaStatus` · detail · link to
   `documentUrl`), and an "unmatched" section using the `nearMiss` hints.
3. On an **order detail page**, call `GET /orders/:id/qc-reports` to show its QC
   history with a link to each report PDF.

> Note: the S3 bucket CORS now allows browser `PUT` (added with this feature) —
> it takes effect once this branch is deployed.

---

## Downloading / previewing a report — use the **audited** route

Every report object now carries a **`downloadPath`** (on the list, on `GET /:id`,
on each entry in `orders[]`, and on `GET /orders/:id/qc-reports`):

```json
"downloadPath": "/api/v1/qc-reports/38/download"
```

**Stop linking `url` / `documentUrl` directly.** Those are the raw public S3
links — opening them bypasses the backend, so we have **no record of who viewed
the file**. Use `downloadPath` instead: it writes an audit row (who + when +
preview-vs-download) and then hands back a short-lived presigned S3 URL.

### How to call it

`downloadPath` is **not** a plain link you can put in an `href`. It sits behind
the same JWT authorizer as every other API call, so a bare browser navigation
(no `Authorization` header) gets a 401 and logs nothing. You must `fetch` it
**with the Bearer token**, then open the `url` it returns:

```js
// Preview in a new tab:
async function openQcReport(downloadPath, token, { download = false } = {}) {
  const sep = downloadPath.includes('?') ? '&' : '?';
  const path = download ? `${downloadPath}${sep}disposition=attachment` : downloadPath;
  const { url } = await fetch(path, {
    headers: { Authorization: `Bearer ${token}` },
  }).then(r => r.json());
  window.open(url, '_blank');        // bytes stream S3 → browser directly
}
```

**Response `200`**
```json
{ "id": 38, "url": "https://…s3…/qc-reports/…?X-Amz-Signature=…",
  "disposition": "inline", "expiresInSeconds": 300 }
```

- `?disposition=inline` (default) → opens in-browser (preview).
- `?disposition=attachment` → forces a download with the original filename.
- The `url` is valid for **5 minutes** — fetch it at click time, don't cache it.

### Who-viewed audit (admin view)

```
GET /api/v1/audit-log?entityType=qc_report&entityId=38&action=qc_report_downloaded
```
returns one row per view/download: `userEmail`, `createdAt`, and
`after.disposition` (`inline` = previewed, `attachment` = downloaded).
