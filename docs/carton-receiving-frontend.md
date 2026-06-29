# Carton-scan receiving — frontend guide

A small **public** API (no Google login) for receiving stock straight from a
carton scan. A scan gives you a **jf_code (SKU)**, a **lot/batch number**, and
optionally an **expiry date**. The flow is:

1. **Look up** the matching order(s) from the scanned fields.
2. The operator **picks an order** and enters the **quantity received**.
3. **Receive** that quantity into Mintsoft. The scanned **lot number overwrites**
   whatever was on the order and is sent to Mintsoft as the batch. The **expiry
   date sent to Mintsoft is taken from the order** (`exp_date`) — the order is the
   source of truth for expiry, so the scanned date isn't used for the receive.

Every endpoint is gated by a single shared **six-digit code** — there is no JWT.

> Base path: `/api/v1/scan`. These routes live on a separate Lambda from the
> authed admin API; only the routes below are exposed.

---

## Authentication — the `X-Quick-Auth` header

Send the six-digit code on **every** request as a header:

```
X-Quick-Auth: 472913
```

The code is case-insensitive and ignores spaces/dashes (it's matched
normalised), so `4729 13` and `472913` are the same. (For convenience you may
instead put it in the JSON body as `"code": "472913"`, but the header is
preferred and works for the GET routes too.)

- **`401 { "error": "Invalid or missing access code." }`** — wrong/absent code.
  A single generic message for every failure; don't surface anything more.
- **`503 { "error": "Receiving is not configured." }`** — the server has no code
  configured. Treat as "feature off".

---

## 1. Location picker

The receive call needs a Mintsoft `warehouseId` + `locationId`. Fetch them from
the synced cache (same data the admin app uses):

### `GET /api/v1/scan/warehouses`
```json
{ "data": [
  { "warehouseId": 1, "name": "Main", "code": "MAIN", "active": true,
    "locations": [ { "locationId": 10, "name": "A1", "locationName": "Aisle 1", "pickSequence": 1 } ] }
] }
```

### `GET /api/v1/scan/warehouses/:warehouseId/locations`
```json
{ "data": [ { "locationId": 10, "warehouseId": 1, "name": "A1", "locationName": "Aisle 1", "pickSequence": 1 } ] }
```

Cache these on the device; the warehouse a scanner serves rarely changes.

---

## 2. Look up the order — `POST /api/v1/scan/lookup`

**Body**
```json
{ "jfCode": "JF1372", "lotNumber": "1372002", "expiryDate": "2027-05-01" }
```
- `jfCode` — **required** (the scanned SKU).
- `lotNumber` — optional but normally present (the carton's lot).
- `expiryDate` — optional, `YYYY-MM-DD`.

**Response `200`**
```json
{ "data": { "candidates": [
  {
    "orderId": 8123,
    "poNumber": "PO-3007",
    "jfCode": "JF1372",
    "productName": "Garment bag, 60×100cm",
    "supplier": "NINGBO TAPE INDUSTRIAL CO., LTD",
    "status": "ARRIVED_AT_WAREHOUSE",
    "quantity": 5000,
    "receivedQuantity": 1000,
    "outstandingQuantity": 4000,
    "lotNumber": "1372002",
    "expDate": "2027-05-01",
    "lotMatch": "exact",
    "expiryMatch": "exact"
  }
] } }
```

Candidates are **every receivable order** whose jf_code matches the scan,
**ranked best-first**. To be a candidate an order must be live, have outstanding
quantity > 0, **and have arrived at the warehouse** — status
`ARRIVED_AT_WAREHOUSE` (this also covers an order that's been part-received but
still has stock outstanding). Orders still in transit/production (`ON_SEA`,
`ON_AIR`, `CONSOLIDATED`, `IN_PRODUCTION`, …) are never returned, even if their
SKU matches the scan. More than one can come back — that's expected; let the
operator pick. An empty `candidates` array means nothing matching that SKU has
arrived yet.

**Rendering the match quality**

- `lotMatch`:
  - `exact` — the order's lot equals the scanned lot.
  - `normalised` — equal ignoring case/spaces/dashes and **leading zeros**
    (`00124` == `124`). Safe to treat as a match; show a subtle "≈" if you like.
  - `unset` — the order has no lot yet (common — the lot is on the carton). This
    is a normal receive target.
  - `none` — the order has a *different* lot. Show a warning before selecting.
- `expiryMatch`: `exact` | `none` when you sent `expiryDate`, else `null`.

Rank order is: lot `exact` → `normalised` → `unset` → `none`, with an expiry
match breaking ties, then most-outstanding first. Default-select the first
candidate; if `outstandingQuantity` is the obvious cap on the quantity input.

Errors: `400 { "error": "jfCode is required." }`,
`400 { "error": "expiryDate is not a valid date." }`.

---

## 3. Receive into Mintsoft — `POST /api/v1/scan/receive`

**Body**
```json
{
  "orderId": 8123,
  "quantity": 480,
  "warehouseId": 1,
  "locationId": 10,
  "lotNumber": "1372002",
  "expiryDate": "2027-05-01",
  "goodsInType": 3,
  "idempotencyKey": "carton-7f3a1c-2026-06-22",
  "operator": "Sam"
}
```
- `orderId`, `quantity`, `warehouseId`, `locationId` — **required**.
- `lotNumber` — the lot **on the carton**. It overwrites the order's `lot_number`
  and is sent to Mintsoft as `BatchNo`. If omitted, the order's existing lot is
  used.
- `expiryDate` — accepted for compatibility but **ignored on receive**. The
  **order's `exp_date` is what's sent to Mintsoft** as `ExpiryDate` (the order is
  the source of truth for expiry). The order **must have an `exp_date` set** — and
  a lot must resolve from the carton or the order — or you get `400` (see below).
- `goodsInType` — optional integer `0–6` (default `3` = Carton).
- `idempotencyKey` — **send one per carton-confirm action** (any stable string,
  ≤64 chars). A retry with the same key returns the prior result instead of
  receiving twice (`"idempotent": true` in the response). This is your guard
  against a double-tap / flaky connection double-submitting.
- `operator` — optional; recorded in the audit trail as `carton-scan:<operator>`.

**Response `200`**
```json
{
  "order": { "id": 8123, "status": "ARRIVED_AT_WAREHOUSE", "receivedQuantity": 1480, "outstandingQuantity": 3520, "lotNumber": "1372002", "expDate": "2027-05-01", "...": "..." },
  "asnId": 99001,
  "receipts": [ { "id": 55, "type": "received", "quantity": 480, "batchNo": "1372002", "expiryDate": "2027-05-01", "asnId": 99001, "...": "..." } ]
}
```
On an idempotent replay the body also carries `"idempotent": true`.

The order's `status` only flips to `RECEIVED` (or `PARTIALLY_RECEIVED` if there
were recorded shortfalls) once the running receipts settle the full ordered
quantity; partial receives leave the status as-is. Re-fetch via a fresh
`/lookup` if you want the updated `outstandingQuantity` for the next carton.

**Errors** (all `{ "error": "...", ... }`):

| Status | When | Extra fields |
|---|---|---|
| `400` | missing/invalid `orderId`, `quantity`, `locationId`, `warehouseId`, `goodsInType`, or `idempotencyKey` | — |
| `400` | order has no `exp_date` set, or no lot resolves (order + scanned both empty) | `missing: ["lot_number","exp_date"]` |
| `400` | `quantity` exceeds the order's remaining | — |
| `400` | order has no jf_code | — |
| `404` | order not found | — |
| `404` | no Mintsoft product with that exact SKU | `candidates: [...]` |
| `409` | order already fully received | `order` |
| `502` | Mintsoft request failed — safe to retry | `details` |

---

## 4. Gemini key for on-device OCR — `GET /api/v1/scan/gemini-key`

For reading the carton label (jf_code / lot / expiry) with image recognition,
the app can fetch the Gemini API key:

```json
{ "apiKey": "AIza..." }
```

Gated by the same `X-Quick-Auth` code. `503 { "error": "Gemini is not
configured." }` if no key is set. **Treat the returned key as a secret** — keep
it in memory, don't log it or persist it to disk; re-fetch when needed.

---

## Suggested flow

1. On launch: prompt for the 6-digit code once, store it in memory, send it on
   every request. Fetch warehouses/locations and let the operator pick a
   location (sticky).
2. Scan → (optionally OCR the label with the Gemini key) → `POST /scan/lookup`.
3. Show candidates ranked; default-select the top one; warn on `lotMatch:"none"`.
4. Operator confirms order + quantity → `POST /scan/receive` with a freshly
   generated per-carton `idempotencyKey`.
5. Show the updated `outstandingQuantity`; loop to the next carton.
