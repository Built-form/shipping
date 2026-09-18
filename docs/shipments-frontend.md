# Shipments — frontend brief

A **shipment** is now a real row: one physical movement of goods (a sea
container, an air consignment, a truck) with a manifest of order lines and one
lifecycle from planning to receipt. Until now a "container" existed only as a
value repeated on every order travelling in it (`containerNumber`), drafts and
planned containers were separate name-keyed tables, and booking was two
non-atomic calls. This document is the API contract ShipLine can move onto, and
what already changed underneath it.

**Nothing in ShipLine has to change yet.** Every existing route and response key
is untouched; the new routes write through to the legacy tables, so the current
Draft / Planned / Containers views keep working and keep seeing everything the
new routes do.

## The model

| field | meaning |
|---|---|
| `stage` | `PLANNED` → `DRAFT` → `BOOKED` → `IN_TRANSIT` → `ARRIVED` → `CLOSED`, or `CANCELLED` |
| `mode` | `SEA` · `AIR` · `ROAD` (`null` = unknown, flagged `needsReview`) |
| `reference` | our internal number, exactly what orders carry as `containerNumber`: `308` (sea), `121. Air Freight` (air). Assigned at booking. Anything else (`UPS`, `test`) is kept verbatim and flagged `needsReview` |
| `trackingRef` | the carrier's reference: ISO container number, AWB, courier number. The only link to the ShipsGo `containers` / `air_shipments` data |
| `name` | human label; for a draft or planned shipment it IS the legacy draft / planned name |

**Effective stage.** The `stage` the API returns is the later of the stage stored
on the shipment (written by `book`, `transition` and draft closes) and the stage
its member orders justify (all received / destroyed → `CLOSED`; any arrived →
`ARRIVED`; any `ON_SEA` / `ON_AIR` → `IN_TRANSIT`). So a shipment follows its
orders when they move through the old routes or the hourly ShipsGo sync, but a
ROAD shipment marked in transit is never pulled back by its `CONSOLIDATED` orders.
`storedStage` and `derivedStage` are returned too.

**Booked shipments read from their orders.** For `BOOKED` and later, `etd`,
`eta`, `vesselName` and `trackingRef` come from the member orders (the ShipsGo
sync keeps those current), falling back to the stored values.

**Drafts and planned containers are shipments.** A draft is a `DRAFT` shipment
whose lines are its allocations; a planned container is a `PLANNED` one. Reusing
a converted draft's name opens a new shipment (a new "generation"); the old one
keeps its number and history.

**Orders.** Every order now carries `shipmentId` (the booked shipment it travels
in, `null` otherwise), and `GET /orders` adds a `shipments` side-map, keyed by
id, next to `purchaseOrders`:

```json
{ "data": [ { "id": 629, "containerNumber": "310", "shipmentId": 42, "…": "…" } ],
  "purchaseOrders": { "…": "…" },
  "shipments": { "42": { "id": 42, "reference": "310", "name": "DRAFT-SEA-260807-161657 - 310 ETD 18 Aug",
                         "mode": "SEA", "stage": "BOOKED", "trackingRef": "SEKU4613920",
                         "vesselName": null, "etd": null, "eta": "2026-10-02", "originPort": null,
                         "needsReview": false } } }
```

## Endpoints

All under `/api/v1`, same auth as everything else. Errors are
`{ error, code, …details }`. A name can contain `/` and spaces, so names are
never in the path: look one up with `?name=`.

### Shipment object

```json
{ "id": 42, "reference": "310", "referenceSeq": 310, "name": "…", "mode": "SEA", "modeSource": "reference",
  "stage": "IN_TRANSIT", "storedStage": "BOOKED", "derivedStage": "IN_TRANSIT",
  "trackingRef": "SEKU4613920", "bookingRef": null, "blNumber": null, "forwarder": null,
  "vesselName": "…", "originPort": "Ningbo", "etd": "2026-09-20", "eta": "2026-10-20", "ata": null,
  "notes": null, "needsReview": false, "reviewNote": null, "origin": "backfill",
  "sourceDraftId": 71, "mergedIntoId": null, "memberCount": 8, "lineCount": 8, "totalUnits": 28048,
  "createdByEmail": "…", "createdAt": "…", "updatedAt": "…",
  "bookedAt": "…", "departedAt": null, "arrivedAt": null, "closedAt": null,
  "cancelledAt": null, "cancelledReason": null, "deletedAt": null }
```

`GET /shipments/:id` adds `lines[]` (`{ orderId, quantity, overAllocated, order: {…} }`),
`orders[]` (members, full order shape, booked shipments only), `mergedFrom[]` and
`tracking` (`{ provider: 'shipsgo', kind: 'container' | 'air', data }` or `null`,
the same shapes as `GET /containers/:n` / `GET /air-shipments/:awb`).

### `GET /shipments`

Query (all optional): `stage` (CSV, effective stage) · `mode` · `q` (substring of
reference, name, tracking ref, booking ref, B/L) · `reference` · `trackingRef` ·
`name` (exact) · `needsReview=true` · `include=lines` · `limit` (default 500, max
2000). Newest first. `200 { data: [shipment] }`

### `GET /shipments/next-reference?mode=SEA|AIR`

Advisory next number: one past the highest carried by a live order or reserved
by an open draft / planned name (`… - 328`). `200 { mode, seq, reference,
maxBooked, maxReserved, reservedBy[] }` · `422` for `ROAD` (no sequence) · `400`
bad mode.

### `POST /shipments` — new draft or planned shipment

```json
{ "mode": "SEA", "stage": "DRAFT", "name": "optional", "reference": "optional reserved number",
  "originPort": "Ningbo", "notes": "…", "lines": [{ "orderId": 629, "quantity": 500 }] }
```

Writes an ordinary legacy draft (registry row, allocations, their history
events) or planned container in the same transaction. No `name` → a
`DRAFT-SEA-yymmdd-hhmmss` stamp; `reference` is appended to the name
(`… - 329`), which is how a draft reserves its number. `201` the shipment (as
`GET /shipments/:id`) plus `warnings?` — over-allocating an order is a warning,
not an error (as today) ·
`409 NAME_IN_USE` / `REFERENCE_IN_USE` · `422 LINES_REQUIRED` (a planned
container is its lines) · `404 ORDER_NOT_FOUND`.

### `PATCH /shipments/:id`

Any of `name`, `mode`, `reference`, `trackingRef`, `vesselName`, `eta`, `etd`,
`ata`, `originPort`, `bookingRef`, `blNumber`, `forwarder`, `notes`,
`needsReview`, `reviewNote`.

- `name` on a DRAFT renames the legacy draft everywhere (allocations,
  documents, QA sheets) like `POST /draft-containers/rename`; on a PLANNED one it
  renames the planned rows; on a booked one it is only the label.
- On a booked shipment `reference`, `trackingRef`, `vesselName`, `eta` and `etd`
  are written to every member order (one audited change per order;
  `reference` → `containerNumber`, `etd` → `estimatedDepartureDate`, the tracking
  ref → `awbNumber` if it is AWB-shaped, else `externalContainerNumber`). When
  ShipsGo tracks the ref the response warns that its hourly sync overwrites
  eta / vessel.
- `reference` is assigned at booking: `422` on a draft.

`200` the shipment plus `warnings?` · `409 NAME_IN_USE` / `REFERENCE_IN_USE` /
`CONFLICT` · `422`.

### `DELETE /shipments/:id`

For PLANNED / DRAFT / CANCELLED, or a BOOKED shipment with no orders left. A
draft closes in legacy as `deleted`; a planned container's rows are removed.
Frees its name and number. `200 { ok, id }` · `409 HAS_MEMBERS` / `NOT_DELETABLE`.

### `PUT /shipments/:id/lines/:orderId` · `DELETE /shipments/:id/lines/:orderId`

DRAFT / PLANNED only (booked lines follow the orders). `PUT { quantity }`
upserts the line (and the legacy allocation, with its `line_added` /
`line_updated` event). `200 { line, lines, warnings? }` · `DELETE` →
`200 { ok }` (`shipmentCancelled: true` when a planned container lost its last
line) · `404 LINE_NOT_FOUND` · `409 NOT_OPEN`.

### `POST /shipments/:id/book` — replaces "Create Real" (pack + close)

```json
{ "reference": "optional", "trackingRef": "MSKU1234565", "vesselName": "…",
  "eta": "2026-10-20", "etd": "2026-09-20", "originPort": "Ningbo",
  "bookingRef": "…", "blNumber": "…", "forwarder": "…", "mode": "only if the draft has none",
  "packs": [{ "orderId": 629, "qty": 500 }] }
```

One transaction: packs the draft's lines (or the given subset of them) exactly
as `POST /containers/pack` does — a full line updates the order, a partial line
splits it — **plus** what Create Real used to write order by order afterwards:
the carrier ref (on the right column by its shape), the ETD
(`estimatedDepartureDate`) and the origin port (`port`); closes the legacy
draft as `converted`;
moves the shipment to `BOOKED`. The reference is the explicit one, else the
number the draft's name reserves if still free, else the next in the mode's
sequence (`ROAD` needs an explicit one). Either everything happens or nothing
does. `200` the shipment plus `booking: { reference, referenceSource
('explicit' | 'name_hint' | 'allocated'), packedOrderIds, draft }` · replay →
`200` the shipment plus `alreadyBooked: true` · `409 REFERENCE_IN_USE` /
`ORDER_IN_OTHER_SHIPMENT` / `ORDER_NOT_SHIPPABLE` / `SPLIT_BLOCKED_BY_RECEIPTS`
(a partial pack of an order with receipts refuses the whole booking) ·
`400 QTY_EXCEEDS` / `NOT_IN_DRAFT` · `422 REFERENCE_MODE_MISMATCH` /
`REFERENCE_REQUIRED` / `NOTHING_TO_BOOK`.

Topping up an existing container (packing into a number that is already
booked) stays on the legacy pack + close flow for now.

### `POST /shipments/:id/transition`

`{ "stage": "IN_TRANSIT" | "ARRIVED" | "CLOSED" | "CANCELLED", "note"?, "force"? }`

- Forward only (stages may be skipped), judged against the effective stage.
  Member orders move forward with it: `IN_TRANSIT` → `ON_SEA` / `ON_AIR` (ROAD:
  none yet, they stay `CONSOLIDATED`), `ARRIVED` → `ARRIVED_AT_WAREHOUSE`. An
  order that cannot move (not packed, already past, FQC) is skipped with a reason
  and never blocks the rest. `CLOSED` needs every order received / destroyed.
- `CANCELLED` only from PLANNED / DRAFT (same write-through as DELETE).
- Backward needs `force: true` and an admin; it moves only the stored stage.

`200 { moved: [{orderId, from, to}], skipped: [{orderId, status, reason}],
stage: { stored, effective }, shipment }` (a same-stage call is
`unchanged: true`) · `409 BACKWARD_TRANSITION` / `BEHIND_MEMBERS` /
`MEMBERS_NOT_TERMINAL` / `NOT_BOOKED` / `NOT_CANCELLABLE` · `403 ADMIN_ONLY` ·
`422 MODE_REQUIRED`.

### `GET /shipments/:id/documents` · `POST /shipments/:id/documents`

Quote / forwarder / supplier documents and QA sheets by shipment, across
renames and anything merged into it: `200 { data: [document], qaDocuments: [qa] }`
(same shapes as `GET /draft-containers/:name/documents` and
`GET /quality-assurance/documents`, plus `shipmentId`). `POST` (DRAFT only)
takes the body of `POST /draft-containers/:name/generate` and produces exactly
the same set (the per-supplier split and shared `batchId` included). Emailing
stays on `POST /draft-container-documents/:id/email`.

### `GET /shipments/:id/history`

The shipment's own events (`entity_type 'shipment'`: `create`, `booked`,
`stage_changed`, `merged`, `converted`, `update`, `line_set`, `line_removed`,
`cancelled`, `reopened`, `delete`) merged with the draft events of every draft
it came from (the `draft_container` vocabulary in
[draft-container-audit-frontend.md](draft-container-audit-frontend.md)).
`?limit=` (default 500). Same row shape as `GET /audit-log`.

## Suggested adoption in ShipLine

1. Read: show the shipment (stage, tracking) on order rows from
   `shipmentId` + the `shipments` side-map; a Shipments list on `GET /shipments`.
2. Draft view: create / rename / delete drafts and edit lines through
   `/shipments`; the Draft tab keeps working unchanged meanwhile.
3. "Create Real": call `POST /shipments/:id/book` instead of pack + close.
4. Container status: `POST /shipments/:id/transition` instead of
   `PATCH /containers/:cn/status`.

## Operations

- Schema: `src/db/migrations/2026-09-18_*.sql`, hand-applied before a deploy
  (`node tools/sql.js -f …`), one file per table.
- `node tools/backfill-shipments.js` — dry-run report; `--apply --confirm-host
  <DB_HOST> [--link-by-name-hint] [--exclude-hint-ids …]` seeds the shipments and
  arms the dual-write hooks (`shipments_backfill_v1` in `app_migrations`).
- `node tools/verify-shipments.js` — exit 0 = the shadow matches legacy;
  `--fix --apply --confirm-host <DB_HOST>` repairs drifted keys.
- Kill switch: `INSERT INTO app_migrations (name) VALUES ('shipments_shadow_off')`
  stops every hook within 60 s, no redeploy. Re-arm with
  `backfill-shipments.js --apply --rearm …`, wait 60 s, `--apply` again, verify.
- A hook never fails the request it shadows; a failure is one coalescing row in
  `shipment_sync_failures`.

## Tests

- `node --test tools/test-shipments-lib.js` — unit, no DB.
- `node tools/test-shipments-shadow-mysql.js` — `shadow()` on real MySQL (own
  in-process server, injected faults).
- Against `npm run dev:local` on the TEST database, with
  `TEST_BASE_URL=http://localhost:3031`: `test-shipments-dualwrite.js`,
  `test-shipments-api.js`, `test-shipments-book.js`,
  `test-shipments-transition.js`, `test-order-split.js`, and
  `test-orders-shape-frozen.js` (`capture` against the previous code, `compare`
  against the new, `audit`). They refuse to run unless the server reports a TEST
  database.
