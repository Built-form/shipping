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
{ "id": 42, "reference": "310", "referenceSeq": 310, "reservedReference": null, "reservedSeq": null,
  "name": "…", "mode": "SEA", "modeSource": "reference",
  "stage": "IN_TRANSIT", "storedStage": "BOOKED", "derivedStage": "IN_TRANSIT",
  "trackingRef": "SEKU4613920", "bookingRef": null, "blNumber": null, "forwarder": null,
  "vesselName": "…", "originPort": "Ningbo", "etd": "2026-09-20", "eta": "2026-10-20", "ata": null,
  "notes": null, "needsReview": false, "reviewNote": null, "origin": "backfill",
  "sourceDraftId": 71, "mergedIntoId": null, "memberCount": 8, "lineCount": 8, "totalUnits": 28048,
  "createdByEmail": "…", "createdAt": "…", "updatedAt": "…",
  "bookedAt": "…", "departedAt": null, "arrivedAt": null, "closedAt": null,
  "cancelledAt": null, "cancelledReason": null, "deletedAt": null }
```

`reservedReference` / `reservedSeq`: the number an open (DRAFT / PLANNED)
shipment's name holds (`… - 329` → `"329"`, on an AIR draft `"104. Air Freight"`),
by the rule booking applies: a hint of the other sequence holds nothing. `null`
from booking on.

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
by an open draft / planned name (`… - 328`), including a draft saved with no
lines. `200 { mode, seq, reference, maxBooked, maxReserved,
reservedBy[{ name, seq, shipmentId }] }` · `422` for `ROAD` (no sequence) · `400`
bad mode.

### `POST /shipments` — new draft or planned shipment

```json
{ "mode": "SEA", "stage": "DRAFT", "name": "optional", "reference": "optional reserved number",
  "reserve": false, "label": "optional, with reserve",
  "originPort": "Ningbo", "notes": "…", "etd": "2026-11-05", "eta": "2026-12-20", "vesselName": "…",
  "forwarder": "…", "bookingRef": "…", "blNumber": "…",
  "lines": [{ "orderId": 629, "quantity": 500 }] }
```

**`reserve: true`** (DRAFT, SEA / AIR, no `name` or `reference`) has the server
take the next number in the mode's sequence and name the draft
`<stamp> - <number> <label>`, one reserver at a time per mode, so two drafts
never hold the same number. This is how a new draft should be created: a draft
may have no lines, and it holds its number from that moment. `400
RESERVE_CONFLICT` (sent with a name, a reference or `stage: PLANNED`) ·
`422 NO_SEQUENCE` (ROAD) · `503 RESERVE_BUSY` (retry).

Writes an ordinary legacy draft (registry row, allocations, their history
events) or planned container in the same transaction. No `name` → a
`DRAFT-SEA-yymmdd-hhmmss` stamp; `reference` is appended to the name
(`… - 329`), which is how a draft reserves its number. `201` the shipment (as
`GET /shipments/:id`) plus `warnings?` — over-allocating an order is a warning,
not an error (as today) ·
`409 NAME_IN_USE` / `REFERENCE_IN_USE` (also when another open shipment's name
holds that number: `reservedBy`, `reservedByShipmentId`) · `422 LINES_REQUIRED`
(a planned container is its lines) · `404 ORDER_NOT_FOUND`.

### `PATCH /shipments/:id`

Any of `name`, `mode`, `reference`, `trackingRef`, `vesselName`, `eta`, `etd`,
`ata`, `originPort`, `bookingRef`, `blNumber`, `forwarder`, `notes`,
`needsReview`, `reviewNote`.

- `name` on a DRAFT renames the legacy draft everywhere (allocations,
  documents, QA sheets) like `POST /draft-containers/rename`; on a PLANNED one it
  renames the planned rows; on a booked one it is only the label.
- On a booked shipment `reference`, `trackingRef`, `vesselName`, `eta`, `etd`
  and `ata` are written to every member order (one audited change per order;
  `reference` → `containerNumber`, `etd` → `estimatedDepartureDate`, `ata` →
  `arrivedDate`, the tracking ref → `awbNumber` if it is AWB-shaped, else
  `externalContainerNumber`). When ShipsGo tracks the ref the response warns
  that its hourly sync overwrites eta / vessel, and replaces the arrival date
  once it reports an actual arrival.
- On a DRAFT or PLANNED shipment the same fields are stored on the shipment
  only. Booking carries them to the orders (see `book`).
- `reference` is assigned at booking: `422` on a draft.
- **Renaming a booked shipment** (`reference`) also moves every record that
  names the old number as text, in the same transaction: balance records,
  their invoice documents, extras, balance sign-offs and assignees (re-keyed),
  packing lists, their sign-offs and approvals, photos, the closed draft's
  registry row and the standing ETA alert. Amounts and statuses never change;
  each money record gets an audit row.
  - Records exist and the body has no `confirmRecords: true` →
    `409 RENAME_TOUCHES_RECORDS { from, reference, records }`, nothing changed.
    `records` counts what would move: `balances`, `paymentDocuments`, `extras`,
    `signOffs`, `assignees`, `packingLists`, `packingSignOffs`,
    `packingApprovals`, `photos`. Show them, then resend with the flag.
  - The new number already has records of its own →
    `409 REFERENCE_HAS_RECORDS { reference, records }`, with or without the flag.
  - Another open draft's name holds the new number → `409 REFERENCE_IN_USE`
    with `reservedBy`.
  - More than 64 characters → `400 BAD_REFERENCE`.
  - The `200` carries `recordsMoved`. Dismissed payment alerts are not moved
    (their key hashes the wording, so the alert comes back under the new
    number). Paperwork uploaded on a draft that only names the old number
    (never closed as converted into it) stays behind and is named in `warnings`.

`200` the shipment plus `warnings?` / `recordsMoved?` · `409 NAME_IN_USE` /
`REFERENCE_IN_USE` / `REFERENCE_HAS_RECORDS` / `RENAME_TOUCHES_RECORDS` /
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
moves the shipment to `BOOKED`. `eta`, `etd`, `vesselName` and `originPort`
fall back to what the draft already stores when the body leaves the key out
(an explicit `null` clears it), so dates set on a draft reach its orders. The
reference is the explicit one, else the
number the draft's name reserves if still free, else the next in the mode's
sequence (`ROAD` needs an explicit one). An explicit reference another open
draft's name holds is `409 REFERENCE_IN_USE` (`reservedBy`). Either everything happens or nothing
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

`{ "stage": "IN_TRANSIT" | "ARRIVED" | "CLOSED" | "CANCELLED", "note"?, "force"?, "etd"? }`

- Forward only (stages may be skipped), judged against the effective stage.
  Member orders move forward with it: `IN_TRANSIT` → `ON_SEA` / `ON_AIR` (ROAD:
  none yet, they stay `CONSOLIDATED`), `ARRIVED` → `ARRIVED_AT_WAREHOUSE`. An
  order that cannot move (not packed, already past, FQC) is skipped with a reason
  and never blocks the rest. `CLOSED` needs every order received, partially
  received (reconciled with a shortfall) or destroyed.
- `etd` (with `IN_TRANSIT` only, else `400 ETD_NOT_APPLICABLE`) is the sailing
  date: written to the shipment and to every member order in the same
  transaction as the move, moved or not (a ROAD shipment moves none). The
  response carries `etdApplied: true`. Send it here rather than as a PATCH
  first: a refused move then leaves no date behind.
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
`GET /quality-assurance/documents`, plus `shipmentId`). A draft's documents stay
with it through booking whatever number it is booked under: they are linked by
shipment id, not by name. On an open draft the list also returns documents made
under its name that were never linked (`shipmentId: null`); booking links them. `POST` (DRAFT only)
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

- Schema: `src/db/migrate/2026-09-21_01_shipments_tables.sql` and
  `2026-09-21_02_shipment_id_columns.sql`, applied by `deploy.sh` (through
  `tools/migrate.js`) before the code ships. The tables are inert until the
  backfill below arms them.
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
- **Not rebuildable any more:** header fields set on an open shipment (etd, eta,
  vessel, port, forwarder, booking ref, B/L, notes) and drafts saved with no
  lines live only in `shipments`. A drop-and-backfill loses them. Re-arm and
  `--fix` are safe; a full re-seed is not.
- The legacy `PATCH /containers/:cn/status` also takes `estimatedDepartureDate`
  (YYYY-MM-DD, `400` otherwise) and saves it with the status: the board sends it
  on a drag to On Sea / On Air.

## Tests

- `node --test tools/test-shipments-lib.js` — unit, no DB.
- `node tools/test-shipments-shadow-mysql.js` — `shadow()` on real MySQL (own
  in-process server, injected faults).
- Against `npm run dev:local` on the TEST database, with
  `TEST_BASE_URL=http://localhost:3031`: `test-shipments-dualwrite.js`,
  `test-shipments-api.js`, `test-shipments-book.js`,
  `test-shipments-transition.js`, `test-shipments-rename.js`, `test-order-split.js`, and
  `test-orders-shape-frozen.js` (`capture` against the previous code, `compare`
  against the new, `audit`). They refuse to run unless the server reports a TEST
  database.
