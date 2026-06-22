# Air (AWB) tracking — frontend feature brief

A description you can hand to an AI/dev to build the UI. The backend is already
done and deployed; this doc is the API contract + suggested UX. It mirrors the
existing **container (ocean) tracking** feature — if the app already shows
container tracking, build air tracking the same way.

## What we're adding

Air-freight orders (status `ON_AIR`) move by Air Waybill (AWB) instead of a sea
container. We now:

1. Let users enter an **AWB number** on an order.
2. Track that AWB with ShipsGo (same provider as container tracking).
3. Show live status, route (origin → destination airport), ETA, and the event
   timeline for the shipment.

The AWB is the air equivalent of the existing `externalContainerNumber` field.

## Data model

- Every order now has an `awbNumber` field (string, nullable). It's editable and
  returned everywhere orders are returned.
- ShipsGo tracking detail for each AWB lives server-side and is exposed via the
  `air-shipments` endpoints below. The backend also auto-copies the key dates
  (`eta`, `arrivedDate`, `shippedDate`) onto the order itself, so a basic UI can
  work from the order alone without calling the tracking endpoints.

## AWB format

- Pattern: 3-digit airline prefix + 8 digits, e.g. `235-98027403` (hyphen
  optional on input — ShipsGo normalises to `333-88888888`).
- Suggested client-side validation regex: `^\d{3}-?\d{8}$`.

## API reference

All endpoints are under the existing API base, require the same Google JWT auth
as the rest of the app, and return JSON.

### 1. Set / edit an order's AWB

The AWB is just another order field — use the existing create/update endpoints.

`POST /api/v1/orders` (create) or `PUT /api/v1/orders/:id` (update):

```json
{ "awbNumber": "235-98027403" }
```

The order in the response now includes:

```json
{ "id": 123, "status": "ON_AIR", "awbNumber": "235-98027403",
  "eta": "2026-06-02", "shippedDate": null, "arrivedDate": null }
```

`GET /api/v1/orders` and the per-ASIN breakdown (`goods_on_air` rows) also
include `awbNumber`.

> Tracking starts automatically — a scheduled job registers any AWB found on an
> order with ShipsGo (runs every 6h). There's no "start tracking" button.

### 2. List all tracked air shipments

`GET /api/v1/air-shipments` → `{ "data": [ AirShipment, ... ] }`
(sorted by ETA/arrival/departure desc). Returns `{ "data": [] }` if nothing is
tracked yet.

### 3. One air shipment by AWB

`GET /api/v1/air-shipments/:awbNumber` → a single `AirShipment` object, or
`404 { "error": "AWB … not tracked." }` if it hasn't been picked up yet.

### `AirShipment` response shape

```json
{
  "awbNumber": "235-98027403",
  "reference": null,
  "status": "EN_ROUTE",
  "airline": { "name": "TURKISH CARGO", "iata": "TK" },
  "origin":      { "name": "Istanbul Airport", "iata": "IST", "country": "TR" },
  "destination": { "name": "London Heathrow",  "iata": "LHR", "country": "GB" },
  "transshipments": [
    { "airport": "Frankfurt", "iata": "FRA", "country": "DE",
      "arrival": "2026-05-30 04:10:00", "arrival_is_actual": true,
      "departure": "2026-05-30 09:55:00", "departure_is_actual": false }
  ],
  "currentLocation": {
    "lat": 48.1, "lng": 11.6,
    "lastEvent": { "event": "DEP", "is_actual": true,  "timestamp": "2026-05-30 09:55:00",
                   "airport": { "iata": "FRA", "name": "Frankfurt", "country": "DE" }, "flight": "TK1635" },
    "nextEvent": { "event": "RCF", "is_actual": false, "timestamp": "2026-05-30 12:30:00",
                   "airport": { "iata": "LHR", "name": "London Heathrow", "country": "GB" }, "flight": "TK1979" }
  },
  "times": {
    "departure": "2026-05-29 22:15:00", "departureIsActual": true,
    "arrival":   "2026-05-30 12:30:00", "arrivalIsActual": false,
    "eta": "2026-05-30 12:30:00", "ata": null,
    "etaInitial": "2026-05-30 10:00:00",
    "totalTransitTime": 38, "transitPercentage": 60
  },
  "milestones": [
    { "event": "RCS", "is_actual": true,  "timestamp": "2026-05-29 14:00:00",
      "airport": { "iata": "IST", "name": "Istanbul Airport", "country": "TR" }, "flight": null },
    { "event": "DEP", "is_actual": true,  "timestamp": "2026-05-29 22:15:00",
      "airport": { "iata": "IST", "name": "Istanbul Airport", "country": "TR" }, "flight": "TK1979" },
    { "event": "RCF", "is_actual": false, "timestamp": "2026-05-30 12:30:00",
      "airport": { "iata": "LHR", "name": "London Heathrow", "country": "GB" }, "flight": "TK1979" }
  ],
  "tags": null,
  "routeGeojson": { "type": "FeatureCollection", "features": [] },
  "shipsgoId": "179254",
  "fetchedAt": "2026-05-30T06:00:00.000Z",
  "updatedAt": "2026-05-30T06:00:01.000Z"
}
```

### Status values (`status`)

| Status | Meaning | Suggested badge |
|---|---|---|
| `NEW` | Registered, ShipsGo hasn't checked it yet | grey "Pending" |
| `INPROGRESS` | ShipsGo checking, airline hasn't reported | grey "Pending" |
| `BOOKED` | Reserved with airline, not yet flown | blue "Booked" |
| `EN_ROUTE` | In transit | amber "In transit" |
| `LANDED` | Arrived at destination airport | green "Landed" |
| `DELIVERED` | Delivered to consignee | green "Delivered" |
| `UNTRACKED` | No tracking info — AWB may be invalid | red "Check AWB" |

### Milestone event codes (`milestones[].event`)

`RCS` Received from shipper · `MAN` Manifested · `DEP` Departed ·
`ARR` Arrived · `RCF` Received from flight · `DLV` Delivered.
`is_actual: true` = it happened (timestamp is actual); `false` = estimated.

## Suggested UX

1. **AWB field on the order** — an editable text input on air orders (status
   `ON_AIR`), next to where the container number shows for sea orders. Validate
   the format; save via `PUT /api/v1/orders/:id`.
2. **Status badge** — from `status` (table above). For `NEW`/`INPROGRESS` show a
   "Pending first check" hint; tracking detail fills in within minutes–hours.
3. **Route summary** — `origin.iata → destination.iata`, airline name, and ETA
   (`times.eta`, or `times.ata` once arrived). Show "departed"/"arrived" using
   the `*IsActual` flags.
4. **Tracking detail panel** (mirror the container one) — a milestone timeline
   from `milestones` (actual = solid, estimated = dashed), and optionally a map
   from `routeGeojson` + `currentLocation.lat/lng`.
5. **Empty / pending states** — a just-entered AWB returns `404` from
   `/air-shipments/:awb` until the next sync, and a tracked-but-unchecked one has
   `status: NEW` with empty route/milestones. Handle both gracefully.

## Notes for the implementer

- Tracking data refreshes server-side every ~6h; it is **not** live-on-request.
  A manual "refresh" button isn't wired up (could be added later).
- A `NEW` shipment legitimately has `airline` populated but empty
  route/dates/milestones — that's ShipsGo not having checked it yet, not an error.
- There's no airline column on the order itself; read airline name from the
  `air-shipments` detail (or the order's pushed `eta`/`shippedDate`/`arrivedDate`
  for a lightweight view).
