# Daily Alerts — slide-out alert window (frontend contract)

Backend for the Shipsline "daily alerts" slider. A nightly Lambda aggregates the
day's key shipping events into a `daily_alerts` table; the authed API exposes
them for a slide-out window where users snooze / dismiss items as they review.

All routes are under the existing authed API (Google JWT, same as `/api/v1/orders`).
The current user's email is taken from the token server-side — the frontend sends
nothing extra for attribution.

## Alert types (`type`)

| `type` | fires when | resolves when |
|---|---|---|
| `container_eta` | a container/order ETA is today or overdue and still in transit | received / arrived |
| `supplier_ready_date` | an order's ready date (actual ?? estimated) is today/overdue while still in a factory stage | leaves factory stage / ready date pushed forward |
| `missing_data` | an in-transit order is missing LOT / MFG / EXP (blocks Mintsoft receiving) | the missing fields are filled, or it ships past |
| `arrived_unreceived` | stock is `ARRIVED_AT_WAREHOUSE` with units still not booked into Mintsoft | fully received |
| `qc_pending` | an order sits in `READY_FOR_QC` without a QC pass | QC approved / order moves on |
| `shipment_late` | an in-transit order's delivery slot has passed, or its carrier ETA slipped ≥ 2 days | arrives / received |

All six share the lifecycle, shape, and endpoints below. `meta` is type-specific
(it always carries `orderId`/`jfCode`/`supplier` plus fields relevant to the type —
e.g. `missing: ["LOT","EXP"]`, `outstanding`, `readyForQcDate`, `etaSlipDays`).

## Lifecycle (important)

Each logical condition (a container arriving, an order's ready date) maps to **one
alert** with a stable, date-free key. The nightly job upserts every condition that
is currently true, refreshing its ETA / ready-date in place (no duplicates when an
ETA shifts). A user can:

- **Snooze N days** — hides it, then it **re-surfaces** after N days **if the
  condition is still unresolved**. The slider offers `2 days` / `1 week`.
- **Dismiss** ("never remind") — permanent.
- **Restore** — undo a snooze/dismiss.

And alerts **auto-resolve**: when the underlying order is updated so the condition
no longer holds — it leaves its factory stage, the ETA clears / goods are received,
or the ready date is pushed into the future — the nightly job marks it `resolved`
and it stops nagging. No clicks needed. There is **no date cutoff**: a genuinely
stuck order keeps re-surfacing until its state actually changes.

`status` (derived): `pending` (active, in the slider) · `snoozed` · `dismissed`
· `resolved`. `severity`: `today` or `overdue` (carried over from a past day).

## Alert shape

```jsonc
{
  "id": 1234,
  "type": "container_eta",            // one of the 6 types in the table above
  "severity": "overdue",              // "today" | "overdue"
  "status": "pending",                // "pending" | "snoozed" | "dismissed" | "resolved"
  "eventDate": "2026-06-12",          // the ETA / ready date that triggered it
  "title": "Container MSKU1234567 — ETA 2026-06-12",
  "body": "Acme Mfg · 3 lines · 4200 units",
  "entityType": "container",          // "container" | "order"
  "entityId": "MSKU1234567",          // container number, or order id (string)
  "meta": { /* structured payload, see below */ },
  "snoozedUntil": null,               // ISO timestamp while snoozed
  "dismissedAt": null,                // ISO timestamp once dismissed
  "resolvedAt": null,                 // ISO timestamp once auto-resolved
  "lastAction": null,                 // "snooze" | "dismiss" | "restore"
  "actionedBy": null,                 // who last snoozed/dismissed/restored (email)
  "actionedAt": null,                 // when they did
  "createdAt": "2026-06-23T04:00:01.000Z"
}
```

`meta` for `container_eta`: `{ containerNumber, eta, suppliers[], poNumbers[], totalQuantity, orders[] }`
where each order is `{ id, jfCode, productName, supplier, quantity, status }`.

`meta` for `supplier_ready_date`: `{ orderId, jfCode, productName, supplier, poNumber,
purchaseOrderId, status, readyKind ("estimated"|"actual"), estimatedReadyDate,
actualReadyDate, quantity }`.

## Endpoints

### `GET /api/v1/alerts` — the slider list
Query params (all optional):
- `status` — `pending` (default), `snoozed`, `dismissed`, `resolved`, or `all`
- `type` — one of `container_eta` · `supplier_ready_date` · `missing_data` · `arrived_unreceived` · `qc_pending` · `shipment_late`
- `limit` — 1–1000 (default 200)

Response (200): `{ "data": [ /* Alert[] */ ], "counts": { "returned": 12, "pending": 12 } }`.
Default surface is `pending`, ordered oldest-event-first (most overdue at top). Use
`counts.pending` for the badge.

### `GET /api/v1/alerts/history` — past actions + who did them
Query params: `limit` (1–1000, default 200), `days` (optional). Returns alerts that
have been actioned or auto-resolved, newest first, each with `lastAction`,
`actionedBy` / `actionedAt` (and `resolvedAt`).

### `PATCH /api/v1/alerts/:id/snooze` — remind me later
Body: `{ "days": 2 }` (optional, default 2; e.g. `7` for a week). → 200 with the
updated Alert (`status: "snoozed"`).

### `PATCH /api/v1/alerts/:id/dismiss` — never remind
→ 200 with the updated Alert (`status: "dismissed"`).

### `PATCH /api/v1/alerts/:id/restore` — undo
→ 200 with the updated Alert (`status: "pending"`).

All three: `400` `{ "error": "A valid alert id is required." }`; `404`
`{ "error": "Alert <id> not found." }`.

## Suggested slider behaviour
- Poll `GET /api/v1/alerts` (pending) on open / interval; render `today` vs `overdue`
  distinctly (e.g. amber for overdue).
- Per row offer **Snooze 2 days · Snooze 1 week · Never remind** → the matching PATCH.
  Optimistically remove on action; on error, restore.
- A "History" tab → `GET /api/v1/alerts/history`, showing `lastAction` + `actionedBy`
  / `actionedAt` (and resolved items).
- Snoozed items quietly come back on their own; resolved/dismissed ones don't.

## Generation schedule
The `generateDailyAlerts` Lambda runs nightly at **04:00 UTC** (early-morning UK).
Idempotent — a manual re-run (`node src/handlers/generate-daily-alerts.js`) or retry
never duplicates, and it auto-resolves conditions that are no longer true.
