# AI Status Suggestions — approval queue (frontend contract)

A dedicated **"AI Suggestions"** view in ShipLine: a human-in-the-loop approval
queue for order **status changes inferred from supplier emails**.

An hourly Lambda (`frontStatusImport`) reads recent inbound supplier emails from
Front, asks Gemini whether each one announces a production/shipping milestone,
matches the extracted PO number to live orders, and queues a **PENDING
suggestion** per affected order. The AI **never** changes an order — a human
**Approves** (which applies the status change to the order + writes the audit
log) or **Denies** (dismisses it). Keeping these AI guesses in their own view —
not mixed into the deterministic daily-alerts slider — is deliberate, so an
operator's trust in the real alerts isn't eroded by AI noise.

Under the hood these are rows in the **same `daily_alerts` table** as the daily
alerts, with `type: "status_suggestion"`. So they reuse the alerts list endpoint
and the alert shape documented in [daily-alerts-frontend.md](daily-alerts-frontend.md) —
this doc only covers what's specific to suggestions. All routes are under the
existing authed API (Google JWT, same as `/api/v1/orders`); the current user's
email is taken from the token server-side for attribution.

## What the AI may suggest (it follows ShipLine's non-admin movement rules)

Every suggestion is a move a **non-admin operator could legitimately make** —
because the approve API applies whatever it's sent with no transition validation.
The backend mirrors ShipLine's `orderHelpers.ts` rules (see
`src/lib/order-transitions.js`):

- **Status values** use the live DB vocabulary: `SCHEDULED · PO_SENT ·
  IN_PRODUCTION · READY_FOR_QC · READY · CONSOLIDATED · ON_SEA · ON_AIR ·
  ARRIVED_AT_WAREHOUSE · RECEIVED · DESTROYED`. (The movement-rules doc labels two
  of these `UNDER_PRODUCTION` and `READY_AT_FACTORY`; those are display aliases for
  `IN_PRODUCTION` and `READY`.)
- **`suggestedStatus` is always exactly ONE valid step**: one stage forward, a
  lateral `ON_SEA`↔`ON_AIR`, or a permitted backward move (`READY_FOR_QC →
  IN_PRODUCTION` on QC-fail). If an email implies a *further* stage (e.g. "on the
  vessel" while the order is still `PO_SENT`), the suggestion proposes only the
  **next** valid step and lists the remaining path (`category: "multi_step"`).
- **FQC samples** (jf_code/asin ends `_FQC`) are never suggested past `READY`.
- Orders already received/terminal (`ARRIVED_AT_WAREHOUSE`, `RECEIVED`,
  `PARTIALLY_RECEIVED`, `DESTROYED`) are never candidates.

Each suggestion carries a **`category`**:

| `category` | meaning |
|---|---|
| `forward` | the email's milestone is exactly the next stage |
| `multi_step` | the email implies a further stage; only the next step is proposed (`remainingPath` lists the rest) |
| `lateral` | sea↔air at the same stage |
| `backward_qc_failed` | QC failed → back to `IN_PRODUCTION` |
| `data_update` | **no status change** — the email provides field data worth saving (e.g. an estimated ready date derived from "production takes ~1 month", or a refreshed ETA). `suggestedStatus` equals `currentStatus`; the value lives in `fieldUpdates`. |

The importer is **conservative** — it suppresses anything that: names no PO (or a
PO not in the system), asserts no milestone, scores below the confidence floor
(`STATUS_IMPORT_CONF_MIN`, default `0.6`), would be a no-op, or (for an FQC order)
would ship it.

## Suggestion shape

Same envelope as any alert (see the daily-alerts doc), with `type:
"status_suggestion"`, `severity: "info"`, `entityType: "order"`, `entityId` =
the order id (string). The AI payload lives in `meta`:

```jsonc
{
  "id": 4821,
  "type": "status_suggestion",
  "severity": "info",
  "status": "pending",                 // "pending" | "dismissed" (denied) | "resolved" (approved)
  "eventDate": "2026-06-24",
  "title": "Suggested: IN_PRODUCTION → READY — JF1372",
  "body": "Acme Mfg · PO PO12345 · AI 92% · \"goods are ready, please arrange inspection\"",
  "entityType": "order",
  "entityId": "5582",
  "meta": {
    "orderId": 5582,
    "poNumber": "PO12345",             // the order's PO as stored
    "extractedPo": "PO# 12345",        // the raw reference Gemini read from the email
    "currentStatus": "IN_PRODUCTION",  // order status when the suggestion was made
    "suggestedStatus": "READY",        // the ONE valid next step Approve will apply (unless overridden)
    "milestone": "READY",              // the stage the email actually evidenced
    "category": "forward",             // forward | multi_step | lateral | backward_qc_failed
    "remainingPath": [],               // for multi_step: stages still to traverse after this one
    "isFqc": false,                    // FQC sample (can't ship past READY)
    "hasQcReport": true,               // QC inspection report attached (READY hard-gate, §3a)
    "gate": {                          // §3 fields required to move INTO suggestedStatus
      "required": ["qcStatus","qcDate","qcInvoiceNumber"],
      "missing": []                    // of those, which are still absent (after fieldUpdates)
    },
    "fieldUpdates": [                  // values the email provided that Approve will also set
      { "field": "qcDate", "from": null, "to": "2026-06-20" },
      { "field": "qcInvoiceNumber", "from": null, "to": "QC-77" }
    ],
    "blockedFields": [],               // values NOT applied + why (e.g. expiry_in_past, lot_expiry_conflict)
    "confidence": 0.92,                // 0..1
    "rationale": "goods are ready, please arrange inspection",
    "model": "gemini-3-flash-preview",
    "jfCode": "JF1372",
    "productName": "...",
    "supplier": "Acme Mfg",
    "source": {
      "messageId": "msg_abc",
      "conversationId": "cnv_xyz",
      "fromEmail": "sales@acme-mfg.com",
      "subject": "Re: PO12345 production update",
      "frontUrl": "https://app.frontapp.com/open/cnv_xyz",  // deep-link to the email
      "receivedAt": "2026-06-24T08:11:00.000Z"
    },
    "sourceRef": "front:msg_abc"
  },
  "lastAction": null,                  // "approve" | "acknowledge" | "deny" once actioned
  "actionNote": null,                  // free-text note the operator left (esp. on acknowledge)
  "actionedBy": null,                  // who actioned it (email / username)
  "actionedAt": null,
  "createdAt": "2026-06-24T09:00:01.000Z"
}
```

> One PO can map to several order lines. Each affected line gets its **own**
> suggestion, approved/denied independently — so a single email can produce
> several cards sharing a `poNumber` but different `orderId` / `entityId`.

## Endpoints

### `GET /api/v1/alerts?type=status_suggestion&status=pending` — the queue
Reuses the standard alerts list (see daily-alerts doc). Pass
`type=status_suggestion`; `status` is `pending` (default) / `dismissed` /
`resolved` / `all`; `limit` 1–1000.

Response (200): `{ "data": [ /* suggestion[] */ ], "counts": { "returned": N, "pending": N } }`.
Use `counts.pending` for the nav badge. Ordered oldest-event-first.

`GET /api/v1/alerts/history` also surfaces approved/denied suggestions (filter the
returned rows by `type === "status_suggestion"`), with `lastAction`
(`approve`/`deny`) + `actionedBy` / `actionedAt`.

### `PATCH /api/v1/alerts/:id/approve` — apply the status change (+ fields)
Applies the status change **and** the email-provided field values
(`meta.fieldUpdates`) to the order in one transaction (stamps the status date key,
writes an `audit_log` row, fires the PO-sent webhook if relevant), then resolves
the suggestion.

- Body: **none required.** Applies `meta.suggestedStatus` + `meta.fieldUpdates`.
- Optional overrides:
  - `{ "status": "ON_SEA" }` — apply a corrected status instead of the AI's.
  - `{ "fields": { "qcDate": "2026-06-20", "qcInvoiceNumber": "QC-77" } }` — apply
    your own field values instead of `meta.fieldUpdates` (camelCase → value).
- Field values are **re-validated at apply time** (the order may have changed):
  mfg/exp snap to the 1st of the month, expiry can't be in the past, and a JF+lot
  can't take a second expiry. Rejected fields are **skipped** (the status move
  still applies) and reported in `skippedFields`.
- There is **no** transition validation on the status itself — the human approver
  is the control. Show `currentStatus → suggestedStatus`, the `fieldUpdates`, and
  any `gate.missing` on the card so they can decide / fill gaps.

Response (200):
```jsonc
{
  "order": { /* full updated order */ },
  "alert": { /* resolved suggestion */ },
  "appliedStatus": "READY",
  "appliedFields": [ { "field": "qcDate", "value": "2026-06-20" } ],
  "skippedFields": [ { "field": "expDate", "reason": "lot_expiry_conflict" } ]
}
```
Errors: `400` invalid id / invalid override status · `404` suggestion or order
not found · `409` `{ "error": "Suggestion already actioned.", "alert": {…} }`
(someone already approved/denied it) **or** `{ "error": "Order already RECEIVED;
suggestion is stale.", "alert": {…} }` (the order reached a received/warehouse
state since the suggestion was made, so it's no longer applicable) — in both
cases drop the card and refresh · `422` malformed suggestion.

### `PATCH /api/v1/alerts/:id/deny` — reject the suggestion
Dismisses it permanently (it won't re-surface for the same suggested change).
Body (optional): `{ "note": "why it was wrong" }`. Response (200): the updated
suggestion (`status: "dismissed"`). Errors: `400` invalid id · `404` not found ·
`409` already actioned.

### `PATCH /api/v1/alerts/:id/acknowledge` — handled it my own way
For when the operator has **seen** the suggestion and dealt with it themselves
(updated the order manually, contacted the supplier, etc.) and wants it off the
queue **without** applying the AI's status change. Does **not** touch the order.
- Body: `{ "note": "what you did instead" }` — optional but the point of this
  action; prompt for it in the UI. Stored as `actionNote`.
Response (200): the updated suggestion (`status: "resolved"`, `lastAction:
"acknowledge"`, `actionNote`, `actionedBy`). Errors: `400` invalid id · `404` not
found · `409` already actioned.

> The generic `/snooze` · `/dismiss` · `/restore` actions still work on these rows
> too, but **Approve / Acknowledge / Deny are the intended buttons** for the AI
> Suggestions view. All three (and the note) appear in
> `GET /api/v1/alerts/history` with `lastAction`, `actionNote`, and `actionedBy`
> (who did it) — so there's a full audit trail of how each suggestion was resolved.

## Suggested view behaviour
- Poll `GET /api/v1/alerts?type=status_suggestion&status=pending` on open /
  interval; badge with `counts.pending`.
- Render each card as **`currentStatus → suggestedStatus`**, the SKU / product,
  PO, supplier, the **confidence %**, and the **rationale quote** — the rationale
  is what lets the operator judge the AI rather than blindly trust it. Link the
  card to `meta.source.frontUrl` so they can open the original email in Front.
- Two primary actions: **Approve** (→ `/approve`) and **Deny** (→ `/deny`).
  Optionally expose an inline status picker before Approve to override
  `suggestedStatus`. Optimistically remove the card on action; on `409`, refresh
  (it was actioned elsewhere).
- An optional "History" tab → `GET /api/v1/alerts/history`, filtered to
  `type === "status_suggestion"`, showing who approved/denied what.

## Schedule & cost control
`frontStatusImport` runs **hourly** and scans **incrementally** — only since the
last successful run (minus a few hours' overlap, capped at 7 days), tracked in
`front_status_import_state` — so it doesn't re-walk Front history every hour. It's
idempotent — each Front message is processed once (deduped on
`front_status_imports.source_ref`), so empty runs are cheap. A keyword pre-filter
and a per-run Gemini cap (`STATUS_IMPORT_MAX_GEMINI`, default 40) bound the AI
spend; if a run hits the cap it leaves the watermark put and the overflow is
picked up next hour. The senders it scans are derived from the `suppliers` /
`supplier_emails` tables **plus a built-in freight-forwarder domain list**
(`dcglogistics.com`, `savinodelbene.com`), optionally extended with
`STATUS_IMPORT_DOMAINS` (comma-separated domains and/or addresses). Forwarder
emails often cite a **container/booking number** instead of a PO, so the matcher
falls back to `external_container_number`/`container_number`; and a forwarder
**ETA-only update** (no milestone) still produces a `data_update` for the orders
in that container.

Manual / test run: `node src/handlers/front-status-import.js` won't auto-execute
(it only exports `handler`); invoke the Lambda with an event like
`{ "sinceDays": 7, "dryRun": true }` to preview without writing alerts.
