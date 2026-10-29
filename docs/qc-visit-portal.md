# QC visit slot confirmation: supplier portal contract

For the **jportal** team. The shipping backend now lets a factory confirm a QC visit slot, or propose a different date, from the supplier portal. This file is the whole contract. The backend side is live in `src/handlers/supplier-portal.js` (the public `supplierPortalApi` Lambda, the same one that serves `po-lookup` and `po-ready-date`).

## Background

ShipLine staff arrange QC visits (ref `QCV_00012`): a date (China time), an optional time window, a factory and an inspector. A visit needs a confirmation from **both** the factory and the inspector before it becomes `CONFIRMED`. Staff record confirmations by hand (email, WeChat, phone). These endpoints let the factory record its own, which lands as a confirmation with channel `portal`.

> **Availability links come first.** Before a date is fixed, staff now send the factory and the inspector a no-login link (`/availability/<token>` in the portal) where each ticks the days they could attend; see [`qc-availability.md`](qc-availability.md). The PO-code confirm flow on this page stays, and is the step after: once staff have picked a day from both answers, the factory confirms that one date here (or by email). When a confirm here completes the pair and the visit becomes `CONFIRMED`, the visit's open availability links are revoked with reason `confirmed`.

## Auth

Same as `po-lookup`: no login. The supplier types a **PO number** and their **portal code**. The PO names its supplier and the code must match that supplier's `portal_code`. The check is case-insensitive and ignores spaces and dashes, and it is constant-time.

- The field is called `portalCode`. `accessCode`, the name the existing routes use, is accepted as an alias, so the form state can be shared.
- Every call re-sends `poNumber` + `portalCode`. There is no session.
- Bad PO or bad code returns **401** `{ error: 'PO not found, or the access code is incorrect.' }`. It is the same message as `po-lookup`, and it never says which part was wrong.
- Both routes are throttled at API Gateway (5 req/s, burst 10, global per route). Over the limit the client gets **429** from the gateway; ask the user to retry in a moment.
- A body that is not valid JSON gets **400** `{ error: 'Invalid request body.', code: 'BAD_BODY' }` on both routes, and a body over the size limit gets **413** `{ error, code: 'TOO_LARGE' }`. Both are JSON, never an HTML error page.

## `POST /api/v1/portal/qc-visit-lookup`

Lists the visits the factory can answer for this PO.

Request:

```json
{ "poNumber": "PO_00306", "portalCode": "ABC-123" }
```

Response **200**:

```json
{
  "data": [
    {
      "ref": "QCV_00012",
      "status": "PROPOSED",
      "visitDate": "2026-11-12",
      "windowStart": "09:30",
      "windowEnd": "12:00",
      "factoryLabel": "Main plant",
      "factoryCity": "Dongguan",
      "poNumbers": ["PO_00306", "PO_00311"],
      "factoryConfirmedAt": null
    }
  ]
}
```

- A visit is listed when it is `PROPOSED` or `CONFIRMED`, it is hosted by this PO's supplier, and at least one of its lines belongs to this PO. Drafts, completed and cancelled visits never appear.
- The list is ordered by date. An empty list (`data: []`) is normal: it means no visit is waiting on this PO.
- `visitDate` is `YYYY-MM-DD` and the window is `HH:MM`. Both are **China time (Asia/Shanghai)**. Either window end can be `null`, which means no time was set.
- `poNumbers` lists this supplier's POs on the visit. Another supplier's PO numbers, for goods moved in for the visit, are never shown.
- `factoryConfirmedAt` (ISO UTC, or `null`) is set while the factory's latest answer is a confirmation. Use it to show "You confirmed on …".

## `POST /api/v1/portal/qc-visit`

Records the factory's answer for one visit.

Request:

```json
{
  "poNumber": "PO_00306",
  "portalCode": "ABC-123",
  "visitRef": "QCV_00012",
  "action": "confirm",
  "visitDate": "2026-11-12",
  "note": "Goods will be packed by then"
}
```

```json
{
  "poNumber": "PO_00306",
  "portalCode": "ABC-123",
  "visitRef": "QCV_00012",
  "action": "counter",
  "counterDate": "2026-11-15",
  "counterWindowStart": "10:00",
  "counterWindowEnd": "16:00",
  "note": "Packing finishes on the 14th"
}
```

| field | required | rules |
|---|---|---|
| `visitRef` | yes | the `ref` from the lookup (case-insensitive). 1 to 16 characters from `A–Z a–z 0–9 _ -` after trimming; anything else is 400 `BAD_FIELD` before any lookup |
| `action` | yes | `confirm` or `counter` |
| `counterDate` | `counter` only | `YYYY-MM-DD`, China time, today or later and at most 366 days ahead ("counterDate is too far ahead.") |
| `counterWindowStart`, `counterWindowEnd` | no | `HH:MM`; end after start when both are given |
| `note` | no | up to 500 characters |
| `visitDate` | no, **recommended** | the date the supplier was shown. If staff have moved the visit to another date since the lookup, the call is refused with 409 `STALE`, so the factory never confirms a date it did not see |

Response **200**: `{ "ok": true, "status": "CONFIRMED" }`. `status` is the visit's status after the call.

What happens:

- **confirm** records a factory confirmation. If the inspector has already confirmed, a `PROPOSED` visit becomes `CONFIRMED`; otherwise it stays `PROPOSED`, waiting on the inspector. Confirming again when the factory has already confirmed is a no-op and still returns 200, so double-submits are safe.
- **counter** records the proposed date and window and clears the factory's confirmation. A `CONFIRMED` visit drops back to `PROPOSED`. Staff see the counter-proposal in ShipLine and either accept it, which moves the visit and asks both parties again, or reply by hand.
- The staff audit trail shows the change as `supplier-portal:<supplier name>`.

Errors:

| status | body | when | UI |
|---|---|---|---|
| 400 | `{ error, code: 'BAD_FIELD', field }` | bad or missing field (see the table) | show `error` next to `field` |
| 400 | `{ error: 'Invalid request body.', code: 'BAD_BODY' }` | the body is not valid JSON | a bug in the portal; generic error |
| 401 | `{ error }` | bad PO number or portal code | back to the code form |
| 404 | `{ error: 'QC visit not found.', code: 'NOT_FOUND' }` | no such ref, the visit is not on this PO, it is hosted by another supplier, or it is no longer `PROPOSED`/`CONFIRMED` (cancelled, completed, or pulled back to draft) | "This visit is no longer open. Please reload." and re-run the lookup |
| 409 | `{ error, code: 'STALE' }` | `visitDate` was sent and no longer matches | re-run the lookup and show the new date |
| 429 | `{ error: 'Too many answers for this visit today. Please contact us directly.', code: 'TOO_MANY' }` | this visit already has 10 portal answers (confirm or counter) from the factory in the last 24 hours (rolling). A repeat confirm that is a no-op does not count | show `error`; the factory should contact staff directly |
| 429 | (API Gateway) | throttled | "Too many requests, try again in a moment" |
| 500 | `{ error: 'An internal error occurred.' }` | server fault | generic retry |

404 is used for every visit mismatch on purpose: the endpoint never confirms that a visit exists.

## What the portal UI should show

A "QC visits" panel on the PO page, after the existing PO lookup:

1. Call `qc-visit-lookup` with the PO number and code the supplier already entered. If `data` is empty, hide the panel.
2. One card per visit: the ref, the date as a weekday plus date (for example "Thu 12 Nov 2026"), the window as "09:30–12:00 (China time)" or "time to be agreed" when it is null, the factory label and city, and the PO numbers. Show a status line:
   - `CONFIRMED`: "Confirmed". If `factoryConfirmedAt` is set, add "you confirmed on <date>".
   - `PROPOSED` with `factoryConfirmedAt` set: "You confirmed, waiting for the inspector".
   - `PROPOSED` without it: "Please confirm this date".
3. Two buttons:
   - **Confirm this date** calls `action: 'confirm'` and sends `visitDate` from the card.
   - **Propose another date** opens a small form (date, optional from/to time, note). The date picker must not allow past dates in China time. On submit it calls `action: 'counter'`.
   - On a card the factory has already confirmed, hide Confirm and keep Propose another date.
4. After any 200, re-run the lookup to refresh the cards.
5. Label every time "China time". The portal never converts times.

## Out of scope

The factory cannot decline outright (use `counter` with a note), change the inspector, or see inspector details. Staff are not emailed when a portal answer lands; it shows up on the visit and in its activity log in ShipLine.
