# Order emails — frontend guide

Every order can now show **the Front email threads about it**. A scheduled job
(`frontEmailIndex`, hourly) walks recent Front conversations, finds the ones that
mention an order's identifiers — **PO number, container / AWB number, SKU
(jf_code), ASIN, lot** — and writes a link row per (thread, order). The frontend
just reads those links and renders them as an **"Emails" tab/section** on the
order.

> Base path: `/api/v1/orders/:id/emails` (authed admin API — send the Google
> JWT like every other order route).

---

## `GET /api/v1/orders/:id/emails`

Returns every email thread linked to the order, best matches first.

Optional query params:
- `?tier=strong` — only the high-confidence links (PO / container / AWB). Use
  this if you want to hide the looser SKU-only matches.
- `?includeBody=1` — also return the **full stored email body** (`body`) for each
  thread. Omitted by default so the list stays light; request it when rendering.
- `?conversationId=cnv_x` — return just that one thread (pair with `includeBody=1`
  to lazy-load a single body on demand instead of fetching every body up front).

```json
{ "data": [
  {
    "conversationId": "cnv_1n1ebjq8",
    "subject": "2*40 HC // MK1003809 // TAM HANGERS // TRHU5800520 - FFAU6158059",
    "preview": "Can we switch two containers delivery time around? …",
    "fromEmail": "sibel.dyonmez@savinodelbene.com",
    "participants": ["purchasing01@jfamedical.co.uk", "sibel.dyonmez@savinodelbene.com"],
    "direction": "inbound",
    "frontUrl": "https://app.frontapp.com/open/cnv_1n1ebjq8",
    "tier": "strong",
    "matchBasis": [
      { "type": "container", "value": "FFAU6158059" },
      { "type": "po", "value": "TAM-123" }
    ],
    "source": "rule",
    "confidence": null,
    "messageCount": 6,
    "firstMessageAt": "2026-05-20T11:11:00.000Z",
    "lastMessageAt": "2026-06-26T08:57:00.000Z"
  }
] }
```

### Fields

| field | meaning |
|---|---|
| `conversationId` | Front conversation id. |
| `subject` / `preview` | Thread subject + a short text snippet. |
| `fromEmail` / `participants` | Sender of the latest message + everyone on the thread. |
| `direction` | `inbound` or `outbound` (the latest message). |
| `frontUrl` | Deep link — **open the thread in Front** (open in a new tab). |
| `tier` | **`strong`** (PO/container/AWB), **`batch`** (jf_code + lot), **`product`** (SKU/ASIN only). |
| `matchBasis` | Why it linked — `[{ type, value }]`. `type` ∈ `po, container, awb, jf_code, asin, lot, supplier, product`. Render as little chips. |
| `source` | **`rule`** (exact identifier) or **`gemini`** (Flash read it from prose). |
| `confidence` | `0–1` for `gemini` links; `null` for `rule` links (rule = certain). |
| `messageCount` | Messages in the thread. |
| `firstMessageAt` / `lastMessageAt` | ISO timestamps. |
| `body` | **Only present with `?includeBody=1`.** The full thread text — every message, oldest→newest, each prefixed with a `[IN]/[OUT] sender timestamp` header. Plain text (HTML stripped). |

Results are already ordered **strong → batch → product**, and newest-first within
each tier. An empty `data` array just means nothing's been linked yet.

---

## Suggested UI

- An **"Emails" tab** (or a card under the order) listing each thread: subject,
  sender, last-message date, a tier badge, and the `matchBasis` chips.
- Render the body inline (request `?includeBody=1`, or lazy-load per thread with
  `?conversationId=…&includeBody=1`), and/or link out to `frontUrl` to open the
  thread in Front (which also handles replying).
- Show a small **count badge** on the tab (`data.length`).
- Consider a **"strong only" toggle** that re-fetches with `?tier=strong` — handy
  for SKUs that have lots of historical orders.
- For `source: "gemini"` rows you may want a subtle "AI-matched" marker; for
  `tier: "product"` a "SKU match" hint, since those are the loosest links.

## What it does / doesn't do

- **Read-only.** The frontend never writes links; the job owns the index.
- Links are **append-only facts** — "this thread referenced this order". A link is
  never auto-removed, even if the order later changes its container.
- A thread can link to **several orders** (e.g. a container holding multiple SKUs)
  and an order can have **many threads**.
- Coverage grows over time: the hourly job indexes new/updated mail. A one-off
  historical backfill is run server-side (`tools/index-emails.js --since 180`).
- **The index only ever goes back six months** — both the live job and any manual
  backfill are hard-capped at 180 days, so older threads are never linked.
