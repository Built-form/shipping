# Artwork confirmation by the supplier, per PO line

Artwork is confirmed per PO line. Staff used to chase the supplier by email and type a date in ShipLine. Now:

1. Staff send the **"Artwork Confirmed"** email from a PO in ShipLine (ARTWORK → REQUEST CONFIRMATION). It carries a "Confirm artwork" button whose link holds a secure token.
2. The link opens a page on the supplier portal with **no sign-in**: the PO's items, a tick box each. The supplier ticks every item whose final artwork they have confirmed.
3. A signed-in portal user gets the same ticks on the order's page (the **Artwork** tab).
4. A tick is applied **automatically**: the line's `artwork_confirmed_by_supplier` becomes the day of the tick. A line still at `PO_SENT` also moves to `IN_PRODUCTION`, but **only when the supplier requires no deposit or the deposit has been paid**. ShipLine shows it on the next orders refresh.

Code: `src/lib/artwork-confirmations.js` (pure rules) and `src/lib/payment-terms.js` (reading deposit terms), tested by `tools/test-artwork-confirmations-lib.js` and `tools/test-payment-terms-lib.js` in `npm run test:unit`; `src/services/artwork-confirmation-routes.js` (staff routes, registered from `src/handlers/orders.js`); `src/services/deposit-gate.js` (the deposit lookups); `src/handlers/artwork-confirmed.js` (the apply Lambda). Portal side: `portal/src/handlers/artworkConfirmations.js`, `portal/src/lib/artworkConfirmations.js`, `portal/web/src/screens/ArtworkScreen.tsx`, `portal/web/src/components/ArtworkChecklist.tsx` (portal README section 19). ShipLine: `PoArtworkPanel.tsx`, `ArtworkRequestModal.tsx`, `artworkConfirmation.ts`.

## Decisions

- **A field of its own.** The supplier's confirmation is `orders.artwork_confirmed_by_supplier` (DATE, the UTC day of the tick; `2026-11-11_05_orders_artwork_confirmed_by_supplier.sql`), returned on every order as `artworkConfirmedBySupplier`. `artwork_confirmed_date` stays staff's own date. The supplier's tick never reads or writes it; **sending the request email sets it** (staff emailing the approved artwork is staff confirming it): today, UK time, on each line the email covers that has no date yet (see the email route below). So the payment rules' "deposit after artwork is confirmed" trigger and the gate on a manual move into production still follow the staff date, not the supplier's tick.
- **The move into production is gated on the deposit** (below). When the gate holds, the confirmation is still recorded and the line stays at `PO_SENT`; nothing moves it later by itself (the supplier's "deposit received" tick on the portal does, through `jportal-deposit-received-<env>`, as before).
- When it does move, the move skips ShipLine's gate for `IN_PRODUCTION` (staff Artwork Confirmed date, lot number, MFG and EXP dates), knowingly.
- **A tick is one-way for the supplier.** There is no untick route. Staff take a confirmation off a line with CLEAR in ShipLine's ARTWORK panel, which makes the line tickable again.
- **Staff can set it by hand.** The order form in ShipLine has an "Artwork Confirmed by Supplier" date beside "Artwork Confirmed" (`artworkConfirmedBySupplier` in `UPDATABLE_FIELDS`, so `PUT /orders/:id` takes it and the audit log records it under the staff user), for a supplier who confirmed by email or phone. Setting it by hand never moves the status; clearing it there makes the line tickable again, as CLEAR does.
- **Opening the link confirms nothing.** The page's read never writes, and the tick is a `PUT` that needs the portal's CSRF header, so a mail scanner that follows the link (Safe Links, Mimecast…) cannot confirm anything.
- **No "I've read it" receipt button** on this email (and so no automatic reminders): the artwork button is the acknowledgment.

## The deposit gate

`depositGate` in `src/lib/artwork-confirmations.js`, fed by `loadDepositGate` in `src/services/deposit-gate.js`. Worked out per PO when a tick is applied.

| deposit % in force | deposit paid | a `PO_SENT` line | `reason` |
|---|---|---|---|
| 0 | – | moves to `IN_PRODUCTION` | `no_deposit` |
| any, or unknown | yes | moves to `IN_PRODUCTION` | `deposit_paid` |
| above 0 | no | stays | `deposit_unpaid` |
| unknown | no | stays | `terms_unknown` |

Unknown terms hold the move on purpose: we cannot say the supplier requires no deposit. A gate that cannot be read (a missing table) holds it too. The audit note on the line says why it did not move.

**The deposit % in force** is what ShipLine's Payments flow page shows: the company's payment rule for the supplier (`payment_rules.deposit_pct`, the supplier's own rule, else the default rule), else the terms of the PO's newest proforma invoice (`purchase_order_invoice_payments.due_terms`, `raw_terms_text`, `deposit_percentage`), else the supplier's JFPRO payment terms (`suppliers.default_payment_terms`, e.g. `30D/70B BOL`, `100B BOL`).

Until now only ShipLine read free-text terms. `src/lib/payment-terms.js` is a **port of ShipLine's parser** (`Shipline/src/components/payments/paymentsFlowMath.ts`: `parsePaymentTerms`, `resolveTermsRule`, and `resolvePolicy`'s deposit %), checked against the original on 45 inputs when it was written. **Change both together**, or the server and the Payments page will read the same terms differently.

**The deposit counts as paid** when a supplier payment was applied to it (a `po_deposit` line on the PO, or a `pi` line on a deposit / full PI of the PO), or such a PI is marked `paid`, or the supplier ticked "deposit received" on the portal (`po_deposit_receipts`). The first three are the rule the portal's orders list uses.

## Tables (this schema)

Both live beside the PO, as `po_deposit_receipts` does, so every reader is on this schema whatever server the portal runs on. `src/db/migrate/2026-11-11_10_artwork_confirmations.sql`.

**`artwork_confirmation_requests`**: one row per email.

| columns | written by |
|---|---|
| `token` (48 hex, unique), `purchase_order_id`, `po_number`, `supplier_name`, `order_ids` (JSON: the lines asked about), `sent_to`, `subject`, `created_by_email`, `expires_at` (60 days) | shipping, on send |
| `front_message_uid`, `front_conversation_id`, `revoked_at`, `revoke_reason` (`staff` \| `send_failed`) | shipping |

The portal only reads it, and never `sent_to`, `created_by_email` or the Front ids. The token is never written to the audit log.

**`artwork_confirmations`**: one row per tick.

| columns | written by |
|---|---|
| `order_id`, `purchase_order_id`, `request_id` (link ticks), `source` (`link` \| `portal`), `supplier_id`, `user_id`, `confirmed_by_email` (signed-in ticks), `confirmed_by_name` (the name typed on the link page, optional), `confirmed_on` (UTC date) | the portal (INSERT only) |
| `applied_at` | shipping |
| `pending_order_id` | generated: `order_id` while `applied_at IS NULL`. Unique, so at most one tick per line waits to be applied; the portal inserts with `INSERT IGNORE`. |

### Grants for the portal's login

`2026-11-11_11_grant_artwork_confirmations_to_jportal.js` applies them on deploy where `jportal_app` exists; the portal's `db/grants/jportal_app.sql` carries the same statements:

```sql
GRANT SELECT (id, token, purchase_order_id, po_number, supplier_name, order_ids, created_at,
              expires_at, revoked_at, revoke_reason)
             ON jfa.artwork_confirmation_requests TO 'jportal_app'@'%';
GRANT SELECT ON jfa.artwork_confirmations TO 'jportal_app'@'%';
GRANT INSERT (order_id, purchase_order_id, request_id, source, supplier_id, user_id,
              confirmed_by_email, confirmed_by_name, confirmed_on)
             ON jfa.artwork_confirmations TO 'jportal_app'@'%';
GRANT SELECT (artwork_confirmed_by_supplier) ON jfa.orders TO 'jportal_app'@'%';
```

## Rules

- **Tickable line**: live, `artwork_confirmed_by_supplier` NULL, no tick waiting, status `PO_SENT`, `IN_PRODUCTION`, `READY_FOR_QC` or `READY`. Staff's `artwork_confirmed_date` plays no part.
- **`_FQC` sample lines are ignored** (`isFqcLine`: `jf_code` or `asin` ends in `_FQC`, as `isFqcOrder`). They have no artwork of their own: never tickable, never in the email, not in the panel's `lines` or counts, and dropped from the `orderIds` of a request that named one before this rule. ShipLine's ART tick leaves them out too, so a PO goes green without them. The one thing that reaches them: a send also sets `artwork_confirmed_date` on the `_FQC` sample of each line in that email, where it has none (`fqcSamplesOf`: `JF0787` in the email dates `JF0787_FQC`; matched field for field on `jf_code` or `asin`, ignoring case; they are in `datedOrderIds`). A sample whose product is not in the email is left alone. The supplier's tick is not copied to them. This filtering is artwork's alone: nothing else about `_FQC` lines changes.
- **No quantities** in the supplier's email: the items are listed by code and name only (the portal's pages show none either).
- **Request state**, one precedence in shipping, the portal and ShipLine: `revoked` > `expired` > `done` > `open`. `done` = none of its lines can be ticked any more. Only `open` takes ticks. Expiry is decided on the database clock.
- Requests are independent: a new one does not close earlier ones. Staff close a link with REVOKE; ticks already saved stay.
- A Front failure on send revokes the request (`send_failed`), so no dead link is left open.

## The email

Template `artwork_confirmation` (name "Artwork Confirmed", category `Artwork`, seeded by `2026-11-11_20_seed_artwork_confirmation_template.js`, editable in ShipLine's templates). Tokens `{poNumber}`, `{supplier}`, `{lines}` (an HTML list of the items asked about). Under the body the sender appends a fixed bilingual block (`artworkBlockHtml`): the items, the button to `${SUPPLIER_PORTAL_URL}/artwork/<token>`, the address as text, a line saying a confirmation cannot be undone, and the day the link stops working. The whole is wrapped in the portal emails' frame (`wrapEmail`) and sent through Front (`sendFrontMessage`).

## Routes

Authed through the greedy `/api/v1/{proxy+}` route. Reads are open to every staff user; writes need an editor role (`403 READ_ONLY` otherwise). Errors are `{ error, code, field? }`.

### `GET /api/v1/purchase-orders/:id/artwork-confirmation`

```ts
{
  enabled: boolean,              // SUPPLIER_PORTAL_URL is set and the tables exist
  deposit: {                     // the gate as it stands now; null when it could not be read
    depositPct: number | null, depositPaid: boolean, clear: boolean,
    reason: 'no_deposit' | 'deposit_paid' | 'deposit_unpaid' | 'terms_unknown',
  } | null,
  lines: {
    orderId: number, jfCode: string | null, productName: string | null, quantity: number,
    status: string | null,
    artworkConfirmedBySupplier: string | null,   // the supplier's confirmation
    artworkConfirmedDate: string | null,         // staff's own date, for reference only
    tickable: boolean,
    confirmation: {              // the newest tick for the line, or null
      confirmedOn: string | null, source: 'link' | 'portal', by: string | null,
      requestId: number | null, confirmedAt: string | null,
      appliedAt: string | null,  // null while the tick is waiting to be applied
    } | null,
  }[],
  requests: {                    // newest first
    id: number, state: 'open' | 'done' | 'expired' | 'revoked', revokeReason: string | null,
    orderIds: number[], sentTo: string[], subject: string | null, sentAt: string | null,
    sentByEmail: string | null, expiresAt: string | null, revokedAt: string | null,
    url: string | null,          // the supplier's link, only while open
  }[],
}
```

### `POST /api/v1/purchase-orders/:id/artwork-confirmation/email`

Body `{ to, subject?, body?, orderIds? }`. `to` as the PO send takes it (`parseRecipients`). `orderIds` defaults to every tickable line; a named line must be tickable. Once Front has accepted the message, every line it covers whose `artwork_confirmed_date` is NULL gets today's date (Europe/London), with an `update` audit row on that order. A line that already has a date keeps it, and a failed send dates nothing. Answers `{ ok, requestId, sentTo, subject, datedOrderIds, artworkConfirmedDate, panel }` (`panel` = the GET above; `artworkConfirmedDate` is the date set, or null when no line needed one). Audits `artwork_confirmation_requested` on the PO, with `datedOrderIds`.

| status | code | when |
|---|---|---|
| 400 | `BAD_FIELD` (+ `field`) | `to`, `orderIds` |
| 403 | `READ_ONLY` | read-only user |
| 404 | `NOT_FOUND` | unknown PO |
| 409 | `PORTAL_URL_UNSET` | `SUPPLIER_PORTAL_URL` is blank |
| 409 | `NOTHING_TO_CONFIRM` | no line is tickable |
| 502 | `FRONT_ERROR` | Front rejected the message; the request is revoked |

### `POST /api/v1/purchase-orders/:id/artwork-confirmation/requests/:rid/revoke`

Closes the link (reason `staff`), audits `artwork_confirmation_revoked`, answers the panel. Revoking twice is a no-op.

### `POST /api/v1/purchase-orders/:id/artwork-confirmation/lines/:orderId/clear`

Takes the supplier's confirmation off a line: `artwork_confirmed_by_supplier` back to NULL, and any tick still waiting for that line dropped. The status is **not** moved back. One `audit_log` row on the order under the staff user. A line that is not confirmed is a no-op. Answers the panel. This is the only write to `orders` in these routes.

## Applying the ticks: `jportal-artwork-confirmed-<env>`

`src/handlers/artwork-confirmed.js`, invoked directly and asynchronously by the portal with `{ purchaseOrderId }` after its own commit, as `jportal-deposit-received-<env>` is. In one transaction it:

- reads the PO's confirmations with `applied_at IS NULL` and locks their live `orders` rows;
- reads the deposit gate, only when a line is still at `PO_SENT`;
- for a line the supplier had not confirmed: sets `artwork_confirmed_by_supplier = confirmed_on`, and `status = 'IN_PRODUCTION'` when it was `PO_SENT` and the gate is clear, with one `audit_log` row (`entity_type 'order'`, `action 'update'`, note "artwork confirmed by supplier (supplier portal)", plus "not moved to production: …" when the gate held) under `supplier-portal:<portal user's email>` or `supplier-portal:<the request's first recipient> (<name typed>)`;
- leaves a line already confirmed, or deleted;
- stamps `applied_at` on every confirmation handled, so a re-run is a no-op and a confirmation staff clear later is not written back.

The portal's invoke is fire-and-forget: a failed start is only logged there. The ticks then wait (ShipLine shows "NOT APPLIED YET" on the line in the ARTWORK panel) until the next tick on that PO applies them.

## Deploy order

1. Shipping: `bash deploy.sh <stage>` (the new `orders` column, the two tables, grants, template, the Lambda, the routes).
2. Portal: `bash deploy.sh <stage>` (routes, IAM, the WAF rules, the pages).
3. ShipLine.

The portal before shipping is harmless: its order page has no Artwork tab and a link answers "not recognised". On test, remember the database is rebuilt from prod every 3 days.

## Not proven without a deploy

- The column-level grants under the portal's real login (the unit tests use a fake pool).
- The deposit lookups in `src/services/deposit-gate.js` against real tables (tested on a fake connection only), and how often real suppliers come out as `terms_unknown`.
- The portal's `availability` Lambda invoking this Lambda from inside the VPC with the widened role.
- The WAF rules 8 and 9 with the added `/api/artwork/` path (an `OrStatement` inside the existing `AndStatement`).
