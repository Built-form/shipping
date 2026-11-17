# Sending a payment to its supplier

"Send to supplier" on a recorded payment (ShipLine: Payments > Payments made, and the prompt after Record payment). It replaces a manual step: one person recorded the payment and attached the proof, another downloaded the proof and emailed it to the supplier, asking for something in return.

Built 2026-11-16. Rules: `src/lib/payment-notices.js`. The send and its SQL: `src/services/payment-notices.js`. Routes: `src/services/payment-notice-routes.js`. Tests: `tools/test-payment-notices-lib.js`, `tools/test-payment-notices-send.js` (both in `npm run test:unit`).

## What goes out

A transfer (`supplier_payments` + its lines) is told to its supplier as one or two **notices**, one email each:

| Notice | Made of | Asks for |
|---|---|---|
| `deposit` | `po_deposit` lines; `pi` lines of type `deposit`; a 100% PI whose money was ticked against no container; extras riding `deposit` | the estimated ready dates, entered on the portal |
| `balance` | `balance` lines (top-ups included), `qc`, `repackaged`; `pi` lines of type `balance`; a 100% PI ticked against containers (`paidWith`); extras riding `balance` | the telex release |

- A transfer that paid both gets two emails. QC units ride in the balance email.
- The telex release the balance email asks for comes back through `docs/telex-releases.md`: uploaded by the supplier on the portal's Payments page, or filed by us from the payment.
- A credit (an extra riding `account`, negative) rides with the transfer's only notice, and with the balance when it has both.
- A forwarder's charge (an extra riding `shipment`) makes the whole transfer **not sendable**: its payee is not a supplier.

Each email goes out one of two ways (`delivery`):

- **`portal`**: a link to the supplier portal's Payments page (`/payments?payment=<id>`), nothing attached. Recipients are pre-approved on the portal for the transfer's supplier, as the PO email does. A deposit also carries a button to each order's page (`/orders/<po id>`), where the ready dates are entered.
- **`attached`**: the proof files ride on the email, with the "Confirm receipt" button. A deposit still carries a button to each order's ready dates where the portal can open the order (next section). Used when a link to the payment cannot work, and the dialog says which:

| `reason` | Meaning |
|---|---|
| `portal_url_unset` | `SUPPLIER_PORTAL_URL` is not configured |
| `portal_switch_off` | `PAYMENT_PROOF_PORTAL` is not `on` for this stage |
| `portal_unavailable` | the portal's `payment_views` table is not on this server |
| `no_supplier` | no live supplier is named **exactly** as the transfer's `supplier_name` |

`PAYMENT_PROOF_PORTAL` is off by default so that shipping can be deployed before the portal's page exists. Turn it on once the portal is live on that stage; turn it off to roll back.

### The ready dates are entered on the portal, never by reply

Decided by the user on 2026-11-17, on the first deposit email sent: its wording asked the supplier to "reply to this email with the estimated ready date of each item". No email asks for that any more. A deposit's ready dates are entered on each order's page of the portal (`/orders/<po id>`), whichever way the proof goes out.

- `readyDatesFor(notice, delivery)` (`src/lib/payment-notices.js`) is the one rule for the preview and both sends: one link per order of the deposit, **only** where `SUPPLIER_PORTAL_URL` is set and a live supplier is named exactly as the transfer. The switch and the `payment_views` table do not matter here: the order pages exist without the Payments page.
- A `portal` deposit carries the buttons under the portal block, as before.
- An `attached` deposit carries a second box under the "proof is attached" one: the ask, a button per order, and how to get an account.
- **An attached send approves nobody on the portal.** Its account lines are the ones for an address still to be approved ("register … and our team will approve your access"). An address new to the portal registers and waits for JFPRO → Portal users. Only a `portal` send pre-approves.
- Where the portal can open none of the orders (`portal_url_unset`, `no_supplier`) the email carries no button and its box says nothing of the portal. The stored wording still names the portal, so ShipLine's dialog warns the sender to reword it.
- A balance never carries them.

The stored wording: `payment_proof_deposit_attached` now reads "… and enter the estimated ready date of each item on our supplier portal." `2026-11-17_10_reword_payment_proof_deposit_attached.js` replaces the body on a server that still holds the seeded one, **exactly** as seeded. A template someone has edited is left as it is and named in the migration's log; reword it by hand in the template editor.

## What a supplier is told about a line

Names are compared exactly (`utf8mb4_0900_bin` in SQL, `===` in code): case, punctuation and trailing spaces all count.

- A line names its PO only when that **PO's own** supplier is exactly the transfer's supplier. This holds for an extra that points at a PO too: the extra's supplier does not vouch for the PO.
- A balance names its container only when the balance record is filed under exactly that supplier. An extra names its container, and says its kind, only when the extra is.
- A container is told by its number only (`supplierRef`), never by a draft's name.
- Otherwise the line is told as its amount alone. The portal reads "Other payment"; the email's `{paidFor}` leaves it out; the dialog counts such lines (`unnamed`) so a supplier name can be fixed before sending.
- Never told: the note, the bank reference, an extra's free-text description, sign-offs.

**A line filed under another supplier does not stop a send.** Recording a payment already allows such a line with a warning (`checkSupplierPaymentLines`), and on TEST two real transfers carry one that differs only by a full stop. It is sent, unnamed.

## Amounts

Added in integer minor units. A notice's amount is the sum of its lines. What no line explains (`notItemised` = transfer minus all lines) is never spread over the notices. Lines one cent above the transfer are a `roundingDifference`.

## What cannot be sent

`GET …/notices` says so in `blocked`, and a send is refused with the same code (409, `allowedNext` says what to do):

| Code | When |
|---|---|
| `NOT_A_SUPPLIER_PAYMENT` | a forwarder's charge is on it |
| `NO_LINES`, `LINE_TARGET_MISSING`, `LINES_EXCEED_TRANSFER` | nothing applied; something it was applied to has been deleted; lines above the transfer by more than 0.01 |
| `NOTHING_SENT` | no money went (a credit covered it) |
| `NO_PROOF` | no document on the transfer |
| `PROOF_NOT_SENDABLE` | a document on it is not a `remittance` filed under exactly the transfer's supplier name. Named in `files`; never dropped silently |
| `PROOFS_NOT_ATTACHABLE` | attached only: more than 5 files, more than 15 MB together, or not PDF / PNG / JPEG |
| `NOTICE_NOT_POSITIVE` | that notice comes to zero or less once its credits are taken off (the other one can still go) |

The proof rule is the same one the portal's `portal_payment_proofs_v` applies, so an email never promises a proof the page does not show.

## Routes

All under the greedy `/api/v1/{proxy+}` route. Roles are checked in the handlers: there is no role middleware.

### `GET /api/v1/supplier-payments/:id/notices`

What the dialog shows. Any signed-in user.

```json
{
  "paymentId": 45, "supplierName": "…", "sendable": true, "blocked": null, "canSend": true,
  "transfer": { "amount": 3000, "currency": "USD", "paidOn": "2026-11-14", "linesTotal": 3000, "notItemised": 0, "roundingDifference": 0 },
  "proofs": [{ "id": 77, "filename": "proof.pdf", "url": "https://…" }],
  "delivery": { "delivery": "portal", "reason": null, "portalSupplier": true },
  "previewToken": "<64 hex>",
  "notices": [{
    "kind": "deposit", "amount": 1000, "state": "not_sent", "lastSentAt": null, "blocked": null,
    "lines": [{ "type": "deposit", "amount": 1000, "named": true, "poId": 101, "poNumber": "PO_001J", "container": null, "shipmentId": null, "invoiceNumber": null, "jfCode": null, "extraKind": null, "containers": null }],
    "paidFor": ["PO_001J"], "unnamed": 0,
    "templateKey": "payment_proof_deposit_portal",
    "tokens": { "amount": "1,000.00", "currency": "USD", "paidOn": "2026-11-14", "paidFor": "PO_001J" },
    "portalUrl": "https://…/payments?payment=45",
    "readyDates": [{ "poNumber": "PO_001J", "url": "https://…/orders/101" }],
    "sends": [{ "id": 5, "kind": "deposit", "delivery": "portal", "status": "sent", "sentTo": ["…"], "sentAt": "…", "sentByEmail": "…", "sendError": null }]
  }],
  "views": [{ "email": "…", "firstViewedAt": "…", "lastViewedAt": "…", "count": 2 }]
}
```

- `lines[].type`: `deposit`, `pi_deposit`, `pi_balance`, `pi_full`, `balance`, `top_up`, `qc`, `repackaged`, `extra`, `credit`.
- `notices[].readyDates`: the buttons the email will carry, for a deposit, under either delivery. `[]` for a balance, and for a deposit whose orders the portal cannot open for this supplier. `portalUrl` stays `null` when the proof goes out attached.
- `notices[].state`: `not_sent`, `sending`, `sent`, `unknown`. `sent` means a send went out to the transfer's **current** supplier name; a transfer moved to another supplier reads `not_sent` again.
- `503 NOT_MIGRATED` on a server without the send log.

### `POST /api/v1/supplier-payments/:id/notices/:kind/email`

`:kind` is `deposit` or `balance`. Admin, standard and accountant users (`403 CANNOT_SEND` otherwise).

```json
{ "sendKey": "<8-64 of A-Z a-z 0-9 - _>", "to": ["a@x.com"], "subject": "…", "body": "<p>…</p>", "delivery": "portal", "previewToken": "…" }
```

- `sendKey`: one per click, made by the client and kept across transport retries of that click.
- `subject` / `body`: the sender's wording; blank falls back to the stored template. `{amount}`, `{currency}`, `{paidOn}` and `{paidFor}` are filled in **here**, from the payment as it stands.
- `delivery` and `previewToken`: what the dialog showed.
- `200 { send, delivery, portalAccess, replayed }`. `portalAccess` is the pre-approval's answer (`users[].outcome`: `invited`, `approved`, `already_active`, `blocked`); `replayed: true` when the key was already recorded.

Refusals beyond the table above:

| Status, code | Meaning |
|---|---|
| `409 DELIVERY_CHANGED` | it would no longer go out the way the dialog showed; body carries the new `delivery` and `reason` |
| `409 PAYMENT_CHANGED` | the payment was edited since the dialog opened |
| `409 SEND_IN_PROGRESS` | this notice is being sent right now |
| `409 UNRESOLVED_SEND` | an earlier send of this notice has an unknown outcome (`sendId`): resolve it first |
| `409 SEND_KEY_REUSED` | the key was already used with other content |
| `404 NO_SUCH_NOTICE` | the payment has no such part |
| `502 FRONT_REFUSED` | Front answered with a refusal (`frontStatus`): nothing was sent |
| `502 SEND_FAILED` | the email could not be prepared (a proof unreadable, no receipt, the deadline): nothing was sent |
| `502 SEND_OUTCOME_UNKNOWN` | Front did not answer, or it accepted and the result could not be recorded. **Not retried.** |

### `POST /api/v1/supplier-payment-sends/:sendId/resolve`

`{ "outcome": "sent" | "not_sent" }` for a send whose outcome is unknown: someone looked in Front. Same roles. `409 NOT_UNKNOWN` for any other send, including one still going.

### On the transfers themselves

`GET /api/v1/supplier-payments`, and the transfer returned by `POST` and `PUT`, carry `notices: [{ kind, state, lastSentAt, delivery }]`. `[]` when there is nothing to send (a forwarder's transfer); **`null` on a server without the send log**, which is how ShipLine knows to hide the feature.

## The send, in order

1. The `sendKey` is looked up first. A key already recorded for this payment, part and content is answered from its row, whatever the payment looks like now.
2. In one transaction, under the locks an edit of the payment takes (`lockSupplierPaymentTargets`; the lines are read again under the lock, and it starts over once if a line appeared that the lock did not cover): the rules are run, the delivery and the `previewToken` compared, and one row is written to `supplier_payment_sends` as `sending`. That commit is the moment the send exists.
3. Outside any transaction: pre-approval (portal), or the receipt token and then the proof files from S3 (attached). A failure here is a definite `failed`.
   - Pre-approval finding no such supplier or no portal: `409 DELIVERY_CHANGED`.
   - Pre-approval failing for another reason does not stop the email: it goes without the "already approved" wording and `portalAccess.status` is `error`.
4. Front is called **once**, and only while at least 20 s of the send's 60 s deadline is left (the call itself aborts at 15 s).
5. Accepted: `sent`, through the one finaliser (the guarded update, an attached send's receipt pointed at the row, the audit, in one transaction). Refused: `failed`. No answer, or accepted but not recorded: `unknown`.

Why a clock and not the function's time limit: the API function may run for 900 s, far beyond API Gateway's 30 s. So the limit says nothing about whether a request is still running. The deadline does: once it has passed, nothing can still be on its way to Front, and a person may say what happened. A row left `sending` past its deadline reads as `unknown`.

States, each change a guarded `UPDATE … WHERE status = …`:

```
sending ──Front accepted──▶ sent
        ──definite failure─▶ failed
        ──no answer────────▶ unknown ──a person: "it went"────▶ sent
                                     ──a person: "it did not"─▶ failed
```

Front's own answer outranks a person's "it did not": if the request that owns a send finally learns Front accepted it, the row becomes `sent` even after someone resolved it the other way.

This gives at most one call to Front per key and no blind retries. It does not claim exactly-once delivery.

## What the portal reads

No frozen copy of a payment exists anywhere: the portal reads the payment live (the user asked for this in place of a copy of what the email said). It reads through three views and has no new right on any payment table:

| View | Rows | Columns |
|---|---|---|
| `portal_payments_v` | live transfers with a `sent` send to their **current** supplier name | `payment_id`, `supplier_name`, `amount`, `currency`, `paid_on`, `last_sent_at` |
| `portal_payment_lines_v` | their lines | `line_id`, `payment_id`, `kind`, `amount`, `paid_with_json`, `payment_type`, `po_id`, `po_number`, `po_supplier`, `jf_code`, `shipment_id`, `shipment_reference`, `shipment_supplier`, `invoice_number`, `top_up`, `extra_kind`, `extra_rides_with`, `extra_supplier` |
| `portal_payment_proofs_v` | their documents that are a `remittance`, live, filed under exactly the transfer's supplier name | `document_id`, `payment_id`, `filename`, `public_url` |

- Views, not column grants: a column grant covers every row of a table, so the portal's login could have read every supplier's amounts, sent or not, and every invoice's file link.
- `SQL SECURITY DEFINER`: the reader is another login that must not hold rights underneath. The definer is whoever runs the migration.
- Every reference comes with the supplier it is filed under (`po_supplier`, `shipment_supplier`, `extra_supplier`), so the portal can test each one against the signed-in user's own suppliers.
- Consequences: an edit shows at once; a deleted payment is gone; a payment moved to another supplier is shown to nobody until it is sent again; there is no "unsend".
- The portal's own table `jportal.payment_views` (who opened which payment) is read here for `views` and for the receipt.

The proof link is the file's public, unguessable S3 address, as a PO PDF's is. Hiding a payment does not stop a link someone already copied.

## Email receipts

- `email_type 'payment_proof'`, `send_table 'supplier_payment_sends'`.
- A portal send is `confirm_via 'portal'` with an `action_url`, confirmed by the sync step `payment_viewed`: that payment opened on the portal after the email.
- An attached send carries the confirm button (`confirm_via 'email'`).
- **Never swept**: the scheduled reminders skip this type (`NEVER_SWEPT_TYPES`), whichever way it went out. The user chose no automatic reminders for these.
- The manual "Send reminder" still works, except `409 PAYMENT_NOT_CURRENT` once the payment is deleted or belongs to another supplier than the email went to.

## Schema

- `2026-11-16_20_supplier_payment_sends.sql`: the send log.
- `2026-11-16_21_portal_payment_views.sql`: the three views.
- `2026-11-16_22_grant_portal_payment_views_to_jportal.js`: `SELECT` on the views for the portal's login.
- `2026-11-16_30_seed_payment_proof_templates.js`: four templates, category `Payment proof`: `payment_proof_{deposit,balance}_{portal,attached}`. Tokens `{amount} {currency} {paidOn} {paidFor}`.
- `2026-11-17_10_reword_payment_proof_deposit_attached.js`: the attached deposit wording, from "reply to this email" to "enter … on our supplier portal". Touches only a row still holding the seeded body.
- `2026-11-16_10_grant_request_departure_to_jportal.js` belongs to the Requests to send page (`docs/request-to-send-portal.md`).

## Rollout (test, then prod; all user-triggered)

1. **Shipping**: `bash deploy.sh <stage>`. With `PAYMENT_PROOF_PORTAL` unset every send goes out attached. ShipLine can ship now.
2. **Portal**: its deploy creates `payment_views`, applies its grants file (the same grants as `_22`) and brings the Payments page.
3. Set `PAYMENT_PROOF_PORTAL=on` in the stage's secret and deploy shipping again. Roll back by turning it off.

## Measured on TEST (2026-11-16, read-only)

44 transfers: 41 named exactly as a live supplier (the other three are two forwarder payments and one supplier spelled differently). 18 deposit only, 23 balance only, 1 both. All 26 documents linked to a transfer are a `remittance` under exactly the transfer's supplier name; 18 transfers have no document. 2 of 101 lines are filed under another spelling of their transfer's supplier.

## Not verified

- A real send through Front as a portal link; the reminder of one. One attached deposit email, sent by the user, was seen on 2026-11-17 (it is what showed the "reply to this email" wording); the reworded one, with its buttons, has not been sent.
- The attached deposit's buttons in a real mail client, and an address new to the portal following one: it registers, waits for approval, and only then reaches the order.
- The routes against a real database, by anything in this repo. When this was written (2026-11-16) the send log and the views were migrated nowhere; the user's attached send since then means the send log is on at least that stage. The views' `SELECT`s were run read-only on TEST with the join to `portal_payments_v` swapped for the live transfers (101 lines, all resolved; 26 proofs).
- No HTTP suite and no two-connection race test exist yet: both need the migrated table. The lock order and the guarded updates are checked against a scripted connection only.
- What Front offers for idempotency: not looked into. The send relies on its own key and on never retrying.
