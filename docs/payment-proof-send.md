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

## A portal link, or not at all

**A payment is sent to a supplier as a link to the supplier portal only. Its proof is never attached to the email** (the user, 2026-11-17: "dont want to send proof attached, only via portal"). Until then a payment the portal could not show went out with the proof attached instead; that way is gone.

The email links to the portal's Payments page (`/payments?payment=<id>`). Recipients are pre-approved on the portal for the transfer's supplier, as the PO email does. Under the wording the send appends, in this order:

1. the button to the payment on the portal, how to get an account, and "No file is attached";
2. what the payment asks for, with its own button:
   - a **deposit**: a button to each order's page (`/orders/<po id>`), where the estimated ready dates are entered (`readyDatesFor`: one per order the supplier is told of);
   - a **balance**: a button to upload the telex release, naming the containers it paid (`telexContainersOf`: the balance and top-up lines filed under exactly this supplier, the ones the portal's row offers an upload for). Not for QC units, a charge, or an invoice paid through containers: the portal offers no upload for those, and the email does not ask.

Where the portal cannot show the payment, **it cannot be sent**: the preview is `blocked` and a send is refused with the same body (`portalRefusal`). `delivery.delivery` is then `null`, with why in `delivery.reason`:

| `reason` | Meaning | Refusal | The sender can |
|---|---|---|---|
| `no_supplier` | no live supplier is named **exactly** as the transfer's `supplier_name` | `409 SUPPLIER_NOT_ON_PORTAL` | make the payment's supplier name and the supplier record match (`allowedNext: ['edit_payment']`) |
| `portal_url_unset` | `SUPPLIER_PORTAL_URL` is not configured | `409 PORTAL_NOT_LIVE` | nothing |
| `portal_unavailable` | the portal's `payment_views` table is not on this server: the portal is not deployed on this stage yet | `409 PORTAL_NOT_LIVE` | nothing |

What is wrong with the payment itself (no proof, a forwarder's payment, …) is said before any of these: that is what the sender can act on, and a forwarder's payment is never told to "match the supplier record".

**There is no switch** (the user, 2026-11-17: an earlier `PAYMENT_PROOF_PORTAL` was removed). Payments can be sent on a stage as soon as the portal is deployed there: its `payment_views` table is the sign. Until then ShipLine's dialog says the supplier portal is not available here, and nothing can be sent. A `PAYMENT_PROOF_PORTAL` left in a stage's secret is not read.

Sends from before 2026-11-17 that went out attached stay in `supplier_payment_sends` with `delivery 'attached'` and are shown as they went; their receipts keep their confirm button. Nothing writes `'attached'` any more.

### The ready dates are entered on the portal, never by reply

Decided by the user on 2026-11-17, on the first deposit email sent: its wording asked the supplier to "reply to this email with the estimated ready date of each item". No email asks for that. A deposit's ready dates are entered on each order's page of the portal, and the email carries a button to each.

### The wording

Two stored templates, category `Payment proof`: `payment_proof_deposit_portal` ("Deposit paid") and `payment_proof_balance_portal` ("Balance paid"). The keys keep their `_portal` from when an `_attached` pair existed. Each says what was paid and points below: "The proof of payment, and anything we need from you for it, is below." Where the proof is, the ready dates and the telex release are said once, by the appended blocks, with their buttons. Before 2026-11-17 the body said them as well, so every email said everything twice in each language.

ShipLine's dialog questions wording that says something is attached.

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
| `MIXED_PAYMENT` | a forwarder's charge and something else are on one transfer: it can be sent to neither (see "A forwarder's payment"). `NOT_A_SUPPLIER_PAYMENT` was this code's name until 2026-11-20, when a transfer that is a forwarder's alone became sendable to the forwarder |
| `NO_LINES`, `LINE_TARGET_MISSING`, `LINES_EXCEED_TRANSFER` | nothing applied; something it was applied to has been deleted; lines above the transfer by more than 0.01 |
| `NOTHING_SENT` | no money went (a credit covered it) |
| `NO_PROOF` | no document on the transfer |
| `PROOF_NOT_SENDABLE` | a document on it is not a `remittance` filed under exactly the transfer's supplier name. Named in `files`; never dropped silently |
| `SUPPLIER_NOT_ON_PORTAL` | no supplier record has the payment's exact supplier name: the portal has nobody to show it to |
| `PORTAL_NOT_LIVE` | the portal's Payments page is not live on this server (`reason` says which of the three) |
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
- `delivery.delivery`: `"portal"`, or `null` when the payment cannot be sent (`blocked` then carries the refusal).
- `notices[].readyDates`: the buttons a deposit's email will carry. `[]` for a balance, and for a deposit whose orders the portal cannot open for this supplier.
- `notices[].telex`: the containers a balance's email asks the telex release of, by number (`null` for one with none). `[]` for a deposit, and for a balance that paid no container.
- `notices[].portalUrl`: `null` when it cannot be sent.
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
| `409 DELIVERY_CHANGED` | the request asked for `"attached"` (a dialog opened before 2026-11-17): it goes out as a portal link now |
| `409 SUPPLIER_NOT_ON_PORTAL`, `409 PORTAL_NOT_LIVE` | the portal cannot show the payment (above): nothing is sent, and nothing is attached instead |
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

`GET /api/v1/supplier-payments`, and the transfer returned by `POST` and `PUT`, carry `notices: [{ kind, state, lastSentAt, delivery }]`. `[]` when there is no supplier part to send (a forwarder's transfer, or a mixed one); **`null` on a server without the send log**, which is how ShipLine knows to hide the feature.

A transfer that is a forwarder's alone also carries `forwarderNotice` (the same shape, `kind: 'forwarder'`) and `acknowledgement` (see "A forwarder's payment"); both are `null` on every other transfer. Each line of kind `extra` carries `forwarderId`.

## The send, in order

1. The `sendKey` is looked up first. A key already recorded for this payment, part and content is answered from its row, whatever the payment looks like now.
2. In one transaction, under the locks an edit of the payment takes (`lockSupplierPaymentTargets`; the lines are read again under the lock, and it starts over once if a line appeared that the lock did not cover): the rules are run, the delivery and the `previewToken` compared, and one row is written to `supplier_payment_sends` as `sending`. That commit is the moment the send exists.
3. Outside any transaction: the recipients are pre-approved on the portal. No file is read: nothing is attached.
   - Pre-approval finding no such supplier or no portal after all: the send is `failed`, `409 SUPPLIER_NOT_ON_PORTAL` or `409 PORTAL_NOT_LIVE`.
   - Pre-approval failing for another reason does not stop the email: it goes without the "already approved" wording and `portalAccess.status` is `error`.
4. Front is called **once**, and only while at least 20 s of the send's 60 s deadline is left (the call itself aborts at 15 s).
5. Accepted: `sent`, through the one finaliser (the guarded update and the audit in one transaction; for a send from before 2026-11-17 that went out attached, also its receipt pointed at the row). Refused: `failed`. No answer, or accepted but not recorded: `unknown`.

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
- A portal send is `confirm_via 'portal'` with an `action_url`, confirmed by the sync step `payment_viewed`: that payment's **proof** opened on the portal after the email. The portal records a view for a proof opened and for nothing else (the user, 2026-11-17): arriving from the email's link does not confirm it. ShipLine's "proof opened on the portal by …" reads the same rows.
- A send from before 2026-11-17 that went out attached kept its confirm button (`confirm_via 'email'`); none is made any more.
- **Never swept**: the scheduled reminders skip this type (`NEVER_SWEPT_TYPES`), whichever way it went out. The user chose no automatic reminders for these.
- The manual "Send reminder" still works, except `409 PAYMENT_NOT_CURRENT` once the payment is deleted or belongs to another supplier than the email went to.

## Schema

- `2026-11-16_20_supplier_payment_sends.sql`: the send log.
- `2026-11-16_21_portal_payment_views.sql`: the three views.
- `2026-11-16_22_grant_portal_payment_views_to_jportal.js`: `SELECT` on the views for the portal's login.
- `2026-11-16_30_seed_payment_proof_templates.js`: four templates, category `Payment proof`: `payment_proof_{deposit,balance}_{portal,attached}`. Tokens `{amount} {currency} {paidOn} {paidFor}`.
- `2026-11-17_10_reword_payment_proof_deposit_attached.js`: the attached deposit wording, from "reply to this email" to "enter … on our supplier portal". Touches only a row still holding the seeded body. Superseded the same day: `_30` retires that template.
- `2026-11-17_30_payment_proof_portal_only.js`: rewords and renames the two templates that stay (only where a row still holds the seeded body; an edited one is left and named in the log), and retires the two `_attached` ones (soft-deleted; their rows stay). The in-code list still holds the retired pair, unchanged, because `_30` of 2026-11-16 and `_10` read it.
- `2026-11-16_10_grant_request_departure_to_jportal.js` belongs to the Requests to send page (`docs/request-to-send-portal.md`).

## A forwarder's payment

"Send to forwarder" (2026-11-20). A recorded payment to a **freight forwarder** is sent to that forwarder's contacts as a link to the portal's Freight payments page (`<portal>/freight-payments?payment=<id>`), where they see it, open its proof and may press Acknowledge. The same route, send log and machinery as a supplier's send (`sendKey`, `previewToken`, the one row written before Front is called, "check Front", resolve, receipt), with the part `forwarder`.

### Whose payment it is

By **forwarder id, never by a name**. `forwarderOfLines` (`src/lib/payment-notices.js`): every line of the transfer is a forwarder's charge (`payment_extras`, `rides_with = 'shipment'`) linked to the same row of the Forwarders list (`payment_extras.forwarder_id`). The transfer's own payee name decides nothing but which proof files count (a `remittance` filed under exactly that name, as for a supplier).

| Code | When |
|---|---|
| `MIXED_PAYMENT` | a forwarder's charge shares the transfer with anything else (goods, a deposit, a credit on the account). Record the forwarder's charges as a payment of their own |
| `FORWARDER_NOT_LINKED` | a charge's payee is typed text, not one of the Forwarders list. Pick the forwarder on the charge (or run `tools/backfill-forwarder-ids.js`) |
| `FORWARDERS_DIFFER` | charges of two forwarders on one transfer |
| `FORWARDER_REMOVED` | the forwarder is off the Forwarders list |
| `FORWARDER_NO_CONTACT` | the forwarder has no contact: nobody could open it |
| `PORTAL_NOT_LIVE` | the portal address is not configured, or the portal's `forwarder_payment_events` table is not on this server (its Freight payments page is not deployed there) |
| `NOT_A_CONTACT` (400) | an address in `to` is not a live contact of that forwarder (`notContacts` names them), or none is |

`NO_LINES`, `NOTHING_SENT`, `LINE_TARGET_MISSING`, `LINES_EXCEED_TRANSFER`, `NO_PROOF`, `PROOF_NOT_SENDABLE` and `NOTICE_NOT_POSITIVE` apply as for a supplier.

### Who it goes to

Only the forwarder's own **contacts** (`forwarder_contacts`): `to` must be a non-empty list of them, trimmed and lower-cased, each once. The portal shows the payment to whoever signs in with a contact's address, so any other address would get an email about a page they cannot open. Every address is pre-approved on the portal with **no supplier linked** (`preapprovePortalAddresses`, as a freight quote request does): a forwarder's contact sees that forwarder's quote requests and payments, and no supplier's orders.

### What the forwarder is told

Per charge: its container (the number only), its kind and its amount. **Never** the description or note typed on it, a PO, an invoice number, or a supplier (the user, 2026-11-20). The stored wording is `payment_proof_forwarder_portal` ("Forwarder paid", category `Payment proof`); `{paidFor}` is the container numbers. Under it the send appends the button to the payment and how to get an account. Nothing is asked for in return.

### What comes back

`GET …/notices` for a forwarder's transfer adds:

- `audience: 'forwarder'` (`'supplier'` on every other);
- `forwarder: { id, name, contacts: [{ name, email }] }`, or `null` while it is linked to none;
- `opened: [{ email, name, firstAt, lastAt, count }]`: who of that forwarder opened a proof;
- `acknowledgement: { byName, byEmail, at, current } | null`: who pressed Acknowledge. `current` is true when it was made of the payment **as it stands**; false when a figure, a container or a proof has changed since.

`views` is always `[]` for it: `jportal.payment_views` is the supplier page's, and an opening there proves nothing about a forwarder.

Both come from the portal's own `forwarder_payment_events`, and only for the forwarder the transfer is linked to **now**: what a forwarder it used to belong to did is never shown as this one's. "As it stands" is one definition, the `revision` column of `portal_forwarder_payments_v`: a hash, computed in the view, of everything the forwarder is shown.

The email's receipt is confirmed (`forwarder_payment_seen`, `src/lib/email-receipt.js`) when that forwarder opened a proof or acknowledged **after the email**. It means they did something with it since; it does not say the payment is unchanged.

### What the portal reads

`2026-11-20_21_portal_forwarder_payment_views.sql`, three definer views, granted to the portal's login by `_22`:

- `portal_forwarder_payments_v`: a live transfer whose every line is a live charge linked to one forwarder (an aggregate over **all** its lines: one line of any other kind, a deleted or missing charge, an unlinked one, or two forwarders keeps the whole transfer out), whose forwarder is on the list, and which a `sent` send of part `forwarder` carries **that same forwarder id** for. With `revision`.
- `portal_forwarder_payment_lines_v`: `amount, extra_kind, shipment_reference`.
- `portal_forwarder_payment_proofs_v`: the documents linked to that payment that are a `remittance` under exactly its name.

The same migration re-creates the **supplier's** `portal_payments_v` with two more conditions: a send counts only when its part is `deposit` or `balance`, and the transfer holds no forwarder charge now. Without them a payment sent to a forwarder would be a row of the supplier's view for any supplier user whose supplier record has exactly the forwarder's name; and a payment sent to a supplier and later turned into a forwarder's would stay there.

Live, like the supplier's page: an edit shows at once. A payment re-pointed at another forwarder leaves the first one's page and is not on the second's until it is sent to it; pointed back, it shows to the first again without a new send.

### On ShipLine's list

`forwarderNotice` and `acknowledgement` on the transfer (above). `notices` stays `[]` for a forwarder's transfer on purpose: a ShipLine from before this shows no button on one, so the send is offered only by a ShipLine that knows it goes to the forwarder's contacts.

### Reminder

A manual reminder of a forwarder's email (`reminderRefusal`) is refused with `PAYMENT_NOT_CURRENT` unless the forwarder view still lists the payment for the forwarder it went to and every address it went to is still a contact.

### Schema

- `2026-11-20_20_forwarder_payment_sends.js`: `supplier_payment_sends.forwarder_id`.
- `2026-11-20_21_portal_forwarder_payment_views.sql`: the three views, and the supplier's first view tightened.
- `2026-11-20_22_grant_portal_forwarder_payment_views_to_jportal.js`: `SELECT` on the three for the portal's login.
- `2026-11-20_23_seed_forwarder_payment_template.js`: the wording.
- The portal's own: `jportal.forwarder_payment_events` (its `2026-11-20_jportal_forwarder_payment_events.sql`), insert-only.

Needs the forwarder links of `2026-11-20_10` and, for payments recorded before them, `tools/backfill-forwarder-ids.js`.

### Tests

`tools/test-forwarder-payment-notices-lib.js` and the forwarder cases of `tools/test-payment-notices-send.js` and `tools/test-email-receipt-lib.js` (all in `npm run test:unit`). Against a real database: `tools/test-portal-forwarder-views.js --confirm-host <TEST host>` (fixtures of its own, removed at the end): who each view lists, that a forwarder's payment is never a row of the supplier's view, what `revision` covers, and that the portal's one-statement write waits for an uncommitted edit and then writes nothing for the revision that went.

### Rollout (all user-triggered)

1. This repo: migrate and deploy. Nothing changes for staff.
2. The portal: its migration (`forwarder_payment_events`), grants and deploy; then its `tools/smoke-forwarder-payments.js`.
3. ShipLine, once step 2's smoke passes. Its button is what turns this on: there is no switch.

To take the portal's page away again, roll **this repo** back first: the portal's table outlives a portal rollback, so the check above would not stop a send, and an open ShipLine tab can still send.

## Rollout (test, then prod; all user-triggered)

1. **Shipping**: `bash deploy.sh <stage>`. **Nothing can be sent yet**: ShipLine's dialog says the supplier portal is not available here. ShipLine can ship; its Send button works after step 2.
2. **Portal**: its deploy creates `payment_views`, applies its grants file (the same grants as `_22`) and brings the Payments page.
   From then on payments can be sent, as links to the portal: no other step, no switch.

## Measured on TEST (2026-11-16, read-only)

44 transfers: 41 named exactly as a live supplier (the other three are two forwarder payments and one supplier spelled differently). 18 deposit only, 23 balance only, 1 both. All 26 documents linked to a transfer are a `remittance` under exactly the transfer's supplier name; 18 transfers have no document. 2 of 101 lines are filed under another spelling of their transfer's supplier.

## Not verified

- A real send through Front as a portal link, in a real mail client; the reminder of one. The only real send seen so far was an attached deposit email (2026-11-17), from before attached sends were removed.
- The routes against a real database, by anything in this repo. When this was written (2026-11-16) the send log and the views were migrated nowhere; the user's attached send since then means the send log is on at least that stage. The views' `SELECT`s were run read-only on TEST with the join to `portal_payments_v` swapped for the live transfers (101 lines, all resolved; 26 proofs).
- No HTTP suite and no two-connection race test exist yet: both need the migrated table. The lock order and the guarded updates are checked against a scripted connection only.
- What Front offers for idempotency: not looked into. The send relies on its own key and on never retrying.
