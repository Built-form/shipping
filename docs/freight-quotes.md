# Freight quotes: asking forwarders for sailings and prices, booking one, sending the contents

Containers from China to the UK used to be arranged by email. Now:

1. **Request.** Staff pick one or more freight forwarders, a date range and the delivery options to quote on ShipLine's FREIGHT page (header tab 09), list any stock movements (goods to be moved from a factory to the port or a consolidation point, with CBM and weight), read and edit the email, and send. Each forwarder gets its own email and its own link.
2. **Answer.** The link opens a page on the supplier portal with **no sign-in**, where the forwarder lists its options: loading date, sailing date, transit time, port A, port B, service, container size, all-in price. A forwarder contact who signs in to the portal sees the same requests under **Freight quotes**.
3. **Compare and book.** The options show in ShipLine as a month calendar and as a list. Staff open one and press **Request booking**; the forwarder gets a second email and confirms on the same page with a booking reference (or declines). Staff can also mark a booking confirmed themselves.
4. **Contents.** Staff attach a draft container to the booking and email the forwarder its contents **broken down by supplier**, with the forwarder quote PDFs and CSVs attached.

Code: `src/lib/freight-quotes.js` (pure rules), tested by `tools/test-freight-quotes-lib.js`; `src/services/freight-quote-routes.js` (the staff routes, registered from `src/handlers/orders.js`), tested by `tools/test-freight-quote-routes.js`; both in `npm run test:unit`. Portal side: `portal/src/handlers/freightQuotes.js`, `portal/src/lib/freightQuotes.js`, `portal/web/src/screens/FreightQuoteScreen.tsx`, `FreightQuotesScreen.tsx` (portal README section 20). ShipLine: `src/components/freight/` (`FreightView.tsx`, `QuotesTab.tsx`, `RequestsTab.tsx`, `BookingsTab.tsx`, `ForwardersTab.tsx`, `DeliveryOptionsTab.tsx`, `FreightEmail.tsx`, `freightQuotes.ts`, `freightCalendar.ts`).

## Decisions

- **Forwarders are a list of their own, kept in ShipLine** (`forwarders`, `forwarder_contacts`), not JFPRO suppliers. A contact's email address is both where requests go and how that person is recognised when they sign in to the portal.
- **One request row per forwarder.** A send to three forwarders is three rows, three tokens and three emails sharing a `batch_id`, so their answers are compared side by side.
- **A request is not tied to a container.** The draft container is attached to the booking, at booking time or later.
- **One all-in price and a currency per option.** Prices in different currencies are never ranked against each other: ShipLine groups by currency. There are no exchange rates anywhere.
- **Each option carries its own container size.** It defaults to the size asked for, but a forwarder may offer a 40GP when the 40HQ is full; ShipLine flags the difference.
- **A sailing may fall up to 14 days outside the range asked for** (`SAILING_MARGIN_DAYS`); ShipLine flags it.
- **Every save is a new answer.** Options are insert-only: a save bumps `answer_count` and inserts rows with that number as `answer_no`. The options on show are those whose `answer_no` equals the request's `answer_count`. The portal's login has no DELETE, and a booking keeps pointing at the option it was made on.
- **A request's state is derived, never stored**: `revoked` > `expired` > `booked` (a booking is `requested` or `confirmed`) > `answered` > `open`. A declined or cancelled booking therefore puts the request back to `answered` with nothing to undo.
- **One live booking per request**, enforced by a unique key on a generated column (`live_request_id`).
- **A confirmation closes the other forwarders' links** of the same send as `filled`, so nobody quotes for a container that is booked. Not earlier: until then a decline must leave the next forwarder bookable. Cancelling a confirmed booking reopens them.
- **Delivery options are staff's own list, as free text** (`freight_delivery_options`, ShipLine's FREIGHT > DELIVERY OPTIONS; seeded with Door to door, Door to port, Port to port, Port to door). A request stores the **text** of the options ticked (`services`), as it stood when sent, and each option the forwarder offers names one of them exactly (`service`). Rewording or removing a list entry therefore changes the next request only. Requests from before 2026-11-16_30 hold four codes (`door_to_door` …) instead; every reader still takes them, and they and their English names keep their Chinese translation in the emails and on the portal (`serviceCode`, `serviceLabel`).
- **Stock movements ride on the request** as JSON (`stock_movements`: `[{ from, to, cbm, weightKg }]`), as sent. They are listed in the quote request email under the request's details and on the forwarder's page. They are information for the quote: nothing prices them separately and nothing links them to a draft container.
- **Every email can be edited before it goes.** Each of the three send dialogs shows the template's subject and body and sends them back as `subject` and `body`; the routes render `{tokens}` in whatever arrives and still append their fixed block (the details and the button with the link), which is not editable. Left out, the stored template is used.
- **Sends are safe to retry.** See "Retries" below.
- **Opening the link answers nothing.** The portal's read never writes and every write is a `PUT` with the portal's CSRF header, so a mail scanner that follows the link changes nothing.
- **No "I've read it" receipt button** on the two link emails. They are listed on ShipLine's Email Receipts page (`confirm_via='portal'`) and show as confirmed once the forwarder answers (`src/lib/email-receipt.js`: `freight_quote_answered`, `freight_booking_answered`). The contents email is a notice (`confirm_via='none'`).

## Tables (this schema)

`src/db/migrate/2026-11-16_10_freight_quotes.sql`, and `2026-11-16_30_freight_delivery_options_and_movements.sql` (the delivery option list, `stock_movements`, `freight_quote_options.service` widened from 16 to 100 characters; `_32` seeds the list). No foreign keys, no ENUMs, UTC.

| table | columns | written by |
|---|---|---|
| `forwarders` | `name`, `notes`, `deleted_at` | shipping |
| `forwarder_contacts` | `forwarder_id`, `name`, `email` (trimmed, lower-case), `deleted_at`; unique per forwarder while live | shipping |
| `freight_delivery_options` | `label` (free text, 100), `created_by_email`, `deleted_at` | shipping; never read by the portal |
| `freight_quote_requests` | `token` (48 hex, unique), `batch_id`, `forwarder_id`, `forwarder_name`, `window_start_date`, `window_end_date`, `origin`, `destination`, `container_size`, `services` (JSON: the delivery options as text), `stock_movements` (JSON, nullable), `staff_note`, `sent_to`, `subject`, `created_by_email`, `sent_at`, `expires_at`, the Front ids, `revoked_at`, `revoke_reason` (`staff` \| `send_failed` \| `filled` \| `forwarder_removed`) | shipping |
| | `answer_count`, `answered_at`, `answered_by_name`, `answered_by_email`, `answer_source` (`link` \| `portal` \| `staff`), `answer_note`, `declined`, `answer_ip` | the portal (shipping for a staff-entered answer) |
| | `row_version` | both |
| `freight_quote_options` | `request_id`, `answer_no`, `line_no`, `loading_date`, `sailing_date`, `transit_days`, `port_from`, `port_to`, `service`, `container_size`, `price`, `currency`, `carrier`, `valid_until`, `note` | INSERT only: the portal, or shipping for a staff-entered answer |
| `freight_bookings` | `request_id`, `option_id`, `status` (`requested` \| `confirmed` \| `declined` \| `cancelled`), `requested_by_email`, `requested_at`, `request_note`, `sent_to`, `subject`, the Front ids, `shipment_id`, `draft_container_name`, `cancelled_at`, `cancelled_by_email`, `cancel_reason`, `contents_sent_at`, `contents_sent_to`, `contents_json` | shipping |
| | `booking_ref`, `response_note`, `responded_at`, `responded_by_name`, `responded_by_email`, `response_source`, `response_ip` | the portal (shipping when staff mark it) |
| | `row_version` | both |

The token is never written to the audit log.

### Grants for the portal's login

`2026-11-16_11_grant_freight_quotes_to_jportal.js` applies them on deploy where `jportal_app` exists; the portal's `db/grants/jportal_app.sql` carries the same statements, so **shipping is deployed first**.

```sql
GRANT SELECT (id, token, forwarder_id, forwarder_name, window_start_date, window_end_date, origin,
              destination, container_size, services, staff_note, created_at, expires_at, revoked_at,
              revoke_reason, answer_count, answered_at, answered_by_name, answer_note, declined, row_version)
             ON jfa.freight_quote_requests TO 'jportal_app'@'%';
GRANT UPDATE (answer_count, answered_at, answered_by_name, answered_by_email, answer_source,
              answer_note, declined, answer_ip, row_version)
             ON jfa.freight_quote_requests TO 'jportal_app'@'%';
GRANT SELECT ON jfa.freight_quote_options TO 'jportal_app'@'%';
GRANT INSERT (request_id, answer_no, line_no, loading_date, sailing_date, transit_days, port_from,
              port_to, service, container_size, price, currency, carrier, valid_until, note)
             ON jfa.freight_quote_options TO 'jportal_app'@'%';
GRANT SELECT (id, request_id, option_id, status, requested_at, request_note, booking_ref,
              response_note, responded_at, responded_by_name, response_source, cancelled_at, row_version)
             ON jfa.freight_bookings TO 'jportal_app'@'%';
GRANT UPDATE (status, booking_ref, response_note, responded_at, responded_by_name,
              responded_by_email, response_source, response_ip, row_version)
             ON jfa.freight_bookings TO 'jportal_app'@'%';
GRANT SELECT (forwarder_id, email, deleted_at) ON jfa.forwarder_contacts TO 'jportal_app'@'%';
```

`2026-11-16_31_grant_freight_stock_movements_to_jportal.js` adds one column to the first grant:

```sql
GRANT SELECT (stock_movements) ON jfa.freight_quote_requests TO 'jportal_app'@'%';
```

The portal never reads who a request or a booking was emailed to, who sent it, or the address an answer came from, and never writes the revoke columns, a cancellation, the attached container or the contents columns.

## Rules (`src/lib/freight-quotes.js`)

- **Vocabularies.** Sizes `20GP`, `40GP`, `40HQ`; currencies `USD`, `GBP`, `EUR`, `CNY`. Delivery options are free text (one line, at most 100 characters; at most 60 in the list, no two alike in any case).
- **A stock movement**: `from` (the factory) and `to` (the port or consolidation point), one line each; `cbm` above zero with at most three decimals; `weightKg` above zero with at most two. At most 30 per request; none is fine.
- **A request**: 1 to 10 forwarders; a range of at most 92 days that is not wholly in the past; 1 to 12 delivery options (the same text twice counts once); `origin` defaults to `China`, `destination` to `UK`, the size to `40HQ`; the link lasts 30 days (7 to 90 on request).
- **An option**: loading and sailing dates real days, loading on or before sailing; sailing inside the range give or take 14 days; transit 1 to 120 whole days; ports one line each; the delivery option exactly one of those asked about; the size from the list; the price above zero with at most two decimals. At most 40 options per answer, and 20 answers per link.
- **An answer** is either at least one option, or `declined` ("cannot quote") with none.
- The portal's `src/lib/freightQuotes.js` mirrors the token shape, the vocabularies, the limits, `requestState` and the option rules; ShipLine takes `state` from the API.

## The three emails

Templates `freight_quote_request`, `freight_booking_request` and `freight_booking_contents` (category `Freight`; `src/lib/email-template-defaults.js`, seeded by `2026-11-16_20_seed_freight_templates.js`), editable in ShipLine's Email Templates. Each is the inside of the card: the sender appends a fixed block and wraps the whole with `wrapEmail`. English and Chinese.

| email | tokens | fixed block |
|---|---|---|
| quote request | `{forwarder} {windowStart} {windowEnd} {origin} {destination} {containerSize} {services}` | what is asked, the stock movements (a table with a total row), the **Enter your quote** button, the link as text, how to register on the portal, when the link stops working |
| booking request | `{forwarder} {sailingDate} {loadingDate} {portFrom} {portTo} {service} {containerSize} {price}` | the option, staff's note, the **Confirm or decline the booking** button |
| contents | `{forwarder} {bookingRef} {sailingDate} {containerName}` | totals, then one table per supplier (port and PO numbers in its heading, a subtotal row) |

**Editing before sending.** ShipLine's three send dialogs load the `Freight` templates (`FreightEmail.tsx`), open on their own by key, and let staff pick another, change the subject and rewrite the body. For the booking request and the contents email every token is known in the dialog, so it is filled in there and the text reads as it will be sent; for the quote request the tokens stay as `{tokens}` and are filled in here per forwarder. If the templates cannot be read the dialog sends without `subject` and `body` and the stored template is used. EMAIL TEMPLATES in the page header opens the template manager.

All go through Front (`sendFrontMessage`), to the forwarder's live contacts. `sendFrontMessage` gained an optional `attachments` list for the contents email.

Before a quote request goes out its recipients are **pre-approved on the portal with no supplier linked** (`preapprovePortalAddresses` in `src/services/portal-access.js`), so a contact who registers has no approval to wait for. Such an account sees no supplier's orders: the portal shows it the forwarder's requests by matching its address against `forwarder_contacts`.

## Routes

All under the staff sign-in (`/api/v1/{proxy+}`). Reads are open to every staff user; writes need an editor role (`403 READ_ONLY` otherwise). Refusals are `{ error, code, field? }`.

| route | does |
|---|---|
| `GET /forwarders` | `{ forwarders: [{ id, name, notes, contacts: [{ id, name, email }] }] }` |
| `POST /forwarders`, `PATCH /forwarders/:id` | `{ name, notes?, contacts: [{ name?, email }] }` → `{ forwarder }`. PATCH takes the whole forwarder: a contact left out is removed |
| `DELETE /forwarders/:id` | soft; its contacts go with it and its open links are revoked as `forwarder_removed`. `409 HAS_BOOKING` while one of its bookings is live |
| `GET /freight-delivery-options` | `{ deliveryOptions: [{ id, label }] }`, in the order they were added |
| `POST /freight-delivery-options`, `PATCH /freight-delivery-options/:id` | `{ label }` → `{ deliveryOptions }` (the whole list). `409 DUPLICATE` for text already in the list (any case), `409 LIST_FULL` at 60 |
| `DELETE /freight-delivery-options/:id` | soft; twice is a no-op. → `{ deliveryOptions }`. Requests already sent keep the text |
| `GET /freight-quotes?from=&to=` | the **board**: `{ enabled, requests, bookings }`. Without a range, every request whose range ended within the last 120 days |
| `GET /freight-quotes/summary` | `{ answered }`: sends with a quote in and nothing booked yet (the tab's badge). A send counts once; a declined, closed or expired request and a removed forwarder do not count |
| `POST /freight-quotes` | `{ forwarderIds, batchId?, windowStart, windowEnd, origin?, destination?, containerSize?, services, stockMovements?, note?, expiryDays?, subject?, body? }` (`services`: the delivery options as text; `stockMovements`: `[{ from, to, cbm, weightKg }]`) → `{ batchId, results: [{ forwarderId, forwarderName, outcome, requestId?, error? }], …board }`. `outcome`: `sent` \| `already_sent` \| `send_unconfirmed` \| `failed` \| `no_contacts`. `409 PORTAL_URL_UNSET` |
| `POST /freight-quotes/:id/revoke` | closes a link. `409 HAS_BOOKING` while a booking on it is live. → the board |
| `POST /freight-quotes/:id/options` | staff type in an emailed answer: `{ options, declined?, note? }`, the same rules. Allowed on an expired link, never on a closed one or under a live booking. → the board |
| `POST /freight-quotes/options/:optionId/book` | `{ note?, shipmentId?, to?, subject?, body? }` → `{ ok, bookingId, …board }`. `409 ALREADY_BOOKED` \| `STALE_OPTION` \| `REVOKED` \| `NO_CONTACTS` \| `CONTAINER_TAKEN`; `502 FRONT_ERROR` (the booking is then cancelled as `send_failed`) |
| `PATCH /freight-bookings/:id` | any of `{ shipmentId: id \| null }`, `{ status: 'confirmed', bookingRef, note? }`, `{ status: 'cancelled' }`. `409 NOT_LIVE` for a declined or cancelled booking. → the board |
| `GET /freight-bookings/:id/contents` | `{ containerName, groups, totals, missingLots, sentAt, changedSinceSent, changes }` |
| `POST /freight-bookings/:id/contents-email` | `{ rowVersion, to?, subject?, body?, attachDocuments? }` → the board. `409 STALE` \| `NO_CONTAINER` \| `NO_LINES` \| `NOT_LIVE` |

**The board.** A request: `{ id, batchId, forwarderId, forwarderName, state, revokeReason, windowStart, windowEnd, origin, destination, containerSize, services, stockMovements, note, sentTo, subject, createdAt, sentAt, sentByEmail, expiresAt, revokedAt, answerCount, answeredAt, answeredBy, answerSource, answerNote, declined, rowVersion, url, options }`. An option: `{ id, requestId, lineNo, loadingDate, sailingDate, transitDays, arrivalDate, portFrom, portTo, service, containerSize, price, currency, carrier, validUntil, note }`. A booking: `{ id, requestId, optionId, status, forwarderId, forwarderName, option, requestedByEmail, requestedAt, requestNote, sentTo, shipmentId, containerName, containerStage, bookingRef, responseNote, respondedAt, respondedBy, responseSource, cancelledAt, cancelReason, contentsSentAt, contentsSentTo, contentsChanged, rowVersion }`. `enabled` is false when `SUPPLIER_PORTAL_URL` is unset or the tables are not on this server.

**Staff marking a booking.** These routes sit behind the staff sign-in and never use the forwarder's token, so there is no override flag: `PATCH … { status: 'confirmed', bookingRef }` records `response_source = 'staff'`. It also corrects the reference of a confirmed booking.

**Cancelling** sets the booking `cancelled`, pushes the request's `expires_at` out 30 days and, when the booking was confirmed, reopens the links that were closed as `filled`. No email is sent; the forwarder's page says the request was withdrawn.

**Closing `filled` links.** A forwarder's own confirmation is written by the portal, whose login cannot revoke. So `syncFilled` runs at the top of the board and summary reads (and after a staff confirmation): requests of the same `batch_id` as a confirmed booking, with no live booking of their own, are revoked as `filled`.

## Retries

- **The quote request.** ShipLine's dialog makes one `batchId` and sends it with every attempt. `(batch_id, forwarder_id)` is unique, and a forwarder that already has a row for the batch is reported as it stands (`already_sent`, or `send_unconfirmed` when Front never answered) and is never emailed again. A Front refusal revokes that forwarder's row as `send_failed`; the others still go.
- **The booking request.** One live booking per request: a second call is `409 ALREADY_BOOKED`.
- **The contents email.** The body carries the booking's `rowVersion`; the route claims it (`UPDATE … WHERE row_version = ?`) before sending, so a retry of a call that did send is `409 STALE`.

## A booking and a new answer at the same moment

The booking route locks the request row, checks the option belongs to the current answer (`409 STALE_OPTION` otherwise), inserts the booking and bumps the request's `row_version`, in one transaction. The portal's save is an optimistic `UPDATE … WHERE row_version = ?`, so whichever commits first wins and the other is refused with the current state. A booked option never changes under staff.

## Contents by supplier

`loadForwarderQuoteForPdf` (in `orders.js`, the loader of the forwarder quote PDF) reads the attached shipment's lines: a draft by name, a booked container by reference (`S.documentSource`). `groupContentsBySupplier` groups them (trimmed, ignoring case, nameless last) with units, cartons, CBM and gross weight per line and per supplier. The email body is `contentsHtml`; unless `attachDocuments` is false, `generateDraftDocumentSet` builds a fresh forwarder quote (the combined PDF and CSV plus one pair per supplier) and all of them are attached. Each attached document gets a `draft_container_document_sends` row, so the draft's own history shows the email.

The lines listed are kept on the booking (`contents_json`, the shape a Request to send keeps). `contentsDrift` compares them with the container now using `src/lib/request-to-send-drift.js`: a line added or removed, units or LOT changed. Carton spec, CBM and weight do not count. ShipLine shows **CONTENTS CHANGED** and offers **RESEND UPDATED CONTENTS**. Attaching another container clears the snapshot.

## Deploy order

1. **Shipping**: the migrations (five tables; then the delivery option list, `stock_movements` and the wider `service`), the grants where `jportal_app` exists, the three templates, the seeded delivery options, the routes.
2. **The portal**: full deploy (routes on two Lambdas, two WAF rules, the grants file).
3. **ShipLine**.

Set `SUPPLIER_PORTAL_URL` (already set for the other portal links). No new environment variable, no new Lambda.

## The smoke check

`node tools/smoke-freight-quotes.js` walks the whole flow on the TEST database with both halves for real: the Express app in-process, and the portal's `availability` and `supplier` handlers in-process on a pool that logs in as the portal's own runtime login (`jportal_app`, from `../portal/.env`), so every portal statement runs under its column grants. Front is stubbed (nothing is emailed), the contents email goes without attachments (nothing is written to S3), and everything it writes is deleted at the end. It refuses to run unless both env files point at explorer-test. 51 checks, all passing on 2026-11-16, including the delivery option list, stock movements read by the portal under its column grant, and an email edited by staff.

## Not proven by the tests or the smoke check

- Front accepting the contents email with its attachments, and the time the whole call takes (PDF generation, S3 reads and the Front call share one API Gateway request).
- The widened WAF rules, and a forwarder contact registering through Cognito and seeing the Freight quotes tab (the smoke check fabricates the claims).

## Not included

- An email to staff when a forwarder answers (the tab's badge shows it).
- Automatic reminders to forwarders who have not answered.
- An email to the forwarder when staff cancel a booking, or to the others when a request is filled.
- Currency conversion, air freight, and more than one live booking per request.
