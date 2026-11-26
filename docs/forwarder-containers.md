# A forwarder's container page

Once a container is real and belongs to a freight forwarder, that forwarder's contacts see it on the supplier portal under **Containers** (which replaced Freight payments). On its page they give its container number (ECN) and the expected loading and departure dates, upload their invoice, proof-of-loading photos and bill of lading, and see the payments we sent for it with the proof and the Acknowledge button. Staff see and decide all of it on the container card in ShipLine, in the **FORWARDER** tab.

Plan and decisions: 2026-11-23 (the user chose: views plus portal-owned tables; staff accept ECN and dates; a confirmed booking or a forwarder picked by hand; one staff-sent email; payments on the container's page).

Built in three slices, all covered here: **A** (the forwarder can see the container, with its payments), **B** (its number and dates, proposed on the portal and accepted here) and **C** (its invoice, loading photos and bill of lading).

Rules: `src/lib/forwarder-containers.js`. Routes: `src/services/forwarder-container-routes.js`. Tests: `tools/test-forwarder-containers-lib.js`, `tools/test-forwarder-container-routes.js` (both in `npm run test:unit`); `tools/test-portal-forwarder-container-view.js` and `tools/test-forwarder-container-races.js` (on the TEST database). Portal side: jportal README section 25. Freight quotes and bookings: `docs/freight-quotes.md`. A forwarder's payment: `docs/payment-proof-send.md`.

## Whose container it is

One rule, held twice: `ownerOf` (applied by the routes to rows they have locked) and the view `portal_forwarder_containers_v` (what the portal reads). The view test holds the two together on the same fixtures.

- Only a **real** container: not deleted, not merged away, with a number, and a **stored** stage from BOOKED on. The stage shown is the effective one (the later of stored and what the member orders justify), which is never earlier than the stored one and never an open or cancelled stage, so the two cannot disagree on this.
- Its forwarder is the one the API shows (`src/lib/forwarder-links.js`): the live booking's when it has one, else the pair on the container itself.
- A live booking must be **confirmed**. One still `requested` shows the container to nobody, a forwarder picked by hand included: the booking outranks the pair.
- The forwarder is on the list (an id, not typed text) and not removed.

| `why` nobody sees it | |
|---|---|
| `gone` | deleted or merged away |
| `not_real` | a draft, a plan, cancelled, or no number yet |
| `booking_not_confirmed` | its booking is still `requested` |
| `no_forwarder` | none on the container, no live booking |
| `forwarder_not_listed` | typed text, not a forwarder from the list |
| `forwarder_removed` | the forwarder was taken off the list |

**Picking a forwarder on a container by hand now shows that container to the forwarder's portal contacts.**

### Payment-only

A forwarder that is not the owner but was **sent** a payment with a charge on a real container sees that container for the payment only (`link_source = 'payment'`): its number, and nothing else of it. Two forwarders paid on one container each see it this way. A payment-only container takes no ECN, dates or files.

## Several forwarders on one container: three duties (2026-11-24)

A quote is often **port to port** (China to a UK port), and another company carries the container from the port to our warehouse; other forwarders are **end to end**; and the customs entry is made by one of them or by a broker that carries nothing. So a container has three **duties**, each with at most one holder. One forwarder may hold several. **Where this section and the ones below differ, this one is right**: the sections below were written when a container had one forwarder, and what they call "its forwarder" is the holder of the **freight** duty.

| Duty | Means | What its holder sends on the portal |
|---|---|---|
| `freight` | origin to the UK port | the number, loading date, departure date, loading photos, bill of lading (everything a forwarder sent before) |
| `delivery` | UK port to our warehouse | the **delivery date**, with a time when it has one |
| `customs` | the customs entry | a **draft customs entry**, for us to approve or send back |
| any duty | | its own invoice; the payments sent to it |

"End to end" is one forwarder holding `freight` and `delivery`; "port to port" is `freight` alone.

**Who holds a duty** is the rule above ("a live booking outranks the pick on the container"), per duty:

- `freight`: the live booking that covers freight (`freight_bookings.scope` `port_to_port` or `end_to_end`), else `shipments.forwarder_id`.
- `delivery`: the live booking that covers delivery (`scope` `end_to_end`), else `shipments.delivery_forwarder_id`.
- `customs`: `shipments.customs_forwarder_id`. Picked by hand only; any forwarder on the list.
- A covering booking still `requested` leaves **that** duty with nobody, its pick included; the other duties are not affected. The holder must be on the list and not removed. The container must be real.

Rules: `dutiesOf`, `heldBy`, `attachedOf`, `linkSourceOf`, `FIELD_DUTY`, `KIND_DUTY` in `src/lib/forwarder-containers.js` (`ownerOf` is still the freight duty). A forwarder that holds at least one duty is **attached**: it has one full row in the view. One that holds none and was sent a payment is payment-only, as before.

### Five rules that hold everywhere

1. **An older portal build can never write for a new kind of holder.** `link_source` stays `booking` / `container` on the freight holder's row and nobody else's; a forwarder that holds delivery and/or customs without freight has `link_source = 'duty'`. A portal build from before duties treats only the first two as rows it may write for, so to it a `duty` row is read-only.
2. **An older ShipLine can never decide what it cannot show: every decision names what it decides.** A proposal that carries a field beyond the original three: `claim`'s `expected` must have a key for each, and `reject` must send `fields` naming them all. A file of a kind beyond the original three: `accept` and `reject` must send its `kind`. Otherwise `409 CLIENT_TOO_OLD` (`allowedNext: ['reload']`). On the tab read the fields from before describe the freight holder only; another attached forwarder's things are **never** in the old `strays` (which an older build offers for dismissal), and the old `documents` lists bills of lading only.
3. **Leaving something out never grants a duty.** A booking made without `scope` is `port_to_port`.
4. **Who holds what is asked at the moment of deciding, under the locks**, at the claim and again at the finish. A change of pick or scope after the claim counts only if it took away a duty the proposal needs.
5. **A proposal is accepted or dismissed whole.** One that carries a field its forwarder no longer holds the duty for (it was end to end when it sent a delivery date) is dismiss-only; the tab read lists such fields in the proposal's `outside`.

### What changed, by place

- **Migrations** `2026-11-24_10` (`shipments.delivery_forwarder_id`, `.customs_forwarder_id`, each indexed; `freight_bookings.scope VARCHAR(16) NOT NULL DEFAULT 'port_to_port'` with a CHECK of three values; `freight_quote_requests.kind`; the two per-leg unique keys; `shipment_forwarder_documents.reference`; and, **last and the one step that is not additive**, `DROP INDEX uq_freight_bookings_live_shipment`, only once both new keys exist), `_11` (the view, re-created; a helper view `forwarder_duty_holders_v`, one row per container and duty that has a holder, **not** granted to the portal), `_12` (**one new grant**, approved by the user on 2026-11-24: `SELECT (kind)` on `freight_quote_requests` for the portal's login) and `_13` (the two delivery email templates, `INSERT IGNORE`). The view needs no grant: one re-created with `CREATE OR REPLACE` keeps its own.
- **The view** has four more columns, at the end: `does_freight`, `does_delivery`, `does_customs` (0 or 1: what this row's forwarder holds) and `delivery_date`. `booking_ref` and `freight_request_id` are the freight booking's and are on the freight holder's row only (another company is never handed a link to that quote); `sailing_date`, `port_from`, `port_to`, `container_size` are on every full row. A payment-only row has 0 in the three flags.
- **`finished`**: arrived, and more than 14 days after the **later** of the arrival and the latest delivery day held. While a delivery holder exists and no order holds a delivery date, not finished until the container is CLOSED (every order received). One answer for every row of the container.
- **`PATCH /api/v1/shipments/:id`**: `deliveryForwarderId`, `customsForwarderId` (a live forwarder's id, or `null`; `400 BAD_FIELD` with `field`). Audited. `GET /shipments/:id` and the compact entry of `GET /orders` answer `duties: { freight, delivery, customs }`, each `{ forwarderId, name, source, bookingId? }` or `{ forwarderId: null, why }`, and the read answers the two picks with their names (`deliveryForwarder`, `customsForwarder`). `forwarder`, `forwarderId`, `forwarderSource` and `booking` keep their meaning: the freight duty. In ShipLine the three are picked together in one small pop-up (since 2026-11-24): under the forwarder's name at the top of the container card, and from "Change" on the FORWARDER tab. A duty a live booking holds is shown there with where to change it, and nothing is sent for it. The Shipments page's details form keeps the freight forwarder's field only.
- **A booking's `scope`**: `docs/freight-quotes.md`.
- **The tab read** adds `duties` (each holder with its `name`, or `why`, `whyText` and the `picked` forwarder behind an empty one), `forwarders` (one section per attached forwarder, the freight holder first: `{ id, name, duties, sources, visibility, contacts, asks, lastNotice, details: { proposal, history }, uploads: { invoice, loadingPhotos, billOfLading, customsDraft }, toReview }`), `strays: { details, uploads }` (what a company that holds **nothing** here left undecided: dismiss only) and `keptDocuments` (every kept document, whatever its kind). `held` gains `deliveryDate` and `deliveryMixed`. A proposal gains `outside`; an upload and a list row gain `duties` (what that forwarder holds now).
- **The email** (`POST …/forwarder-notice`) takes `forwarderId`: which attached forwarder it goes to (left out: the only one attached, else the freight holder; `409 NOT_VISIBLE` when that forwarder holds nothing; `400 BAD_FIELD`). It asks each company only for what is its own (`containerAsks`): the delivery holder for the delivery date, the customs holder for the draft customs entry.
- **The list** (`/forwarder-submissions`) joins rows with `link_source` `booking`, `container` or `duty`; a row is `theirs` when its forwarder still holds every duty it needs.
- **Locks**: the order is the same. Step 2 takes **all** the container's live bookings; step 3 the forwarders named by the bookings **and by all three picks**.
- **A merge** keeps the survivor's own picks, all three, when it has any, else takes the loser's, all three: never a mix.

### The delivery date

One form, as text, to the minute: `YYYY-MM-DD` when the stored hour and minute are `00:00`, else `YYYY-MM-DDTHH:MM`. "No time" and midnight are the same thing (the column is a DATETIME and the card always stores a time); seconds never count. What we hold is produced **in SQL** by one expression (`deliverySelect`), used by the view and by a decision's locked read of the member orders; what comes in goes through `normDelivery`. The same rule with the same test vectors is in the portal (`src/lib/forwarderContainers.js`, `web/src/lib/forwarderContainers.ts`) and in ShipLine (`forwarderPortal.ts`). Written as `YYYY-MM-DD HH:MM:00`, the shape a hand-typed value has.

- **It is held per order.** `held.deliveryDate` is the one value every member order shares, else `null` with `held.deliveryMixed` (each distinct value, `null` for an order with none). The view's `delivery_date` is the shared value or NULL.
- **A claim records** which orders are on the container and what each holds for delivery; **a finish refuses** `CONTAINER_CHANGED` with `fields: ['members']` when an order joined or left, and `['deliveryDate']` when one order's value moved. A delivery date needs at least one member order to carry it (`NO_ORDERS`).
- **Accepting writes the one proposed value to every member order**, through `patchShipmentIn`'s `deliveryDate` (written to the orders only: no shipment column; `422` when the container is not booked). That is also how orders that differ are put right.
- **The order of dates** is decided at the claim and again at the finish: loading on or before departure, departure on or before **every** delivery day that would then be held. A pair is judged only when the proposal touches one of its sides. `DATES_OUT_OF_ORDER` carries `fields` (`['loadingDate', 'etd']`, `['etd', 'deliveryDate']` or `['loadingDate', 'deliveryDate']`).
- An accepted delivery date moves whatever reads the orders' delivery date (payment due dates, the calendar, the ARRIVED gate) exactly as typing it does. ShipLine's "45 days ahead" guard is for typing by hand and is not applied to a date staff accept. The card's own DELIVERY save and the order form are not changed.

### The draft customs entry

A fourth kind of file, `customs_draft` (PDF, PNG or JPEG; 15 MB), sent by the customs holder. Accepting it is **approving** it: the same four steps and locks as any file, filed as a `shipment_forwarder_documents` row with `kind = 'customs_draft'` under the forwarder that sent it. Approved drafts are listed newest approval first with who sent each; nothing is called "current", and one approved under a former customs holder stays listed under that forwarder's name. There is no final entry (C88 / MRN) in this work.

### The customs entry itself

A fifth kind, `customs_entry` (PDF, PNG or JPEG; 15 MB): the entry as customs took it, sent by the customs holder **with its MRN**. **It needs no draft before it** (some forwarders never send one), and our team accept it or send it back like any other file.

- **The MRN is required**: 18 characters after upper-casing and dropping spaces and hyphens, two digits, two letters, then fourteen letters or digits (`normMrn`, `mrnProblem`; the check character is not verified). The same rule and test vectors are in the portal.
- **Where it travels.** The portal keeps it in the upload's `note`, the one text column of that row its login may write, so this needed **no portal migration and no new grant**: for this kind the note IS the MRN and nothing else (the page shows an MRN field in place of a note). The tab read gives it as the upload's `reference` (and `note: null`).
- **On accept** it is written, in its one form, to `shipment_forwarder_documents.reference` (new, `2026-11-24_10`), and the document is listed with it. An entry whose note is not an MRN is refused before anything is copied: `409 BAD_REFERENCE` (`allowedNext: ['reject']`).
- The container email asks the customs holder for both: the draft "if you send one", and the entry with its MRN.

### A delivery booking (the delivery leg through Freight quotes)

A quote request has a **kind** (`docs/freight-quotes.md`, "A delivery request"): a `delivery` request goes to a UK haulier, and its booking has scope `delivery`: it covers the delivery duty alone. So a container can hold **two live bookings, one per leg**: one that covers the freight and one that covers the delivery (an end-to-end booking covers both, and leaves room for neither).

- Two unique keys replace the one of 2026-11-20: `uq_freight_bookings_live_freight` (scope `port_to_port` or `end_to_end`) and `uq_freight_bookings_live_delivery` (scope `end_to_end` or `delivery`). The check that answers `409 CONTAINER_TAKEN` asks whether another live booking on the container covers a duty the new one covers; the keys settle two attaches at once.
- `dutiesOf` is unchanged: per duty, the live booking that covers it. A delivery booking still `requested` leaves the delivery with nobody, the delivery pick included; the freight is not its to hold up.
- Its forwarder holds delivery and not freight, so its row in the view is a `duty` row, as for a hand pick: rule 1 is untouched.
- The shipment read answers `deliveryBooking: { id, status, bookingRef, forwarderId, forwarder } | null` beside `booking` (which stays the freight booking). Two joins read the two bookings apart, so they never blend into one row.
- A merge moves a loser's live booking only when no live booking of the survivor covers a duty it covers. A booking attached to the survivor at the same moment fails the **whole** merge (`409 RETRY`), nothing half-moved.

New refusals, all `409`: `BAD_REFERENCE` (above), `NOT_THEIR_DUTY` (the forwarder holds something on the container, but not what `fields` or `kind` need; `allowedNext: ['reject']`) and `CLIENT_TOO_OLD` (rule 2; `allowedNext: ['reload']`).

### Rolling back (all user-triggered)

Released together, shipping → portal → ShipLine: ShipLine is the only place a delivery or customs holder, or an end-to-end booking, can be made, so until it is out every row is a freight holder and all three apps behave as before.

| Rolling back | Allowed when | What then happens |
|---|---|---|
| ShipLine | any time | it cannot make duties; `CLIENT_TOO_OLD` stops it deciding what it cannot show |
| the portal | any time | rule 1: delivery-only and customs-only companies see their containers read-only; a freight holder keeps its page |
| the portal, once a **delivery request** exists | **not at all**: fix forward | an older build shows a UK haulier the sea-freight form (loading date, sailing date, transit of at least a day) |
| shipping | only after ShipLine is back **and** `node tools/list-forwarder-duties.js` lists nothing (exit 0): no pick, no scope other than `port_to_port`, no undecided delivery or customs submission, no container with two live bookings, **no kept customs document (an approved draft, or an entry with its MRN), and no delivery request in any state**. Once either of the last two exists shipping is not rolled back below this release: older code lists every kept document as a bill of lading, and reads a delivery request as a freight request (its booking would make the haulier the container's freight forwarder, with no key left behind "one live booking per container"). Neither kind of record is deleted to make a rollback possible | the new columns and the view **stay**: they are additive, and every older build reads them as before. Claims in flight lapse in 10 minutes |

### Tests

`npm run test:unit` covers the rules and the routes with scripted statements. **The release gate runs on TEST after its migration**, before anything is deployed: `tools/test-portal-forwarder-container-view.js` (the view against `dutiesOf` on the same fixtures, row for row and flag for flag; the delivery date's form; `finished`) and `tools/test-forwarder-container-races.js` (the portal's own statements and the decisions on two connections: a delivery pick, a booking's scope and a holder's removal against a write and against a claim; a finish after the duty moved, after a delivery date was typed on one order, after an order joined or left; one person who is a contact of two companies; the statement of a portal build from before duties). If the portal's statement does not **wait** on the regrouped view, the release stops there. Not included: more than one holder per duty, a third leg, proof-of-delivery uploads, a forwarder seeing another forwarder's files (since 2026-11-26 an accepted bill of lading is the one exception: "A container's documents" below), and the delivery leg through Freight quotes (a second live booking per container: planned, not built).

## The view

`portal_forwarder_containers_v` (`2026-11-23_11`; re-created by `2026-11-24_11` with the duty flags and the delivery date, see above), `SQL SECURITY DEFINER`, `GRANT SELECT` to the portal's login (`_12`). The portal's login gains no right on `shipments`, `orders` or `freight_bookings`.

One row per container and forwarder; a full row outranks a payment-only one.

| Column | |
|---|---|
| `shipment_id`, `forwarder_id` | who and which. **The view lists every forwarder's containers: the portal filters by the caller's forwarder ids and joins its own rows on both ids** |
| `link_source` | `booking` \| `container` (full) \| `payment` |
| `reference`, `mode` | our number; SEA or AIR |
| `stage`, `ecn`, `etd`, `eta`, `vessel_name` | as the API shows a booked container (`rowToShipment`): the member orders win over the stored copies. ECN is the orders' `external_container_number` (`awb_number` first for AIR) |
| `loading_date` | `shipments.loading_date` (new, `2026-11-23_10`) |
| `arrived_on`, `finished` | `shipments.ata`, else the latest member `arrived_date`; `finished` = more than 14 days after it, on the server's UTC date |
| `origin_port`, `booking_ref` | the container's; the booking's reference first |
| `sailing_date`, `port_from`, `port_to`, `container_size`, `freight_request_id` | the booked option, when it came from a booking |

A payment-only row carries the first five and NULL in the rest. No column says anything of a supplier, a line, a quantity or a price.

`portal_forwarder_payment_lines_v` gained `shipment_id`: the charge's own shipment id, else the live container whose number the charge carries.

## Routes

Under the staff sign-in (`/api/v1/{proxy+}`). Reading: any staff user. Emailing and deciding: **admin and standard** (`403 CANNOT_DECIDE` otherwise; the read's `canDecide` says so first, and ShipLine never calls a route that would answer 403, because it treats a 403 as a sign-out). `503 NOT_MIGRATED` without this feature's tables.

### `GET /api/v1/shipments/:id/forwarder-portal`

What the FORWARDER tab shows.

```json
{ "shipmentId": 1214, "reference": "339", "mode": "SEA",
  "forwarder": { "id": 4, "name": "…", "source": "booking" },
  "visibility": { "mode": "full", "why": null }, "whyText": null,
  "contacts": [{ "name": "Mei", "email": "mei@…" }],
  "held": { "ecn": null, "loadingDate": null, "etd": "2026-10-21", "eta": null, "bookingRef": "hello", "stage": "BOOKED" },
  "asks": ["ecn", "loadingDate", "invoice", "photos", "billOfLading"],
  "portalUrl": "https://…/freight-containers/1214",
  "notices": [{ "id": 5, "status": "sent", "forwarderId": 4, "sentTo": ["…"], "subject": "…", "sentByEmail": "…", "createdAt": "…", "sentAt": "…" }],
  "payments": [{ "paymentId": 16, "forwarderId": 4, "forwarderName": "…", "paidOn": "2026-09-30", "amount": 9375, "currency": "USD",
                 "share": 4500, "lines": [{ "kind": "freight", "amount": 4500 }], "alsoPays": ["308"], "sentAt": "…",
                 "opened": [{ "email": "…", "name": "…", "firstAt": "…", "lastAt": "…", "count": 1 }],
                 "acknowledgement": { "byName": "…", "byEmail": "…", "at": "…", "current": true } }],
  "paymentOnly": [{ "forwarderId": 3, "forwarderName": "DCG" }],
  "canDecide": true }
```

- `forwarder`: the one the card shows, whether or not it may see the container. `visibility.mode` is `full` or `none` for **that** forwarder; `why` and `whyText` say why not.
- `contacts`: who can sign in for it. None: nobody can.
- `held`: what we hold, as the card shows it. `asks`: what the email would ask for.
- `payments`: payments **sent** to a forwarder that have a charge on this container (the portal's own views). `share` is this container's part, `alsoPays` the other containers of the same transfer. `opened` and `acknowledgement` are that forwarder's, by forwarder id; `current` is false when the acknowledgement was of an earlier figure, container or proof.
- `paymentOnly`: forwarders that see this container for a payment only.

### `POST /api/v1/shipments/:id/forwarder-notice`

`{ sendKey, to?, subject?, body? }` → `{ ok, outcome, noticeId }`. Emails the forwarder's contacts the link to the container's page (template `freight_container_details`, category `Freight`; tokens `{forwarder} {containerName} {bookingRef}`). The fixed block under the body lists the container, what we do not hold yet and the papers, the **Open the container** button, and how to register. That page needs a sign-in, so the recipients are pre-approved on the portal first.

With `clearance: { containerCostUsd, rows }` (2026-11-24) the same email carries the container's **clearance instructions** to whoever holds the `customs` duty, attached as a CSV and kept: `docs/clearance-instructions.md`.

- **Whose container it is, is decided under a lock** on the shipment row and its live booking, not on the page's read.
- `to` can only **untick** contacts: `400 NOT_A_CONTACT` (`notContacts`) for any other address. Nobody left: `409 NO_CONTACTS`.
- `409 NOT_VISIBLE` (`why`) when the container is on no forwarder's page; `409 PORTAL_URL_UNSET`.
- **`sendKey`** is a uuid the dialog makes once and sends with every attempt. The row (`freight_container_notices`) is written `sending` and committed before Front is called.

| The key already has a row | Answer |
|---|---|
| `sent` | `outcome: 'already_sent'`, nobody is emailed |
| `sending` (Front never answered) | `outcome: 'send_unconfirmed'`, and that key is **never sent again**: check Front, then open the dialog again for a new key |
| `failed` (Front refused: `502 FRONT_ERROR`) | sent again on the same row |
| for another container or forwarder | `409 SEND_KEY_CLASH` |

Exactly-once is not claimed: a send Front took whose answer was lost shows as unconfirmed.

On ShipLine's Email Receipts page it is a portal email (`confirm_via 'portal'`), confirmed once **that forwarder** sends something complete for **that container** at or after the email went: its number or dates, or a file that arrived (`forwarder_container_answered` in `src/lib/email-receipt.js`).

Audit: `freight_container_notice` `sent`.

### The forwarder's proposal on the tab read

`GET …/forwarder-portal` also answers, from slice B:

```json
{ "unavailable": false,
  "details": { "proposal": { "id": 51, "forwarderId": 4, "forwarderName": "…", "status": "pending", "waiting": true, "live": true, "actionable": true,
                             "requested": { "ecn": "CSQU3054383", "loadingDate": "2026-10-18" }, "base": { "etd": "2026-10-21" },
                             "note": "…", "byName": "Mei", "byEmail": "mei@…", "at": "…",
                             "claimedBy": null, "claimedAt": null, "decidedNote": null, "decidedBy": null, "decidedAt": null },
               "history": [], "strays": [] },
  "toReview": 1 }
```

- `proposal`: the **live** proposal of the container's own forwarder: its newest row, whatever became of it (so a decided newest one never brings an older undecided one back). `history`: that forwarder's older ones. `strays`: undecided live proposals of a forwarder the container is **no longer with**: they can be dismissed, never accepted, and are not counted.
- `status` reads `applying` only while a claim stands; one that lapsed (10 minutes) reads `pending` again.
- `requested` holds only what differed from what we held when it was sent; `base` what we held then.
- `unavailable: true` only when the portal's tables do not exist on this server (errno 1146 or 1049). A missing grant or any other fault is an error, never an empty answer.

## A container's number and dates (slice B)

A forwarder **proposes** the container's number (an air waybill number by air), the expected loading date and the expected departure date on the portal; nothing is on the container until staff accept it. The portal writes one row of its `forwarder_container_details` per send, insert only: the live proposal is the newest row of a container and forwarder.

Accepting is three steps:

| Step | Where | What it guarantees |
|---|---|---|
| `POST /api/v1/forwarder-details/:id/claim` `{ expected: { ecn, loadingDate, etd } }` → `{ claimToken, expiresInSeconds, requested, held }` | server, one transaction under the locks | the proposal is the live one and undecided, **no other accept is in progress on that container**, the container is still that forwarder's, and it holds exactly what the card showed (`expected`). A number needs at least one member order to carry it and must pass its check (ISO 6346 shape and check digit by sea; 5 to 40 characters by air); the resulting loading date is on or before the departure date. Only then is anything written anywhere. A claim nobody finished is put back first |
| the number | ShipLine, in the browser | written through `App.prepareEcnWrite` (the duplicate block, the confirmation, the date reset), which exists only there. **This step is not fenced by the server**: see Limits |
| `POST /api/v1/forwarder-details/:id/finish` `{ claimToken, ecnReset? }` → `{ ok, status: 'applied', requested }` | server, one transaction under the same locks | the claim is this token's and still stands; the container is still theirs; the number is on **every** member order (an empty container never passes); nothing else moved since the claim; the dates are in order. Then the dates are written by `PATCH /shipments/:id`'s own code (`patchShipmentIn`, `src/services/shipment-routes.js`) and the proposal is marked accepted. A refusal writes nothing |

`ecnReset: { etd }` is the departure date ShipLine's own reset wrote when the accept **replaced** an existing number (taken from the patch it made, never read back). It is the one difference from what the claim saw that `finish` allows, and it is ignored when no number was replaced.

`POST …/release` `{ claimToken }` gives a claim up (the number check was cancelled or refused, or `finish` was refused): the proposal waits again at once. An old token does nothing. `POST …/reject` `{ note }` (3 to 500 characters; the forwarder reads it) sends a proposal back; it is allowed on a proposal whose container is no longer its forwarder's (a dismissal), and takes no ownership lock.

Refusals are `409` with `code` and `allowedNext` (`['reload']`, `['reject']` or `['retry']`), and where they apply `fields`, `held`, `claimedBy`, `claimedAt`, `why`:

| Code | Means |
|---|---|
| `CLAIM_IN_PROGRESS` | someone is accepting a proposal on this container now (`claimedBy`, `claimedAt`) |
| `ALREADY_DECIDED`, `SUPERSEDED` | accepted or sent back already; a newer proposal exists |
| `NOT_THEIR_CONTAINER` | no longer that forwarder's (`why`): it can only be dismissed |
| `CONTAINER_CHANGED` | the container holds something else than the card showed, or than the claim saw (`fields`, `held`) |
| `NO_ORDERS`, `BAD_PROPOSAL`, `EMPTY_PROPOSAL`, `DATES_OUT_OF_ORDER` | it cannot be accepted as it is: send it back |
| `NOT_ON_CONTAINER` | the number is not on every member order (`finish`) |
| `CLAIM_LOST` | the token is not the standing claim's (it lapsed, was released, or another tab claimed) |
| `404 NO_SUCH_PROPOSAL`, `503 RETRY` | no such row; a deadlock that was tried twice |

Audit: `forwarder_container_details` `applied` (what the claim saw, what was asked, the reset) and `rejected` (the note).

### Locks

Every decision's transaction **opens with its locks**, in this order, and makes its first plain read only after the last of them (under REPEATABLE READ a plain read made earlier would fix the snapshot before the locks were held):

1. the `shipments` row, `FOR UPDATE`;
2. its live booking rows, `FOR UPDATE OF fb`;
3. the forwarders those name, `FOR SHARE` (so a forwarder being removed from the list waits too);
4. for a claim and a finish, the member orders, `FOR UPDATE` on the `shipment_id` index (an order joining or leaving the container waits);
5. the container's proposal rows.

The routes that change ownership write those same rows, so each is serialised against a decision. The legacy order route locks in another order, so a deadlock is possible: a decision is tried once more on errno 1213, then answers `503 RETRY`. Which proposal's container it is, is read before the transaction (a plain read).

`tools/test-forwarder-container-races.js` holds this on a real server with two connections (TEST only; its fixtures are removed): the portal's `INSERT … SELECT` and a claim each **wait** for an uncommitted change of forwarder, booking, order number or membership and then read its result; a change that meets a claim in progress waits for it; a whole accept; a claim that lapses; a deadlock tried once more. For a forwarder's files it runs the accept route itself against the portal's own withdrawal in each order (the accept waits for an uncommitted withdrawal and is then refused with nothing copied; a withdrawal waits for an accept that holds its locks and then changes nothing; one that lands between our copy and the decision is refused and leaves the copy the sweep removes), against a change of forwarder, and as two accepts at once on two connections; both buckets are a stand-in there, so no file is sent. The dates a finish writes go through a stand-in there (PATCH's own body needs the whole app): the real one is exercised through the local API.

### What waits, across containers

- `GET /api/v1/forwarder-submissions` → `{ data, unavailable, canDecide }`: what waits for our team on a container that is still its forwarder's, newest first (200 at most). With `?status=all`: everything, each row saying `waiting`, `live` (false: a newer one replaced it), `theirs` (false: the container is no longer that forwarder's) and `actionable`.
- `GET /api/v1/forwarder-submissions/summary` → `{ toReview, unavailable }`: the count ShipLine adds to its Freight badge.

A row: `{ kind: 'details', id, shipmentId, reference, mode, forwarderId, forwarderName, status, waiting, live, theirs, actionable, requested, byName, byEmail, at, decidedAt }`.

### The loading date

`shipments.loading_date` is a header field of `PATCH /api/v1/shipments/:id` (`loadingDate`, `400 BAD_DATE`), on the shipment only (no order carries one), answered by `GET /shipments/:id` and in the compact entry of `GET /orders`' `shipments` side-map. `tools/test-shipments-api.js` covers the PATCH.

### Limits

- **The number's write is not a server transaction.** If the container changes forwarder, or is merged or deleted, in the seconds between the claim and ShipLine's write, the number is still written and the proposal is then not marked accepted. That is what typing the same number by hand produces today.
- **An accept cut short can leave the number on the container without the dates.** The tab then shows the number as already on the container and the proposal as still to accept.
- **Accepting a number that replaces one** resets the container's dates as it does today; if that leaves the proposed loading date after the departure date, `finish` refuses and staff set the dates by hand.
- **Accepting a container number registers it for tracking** (the billed ShipsGo event), exactly as typing one does.

## A forwarder's files (slice C)

On the container's page a forwarder uploads its **invoice**, **proof-of-loading photos** and **bill of lading**. Each is one row of the portal's `forwarder_uploads`, the file in the portal's bucket at `final_key` (`files/fwd/<upload id>/<attempt>.<ext>`): a key of one finalise attempt's own, which never changes once the row is `sent`. Our team **accept** each (it is copied to our bucket and filed) or **send it back** with a note the forwarder reads. Routes: `src/services/forwarder-upload-routes.js`; tests: `tools/test-forwarder-upload-routes.js`.

| Route | Does |
|---|---|
| `GET /api/v1/forwarder-uploads/:id/file` | `{ url, filename, contentType, expiresIn }`: a 300-second link to the file as the forwarder sent it, in the portal's bucket |
| `POST /api/v1/forwarder-uploads/:id/accept` `{ preset? }` | files it (below). `{ filed: { as, id }, upload, replayed }` |
| `POST /api/v1/forwarder-uploads/:id/reject` `{ note }` | sends it back (3 to 500 characters). Allowed on a file whose container is no longer its forwarder's (a dismissal) |
| `DELETE /api/v1/forwarder-documents/:id` | removes a filed bill of lading: an admin, or whoever accepted it (`409 NOT_YOURS_TO_REMOVE` otherwise, never a 403) |

**Accepting**, in the order the telex release's accept taught us:

1. **The rules first.** The row is `sent` and undecided, and the portal's view still gives its container to that forwarder. Asked again for one already `received`, it answers what was filed (`replayed: true`; `filed: null` when that was deleted since).
2. **Where our copy goes, then the copy.** A short transaction locks the row and reads it again (step 1's read was a plain one): taken back or sent back since, it is refused there and **nothing is copied**; received since by another request, this caller is answered with what that one filed. It then gives the row its `filed_key` if it has none: the kind's folder in our bucket, a random part made **once** and kept, and a clean name with the type's extension. Then, outside any transaction, the object is read from the portal's bucket **by its key, with no ETag condition** (`s3_etag` on the row is the staging object's, and S3 does not promise a copy keeps it), checked (its first bytes are the type on the row and one its kind takes; its size is the size on the row and within the kind's cap) and written to our bucket at `filed_key`. A retry writes over its own copy and never makes a second; the address cannot be worked out from the upload's id and name.
3. **One transaction under the locks** (the shipment row, its live booking, their forwarders, then the upload's row): still `sent` and undecided **with the same `final_key`, `s3_etag` and `filed_key`** (`409 FILE_CHANGED` otherwise); the container still that forwarder's (`409 NOT_THEIR_CONTAINER`, `allowedNext: ['reject']`); the target row inserted with `forwarder_upload_id` (unique); the portal's row marked `received` with `filed_id`. **Two accepts at once file once, and both are answered `200` with the same filed row** (the second with `replayed: true`): the row's lock serialises them, so the second finds the row already `received` when it gets the locks, and is told what the first filed. The unique key is the line behind that.
4. An invoice's usual reading starts after the response.

| Kind | Filed as |
|---|---|
| `invoice` (PDF, PNG, JPEG; 10 MB) | a `shipment_payment_documents` row, `balance_invoice`, on the shipment, under the name that forwarder is **paid under** on that container (the newest live `payment_extras` row with its id), else its name on the Forwarders list; with `forwarder_upload_id` and `forwarder_id`. Refused `409 FILING_NAME_CLASH` when that name is exactly a supplier's with goods on board, and `409 DUPLICATE_DOCUMENT` by the store's own rule (same name and size on the shipment). ShipLine puts a document that carries a forwarder id in that forwarder's box by the id first |
| `loading_photo` (PNG, JPEG; 15 MB) | a `container_photos` row under the container's **present** number (the number it was sent for stays in the audit), preset "Proof of loading" unless the call names another; its description is the forwarder's note, else the preset |
| `bill_of_lading` (PDF, PNG, JPEG; 15 MB) | a `shipment_forwarder_documents` row. Never `shipments.bl_number`, which drives a payment flag |

Refusals are `409` with `code` and `allowedNext`: `NO_SUCH_UPLOAD` (404), `WITHDRAWN`, `NOT_SENT`, `ALREADY_DECIDED`, `NOT_THEIR_CONTAINER`, `FILE_UNAVAILABLE`, `FILE_NOT_ITS_TYPE`, `FILE_SIZE_CHANGED`, `FILE_TOO_LARGE`, `FILE_CHANGED`, `FILING_NAME_CLASH` (`name`), `DUPLICATE_DOCUMENT` (`existingId`), `503 RETRY`, `503 PORTAL_UNAVAILABLE` (the portal's table is not on this server).

Audit: the filed row's own `create`, and `forwarder_upload` `accept` / `reject`; `shipment_forwarder_document` `delete`.

**On the tab read** (`GET …/forwarder-portal`): `uploads: { invoice, loadingPhotos, billOfLading, strays }` (what was sent, newest first, each `{ id, kind, filename, contentType, fileSize, note, byName, byEmail, sentAt, state, waiting, theirs, actionable, decidedNote, decidedBy, decidedAt, filedId }`; `state` is `open`, `received`, `rejected` or `withdrawn`; never a storage key), `documents` (the bills of lading kept, each with `canRemove`), and `toReview` counts the files that wait with any proposal. **In the list** (`/forwarder-submissions`) a file is a row whose `kind` is its own, with `filename` and `fileSize`; the summary counts them.

**Bucket rights** (`serverless.yml`): `s3:GetObject` on the portal bucket's `files/fwd/*`. Accepted files follow the PO bucket's house rule (an unguessable public address).

**Stray copies.** A crash between the copy and the transaction leaves at most one object per upload in our bucket, at the row's `filed_key`. `tools/sweep-forwarder-copies.js` lists (and with `--delete --confirm-host` removes) the copy of an upload that can no longer be accepted: rejected more than 7 days ago, or withdrawn more than 7 days ago. An undecided upload's copy is never touched. It runs on no schedule.

**Older containers (optional).** A container made before this page knows its forwarder only through its payments, so that forwarder sees it for the payment alone. `tools/backfill-container-forwarders.js` gives such a container its forwarder: a real container with no forwarder (linked or typed), no live freight booking, not yet arrived, whose forwarder charges (`payment_extras.rides_with = 'shipment'`) all name one forwarder from the list. A container with an unlinked charge, or charges to two forwarders, is listed and left alone. Dry run by default; `--apply --confirm-host <DB_HOST>` writes `shipments.forwarder_id` and `.forwarder` with one audit row each. The consequence is the one the picker states: that forwarder's portal contacts then see the container in full and can send its number, dates and files.

**Not exercised on a real bucket yet** (2026-11-23): upload, finalise, view and accept need the portal's bucket and its deploy. Covered by scripted-S3 unit tests on both sides; the first real run belongs on the test stage after its deploy (the portal's `tools/smoke-forwarder-uploads.js`).

## A container's documents (2026-11-26)

A forwarder working on a container downloads its papers from the container's page on the portal: the **bill of lading**, the suppliers' **packing lists** and **commercial invoices**, and the **clearance instructions** our team sent (`docs/clearance-instructions.md`). The user's decisions, 2026-11-26: every forwarder **attached** to the container (holding freight, delivery or customs) gets all four, the suppliers' names and our prices included; a payment-only forwarder gets none; the clearance sheet is the one staff sent, never one generated on the portal. Portal side: jportal README section 25, "Documents".

`portal_forwarder_documents_v` (`2026-11-26_10`, `SQL SECURITY DEFINER`; `GRANT SELECT` to the portal's login by `_11`, approved by the user 2026-11-26): one row per file our team **hold**. An upload still waiting on the SUPPLIER PORTAL or FORWARDER tab is not in it.

| `kind` | Rows |
|---|---|
| `bill_of_lading` | `shipment_forwarder_documents`, live, `kind = 'bill_of_lading'` (never a customs draft or entry) |
| `telex_release` | `shipment_telex_releases`, live |
| `packing_list` | `packing_lists`, live and not superseded, filed on the container's number, or on the draft that was **converted** into it. A draft that was never closed (`draftsBehind` in `src/services/packing-review.js` follows those in code) is not followed |
| `commercial_invoice` | `shipment_payment_documents`: `balance_invoice`, on a shipment, `forwarder_id IS NULL`, and filed under a supplier **with goods on that shipment** (a live order of the shipment, or of its `shipment_lines`, whose supplier is that name, trimmed, under the tables' own case-insensitive comparison). So a forwarder's invoice, also one uploaded by hand under the forwarder's name, a proof of payment and a QC invoice are never in it |

Columns: `shipment_id, kind, document_id, supplier_name, filename, s3_key, content_type, file_size, filed_at`. **No forwarder**: the view says what a container's papers are, and the portal decides who may have them (a contact of a forwarder with a full row of `portal_forwarder_containers_v` for that shipment). A file is named by its **key** in `PO_DOCS_BUCKET`, never by its public address: the portal's role may read four folders of that bucket (`forwarder-documents/`, `telex-releases/`, `packing-lists/`, `shipment-payments/`) and signs a 300-second download per click.

Checked read-only against the live data on 2026-11-26 (the view's own SELECT, not the view): 105 packing lists on 45 containers, 94 invoices on 44, 24 telex releases on 10, no accepted bill of lading yet. Of 130 live balance invoices on shipments, 36 are left out by the goods-on-board rule: 29 filed under "DCG" (a forwarder) and 7 under a supplier name that no order of that shipment carries. The second kind is worth a look if a forwarder reports a missing invoice: refile it under the supplier's name as the orders spell it.

Tests: `tools/test-view-collations-lib.js` holds the view's shape (every `kind` wrapped for both session collations, a key and never an address, the exclusions) and the grant. **The view has been applied nowhere**: `tools/migrate.js --apply` checks it under both collations when it runs; there is no fixture test on a database for it yet.

Rollback, once no deployed portal reads it: `DROP VIEW portal_forwarder_documents_v`. Nothing else refers to it.

## Tables (this schema, `2026-11-23_10`)

| | |
|---|---|
| `shipments.loading_date` | the expected loading date. On the shipment only, as `bl_number` is |
| `freight_container_notices` | one row per email: `shipment_id`, `shipment_reference`, `forwarder_id`, `send_key` (unique), `status`, `sent_to`, `subject`, the Front ids, `sent_by_email`, `sent_at` |
| `shipment_forwarder_documents` | a forwarder's bill of lading file accepted from the portal. Its own table: not a telex release, not an invoice, not a generated document. Removed rows are kept (`deleted_at`) |
| `shipment_payment_documents.forwarder_upload_id`, `.forwarder_id`; `container_photos.forwarder_upload_id` | the portal upload an accepted invoice or photo came from (unique: one upload files once), and whose invoice it is. Both are answered with the document and the photo |

Shipment ids are not durable across a re-seed: `tools/backfill-shipments.js` refuses to seed an emptied `shipments` while these tables still hold rows.

## Rollout (test, then prod; all user-triggered)

1. **Shipping**: `bash deploy.sh <stage>` runs the four `2026-11-23` migrations, then deploys. Nothing changes for staff or forwarders.
2. **Portal, migration and grants only**, then its smoke tool. No page yet.
3. **ShipLine**: the FORWARDER tab.
4. **Portal deploy**: the Containers page goes live last, so nothing can be sent before staff can review it.

Taking it back: roll the **portal** back first (that removes the routes, which is what stops intake); what was sent stays reviewable in ShipLine.

`tools/test-portal-forwarder-views.js` now expects `shipment_id` among the columns of a payment line.

## Not included

- An email to staff when a forwarder sends something; automatic reminders; a page that needs no sign-in.
- A forwarder paid on a container sending its ECN, dates or files (they see the payment only until staff pick them or book).
- A server check on the number's write itself (a claim token on the order route).
- A forwarder clearing a value we hold: a proposal can only set one.
- Checking a forwarder's invoice against its booking.
