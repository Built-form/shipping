# Clearance instructions

The sheet our team send to the forwarder that handles a container's customs: one row per product type of a supplier, with the commercial invoice, the goods, their origin, cartons, weights, volume, commodity code, duty and value, and the container's freight cost shared between the rows by volume. Until 2026-11-24 it was typed by hand into a spreadsheet and emailed. It is now filled from what we hold, corrected in ShipLine, written here, emailed with the file attached and listed on the container's page on the supplier portal.

Built on the forwarder container page (`docs/forwarder-containers.md`): the customs holder is that page's `customs` duty, and the email is its "Email forwarder" with the sheet on it.

## Who does what

| Where | What |
|---|---|
| ShipLine (`src/components/freight/clearanceInstructions.ts`, `ClearanceInstructionsDialog.tsx`) | builds the rows from what it holds, lets a cell be corrected, shows the blanks. Opened from **Clearance instructions** on the container card's FORWARDER tab. **The commodity code and the duty are JFPRO's and cannot be typed** (the user, 2026-11-26): both cells are read-only; a wrong or missing one is changed in JFPRO (the product, or Duty rates) and the dialog opened again. Correcting a row's origin reads its duty from JFPRO again (`withJfproDuty`), and "USE Vn" takes neither cell from the earlier send (`withJfproCells`) |
| shipping (`src/lib/clearance-instructions.js`) | checks the rows (the check that decides), writes the file, keeps what was sent, remembers the duty by commodity code |
| supplier portal | lists what was sent on the container's page, for the company it was sent to, and gives the file again |

## Where each figure comes from (the user's decisions, 2026-11-24)

| Column | Source |
|---|---|
| Supplier, JFA / HW | the container's order lines |
| Commercial invoice number | read off the supplier's invoice on the INVOICES tab (the newest balance invoice with a number); else the number its packing list prints; else typed |
| Description of goods | the product's JFPRO **category** and the JF codes in the row ("Gauze Swabs JF1477 JF1475"); the product's name where it has no category. A product's category is the first of its set, as JFPRO itself shows one. Not the subcategory (the user, 2026-11-24). Editable |
| Country of origin | since 2026-11-25 (ShipLine): the **order's supplier's** country as JFPRO reads it (`GET /api/duty-rates` `suppliers[].countryCode`); only when JFPRO does not know that supplier, the product's main supplier's country, said on the row |
| Commodity code | JFPRO product. Not editable in ShipLine since 2026-11-26. This server still checks only its shape (8 or 10 digits), not that it is JFPRO's: it cannot read JFPRO's products |
| Cartons, net and gross weight | **the supplier's packing list when one was read for that product**, else ours: cartons from the order's units per carton, gross from the order's carton weight, net from JFPRO's net carton weight. The row says when the list's figures differ from ours |
| Volume | the order's carton spec (as the container card derives it) |
| Duty percentage | since 2026-11-25 (ShipLine): JFPRO's **Duty rates, Import (UK)** for the commodity code AND the origin (`GET /api/duty-rates?market=GB`); no rate there = blank, to be filled. A whole-number percent. `clearance_duty_rates` (last sent, by code only) is still written on a send but no longer read to fill the sheet: one code pays different duty from different countries |
| Value of goods | units × the order's unit price |
| Container cost | the freight booking's price when it is in dollars, else typed |
| Cost of shipping | the cost shared by volume: cost ÷ total CBM (to two places) is a rate to four places; a row's share is the rate × its CBM, to the dollar. The same sum in ShipLine and here, with the same figures in both test files |

**A row** is a supplier's products that share a JFPRO category AND a commodity code. Factory QC samples (`_FQC`) are not in the box and not on the sheet. Rows that say the same thing (supplier, invoice, company, description, origin, code, duty) are sent as one.

**What is not held is left blank, never guessed.** The sheet can be looked at and downloaded at any time, blanks and all. It is **sent only complete**: every cell filled, a commodity code of 8 or 10 digits, net weight not above gross, a duty between 0 and 100 (0 is a value).

## The file

The hand-made sheet, laid out to the character (`tools/test-clearance-instructions-lib.js` holds one of them as its golden case): a title row (`<container> - Clearance Instructions`), the heading row **with its own words** (its misspelling "Commerical" and the space before "Description" included: the forwarder knows the sheet by them), the rows, then `TOTAL CBM`, `Cost/CBM` and `TOTAL`. CRLF, no byte order mark. The container is named by the carrier's number when it has one, else by ours. Named `Clearance Instructions - <container>.csv`.

## Routes (staff sign-in)

| Route | Answer |
|---|---|
| `GET /api/v1/shipments/:id/clearance-instructions` | `{ shipmentId, title, filename, canSend, customs: { forwarderId, name } \| null, dutyRates: { code: percent }, sent: [{ id, version, forwarderId, forwarderName, title, filename, containerCostUsd, totalCbm, rows, csv, status, sentAt, sentByEmail, sentTo }] }`, newest first. Any staff user |
| `POST /api/v1/shipments/:id/clearance-instructions/preview` | `{ containerCostUsd, rows }` → `{ title, filename, csv, problems }`: the file as it would be sent, blanks and all. Writes nothing. Any staff user |
| `POST /api/v1/shipments/:id/forwarder-notice` with `clearance: { containerCostUsd, rows }` | sends it: the same email, key and row as "Email forwarder" (`docs/forwarder-containers.md`), to whoever holds the `customs` duty (or `forwarderId`, which must hold it), with the file attached, on the `freight_clearance_instructions` template. → `{ ok, outcome, noticeId, clearanceId, version }`. Admin and standard |

Refusals of the send, beside the notice's own: `422 CLEARANCE_INCOMPLETE` (`problems: [{ row, field, message }]`, `allowedNext: ['fill']`; checked before anything is read) · `409 NO_CUSTOMS_HOLDER` (`allowedNext: ['pick']`) · `409 NOT_THEIR_DUTY` (`kind: 'clearance'`).

A row: `{ supplier, invoiceNumber, company: 'JFA' | 'HW', description, origin, cartons, unitType, netKg, grossKg, cbm, commodityCode, dutyPercent, valueUsd, jfCodes }`. `jfCodes` is kept with what was sent and is not on the file.

## What is kept

- `shipment_clearance_instructions`: one row per email that carried a sheet (`notice_id` unique), written in the transaction that claims the send, **before Front is called**: its `version`, `rows_json` and the file itself (`csv`, text in the row: no bucket). Whether it went is its notice's `status`: `sent`, `sending` (Front never answered: it may or may not have gone) or `failed` (Front refused; the same key sends again over the same row).
- `clearance_duty_rates`: the duty each commodity code last went with, written only once the email has gone.
- The email is on the Email Receipts list as `freight_clearance_instructions` with nothing to confirm (`confirm_via 'none'`), and audited (`freight_container_notice`, with the sheet's id, version, filename and row count).

## The portal

`portal_forwarder_clearance_v` (definer view, `2026-11-24_21`): the sheets whose email is marked **sent**: `id, shipment_id, forwarder_id, version, filename, csv, sent_at`. Granted to the portal's login by `2026-11-24_22` (SELECT on the view only; approved by the user 2026-11-24). The portal lists a container's sheets on its page. Until 2026-11-26 it gave a file only to a contact of the company it was sent to; since then to a contact of **any** forwarder attached to the container (freight, delivery or customs), with the container's other papers (`docs/forwarder-containers.md`, "A container's documents"). Never to a payment-only forwarder.

## Rollout

shipping first (migrations `2026-11-24_20` tables, `_21` view, `_22` grant, `_23` email template, then deploy), then the portal, then ShipLine. Each side stands without the next: a ShipLine without the routes offers nothing (`getClearanceInstructions` answers null on a missing route or `NOT_MIGRATED`); a portal without the view lists nothing and logs it. Until `_23` has run, the send dialog opens on another Freight template: run the migration before the first send.

Rollback, once no deployed code reads them: `DROP VIEW portal_forwarder_clearance_v; DROP TABLE shipment_clearance_instructions, clearance_duty_rates`. Nothing else refers to them.

## Not done

- No tariff lookup: duty is what JFPRO's Duty rates hold for the code and the origin. A pair JFPRO lacks is filled there, not typed on the sheet.
- No check here that a sent code or duty is JFPRO's: ShipLine's cells are read-only, and this server checks their shape only.
- A supplier with two invoices in one container has both numbers on each of its rows; which line is on which invoice is not known.
- The value is units × our order price, not the invoice's own total.
- Never run against a database or Front: the routes are unit-tested against a scripted connection; the view and the grant have not been applied anywhere.
