# Request to send: a link to the supplier portal, attached only as a fallback

A **Request to send** (a `draft_container_documents` row of type `supplier-quote`: one supplier's packing list for one container, PDF and CSV) used to be emailed with both files attached and a "Confirm receipt" button. It now goes out the way the PO and the signed PI do (`docs/po-portal-send.md`): the email carries a button to the supplier portal and **no attachment**, every recipient is **pre-approved** as a portal user for that supplier, and opening the file on the portal is what confirms the email.

Unlike the PO send, this one has a fallback. The portal lists a Request to send only to users linked to a live supplier named **exactly** as the document's `supplier` (portal README section 10). When no such supplier exists the link would open an empty page, so the request goes out the old way: PDF and CSV attached, confirm button and all.

Forwarder quotes (`forwarder-quote`, `quote`) are untouched: always attached.

Code: `src/lib/request-to-send-portal.js` (pure rules), `src/services/request-to-send-portal.js` (SQL), both tested by `tools/test-request-to-send-portal-lib.js` in `npm run test:unit`; the routes in `src/handlers/orders.js`. Portal side: `portal/src/handlers/sentDocuments.js` (`listSupplierQuotes`, `markSupplierQuoteViewed`), `portal/web/src/screens/RequestsToSendScreen.tsx`.

## What the supplier is told the container is called

A draft's name is internal. It is ShipLine's stamp followed by whatever the team typed: other suppliers' names, a departure date long out of date (`DRAFT-SEA-260923-141617 - 333 Sunmed Kingphar Foreu ETD 13 Oct`). A supplier is shown the container's **number** and nothing else (`supplierRef` in `src/lib/request-to-send-portal.js`, over `parseReference` and `parseNameHint`, the rule booking itself reads the number with):

| Name on the document | Shown |
|---|---|
| `328`, `121. Air Freight` (a booked container: its reference) | as it is |
| `… - 333 Sunmed Kingphar Foreu ETD 13 Oct` | `333` |
| `… - 126. Air Freight`, `DRAFT-AIR-… - 127` | `126. Air Freight`, `127. Air Freight` |
| no number in it (`… - TAM Container 2`, `DRAFT-SEA-260903-100903`) | nothing of the name |

Where it applies, for a Request to send only (a forwarder's documents and emails keep the name):

- **The PDF**: "Ref:" prints the number (empty when there is none). `draft_container_documents.draft_container_name` still stores the whole name: it is how the document is found again.
- **Its files**: `request-to-send-<supplier's letters>-<number>-v<n>.pdf` / `.csv` (`requestNames`). The file's address is what the portal hands the supplier and what an attachment is called.
- **The email**: `{draftContainerName}` is the number (`emailName`; the word "shipment" when there is none), in the stored template and in a subject or body the caller sends. And any occurrence of the whole name in that subject or body is replaced the same way (`hideInternalName`): ShipLine's dialog fills the token itself, and a ShipLine from before 2026-11-13 fills it with the whole name. `GET …/:id/delivery` returns `supplierRef` so the dialog shows the subject as it will go.
- **The portal** lists the request by the same number (its own copy of the rule, `supplierRefOf` in the portal's `src/lib/sentDocuments.js`, pinned to the same table of names in both repos' tests) and never sends the name to the browser.

**Documents generated before 2026-11-13 are unchanged**: the whole name is still printed inside the PDF and is still part of the file's address, which is what the supplier's browser opens and what the download is called. Only generating the request again produces a clean one.

## Which way it goes

`resolveDelivery(conn, doc)` → `{ delivery, reason, portalUrl, registerUrl, suppliers }`.

| `delivery` | `reason` | When |
|---|---|---|
| `portal` | `null` | a Request to send, `SUPPLIER_PORTAL_URL` set, the portal schema is there, and a live `suppliers` row has the document's exact supplier name (`utf8mb4_0900_bin`) |
| `attached` | `not_request_to_send` | a forwarder quote or a plain quote |
| `attached` | `portal_url_unset` | `SUPPLIER_PORTAL_URL` is not configured |
| `attached` | `portal_unavailable` | no portal database on this server |
| `attached` | `no_supplier` | no live supplier has that exact name (a short code, a different case, a renamed supplier), or the document has no supplier |

`no_supplier` is the one staff can fix: correct the supplier name on the order lines (or the supplier row) and generate the request again.

## Routes

### `GET /api/v1/draft-container-documents/:id/delivery`

For the send dialog, read only: `{ documentId, type, supplier, delivery, reason, portalUrl, suppliers: [{ id, name }] }`. `suppliers` are the portal supplier rows every recipient will be linked to. `404` for a missing or deleted document.

### `POST /api/v1/draft-container-documents/:id/email`

Body: `{ to, subject?, body?, delivery?, fields? }`.

- **`delivery`**: the way the dialog told the sender it would go (`"portal"` or `"attached"`, from the route above). The server works it out again; if it differs, **`409`** `{ code: "DELIVERY_CHANGED", error, delivery, reason, allowedNext: [delivery] }` and nothing is sent. The email's wording depends on the way it goes ("on the portal" against "attached"), so a change is handed back to the sender, never sent as written. Read the delivery again, show the sender, let them send again.
- **No `delivery`** (a ShipLine from before this): the old email, attached, nothing checked. This is what makes it safe to deploy shipping first.
- **`fields`**: `{ etd?: "YYYY-MM-DD", port?: string }`, what the email says about the departure. The dialog fills the template's `[etd:date]` and `[port:port]` fields and passes the same values here. Stored on the send row (`request_etd`, `request_port`) and shown on the portal beside the PDF: **what the email said, never recomputed from the shipment**. Not a real calendar day, or a port over 128 characters → `400`. Kept for a Request to send only.
- **`to`**, for a portal delivery, is parsed as the PO send parses it (`parseRecipients`: several addresses per box, `Name <a@b.com>`, full-width characters folded); anything that is not one clean address → `400` with `invalid: [...]`.

Response (both ways): the old fields, plus `delivery`, `deliveryReason`, `etd`, `port`, `portalUrl` and `portalAccess` (`null` when attached). `attachments` is `[]` for a link.

What a portal delivery does, in order:

1. **Pre-approves every recipient** (`preapprovePortalUsers`, its own committed transaction, before Front): the same per-address rules as the PO send (`invited` / `approved` / `already_active` / `blocked`), linked to the live supplier rows named exactly as the document's supplier and to nothing wider. `decided_note`: "Pre-approved from ShipLine: Request to send <ref> v<n> emailed by <sender>". If pre-approval finds the supplier row or the portal has gone since step 0, the answer is the same `409 DELIVERY_CHANGED`. If it **fails** (a database error), the email still goes as a link, without the "already approved" wording, and `portalAccess.status` is `error`.
2. **Sends the email** through Front: the template body (stored `request_to_send_portal`, or the sender's edit), then the fixed bilingual portal block (`requestPortalBlockHtml`: what is waiting, the ETD and port if given, the button to `${SUPPLIER_PORTAL_URL}/requests-to-send?doc=<document id>`, how to register), inside the portal emails' frame. No attachment, no confirm button.
3. **Writes the send row** with `delivery = 'portal'`, `request_etd`, `request_port`. The row is what lists the request on the portal, so the link works only because it is there.
4. **Logs the email** in `email_receipts` as `confirm_via 'portal'` with `action_url` = the portal link.

## Trust

Pre-approval means: whoever owns a mailbox a Request to send goes to can sign in to the portal as that supplier and see everything the portal shows them: their **purchase orders with prices**, proforma invoices, quotes. A packing list has no prices, so this is more than the attachment ever gave away. A logistics contact or a freight agent on the recipient list gets all of it. ShipLine's dialog says so before sending and lists what happened to each address after; disable a user in JFPRO (Portal users) if it was not wanted.

There is no server-side role check on these routes, as on the PO send: ShipLine's read-only roles are held back by the frontend only.

## Email Receipts

- `email_type` is `request_to_send` for a Request to send from now on, whichever way it goes (earlier rows stay `draft_container_quote`, as every forwarder quote still is).
- A link: `confirm_via 'portal'`, chased with reminders that repeat the portal link (`CHASED_PORTAL_STEPS`) until one of these confirms it (`syncPortalReceipts`, each only for what happened **after** the email went out):
  - **`request_viewed`**: a row in the portal's `supplier_quote_views` for that document **or a later version for the same container and supplier**. The portal writes one when the supplier opens the PDF or the CSV, and when they arrive on the page through the email's link (signed in, on the row the link names): following the link is itself the acknowledgement, so nobody has to download anything to stop the reminders.
  - **`request_pl_uploaded`** / **`request_ci_uploaded`**: the supplier uploaded a packing list, or an invoice, **for that shipment** through the portal (`requestUploadSources`): sent to our team (any status but `draft` / `discarded`), filed by staff on that container or shipment or, while staff have not decided, matched to it by shipping's own check with `match_confidence = 'confident'`, and uploaded by someone whose suppliers include one named exactly as the request's. The supplier is never asked which shipment an upload is for, so an upload that matched nothing confirms nothing until staff file it.
  
  While the portal's view table cannot be read, these emails are not chased.
- Attached: `confirm_via 'email'`, the confirm button, exactly as before.

## When a request is done (the portal hides it)

The portal's Requests to send page keeps a request out of the way, behind "Show completed requests", once nothing more is owed for it (portal `supplierQuoteProgressSql` and `requestDone`; portal README section 10). The portal reads shipping's tables for this; shipping only grants the columns.

| `done` | When |
|---|---|
| `shipped` | the document's shipment (`draft_container_documents.shipment_id`) has stored stage `IN_TRANSIT`, `ARRIVED` or `CLOSED`, **or** one of its live orders (`orders.shipment_id`) is `ON_SEA`, `ON_AIR`, `ARRIVED_AT_WAREHOUSE`, `RECEIVED` or `PARTIALLY_RECEIVED`. Both halves, as `S.effectiveStage` needs both: the stored stage stays `BOOKED` when the carrier sync or receiving moves the orders. |
| `documents` | a live `packing_lists` row is filed in that container (`container_kind 'booked'`, `container_number` = the shipment's reference) **and** a live `shipment_payment_documents` `balance_invoice` is filed on that shipment, both under the request's supplier. Filed, whichever way they arrived (a portal upload staff imported, or one staff uploaded). An upload still waiting for staff does not count. |

The supplier is matched by name with everything but letters and digits dropped (`Co., Ltd` = `Co.,Ltd.`): the request carries the order lines' supplier text, a packing list a supplier id and a name, an invoice a name, and they often belong to different supplier rows of the same company. On TEST on 2026-11-13, of 141 emailed requests 124 read as shipped, 2 more as documents, 15 open.

Not covered, and so left open on the page: a cancelled or deleted draft's request, and a draft's request whose `shipment_id` points at a shipment row that was since replaced.

## Views

`GET /api/v1/shipments/:id/documents` and `GET /api/v1/draft-containers/:name/documents` now return, on each document's `sends[]`: `delivery`, `etd`, `port`; and on each `supplier-quote` document: `views: [{ email, firstViewedAt, lastViewedAt, count }]` (`loadSupplierQuoteViews`, empty when the portal table is not there).

## Schema

- `2026-11-13_10_request_to_send_delivery.sql`: `draft_container_document_sends.delivery`, `.request_etd`, `.request_port`. The code answers a server without them as empty (a send is recorded without them), so the order of migration and deploy does not matter.
- `2026-11-13_11_grant_request_to_send_to_jportal.js`: column grants for the portal's login: `csv_public_url` and `shipment_id` on the documents, `request_etd` / `request_port` on the sends, and for "done": `shipments (id, reference, stage)`, `orders (shipment_id)`, `packing_lists (container_kind, container_number, supplier_name, deleted_at)`, `shipment_payment_documents (shipment_id, supplier_name, doc_kind, deleted_at)`. The portal's `db/grants/jportal_app.sql` carries the same. Without them the portal's page still lists every request, all as open.
- `2026-11-13_20_seed_request_to_send_portal_template.js`: the `request_to_send_portal` template, category `Supplier Quote`. The templates ops wrote for the attached email stay, and are what the dialog offers when a request goes out attached. **A sender who picks an "attached" template for a link says "attached" with nothing attached**: the dialog defaults to the right one and warns.

## Rollout (test, then prod)

1. **Shipping**: `bash deploy.sh <stage>` (the three migrations, the routes). Nothing changes for senders yet: the old ShipLine sends no `delivery`.
2. **Portal**: `bash deploy.sh <stage>` in `portal` (creates `supplier_quote_views`, its grants, the `/viewed` route, the Requests to send page). Its page reads `request_etd` / `request_port` / `csv_public_url`, so it must come **after** step 1 and after the grants are on the server.
3. **ShipLine**: deploy as usual. From here requests go out as links.

On test, the database is rebuilt from prod every 3 days: the migrations and grants go with it until prod has them.

Not verified by the unit tests: the routes against a real database and Front (no smoke tool yet), and that the files' S3 address opens from mainland China (portal README section 10, "Links"), which now matters for these files too.
