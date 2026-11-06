# PO send: a link to the supplier portal, not an attachment

ShipLine's PO **DOCUMENTS → SEND** no longer attaches the PDF. The email (via Front, as before) tells the supplier a new PO PDF is on the supplier portal, with a button to the order there and how to register. Before it goes out, every supplier recipient is **pre-approved** as a portal user for the PO's supplier, so registering with that address needs no approval in JFPRO. When the supplier opens the PDF on the portal, the portal records it and ShipLine shows a **SEEN** tick on the PO.

Code: `src/lib/po-portal-email.js` (pure rules, tested by `tools/test-po-portal-email-lib.js` in `npm run test:unit`), `src/services/portal-access.js` (SQL), the route `POST /api/v1/purchase-order-documents/:id/email` in `src/handlers/orders.js`. Portal side: `portal/src/handlers/sentDocuments.js` (`markPoDocumentViewed`), `portal/db/migrations/2026-11-03_jportal_po_document_views.sql`, `portal/web/src/components/EmailedPdfsPanel.tsx`.

## The email

- Template `purchase_order_portal` (category `PO`, seeded by `2026-11-03_20_seed_po_portal_template.js`, editable in ShipLine's templates). Tokens `{poNumber}` and `{version}`, filled in for the stored template and for a body/subject edited in the send dialog alike. ShipLine's send dialog now defaults to it; the old `purchase_order` template ("Please see attached PO") is left in the table untouched. Retire or reword it, or a sender who picks it says "attached" with nothing attached.
- Under the body the sender appends a fixed, bilingual **portal block** (`portalBlockHtml`): a "Go to the supplier portal" button to `${SUPPLIER_PORTAL_URL}/orders/<po id>?tab=pdfs` (the order's page on the portal, opened on its Emailed PDFs tab; sign-in required). The email never contains the PDF's own address: the supplier downloads it from that page. Then the same address as text, how to register at `/register`, and a line saying no PDF is attached. It says the address is "already approved" only when every recipient was granted access to the PO's own supplier (below); otherwise it says our team will approve them.
- A signed-out supplier who follows the link signs in (or registers, verifies and signs in) and lands back on that page: the portal now remembers the page that sent them to sign in (`portal/web/src/lib/returnTo.ts`).
- The send row in `purchase_order_document_sends` is written as before. It is also what lists the version on the portal (portal README section 10), so the link works only because the row is there.
- `SUPPLIER_PORTAL_URL` unset → `409 PORTAL_URL_UNSET`, nothing sent. It is set on both stages (`https://portal.built-form.co.uk`, `https://jportal-test.built-form.co.uk`).

## Pre-approval

`preapprovePortalUsers`, in its own transaction, committed **before** the Front call.

- **Recipients as typed.** Each recipient box may hold several addresses (`,` `;` or new lines), `Name <a@b.com>`, `<a@b.com>` or `mailto:a@b.com`; invisible characters (zero-width space, direction marks) are dropped and full-width characters (`＠` `．`) folded to ASCII (`parseRecipients`). Duplicates (ignoring case) go out once. Anything still not one plain address → `400` naming it (`invalid: [...]`), nothing sent. Before 2026-11-04 `<a@b.com>`, `mailto:` and a trailing zero-width space were accepted and pre-approved a row the supplier could never sign in as.
- **Who.** **Every** recipient, staff included, trimmed and lower-cased (the portal matches the row by the lower-cased email claim). Until 2026-11-04 ShipLine users and their company domain were skipped; colleagues sent a PO (PO_00430J) then registered and sat "waiting for approval".
- **For which suppliers.** Every live `suppliers` row named **exactly** (`utf8mb4_0900_bin`) as the PO's `supplier` **or as the `supplier` of any of its live lines** (`orders`; older POs' lines often name another row of the same company, e.g. `Suzhou Sunmed Co., Ltd` vs `Suzhou Sunmed Co.,Ltd.`). The portal shows the PO itself only to users linked to a supplier named as the PO, so when only line suppliers match, recipients are linked to those, `poSupplierMatched` is `false`, the email does not say "already approved", and ShipLine asks for the PO's supplier name to be fixed. Nothing matches → `no_supplier`: nobody is pre-approved.
- **Per address**, on `jportal.supplier_users` (locked `FOR UPDATE`):

  | Row | Action | Outcome |
  |---|---|---|
  | none | INSERT `status='active'`, unbound (`cognito_sub` NULL), `approved_by`/`created_by` = sender, `approved_at` = now (UTC), `decided_note` "Pre-approved from ShipLine: PO … emailed by …" | `invited` |
  | `pending` | UPDATE to `active` with the same approval columns, `row_version + 1` (so an admin with it open in JFPRO gets a conflict) | `approved` |
  | `active` | nothing | `already_active` |
  | `rejected` / `disabled` / unknown | nothing: an admin's decision is never undone from here | `blocked` |

  Then `INSERT IGNORE` one `user_supplier_access` row per matching supplier (with the `supplier_name` snapshot), except for `blocked`.
- **Concurrency.** The transaction is `READ COMMITTED` (set immediately before `BEGIN`). Under REPEATABLE READ two sends that each invite a new address deadlocked: the `FOR UPDATE` reads of missing emails took gap locks on `uniq_email` and both INSERTs then waited on each other's gap (every time for the same address, and for different ones sharing a gap, which with few portal users is most). The loser reported `error`. A deadlock that can still happen is retried (3 attempts in all).
- The portal binds an unbound `active` row to the Cognito user on their first sign-in with that verified email and keeps it `active` (portal README section 4, "A signed-in user with no row"). Self-registration verifies the email, so the binding is safe.
- Pre-approved rows show in JFPRO's Portal users page like any other, where they can be disabled or have suppliers changed.
- **Never blocks the email.** No portal schema on the server (`SOURCE_MISSING`) → `unavailable`; any other error is logged → `error`. Either way the email is sent, without the "already approved" wording.
- The response and the `document_emailed` audit row carry `portalUrl` and `portalAccess: { status, suppliers, poSupplierMatched, users: [{ email, outcome, suppliersAdded }] }`. ShipLine alerts the sender only when something needs a look (blocked addresses, `poSupplierMatched: false`, `no_supplier`, `unavailable`, `error`).
- Shipping's DB login is `admin` on both stages: no grants needed for these writes.
- End-to-end check on TEST: `node tools/smoke-po-portal-preapproval.js` drives this route and the portal's own `supplier` handler in-process (recipients sign in with fabricated Cognito claims), Front stubbed, no network; it deletes what it creates.

**Trust note.** Pre-approval means: whoever owns a mailbox we send a PO to can see that supplier's POs on the portal. That is the same trust as emailing them the PDF was, now extended to the supplier's other POs and the portal's other modules. A mistyped recipient is therefore worth more care than before; disable the user in JFPRO if it happens. This includes colleagues: a staff address sent a PO becomes a portal user for that supplier (disable it in JFPRO if that was not wanted).

## Views (the SEEN tick)

- The portal writes `jportal.po_document_views` (one row per document and user: first and last view, count) when a supplier opens an emailed PO PDF from the order's Emailed PDFs tab (`POST /api/supplier/po-documents/{id}/viewed`, same access rule as the list). Opening the order page alone does not count.
- `loadPoDocumentViews` reads it cross-schema; a missing table reads as no views.
- `GET /api/v1/purchase-orders/:id/documents` adds `views: [{ email, firstViewedAt, lastViewedAt, count }]` to every document; the `GET /orders` PO bundle adds it to each PO's **newest** document only (the bundle is polled).
- ShipLine: `LifecycleTicks` shows **SEEN** after **PO** once the latest version has been emailed: green when someone opened that version (hover lists who and when), hollow until then. A new version starts unseen. The DOCUMENTS list and the send dialog list "👁 opened on the portal by …" per version.

## Rollout (test, then prod)

1. Portal: `bash deploy.sh <stage>` in `portal` (migration creates `po_document_views`, re-applies `db/grants/jportal_app.sql`, deploys the `/viewed` route and the web build).
2. Shipping: `bash deploy.sh <stage>` (seeds `purchase_order_portal`, deploys the route).
3. ShipLine: deploy as usual.

Shipping before the portal is safe: views read as none. On test, remember the DB is rebuilt from prod every 3 days.
