# QC Visits: deploy checklist and feature reference

For the engineer who reviews, deploys and supports QC Visits. It describes the code as it stands on branch `test` (uncommitted, 2026-10-27; kits moved to JFPRO on 2026-10-28, see §8) in `shipping` (backend) and `ShipLine` (frontend). The design history is in `ShipLine/docs/plans/qc-visits.md` (plan) and `ShipLine/docs/plans/qc-visits-contract.md` (build contract). Where the code differs from them, this file follows the code and says so. **[unverified]** marks anything not confirmed by reading or running the code.

---

## 1. What it does

- Adds a **QC VISITS** view to ShipLine (`/qc-visits`, header tab `10`, placed after QC) for **arranging** a factory QC visit in China.
- **Candidates** lists PO lines in `UNDER_PRODUCTION` / `READY_FOR_QC`. The user ticks lines and arranges a visit: units to inspect per line, host factory (`qc_factories`), goods moved in from other suppliers, inspector (`qc_inspectors`), and the date and window in China time (UK time shown alongside).
- A visit goes `DRAFT → PROPOSED → CONFIRMED → COMPLETED`, or `CANCELLED` (with a category; two categories flag a **wasted trip**). It reaches CONFIRMED only when both the factory and the inspector have confirmed.
- **Kits** come from JFPRO (§8): a product's inspection kit is its JFPRO QA documents at the Plan of Action stage "China Agent — factory QC", attached to the product or to a product group it is in. ShipLine lists them; shipping stores no kit. There is no checklist, no company default kit and no brief PDF.
- **Comms**: proposal and cancellation emails to the factory or inspector go through Front. They can carry the products' kit documents (inspector proposals only), an ICS invite and a QA document, and they get an email receipt.
- **Supplier portal**: two public endpoints let the factory confirm the slot or counter-propose a date. The jportal UI is not built.
- **Alerts**: the nightly job adds a `qc_visit` alert type. `GET /orders` gains `qcVisitIds`.

**Out of scope, by decision (plan §0, §9 "Phase 5"):** recording the outcome. Nothing attaches a QC report to a visit, shows per-line verdicts or links a QC invoice to a `wasted_trip`. A visit never writes to `orders`. The planned hooks (`qc_reports.visit_id`, `qc_visit_id` on the QC invoice) **do not exist**. Workflows integration is also out.

---

## 2. Deploy

### 2.1 Order: backend first, then frontend

1. **Backend: `bash deploy.sh test`, then `bash deploy.sh` (prod is stage `dev`).** `deploy.sh` runs `node tools/migrate.js --stage <stage> --apply` **before** `serverless deploy`, under `set -e`, so a failed migration stops the deploy. Run `node tools/migrate.js --stage <stage>` (a dry run) first to see the pending files.
2. **Frontend: after the backend is live on that stage.**

Why the order matters:
- `GET /orders` (`src/lib/order-shape.js` `ORDER_SELECT`) now has a correlated subquery on `qc_visit_lines` JOIN `qc_visits`. If those tables are missing, **every** `GET /orders` fails, and ShipLine polls it every 5 s. The migrations land first, so the tables exist before the code that reads them.
- The nightly `generateDailyAlerts` job (`src/services/daily-alerts.js`) now queries `qc_visits`, `qc_visit_lines` and `orders` through `loadQcVisitAlertInputs`. That call is not wrapped in try/catch, so a missing table fails the **whole** alert run, not just the QC part.
- The frontend calls `/api/v1/qc-visits*`, `/qc-factories`, `/qc-inspectors` and `/qc-kits`. Against an old backend these 404. `mapOrder` copes with a missing `qcVisitIds`, so the rest of the app is unaffected.
- **The view `qc_kit_documents` is created by the `_20` migration**, as the app's database user, which must be able to read the JFPRO documents tables (§2.3). If it cannot, the migration fails and `deploy.sh` stops before any code ships. Without the view `GET /qc-kits` and an inspector email with kit documents fail with 500. The visit routes' `loadKitStatus` is best effort: if the view cannot be read it logs a warning and raises no `kit_missing`.

### 2.2 Migrations (`src/db/migrate/`, all additive and idempotent)

| file | does |
|---|---|
| `2026-10-28_10_qc_visits.sql` | `qc_factories`, `qc_inspectors`, `qc_visits`, `qc_visit_lines`, `qc_visit_moves`, `qc_visit_confirmations`, `qc_visit_attachments` |
| `2026-10-28_11_qc_inspectors_seed.js` | **AQI inspector seed**: `INSERT IGNORE ('AQI','agency','AQI Service', …)` on `uq_qc_inspectors_name` |
| `2026-10-28_20_qc_kit_documents_view.sql` | View `qc_kit_documents` over the JFPRO documents tables (§8). `SQL SECURITY INVOKER` with no `DEFINER`: created and read as the app's database user. `CREATE OR REPLACE`, so a re-run is safe |
| `2026-10-28_30_qc_visit_sends.sql` | `qc_visit_sends` (with `kit_documents_json`) |
| `2026-10-28_31_seed_qc_visit_templates.js` | **Template seed**: `qc_visit_factory`, `qc_visit_inspector`, `qc_visit_cancel` (category `QC Visit`) from `src/lib/email-template-defaults.js`, `INSERT IGNORE` on `email_templates.uk_template_key` |

All tables follow the house rules: no FKs, no ENUMs, UTC timestamps, `utf8mb4_0900_ai_ci`. `qc_visit_attachments` is in `_10`, not in `_20` as the plan had it. There are no kit tables: the original `_20` (kit and brief tables) and `_21` (default kit seed) files were deleted on 2026-10-28, before anything ran. `_20` is now the view.

Unlike the older cross-database views (`src/db/migrations/README.md`), the kit view is a migration, not hand-applied as admin (§2.3).

### 2.3 Infrastructure and configuration

- **JFPRO read access for the kit view (check BEFORE deploying, on test and on prod):** the `_20` migration creates `qc_kit_documents` as the app's database user (`DB_USER` in the `shipping/<env>` secret), and the Lambdas read it as the same user. That user already reads `jfpro.products`, `jfpro.suppliers` and `jfpro.supplier_bank_accounts` directly; it also needs `SELECT` on `jfpro.documents`, `document_versions`, `document_attachments`, `document_tags` and `product_group_members`. Check with the app's credentials, e.g. `node tools/sql.js "SELECT COUNT(*) FROM jfpro.document_tags"`. If it is refused, grant `SELECT` on those tables to that user as admin. Otherwise the migration fails and `deploy.sh` stops before any code ships, which is safe but blocks the deploy.
- **Authed routes:** no `serverless.yml` change. They are Express routes in the `ordersApi` Lambda behind the greedy `/api/v1/{proxy+}` route, registered in `src/handlers/orders.js` after `registerContainerPhotoRoutes` (`registerQcVisitRoutes`, `registerQcKitRoutes`, `registerQcVisitCommsRoutes`).
- **New public API Gateway routes** (`serverless.yml`, function `supplierPortalApi`): `POST /api/v1/portal/qc-visit-lookup` and `POST /api/v1/portal/qc-visit`, each throttled at 5 req/s with a burst of 10. The throttle stage resource gains `DependsOn: HttpApiRoutePostApiV1PortalQcDashvisitDashlookup` and `HttpApiRoutePostApiV1PortalQcDashvisit`. **[unverified]** These logical IDs follow the existing naming pattern, but only `serverless deploy` (or `serverless package`) proves CloudFormation accepts them. Run `serverless package --stage test` first if you want to check without deploying.
- **Env vars used:**
  - `PO_DOCS_BUCKET` (`shipping-purchase-orders-<stage>`): visit attachments and the QA document attachment. Routes return 500 `NO_BUCKET` if it is unset.
  - `JFPRO_DOCS_BUCKET` (**new**, `jfpro-documents-<stage>`, set in `serverless.yml`): JFPRO's documents bucket, where the kit documents live. `GET /qc-kits` and an inspector email with kit documents return 500 `NO_BUCKET` if it is unset. The bucket name comes from this variable, never from `document_versions.s3_bucket` (the test database is a nightly copy of production; `jfpro-documents-test` is a mirror of `jfpro-documents-dev`).
  - `FRONT_API_TOKEN` and `FRONT_CHANNEL_ID`: needed only by `POST /qc-visits/:id/email`, which returns 500 `FRONT_NOT_CONFIGURED` if either is empty. **[unverified]** whether the `shipping/test` secret has them.
  - `PUBLIC_API_BASE_URL`: optional. `email-receipt.js` reads it for the confirm-receipt link; without it the link base comes from the request.
  - **Gemini is not used** by any QC visit code. Email bodies come from templates.
- **Bucket policy:** `PurchaseOrderDocsBucketPolicy` gains a `Deny` of anonymous `s3:GetObject` on `qc-visits/*` (§4.3).
- **IAM:** one new grant, `s3:GetObject` (read only) on `arn:aws:s3:::jfpro-documents-<stage>/*`, for the presigned kit links and the email attachments. JFPRO's bucket is in the same region (eu-north-1) and uses AES256 (no KMS key to grant), per JFPRO `api/serverless.yml`; **[unverified]** that it is in the same AWS account and that its bucket policy (if any) does not deny the shipping role. The existing `s3:PutObject` / `s3:GetObject` on `shipping-purchase-orders-<stage>/*` covers visit attachments. There is no `DeleteObject`, so attachment deletes are soft (`deleted_at`).
- **Dependencies:** none added. The QC code uses `@aws-sdk/*` and `uuid`, which are already in the repo. `package.json` changes only `test:unit`, which now also runs `tools/test-qc-visits-lib.js`, `tools/test-ics-lib.js`, `tools/test-qc-kit-docs-lib.js` and `tools/test-qc-alerts-lib.js`.
- **Frontend:** `npm run lint` (tsc) and `npm test`. The deploy target is **[unverified]**: there is a `vercel.json`, and it is presumably the normal ShipLine release.

### 2.4 Pre-deploy checks

- [ ] `cd shipping && npm run test:unit`. It passed on 2026-10-28 after the JFPRO kit change.
- [ ] `node -e "require('./src/handlers/orders.js')"` loads without error. The three route modules loaded OK on their own.
- [ ] `cd ShipLine && npm run lint && npm test`. tsc was clean and the `qcVisits/*.test.ts` suites passed (31 tests) on 2026-10-27, but UI components were still being written then, so re-run this.
- [ ] `tools/test-orders-shape-frozen.js` against the test stack after the deploy. `qcVisitIds` is on its allowlist.
- [ ] `node tools/migrate.js --stage test` lists exactly the five `2026-10-28_*` files (`_10`, `_11`, `_20`, `_30`, `_31`).
- [ ] The app's database user can read the JFPRO documents tables on the stage (§2.3), so the `_20` view migration can run.

### 2.5 Smoke test (test stage, step by step)

Run this on the test stack. Note that the test stack's EventBridge schedules are disabled (`schedulesEnabled.test: false`).

1. **Migrations, seeds and the view.** Run `node tools/migrate.js --stage test --status`: no pending files. In SQL: `qc_inspectors` has one `AQI` row; `email_templates` has the three `qc_visit_*` keys; `SELECT jf_code, document_name, attached_via FROM qc_kit_documents LIMIT 20` returns rows (if production has any China factory QC documents).
2. **Orders still load.** Open ShipLine; the manifest loads. `GET /api/v1/orders` returns `qcVisitIds: []` on every order.
3. **Arrange a visit end to end.**
   - QC VISITS → Candidates. Tick a line in production at a test supplier → Arrange.
   - Add a factory: label, address and **city**. Without a city the `factory_no_city` warning appears at propose time.
   - Pick AQI as inspector, set a date ≥ today with a window, and save. You get a DRAFT `QCV_000nn`.
   - Reload the page: `GET /orders` now carries that visit id on the order.
4. **Kit documents.** Use a line whose product has a China factory QC document in JFPRO (pick one from the step 1 query: the test database is a nightly copy of production and no JFPRO test web app is known, so a document added in JFPRO today reaches test tomorrow). Open the line's kit from the visit: it lists the document (name, file, version, size, uploaded by/at, "via product group" where so) with VIEW and DOWNLOAD links that work (presigned on `jfpro-documents-test`, 15 minutes). A line whose product has none shows the empty state and a `kit_missing` warning.
5. **Propose.** Press Propose. If a product has no QC document, the `kit_missing` block must be acknowledged (PROPOSE ANYWAY). Status becomes PROPOSED.
6. **Email to yourself.** Schedule tab → Send to inspector. Put **your own address** in `to`, with the QC documents and ICS ticked, and send. Check: the email arrives through Front with each product's kit documents and `QCV_000nn.ics` (it opens in a calendar at the China time); a `qc_visit_sends` row exists and its `kit_documents_json` lists the attached documents; `GET /qc-visits/:id/sends` returns them as `kitDocuments`; the receipt badge shows it; opening the confirm link marks it confirmed. Repeat for the factory audience (it needs the `[…]` required fields). **The test stack sends real email through Front: use only your own address.**
7. **Inspector confirmation by hand.** Record an inspector `confirmed` row (channel `wechat`). The visit stays PROPOSED.
8. **Portal confirm with a test PO.** Use a PO that is on the visit, plus that supplier's `portal_code`:
   ```
   curl -s -X POST "$API/api/v1/portal/qc-visit-lookup" -H 'content-type: application/json' \
     -d '{"poNumber":"PO_xxxxx","portalCode":"XXXX"}'
   curl -s -X POST "$API/api/v1/portal/qc-visit" -H 'content-type: application/json' \
     -d '{"poNumber":"PO_xxxxx","portalCode":"XXXX","visitRef":"QCV_000nn","action":"confirm","visitDate":"YYYY-MM-DD"}'
   ```
   Expect the lookup to list the visit, and the confirm to return `{ok:true,status:"CONFIRMED"}`. In ShipLine, the factory confirmation shows channel `portal`, and the activity log shows `supplier-portal:<name>`. Also try a wrong code (401), a PO from another supplier or a wrong ref (404), and a stale `visitDate` (409 `STALE`).
9. **Clearing rule.** Change the window on the CONFIRMED visit. It drops to PROPOSED and both confirmations clear. Changing only the factory contact phone keeps it CONFIRMED.
10. **Cancel and clone.** Cancel with `factory_not_ready`: `wastedTrip` is true and the "wasted trips" filter shows it. Clone it: you get a new DRAFT with `clonedFromVisitId`.
11. **Alert run.** Invoke the job by hand, since the schedule is off on test: `aws lambda invoke --function-name shipping-serverless-test-generateDailyAlerts out.json`. **[unverified]** The function name assumes the standard `<service>-<stage>-<function>` naming. Check that `out.json` reports `qcVisit: N`, and that the alerts bell shows `qc_visit` alerts that open the visit. Note: this is the full nightly job, so it also refreshes and auto-resolves the other alert types on test.
12. **Read-only.** As an `accountant`, the view renders and every write gets 403 `{code:'READ_ONLY'}`. The warehouse and exec shells never show the view (`App.tsx` redirects them).

---

## 3. Data model

| table | purpose |
|---|---|
| `qc_factories` | Factory sites per supplier: free-text `supplier_name` plus `supplier_code` snapshot, label, address, city, contact, `is_default`. Soft delete; live `(supplier, label)` uniqueness is enforced in code |
| `qc_inspectors` | Inspectors and agencies (`internal`/`agency`). Deactivated (`active=0`), never deleted. Seeded with AQI |
| `qc_visits` | The visit: `ref` `QCV_00001`, status, host `supplier_name`, factory and inspector **snapshot** columns, `visit_date` + `window_*` (China wall-clock, `tz`), cancel category / `wasted_trip`, denormalised `*_confirmed_at`. `updated_at` is `TIMESTAMP(3)` and serves as the optimistic-concurrency token |
| `qc_visit_lines` | One order per visit: `quantity_to_inspect`, `po_quantity_snapshot` (for `po_shrank`), sample method / AQL, note |
| `qc_visit_moves` | Goods moved from another supplier to the host factory, so that one stop covers them |
| `qc_visit_confirmations` | Append-only log per party: `proposed`, `confirmed`, `declined`, `counter`, with the channel (`portal` for supplier-portal writes) |
| `qc_visit_attachments` | Free-form files on a visit (`qc-visits/<uuid>/<name>`, 25 MB max). Soft delete |
| `qc_visit_sends` | One row per Front email (audience, `proposal`/`cancel`, recipients, Front ids). `kit_documents_json` is `[{ documentId, versionId, version, filename }]` of the JFPRO kit documents attached, or NULL. It is linked from `email_receipts` (`email_type='qc_visit'`) |
| view `qc_kit_documents` | Migration `_20` (§2.2). One row per (product, attachment path, document) for JFPRO documents with category `qa` and `qa_type` tag `poa_china` or no (or an empty) `qa_type` tag, not deleted, current version only, attached to the product directly (`attached_via 'product'`) or to a product group it is in (`'product_group'`). Columns: `jf_code` (`UPPER(TRIM(jfcode))`), `product_id`, `product_name`, `document_id`, `document_name`, `version_id`, `version_number`, `s3_key`, `filename`, `mime_type`, `size_bytes`, `uploaded_by`, `uploaded_at`, `attached_via`, `document_updated_at` |

Existing tables it reads: `orders` (read only), the `suppliers` view, `jfpro.products.repackable` (best effort, for `fqc_missing`), the JFPRO documents through `qc_kit_documents`, `email_templates`, `email_receipts`, `quality_assurance_documents`, `audit_log` (entity types `qc_visit`, `qc_factory`, `qc_inspector`) and `daily_alerts`.

---

## 4. API

All authed routes are under `/api/v1`. Writes require `admin` or `standard`. Visit writes take `updatedAt` (the client's copy) and are checked at ms precision under `SELECT … FOR UPDATE`. Order statuses go out in the server dialect (`IN_PRODUCTION`, …).

### 4.1 Routes

| route | file | what |
|---|---|---|
| `GET /qc-visits` | qc-visit-routes | List: `?status=CSV&supplier&inspectorId&from&to&includeLines=1&wasted=1`, with `view` warnings on open visits |
| `POST /qc-visits/warnings` | qc-visit-routes | Readiness warnings for a draft line set (registered before `/:id`) |
| `POST /qc-visits` | qc-visit-routes | Create a DRAFT (blocks at stage `create`) |
| `GET /qc-visits/:id` | qc-visit-routes | Detail with lines, moves, confirmations and attachments |
| `PATCH /qc-visits/:id` | qc-visit-routes | Edit header, snapshot fields or factory/inspector. Applies the clearing rule; `supplierName` can change only in DRAFT |
| `PUT /qc-visits/:id/lines` | qc-visit-routes | Replace lines and moves. Re-snapshots PO quantity; a scope change runs the clearing rule |
| `POST /qc-visits/:id/transition` | qc-visit-routes | `to` PROPOSED / CONFIRMED / COMPLETED / CANCELLED |
| `POST /qc-visits/:id/confirmations` | qc-visit-routes | Record a party's confirmation, decline or counter (refused in DRAFT) |
| `POST /qc-visits/:id/accept-counter` | qc-visit-routes | Apply a counter-proposal's date/window and confirm that party |
| `POST /qc-visits/:id/clone` | qc-visit-routes | New DRAFT from a COMPLETED or CANCELLED visit (409 `NOT_CLONEABLE` otherwise). Drops lines that left the candidate statuses (`droppedOrderIds`) |
| `POST /qc-visits/:id/attachments/upload-url` | qc-visit-routes | Presigned PUT ticket (600 s, 25 MB). `contentType` must be on the allowlist (§4.3) |
| `POST /qc-visits/:id/attachments` | qc-visit-routes | Register an uploaded file (HeadObject check) |
| `DELETE /qc-visits/:id/attachments/:attId` | qc-visit-routes | Soft delete |
| `GET /qc-factories` | qc-visit-routes | `?supplier=&includeDeleted=1`. Lazily re-homes factories whose `supplier_code` now maps to a renamed supplier |
| `POST /qc-factories` | qc-visit-routes | Create. Setting `is_default` clears the others for that supplier |
| `PATCH /qc-factories/:id` / `DELETE /qc-factories/:id` | qc-visit-routes | Edit / soft delete (409 `IN_USE` while an open visit uses it) |
| `GET /qc-inspectors` | qc-visit-routes | `?includeInactive=1` |
| `POST /qc-inspectors` / `PATCH /qc-inspectors/:id` | qc-visit-routes | Create / edit. Deactivating is refused (`IN_USE`) while open visits use it |
| `GET /qc-kits` | qc-kit-routes | Read only. `?jfCodes=A,B` is required (normalised, `_FQC` stripped, de-duplicated; 400 `BAD_FIELD` when missing, empty or over 500 codes). Returns `{ data: QcKit[] }`, one per requested code in request order, even with no documents (§8) |
| `GET /qc-visits/:id/sends` | qc-visit-comms-routes | Email sends, newest first, each with `kitDocuments` (array or null) |
| `GET /qc-visits/:id/email-preview` | qc-visit-comms-routes | `?audience&kind`: rendered template plus suggested `to` |
| `POST /qc-visits/:id/email` | qc-visit-comms-routes | Send via Front, max 20 recipients, attachments ≤ 20 MB in total (kit documents over the budget are skipped and listed). `attachKitFiles: true` attaches the kit documents on inspector proposals only; `attachBrief` from older clients is ignored |
| `GET /qc-visits/:id/ics` | qc-visit-comms-routes | `<ref>.ics` (`?cancel=1` → `METHOD:CANCEL`). Lists products without PO numbers (§4.3) |
| `POST /portal/qc-visit-lookup` | supplier-portal.js (**public**) | PO number + portal code → PROPOSED/CONFIRMED visits hosted by that PO's supplier |
| `POST /portal/qc-visit` | supplier-portal.js (**public**) | `confirm` / `counter` as the factory. Contract for jportal: `docs/qc-visit-portal.md` |

Changed existing reads: `GET /orders` adds `qcVisitIds` (open visits only). `GET /alerts` can return type `qc_visit`.

### 4.2 Error codes (`{ error, code, …payload }`)

| HTTP | code | when |
|---|---|---|
| 400 | `BAD_FIELD` (+`field`) | validation. Also `contentType` outside the upload allowlist, `attachQaDocumentId` for a QA document that covers none of the visit's orders, and `jfCodes` on `GET /qc-kits` missing, empty or over 500 codes |
| 403 | `READ_ONLY` | write by a role other than admin/standard |
| 404 | `NOT_FOUND` | missing visit, factory, attachment, QA document or confirmation. The portal uses 404 for every visit mismatch on purpose |
| 409 | `STALE` (+`visit`) | `updatedAt` mismatch; the payload carries the current detail |
| 409 | `BLOCKED` (+`blockers: Warning[]`) | create, lines or transition blocked by warnings or transition rules |
| 409 | `TERMINAL` | write to a COMPLETED/CANCELLED visit |
| 409 | `NOT_PROPOSED` | confirmation or accept-counter on a DRAFT |
| 409 | `IN_USE` (+`visits`) | delete a factory / deactivate an inspector used by an open visit |
| 409 | `DUPLICATE` | live factory label or inspector name already taken |
| 409 | `DUPLICATE_UPLOAD`, `NOT_UPLOADED` | file registration: key already registered / object not in S3 |
| 409 | `NOT_CLONEABLE` | clone of a DRAFT, PROPOSED or CONFIRMED visit: only a completed or cancelled visit can be cloned (an open one would double-book its units past the `qty_exceeds` check) |
| 409 | `NOTHING_TO_CLONE` | clone where no line is still a candidate |
| 413 | `TOO_LARGE` | uploaded object over the size limit |
| 500 | `NO_BUCKET`, `FRONT_NOT_CONFIGURED` | missing env (`PO_DOCS_BUCKET`; `JFPRO_DOCS_BUCKET` for `GET /qc-kits` and kit attachments; Front) |
| 502 | `FRONT_FAILED` (+`frontStatus`, `frontBody`) | Front rejected the message |
| 401 / 429 | (portal only) | bad PO or code / API Gateway throttle |
| 429 | `TOO_MANY` (portal only) | 10 factory portal answers for the visit in the last rolling 24 h |
| 400 | `BAD_BODY` (portal only) | request body is not valid JSON |

### 4.3 Security

- **Kit documents go to the inspector only.** `POST /qc-visits/:id/email` attaches them only on a proposal whose `audience` is `inspector`; on any other email `attachKitFiles` is ignored. `attachBrief` is ignored everywhere (there is no brief). A QA document can be attached only when it covers at least one of the visit's orders.
- **The factory never sees another supplier's PO numbers.** In factory emails (proposal and cancel), `{lines}` and the attached `.ics` print a line whose order belongs to a different supplier than the visit's `supplier_name` (compared with `normSupplier`) without its PO number, e.g. `JF0208 — Widget — 500 units (moved in)`. The downloadable `GET /ics` has no audience, so it lists every product as JF code — product — quantity, with no PO numbers. The inspector email keeps the full details. The email subject is one line, at most 255 characters.
- **Uploads and views.** The visit attachment upload-url route accepts only images (JPEG, PNG, GIF, WebP, HEIC), PDF, MP4/QuickTime, Word/Excel/PowerPoint, CSV, plain text and zip; anything else (SVG and HTML included) is 400 `BAD_FIELD` `field: 'contentType'`. A view (`url`) link, for visit attachments and for JFPRO kit documents alike (`src/lib/qc-file-types.js`), opens inline only for JPEG, PNG, GIF, WebP, PDF and MP4; any other stored type is served as an `application/octet-stream` attachment. Every `Content-Disposition` carries an ASCII `filename` and a UTF-8 `filename*`. Kit document links are presigned GETs on the JFPRO bucket, valid 15 minutes; the S3 key never leaves the server.
- **No anonymous reads of QC files.** The bucket stays public-read for older documents, but `PurchaseOrderDocsBucketPolicy` now also denies `s3:GetObject` to anonymous principals under `qc-visits/`. (JFPRO's documents bucket blocks public access itself.) Presigned URLs (signed by the Lambda role) and the Lambdas' own reads, including email attachments, keep working. The live→test mirror uses `CopyObject` under its own IAM grant, so it is unaffected.

---

## 5. State machine

| from | to | how | requires |
|---|---|---|---|
| — | DRAFT | `POST /qc-visits` | supplier, ≥ 1 line. No `create`-stage blocks (qty > remaining, line not in candidate status, order deleted, foreign line without a move, bad window) |
| DRAFT | PROPOSED | transition | visit date; factory (id, or label + address); inspector (id or name); no `propose` blocks (`kit_missing` can be acknowledged; also `factory_deleted`, `inspector_deleted`, …). Empty snapshot columns are filled from the live rows |
| PROPOSED | CONFIRMED | automatic when the second party's latest effective row is `confirmed` (manual transition also allowed, with the same check) | both parties confirmed |
| CONFIRMED | PROPOSED | a clearing edit, or a `declined`/`counter` row | — (`proposed_at` kept, `confirmed_at` nulled, audit `reproposed`) |
| CONFIRMED | COMPLETED | transition (manual) | `visit_date` ≤ today in China |
| DRAFT / PROPOSED / CONFIRMED | CANCELLED | transition | `cancelCategory` (not needed from DRAFT); `reason` when `other`. `factory_not_ready` and `goods_not_at_site` set `wasted_trip=1` |
| COMPLETED / CANCELLED | new DRAFT | `POST /:id/clone` | at least one line still a candidate (an open visit → 409 `NOT_CLONEABLE`) |

COMPLETED and CANCELLED are terminal.

**Clearing rule** (`clearingFor` in `src/lib/qc-visits.js`, applied by `clearingPatch` in `qc-visit-routes.js`, **in PROPOSED and CONFIRMED**):

| edited | clears | CONFIRMED becomes |
|---|---|---|
| `visitDate`, `windowStart`, `windowEnd` | both | PROPOSED |
| `factoryId`, factory label/address/city/province | both | PROPOSED |
| factory contact fields only | nothing | stays |
| `inspectorId` or any inspector snapshot field | inspector | PROPOSED |
| lines or moves (`PUT /lines`, only when the set actually changes) | both | PROPOSED |
| notes, production state, report deadline, escalation, attachments | nothing | stays |
| accept-counter | the other party (the slot changed) | PROPOSED until both confirm |

A cleared party's old `confirmed` row stays in the log but no longer counts (`effectiveConfirmations`): the `*_confirmed_at` columns are authoritative.

---

## 6. Known limitations and open decisions

1. **QC files are no longer public.** The bucket is still public-read for other documents, but the `PurchaseOrderDocsBucketPolicy` in `serverless.yml` now denies anonymous `s3:GetObject` on `qc-visits/*` (§4.3). Visit attachments are readable only through presigned GET URLs (3600 s, not the plan's 300 s) or the Lambda role; kit documents only through 900 s presigned URLs on the JFPRO bucket. No public URL is ever stored, and email attachments are sent as bytes. **[unverified until deployed]** the policy change ships with the next `serverless deploy`; after it, a plain object URL under that prefix returns 403.
2. **Kits are whatever JFPRO holds now.** Nothing in shipping pins a kit version to a visit; `qc_visit_sends.kit_documents_json` records which document versions went with each email. A kit document over the 20 MB budget is skipped (listed in `skippedAttachments`), not linked.
3. **The `unvisited_ready` alert may be noisy.** It raises one alert per order in `READY_FOR_QC` that is on no open visit (`_FQC` children of a candidate parent are skipped), with `entityType:'order'`. On first deploy, every such order alerts at once. Watch the first run's `qcVisit` count and consider a threshold or opt-in.
4. **The factory `supplier_code` is never set from the UI.** `getSuppliers()` does not return the JFPRO supplier `code`, so `App.tsx` builds `QcSupplierOption` without `code` (CONTRACT-GAP). `FactoryPicker` sends `supplierCode: null` **explicitly**, and the server skips its own `SELECT code FROM suppliers WHERE name = ?` fallback when the key is present (`'supplier_code' in cols`). So UI-created factories get `supplier_code = NULL` and **never re-home** when the supplier is renamed in JFPRO. Fix either side: have the frontend omit the key, or have the server treat null as "look it up".
5. **The clearing rule also applies in PROPOSED.** The plan (§6) says PROPOSED edits have no side effects. The code clears the affected party's `*_confirmed_at` in PROPOSED as well (`CLEAR_IN_STATUSES = ['PROPOSED','CONFIRMED']`). That is defensible, since a factory that confirmed a date should not count as confirming a new one, but it differs from the plan. Decide which is intended.
6. **The jportal UI for slot confirmation is not built.** The backend endpoints and `docs/qc-visit-portal.md` exist; the supplier-facing screen is the jportal team's change in another repo. Until then, factory confirmations are recorded by staff, or by hand-made API calls.
7. **No run against real MySQL yet.** All tests are pure-lib unit tests (`qc-visits`, `ics`, `qc-kit-docs`, `qc-alerts`). No route, migration, seed, the `qc_kit_documents` view or the `GET /orders` subquery has been executed against a database. Specific risks: `IN (?)` array expansion, `TIMESTAMP(3)` ms comparison for `STALE`, `TIME`/`DATE` string formats, and the `JSON_ARRAYAGG` subquery's cost on `GET /orders`. **The test-stage smoke test (§2.5) is the first real run.**
8. Other code-level notes:
   - `attachIcs` defaults to **on** in `POST /email` unless `false` is sent.
   - The portal accepts `accessCode` as an alias for `portalCode`, and `qc-visit-lookup` returns an extra `factoryConfirmedAt`.
   - `fqc_missing` depends on a cross-schema read of `jfpro.products`. If that fails, it is silently skipped.
   - Supplier matching is by free-text name everywhere, as elsewhere in shipping.
   - The plan's open question Q16 (`_FQC` handling) carries the plan's default and has no recorded user answer. Q15 (brief vs QA document) is moot: there is no brief.
   - `GET /qc-kits` gives `productId` / `productName` from the view rows, so they are null for a product with no kit documents (the frontend has the product name from its orders).
   - `kit_missing` is raised only when the view says a code has no document; if the view cannot be read, no `kit_missing` is raised (logged as a warning).

### UI (verify against final code)

The components in `ShipLine/src/components/qcVisits/` were still being written by other agents when this was drafted. Their roles per the contract §6.3:
- `QcVisitsView` loads visits every 30 s, plus inspectors and kits, and renders the tab strip `candidates | visits | kits | calendar` (default `visits`) and the pop-ups.
- `CandidatesTab` and `ArrangeVisitModal`, with `FactoryPicker` / `InspectorPicker`: selection and creation.
- `VisitsTab`: filters `vst`, `vsup`, `vinsp`, `vfrom`/`vto`, `vwaste`.
- `VisitDetailModal`: tabs `lines | schedule | activity` via `vtab` (no Brief tab). `VisitAttachments` and `VisitSendModal` sit on the Schedule tab.
- `KitsTab` (QC docs per JF code on candidate orders and open visits) and `KitDocsModal` (read-only list of a product's JFPRO QC documents, with an OPEN IN JFPRO link where the URL is known).
- `CalendarTab`: 6 weeks, trips and inspector clashes.
- `DailyAlertsPanel`: opens `qc_visit` alerts.
- `OrderDetailModal`: "on visit" chip from `qcVisitIds`.
- Shared changes: template category `'QC Visit'` (`emailTemplateSelect.tsx`), autofill tokens (`requiredFields.tsx`) and `EmailReceiptBadge` for `qc_visit_sends`.
- Deep link: `/qc-visits?visit=<id>&vtab=schedule`, kit `?kit=<JF>`.
- `accountant` sees the view read-only; the warehouse and exec shells are redirected away from it.

---

## 7. Rollback

- **Migrations are additive** (new tables and seed rows only; no existing table is altered). Rolling back code does **not** require dropping anything; leave the tables and the `qc_kit_documents` view in place (`DROP VIEW qc_kit_documents` only once no deployed code reads it). Do not drop them while any deployed code still has the `qc_visit_ids` subquery in `ORDER_SELECT`.
- **Backend code rollback:** redeploy the previous commit with `bash deploy.sh <stage>`. `migrate.js` does not reverse migrations; the four files stay recorded in `schema_migrations` and the tables stay. Old code ignores them. Leftover `qc_visit` rows in `daily_alerts` are no longer in `MANAGED_TYPES` and will **not auto-resolve**. Dismiss them, or `UPDATE daily_alerts SET status='resolved' WHERE type='qc_visit'` **[unverified column/values; check the schema]**.
- **Hide the feature without a backend rollback (frontend only):** remove the `{ id: 'QC_VISITS', … }` entry from `TABS` in `src/components/shared/AppHeader.tsx`, and/or the `{view === 'QC_VISITS' && …}` mount in `App.tsx` (keep `QC_VISITS` in `APP_VIEWS`/`PATHS` so the types compile). The routes stay reachable only to authed users.
- **Quiet the alerts only:** remove `'qc_visit'` from `GENERATED_TYPES` in `daily-alerts.js` and add it to `RETIRED_TYPES`, so existing rows auto-resolve, then deploy.
- **Close the portal endpoints only:** remove the two `httpApi` events and their `DependsOn` / throttle entries from `serverless.yml`, and deploy.
- **Templates:** the seeded `qc_visit_*` templates are inert without the comms routes. Delete them by hand only if wanted.

## 8. Kits from JFPRO (2026-10-28)

An inspection kit is no longer stored in shipping. The structured kit
(sampling, checklist, tests, packaging, deliverables, versions), the company
default kit, the visit brief PDF and per-line extra checks were removed before
anything was deployed, together with their tables, routes and the temporary
"PDFs only" switch.

**What a kit is.** A product's kit is its JFPRO documents with category `qa`
whose `qa_type` tag is `poa_china` (Plan of Action stage "China Agent —
factory QC"). A QA document with no `qa_type` tag (or an empty one) counts
too: JFPRO's UploadModal treats a missing tag as `poa_china`. Documents
attached to the product directly or to a product group the product belongs to
both count; only each document's current version is used. Deleted documents
never count.

**Where it is managed.** In JFPRO (https://jfpro.built-form.co.uk, product page
`/products/<jfCode>`): add a document with category QA and stage "China
Agent — factory QC". ShipLine only shows kits; shipping only reads them.

**How shipping reads it.**
- View `qc_kit_documents` (migration `src/db/migrate/2026-10-28_20_qc_kit_documents_view.sql`,
  §2.2), keyed by `UPPER(TRIM(jfcode))`.
- `src/lib/qc-kit-docs.js` (pure, tested by `tools/test-qc-kit-docs-lib.js`)
  groups the view rows by JF code, keeps each document once per code
  (`attachedVia` `product` wins over `product_group`), sorts by name then
  document id, and picks the documents to email for a visit (each document
  once across the visit's codes; clashing file names get the JF code as a
  prefix).
- Bytes and links come from the JFPRO documents bucket `JFPRO_DOCS_BUCKET`
  (`jfpro-documents-<stage>`, read-only IAM grant), never from
  `document_versions.s3_bucket`.

**`GET /api/v1/qc-kits?jfCodes=A,B`** (any signed-in user; required; at most
500 codes after normalising, `_FQC` stripped) returns `{ data: QcKit[] }`,
one per requested code in request order, even with no documents:

```
QcKit         = { jfCode, productId | null, productName | null, documents: QcKitDocument[] }
QcKitDocument = { documentId, versionId, version, name, filename, mimeType | null,
                  sizeBytes | null, uploadedBy | null, uploadedAt (ISO) | null,
                  attachedVia: 'product' | 'product_group', url, downloadUrl }
```

`url` is a 15-minute presigned GET (inline for PDF, images and MP4, otherwise a
download); `downloadUrl` always downloads. Every other kit route (save a kit,
kit files, briefs) is gone.

**`kit_missing`.** Raised for a line whose JF code (the parent's for an
`_FQC` line) has no kit document in the view: "<JF code> (PO <n>) has no China
factory QC document in JFPRO." Levels are unchanged: `warn` at create,
`block` at propose (acknowledgeable: it then becomes `warn`), `warn` on view.

**The inspector email.** With `attachKitFiles: true` on an inspector proposal,
`POST /qc-visits/:id/email` attaches the kit documents of the visit's JF codes
within the 20 MB budget (the QA PDF and the .ics go first; documents over the
budget or unreadable are skipped and listed in `skippedAttachments`), and
records the attached ones in `qc_visit_sends.kit_documents_json`
(`[{ documentId, versionId, version, filename }]`, NULL when none).
`GET /qc-visits/:id/sends` returns them as `kitDocuments` (array or null).
`attachBrief` is ignored. The inspector template still reads "The inspection
documents for these products are attached".
