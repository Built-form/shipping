# Supplier proforma invoices (PI submissions) — frontend guide

Routes: `src/services/pi-submission-routes.js` · rules: `src/lib/pi-submissions.js`,
`src/lib/bank-verification.js` · checker: `src/services/pi-check.js` (Lambda
`jportal-pi-check-<env>`, handler `src/handlers/pi-check.js`) · tests:
`tools/test-pi-check-lib.js`, `tools/test-pi-submissions-lib.js` (unit, in `npm run test:unit`),
`tools/test-pi-check-flow.js`, `tools/test-pi-submissions.js`, `tools/test-pi-import-flow.js`
(TEST stage) · data: `jportal.pi_submissions`, owned by the supplier portal (the `portal`
repo; its README §11 "Proforma invoice uploads" is the other half of this contract).

Suppliers upload their proforma invoice (PI) in the supplier portal, from the PO's page.
Shipping's `piCheck` Lambda checks it against the PO with **the same Gemini prompt, schema
and PO loader as a staff upload** and writes the result back to the row. The supplier sees
the warnings, then submits the PI or discards it. Submitted PIs wait here until staff
**import** them (the same rows a staff upload makes) or **reject** them with a note the
supplier sees. The supplier is emailed the decision through Front.

```
portal:   draft ──submit──▶ submitted ──withdraw──▶ withdrawn
            │                   │
            └─discard─▶ discarded
shipping:                       ├──import──▶ imported   (purchase_order_invoices + check + payment row)
                                └──reject──▶ rejected   (note shown to the supplier)
```

At most **one** submitted PI per PO (a unique key). Drafts and discarded rows are never
shown to staff.

## The page

ShipLine's **SUPPLIER PORTAL** tab, sub-tab **Proforma invoices**:

- **Badge / left column** — `GET /api/v1/pi-submissions/suppliers`: per supplier, the pending
  count, POs, how many have a major verdict, and the oldest. Oldest first.
- **List** — `GET /api/v1/pi-submissions?supplierId=…` (pending, oldest first) and
  `?status=decided` or `?status=all` for history. Each item has the verdict, the supplier's
  note, `poChanged`, `blockers` and `bankCheck.state` for the chips:
  `changed` → red **BANK DETAILS CHANGED**, `no_history` → red **NO BANK HISTORY**,
  `not_extracted` / `not_checked` → grey **NO BANK DETAILS READ**; `unchanged` → no chip.
- **Detail** — `GET /api/v1/pi-submissions/:id`: the document (`file.url`, a 15-minute S3
  link — fetch the detail again after `file.expiresAt`), the check result, the payment
  terms, the PO as checked vs now (`poDiff`), other PIs on the PO, the audit trail.
- **Import** — optional note, a "Re-run check against current PO" checkbox (tick it by
  default when `poChanged` or `!check.usable`), and the bank dialog when
  `bankCheck.state` is `changed` or `no_history` (Import disabled until a method is chosen).
  When `!check.usable`, no bank dialog: say the PI can't be paid until its bank details are
  verified, after the re-check or by entering them with verify-bank.
- **Reject** — the note is required and is shown to the supplier word for word.
- **Email** — `notifiedAt` set: sent. `notifyError` set and `notifiedAt` null: failed
  ("Retry email" → `notify`). Both null on a decided row: not sent ("Send email").

Who: admin and standard users import, reject, verify bank details and send the email
(`X-User-Type`); read-only users only look (403 `CANNOT_DECIDE` from the server).

## Response envelope (read this)

Shipping's convention, which these routes follow: **lists are wrapped in `{ data }`; a
single object and the answer of an action are NOT wrapped.** So, against the plan's
appendix:

| Route | Answers with |
| --- | --- |
| `GET /api/v1/pi-submissions` | `200 { data: PiSubmission[], hasMore: boolean }` |
| `GET /api/v1/pi-submissions/suppliers` | `200 { data: PiSupplierSummary[] }` |
| `GET /api/v1/pi-submissions/:id` | `200 PiSubmissionDetail` — **not** `{ data: … }` |
| `POST /api/v1/pi-submissions/:id/import` | `200 { submission, invoice, check, recheck?, notification }` — **not** wrapped |
| `POST /api/v1/pi-submissions/:id/reject` | `200 { submission, notification }` — **not** wrapped |
| `POST /api/v1/pi-submissions/:id/notify` | `200 { submission, notification }` — **not** wrapped |
| `POST /api/v1/purchase-order-invoices/:id/verify-bank` | `200 Payment` (the invoice's payment JSON, as on `GET /purchase-orders/:id/invoices`) |

(The same as `/po-change-requests`: lists `{ data }`, apply → `{ request, order }`.)

Errors are shipping's usual `{ error, code, ...payload }`.

## Routes

`GET /api/v1/pi-submissions?status=&supplierId=&purchaseOrderId=&limit=&offset=`
- `status`: `submitted` (default), `imported`, `rejected`, `withdrawn`, several
  comma-separated, `decided` (imported + rejected + withdrawn) or `all`.
- `limit` defaults to 100, at most 500; `hasMore` says whether there is another page.
- Order: oldest submitted first for `submitted` alone; otherwise newest change first.
- Items leave out the check result and the PO snapshot. `poChanged` is worked out only for
  `submitted` rows with a usable check (false otherwise); `blockers` only for `submitted`.

`POST /api/v1/pi-submissions/:id/import` — body
`{ rowVersion: number, note?: string, recheck?: boolean, bankVerification?: { method, note? } }`
- `rowVersion` is required: the one on the screen.
- `note` (≤ 1000) is shown to the supplier.
- `recheck: true` queues a fresh check of the imported invoice against the PO as it is now
  (`triggeredBy: 'import_recheck'`); it is queued anyway when the check is unusable. It runs
  after the response; the PO page shows it pending until it lands.
- `bankVerification` — `method`: `phone_known_number` | `existing_email_thread` | `other`
  (`other` needs a `note`). **Required** when the check is usable and `bankCheck.state` is
  `changed` or `no_history` (422 `BANK_CHANGE_UNCONFIRMED` without it). **Refused** (400) when
  the check is unusable. Not needed for `unchanged` (recorded as `matches_previous`) or
  `not_extracted` (the payment row then starts `needs_verification`).
- `check` in the answer is the copied supplier check (`triggeredBy: 'supplier_portal'`), or
  the pending re-check when the supplier's check was unusable; `recheck` is the pending
  re-check when both exist.

`POST /api/v1/pi-submissions/:id/reject` — body `{ rowVersion: number, note: string }` (note
required, ≤ 1000, shown to the supplier).

`POST /api/v1/pi-submissions/:id/notify` — body `{ force?: boolean }`. Sends the decision
email of an imported or rejected row. Allowed whenever `notifiedAt` is null, whatever
`notifyError` says; `force: true` only when it was already sent.

`POST /api/v1/purchase-order-invoices/:id/verify-bank` (`:id` = `purchase_order_invoices.id`)
— body `{ method, note?, bankDetails?: BankDetails }`. Records that someone confirmed the
PI's bank details: the payment row becomes `verified`. `bankDetails` are the details as
confirmed with the supplier, and replace the ones on the row: the **only** way to put bank
details on a PI by hand (when the check read none, or misread them). Without `bankDetails`
the details already on the row are verified. Only for portal PIs (409 `NOT_PORTAL_PI`
otherwise).

## Refusals

| Status | Code | When |
| --- | --- | --- |
| 400 | `BAD_FIELD` | bad id / filter / body; a missing reject note; `method: 'other'` without a note; `bankVerification` on an unusable check |
| 403 | `CANNOT_DECIDE` | read-only (or accountant) user on import / reject / notify / verify-bank |
| 404 | `NOT_FOUND` | no such submission (drafts and discarded rows count as not found), or invoice |
| 409 | `NOT_SUBMITTED` | already imported / rejected / withdrawn — payload `status`, `importedInvoiceId` |
| 409 | `STALE` | `rowVersion` no longer matches — reload |
| 409 | `CONFLICT` | someone else was deciding at the same moment, or the same submission was imported twice at once — reload |
| 409 | `NOT_DECIDED` | `notify` on a row that is not imported or rejected — payload `status` |
| 409 | `ALREADY_NOTIFIED` | `notify` without `force` when it was already sent — payload `notifiedAt` |
| 409 | `NO_BANK_DETAILS` | `verify-bank` with no details on the row and no `bankDetails` |
| 409 | `NOT_PORTAL_PI` | `verify-bank` on a staff-uploaded / legacy PI |
| 409 | `BANK_UNVERIFIED` | `PUT /purchase-orders/:poId/invoices/:invoiceId/payment-status` to `arranged` or `paid` on an unverified portal PI |
| 422 | `NOT_IMPORTABLE` | payload `blockers: string[]` (below) |
| 422 | `BANK_CHANGE_UNCONFIRMED` | import without `bankVerification` when it is required — payload `bankCheck: { state, fields }` |
| 422 | `BANK_UNVERIFIED` | `POST/PUT /supplier-payments` with a line on an unverified portal PI — payload `kind`, `id`, `invoiceId` |
| 502 | `FILE_UNAVAILABLE` | the portal's bucket could not be read, or the copy could not be written; try again |

Blockers (`blockers[]`, codes): `not_submitted`, `po_deleted`, `po_supplier_changed` (the PO's
supplier is no longer exactly the name the supplier's portal access was granted on),
`bad_key`, `bad_type`, `too_large`, `no_etag` (the file was never validated by a check),
`file_gone` (no longer in the bucket — e.g. a test database copied from prod),
`file_changed` (the bytes differ from the checked ones). `file_gone` / `file_changed` come
from the detail's look at the bucket and from the import itself.

## Types

```ts
type BankDetails = { beneficiaryName: string|null; bankName: string|null; bankAddress: string|null;
  accountNumber: string|null; iban: string|null; swiftBic: string|null; intermediaryBank: string|null };
type BankState = 'unchanged'|'changed'|'no_history'|'not_extracted'|'not_checked';

type PiSubmission = {             // list item
  id: number; purchaseOrderId: number; poNumber: string|null; supplierId: number; supplierName: string;
  filename: string; contentType: string; fileSize: number;
  status: 'submitted'|'imported'|'rejected'|'withdrawn';
  submittedByEmail: string; submittedAt: string|null; supplierNote: string|null; warningsAcknowledged: boolean;
  checkStatus: 'awaiting_upload'|'pending'|'succeeded'|'failed'; verdict: string|null; discrepancyCount: number|null;
  decidedBy: string|null; decidedAt: string|null; decidedNote: string|null; importedInvoiceId: number|null;
  notifiedAt: string|null; notifyError: string|null; rowVersion: number;
  poChanged: boolean; bankCheck: { state: BankState }; blockers: string[];
};
type PiSubmissionDetail = PiSubmission & {
  check: { status: string; verdict: string|null; discrepancyCount: number|null; result: any|null; modelUsed: string|null;
           inputTokens: number|null; outputTokens: number|null; totalTokens: number|null;
           errorCode: string|null; errorMessage: string|null; checkedAt: string|null;
           attempts: number;                          // extra: check attempts used (of 3)
           usable: boolean; unusableReason: string|null };
  poSnapshot: any|null; poCurrent: any|null;          // loadPoForCheck shape: { poNumber, supplier, buyer, currency, orderDate, notes, lines[], subtotal, shippingTotal, grandTotal }
  poDiff: { changed: boolean;
            comparable: boolean;                      // extra: false when either side is missing (changed is then false)
            header: {field: string; then: any; now: any}[];
            lines: { added: any[]; removed: any[]; changed: {key: string; field: string; then: any; now: any}[] } };
  file: { url: string; expiresAt: string } | null;    // null when the object is gone or unreadable
  poInvoices: { id: number; filename: string; uploadedAt: string; verdict: string|null; piSubmissionId: number|null }[];
  otherPending: { id: number; filename: string; submittedAt: string|null }[];
  importedInvoice: (Invoice & { deleted: boolean; latestCheck: InvoiceCheck|null; payment: Payment|null }) | null;
  canImport: boolean;
  bankCheck: { state: BankState; fields: {field: string; previous: string|null; now: string|null}[];
               current: BankDetails|null;
               previous: { invoiceId: number; poNumber: string|null; verifiedAt: string|null; details: BankDetails } | null };
  audit: { action: string; userEmail: string|null; createdAt: string; after: any }[];
};
type PiSupplierSummary = { supplierId: number; supplierName: string; pending: number; purchaseOrders: number; majorCount: number; oldestSubmittedAt: string|null };
type PiNotification = { sent: boolean; to?: string; reason?: string };
```

`bankCheck.previous` is reported for `not_checked` too (it is what the bank dialog compares
with once details exist). `fields` lists the differing fields for `changed`, and every field
read for `no_history`. `audit` actions: `check_succeeded`, `check_failed` (piCheck),
`import`, `reject`, `notified`, `notify_failed`.

**Fields added to existing JSON**
- Invoice JSON (`GET /purchase-orders/:id/invoices`, the PO bundles on `GET /orders`, …):
  `piSubmissionId: number|null`. When set, `uploadedByEmail` is the supplier's email (display
  only); the staff member who imported it is in the submission's `decidedBy` and the audit.
- Payment JSON (the invoice's `payment`, `GET /purchase-order-invoice-payments`,
  `PUT …/payment-status`, verify-bank): `bankVerificationStatus: 'verified'|'needs_verification'|null`
  (null = not applicable: staff-uploaded and legacy PIs), `bankVerified: BankDetails|null`
  (the details as verified), `bankVerifiedBy`, `bankVerifiedAt`, `bankVerificationMethod`
  (`phone_known_number` | `existing_email_thread` | `other` | `matches_previous`),
  `bankVerificationNote`.
- Invoice check `triggeredBy` gains `'supplier_portal'` (copied at import) and
  `'import_recheck'` (the re-check an import queues).

## Bank verification

A PI that came through the portal (`piSubmissionId` set) is payable **only** when its payment
row exists **and** is `verified`. Import always creates that row. It is enforced where money
moves: `PUT …/payment-status` refuses `arranged` / `paid` (409 `BANK_UNVERIFIED`), and a
supplier transfer refuses a line on it (422 `BANK_UNVERIFIED`) before it can be marked paid.
`pending` and `skipped` are always allowed. Staff-uploaded PIs are not affected.

- **At import**: `unchanged` → verified (`matches_previous`); `changed` / `no_history` → verified
  with the method staff chose; `not_extracted` → `needs_verification`; unusable check →
  an empty row, `needs_verification`, until the re-check fills it.
- **After any re-check** (the import's, the PO page's ↻, any future one): if the check reads
  different details, the PI goes back to `needs_verification` (audited
  `bank_details_unverified` on the purchase order). The same details, formatted differently,
  stay verified: the comparison is on the normalised beneficiary, bank name, IBAN, account
  number, SWIFT and intermediary bank (not the bank's address).
- **"Previous"** = the newest payment row of another live PI of the same supplier: verified
  rows first (their verified details), then legacy PIs with bank details. Portal PIs belong to
  the supplier through `pi_submissions.supplier_id`; legacy PIs through the PO's supplier name
  (exact match). Not `purchase_orders.company_id` — that is the buyer.
- Show `BANK UNVERIFIED` on a PI whose `bankVerificationStatus === 'needs_verification'`, with
  the verify dialog; disable arranged / paid while it is. Warn in the dialog that a later
  re-check re-reads the details and can make the PI unverified again.

## The check (piCheck) and its states

The portal starts it; shipping's claim is authoritative. Error codes in `check.errorCode`:

| `errorCode` | Retried by the supplier? | Portal problem |
| --- | --- | --- |
| `FILE_INVALID` (over 10 MiB, or not the declared type) / `DOCUMENT_UNPROCESSABLE` (400 / 413 / invalid argument / page or payload limit) | no | `file` |
| `PO_NOT_FOUND` / `PO_NO_LINES` | no | `po` |
| `MODEL_UNAVAILABLE` (429 / 5xx / timeout; also an unexpected internal error), `NOT_CONFIGURED` (401 / 403 / no key), `MODEL_NO_JSON`, `MODEL_BAD_OUTPUT` | yes (`RETRYABLE_CODES`) | `unavailable` |

**Shared constants** (copied in the portal's `src/lib/piSubmissions.js` and README §11; keep
both in step):
- `STALE_CHECK_SECONDS = 600` (piCheck's timeout is 300 s, so a claim's run is dead first)
- `MAX_CHECK_ATTEMPTS = 3`
- `RETRYABLE_CODES = [MODEL_UNAVAILABLE, NOT_CONFIGURED, MODEL_NO_JSON, MODEL_BAD_OUTPUT]`
- key regex `^pi/[0-9a-f-]{36}\.(pdf|png|jpg)$`
- allowed types: `application/pdf`, `image/png`, `image/jpeg` (magic bytes: `%PDF-` within the
  first 1024 bytes; the PNG and JPEG signatures)
- 10 MiB (10 × 1024 × 1024 bytes)
- `s3_etag` is stored exactly as S3 returns `ETag`, **quotes included** (`"…"`), by piCheck and
  by the portal's submit; the import reads with `IfMatch` on it.

Triggers: the bucket's EventBridge "Object Created" event (shipping's rule; may claim only
`awaiting_upload`), and a direct async invoke `{ submissionId }` (the portal's `uploaded` and
`recheck`; may also claim a stale `pending` or a retryable failure, and does nothing — no
attempt used — when the object is missing). The Lambda answers nothing to the caller; poll
the row.

## Emails

Templates (`email_templates`, category **`Supplier portal`**, editable in ShipLine's template
editor; looked up by key, so the category never affects sending, and keeps them out of the
manual send dialogs):
- `supplier_pi_imported` — subject `PI accepted – {poNumber}`
- `supplier_pi_rejected` — subject `PI not accepted – {poNumber}`, with the reason and a link
  to upload a corrected PI

Tokens (body; all HTML-escaped by the sender): `{poNumber}` `{filename}` `{note}` (the staff
note as text) `{noteHtml}` (the note as a paragraph, or nothing) `{portalLinkHtml}` (a link
into the portal, or nothing when `SUPPLIER_PORTAL_URL` is unset). The subject takes
`{poNumber}` only. Seeded by `src/db/migrate/2026-10-20_20_seed_supplier_pi_email_templates.js`
(INSERT IGNORE: an edit is never overwritten).

Recipient: the submission's `submitted_by_email` (the uploader). On test, `PI_NOTIFY_REDIRECT_TO`
sends every one there instead (the subject is prefixed `[for <supplier address>]`).

**Double send:** if the process dies after Front accepts the email but before `notified_at`
is written, a later `notify` sends it again. Accepted: it is rare, and a duplicate "PI
accepted" is harmless.

## Configuration

`serverless.yml` (provider environment):
- `PI_UPLOADS_BUCKET: jportal-pi-uploads-${self:custom.envName}` — the portal's bucket (its
  stack creates it). Shipping `dev` stage = envName `prod` = portal stage `prod`.
- `JPORTAL_DB_SCHEMA` (already there; default `jportal`).
- `SUPPLIER_PORTAL_URL`, `PI_NOTIFY_REDIRECT_TO` — from the `shipping/<env>` secret, both
  optional (blank = no link / no redirect). **Set `PI_NOTIFY_REDIRECT_TO` in `shipping/test`;
  leave it absent in `shipping/prod`.**
- IAM: `s3:GetObject` on `arn:aws:s3:::jportal-pi-uploads-<env>/pi/*`, and `s3:ListBucket`
  on the bucket (only so a missing object answers 404 rather than 403). No tagging, no delete:
  the portal owns the tags and the lifecycle.
- Function `piCheck`, named **`jportal-pi-check-<env>`** (the portal invokes it by that name),
  300 s, 1024 MB, reserved concurrency 3, one async retry, its EventBridge rule on the
  portal bucket by name.

## Database grants (run by hand, as admin)

Shipping's login (`DB_USER` of `shipping/<env>`) reads and decides `jportal.pi_submissions`,
but never creates or deletes its rows:

```sql
GRANT SELECT ON jportal.pi_submissions TO '<shipping login>'@'%';
GRANT UPDATE (check_status, check_attempt, check_started_at, verdict, discrepancy_count, result_json,
              po_snapshot_json, model_used, input_tokens, output_tokens, total_tokens, error_code,
              error_message, checked_at, s3_etag, file_size,
              status, decided_by, decided_at, decided_note, imported_invoice_id, notified_at, notify_error,
              row_version)
      ON jportal.pi_submissions TO '<shipping login>'@'%';
```

(Use the test schema name on test if `JPORTAL_DB_SCHEMA` differs.) On test, confirm that
`SELECT … FOR UPDATE` works with the column-level UPDATE grant (MySQL wants UPDATE on at
least one column for a locking read); if it does not, grant table-level `UPDATE` instead.
The previous-bank lookup joins `jportal.pi_submissions`, which the SELECT covers. No INSERT on
`jportal.audit_logs`: shipping's own trail is its `audit_log` (`entity_type 'pi_submission'`).

The TEST suites write fixtures through `FIXTURE_DB_USER` / `FIXTURE_DB_PASSWORD` (a login
with INSERT + DELETE on `<jportal>.pi_submissions` on test), falling back to `DB_USER`.

## Deploy order

Migrations (deploy.sh applies them before the code): `2026-10-20_10_pi_submission_invoice_link.sql`
(`purchase_order_invoices.pi_submission_id`, unique), `_20_seed_supplier_pi_email_templates.js`,
`_30_invoice_bank_verification.sql` (the `bank_*` columns on `purchase_order_invoice_payments`).

1. Portal on test (flag off): its migration creates `jportal.pi_submissions`, its stack the
   bucket with EventBridge on.
2. Shipping on test: the grants above; `SUPPLIER_PORTAL_URL` and `PI_NOTIFY_REDIRECT_TO` in
   `shipping/test`; `bash deploy.sh test`; then `npm run test:unit`,
   `node tools/test-pi-check-flow.js`, `node tools/test-pi-submissions.js`,
   `PI_NOTIFY_REDIRECT_TO=<mailbox> node tools/test-pi-import-flow.js`.
3. Portal flag on for test; the portal's smoke test.
4. ShipLine on test.
5. Production: portal (flag off) → grants for both logins → shipping (`bash deploy.sh`, stage
   `dev`) → ShipLine → flip the portal flag. Shipping must be live before the flag flips.

`piCheck`'s EventBridge rule can be deployed before the bucket exists (it matches by name).
