# Supplier commercial invoices (CI submissions) — frontend guide

Routes: `src/services/ci-submission-routes.js` · rules and ranking: `src/lib/ci-submissions.js` ·
checker: `src/services/ci-check.js` (Lambda `jportal-ci-check-<env>`, handler `src/handlers/ci-check.js`) ·
reader: `src/services/shipment-payment-extract.js` (`readDocument`, `assessFit`,
`runShipmentPaymentExtraction`) · tests: `tools/test-ci-submissions-lib.js` (unit, in
`npm run test:unit`) · data: `jportal.ci_submissions`, owned by the supplier portal (the `portal` repo;
its README §13 "Commercial-invoice uploads" is the other half of this contract), and the
`shipment_payment_documents` rows an import makes.

A **commercial invoice** is the supplier's invoice for the goods in a shipment — the document staff
otherwise receive by email and upload on the shipment (`POST /api/v1/shipment-payment-documents`,
`docKind: balance_invoice`, see [shipment-payments-frontend.md](shipment-payments-frontend.md)). It is
**not** a proforma: a PI is per purchase order and has its own flow
([pi-submissions-frontend.md](pi-submissions-frontend.md)).

Suppliers upload one on the supplier portal's "Send invoices" page **without choosing a shipment** —
they do not know our shipment numbers. The portal sends it to staff as soon as the file has arrived
and shows the supplier only "Sent to our team", then "Filed (shipment 324)" or "Not accepted: <note>".
Shipping's `ciCheck` Lambda reads it with **the balance-invoice reader's own prompt and schema**
(told it came through the portal, shipment unknown) and **suggests which of the supplier's booked
shipments it belongs to**. Staff confirm or choose the shipment here and **import** it: it becomes an
ordinary `shipment_payment_documents` row on that shipment (`docKind: balance_invoice`,
`extractStatus: processing`) and is **read in the background exactly as an emailed invoice uploaded
by staff is** — `fit`, `check`, the per-PO split, on the Payments page with the shipment's other
documents. Or staff **reject** it with a note the supplier sees. No email.

```
portal:   draft ──sent on upload──▶ submitted ──withdraw──▶ withdrawn
            │                       │
            └─discard─▶ discarded   ├──import──▶ imported   (shipment_payment_documents row, read as usual)
shipping:                           └──reject──▶ rejected   (note shown to the supplier)
```

**Staff always choose the shipment.** The check only suggests one, and nothing reaches
`shipment_payment_documents` without an Import. Drafts and discarded rows are never shown to staff.
A submitted row may still be `checkStatus: pending` for a minute or two: Import does not wait for it
(the read after import is the one that matters for payments).

## The page

ShipLine's **SUPPLIER PORTAL** tab, sub-tab **Invoices** (beside Proforma invoices and Packing lists),
Pending and History tabs:

- **Badge / supplier filter** — `GET /api/v1/ci-submissions/suppliers`: per supplier group, the
  pending count, the total and the oldest pending.
- **List** — `GET /api/v1/ci-submissions?status=pending&supplierKey=s:11` and `?status=history`. Each
  item carries the suggestion (`matchConfidence`, `matchReason`, `suggested.shipmentReference`), the
  invoice number read, and `blockers`.
- **Review panel** — `GET /api/v1/ci-submissions/:id`:
  - the file (`fileUrl`; a 15-minute link while it is only in the portal's bucket — reload after
    `fileExpiresAt`; an imported one links to its permanent copy, `fileExpiresAt: null`);
  - what was read (`extraction`: supplier as printed, invoice number and date, currency, totals,
    container / B/L / PO references, lines, other charges, bank details);
  - **the suggestion**: `candidates.live` (re-ranked on every load against today's shipments from
    the stored reading — a shipment booked after the upload shows up here without a re-read) with
    `liveConfidence` / `liveReason`, else `candidates.stored` (as the check ranked them). Show each
    candidate's reference, carrier box number, stage, ETD, `poHits`, goods vs on board
    (`goods` / `onBoard`, `amountRatio`) and its `checks` (the same `fit` lines the Payments page
    shows: supplier, container, POs, currency/amount, each ✓ / ✗ / –);
  - pre-select the top candidate when the confidence is `confident`; with `unsure` pre-select it
    but say "please confirm"; with `none` (or no reading: `liveError`) pre-select nothing and open
    the **shipment picker** (search any booked shipment by reference or box number);
  - **Preview** — `GET /api/v1/ci-submissions/:id/preview?shipmentId=61` for a candidate or a picked
    shipment: the same evidence for that shipment, who is on it (`suppliers`), the name it would be
    filed under (`supplierName`), and `duplicateDocumentId` when the same file is already there;
  - "Read again" (`canRecheck`) — `POST …/recheck`, then poll the detail while `check.status` is
    `pending`.
- **Import** — on the chosen shipment. Confirm `force` when the supplier has no POs on it (422
  `SUPPLIER_NOT_ON_SHIPMENT`, with `suppliers` to pick another name from) or the file is already
  there (409 `DUPLICATE_DOCUMENT` — usually the same invoice that also came by email: prefer
  rejecting it with "already received"). After import, "Open payments" goes to the shipment on the
  Payments page, where the document shows with its read (`document.extractStatus`).
- **Reject** — the note is required and shown to the supplier word for word.

Who: admin and standard users import, reject and re-read (`X-User-Type`); read-only users only look
(403 `CANNOT_DECIDE`).

## Response envelope

Shipping's convention: **lists are wrapped in `{ data }`; a single object and the answer of an action
are NOT wrapped.** ShipLine reads `json.data ?? json` either way.

| Route | Answers with |
| --- | --- |
| `GET /api/v1/ci-submissions` | `200 { data: CiSubmission[], hasMore: boolean }` |
| `GET /api/v1/ci-submissions/suppliers` | `200 { data: CiSupplierSummary[] }` |
| `GET /api/v1/ci-submissions/:id` | `200 CiSubmissionDetail` — **not** `{ data: … }` |
| `GET /api/v1/ci-submissions/:id/preview` | `200 CiPreview` — **not** wrapped |
| `POST /api/v1/ci-submissions/:id/import` | `200 { submission: CiSubmission, document: ShipmentPaymentDocument }` |
| `POST /api/v1/ci-submissions/:id/reject` | `200 { submission: CiSubmission }` |
| `POST /api/v1/ci-submissions/:id/recheck` | `202 { submission: CiSubmission }` |

Errors are shipping's usual `{ error, code, ...payload }`.

## Routes

`GET /api/v1/ci-submissions?status=&supplierKey=&supplierId=&limit=&offset=`
- `status`: `pending` (= `submitted`, the default), `history` (= `imported`, `rejected`, `withdrawn`),
  or any of `submitted` / `imported` / `rejected` / `withdrawn` comma-separated, `decided`, `all`.
- `supplierKey` (`s:<id>`) and `supplierId` both filter on the row's `supplierId` — the lowest id of
  the uploader's group, which the portal writes at create.
- `limit` defaults to 100, at most 500; `hasMore` says whether there is another page.
- Order: oldest submitted first for `submitted` alone; otherwise newest change first.
- Items leave out the reading and the candidates (the detail has them). `blockers` only for
  `submitted` rows.

`GET /api/v1/ci-submissions/suppliers` — one entry per `supplierId` (= per group): `supplierKey`,
`supplierId`, `supplierName` (live name, else the rows' snapshot), `supplierIds` (the union of the
rows' `group_supplier_ids`), `pending`, `total`, `oldestSubmittedAt`. Pending first, oldest first.

`GET /api/v1/ci-submissions/:id` — see `CiSubmissionDetail`. `candidates.live` is computed only for a
`submitted` row with a reading; `liveError` says why it is null ("The file has not been read.",
"The supplier no longer exists.", or a load error — show the stored ones with a note).

`GET /api/v1/ci-submissions/:id/preview?shipmentId=61 | ?shipmentReference=324 [&supplierName=…]`
- Any stage: `shipment.booked` says whether Import will take it (it refuses `NOT_BOOKED`).
- `supplierName` (optional): judge it as filed under that name (one from `suppliers`); else the
  group's spelling on that shipment's POs with the most on board.
- `evidence` is null when there is no reading (`extractionUsable: false`); `suppliers` still lists
  who is on board, `inGroup` marking the uploader's own names.
- No Gemini call; 404 `SHIPMENT_NOT_FOUND`, 409 `MERGED { mergedIntoId }` (preview the survivor).

`POST /api/v1/ci-submissions/:id/import` — body
`{ rowVersion, shipmentId | shipmentReference, supplierName?, note?, force? }`
- `rowVersion`: the `submission.rowVersion` you were looking at (`STALE` otherwise: the check or the
  supplier changed the row — reload).
- The shipment must be live and booked (`BOOKED`, `IN_TRANSIT`, `ARRIVED`, `CLOSED`), like a staff
  upload's.
- The document's `supplierName` (what the Payments page groups by): the body's `supplierName` when it
  is one of the shipment's PO supplier names (sent back in that PO's spelling), else the group's
  spelling there with the most on board. None → 422 `SUPPLIER_NOT_ON_SHIPMENT` unless `force`, which
  files it under the body's name, else the suggestion's, else the portal row's.
- A live document with the same file name and size on the shipment → 409 `DUPLICATE_DOCUMENT
  { existingId }` unless `force`.
- `note` (optional) is shown to the supplier with "Filed".
- What it writes: the bytes copied (exactly the ones the portal validated, `IfMatch` its ETag) to the
  PO bucket at `shipment-payments/<uuid>/<name>`; a `shipment_payment_documents` row (`doc_kind
  balance_invoice`, `extract_status processing`, `uploaded_by_email` = **the supplier's** email,
  `notes` "Uploaded by the supplier through the supplier portal (invoice upload <id>).") with its
  `create` audit (`after.source: 'portal'`, `ciSubmissionId`); the submission `imported` with
  `documentId` and `confirmed`. After the response, `runShipmentPaymentExtraction` — the background
  read every staff upload gets. Poll `GET /api/v1/shipment-payment-documents?shipmentId=` (or the
  Payments page's feed) for its `extractStatus`.

`POST /api/v1/ci-submissions/:id/reject` — body `{ rowVersion, note }`; the note is required (at most
1000 characters).

`POST /api/v1/ci-submissions/:id/recheck` — no body. Runs the check again **after the response**, in
the API Lambda (the staff claim: any submitted row not being read right now, up to
`check.maxStaffAttempts` reads in all). For a read that failed, never started (`awaiting_upload`
past ten minutes), or looks wrong. A failed re-read leaves the previous suggestion in place, with
`check.status: 'failed'`. 409 `NOT_RECHECKABLE { reason }`: `in_progress`, `attempts_exhausted`,
`not_submitted`.

## Refusals

| Status | Code | When |
| --- | --- | --- |
| 400 | `BAD_FIELD` | bad id / filter / body; no shipment; a missing reject note; a `supplierKey` that is not `s:<id>` |
| 403 | `CANNOT_DECIDE` | read-only user on import / reject / recheck |
| 404 | `NOT_FOUND` | no such submission (drafts and discarded rows count as not found) |
| 404 | `SHIPMENT_NOT_FOUND` | no live shipment with that id or reference |
| 409 | `NOT_SUBMITTED` | already imported / rejected / withdrawn — payload `status`, `documentId` |
| 409 | `STALE` | `rowVersion` no longer matches — reload |
| 409 | `CONFLICT` | someone else was deciding at the same moment — reload |
| 409 | `MERGED` | the shipment was merged — payload `mergedIntoId`; use the survivor |
| 409 | `DUPLICATE_DOCUMENT` | the same file name and size is already on that shipment — payload `existingId`; `force: true` files it anyway |
| 409 | `NOT_RECHECKABLE` | recheck refused — payload `reason` |
| 422 | `NOT_IMPORTABLE` | payload `blockers: string[]` (below) |
| 422 | `NOT_BOOKED` | the shipment is not booked yet — payload `stage` |
| 422 | `SUPPLIER_NOT_FOUND` | none of the row's group ids has a live `suppliers` row |
| 422 | `SUPPLIER_NOT_ON_SHIPMENT` | the supplier (or the body's `supplierName`) has no POs on that shipment — payload `suppliers`; retry with `force: true` or another `supplierName` |
| 502 | `FILE_UNAVAILABLE` | the portal's bucket could not be read, or the copy could not be written; try again |

Blockers (`blockers[]`, codes): `not_submitted`, `supplier_not_found`, `bad_key`, `bad_type`,
`too_large`, `no_etag` (the portal never validated the bytes), `file_gone` (no longer in the portal's
bucket: it keeps `ci/*` objects 180 days; a test database copied from prod), `file_changed` (the bytes
differ from the validated ones).

## Types

```ts
type CiStatus = 'submitted'|'imported'|'rejected'|'withdrawn';
type CiMatchConfidence = 'confident'|'unsure'|'none';
type CiMatchReason = 'container'|'po_numbers'|'amount'|'single'|'conflict';
type FitCheck = { key: 'supplier'|'container'|'pos'|'currency'|'amount'; ok: boolean|null; text: string };

type CiSubmission = {              // list item; the `submission` of the detail and of the actions
  id: number; supplierId: number; supplierKey: string; supplierName: string;   // supplierKey = 's:<supplierId>'
  groupSupplierIds: number[]; supplierExists: boolean|null;
  submittedByEmail: string; filename: string; contentType: string; fileSize: number|null;
  status: CiStatus; checkStatus: 'awaiting_upload'|'pending'|'succeeded'|'failed'; errorCode: string|null;
  invoiceNumber: string|null;
  matchConfidence: CiMatchConfidence|null; matchReason: CiMatchReason|null;
  suggested: { shipmentId: number|null; shipmentReference: string|null; supplierName: string|null }|null;
  confirmed: { shipmentId: number|null; shipmentReference: string|null; supplierName: string|null }|null;   // imported only; supplierName = the document's (filed under), not the row's
  documentId: number|null;                                                      // the shipment_payment_documents row
  submittedAt: string|null; decidedBy: string|null; decidedAt: string|null; decidedNote: string|null;
  rowVersion: number; createdAt: string|null;
  blockers: string[];
};

type CiCandidate = {
  shipmentId: number; reference: string|null; name: string|null; stage: string|null; mode: string|null;
  trackingRef: string|null; blNumber: string|null; bookingRef: string|null; etd: string|null; eta: string|null;
  supplierName: string|null;          // the name it would be filed under there
  onBoard: number|null; currency: string|null;   // the supplier's goods on board, at our prices
  goods: number|null;                 // the goods value the invoice bills (its lines, else total less charges)
  amountRatio: number|null;           // goods / onBoard
  container: boolean|null; pos: boolean|null;    // the fit checks that decide: true ties it here, false points elsewhere
  poHits: string[];                   // the supplier's POs on board that the invoice names
  amountOver: boolean; currencyMiss: boolean;
  fitVerdict: 'match'|'unconfirmed'|'mismatch';
  checks: FitCheck[];
  score: number; rank: number;
};

type CiSubmissionDetail = {
  submission: CiSubmission;
  check: { status: string; modelUsed: string|null; inputTokens: number|null; outputTokens: number|null; totalTokens: number|null;
           errorCode: string|null; errorMessage: string|null; checkedAt: string|null; attempts: number; maxStaffAttempts: number;
           invoiceNumber: string|null };
  extraction: (ShipmentPaymentExtract & { supplierPrinted: string|null })|null;   // the reader's schema
  candidates: {
    considered: number; stored: CiCandidate[];
    live: CiCandidate[]|null; liveConsidered: number|null;
    liveConfidence: CiMatchConfidence|null; liveReason: CiMatchReason|null; liveError: string|null;
  };
  fileUrl: string|null; fileExpiresAt: string|null;
  document: (ShipmentPaymentDocument & { deleted: boolean })|null;   // once imported: extractStatus, extracted.fit / .check …
  canImport: boolean; canRecheck: boolean;
  audit: { action: string; userEmail: string|null; createdAt: string|null; after: unknown }[];
};

type CiPreview = {
  shipment: { id: number; reference: string|null; name: string|null; stage: string|null; mode: string|null;
              trackingRef: string|null; blNumber: string|null; bookingRef: string|null; etd: string|null; eta: string|null;
              booked: boolean };
  supplierName: string|null; groupOnBoard: boolean;
  suppliers: { supplierName: string; poNumbers: string[]; onBoard: number; inGroup: boolean }[];
  evidence: CiCandidate|null; extractionUsable: boolean;
  duplicateDocumentId: number|null;
};
```

## The check (ciCheck) and the suggestion

The portal starts the check (the bucket's EventBridge event for `ci/*`, or a direct `{ submissionId }`
invoke from its `uploaded`); shipping's claim is authoritative (PL's `planEvent` / `claimWhere`: a draft
or a submitted row). Then: GetObject, size and magic bytes, pin `s3_etag` + `file_size`; the supplier
group (`SUPPLIER_NOT_FOUND` before any model call) and its QC units; `readDocument` with the bytes and
a context saying "sent through the supplier portal, shipment unknown: copy every container, B/L and PO
reference as printed"; the suggestion; one guarded UPDATE of the check and match column groups. A
document that is not an invoice (a remittance, say) still succeeds — `extraction.documentKind` says so
and staff decide.

| `errorCode` | Retried by | Note |
| --- | --- | --- |
| `FILE_INVALID` (over 10 MiB, or not the declared type) | staff only | the portal validates the bytes too, so rare |
| `SUPPLIER_NOT_FOUND` | staff only | the group has no live supplier |
| `DOCUMENT_UNPROCESSABLE` (400 / 413 / invalid argument / page or payload limit) | staff only | choose the shipment by hand |
| `MODEL_UNAVAILABLE` (429 / 5xx / timeout; also an unexpected internal error), `NOT_CONFIGURED`, `MODEL_NO_JSON`, `MODEL_BAD_OUTPUT` | the portal's direct invoke (`RETRYABLE_CODES`) and staff | |

**Candidates** (`ci-check.js loadCandidates`): the group's shipments in `BOOKED`, `IN_TRANSIT`,
`ARRIVED` or `CLOSED`, live and not merged, holding a live order line of a purchase order filed under
one of the group's names (`purchase_orders.supplier IN (names)`); open ones first (`CLOSED` last),
newest ETD first, at most `CANDIDATE_LIMIT` (30). For each, `candidateEvidence`:
`assessFit` (the staff upload's "does it belong here" — container / B/L / booking against the
shipment's reference, carrier box number, B/L and booking reference; PO references against the POs on
board; currency; amount against what the supplier has on board), the group's POs the invoice names
there (`poHits`, matched on letters + digits, then on the digit core, as `matchPoRef`), and the goods
value it bills against the supplier's goods on board (`amountRatio`).

**Ranking** (`lib/ci-submissions.js rankCandidates`), the top one suggested:

| Confidence | Reason | When |
| --- | --- | --- |
| confident | `container` | the invoice prints this shipment's container, B/L or booking number, and no other candidate's (or another's too, but this one has more of its POs) |
| confident | `po_numbers` | it names more of the supplier's POs here than on any other candidate, and nothing points elsewhere |
| confident | `amount` | its POs are on several candidates (a split PO) and only this one's goods value matches (within 5 %) |
| confident | `single` | the supplier's only booked shipment, nothing contradicts it, and the goods value matches |
| unsure | `conflict` | two candidates tie on the deciding evidence |
| unsure | `po_numbers` | POs named, but something points elsewhere (the amount is over what is on board, another currency) |
| unsure | `single` | the only booked shipment, with nothing to confirm it |
| none | — | nothing ties it anywhere: `suggested` is null |

`candidates_json` keeps the top five with their evidence (`{ v: 1, considered, groupNames, top }`).

## The supplier group

As for packing lists ([pl-submissions-frontend.md](pl-submissions-frontend.md), "The supplier
group"): the portal writes `group_supplier_ids` (every live supplier id the uploader is granted,
ascending), `supplier_id` (the lowest) and `supplier_name` at create; shipping reads the group
(`pl-results.js portalGroup`) and never writes it. A login holding two companies (Sunmed and
Medoffice) has both in its group: the candidates are both companies' shipments, and each candidate's
`supplierName` is the spelling on that shipment's POs with the most on board — so the document is filed
under the company whose goods are in the box, and staff can pick the other from `suppliers`.

## Configuration

`serverless.yml`:
- `PI_UPLOADS_BUCKET: jportal-pi-uploads-<env>` — the portal's bucket, read under `ci/*` too.
- IAM: `s3:GetObject` on `arn:aws:s3:::jportal-pi-uploads-<env>/ci/*` (next to `pi/*` and `pl/*`) for
  the shared role, so both `ciCheck` and `ordersApi` (import, the file link, staff re-reads) can read
  it; `s3:ListBucket` on the bucket (a missing object answers 404). No tagging, no delete.
- Function `ciCheck`, named **`jportal-ci-check-<env>`**, 300 s, 1024 MB, reserved concurrency 3, one
  async retry, its EventBridge rule on the portal bucket with prefix `ci/`, the provider VPC and env
  (`GEMINI_API_KEY`; the reader's models are `shipment-payment-extract.js`'s).
- Alarm `jportal-ci-check-<env>-errors` (`AlarmActions`: `ALERTS_TOPIC_ARN`, as for the PL alarms).
- The portal's `deploy.sh` checks `jportal-ci-check-<stage>` exists and that a rule naming the bucket
  targets it, like the PI and PL checks.

## Database grants (run by hand, as admin)

Shipping's login (`DB_USER` of `shipping/<env>`) reads the whole table and writes its three column
groups; it never creates or deletes rows, and never writes `group_supplier_ids`.

```sql
GRANT SELECT ON jportal.ci_submissions TO '<shipping login>'@'%';
-- check
GRANT UPDATE (check_status, check_attempt, check_started_at, error_code, error_message, checked_at,
              model_used, input_tokens, output_tokens, total_tokens, s3_etag, file_size,
              extract_json, invoice_number)
      ON jportal.ci_submissions TO '<shipping login>'@'%';
-- match
GRANT UPDATE (candidates_json, match_confidence, match_reason, suggested_shipment_id,
              suggested_shipment_reference, suggested_supplier_name)
      ON jportal.ci_submissions TO '<shipping login>'@'%';
-- decision
GRANT UPDATE (status, decided_by, decided_at, decided_note, document_id, confirmed_shipment_id,
              confirmed_shipment_reference, row_version)
      ON jportal.ci_submissions TO '<shipping login>'@'%';
```

(Use the test schema name on test if `JPORTAL_DB_SCHEMA` differs.) `SELECT … FOR UPDATE` needs UPDATE
on at least one column, which the groups give. No grant on `jportal.audit_logs`: shipping's own trail
is its `audit_log` (`entity_type 'ci_submission'`: `check_succeeded`, `check_failed`, `recheck`,
`import`, `reject`; and `'shipment_payment_document'` `create` for the filing).

## The shared contract

| Constant | Portal (`src/lib/ciSubmissions.js`) | Shipping (`src/lib/ci-submissions.js`) |
| --- | --- | --- |
| Key pattern | `CI_KEY_RE` `^ci/[0-9a-f-]{36}\.(pdf\|png\|jpg)$` | `CI_KEY_RE`, the same |
| Types, size | PDF, PNG and JPEG by magic bytes; 10 MiB | the same (PI's `sniffType`, `MAX_FILE_BYTES`) |
| `STATUSES` | `draft`, `submitted`, `imported`, `rejected`, `withdrawn`, `discarded` | the same |
| `STALE_CHECK_SECONDS` / `MAX_CHECK_ATTEMPTS` / `RETRYABLE_CODES` | not read (the supplier is shown no check) | 600 / 3 / PI's, for the portal's triggers; staff re-reads up to `STAFF_MAX_CHECK_ATTEMPTS` (5) |
| `group_supplier_ids` | written at create | read as the group; never written |
| `s3_etag` | pinned when the invoice is sent (`uploaded`) | pinned by the check (same bytes, same value); Import reads `IfMatch` it |
| `match_confidence` / `match_reason` / `suggested_*` / `candidates_json` | not readable by the portal login | written by the check |
| `decided_note`, `confirmed_shipment_reference` | shown to the supplier ("Not accepted: …", "Filed (shipment 324)") | written at reject / import |
| `decided_by`, `document_id`, `confirmed_shipment_id` | not readable by the portal login | written at import |
| `CI_DRAFT_LIMIT` / `CI_CREATE_LIMIT` | 10 / 30 (the daily cap on Gemini reads a user can cause) | — |

Who writes what in `ci_submissions`: the portal creates rows and moves `status` draft→submitted,
draft→discarded and submitted→withdrawn (with `submitted_at`, `row_version`, and `s3_etag` where it is
NULL); shipping writes the check, match and decision groups above with its own login.

## Deploy order

No shipping migration: the table is the portal's, and `shipment_payment_documents` is unchanged.

1. Portal on test (flag off): its migration creates `jportal.ci_submissions`; the bucket already
   exists (the PI one), its EventBridge notifications on.
2. Shipping on test: the grants above; `bash deploy.sh test` (`ciCheck` and its rule, the routes);
   `npm run test:unit`.
3. Portal flag on for test (`CI_UPLOAD_ENABLED=true`, `put-secret`, redeploy — its wiring check covers
   `jportal-ci-check-test`); upload an invoice; see the suggestion here; import it; the document shows
   on the Payments page with its read; the portal shows "Filed".
4. ShipLine on test (the Invoices sub-tab).
5. Production: portal (flag off) → grants → shipping (`bash deploy.sh`, stage `dev` = envName `prod`) →
   ShipLine → flip the portal flag. Shipping must be live before the flag flips.
