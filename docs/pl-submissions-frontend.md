# Supplier packing lists (PL submissions) — frontend guide

Routes: `src/services/pl-submission-routes.js` · rules: `src/lib/pl-submissions.js` · matcher:
`src/lib/pl-match.js` · results: `src/services/pl-results.js` · checker: `src/services/pl-check.js`
(Lambda `jportal-pl-check-<env>`, handler `src/handlers/pl-check.js`) · refresh: Lambda
`jportal-pl-refresh-<env>` (handler `src/handlers/pl-refresh.js`) · tests:
`tools/test-pl-match-lib.js`, `tools/test-pl-submissions-lib.js` (unit, in `npm run test:unit`),
`tools/test-pl-check-flow.js`, `tools/test-pl-submissions.js` (TEST stage) · data:
`jportal.pl_submissions`, owned by the supplier portal (the `portal` repo; its README §12 "Packing-list
uploads" is the other half of this contract), plus `packing_lists.source` / `portal_submission_id`
(`src/db/migrate/2026-10-21_10_packing_list_portal_source.sql`).

Suppliers upload a packing list (PL) in the supplier portal's "Send packing lists" page. Shipping's
`plCheck` Lambda reads it with **the staff check's own prompt and schema** (`readPackingList`),
**suggests which of the supplier's containers it belongs to** (booked containers and open drafts holding
live lines of the uploader's supplier group — see "The supplier group") and writes a supplier-safe result. The supplier sees "Suggested: container
324" with a provisional breakdown, or "Our team will match this to a shipment", and sends it. Staff
confirm the container here and **import** it as an ordinary packing list (`source: 'portal'`) that
reuses the stored extraction — no second Gemini read — into the existing packing-list check, or
**reject** it with a note the supplier sees. The supplier's page then shows "Confirmed by our team"
with the live pooled check, kept current by the `plRefresh` Lambda on every page open. No email.

```
portal:   draft ──submit──▶ submitted ──withdraw──▶ withdrawn
            │                   │ ▲
            └─discard─▶ discarded │ │ returned to Pending (its packing list or container deleted)
shipping:                       ├─┼──import──▶ imported   (packing_lists row, source 'portal')
                                └──reject───▶ rejected   (note shown to the supplier)
```

**Staff always confirm the container.** The matcher only suggests one; nothing reaches
`packing_lists` without an Import. Drafts and discarded rows are never shown to staff.

## The page

ShipLine's **SUPPLIER PORTAL** tab, sub-tab **Packing lists**, Pending and History tabs:

- **Badge / supplier filter** — `GET /api/v1/pl-submissions/suppliers`: per supplier (one entry per
  group, keyed by the rows' `supplierId`), the pending count, the total and the oldest pending.
- **List** — `GET /api/v1/pl-submissions?status=pending&supplierKey=s:11` and `?status=history`.
  Each item carries the match (`matchConfidence`, `matchReason`, `suggestedLabel`), the supplier's
  note and acknowledgement, `resultState` (`replaced`, `in_review` chips) and `blockers`.
- **Detail** — `GET /api/v1/pl-submissions/:id`: the file (`fileUrl`, a 15-minute link — reload after
  `fileExpiresAt`), the extraction, **what the supplier acknowledged** (`acknowledged`, or "supplier
  saw no result"), the ranked candidates with their evidence and the conflicts (`candidates`), the
  linked or previous packing list, and the replace choice to pre-select (`defaultReplacesId`,
  `defaultContainer`).
- **Preview** — `GET /api/v1/pl-submissions/:id/preview?draftContainerId=129` (a picked candidate —
  always by `draftContainerId` for a draft, so it follows a rename or a booking; `containerNumber`
  for a booked one or a typed-in reference): the full staff comparison, pooled with the supplier's
  PLs already there, rendered through the same component as `GET /packing-lists/check`.
- **Import** — pick a candidate or type any container, optionally **replace** an existing PL
  (`replacesId`), confirm `force` when the supplier has no lines there (422
  `SUPPLIER_NOT_ON_CONTAINER`). After import, "Open container check" opens the container's packing
  review as usual.
- **Request correction** — `reject`: the note is required and shown to the supplier word for word.

Who: admin and standard users import and reject (`X-User-Type`); read-only users only look (403
`CANNOT_DECIDE`).

## Response envelope

Shipping's convention: **lists are wrapped in `{ data }`; a single object and the answer of an action
are NOT wrapped.** ShipLine reads `json.data ?? json` either way.

| Route | Answers with |
| --- | --- |
| `GET /api/v1/pl-submissions` | `200 { data: PlSubmission[], hasMore: boolean }` |
| `GET /api/v1/pl-submissions/suppliers` | `200 { data: PlSupplierSummary[] }` |
| `GET /api/v1/pl-submissions/:id` | `200 PlSubmissionDetail` — **not** `{ data: … }` |
| `GET /api/v1/pl-submissions/:id/preview` | `200 PlSubmissionPreview` — **not** wrapped |
| `POST /api/v1/pl-submissions/:id/import` | `200 { submission: PlSubmission, packingList: PackingList }` |
| `POST /api/v1/pl-submissions/:id/reject` | `200 { submission: PlSubmission }` |

Errors are shipping's usual `{ error, code, ...payload }`.

## Routes

`GET /api/v1/pl-submissions?status=&supplierKey=&supplierId=&limit=&offset=`
- `status`: `pending` (= `submitted`, the default), `history` (= `imported`, `rejected`, `withdrawn`),
  or any of `submitted` / `imported` / `rejected` / `withdrawn` comma-separated, `decided`, `all`.
- `supplierKey` (`s:<id>` from `/suppliers`) and `supplierId` both filter on the row's `supplierId`
  — the lowest id of the uploader's group, which the portal writes at create (see "The supplier
  group" below). `supplierKey` is simply `s:<supplierId>`, kept for ShipLine; both are accepted.
- `limit` defaults to 100, at most 500; `hasMore` says whether there is another page.
- Order: oldest submitted first for `submitted` alone; otherwise newest change first.
- Items leave out the extraction, the candidates and the result (the detail has them). `blockers`
  only for `submitted` rows.

`GET /api/v1/pl-submissions/suppliers` — every supplier with rows staff can see, one entry per
`supplierId` (= per group): `supplierKey` (`s:<supplierId>`), `supplierId`, `supplierName` (the live
`suppliers.name` of that id, else the rows' snapshot), `supplierIds` (the union of the rows'
`group_supplier_ids`). Suppliers with pending rows first (oldest first), then by name.

`GET /api/v1/pl-submissions/:id` — see `PlSubmissionDetail`. `candidates.live` is re-scored on every
call (cheap: the lines of the supplier's open containers); `candidates.stored` is what the check saw.
If the live load fails, `live` is null and `liveError` says why — show the stored ones with a note.

`GET /api/v1/pl-submissions/:id/preview?containerNumber=324 | ?draftContainerId=129 [&replacesId=7]`
- `containerNumber` is resolved as `/packing-lists/context` does (a booked reference, the carrier's
  box number, an exact draft name, or the number an open draft reserves). A stored suggestion for a
  draft must be sent as `draftContainerId`: `followDraft` follows it through renames and bookings,
  and `container.followedFrom` says "now booked as 324".
- `replacesId` drops that packing list out of the pool, as the import with it will.
- `onBoard: false` means the supplier's group has no live line there — no line whose order's or
  PO's supplier name is one of the group's names (the import will need `force`). `packingLists` are
  the active packing lists filed under the group's keys (`s:<id>` for each granted id: what the
  comparison pools).
- A deleted draft answers 422 `CONTAINER_EMPTY` ("suggested draft no longer exists").

`POST /api/v1/pl-submissions/:id/import` — body
`{ rowVersion: number, containerNumber?: string, draftContainerId?: number, replacesId?: number, force?: boolean, note?: string }`
- `rowVersion` is required: the one on the screen (409 `STALE` otherwise).
- One of `containerNumber` / `draftContainerId`, resolved as for the preview.
- `replacesId`: the supplier's packing list in that container this one corrects (the same versioning
  as `POST /packing-lists` with `replacesId`): the old one gets `supersededById` and leaves the
  check; its submission becomes `replaced` on the supplier's page at once. Unlike the staff upload,
  a refused replace **refuses the import** (404 `REPLACES_NOT_FOUND`, 409 `ALREADY_REPLACED`) rather
  than filing the document as an extra.
- `force`: file it although the group has no live line in the container (ask the user to confirm).
  The supplier's page then shows "Filed by our team; your orders aren't allocated to this shipment
  yet" (`container: null`) until lines are allocated — the next refresh shows the full result.
- `note` (≤ 1000) is shown to the supplier.
- What it does: copies the file into `PO_DOCS_BUCKET` (`packing-lists/<uuid>/<name>`), inserts the
  `packing_lists` row (`source: 'portal'`, `portalSubmissionId`; its `supplierKey` is
  `s:<supplierId>` and `supplierName` the row's `supplierName` — see "The supplier group"), **reuses
  the stored extraction** (`storeAnalysis`: the row is `succeeded` in the response), supersedes
  `replacesId`, marks the submission imported (`confirmed*`, `packingListId`). With no usable
  extraction (a `NO_ROWS` or failed check) the row is returned `processing` and the file is read
  again in the background, as a staff upload is; poll `GET /packing-lists/:id`.
- A previous filing (after a return to Pending, `packingListId` still set) that is still live is
  retired first (audit `refiled` on the packing list).

`POST /api/v1/pl-submissions/:id/reject` — body `{ rowVersion: number, note: string }` (note required,
≤ 1000, shown to the supplier). Nothing is filed; the supplier uploads a corrected version, which the
matcher then suggests the original container for (`matchReason: 'replaces'`).

## Refusals

| Status | Code | When |
| --- | --- | --- |
| 400 | `BAD_FIELD` | bad id / filter / body; no container; a missing reject note; a `supplierKey` that is not `s:<id>` |
| 403 | `CANNOT_DECIDE` | read-only (or accountant) user on import / reject |
| 404 | `NOT_FOUND` | no such submission (drafts and discarded rows count as not found) |
| 404 | `CONTAINER_NOT_FOUND` | no booked container or open draft matches (preview, import) |
| 404 | `REPLACES_NOT_FOUND` | `replacesId` is not one of this supplier's packing lists in that container |
| 409 | `NOT_SUBMITTED` | already imported / rejected / withdrawn — payload `status`, `packingListId` |
| 409 | `STALE` | `rowVersion` no longer matches — reload (the supplier withdrew, a refresh changed the row, …) |
| 409 | `CONFLICT` | someone else was deciding at the same moment, or the same submission was imported twice at once — reload |
| 409 | `AMBIGUOUS_CONTAINER` | several open drafts reserve that number — payload `candidates: [{ draftContainerId, draftName, lineCount, units, … }]`; send one's `draftContainerId` |
| 409 | `ALREADY_REPLACED` | `replacesId` was already replaced — payload `supersededById` |
| 422 | `NOT_IMPORTABLE` | payload `blockers: string[]` (below) |
| 422 | `SUPPLIER_NOT_FOUND` | none of the row's group ids has a live `suppliers` row (payload `blockers: ['supplier_not_found']`) |
| 422 | `SUPPLIER_NOT_ON_CONTAINER` | the group has no live line in the container (no line under one of its names) — payload `suppliers` (who is on the container, by supplier name, in the picker's shape); retry with `force: true` after confirmation |
| 422 | `CONTAINER_EMPTY` | the draft has no lines (deleted, or emptied) |
| 502 | `FILE_UNAVAILABLE` | the portal's bucket could not be read, or the copy could not be written; try again |

Blockers (`blockers[]`, codes): `not_submitted`, `supplier_not_found`, `bad_key`, `bad_type`,
`too_large`, `no_etag` (the file was never validated by a check), `file_gone` (no longer in the bucket:
the portal keeps `pl/*` objects 180 days; a test database copied from prod), `file_changed` (the bytes
differ from the checked ones). `file_gone` / `file_changed` come from the detail's look at the bucket
and from the import itself.

## Types

```ts
type PlStatus = 'submitted'|'imported'|'rejected'|'withdrawn';
type PlMatchConfidence = 'confident'|'unsure'|'none';
type PlMatchReason = 'replaces'|'box_number'|'po_numbers'|'lines'|'single'|'conflict';
type PlResultState = 'provisional'|'confirmed'|'unmatched'|'in_review'|'replaced';   // 'restricted' exists only on the portal's side
type PlContainer = { kind: 'booked'|'draft'; containerNumber: string|null; draftContainerId: number|null; label: string|null };

type PlSubmission = {              // list item; the `submission` of the detail and of the actions
  id: number; supplierId: number; supplierName: string; supplierKey: string; supplierExists: boolean;   // supplierKey = 's:<supplierId>'
  submittedByEmail: string; filename: string; contentType: string; fileSize: number|null;
  replacesSubmissionId: number|null;
  status: PlStatus; checkStatus: 'awaiting_upload'|'pending'|'succeeded'|'failed'; errorCode: string|null;
  rowCount: number|null; invoiceNumber: string|null;
  matchConfidence: PlMatchConfidence|null; matchReason: PlMatchReason|null;
  suggested: PlContainer|null; confirmed: PlContainer|null;                 // as objects…
  suggestedKind: string|null; suggestedContainerNumber: string|null; suggestedDraftId: number|null; suggestedLabel: string|null;
  confirmedKind: string|null; confirmedContainerNumber: string|null; confirmedDraftId: number|null; confirmedLabel: string|null;   // …and flat
  verdict: 'match'|'differences'|null; discrepancyCount: number|null; resultState: PlResultState|null; resultAt: string|null;
  supplierNote: string|null; warningsAcknowledged: boolean; submittedAt: string|null;
  decidedBy: string|null; decidedAt: string|null; decidedNote: string|null;
  packingListId: number|null;      // the filing; after a return to Pending, the PREVIOUS filing
  rowVersion: number; createdAt: string|null; blockers: string[];
};
type PlSupplierSummary = { supplierKey: string; supplierId: number; supplierName: string; supplierIds: number[]; pending: number; total: number; oldestSubmittedAt: string|null };
                                   // supplierKey = 's:<supplierId>'; supplierIds = the union of the rows' group_supplier_ids

type PlCandidate = {               // one scored container (pl-match.js scoreCandidates' entry)
  key: string;                     // 'booked:324' | 'draft:129'
  container: PlContainer;
  score: number; coverage: number; exact: number; recall: number; lineHit: number; poShare: number;   // 0..1 (score may exceed 1)
  packedUnits: number; expectedUnits: number; matched: number; unexpected: number; missing: number;
  boxHit: boolean; boxNumbers: string[];           // a valid printed ISO 6346 number is this container's box
  nearBox: string|null; nearBoxes: string[];       // printed numbers one edit away from its box (display only, never scores)
  refHit: boolean;                                 // the booked reference / the number a draft's name reserves is printed
  replacesHit: boolean;                            // the submission this one replaces was filed here
  conflicts: string[];                             // the conflict codes this candidate is part of
};
type PlConflict = { code: string; message: string; candidates: string[]; boxNumbers?: string[] };   // the matcher's top-level list
type PlAcknowledged = { sawNoResult: true } | { sawNoResult: false; suggestedLabel: string|null; confidence: PlMatchConfidence|null; reason: PlMatchReason|null };

type PlSubmissionDetail = {
  submission: PlSubmission;
  check: { status: string; modelUsed: string|null; inputTokens: number|null; outputTokens: number|null; totalTokens: number|null;
           errorCode: string|null; errorMessage: string|null; checkedAt: string|null; attempts: number; rowCount: number|null; invoiceNumber: string|null };
  extraction: any|null;            // readPackingList output as stored: { invoiceNumber, documentDate, supplierName, supplierPrinted, containerNumbers[], totalQuantity, totalCartons, …, lines[] }
  acknowledged: PlAcknowledged|null;   // null when the check never succeeded (the supplier saw the failure and wrote a note)
  candidates: { stored: PlCandidate[]; live: PlCandidate[]|null; liveError: string|null;
                conflicts: PlConflict[];          // live when the live scoring ran, else the stored ones
                storedConflicts: PlConflict[]; foreignBox: string[]|null };
  fileUrl: string|null; fileExpiresAt: string|null;   // imported: the PO-bucket copy; else the portal object (null when gone)
  packingList: (PackingList & { deleted: boolean })|null;           // imported rows
  previousPackingList: (PackingList & { deleted: boolean })|null;   // after a return to Pending
  defaultReplacesId: number|null; defaultContainer: PlContainer|null;   // the imported version it corrects, still live: pre-select both
  result: any|null;                // result_json as the supplier sees it (v:1)
  canImport: boolean;
  audit: { action: string; userEmail: string|null; createdAt: string; after: any }[];
};
type PlSubmissionPreview = {
  supplierKey: string; container: PackingListContainer;   // describeContainer: { kind, containerNumber, draftContainerId, draftName, label, followedFrom? }
  comparison: PackingListComparison|null;                 // the staff shape of docs/packing-lists-frontend.md §3, pooled, sign-offs applied; null when nothing can be compared
  packingLists: PackingList[];                            // the supplier's active documents there (replace candidates)
  duplicateInvoices: { invoiceNumber: string; packingListIds: (number|null)[] }[];   // null = this submission
  onBoard: boolean; extractionUsable: boolean;
};
```

`acknowledged` is inferred: the portal asks for an acknowledgement only for a confident suggestion
with differences (or printed totals that do not add up), and the one way such a row arrives with
`warningsAcknowledged: false` is the portal's group gate, which showed the supplier no result (the
gate's answer is in jportal's own audit, not on the row). `audit` actions: `check_succeeded`,
`check_failed` (plCheck), `result_refreshed`, `returned_to_pending` (pl-results), `import`, `reject`.

**Conflict codes** (`PlConflict.code`, and `PlCandidate.conflicts[]` for the candidates involved;
`src/lib/pl-match.js CONFLICT_CODES`): `box_hit_not_winner`, `multiple_box_hits`, `foreign_box`
(top-level only — it names no candidate; `boxNumbers` lists the printed numbers), `po_share_not_winner`,
`replaces_not_winner`. Any of them caps the suggestion at `unsure` with `matchReason: 'conflict'`.

**Match reasons** (the rule that decided; `docs`: the plan's table): `replaces` (the corrected
version's original container), `box_number` (the printed ISO 6346 number, check digit verified),
`po_numbers` (only this candidate holds the printed POs), `lines` (the lines, with a clear margin),
`single` (the only candidate, nearly exact), `conflict`. `unsure` with reason `lines` is the fallback
(coverage ≥ 0.30); `none` has no reason.

**Fields added to existing JSON** — packing list JSON (`GET /packing-lists`, `/:id`, `/check`,
`/status`, the preview): `source: 'staff'|'portal'` and `portalSubmissionId: number|null`. Show a
"From supplier portal" pill where `source === 'portal'`.

## The check (plCheck), the refresh (plRefresh) and the supplier's result

The portal starts the check (the bucket's EventBridge event for `pl/*`, or a direct `{ submissionId }`
invoke); shipping's claim is authoritative (the same `planEvent` / `claimWhere` as PIs). Then:
GetObject, size and magic bytes, pin `s3_etag` + `file_size`; the supplier group (`SUPPLIER_NOT_FOUND`
before any model call); `readPackingList` with the Lambda's deadline; the candidates loaded and scored;
the provisional pooled result for a confident match; `duplicateOf` (another submitted or imported row
of the supplier's with the same ETag, in `result_json`); one guarded UPDATE of the check, match and
result column groups.

| `errorCode` | Retried by the supplier? | Portal rule |
| --- | --- | --- |
| `FILE_INVALID` (over 10 MiB, or not the declared type) | no | refused (`CHECK_FAILED`) |
| `SUPPLIER_NOT_FOUND` | no | refused |
| `NO_ROWS` (the file was read, no packing row came out) / `DOCUMENT_UNPROCESSABLE` (400 / 413 / invalid argument / page or payload limit) | no | sendable with a note and an acknowledgement; staff read the file, Import reads it again |
| `MODEL_UNAVAILABLE` (429 / 5xx / timeout; also an unexpected internal error), `NOT_CONFIGURED`, `MODEL_NO_JSON`, `MODEL_BAD_OUTPUT` | yes (`RETRYABLE_CODES`) | "checker unavailable" |

## The supplier group

`suppliers.portal_code` plays no part in this flow. The "company" behind a submission is **the set
of supplier ids the portal user has access to**, which the portal writes into
`pl_submissions.group_supplier_ids` (a JSON array of ints, sorted) **at create**, together with
`supplier_id` = the lowest of those ids and `supplier_name` = that row's live name. Shipping treats
`group_supplier_ids` as the authority and **never writes it**; a legacy row with `NULL` is read as
the group of its own `supplier_id`.

- **Resolution** (`pl-results.js portalGroup`): the ids → the live `suppliers.name` of each
  (`WHERE id IN (?) AND deleted_at IS NULL`). No live name at all is `SUPPLIER_NOT_FOUND` (the check
  fails before any model call; preview and import refuse 422).
- **Line membership** (`packing-list-check.js lineInGroup`): a container line is the group's when
  the order's supplier name **or** the PO's supplier name is one of the group's names, compared
  with the existing `nameKey` normaliser (trimmed, single-spaced, lower-cased). This rule drives the
  candidate loader (containers holding LIVE-status lines of any group name; the lines kept per
  candidate are the group's), the expected lines of every supplier-facing result, the import gate
  (`SUPPLIER_NOT_ON_CONTAINER`), the forced-import detection on refresh and the preview's `onBoard`.
- **The filing.** On import the packing list gets `supplier_key = 's:' + supplier_id` (the lowest
  granted id the portal stored) and `supplier_name` = the row's `supplier_name`. No lookup, no
  staff grouping (the import audit records `supplierKey` and `groupSupplierIds`).
- **Pooling** (`pooledComparison`): the active `packing_lists` rows in the container whose
  `supplier_key` is `'s:' + <any id in group_supplier_ids>` (plus the row's own filing for an
  imported row, and the document being checked for a provisional result); sign-offs are loaded for
  those keys.
- **Nothing in this flow calls the staff grouping** (`groupFor` / `resolveSupplierNames` /
  `resolveSupplierChoice`, which share a `portal_code`), not even indirectly: the group's lines are
  read untagged (`queryContainerLines`), and the `SUPPLIER_NOT_ON_CONTAINER` payload's `suppliers`
  is built by supplier name (`s:<id>` of the live row carrying that exact name, else `n:<name>`),
  not by the staff picker.
- **Accepted limit, for now.** The staff container check (`packing-review.js` /
  `packing-list-check.js`: the picker, `loadExpectedLines`, sign-offs, approvals) still groups the
  container's lines by `portal_code` (`s:<lowest id sharing the code>`). Where that key differs
  from `s:<lowest granted id>` — a company whose lowest-id row is not the one the portal user was
  granted first, or whose rows do not share a code — a portal-imported list appears in the staff
  check under a **different supplier entry** than the container's lines (its expected lines there
  are empty, every row "not expected"), while the supplier's own page, which uses the group's
  names, is right. This will be fixed when the staff grouping moves off `portal_code`; nothing in
  the portal flow needs to change then.
- **`/suppliers` and the list filter** key by `supplier_id` (the lowest granted id), so
  `supplierKey` is `s:<supplier_id>` with no lookup.

What the portal must write at create, for this to hold: `group_supplier_ids` = every supplier id
the user is granted (sorted ints), `supplier_id` = `MIN(group_supplier_ids)`, `supplier_name` = that
supplier's live name. If the portal's own group gate reads `group_supplier_ids`, it should treat
`NULL` (a row created before this rule) as `[supplier_id]`, as shipping does.

**plRefresh** (`{ submissionId }`, or `{ submissionId, action: 'refresh' }` to plCheck) recomputes
`result_json` (`pl-results.js computeSupplierResult`): a checked draft is re-scored (`provisional` /
`unmatched`); an imported row follows its packing list (`confirmed` with the pooled check and the
sign-offs as `accepted`; `confirmed` with `container: null` for a forced import; `replaced` when
superseded; **back to Pending** — `status: 'submitted'`, `result_state: 'in_review'`, the decision
cleared, `packingListId` kept — when the packing list or its container is gone). Every other status is
frozen. The write is guarded on the row as read and bumps `rowVersion` only when the result changed,
so a submit that lands mid-refresh makes the refresh write 0 rows, and an open staff Import never goes
`STALE` under a refresh.

**Hooks** that call the same sync after their commit: `DELETE /packing-lists/:id` (the deleted PL and
the version it restores), `POST /draft-containers/close` with reason `deleted`, `DELETE /shipments/:id`
and a cancelling transition (the draft's portal PLs), and the import itself (the new PL and the one it
superseded). A missed hook is caught by the next refresh.

**Supplier isolation**: every supplier-facing result is built only from the uploader's supplier group
— expected lines from `loadGroupLines` (the container's lines under the group's names), pooled
documents only the `packing_lists` rows under the group's keys (`s:<id>` per granted id, plus the
row's own), `elsewhere` only placements whose order is the group's by name — and `toSupplierComparison`
strips ids, fingerprints, match methods, sign-off emails and the container's staff-only details.
`tools/test-pl-submissions-lib.js` and the TEST suites assert it (the isolation cases use a group
that excludes the other supplier on the container).

**Shared constants** (copied in the portal's `src/lib/plSubmissions.js` and README §12; keep in step):
`STALE_CHECK_SECONDS = 600`, `MAX_CHECK_ATTEMPTS = 3`, `RETRYABLE_CODES`, key regex
`^pl/[0-9a-f-]{36}\.(pdf|png|jpg)$`, the allowed types and the 10 MiB limit (all PI's,
`src/lib/pi-submissions.js`), the result states, `RESULT_VERSION = 1`, 300 result lines.
`s3_etag` is stored exactly as S3 returns `ETag`, quotes included.

## Configuration

`serverless.yml`:
- `PI_UPLOADS_BUCKET: jportal-pi-uploads-<env>` — the portal's bucket, now read under `pl/*` too.
- IAM: `s3:GetObject` on `arn:aws:s3:::jportal-pi-uploads-<env>/pl/*` (next to `pi/*`), `s3:ListBucket`
  on the bucket (so a missing object answers 404). No tagging, no delete.
- Function `plCheck`, named **`jportal-pl-check-<env>`**, 300 s, 1024 MB, reserved concurrency 5, one
  async retry, its EventBridge rule on the portal bucket with prefix `pl/`, the provider VPC and env
  (`GEMINI_API_KEY`, `PACKING_LIST_GEMINI_MODEL`).
- Function `plRefresh`, named **`jportal-pl-refresh-<env>`**, 60 s, 512 MB, reserved concurrency 5,
  database only (no S3, no Gemini), so a batch of checks never holds up page opens.
- Alarms (`resources`): `jportal-pl-check-<env>-errors`, `-throttles`, `-duration-p99` (> 270 s) and
  `jportal-pl-refresh-<env>-errors`, `-throttles`; 5-minute periods, missing data not breaching.
  `AlarmActions` is the SNS topic in the optional secret key **`ALERTS_TOPIC_ARN`** (`shipping/<env>`,
  threaded as `custom.alertsTopicArn` through a `HasAlertsTopic` condition); absent, the alarms exist
  with no action.
- The portal's `deploy.sh` checks `jportal-pl-check-<stage>` and `jportal-pl-refresh-<stage>` exist and
  that it may invoke them, like `jportal-pi-check-<stage>`.

## Database grants (run by hand, as admin)

Shipping's login (`DB_USER` of `shipping/<env>`) reads the whole table and writes its four column
groups; it never creates or deletes rows. `group_supplier_ids` is **not** among the columns it
writes: the portal INSERTs it at create (its login needs INSERT on it, as on `supplier_id`), and
shipping only reads it. (A grant given earlier on that column is harmless but no longer needed.)

```sql
GRANT SELECT ON jportal.pl_submissions TO '<shipping login>'@'%';
-- check
GRANT UPDATE (check_status, check_attempt, check_started_at, error_code, error_message, checked_at,
              model_used, input_tokens, output_tokens, total_tokens, s3_etag, file_size,
              extract_json, row_count, invoice_number)
      ON jportal.pl_submissions TO '<shipping login>'@'%';
-- match
GRANT UPDATE (candidates_json, match_confidence, match_reason, suggested_kind, suggested_container_number,
              suggested_draft_id, suggested_label)
      ON jportal.pl_submissions TO '<shipping login>'@'%';
-- result
GRANT UPDATE (verdict, discrepancy_count, result_json, result_state, result_at)
      ON jportal.pl_submissions TO '<shipping login>'@'%';
-- decision
GRANT UPDATE (status, decided_by, decided_at, decided_note, packing_list_id, confirmed_kind,
              confirmed_container_number, confirmed_draft_id, confirmed_label, row_version)
      ON jportal.pl_submissions TO '<shipping login>'@'%';
```

(Use the test schema name on test if `JPORTAL_DB_SCHEMA` differs.) `SELECT … FOR UPDATE` needs UPDATE
on at least one column, which the groups give. No grant on `jportal.audit_logs`: shipping's own trail
is its `audit_log` (`entity_type 'pl_submission'`, and `'packing_list'` for the filing).

The TEST suites write fixtures through `FIXTURE_DB_USER` / `FIXTURE_DB_PASSWORD` (falling back to
`DB_USER`), a login that additionally needs:

```sql
GRANT INSERT, DELETE ON jportal.pl_submissions TO '<fixture login>'@'%';
GRANT UPDATE ON jportal.pl_submissions TO '<fixture login>'@'%';   -- the suites simulate the portal's submit
```

`tools/test-pl-submissions.js` starts with one UPDATE per column group under the shipping login
(`pl-test-helpers.js grantsProbe`), so a missing grant fails on test, not in production.

## Deploy order

Migration (deploy.sh applies it before the code): `2026-10-21_10_packing_list_portal_source.sql`
(`packing_lists.source`, `portal_submission_id`, the unique key on the live filing).

1. Portal on test (flag off): its migration creates `jportal.pl_submissions`; the bucket already
   exists (the PI one), its EventBridge notifications on.
2. Shipping on test: the grants above (shipping and fixture logins); optionally `ALERTS_TOPIC_ARN` in
   `shipping/test`; `bash deploy.sh test`; then `npm run test:unit`, `node tools/test-pl-check-flow.js`,
   `node tools/test-pl-submissions.js`.
3. Portal flag on for test (`PL_UPLOAD_ENABLED=true`, `put-secret`, redeploy — its wiring check covers
   all three functions); the portal's smoke test.
4. ShipLine on test.
5. Production: portal (flag off) → grants for both logins → shipping (`bash deploy.sh`, stage `dev` =
   envName `prod`) → ShipLine → flip the portal flag. Shipping must be live before the flag flips.

`plCheck`'s EventBridge rule can be deployed before the bucket has `pl/` objects (it matches by name).

## Tuning the matcher: the precision query

Run this before changing any threshold in `src/lib/pl-match.js`. A confident suggestion counts as
right when staff imported the submission into the suggested container (`confirmed_*` equals
`suggested_*`; a draft followed into its booking is matched on `confirmed_draft_id`). Rows imported
with `force` or into a typed-in container count as wrong, which is the point.

```sql
SELECT YEARWEEK(decided_at, 3)                                   AS week,
       match_reason,
       COUNT(*)                                                   AS imported,
       SUM(suggested_kind = confirmed_kind
           AND ((suggested_kind = 'booked' AND suggested_container_number = confirmed_container_number)
             OR (suggested_kind = 'draft'  AND suggested_draft_id <=> confirmed_draft_id)))
                                                                 AS as_suggested,
       ROUND(100 * SUM(suggested_kind = confirmed_kind
           AND ((suggested_kind = 'booked' AND suggested_container_number = confirmed_container_number)
             OR (suggested_kind = 'draft'  AND suggested_draft_id <=> confirmed_draft_id))) / COUNT(*), 1)
                                                                 AS precision_pct
  FROM jportal.pl_submissions
 WHERE status = 'imported' AND match_confidence = 'confident'
 GROUP BY week, match_reason
 ORDER BY week DESC, match_reason;
```

Add `AND decided_at >= UTC_TIMESTAMP() - INTERVAL 12 WEEK` for a window. The per-candidate ratios in
`candidates_json.top[]` and the structured log line (`[pl-match] scored` in `plCheck`'s log group:
`submissionId, rule, confidence, reason, candidateCount, capCut, codeFilterFallback, conflicts,
winnerCoverage, winnerPoShare, winnerScore`; CloudWatch Insights `filter @message like "[pl-match]
scored" | stats count() by rule, confidence`) say *why* a wrong suggestion scored as it did.
