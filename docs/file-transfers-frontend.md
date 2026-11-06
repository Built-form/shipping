# Supplier portal file transfers — frontend guide

Routes: `src/services/file-transfer-routes.js` · rules: `src/lib/file-transfers.js` · tests:
`tools/test-file-transfers-lib.js` (unit, in `npm run test:unit`; the routes run against a fake
connection, a fake S3 and a stubbed Front) · data: `jportal.file_transfers` and
`jportal.file_transfer_views`, owned by the supplier portal (the `portal` repo; its README §18
"File transfers" is the other half of this contract) · email: template `file_transfer_portal`
(seeded by `2026-11-04_20_seed_file_transfer_template.js`, editable in ShipLine's templates) ·
ShipLine: `src/components/FileTransfersView.tsx` (section `files` of the SUPPLIER PORTAL tab).

A **file transfer** is one file sent one way between a supplier and us: a shared folder with
a record. Any kind of file but a program (by its extension, and by its first bytes), up to
**250 MiB**. Nothing reads a transferred file: no check, no import, no Gemini.

```
from_supplier   portal: draft ──uploaded (checked, copied)──▶ sent ──withdraw (supplier)──▶ withdrawn
                shipping: POST …/open records a staff view ("opened by our team" on the portal)

to_supplier     shipping: POST (draft + presigned PUT) ──PUT (browser → S3)──▶ …/uploaded (checked, pinned, emailed) ──▶ sent
                          sent ──withdraw──▶ withdrawn · draft ──discard──▶ discarded
                portal: the supplier downloads it (a supplier view comes back here as `seen`)
```

Drafts and discarded rows are never listed to staff. A withdrawn supplier file stays listed
(History): the supplier took it back, the record remains.

## The page

ShipLine's **SUPPLIER PORTAL** tab, sub-tab **Files** (beside Changes, Proforma invoices,
Packing lists and Invoices), with Inbox / Sent / History:

- **Badge / left column** — `GET /api/v1/file-transfers/suppliers`: per supplier, how many of
  their files nobody here has opened (`pending`), the totals each way, the oldest unopened.
  Suppliers with an unopened file first, oldest first. The tab strip's count and the nav
  badge's fifth part are the sum of `pending`.
- **Inbox** — `GET /api/v1/file-transfers?direction=from_supplier` (status `open` = sent),
  unopened first. **Open** on a row is `POST …/:id/open`: it records this user's view and
  answers the download link; open the link in a new tab (it is an attachment under the file's
  own name). The row then shows OPENED, and the supplier's portal shows "opened by our team".
- **Sent** — `?direction=to_supplier`: each row says whether anyone at the supplier opened it
  (`seen` / `viewers` / `seenAt`: SEEN or NOT OPENED) and how the email went (`notifiedAt` /
  `notifyError`: EMAILED, NOT EMAILED, EMAIL FAILED, NO PORTAL USER). **Download** is
  `GET …/:id` (records nothing). **Email** / **Re-email** is `POST …/:id/notify`;
  **Withdraw** `POST …/:id/withdraw`.
- **History** — `?status=history` (withdrawn, either way).
- **Send a file** (the button in the header band) — the three-step send below, for one
  supplier (JFPRO's supplier list, `api.getSuppliers`), with an optional message.

Who: admin and standard users open, send, email and withdraw (`X-User-Type`); read-only users
only look (403 `CANNOT_DECIDE`), and do not get the SUPPLIER PORTAL tab anyway.

## Sending a file (three steps)

Two AWS limits shape it, as for QC reports: the HTTP API caps a body at 6 MB, so the bytes
never pass through the API; the browser PUTs them straight to the **portal's** bucket
(`jportal-pi-uploads-<env>`, key `files/staff/<uuid>.<ext>`), to a URL this API signs. That
bucket's CORS rule names ShipLine's hosts (`shipline.built-form.co.uk`, `*.vercel.app`,
`localhost:3000/3030`) for PUT only; a signed URL is still needed to write anything.

```
1. POST /api/v1/file-transfers                      -> 201 { transfer (draft), upload: { url, method, headers, expiresAt } }
2. PUT  upload.url  (browser -> S3)                 -> 200   (send the bytes with upload.headers verbatim: Content-Type)
3. POST /api/v1/file-transfers/:id/uploaded         -> 200 { transfer (sent), notification }
```

- Step 1 body: `{ supplierId, filename, contentType, fileSize, note? }`. `contentType` is the
  browser's when it is one (else `application/octet-stream`; the API does the same), and it is
  signed into the PUT with the length, so the browser must send exactly what it declared.
  `note` ≤ 2000 characters, shown beside the file on the portal and in the email.
- The signed PUT lasts 15 minutes. If S3 answers `403` with "Request has expired" (a big file
  on a slow line), `POST …/:id/upload-url` gives a fresh one for the same draft, within an hour
  of step 1; send the same bytes again.
- Step 3 checks the object (there, not empty, ≤ 250 MiB, and its first bytes are not a
  program), pins its ETag, publishes the row and **emails the supplier's active portal users**
  through Front (template `file_transfer_portal`, a link to the portal's `/files` page;
  `PI_NOTIFY_REDIRECT_TO` redirects it on test). `notification` is `{ sent, to, reason? }`;
  `reason: 'no_recipients'` means the supplier has no active portal user — the file is on the
  portal, nobody was told (tell them another way, or set a user up: a PO send pre-approves its
  recipients). A refused step 3 (`FILE_BLOCKED`, `FILE_TOO_LARGE`) leaves the draft: discard it
  (`POST …/:id/discard`) or it stays nobody's.
- Step 3 is idempotent: a row already sent answers as it is, with `reason: 'already_sent'`
  and no second email.

## Response envelope

Shipping's convention: **lists are wrapped in `{ data }`; a single object and the answer of an
action are NOT wrapped.** ShipLine reads `json.data ?? json` either way.

| Route | Answers with |
| --- | --- |
| `GET /api/v1/file-transfers` | `200 { data: FileTransfer[], hasMore: boolean }` |
| `GET /api/v1/file-transfers/suppliers` | `200 { data: FileTransferSupplierSummary[] }` |
| `GET /api/v1/file-transfers/:id` | `200 { transfer (with views), file: { url, expiresAt } \| null, fileState: { gone, changed } }` |
| `POST /api/v1/file-transfers/:id/open` | the same as `GET …/:id`, after recording the view |
| `POST /api/v1/file-transfers` | `201 { transfer, upload }` |
| `POST /api/v1/file-transfers/:id/upload-url` | `200 { upload }` |
| `POST /api/v1/file-transfers/:id/uploaded` | `200 { transfer, notification }` |
| `POST /api/v1/file-transfers/:id/notify` | `200 { transfer, notification }` |
| `POST /api/v1/file-transfers/:id/withdraw` | `200 { transfer }` |
| `POST /api/v1/file-transfers/:id/discard` | `200 { transfer }` |

Errors are shipping's usual `{ error, code, ...payload }`.

## Routes

`GET /api/v1/file-transfers?status=&direction=&supplierId=&unopened=&limit=&offset=`
- `status`: `open` (= `sent`, the default), `history` (= `withdrawn`), `all`, or `sent` /
  `withdrawn` comma-separated. Drafts and discarded rows are never listed.
- `direction`: `from_supplier` | `to_supplier`; absent: both.
- `unopened=1`: only supplier files no staff member has opened.
- `limit` defaults to 100, at most 500; `hasMore` says whether there is another page. Newest
  sent first.

`FileTransfer` (`lib/file-transfers.js transferToJson`): `id, direction, status, supplierId,
supplierKey ('s:<id>'), supplierName (live, else the snapshot), supplierExists, filename,
contentType, fileSize, note, sentBy, sentAt, withdrawnAt, withdrawnBy, seen, seenAt, viewers,
notifiedAt, notifyError (ours only), rowVersion, createdAt`. `seen` is about the OTHER side:
a staff view for a supplier's file, a supplier view for ours. Never a storage key or an ETag.
The detail adds `views: [{ kind: 'supplier' | 'staff', email, firstViewedAt, lastViewedAt,
count }]`.

`GET /api/v1/file-transfers/:id` — the row, its views, and a 15-minute download link for the
readable key: a supplier's file only once the portal published it (`final_s3_key`), ours once
pinned. `file` is null and `fileState` says why when there is nothing to read, the object is
gone, or its bytes changed since they were checked (then it is not served).

`POST /api/v1/file-transfers/:id/open` — records this user's view of a supplier's file (once
per person: a second open counts up `count`), then answers as the GET does. For our own file
it records nothing.

`POST /api/v1/file-transfers` — body `{ supplierId, filename, contentType?, fileSize, note? }`
(step 1 above). `404 SUPPLIER_NOT_FOUND` for a deleted or unknown supplier.

`POST /api/v1/file-transfers/:id/upload-url` — a fresh PUT for a draft still waiting for its
file, within an hour of its creation.

`POST /api/v1/file-transfers/:id/uploaded` — step 3 above.

`POST /api/v1/file-transfers/:id/notify` — body `{ force?: boolean }`. Allowed whenever
`notifiedAt` is null, whatever `notifyError` says; `force: true` only when it was already sent.

`POST /api/v1/file-transfers/:id/withdraw` — body `{ rowVersion }`. Our sent file disappears
from the supplier's Files page; the object stays (nothing deletes under `files/`).

`POST /api/v1/file-transfers/:id/discard` — body `{ rowVersion }`. One of our drafts.

## Refusals

| Status | Code | When |
| --- | --- | --- |
| 400 | `BAD_FIELD` | bad id / filter / body; an empty file; a note over 2000 characters |
| 403 | `CANNOT_DECIDE` | read-only user on open / send / notify / withdraw / discard |
| 404 | `NOT_FOUND` | no such transfer, or a draft / discarded row (nobody's) |
| 404 | `SUPPLIER_NOT_FOUND` | step 1 for a supplier that is not live |
| 409 | `NOT_DRAFT` | upload-url / discard on a row already sent (payload `status`) |
| 409 | `DRAFT_EXPIRED` | upload-url over an hour after step 1 |
| 409 | `FILE_MISSING` | step 3 before the PUT landed |
| 409 | `FILE_CHANGED` | the object changed while it was being checked |
| 409 | `NOT_SENT` | notify / withdraw on a row that is not `sent` (payload `status`) |
| 409 | `ALREADY_NOTIFIED` | notify without `force` when already sent (payload `notifiedAt`) |
| 409 | `STALE` | `rowVersion` no longer matches — reload |
| 413 | `FILE_TOO_LARGE` | over 250 MiB, at step 1 or step 3 |
| 422 | `FILE_BLOCKED` | a program: by extension at step 1, by its first bytes at step 3 |
| 502 | `FILE_UNAVAILABLE` | the portal's bucket could not be read; try again |
| 503 | `PORTAL_UNAVAILABLE` | no `jportal.file_transfers` on this server yet (deploy the portal first) |

## Rollout (test, then prod)

1. Portal: `bash deploy.sh <stage>` in `portal` (the migration creates `file_transfers` and
   `file_transfer_views`, re-applies `db/grants/jportal_app.sql`, adds the `/files/*`
   CloudFront behaviour, the bucket's CORS rule and lifecycle rules, and deploys the routes
   and the web build).
2. Shipping: `bash deploy.sh <stage>` (seeds `file_transfer_portal`, deploys the routes with
   the new bucket rights).
3. ShipLine: deploy as usual (4.30).

Shipping before the portal answers `503 PORTAL_UNAVAILABLE` on these routes until the
portal's migration has run; ShipLine shows the section as unavailable. On test, remember
the DB is rebuilt from prod every 3 days.

## Known limits

- A step-3 refusal, or a browser closed after step 2, leaves an object under `files/staff/`
  that nothing points at; the portal's own orphans (a publish that failed after its copy)
  sit under `files/sup/`. Nothing deletes under `files/`; both are rare and small.
- The email goes to the supplier's **active portal users** (`jportal.supplier_users` through
  `user_supplier_access`), not to `supplier_emails`: a supplier with no portal user is told
  nothing (`no_recipients`), on purpose — the file can only be fetched on the portal.
