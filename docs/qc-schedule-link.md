# QC visit schedule link

The inspector's QC visit email no longer needs the products' QC documents attached. It carries one **no-login link** to a page listing every product to inspect, with its files, always as the visit is now. When the products change after an email has gone out, ShipLine says so and offers a one-click **update email**, and the page marks what changed.

Code: `src/lib/qc-schedule.js` (pure: token, snapshot + diff, email paragraph, page HTML), `src/services/qc-schedule-routes.js` (SQL, `/schedule-link` routes), `src/handlers/qc-schedule.js` (the public page Lambda `qcScheduleApi`), plus changes in `qc-visit-comms-routes.js` and `qc-visit-routes.js`. Migrations `2026-11-03_10_qc_visit_schedule_links.sql`, `2026-11-03_11_qc_visit_update_template.js`.

## The link

- One live link per visit for the inspector (`qc_visit_links`, `audience = 'inspector'`; `factory` is reserved, not issued). Created by the first inspector email sent with **Include schedule link**, or by `POST …/schedule-link`, then reused by every later email: the inspector keeps one URL for the life of the visit.
- Token: 43 base64url characters (32 random bytes). It opens files, so it is long. Never written to the audit log.
- URL: `${PUBLIC_API_BASE_URL or the request host}/api/v1/qc-schedule/<token>`, the same host as the confirm-receipt links.
- **Rotate** (`POST …/schedule-link { rotate: true }`) revokes it (`revoke_reason 'rotated'`) and issues a new one; the old URL shows "Link no longer active". Email the inspector again afterwards.
- Opens are counted (`first_opened_at`, `last_opened_at`, `open_count`) so staff can see the inspector looked.

## The page (`GET /api/v1/qc-schedule/{token}`)

Public, HTML, `Cache-Control: no-store`, `noindex`, `Referrer-Policy: no-referrer`, a CSP with no scripts. Shows:

- the visit header as the inspector email prints it (date and window in China and UK time, supplier, factory, address, contact, production state, report deadline, escalation);
- **products**, one card per JF code (`_FQC` lines join their parent), each with its PO lines (units, sampling / AQL, note) and its files: the JFPRO kit documents of the code (view `qc_kit_documents`, current versions) and the visit uploads tagged with that code;
- **files for the whole visit** (uploads with no JF code);
- a products CSV download (the same file as `GET /qc-visits/:id/products.csv?audience=inspector`);
- **changes since the last email to the inspector** (the newest proposal / update send with a snapshot): a banner, NEW / CHANGED tags with old values struck through, a "Removed — do not inspect" list, and NEW on files added after that email.

A cancelled visit shows only "cancelled"; completed and draft visits show a notice above the page. An upload whose product has left the visit is no longer shown or served.

`GET /api/v1/qc-schedule/{token}/files/{file}` — `k<versionId>` (a current kit document version of one of the visit's codes), `a<attachmentId>` (an upload of this visit), `products.csv`. Re-checks the link and the file on every click and answers 302 to a 5-minute presigned URL (inline for PDFs and images, otherwise a download). Both routes are throttled at API Gateway (page 5/s burst 10, files 10/s burst 20).

## Emails

`POST /api/v1/qc-visits/:id/email` and `GET …/email-preview` gain:

| field | for | what |
|---|---|---|
| `kind: 'update'` | factory, inspector | template `qc_visit_update`: `{changes}` lists what was added / removed / changed since that party's last proposal or update, `{lines}` the full list now. Not refused without a date; refused (409 TERMINAL) on completed / cancelled visits. |
| `includeScheduleLink` | inspector, proposal or update | reuse or create the link before anything is sent and put it in the body (`{scheduleLink}`; appended when the template lacks the token; a posted preview's placeholder or an older link is replaced). |

`attachKitFiles` and `attachProductList` now also work on updates. The preview answers `scheduleLinkAllowed`, `scheduleLinkUrl` (the live link, null when sending would create it) and, for updates, `changes` (`{ sendId, sentAt, kind, comparable, added, removed, changed, count }`, or null).

Every proposal and update stores the lines it listed in `qc_visit_sends.lines_snapshot_json` (every line, PO numbers included; diffs are filtered per audience when shown, so the factory never sees another supplier's PO) and the link it carried in `schedule_link_id`. Sends list `scheduleLinkId` and `lineCount`.

Templates: `qc_visit_update` is seeded; `qc_visit_inspector` no longer says the documents are attached and carries `{scheduleLink}` (the migration rewrites the stored body only where it is still the unedited seed).

## Visit detail and attachments

- `GET /api/v1/qc-visits/:id` (and every visit write) returns `sendChanges: { factory, inspector }`, each `null` (never emailed) or `{ sendId, sentAt, kind, comparable, added, removed, changed, count }`. `comparable: false` means the last email predates snapshots, so nothing can be listed. Lines are compared by order on `quantityToInspect`, `sampleMethod` and `note`.
- Attachments carry `jfCode` (null: the whole visit). `POST …/attachments` takes `jfCode` (must be one of the visit's JF codes); `PATCH /api/v1/qc-visits/:id/attachments/:attId { jfCode?, note? }` moves a file between products / the whole visit.
- `GET /api/v1/qc-visits/:id/schedule-link` → `{ link: { id, audience, url, createdByEmail, createdAt, firstOpenedAt, lastOpenedAt, openCount } | null }`; `POST` the same path `{ rotate? }` → 201 / 200 `{ link }`. Writes need an editor role.

## Deploy

`bash deploy.sh <stage>` applies both migrations, then ships the new `qcScheduleApi` function and its two throttled routes. No secret or IAM change: it uses the provider-level `PO_DOCS_BUCKET` / `JFPRO_DOCS_BUCKET` read access the authed API already has.
