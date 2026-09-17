# Draft container history (audit trail) — frontend brief

Every draft container now has a permanent history: lines added, changed and
removed, every PDF generated, every email sent, renames, and how the draft
ended (converted into a real container, or deleted). The history stays
readable after the draft is gone. This document is the API contract plus the
UX ShipLine implements on top of it.

## The model

Drafts are implicit — a draft is "the allocation rows sharing a
`draft_container_name`" — so `audit_log.entity_id` (an integer) could not point
at one, and a rename or a delete made any history unreachable.

The new `draft_containers` **registry** gives every name that has ever existed a
stable integer id. Every draft event is a row in the existing `audit_log` table
with `entity_type = 'draft_container'` and `entity_id = <registry id>`:

- renames keep the id (the registry row's `name` changes, history stays put);
- a registry row is never deleted — closing a draft only stamps `closed_reason`
  / `closed_at` / `closed_by_email` / `container_number`;
- the row is created on the first line, document or QA sheet for a name.

At first deploy a one-shot backfill registers every name that already left a
trace (allocations, quote/forwarder documents, QA sheets) and reconstructs the
events those tables imply (`line_added` per live allocation, `document_generated`
per document, `document_sent` per email, QA equivalents) at their original
timestamps. Reconstructed rows carry `backfilled: true` in their payload.

### Event vocabulary (`audit_log.action`)

| action | before_json | after_json |
|---|---|---|
| `create` | — | `{ draftName }` |
| `line_added` | — | `{ draftName, allocationId, orderId, allocated, jfCode, asin, productName, supplier, poNumber, orderQuantity, movedFrom? }` |
| `line_updated` | line snapshot (old `allocated`) | line snapshot (new `allocated`) |
| `line_removed` | line snapshot `+ movedTo?` | — |
| `renamed` | `{ draftName: old }` | `{ draftName: new, allocations, documents, qaDocuments }` |
| `document_generated` | — | `{ draftName, documentId, type, supplier, version, url, csvUrl, batchId, comments, supplierDocuments[], failedSuppliers[] }` — one event per generate run |
| `document_sent` | — | `{ draftName, documentId, type, supplier, version, sendId, sentTo[], subject, frontMessageUid, frontConversationId, receiptToken, attachments[] }` — one event per email |
| `qa_document_generated` | — | `{ draftName, documentId, ref, version, orderIds[], rowCount, url, csvUrl, comments }` |
| `qa_document_sent` | — | `{ draftName, documentId, ref, version, sendId, sentTo[], subject, frontMessageUid, frontConversationId, receiptToken, filename }` |
| `converted` | `{ draftName, lineCount, totalUnits, lines[] }` | `{ draftName, containerNumber, externalContainerNumber, vesselName, eta, etd, freightType, port, awbNumber, packs[] }` |
| `deleted` | `{ draftName, lineCount, totalUnits, lines[] }` | — |
| `reopened` | `{ draftName, closedReason, containerNumber }` | `{ draftName }` — a closed name received a new line |

Every payload carries `draftName`, so a row reads on its own in the global feed.
`user_email` is the signed-in user (from the JWT), `null` for reconstructed rows
without a recorded author.

A per-line `PUT` that changes `draftContainerName` is a line **moving** between
drafts (`line_removed` on the source with `movedTo`, `line_added` on the target
with `movedFrom`). The whole-draft rename is the endpoint below.

## Endpoints

All under the usual `/api/v1`, same auth as everything else.

### `POST /draft-containers/rename` — rename a draft everywhere

```json
{ "from": "DRAFT-SEA-260917-110203 - 268", "to": "DRAFT-SEA-260917-110203 - 268 Ningbo" }
```

One transaction renames the allocations, the quote/forwarder documents and the
QA sheets tagged with the name, and the registry row; writes one `renamed`
event. Replaces the per-line PUT fan-out, which could split a draft in two on a
partial failure and left its documents orphaned under the old name.

- `200 { ok, id, name, allocations, documents, qaDocuments }` (`unchanged: true`
  when `from === to`)
- `404` unknown draft · `409` another draft holds `to` · `400` missing / >100 chars

### `POST /draft-containers/close` — delete or convert a whole draft

```json
{ "name": "DRAFT-SEA-… - 268", "reason": "converted",
  "containerNumber": "268", "externalContainerNumber": "MSCU1234567",
  "vesselName": "…", "eta": "2026-10-01", "etd": "2026-09-20",
  "freightType": "SEA", "port": "Ningbo", "packs": [{ "orderId": 629, "qty": 500 }] }
```

Deletes every allocation in one statement, stamps the registry row, writes one
`converted` or `deleted` event carrying a snapshot of the lines it held. For
`reason: "converted"` `containerNumber` is required and is stored on the
registry row (`container_number`) so the list can say what the draft became.

- `200 { ok, id, name, reason, deleted, alreadyClosed }` — closing an
  already-closed draft is a no-op (`alreadyClosed: true`, no second event)
- `404` unknown draft · `400` bad reason / missing containerNumber

### `GET /draft-container-registry` — every draft that ever existed

Query (all optional): `name` exact lookup · `q` substring on name or container
number · `status` = `open` | `empty` | `converted` | `deleted` | `closed` ·
`limit` (default 500, max 2000). Newest activity first.

```json
{ "data": [{
  "id": 12, "name": "DRAFT-SEA-260917-110203 - 268 Ningbo",
  "status": "converted",                      // open | empty | converted | deleted
  "createdAt": "…", "createdByEmail": "…", "lastActivityAt": "…",
  "closedAt": "…", "closedByEmail": "…", "closedReason": "converted",
  "containerNumber": "268",
  "lineCount": 0, "totalUnits": 0,            // live counts
  "documentCount": 3, "qaDocumentCount": 1, "eventCount": 41
}] }
```

`status`: `closed_reason` when set, else `open` if lines exist, else `empty`
(every line was removed one at a time — the pre-registry delete path — and the
draft was never closed).

### History — the existing audit-log route

`GET /audit-log?entityType=draft_container&entityId=<registry id>&limit=500`

Same shape as every other audit read. The global feed (`GET /audit-log` with
no entity filter) includes draft events too, so `?action=converted` lists every
draft that became a container.

## Where it appears in ShipLine

1. **On each draft container card** (Draft view · Build tab): a `HISTORY`
   section under the QA documents, lazily loaded, reloading as the lines
   change. Timeline, newest first, one plain sentence per event.
2. **Audit log · Draft containers tab** (header menu → Audit log): the
   registry, searchable by name / container number / person, filterable by
   status; pick a draft to read its full history — the way to reach a draft
   after it has been converted or deleted.
3. **Audit log · All changes**: draft events render as the same sentences,
   with a `Draft <name>` chip.

Rename / delete / "Create Real" in the Draft view now call the whole-draft
routes above. If the backend has not been deployed yet they fall back to the
old per-line PUT/DELETE calls (which record nothing), and the history sections
say so.

## Tests

- `node --test tools/test-draft-audit-lib.js` — unit tests for
  `src/lib/draft-audit.js` against a scripted connection (no DB).
- `node tools/test-draft-container-audit.js <orderA> <orderB>` — end-to-end
  smoke test against a running local server (`npm run dev:local`, port 3031).
