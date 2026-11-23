# Telex releases

The telex-released bill of lading a supplier sends once a container's balance is paid. (A **forwarder's** own bill of lading file is something else, with its own table: `docs/forwarder-containers.md`.) One per supplier per shipment, as a packing list or an invoice is. Until now they were kept in Notion.

Built 2026-11-16, the last part of "Send to supplier" (`docs/payment-proof-send.md`): the balance email asks for the telex release; this is where it comes back to.

Rules: `src/lib/telex-releases.js`. SQL and routes: `src/services/telex-release-routes.js`. Tests: `tools/test-telex-releases-lib.js`, `tools/test-telex-releases-routes.js` (both in `npm run test:unit`).

## Where they live, and why not with the invoices

In their **own table**, `shipment_telex_releases` (`2026-11-16_40`). The first plan was a new `doc_kind 'telex_release'` on `shipment_payment_documents`. An audit of that table's readers ruled it out:

- about thirty places in shipping and ShipLine treat "not a proof of payment" as "an invoice": such a row would be read by the invoice check, offered to pay, and shown as a balance's invoice file;
- `GET /supplier-payments` lists as a proof any document on a supplier alone that is not a `balance_invoice`;
- the document reader rewrites a row's `doc_kind` to `remittance` when the file reads like one.

Each could be guarded, and every reader written later would have to be too. A table of its own cannot be mistaken for either.

| Column | |
|---|---|
| `shipment_id`, `shipment_reference` | the container |
| `supplier_name` | whose release it is. Compared **exactly** everywhere (case, punctuation, trailing spaces) |
| `filename`, `s3_key`, `public_url`, `content_type`, `file_size` | the file, in the PO bucket under `telex-releases/<uuid>/<name>`. Public by its unguessable link, as a PO PDF is |
| `source` | `staff` (filed in ShipLine) or `portal` (uploaded by the supplier and accepted) |
| `portal_submission_id` | the portal upload it was accepted from. Unique: accepting twice files once |
| `deleted_at`, `deleted_by_email` | soft delete |

More than one row per supplier and shipment is allowed (a corrected copy); the newest is listed first.

## Where they are shown

On the **shipment card and the container card**, in a **TELEX** tab next to PACKING LISTS and laid out as it is: one block a supplier, its files under it. Not on the Payments page (the first build put a chip and a block there; the user moved it the same day, 2026-11-16: a telex release is a paper of the shipment, as a packing list is).

`GET /api/v1/telex-releases?shipmentId=N` is what the tab lists:

```json
{ "shipmentId": 61, "reference": "333", "canFile": true,
  "suppliers": [{
    "supplierName": "…", "onBoard": true, "balance": true, "state": "uploaded",
    "releases": [{ "id": 9, "filename": "…", "url": "…", "source": "staff", "uploadedByEmail": "…", "createdAt": "…" }],
    "upload": { "id": 3, "status": "open", "note": null, "uploadedByEmail": "…", "file": { "filename": "…", "sentAt": "…" } }
  }] }
```

Who gets a block: every supplier with a live order line on the shipment (`onBoard`: `orders.shipment_id`, or `shipment_lines` while it is a draft or planned), every supplier a balance record of the shipment is filed under (`balance`: `shipment_payments.supplier_name`, the name the portal knows a paid balance by), and anyone a release is filed for or an upload came from. By **exact** name: two spellings of one company are two blocks, which is how a release filed under the wrong one gets seen. Those on the shipment or with a balance come first, by name.

| `state` | |
|---|---|
| `received` | a release is filed for that shipment and exactly that supplier |
| `uploaded` | the supplier's latest upload is waiting for us |
| `rejected` | we sent their latest upload back |
| `awaiting` | nothing yet |

`upload` is the supplier's upload still waiting for us (shown even when a release is already filed, so it can still be decided), else the one we last sent back, whose `note` is what we told them.

ShipLine offers the tab on a booked shipment. Our team can file a release there for **any** supplier in the list, whatever was paid and however: that covers a PI paid through containers, which the portal cannot offer an upload for.

## Routes

All under the greedy `/api/v1/{proxy+}` route. Filing, deleting, accepting and sending back: **admin and standard** users (`403 CANNOT_FILE` otherwise; an accountant's writes are payments). Reading: any signed-in user.

| Route | |
|---|---|
| `GET /api/v1/telex-releases?shipmentId=N` | the shipment's suppliers, each with its state, its filed releases and the supplier's upload (above). `404 NO_SUCH_SHIPMENT` |
| `POST /api/v1/telex-releases` | `{ shipmentId, supplierName, filename, dataBase64, notes? }`: file one ourselves. `201 { release }` |
| `DELETE /api/v1/telex-releases/:id` | an admin, or whoever filed it (`403 NOT_YOURS`) |
| `GET /api/v1/telex-submissions[?status=all]` | suppliers' uploads: those waiting for us, or all. `{ data, unavailable, canFile }` |
| `GET /api/v1/telex-submissions/:id/file` | a five-minute link to the supplier's file |
| `POST /api/v1/telex-submissions/:id/accept` | files it. `{ release, submission, replayed }` |
| `POST /api/v1/telex-submissions/:id/reject` | `{ note }`: sends it back |

A file is a **PDF, PNG or JPEG of at most 15 MB**, told by its first bytes, never by its name or the browser's word for it: `422 BAD_TYPE`, `413 TOO_LARGE`. `404 NO_SUCH_SHIPMENT` for a deleted shipment. `503 NOT_MIGRATED` without the table; `503 PORTAL_UNAVAILABLE` for an upload route on a server without the portal's side.

Accept and send back, on an upload:

| Status, code | |
|---|---|
| `409 ALREADY_DECIDED` | it was received, sent back or voided meanwhile (`allowedNext: ['reload']`) |
| `409 FILE_NOT_SENT` | its file has not arrived, or the supplier withdrew it |
| `409 SHIPMENT_GONE` | accept only: the shipment no longer exists (`allowedNext: ['reject']`) |
| `409 FILE_UNAVAILABLE`, `422 BAD_TYPE`, `413 TOO_LARGE` | accept only: the file cannot be read from the portal, or is not something we file (`allowedNext: ['reject']`) |
| `400 NOTE_REQUIRED` | send back needs at least 3 characters: the supplier reads them |

## A supplier's upload

On the portal's Payments page each payment lists the containers its balance paid, and the supplier uploads the release there (jportal README section 22). The upload is a **portal File** (`jportal.file_transfers`, sent from the supplier: the same bucket, checks and record as any file they send) **tagged** by a row of `jportal.telex_submissions` for a payment and a container.

- The portal admits an upload only for a payment the user can see, and a `balance` line of **that** payment on **that** shipment filed under exactly the payment's supplier. So `supplier_name` and `shipment_id` on the row are already checked; accepting files the release under them.
- It also appears in SUPPLIER PORTAL > Files, as every file a supplier sends does. That is what the nav badge counts.

Accepting, in order:

1. The rules, before anything is read. An upload already received is answered with its release (`replayed: true`).
2. Outside any transaction: the file is read from the portal's bucket (`PI_UPLOADS_BUCKET`, `files/sup/…`) pinned to the ETag the portal stored, checked for what it is, and written to the PO bucket.
3. One transaction: the upload is locked and checked again, the release is written (`source 'portal'`, `portal_submission_id`), the portal's row becomes `received` with `release_id`, and the audit entry is made. Two people accepting at once: the unique key lets one through, and the other is answered with that one's release.

## What the portal is told

The portal reads only its own tables. Our answer travels on the `telex_submissions` row, which shipping writes under its own login:

| We do | The row |
|---|---|
| accept an upload | `status 'received'`, `release_id`, `decided_by`, `decided_at` |
| send one back | `status 'rejected'`, `decided_note` |
| file a release ourselves | a **new row with no file** (`transfer_id NULL`, `status 'received'`, `release_id`), so the supplier is not asked for one we already hold |
| delete a filed release | its rows go to `status 'void'`: the supplier can upload again |

Telling the portal never stops a filing: on a server without the portal's table the release is filed and nothing is told.

No new right for the portal's login on any table of ours.

## Audit

`shipment_telex_release` `create` (with `source`, and `telexSubmissionId` when accepted) and `delete`; `telex_submission` `reject` (with the note).

## Rollout (test, then prod; all user-triggered)

1. **Shipping**: `bash deploy.sh <stage>` creates `shipment_telex_releases`. Filing from ShipLine works from here.
2. **Portal**: its deploy creates `telex_submissions`, applies its grants file, and brings the upload on the Payments page.
3. **ShipLine**: the TELEX tab on the shipment card and the container card, and SUPPLIER PORTAL > Telex.

Any order is safe: each side shows nothing until the other is there.

## Not verified

- Any of it against a real database or bucket: the table is migrated nowhere yet. The routes are checked against a scripted connection and a scripted S3 only.
- Reading the portal's bucket and writing ours on accept. The rights are there (`files/sup/*` read, the PO bucket write), unchanged.
- That shipping's login may write `jportal.telex_submissions` on each stage. It is `admin` on both today, as for the other portal tables it writes.
- A real upload by a supplier, end to end.
