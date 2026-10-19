# Supplier changes (PO change requests) — frontend guide

Routes: `src/services/po-change-request-routes.js` · rules: `src/lib/po-change-requests.js`
· tests: `tools/test-po-change-requests-lib.js` (unit, in `npm run test:unit`)
· data: `jportal.po_change_requests`, owned by the supplier portal (the `jportal` repo).

Suppliers now update their purchase-order lines in the supplier portal instead of
the old access-code page. The portal **does not change orders**: each change is
saved as a *pending request*. ShipLine shows the line's current value beside the
value the supplier asked for, and staff approve it, which writes it to the order.

A supplier can ask for three things per line:

| Field | Meaning | Written to |
| --- | --- | --- |
| `estimatedReadyDate` | new estimated ready date, or `null` to clear it | `orders.estimated_ready_date` |
| `actualReadyDate` | new actual ready date, or `null` to clear it | `orders.actual_ready_date` |
| `status` → `READY_FOR_QC` | "goods finished, come and inspect" | `orders.status` (stamps `dates.ready_for_qc`, as a normal status change does) |

A line has **at most one pending request**. If the supplier edits the line again
before it is approved, the old request is replaced (`superseded`) by a new one with
a new id, so always act on what the page last loaded.

## The page: "Supplier changes"

A dedicated page, e.g. `/supplier-changes`, in the main nav with a badge showing the
total pending count (sum of `pending` from the suppliers route).

**Left / top: suppliers with pending changes** — `GET /api/v1/po-change-requests/suppliers`.
One row per supplier: name, pending count, POs affected, oldest request ("waiting
3 days"). Sort by oldest first. Selecting one loads its requests.

**Main: that supplier's requests** — `GET /api/v1/po-change-requests?supplierId=…`.
Group by PO (`poNumber`), one row per line:

```
PO_00412J
  JF1234  Bamboo hanger 45cm     Estimated ready   01 Nov 2026  →  20 Nov 2026     [Approve]
                                 Actual ready      —            →  14 Oct 2026
                                 Status            In production → Ready for QC
  JF1290  Velvet hanger           Estimated ready  05 Nov 2026  →  —  (cleared)    [Approve]
                                 ⚠ Can't approve: the line has moved past the factory stages.
                                                                    [Approve all applicable (7)]
```

- **Old → new.** Each entry in `changes[]` is one row: `oldValue` (the order's value
  *now*) → `newValue`. Show `null` as "—" and a `newValue` of `null` as "cleared".
- **Changed since the supplier sent it.** When `changedSinceSubmit` is true, someone
  changed the order after the supplier sent this request. Show a small note: "was
  {submittedOldValue} when the supplier sent this". The approval still overwrites it.
- **Already done.** `alreadyApplied: true` means the order already holds the new
  value; approving just closes the request. Grey the arrow.
- **Can't approve.** When `canApply` is false, disable the row's button and show
  `blockers[].message`. These rows stay pending until someone rejects them, or the supplier changes or
  withdraws them.
- Show `submittedBy` and `submittedAt` on each row (small, muted).

**Approve one** — `POST /api/v1/po-change-requests/:id/apply` with
`{ rowVersion }` from the loaded row. On success, replace the row with the
returned `request` (now `applied`), and update any cached order with `order`.

**Reject one** — a "Reject" button beside Approve on every pending row, including
rows that can't be approved (that is how those get cleared).
`POST /api/v1/po-change-requests/:id/reject` with `{ rowVersion }`, and optionally
a `note` asking for a reason. The request leaves the list; the order is not
changed. The supplier can send a new change for that line afterwards.

**Approve all** — a button per supplier, labelled with the number of approvable
rows. Confirm first ("Apply 7 changes from Acme to their orders?"), then
`POST /api/v1/po-change-requests/apply-supplier` with `{ supplierId, ids }`,
where `ids` are the ids of the rows **shown and approvable** (required, at most
500). The route only ever applies the ids it is sent, so a request that arrives
while the page is open is never applied unseen. Then
show "7 applied, 1 skipped" and list the skipped ones with their reasons, and
reload the supplier's list and the suppliers list.

Optional: a note field on approve (`note`, up to 500 characters) stored with the
decision. A "History" tab can list decided rows with `?status=applied` (or `all`).
On those rows `oldValue` is the value when the supplier sent the request (what
it changed), not today's value, and the two flags are always `false`.

Who: any signed-in ShipLine user, like `PATCH /orders/:id/status`. If only admins
should approve, hide the buttons for non-admins (`X-User-Type`) and tell the
backend so it can enforce it too.

## Routes

| Route | Returns |
| --- | --- |
| `GET /api/v1/po-change-requests/suppliers` | `{ data: SupplierSummary[] }` — suppliers with pending requests |
| `GET /api/v1/po-change-requests?status=&supplierId=&purchaseOrderId=&orderId=` | `{ data: ChangeRequest[] }` — `status` defaults to `pending`; `all` for every status; at most 500 |
| `POST /api/v1/po-change-requests/:id/apply` | `200 { request: ChangeRequest, order: Order \| null }` |
| `POST /api/v1/po-change-requests/apply-supplier` | `200 { applied: ChangeRequest[], skipped: Skipped[], orders: Order[] }` |
| `POST /api/v1/po-change-requests/:id/reject` | `200 { request: ChangeRequest }` — now `rejected`; the order is not touched |

`apply` and `reject` body: `{ rowVersion?: number, note?: string }`.
`apply-supplier` body: `{ supplierId: number, ids: number[], note?: string }` (1 to 500 ids).
`ids` is required (`400 BAD_FIELD` without it); there is no "apply everything
pending" call.

`order` / `orders` are the same `Order` shape as `GET /api/v1/orders` (only the
orders that actually changed; `order` is `null` when nothing needed writing).

Refusals (`{ error, code }`):

| Status | Code | When |
| --- | --- | --- |
| 400 | `BAD_FIELD` | bad id, status filter, note, or `ids` missing, empty or not ids |
| 400 | `TOO_MANY` | more than 500 `ids` in one bulk call; send them in parts |
| 404 | `NOT_FOUND` | no such request |
| 409 | `NOT_PENDING` | already applied, rejected, superseded or withdrawn — reload |
| 409 | `STALE` | `rowVersion` no longer matches — reload |
| 409 | `CONFLICT` | the supplier was saving the same lines at that moment — reload and try again |
| 422 | `NOT_APPLICABLE` | can't be applied as it stands; `blockers` says why |

Bulk approval never fails as a whole because of one bad row: rows that can't be
applied come back in `skipped` and the rest are applied.

```ts
interface SupplierSummary {
  supplierId: number;
  supplierName: string;
  pending: number;          // pending requests
  applicable: number;       // of those, approvable right now
  purchaseOrders: number;   // distinct POs
  oldestSubmittedAt: string | null; // ISO, UTC
}

type ChangeField = 'estimatedReadyDate' | 'actualReadyDate' | 'status';

interface Change {
  field: ChangeField;
  oldValue: string | null;          // pending: the order NOW ('YYYY-MM-DD', or a status); decided: = submittedOldValue
  newValue: string | null;          // what the supplier asked for; null = clear
  submittedOldValue: string | null; // the order when the supplier sent it
  changedSinceSubmit: boolean;      // oldValue !== submittedOldValue
  alreadyApplied: boolean;          // oldValue === newValue
}

interface Blocker {
  code: 'line_missing' | 'po_supplier_changed' | 'line_locked' | 'bad_date' | 'qc_from' | 'qc_no_date'
      | 'nothing_requested' | 'not_pending';
  message: string;                  // English, ready to show
}

interface ChangeRequest {
  id: number;
  status: 'pending' | 'applied' | 'rejected' | 'superseded' | 'withdrawn';
  rowVersion: number;               // send back on apply
  supplierId: number;
  supplierName: string;
  purchaseOrderId: number;
  poNumber: string | null;
  orderId: number;                  // the line (orders.id)
  jfCode: string | null;
  productName: string | null;
  lineStatus: string | null;        // the line's status now; null when deleted
  changes: Change[];
  canApply: boolean;                // pending and no blockers
  blockers: Blocker[];
  submittedBy: string;              // supplier user's email
  submittedAt: string | null;       // ISO, UTC
  decidedBy: string | null;         // ShipLine user who applied it
  decidedAt: string | null;
  decidedNote: string | null;
  appliedAt: string | null;
}

interface Skipped { id: number; orderId: number | null; blockers: Blocker[] }
```

Blockers, for reference: the line was deleted or moved to another PO
(`line_missing`); the PO was deleted, or its supplier is no longer the one that
sent the request, e.g. the PO was reassigned or the supplier deleted
(`po_supplier_changed`, checked again when approving); it has moved past the
factory stages — only `PO_SENT`, `IN_PRODUCTION`, `READY_FOR_QC` and `READY`
lines can change (`line_locked`); "ready for QC" only from `IN_PRODUCTION`
(`qc_from`) and only with an actual ready date, already on the order or in the
same request (`qc_no_date`). Apart from `po_supplier_changed`, these are the same
rules the old supplier page enforced.

## What to deploy

1. **Database access.** The shipping API reads and updates
   `jportal.po_change_requests`, a table in the supplier portal's schema on the same
   MySQL server. Its database login (`DB_USER` in the `shipping/test` and
   `shipping/prod` secrets) needs, if it is not `admin`:

   ```sql
   GRANT SELECT, UPDATE ON jportal.po_change_requests TO '<shipping login>'@'%';
   ```

   The portal's own migrations must have run on that server first (they create the
   table). If the schema is not called `jportal` there, add `JPORTAL_DB_SCHEMA` to
   the shipping secret; otherwise nothing to add.
2. **Shipping API.** `bash deploy.sh test`, check, then `bash deploy.sh` (prod). No
   migration: the table belongs to the portal.
3. **ShipLine.** The new page above, deployed after the API.
4. **Supplier portal.** Nothing to deploy — it already saves the requests.
