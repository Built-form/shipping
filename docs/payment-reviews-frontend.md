# Payment sign-offs — frontend guide

Every payment on ShipLine's Payments flow page is signed off by **two different
people** before it is paid, and has **one assignee**: the accountant who pays it.
The assignee is never one of the two.

Who can do what (user decision, 2026-09-28):

| Role | Sign off | Assign | Record a payment (and upload its proof) |
| --- | --- | --- | --- |
| `admin`, `standard` | yes | yes | yes |
| `accountant` | **no** | yes | yes |
| anything else | no | no | — |

The API enforces the first two columns. Recording a payment is the existing
`/api/v1/supplier-payments` routes; the accountant exception for those lives in
ShipLine's read-only backstop (`api.ts`, `ACCOUNTANT_WRITES`).

```
GET    /api/v1/payment-reviews                    → { data, assignees, required }   current sign-offs, every assignee
GET    /api/v1/payment-reviews?paymentKey=…       → one payment's history (withdrawn and replaced included)
POST   /api/v1/payment-reviews                    → 201 { review, reviews }          sign off as the caller
DELETE /api/v1/payment-reviews/:id                → 200 { reviews }                  withdraw (own; any as admin)
GET    /api/v1/payment-assignees/candidates       → { data: [{ email, displayName }] }  the accountants
PUT    /api/v1/payment-assignees                  → 200 { assignee }                 set or clear (null)
```

Code: routes `src/services/payment-review-routes.js`, rules
`src/lib/payment-reviews.js`, schema
`src/db/migrate/2026-09-28_20_payment_reviews.sql`. Tests:
`tools/test-payment-reviews-lib.js` (in `npm run test:unit`) and
`tools/test-payment-reviews.js` (HTTP, in-process, TEST database).

## What a "payment" is

The page works each payment out from the terms and the goods on board — there
is no payment row. A sign-off is keyed by the key the page builds:

```
deposit:<purchase order id>                     every deposit due on that PO
balance:<CUR>:<container>|<supplier>            one supplier's balance in one container
                                                (container upper-cased, '-' for none;
                                                 supplier lower-cased, spaces collapsed)
qc:<QC invoice document id>                     the QC units one uploaded QC invoice bills
                                                (shipment_payment_documents.id) — 2026-10-02
```

A QC invoice's figure is what is still owed on the QC units it bills, so paying
one of them changes the figure and the invoice is signed off again. `kind` is
`'qc'`, `currency` and `containerRef` come back null from the key.

Anything else is `400 BAD_FIELD`. At most 255 characters.

**A review records the figure the reviewer saw** (`amount`, `currency`). Only
the page knows today's figure, so the page decides whether a review still
counts: one whose figure differs by a cent or more does not, and the payment is
signed off again (user: "should need reviewing again"). A reviewer holds one
active review per payment: reviewing a changed figure revokes the old row
(`revokedReason: 'superseded'`) and adds a new one.

## POST /api/v1/payment-reviews

```json
{ "paymentKey": "balance:USD:307|lipu dong fang wooden articles co., ltd.",
  "currency": "USD", "amount": 2296,
  "supplierName": "Lipu Dong Fang Wooden Articles Co., Ltd.", "poNumbers": ["PO_00278H"], "dueDate": "2026-10-12" }
```

The reviewer is the signed-in user (`req.userEmail`) — never a body field.
`amount` is rounded to the cent and must be more than 0; a balance key's
currency must match `currency`; `supplierName`, `poNumbers`, `dueDate` are kept
for the history only.

| Status | `code` | When |
| --- | --- | --- |
| 403 | `CANNOT_REVIEW` | the caller is not admin or standard (accountants included) |
| 409 | `ASSIGNEE_CANNOT_REVIEW` | the caller is this payment's assignee |
| 409 | `ALREADY_REVIEWED` | the caller already signed off this figure |
| 400 | `BAD_FIELD` | bad key, amount, currency or date |

## DELETE /api/v1/payment-reviews/:id

Withdraws a sign-off. The row stays (`revokedReason: 'withdrawn'`,
`revokedByEmail`). `403 NOT_YOURS` for someone else's unless admin; `409
NOT_ACTIVE` when it was already withdrawn or replaced; `404` when unknown.

## PUT /api/v1/payment-assignees

`{ "paymentKey": "deposit:306", "assigneeEmail": "someone@built-form.co.uk" }`,
or `"assigneeEmail": null` to unassign (the row is deleted; the audit log keeps
it). One per payment.

| Status | `code` | When |
| --- | --- | --- |
| 403 | `CANNOT_ASSIGN` | the caller is not admin, standard or accountant |
| 422 | `NOT_ACCOUNTANT` | the address is not an `accountant` on `shipping_allowed_emails` |
| 409 | `ASSIGNEE_HAS_REVIEWED` | that person holds a current sign-off on this payment |

`/api/v1/users` is admin-only, so the picker reads
`GET /api/v1/payment-assignees/candidates` instead (email + display name of
every accountant; any allowlisted user).

## Shapes

```ts
interface PaymentReview {
  id: number; paymentKey: string; kind: 'deposit' | 'balance' | 'qc';
  supplierName: string | null; poNumbers: string | null; containerRef: string | null;
  currency: string; amount: number; dueDate: string | null;
  reviewedByEmail: string; reviewedByName: string | null;   // name from shipping_allowed_emails.display_name
  reviewedAt: string; revokedAt: string | null; revokedByEmail: string | null;
  revokedReason: 'withdrawn' | 'superseded' | null;
}
interface PaymentAssignee {
  paymentKey: string; assigneeEmail: string; assigneeName: string | null;
  assignedByEmail: string | null; assignedAt: string;
}
```

## Audit

`audit_log` rows, `entity_type` `payment_review` (create; update on withdraw or
replace) and `payment_assignee` (create / update / delete).

## Not enforced here

- **The API never refuses a payment for missing sign-offs.** Since 2026-10-02
  ShipLine's Record payment FORM does (user's rule; before that it only
  warned): every new line needs an uploaded invoice and two sign-offs at
  today's figure, and a new payment needs its proof document. It is a rule of
  that form only — `POST /supplier-payments` still accepts anything, and the
  status chips ("mark paid") on a balance record are not covered.
  - Going forward only: lines already on a recorded payment are not checked
    again, and an edit asks for no proof. A line added in an edit is checked.
  - A top-up (`source: 'top_up'`) needs no invoice; a credit being used needs
    nothing; a payment made wholly by credit needs no proof.
  - An admin may record anyway with a typed reason. The form writes it as the
    last line of the payment's `note`:
    `[OVERRIDE] Recorded without: <what was missing>. Reason: <text>`.
  - The PO page and the container/shipment balance tabs open the same form
    without the payments model, so they cannot check anything: a new line
    cannot be recorded from them, and the form says to use the Payments page.
  - Rules and wording: `ShipLine/src/components/payments/payRules.ts`.
- The accountant's view-only status is still enforced in ShipLine's browser
  code only; the API does not refuse other writes from an accountant token.
  (Pre-existing; unchanged by this feature.)
