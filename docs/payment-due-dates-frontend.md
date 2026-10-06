# Due dates set by hand — frontend guide

ShipLine's Payments flow page works each payment's due date out from the
supplier's terms, the company payment rules and the goods' dates. A person can
set a date by hand instead — for a whole payment, or for one row (one PO's
share) of it — and clear it again to go back to the derived one.

Same base URL and auth as the rest of the API. Routes in
`src/services/payment-due-date-routes.js`, rules in `src/lib/payment-due-dates.js`,
table `payment_due_dates` (migration `2026-10-06_10_payment_due_dates.sql`).

## Keys

A payment has no row of its own (the page works it out), so a date hangs on the
key the page builds — the same keys sign-offs use, plus one for a single row:

```
deposit:<purchase order id>                     the PO's deposit payment
balance:<currency>:<CONTAINER>|<supplier key>   a supplier's balance in one container
item:<row id>                                   ONE row of a payment: a PO's share
                                                (e.g. item:derived:bal:368:330,
                                                 item:stated:17:330, item:derived:dep:368)
```

A row's own date beats a date on its payment. For goods not booked into a
container the row id is the PO's (`item:derived:bal:<po>:none`), so one date
covers that PO's unbooked goods wherever parts of them sit (draft, plan, none)
— the page strips the `@…` part a split row carries before building the key.
QC-invoice keys (`qc:<id>`) are refused: QC units are paid when the user
chooses and take no due date.

## Routes

```
GET    /api/v1/payment-due-dates          → { data: PaymentDueDate[] }   (any allowlisted user)
PUT    /api/v1/payment-due-dates          → { dueDate: PaymentDueDate, unchanged?: true }
DELETE /api/v1/payment-due-dates/:id      → { removed: PaymentDueDate }  (404 NOT_FOUND once gone)
```

```json
PUT body   { "key": "balance:USD:330|suzhou sunmed co.,ltd.", "dueDate": "2026-11-20", "note": "agreed with the supplier" }
PaymentDueDate {
  "id": 7, "key": "balance:USD:330|suzhou sunmed co.,ltd.", "scope": "payment",   // "item" for item:… keys
  "dueDate": "2026-11-20", "note": "agreed with the supplier",
  "setByEmail": "ops@example.com", "setByName": "Ops", "setAt": "2026-10-06T09:30:00.000Z"
}
```

- One row per key: PUT again replaces the date and note (the same date and note
  again answers `unchanged: true` and writes nothing). DELETE removes the row.
- `dueDate` must be a real calendar date, `YYYY-MM-DD`; `note` optional, up to
  500 characters. Otherwise `400 BAD_FIELD`.
- **Who:** admin and standard users set and clear (`403 CANNOT_SET_DUE_DATE`
  otherwise); accountants read. Same people as sign-offs.
- Every write is in `audit_log` as `entity_type = 'payment_due_date'`.
- A container rename (`PATCH /shipments/:id { reference }`) carries these rows
  with it, re-keyed, and counts them as `dueDates` in the records it reports.

## What the page does with them

- The date replaces the derived one on that row (`PaymentItem.dueDate`,
  `contractualDate`); the chart, the windows, "overdue" and the row order
  follow. The derived date is kept beside it (`dueOverride.derivedDueDate`).
- Nothing else changes: a "not payable yet" row stays not payable; grace days
  are not added on top; a date set behind today is overdue, never "slipped";
  owed air freight keeps the date set rather than riding the next transfer.
  An extra charge riding with the payment takes the payment's date as usual.
- Every date cell marks a date set by hand with a pen and a hover ("Set by hand
  by Ops on 06 OCT · derived 01 NOV"); the pop-up's flag list carries "date set
  by hand". A group (a container row, a payment) whose rows are only partly
  set says "N of M rows have a date set by hand".
- Every date cell edits in place: click the date, type, Enter (Escape
  cancels), ↺ goes back to the derived date. On a Balances due payment row
  that is the whole payment (every PO in it, stored under the payment key); on
  a Balances due container header, and on a By container row, it is every
  supplier's — and forwarder's — payment in that container (one row per key);
  on a Deposits due or Not payable yet row it is that line (the deposit key,
  or the row key). The payment pop-up has the same:
  "Set date" / "Change" / "Back to derived" in the header for the whole
  payment, and each PO row's date in place for that row alone.
- An extra charge in a payment (or a forwarder's cost that is a payment on its
  own) takes a date set on its payment key like any other row.
- A backend without the route: the page shows derived dates and offers no
  controls. Deploy the backend first.
