# Ready-date reminders: a daily email to each supplier who owes us a date

A supplier is emailed a link to the supplier portal **every working day** while one of their PO lines is waiting for a ready date. Nothing is sent from a dialog: the `sendReadyDateReminders` Lambda does it.

Code: `src/lib/ready-date-reminders.js` (pure rules), `src/services/ready-date-reminders.js` (the sweep), `src/handlers/send-ready-date-reminders.js` (the Lambda and a local runner), tested by `tools/test-ready-date-reminders.js` in `npm run test:unit`. Table `ready_date_reminders` (`2026-11-18_10`), template `ready_date_reminder` (`2026-11-18_20`).

## Which lines

A line is in the email for one of two reasons (its `kind`):

| kind | the line | asked for |
|---|---|---|
| `estimated` | has no estimated ready date; staff's **Artwork Confirmed** date (`artwork_confirmed_date`, the date "by us", not the supplier's tick) is at least 7 days ago; the PO's deposit is paid or none is required | the estimated ready date |
| `actual` | has an estimated ready date that is before today | the actual ready date |

Both kinds also need all of this:

- **Status** is on ShipLine's board columns 01 to 06: `SCHEDULED`, `PO_SENT`, `IN_PRODUCTION`, `READY_FOR_QC`, `READY`, `CONSOLIDATED` (`CHASED_STATUSES`).
- **No actual ready date.** A line that has one is never chased, for either date.
- Not an `_FQC` sample line (it follows its product).
- The supplier has not already sent that date on the portal. The portal stages a date as a pending change request for staff to approve (`jportal.po_change_requests`); while it waits, the line is left out. If staff reject it, the line is chased again.

The **deposit** rule is the artwork gate (`loadDepositGate`, docs/artwork-confirmations.md): clear with 0 % in force or a deposit on record; unknown terms with nothing paid hold the line. It applies to `estimated` lines only. An `actual` line is asked for whatever the deposit.

"Today" is the date in Europe/London, as `artwork_confirmed_date` is.

## One email per supplier

All of a supplier's purchase orders go in **one** email a day. Suppliers are matched on the PO's supplier name reduced to letters and digits, lower case (`supplierKey`), so `Suzhou Sunmed Co., Ltd` and `Suzhou Sunmed Co.,Ltd.` are one supplier. A PO with no supplier name gets an email of its own.

**Recipients**: for each PO, the addresses its PO email last went to from ShipLine (that send pre-approved them on the portal, docs/po-portal-send.md); for a PO never emailed, the addresses of its latest artwork request. Duplicates across the supplier's POs go out once. A supplier with no address at all is skipped and logged (`noRecipients`).

**The email**: template `ready_date_reminder` (name "Ready date reminder", category `Supplier portal`, editable in ShipLine's templates; tokens `{supplier}` `{poNumbers}` `{count}`). Under the body the sender appends a fixed bilingual block (`reminderBlockHtml`): an "Estimated ready date needed" section and an "Actual ready date needed" section, each only when it has items, listing them by PO with a link to the PO's page (`<portal>/orders/<id>`); then the button (the PO's page for one PO, `<portal>/orders` for several), how to enter the dates, and how to register. From the second email of a chase it shows "Reminder #n". Sent through Front, in the portal emails' frame.

## When

- Monday to Friday at 01:00 UTC (09:00 in China). Never at a weekend: the schedule has no weekend tick and the code refuses one (`aborted: 'weekend'`).
- At most one email per supplier in 20 hours. A run **claims** the supplier before sending (`ready_date_reminders.last_sent_at`), so a manual run and the scheduled one, or two at once, cannot both email them. A failed send gives the claim back.
- There is no cap: the emails stop only when no line is waiting. To stop them all, disable the schedule.
- Public holidays are not known to it.

## Chases and the Email Receipts page

`ready_date_reminders` holds one row per **chase**: one supplier's run of emails, from the first until none of the lines it asked about is waiting (`closed_at`). A supplier has at most one open chase. One that is chased again later starts a new one.

On ShipLine's Email Receipts page a chase is **one row** (`email_type = 'ready_date_reminder'`, `confirm_via = 'portal'`, `send_table = 'ready_date_reminders'`): the first email is the row, each later one a reminder under it (`email_receipt_reminders`, "Reminded n×", marked Auto). It turns **Confirmed** when the chase closes (`ready_dates_added` in `syncPortalReceipts`, which also closes the chase, so the page is right between sweeps). The row has no `action_url`, so the 48 hour receipt sweep leaves it alone and it has no "Send reminder" button.

## Running it by hand

```
node src/handlers/send-ready-date-reminders.js --dry-run   # who would be emailed about what; sends and writes nothing
node src/handlers/send-ready-date-reminders.js --confirm-host <DB_HOST> --confirm-portal <SUPPLIER_PORTAL_URL>   # SENDS
```

Without `--dry-run` the local runner refuses to start unless both flags repeat the database host and the portal address it is about to use, and prints the two when they are missing. `.env` can pair the live database with the test portal's address: on 2026-11-18 a local run did, and twelve suppliers were emailed links to `jportal-test`. The deployed Lambda takes both from its stage's secret, so it has no such check.

The Lambda takes `{ "dryRun": true }`, `{ "force": true }` (run at a weekend) and `{ "today": "YYYY-MM-DD" }`. The result counts `lines` by kind, `suppliers`, `sent`, `failed`, and what was skipped (`answeredOnPortal`, `depositHeld` per PO, `noRecipients`, `alreadySent`).

## Known gaps

- **Scheduled and Consolidated lines cannot be dated by the supplier.** The portal lets a supplier change ready dates only from `PO_SENT` to `READY`. A line at `SCHEDULED` or `CONSOLIDATED` is in the email, but its date has to be entered by staff in ShipLine.
- A date entered by staff, or approved from the portal, stops the reminder on the next run; there is no "thank you" email.
- ShipLine shows the row's type from its own list (`EMAIL_TYPE_LABEL`); without that entry it shows `ready_date_reminder` as typed.
