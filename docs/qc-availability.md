# QC visit availability links

Staff send a QC visit's factory and inspector a **no-login link** into the supplier portal (jportal). The page shows the proposed date and the days around it; the recipient ticks every whole day (China time) they could attend, and can change the answer while the link is open. ShipLine shows both parties' answers side by side so staff can pick a day both can do and set the visit date. Nothing is confirmed automatically: confirmation stays the existing flow (by hand, or the PO-code portal form in [`qc-visit-portal.md`](qc-visit-portal.md)).

Code: `src/lib/qc-availability.js` (pure rules, tested by `tools/test-qc-availability-lib.js` in `npm run test:unit`), `src/services/qc-availability-routes.js` (SQL and routes, registered from `src/handlers/orders.js`). The page and its API are jportal's (`portal/src/handlers/availability.js`, `portal/web/src/screens/AvailabilityScreen.tsx`).

## Ownership

The rows live in **jportal's schema**: `<jportal>.qc_availability_requests`, created by jportal's migration `portal/db/migrations/2026-10-29_jportal_qc_availability.sql`. Shipping has **no migration** for it and runs no DDL against it. It reads and writes it cross-schema through `JPORTAL_DB_SCHEMA` (default `jportal`), the same pattern as `po_change_requests` and `pi_submissions`.

| columns | written by |
|---|---|
| `token`, `visit_id`, `visit_ref`, `party`, `recipient_name`, `recipient_email`, `supplier_name`, `factory_label`, `factory_city`, `proposed_date`, `window_start_date`, `window_end_date`, `preferred_lang`, `expires_at`, `created_by_email`, `created_at` | shipping (INSERT) |
| `revoked_at`, `revoke_reason`, `proposed_date`, `row_version` | shipping (UPDATE) |
| `available_days`, `none_available`, `answered_by_name`, `answer_note`, `answered_at`, `answer_count`, `answer_ip`, `row_version` | jportal |

Shipping never writes the answer columns. Every shipping UPDATE bumps `row_version`, so the portal's optimistic answer write (`… WHERE id = ? AND row_version = ?`) sees the change, re-reads and shows the new state. Shipping takes no locking read on the table (the column-level grant may not allow one); its updates are conditional (`WHERE id = ? AND revoked_at IS NULL`), and link creation is serialised per visit by the `qc_visits` row lock.

The token is 5 lower-case base36 characters (~60 million codes; lower case because the column collation is case-insensitive). Links made before 2026-10-29 carry 48 hex characters and still work. A clash with an existing token (unique key) is retried with a fresh token, up to 5 times. It is never written to shipping's audit log, never returned once the link is revoked or expired, and `recipient_email` / `created_by_email` are never shown on the portal page.

### Grant for shipping's login (by hand, as admin)

jportal's migration applies its own runtime grant (`jportal_app`). Shipping's DB login is granted by hand, once per server, after the jportal migration has created the table:

```sql
GRANT SELECT, INSERT, UPDATE (revoked_at, revoke_reason, proposed_date, row_version)
   ON jportal.qc_availability_requests TO '<shipping login>'@'%';
```

(Use the real schema name if `JPORTAL_DB_SCHEMA` is not `jportal`.) Without the table or the grant, shipping degrades instead of failing: see "Missing portal" below.

## Configuration

- `SUPPLIER_PORTAL_URL` (serverless.yml, from the stage secret; already used for the PI decision emails) is the link host. A link is `${SUPPLIER_PORTAL_URL without trailing slashes}/a/<token>` (jportal also still serves the older `/availability/<token>`). Blank means the feature is off: `GET …/availability` answers `enabled: false`, creating a link is `409 PORTAL_URL_UNSET`, and the email preview reports `availabilityEnabled: false`.
- `JPORTAL_DB_SCHEMA` (default `jportal`): the schema holding the table.

## Rules

- **Window**: default = the visit date and the next 5 days (6 tickable days). Staff may widen or narrow it when sending a link from the panel. At most **31 days counting both ends** (`end − start ≤ 30`), `end ≥ start`, the visit date must be inside it, and the window must not have ended: `end ≥ today`, where today is the **China calendar day** (`chinaToday()`, fixed +08:00), never the server's UTC day.
- **Language**: `en` | `zh` | null; default `zh` for the factory, `en` for the inspector.
- **Expiry**: `expiresInDays` 1..90, default 30.
- **Request state**, one precedence used by shipping, jportal and ShipLine alike: `revoked` > `expired` > `answered` > `open`. Only `open` and `answered` can be answered. An expired link keeps its saved answer, and that answer still counts in the side-by-side view.
- **One live link per party**: creating a link for a party revokes that party's earlier unrevoked links with reason `superseded`, in the same transaction as the insert.
- **Revoke reasons**: `staff` (REVOKE in ShipLine), `superseded`, `date_moved`, `confirmed`, `completed`, `cancelled`. The portal page words its message from the reason.

### Lifecycle: open links follow the visit

`syncAvailabilityLinks(conn, { visitId, before, after })` runs on the visit write's own connection and transaction whenever a write may change `status` or `visit_date`:

- from the `updateVisit` closure in `src/services/qc-visit-routes.js`, which covers PATCH (the edit form and ShipLine's USE button), accept-counter, every transition and the confirmation auto-confirm;
- from the supplier portal's confirm in `src/handlers/supplier-portal.js` (`POST /api/v1/portal/qc-visit`), when it moves the visit to CONFIRMED or back to PROPOSED.

For each link not yet revoked (open, answered **or expired**):

| visit change | effect on the link |
|---|---|
| status becomes `CANCELLED` / `COMPLETED` / `CONFIRMED` | revoked, reason `cancelled` / `completed` / `confirmed` (expired links too) |
| `visit_date` moves outside the link's window (or is cleared) | revoked, reason `date_moved` (expired links too) |
| `visit_date` moves inside the window | `proposed_date` updated, so the page's "proposed" badge follows (open / answered links only) |

Decisions behind this: a date change inside the window keeps the link (the ticks still mean something); outside it revokes (the page would propose a date that no longer exists). Links are revoked on CONFIRMED because availability is moot once both parties agreed; if the visit later drops back to PROPOSED, staff send fresh links. Expired links are revoked too: an expired link's saved answer still counts in `collated`, so without the revoke a cancelled visit, or one whose date moved away, would keep showing it. Already-revoked links are left alone.

The hook runs under a `SAVEPOINT` and **never fails the visit write**: a missing table or grant is logged at info, any other error at warn, and either is rolled back to the savepoint while the visit write commits. Only an error that has already ended the transaction (a deadlock) propagates, since the visit write is lost then anyway. A rollback of the visit write rolls the link changes back with it. When it revokes anything it audits `availability_links_revoked { linkIds, reason }` on the visit.

## Routes

Authed through the greedy `/api/v1/{proxy+}` route like the other QC routes. Reads are open to every staff user; writes need an editor role (`admin`, `standard`) and answer read-only users `403 { error: 'Read only.', code: 'READ_ONLY' }`. Errors are `{ error, code, field? }`. Unknown visit → `404 NOT_FOUND`.

### `GET /api/v1/qc-visits/:id/availability`

```ts
{
  enabled: boolean,              // SUPPLIER_PORTAL_URL is set
  unavailable?: true,            // the jportal table (or its grant) is missing here
  links: Link[],                 // every request of the visit, newest first
  collated: {
    days: { date: string, factory: boolean | null, inspector: boolean | null, both: boolean }[],
    parties: { factory: PartySummary | null, inspector: PartySummary | null },
  } | null,                      // null only when unavailable
}
```

When the table is missing: `{ enabled, unavailable: true, links: [], collated: null }` (200, never a 500).

`collated` takes, per party, the newest request that is **not revoked** (an expired one still counts). `days` is every date of the union of the chosen requests' windows, sorted. A cell is `true` (ticked), `false` (answered and not ticked, or "none available"), or `null` (that party has not answered, or the day is outside its window). `both` is true only when both cells are `true`. A party with no unrevoked request is `null` in `parties` and contributes no days.

```ts
type Link = {
  id: number, party: 'factory' | 'inspector',
  url: string | null,            // only while state is open or answered (and the portal URL is set)
  state: 'open' | 'answered' | 'expired' | 'revoked',
  revokeReason: string | null,
  recipientName: string | null,
  windowStartDate: string, windowEndDate: string, proposedDate: string,   // YYYY-MM-DD
  preferredLang: 'en' | 'zh' | null,
  expiresAt: string, revokedAt: string | null,                           // ISO 8601 UTC
  createdByEmail: string | null, createdAt: string,
  availableDays: string[] | null, // null until answered
  noneAvailable: boolean,
  answeredByName: string | null, answerNote: string | null,
  answeredAt: string | null, answerCount: number,
}

type PartySummary = {
  linkId: number, state: Link['state'], revokeReason: string | null,
  windowStartDate: string, windowEndDate: string,
  availableDays: string[] | null, noneAvailable: boolean,
  answeredAt: string | null, answeredByName: string | null, answerNote: string | null,
  expiresAt: string,
}
```

### `POST /api/v1/qc-visits/:id/availability-links`

Body `{ party, windowStartDate?, windowEndDate?, recipientName?, recipientEmail?, preferredLang?, expiresInDays? }`. In one transaction (the visit row locked): revoke the party's earlier links (`superseded`), insert the new request with a snapshot of the visit (`visit_ref`, `supplier_name`, `factory_label`, `factory_city`, `proposed_date = visit_date`). Audits `availability_link_created { linkId, party, windowStartDate, windowEndDate, proposedDate, expiresInDays, supersededLinkIds? }` on `qc_visit` (never the token).

| status | code | when |
|---|---|---|
| 201 | | the new `Link` |
| 400 | `BAD_FIELD` (+ `field`) | `party`, `windowStartDate`, `windowEndDate`, `recipientName`, `recipientEmail`, `preferredLang`, `expiresInDays` as in Rules |
| 403 | `READ_ONLY` | read-only user |
| 404 | `NOT_FOUND` | unknown visit |
| 409 | `PORTAL_URL_UNSET` | `SUPPLIER_PORTAL_URL` is blank |
| 409 | `TERMINAL` (+ `status`) | the visit is COMPLETED or CANCELLED |
| 409 | `NO_DATE` | the visit has no date |
| 503 | `PORTAL_UNAVAILABLE` | the jportal table or grant is missing; `error` is "The supplier portal is not available in this environment yet." Nothing is written. |

### `POST /api/v1/qc-visits/:id/availability-links/:linkId/revoke`

Body ignored (ShipLine sends `{}`). Sets `revoked_at`, `revoke_reason = 'staff'`, bumps `row_version`; audits `availability_link_revoked { linkId, party, reason: 'staff' }`. Revoking an already-revoked link is a no-op that returns it unchanged. Answers **200** with the `Link`; `403 READ_ONLY`; `404 NOT_FOUND` for an unknown visit or a link of another visit; `503 PORTAL_UNAVAILABLE` as above.

### Email: "Include availability link"

The proposal email (`src/services/qc-visit-comms-routes.js`) can carry the link:

- `GET /api/v1/qc-visits/:id/email-preview?…&includeAvailabilityLink=1` (`1`, `true` or `yes`) renders the party's open / answered link if it has one, else the placeholder text **"(availability link will be created when sent)"** (`AVAILABILITY_PLACEHOLDER` in the lib). The preview never creates a link. Every preview response carries `availabilityEnabled: boolean` so ShipLine knows whether to offer the tick: false when `SUPPLIER_PORTAL_URL` is unset or (for a proposal) the jportal table is missing, and then also `availabilityUnavailable: true` for the table. When it is false the preview shows neither a link nor the placeholder.
- `POST /api/v1/qc-visits/:id/email` with `includeAvailabilityLink: true` (proposal kind only; ignored for a cancellation) locks and re-reads the visit row, then reuses the party's open / answered link as it is (no window checks: a reused link is never refused because its dates are now past) or creates one with the **default window**, `recipient_email = to[0]` and `recipient_name` from the factory contact / the inspector, in its own transaction **before** any attachment is fetched or anything is sent. The email path never asks for a window: staff who want a custom one press SEND LINK in the panel first, and the email then reuses that link. `409 PORTAL_URL_UNSET` / `NO_DATE` and `503 PORTAL_UNAVAILABLE` stop the send.
- The `{availabilityLink}` template token renders the whole paragraph (lead-in in English, plus Chinese for the factory, and the link), or nothing when the email carries no link. A posted body (ShipLine sends the previewed body back as-is) is fixed up on send: any other `/a/<5 base36>` or `/availability/<48 hex>` link in it (an older, superseded one) becomes the current URL, a leftover placeholder becomes the current URL, and a body with neither the URL nor the placeholder gets the paragraph appended. A send **without** a link strips the placeholder (the whole paragraph, or the bare text) so it never reaches a recipient (`stripAvailabilityPlaceholderHtml` / `…Text`) (`appendAvailabilityHtml`; `appendAvailabilityText` for a plain-text rendering). Templates stored before the token existed (the seed is `INSERT IGNORE`) therefore still carry the link.

## Missing portal

On a server where jportal's table or shipping's grant is missing (MySQL errno 1049, 1142, 1143 or 1146, `SOURCE_MISSING` in `src/lib/jportal.js`): GET answers `unavailable: true`, the writes and the email answer `503 PORTAL_UNAVAILABLE` (rolled back, never a 500), the preview answers `availabilityEnabled: false, availabilityUnavailable: true` and shows no placeholder, and visit writes carry on (the hook logs at info).

**Prod caveat**: this only works in prod once jportal is deployed there on shipping's MySQL server, which the picker page needs anyway. Until then prod degrades as above.

## Deploy order

1. jportal: `bash deploy.sh test` in `portal` (runs its migration and the `jportal_app` grants).
2. Run the shipping-login `GRANT` above by hand on the server.
3. Confirm `SUPPLIER_PORTAL_URL` is in the `shipping/<stage>` secret, then deploy shipping.
4. Deploy ShipLine.

Shipping deployed before step 1–2 is harmless: everything degrades as described under "Missing portal".
