# Info bubbles (UI hints) — frontend guide

Routes: `src/services/ui-hint-routes.js` · rules: `src/lib/ui-hints.js` · schema:
`src/db/migrate/2026-11-10_10_shipline_ui_hints.sql` (applied by `bash deploy.sh`;
the Lambdas run no DDL) · tests: `tools/test-ui-hints-lib.js` (unit, in
`npm run test:unit`: the rules, the contract fixture, and the routes over a fake
connection) · contract: `docs/frontend/ui-hints-contract.json`.

ShipLine users write short explanations on the app's captions (a column head, a
field label, a section heading, a tab) for everyone to read: "info bubbles", the
same feature JFPRO has. A caption with a hint is underlined; hovering it shows
the text. This API stores the texts; the page decides where each one shows.

## The key

The page builds a key from where the caption sits and what it is:

```
key   := scope ('/' segment){1,4}         payments.ready/col.supplier
scope := part ('.' part){0,4}             view . tab . sub-tab
part  := [a-z0-9][a-z0-9_-]{0,59}         a segment after '/' may also contain '.'
```

At most 160 characters. One row per key. The grammar and limits are JFPRO's, so
a key reads the same in both apps; the kind prefixes (`col.`, `field.`, `stat.`,
`tab.`, `section.`, `stage.`) are the page's business, the server only checks the
grammar. **Keys are permanent addresses**: renaming a scope or a caption id on
the page orphans the hints written on it — use the move route to carry them.

Keys contain `/`, and behind the HTTP API an encoded `%2F` arrives as a real
slash, so a key always travels in the request body; an existing hint is
addressed by its numeric `id`.

## Routes

| Route | Who | Returns |
| --- | --- | --- |
| `GET /api/v1/ui-hints` | everyone | `{ data: Hint[] }`, every hint, ordered by key |
| `PUT /api/v1/ui-hints` | admin, standard | create `201 { hint }`; update `200 { hint }` (`unchanged: true` when nothing differs; no new version) |
| `POST /api/v1/ui-hints/:id/move` | admin, standard | `200 { hint }` — the same text under `toKey` |
| `DELETE /api/v1/ui-hints/:id?rowVersion=n` | the author, or an admin | `200 { removed: Hint }` |

PUT body — create: `{ key, rowVersion: 0, body, linkPath?, linkLabel? }`; update:
`{ key, id, rowVersion, body, linkPath?, linkLabel? }` with the `rowVersion` the
editor read. Move body: `{ rowVersion, toKey }`.

- `body`: plain text, line breaks kept (CRLF becomes LF), trimmed, 1–600
  characters. Show it as text (`white-space: pre-wrap`), never as HTML.
- `linkPath` + `linkLabel`: both or neither. The path is a ShipLine path
  starting with one of `/bridge /purchase-orders /manifest /draft /warehouse
  /delivered /qc-visits /supplier-portal /shipments /payments /audit-log
  /email-receipts /users /info` (query allowed, at most 200 characters; no `..`,
  no `//`, no backslash). Label at most 80 characters.

**Concurrency.** A hint is born at `rowVersion: 1` and every change adds one.
`rowVersion: 0` only ever creates. Every change to an existing hint names its
`id` and the `rowVersion` the editor read; when someone else saved in between,
the answer is `409 STALE` with the hint as it is now in `current`, so the page
can show theirs beside mine and let the user choose.

Refusals, `{ error, code, current? }`:

| Status | Code | When |
| --- | --- | --- |
| 403 | `CANNOT_WRITE_HINTS` | an accountant, warehouse or exec account writes. Behind ShipLine's own gateway check a 403 reaches `api.ts` as a `ForbiddenError` without its body: word the refusal by what was attempted |
| 403 | `NOT_AUTHOR` | a standard user deletes someone else's hint |
| 400 | `BAD_FIELD` | key, body, link, id or `rowVersion` malformed; a move to where the hint already is |
| 404 | `DELETED` | the hint is gone — or the key was deleted and written again by someone else (new `id`) |
| 409 | `STALE` | `rowVersion` is old; carries `current` |
| 409 | `OCCUPIED` | create on a key that already has a hint; carries `current` |
| 409 | `OCCUPIED_MOVE` | move onto a key that already has a hint |

```ts
interface Hint {
  id: number; key: string; body: string;
  linkPath: string | null; linkLabel: string | null;
  createdBy: string; updatedBy: string; updatedByName: string | null;
  createdAt: string; updatedAt: string;   // ISO, UTC
  rowVersion: number;
}
```

## The contract file

`docs/frontend/ui-hints-contract.json` is the grammar, the limits, the roles, the
link roots and a set of accepted and refused cases, written from the rules by
`node tools/gen-ui-hints-contract.js --write`. ShipLine keeps an identical copy at
`src/hints/ui-hints-contract.json` and tests its own key, body, link and delete
rules against it. Change a rule here → regenerate → copy the file to ShipLine.
`tools/test-ui-hints-lib.js` fails when the file is not what the rules build.

## Audit

`audit_log` rows, `entity_type` `ui_hint`: `create`, `update` (text, link or a
move, which changes `key`) and `delete`, with what changed.

## On the page

Fetch the list once and on a slow refresh; never per caption. An API build
without these routes answers `404` with no `error` field (`isRouteMissing`): the
page then shows its captions exactly as before and offers no Explain mode.
Deploy the backend first.
