# JFPro frontend guide — supplier unification (handoff for an AI agent)

> **Audience:** an AI agent working in the **JFPro** repo (the app that owns `jfpro.*`).
> **You do NOT need this repo (Shipsline/shipping).** Everything you need is below.
> **Date of the change:** 2026-06-30. All DB changes are already applied to production.

## 1. What happened (context)

Shipsline (the shipping app, DB `jfa`) used to keep its **own** supplier roster. We merged it into **JFPro's `jfpro.suppliers`**, which is now the **single source of truth**. Shipsline no longer has its own supplier tables — it reads `jfpro.suppliers` / `jfpro.supplier_contacts` live through database views.

Concretely, on the JFPro database:
- `jfpro.suppliers` grew from **57 → 214** rows. **157** of those are suppliers imported from Shipsline.
- The 157 imported rows are **marked as legacy** two ways (see §3).
- `jfpro.suppliers` gained **6 new columns** (all nullable/additive — your existing queries are unaffected).
- A `BEFORE INSERT` **trigger** now auto-fills a supplier portal access code on every new row.
- ~95 contact rows were added to `jfpro.supplier_contacts`.

**Nothing about JFPro is broken and no deploy is required for it to keep working.** This guide is about *optional, recommended* UI improvements + a few guardrails.

## 2. New / relevant columns on `jfpro.suppliers`

Pre-existing (unchanged): `id, name, contactPerson, email, wechat, countryOfOrigin, port, paymentTerms, shippingTerms, is_deleted, created_at, updated_at, customFields`.

**Added 2026-06-30** (all nullable):

| Column | Type | Meaning |
|---|---|---|
| `portal_code` | varchar(32) | **Access code for Shipsline's public supplier portal.** Auto-generated (see §5). Indexed. |
| `code` | varchar(64) | Shipsline vendor/short code |
| `country_code` | char(2) | ISO-2 country (distinct from the full-name `countryOfOrigin`) |
| `default_currency` | char(3) | e.g. `USD` |
| `source` | varchar(32) | **`'shipsline'` on the 157 legacy imports**, `NULL` on native JFPro suppliers. Indexed. |
| `jfa_id` | int | Original Shipsline supplier id (provenance). Set on imported + merged rows. |

> Note: `contact_phone`, `address`, `notes`, and `active` were briefly added then dropped (unused on both sides) — do not reference them.

Contacts live in **`jfpro.supplier_contacts`** (`id, supplier_id, name, email, type, wechat, phone, is_primary, …`). Shipsline reads these as the supplier's email list.

## 3. How the legacy (Shipsline) suppliers are marked

Two equivalent markers on the 157 imported rows — use whichever fits your stack:
- **Column:** `suppliers.source = 'shipsline'` (fastest to filter; indexed).
- **Tag:** a `supplier_tag` named **`shipsline-legacy`** → `jfpro.tags.id = 16`, linked via `jfpro.supplier_tags (supplier_id, tag_id=16)`.

```sql
-- the 157 legacy suppliers, either way:
SELECT * FROM jfpro.suppliers WHERE source = 'shipsline';
SELECT s.* FROM jfpro.suppliers s
  JOIN jfpro.supplier_tags st ON st.supplier_id = s.id AND st.tag_id = 16;
```

## 4. Frontend tasks

### 4a. (Recommended) Filter legacy suppliers out of the default list
The supplier list jumped 57 → 214. Default the list to **hide** `source='shipsline'` with a toggle/checkbox **"Show Shipsline legacy"** to reveal them. A `Shipsline (legacy)` badge on those rows (from the `shipsline-legacy` tag) is a nice touch.

**Acceptance:** default supplier list shows ~57 native suppliers; toggling on shows all 214; legacy rows are visually distinguishable.

### 4b. (Optional) Surface the new fields on the supplier detail/edit screen
Show/allow editing of `code, country_code, default_currency`. Show `portal_code` (see guardrail §5) and `countryOfOrigin`/`port`/`wechat` if not already shown.

**Acceptance:** editing these fields persists and round-trips; no validation errors from the wider schema.

### 4c. (No work, just verify) Create-supplier still works
Creating a supplier needs **no change**. Do **not** send `portal_code` on create — the DB trigger fills it. `source`/`jfa_id` stay `NULL` for natively-created suppliers (correct — they aren't Shipsline legacy).

**Acceptance:** create a new supplier; confirm it saves and that `portal_code` is auto-populated (8 chars).

## 5. Guardrails (important)

- **`portal_code` is live-critical for Shipsline's public portal.** Suppliers use it (with their PO number) to log in and set ready dates. **Never blank it.** If you make it editable, forbid empty and offer a "Regenerate" action instead of free-text clearing. It is **auto-generated on insert** by trigger `jfpro.suppliers_portal_code_bi` (8 chars over `23456789ABCDEFGHJKMNPQRSTUVWXYZ`), so you normally treat it as read-only.
- **Renaming a supplier has a cross-app effect.** Shipsline links orders/POs to suppliers by the **supplier name string** (not id), and its public portal matches a PO's supplier name to `jfpro.suppliers.name`. If you rename a supplier in JFPro, that supplier's existing Shipsline POs may stop resolving in the portal until their stored names are updated. Treat supplier renames as significant; ideally warn the user.
- **`is_deleted` is shared.** Shipsline's view maps `jfpro.suppliers.is_deleted` → its own `deleted_at`. If JFPro soft-deletes a supplier, it disappears from Shipsline too (lists, quotes, portal). That's usually desired — just know it's not JFPro-local.
- **Don't drop/repurpose** `source`, `jfa_id`, or the `shipsline-legacy` tag — they're the legacy marker + rollback key.
- **Keep `name` reasonably unique among live suppliers.** Shipsline's portal name-match assumes one live supplier per name. The portal tolerates duplicates (it tries each match's code), but duplicate live names are best avoided.

## 6. Quick test plan
1. Supplier list defaults to ~57; toggle reveals 214; legacy rows badged.
2. Create a supplier → saves, `portal_code` auto-set (8 chars), `source` NULL.
3. Edit a native supplier's `code`/`country_code`/`default_currency` → persists.
4. Open a legacy (`source='shipsline'`) supplier → its `portal_code`, `code`, contacts render.
5. (Sanity) existing JFPro supplier screens that predate this change still load with the wider table.

## 7. Reference: the markers in one place
- Legacy suppliers: `suppliers.source = 'shipsline'` **or** tag `shipsline-legacy` (`tags.id=16`, `supplier_tags`).
- Auto-code trigger: `jfpro.suppliers_portal_code_bi` (BEFORE INSERT).
- Provenance: `suppliers.jfa_id` = original Shipsline id.
- Contacts: `jfpro.supplier_contacts` (single source for both apps).
