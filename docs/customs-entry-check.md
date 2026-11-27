# Customs-entry check

An AI report on a customs document of a container (a forwarder's draft entry, the entry as customs accepted it, or a file our team add): does it match what we have on file for that container, or at least come to the same end result? Built 2026-11-27. The broker groups and orders the goods its own way, so nothing is compared by position.

## How a report is made

| Step | Who | What |
|---|---|---|
| 1. Read | Gemini, shown nothing of ours | the document into JSON: the header, every goods item, the taxes (`readEntry`) |
| 2. Sums | code (`src/lib/customs-entry-compare.js`) | their items against our rows: per commodity code (the end result), and in groups tied by the product codes an item's description prints |
| 3. Judge | Gemini, shown the reading, our rows and the sums | which item is which of our rows, the likely reason for each difference, a verdict and a summary in plain words (`judgeEntry`) |

The report's verdict is the **more serious** of the model's and the sums'. The model can raise what the sums cannot see (a wrong importer, a typo it recognises); it cannot pass figures that do not agree. Every figure in the report, the pound effect of a difference included, is computed in code.

| Verdict | Meaning |
|---|---|
| `match` | every item is one of our rows and every figure agrees |
| `same_result` | grouped, ordered or described differently, same commodity codes, cartons, weights, value and duty |
| `incomplete` | nothing disagrees, but a commodity code, duty rate, carton count, gross weight or value could not be compared |
| `differences` | a code, duty rate, origin or figure that is not ours; goods of ours not declared; declared goods that are not ours; the wrong container |
| `no_basis` | nothing on file for the container |
| `unreadable` | no goods items could be read |

Tolerances: cartons and duty rates exact; weights 0.5 kg or 0.5%; value $1 or 0.1%.

## What "on file" is (`loadBasis`)

1. **`sheet`**: the newest clearance instructions sent from ShipLine for the container (`shipment_clearance_instructions`, `docs/clearance-instructions.md`), with its freight cost.
2. **`live`**, when no sheet was sent: the same rows built now, by ShipLine's own rules (`clearanceInstructions.ts`): the container's lines (not `_FQC`, not `SCHEDULED` / `PO_SENT` / `DESTROYED` / `REPACKAGED`), JFPRO's commodity code, category and net carton weight, the supplier's country, JFPRO's Import (UK) duty for that code and country, and the supplier's packing list for cartons and weights where one was read. Values are units × our order price. Freight is not compared.
3. **`none`**: the container has no lines.

## Routes (staff sign-in; `src/services/customs-entry-routes.js`)

| Route | Answer |
|---|---|
| `GET /api/v1/shipments/:id/customs-checks` | `{ shipmentId, documents: [{ key, source: 'upload' \| 'document' \| 'staff', uploadId, documentId, kind, filename, reference, forwarderName, state: 'waiting' \| 'filed' \| 'sent_back' \| 'ours', at, runs, check }], unchecked }`. `check` is the document's newest report in short, or null |
| `POST /api/v1/shipments/:id/customs-checks` | body `{ uploadId }`, `{ documentId }`, `{ checkId }` (run again), `{ missing: true }` (every document never checked, at most 4 a call) or `{ filename, dataBase64, kind? }` (a file of our own, 4 MB). `202 { started: [check], alreadyRunning: [check] }` |
| `GET /api/v1/customs-checks/:id` | `{ check }` with `report` |

A check in short: `{ id, shipmentId, source, uploadId, documentId, kind, filename, status: 'processing' | 'succeeded' | 'failed', verdict, headline, summary, mrn, basisKind, findingCount, error: { code, message } | null, requestedByEmail, createdAt, analyzedAt }`. A run left `processing` for 15 minutes is shown `failed` with `INTERRUPTED` and may be run again.

The report: `{ verdict, verdicts: { numbers, ai }, headline, summary, findings: [{ severity: 'critical' | 'warning' | 'info', field, itemNo, entry, ours, explanation, effect }], reasons, notCompared, checks, byCode, groups, selfCheck, basis, container, document, items, rows, models }`.

Accepting a forwarder's `customs_draft` or `customs_entry` upload starts its report when it has none. A report on an upload still waiting is asked for with `{ uploadId }` or `{ missing: true }`.

Refusals: `404 NO_SUCH_SHIPMENT` · `404 NO_SUCH_DOCUMENT` · `400 BAD_FIELD` · `400 FILE_NOT_ITS_TYPE` · `413 FILE_TOO_LARGE` · `503 NOT_MIGRATED`.

## What is kept

`customs_entry_checks` (migration `2026-11-27_40`): one row per run on one file, with what the model read (`extract_json`) and the report as shown (`report_json`). Running again makes a new row.

## From the command line

```
node tools/check-customs-entry.js <entry.pdf> [--container 313 | --shipment 68] [--json] [--out report.json]
```

Finds the container from the number the entry prints, reads the database `.env` names and writes nothing.

## Not done

- No ShipLine screen yet: the routes are not called by anything.
- The routes and the kept run have not been run against a database: only the reading, the sums and the judgement have (through the command-line tool, on one CDS entry).
- A draft in a broker's own layout has not been tried: only a CDS print.
- JFPRO's duty rates are what our team typed. Nothing here looks a code up in the UK tariff.
- Freight is compared only when a clearance sheet holds the container's cost, and only against an addition of the same amount in dollars.
