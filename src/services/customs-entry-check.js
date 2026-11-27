'use strict';

// Customs-entry check (2026-11-27): an AI report on a forwarder's customs entry
// (or its draft): does it match what we have on file for that container, or at
// least come to the same end result?
//
// Three steps, two of them the model's:
//   1. READ (model, blind). The document is read into JSON with nothing of ours
//      beside it: a reader shown the expected answer tends to find it.
//   2. SUMS (code, src/lib/customs-entry-compare.js). Their items against our
//      rows, by commodity code and by product code, never by position.
//   3. JUDGE (model, sighted). The reading, our rows and the sums go to the
//      model, which says which of their items are which of our rows, what each
//      difference most likely is, and gives a verdict in plain words.
// The report's verdict is the MORE SERIOUS of the model's and the sums': the
// model can raise a concern the sums cannot see (a wrong importer, a typo it
// recognises), but it cannot wave through figures that do not agree.
//
// "What we have on file" (loadBasis) is the clearance instructions sent for the
// container from ShipLine (docs/clearance-instructions.md) when there are any;
// else the same rows built here from the container's lines, JFPRO's commodity
// codes and duty rates, and the supplier's packing list.
//
// The pool has a single connection, so nothing here holds one across a model
// call: load, release, call, reacquire (as packing-list-check.js).

const { GoogleGenAI } = require('@google/genai');
const log = require('../lib/logger');
const { withGeminiRetry } = require('../lib/gemini-retry');
const C = require('../lib/customs-entry-compare');

const { isSourceMissing } = require('../lib/jportal');

// A table of another app's (JFPRO's) or of a later migration that this login cannot
// read here: absent, not broken (1044 is "access denied to database").
const unreadable = e => isSourceMissing(e) || (Boolean(e) && e.errno === 1044);

// Flash reads a clean print well; Pro is the fallback, and the default for the
// judgement, which is reasoning and not reading. Env overrides for retuning.
const READ_MODEL = process.env.CUSTOMS_ENTRY_READ_MODEL || 'gemini-3.8-flash';
const JUDGE_MODEL = process.env.CUSTOMS_ENTRY_JUDGE_MODEL || 'gemini-3.1-pro-preview';
// As the packing-list check: `priority` answers in seconds where the standard tier can take minutes.
const GEMINI_TIER = process.env.CUSTOMS_ENTRY_GEMINI_TIER === undefined ? 'priority' : process.env.CUSTOMS_ENTRY_GEMINI_TIER.trim();
const FALLBACK_MIN_MS = 120 * 1000;

// A line on its way in no shipment (packing-list-check.js INACTIVE_STATUSES): never expected in a container.
const INACTIVE_STATUSES = ['SCHEDULED', 'PO_SENT', 'DESTROYED', 'REPACKAGED'];

// ── 1. The reading ───────────────────────────────────────────────────────
const READ_SYSTEM = `You read UK IMPORT CUSTOMS DECLARATIONS for a UK importer and return structured JSON. The document is a PDF or image of one of:
- a customs entry as HMRC's CDS accepted it: a "CDS Import" print of numbered boxes ("[33] Commodity [6/14]", "[42] Item price [4/14]"), with a first page for the header and item 1 and "List of Items" pages for the rest;
- an older C88 / SAD or CHIEF "E2" entry print;
- a customs broker's DRAFT of an entry, in the broker's own layout.

Copy what is printed. Never calculate a figure, never fill a gap from general knowledge, never carry one item's value onto another. Null (or an empty list) when the document does not print it.

Header:
- documentType: "entry" for an entry customs has accepted (it has an MRN and a status such as Accepted or Cleared), "draft" for a draft or pre-lodgement not yet accepted, "other" if this is not a customs declaration at all.
- mrn: the Movement Reference Number, 18 letters and digits, WITHOUT the spaces it is printed with ("26GB D09IA4NI RXQA R0" -> "26GBD09IA4NIRXQAR0"). lrn: the Local Reference Number.
- declarationStatus, and acceptanceDate as YYYY-MM-DD (the "Status date/time" of an accepted entry).
- exporterName, importerName, importerEori (the importer's EORI: "GB" and 12 digits), declarantName.
- vessel: the arriving vessel's name ("11|MSC KALINA" -> "MSC KALINA"). deliveryTerms: the incoterm and place ("FOB|GB ILKESTON" -> "FOB GB ILKESTON").
- containerNumbers: every container number printed (4 letters and 7 digits).
- totalPackages, totalGrossKg: the header's own totals ("[6] Total packages"; the header's "[35] Gross mass").
- invoiceTotal and invoiceCurrency: the header's invoice total ("[22] Invoice total 49072.10 USD").
- invoiceNumbers: commercial invoice numbers cited at header level, if any.
- exchangeRates: each rate printed, as how many units of that currency make 1 GBP ("1 USD ... (for 1 GBP) ... 1.3396" -> currency "USD", perGbp 1.3396).
- additions: each header-level addition or deduction ("[45] Additions and deductions": "AV GBP 1592.00" -> code "AV", currency "GBP", amount 1592.00).
- fiscalReferences: each additional fiscal reference as printed ("FR1|GB240737125").
- taxTotals: the summary of taxes for the whole entry, one per tax type: type ("A00" is customs duty, "B00" is import VAT), payable (the "Payable amount") and assessed (the "Assessed amount").

Items: one entry for EVERY goods item, in document order, across all pages.
- itemNo: the item number.
- description: the description of goods, verbatim, on one line.
- packages: the number of packages ("37|PK|ADDRESSED" -> 37). packageKind: the kind code ("PK").
- commodityCode: the commodity code's digits without spaces. On a CDS print the 8-digit code and a 2-digit TARIC code sit in adjoining cells ("30059031" then "00"): return commodityCode "30059031" and taricCode "00". If one 10-digit number is printed, return its first 8 digits as commodityCode and its last 2 as taricCode.
- originCountry: the country of origin's 2-letter code.
- procedureCode: the procedure code with any additional procedure code ("4000" and "000" -> "4000 000"). preference: the preference code ("100").
- grossKg, netKg: the item's gross and net mass in kilograms.
- itemPrice and itemCurrency: the item's price or amount invoiced ("USD 1739.00" -> 1739.00 and "USD"). statisticalValue: the statistical value.
- invoiceNumbers: the commercial invoice number(s) the item cites: a previous document or document line of type 380 ("Z|380|26/89138" -> "26/89138").
- taxes: every row of the item's calculation of taxes: type ("A00", "B00", ...), base (the tax base), rate (the percentage as a number: "20.00" -> 20), payable, assessed ("Total tax assessed").

Copy digits exactly. Better null than a guess.`;

const S = t => ({ type: [t, 'null'] });
const LIST = items => ({ type: 'array', items });
const OBJ = properties => ({ type: 'object', additionalProperties: false, properties, required: Object.keys(properties) });

const READ_SCHEMA = OBJ({
    documentType: { type: 'string', enum: ['entry', 'draft', 'other'] },
    mrn: S('string'),
    lrn: S('string'),
    declarationStatus: S('string'),
    acceptanceDate: S('string'),
    exporterName: S('string'),
    importerName: S('string'),
    importerEori: S('string'),
    declarantName: S('string'),
    vessel: S('string'),
    deliveryTerms: S('string'),
    containerNumbers: LIST({ type: 'string' }),
    totalPackages: S('integer'),
    totalGrossKg: S('number'),
    invoiceTotal: S('number'),
    invoiceCurrency: S('string'),
    invoiceNumbers: LIST({ type: 'string' }),
    exchangeRates: LIST(OBJ({ currency: { type: 'string' }, perGbp: S('number') })),
    additions: LIST(OBJ({ code: { type: 'string' }, currency: S('string'), amount: S('number') })),
    fiscalReferences: LIST({ type: 'string' }),
    taxTotals: LIST(OBJ({ type: { type: 'string' }, payable: S('number'), assessed: S('number') })),
    items: LIST(OBJ({
        itemNo: S('integer'),
        description: S('string'),
        packages: S('integer'),
        packageKind: S('string'),
        commodityCode: S('string'),
        taricCode: S('string'),
        originCountry: S('string'),
        procedureCode: S('string'),
        preference: S('string'),
        grossKg: S('number'),
        netKg: S('number'),
        itemPrice: S('number'),
        itemCurrency: S('string'),
        statisticalValue: S('number'),
        invoiceNumbers: LIST({ type: 'string' }),
        taxes: LIST(OBJ({ type: { type: 'string' }, base: S('number'), rate: S('number'), payable: S('number'), assessed: S('number') })),
    })),
});

// ── 3. The judgement ─────────────────────────────────────────────────────
const JUDGE_SYSTEM = `You check a UK import customs entry against what the importer has on file for that container, and write the verdict a logistics clerk will act on. You are given three things as JSON:
- ENTRY: what the customs broker declared, as read from the document.
- OURS: the importer's own rows for the container (what it told the broker, or what its own records say is in the container). "basis" says which, and "notes" say where a figure came from.
- NUMBERS: sums and differences already worked out by a program. Trust these figures. Do not redo the arithmetic and do not contradict it. "tolerances" are already allowed for: do not report rounding inside them.

The broker groups and orders goods its own way: several of our rows may be one entry item, or one of our rows may be split across items. That alone is NOT a difference. What matters is the END RESULT: the same goods, under the same commodity codes and origin, with the same cartons, weights and value, at the same duty rate, so the same duty and VAT.

verdict:
- "match": every item corresponds to our rows and every figure agrees.
- "same_result": grouped, ordered or described differently, or with a slip that changes nothing (a mistyped product code in a description), but the end result is the same.
- "incomplete": nothing disagrees, but something essential could not be compared (NUMBERS.notCompared).
- "differences": anything that changes, or may change, what is declared or paid: a commodity code or duty rate that is not ours; cartons, weight or value that do not agree; goods of ours that are not declared, or declared goods that are not ours; the wrong importer, container or invoice.

items: for EVERY entry item, which of our rows it covers (ourRows: the "row" numbers of OURS; empty if none), a status ("match", "same_result", "differs", "not_ours") and a short note only when there is something to say.

findings: one for each thing a person should look at, most serious first. Nothing to report is a valid answer: an empty list.
- severity "critical": changes duty or VAT, or is a compliance risk (a wrong commodity code or origin, goods not declared, the wrong importer). "warning": a figure that disagrees and needs checking. "info": worth knowing, changes nothing.
- field: what it is about ("commodity code", "value", "cartons", "product code", "importer", ...). itemNo: the entry item, or null for the whole entry.
- entry and ours: what each side says, as short text with units.
- explanation: what is different and the most likely reason, when the figures point to one. A value divided by our units (a row's "units", when OURS gives them; else its cartons) gives the price each side used: say both. A product code printed on the entry that is not ours but is one keystroke from one of ours (NUMBERS.groups[].possibleTypos), where the figures only add up with ours, is probably a typo: say so. A row of ours that no item covers, whose cartons and value are exactly what the entry is short by, was probably not declared (or did not ship): say which.
- effect: what it changes in pounds, taken from NUMBERS' "approx" figures (valueGbp, dutyGbp, vatGbp) and from nowhere else; "none" when it changes nothing; null when NUMBERS gives no such figure.
Also look at what the sums do not: is the importer the right company; does anything in the entry look out of place (an unexpected procedure, preference, VAT rate or tax type). Do not invent differences. Do not explain customs terms the clerk does not need.

summary: two to four plain sentences. Start with the answer: does it match, come to the same result, or differ, and why in one clause. Then the one or two things to do, if any. No customs jargon without saying what it means, no headings, no lists.`;

const JUDGE_SCHEMA = OBJ({
    verdict: { type: 'string', enum: C.VERDICTS },
    summary: { type: 'string' },
    items: LIST(OBJ({
        itemNo: { type: 'integer' },
        ourRows: LIST({ type: 'integer' }),
        status: { type: 'string', enum: ['match', 'same_result', 'differs', 'not_ours'] },
        note: S('string'),
    })),
    findings: LIST(OBJ({
        severity: { type: 'string', enum: ['critical', 'warning', 'info'] },
        field: S('string'),
        itemNo: S('integer'),
        entry: S('string'),
        ours: S('string'),
        explanation: { type: 'string' },
        effect: S('string'),
    })),
});

const coded = (code, message, cause) => Object.assign(new Error(message), { code, ...(cause ? { cause } : {}) });

/** One structured answer from Gemini: the first of `models` that gives parseable JSON. */
async function callModel({ label, models, contents, system, schema, maxOutputTokens, deadline = null }) {
    if (!process.env.GEMINI_API_KEY) throw coded('NOT_CONFIGURED', 'GEMINI_API_KEY is not configured.');
    const ai = new GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY });
    let lastErr;
    for (const modelId of [...new Set(models.filter(Boolean))]) {
        if (lastErr && deadline != null && deadline - Date.now() < FALLBACK_MIN_MS) {
            log.warn(`[customs-entry-check] ${label}: no time left for the fallback model`, { modelId });
            break;
        }
        try {
            const startedAt = Date.now();
            const response = await withGeminiRetry(`customs-entry-${label}(${modelId})`, () => ai.models.generateContent({
                model: modelId,
                contents,
                config: {
                    temperature: 0,
                    maxOutputTokens,
                    systemInstruction: system,
                    responseMimeType: 'application/json',
                    responseSchema: schema,
                    ...(GEMINI_TIER ? { serviceTier: GEMINI_TIER } : {}),
                },
            }), 5, { deadline });
            log.info(`[customs-entry-check] ${label}`, { modelId, ms: Date.now() - startedAt, tokens: response?.usageMetadata?.totalTokenCount ?? null });
            let parsed;
            try { parsed = JSON.parse(response?.text || ''); }
            catch { throw coded('MODEL_NO_JSON', 'The model did not return parseable JSON.'); }
            return { parsed, modelUsed: modelId };
        } catch (e) {
            lastErr = e;
            log.warn(`[customs-entry-check] ${label}: model failed`, { modelId, error: String(e.message || '').slice(0, 160) });
        }
    }
    throw coded(lastErr?.code === 'MODEL_NO_JSON' ? 'MODEL_NO_JSON' : lastErr?.code === 'NOT_CONFIGURED' ? 'NOT_CONFIGURED' : 'MODEL_CALL_FAILED',
        `Gemini call failed: ${lastErr?.message ?? 'unknown error'}`, lastErr);
}

/** Step 1. → { entry (cleaned), raw (as the model gave it), modelUsed } */
async function readEntry({ bytes, contentType, model = null, deadline = null }) {
    const out = await callModel({
        label: 'read',
        models: [model || READ_MODEL, JUDGE_MODEL],
        contents: [
            { text: 'Read this customs declaration per the schema: the header, every goods item and the taxes.' },
            { inlineData: { mimeType: contentType || 'application/pdf', data: bytes.toString('base64') } },
        ],
        system: READ_SYSTEM, schema: READ_SCHEMA, maxOutputTokens: 32768, deadline,
    });
    return { entry: C.normaliseEntry(out.parsed), raw: out.parsed, modelUsed: out.modelUsed };
}

/** What the judge is shown: the three things its instructions name. */
function judgeInput({ entry, basis, rows, container, comparison }) {
    return {
        ENTRY: {
            documentType: entry.documentType, mrn: entry.mrn, status: entry.status, importer: entry.importer, importerEori: entry.importerEori,
            exporter: entry.exporter, declarant: entry.declarant, vessel: entry.vessel, deliveryTerms: entry.deliveryTerms,
            containerNumbers: entry.containerNumbers, totalPackages: entry.totalPackages, totalGrossKg: entry.totalGrossKg,
            invoiceTotal: entry.invoiceTotal, invoiceCurrency: entry.invoiceCurrency, exchangeRates: entry.exchangeRates,
            additions: entry.additions, fiscalReferences: entry.fiscalReferences, taxTotals: entry.taxTotals,
            items: entry.items.map(i => ({
                itemNo: i.itemNo, description: i.description, productCodes: i.jfCodes, commodityCode: i.commodityCode, origin: i.origin,
                packages: i.packages, grossKg: i.grossKg, netKg: i.netKg, price: i.price, currency: i.currency,
                procedure: i.procedure, preference: i.preference, invoiceNumbers: i.invoiceNumbers, taxes: i.taxes,
            })),
        },
        OURS: {
            basis: basis.label,
            notes: basis.notes || [],
            container: container ? { ourNumber: container.reference, carrierNumber: container.trackingRef, vessel: container.vesselName } : null,
            freightUsd: basis.containerCostUsd ?? null,
            rows: rows.map(r => ({
                row: r.index + 1, supplier: r.supplier, description: r.description, productCodes: r.jfCodes, commodityCode: r.commodityCode,
                origin: r.originCode || r.origin, cartons: r.cartons, ...(r.units !== null ? { units: r.units } : {}),
                grossKg: r.grossKg, netKg: r.netKg, valueUsd: r.valueUsd,
                dutyPercent: r.dutyPercent, invoiceNumbers: r.invoiceNumbers, notes: r.notes,
            })),
        },
        NUMBERS: {
            verdict: comparison.verdict, reasons: comparison.reasons, notCompared: comparison.notCompared,
            tolerances: { cartons: 'exact', dutyRate: 'exact', kg: '0.5 kg or 0.5%', value: '1.00 or 0.1%' },
            wholeContainer: comparison.header, byCommodityCode: comparison.byCode, groups: comparison.groups, entryAddsUp: comparison.selfCheck,
        },
    };
}

/** Step 3. → { judgement, modelUsed } */
async function judgeEntry({ entry, basis, rows, container, comparison, model = null, deadline = null }) {
    const out = await callModel({
        label: 'judge',
        models: [model || JUDGE_MODEL, READ_MODEL],
        contents: [{ text: `ENTRY, OURS and NUMBERS follow as one JSON object.\n\n${JSON.stringify(judgeInput({ entry, basis, rows, container, comparison }))}` }],
        system: JUDGE_SYSTEM, schema: JUDGE_SCHEMA, maxOutputTokens: 16384, deadline,
    });
    return { judgement: out.parsed, modelUsed: out.modelUsed };
}

/**
 * Steps 2 and 3 on an entry already read. → the report (lib buildReport).
 * With nothing on file, or nothing read, there is nothing to judge: the report
 * says so (`no_basis` / `unreadable`). A judge that cannot be had leaves the
 * sums' verdict standing, and `models.judgeError` says why.
 */
async function assess({ entry, basis, container = null, readModel = null, judgeModel = null, deadline = null }) {
    const models = { read: readModel, judge: null };
    if (!entry.items.length || basis.kind === 'none' || !(basis.rows || []).length) {
        return C.buildReport({ entry, basis, container, comparison: null, judgement: null, models });
    }
    const rows = C.normaliseOurs(basis.rows);
    const comparison = C.compare({ entry, rows, container, containerCostUsd: basis.containerCostUsd ?? null });
    let judgement = null;
    try {
        const out = await judgeEntry({ entry, basis, rows, container, comparison, model: judgeModel, deadline });
        judgement = out.judgement;
        models.judge = out.modelUsed;
    } catch (e) {
        log.warn('[customs-entry-check] no judgement: the sums stand alone', { code: e.code, error: String(e.message || '').slice(0, 160) });
        models.judgeError = e.code || 'MODEL_CALL_FAILED';
    }
    return C.buildReport({ entry, basis: { ...basis, rows }, container, comparison, judgement, models });
}

// ── What we have on file ─────────────────────────────────────────────────
const nameKey = v => String(v ?? '').trim().replace(/\s+/g, ' ').toLowerCase();
const baseCode = v => String(v ?? '').trim().toUpperCase().replace(/_FQC$/, '');
const round = (n, dp) => Math.round((n + Number.EPSILON) * 10 ** dp) / 10 ** dp;
const positive = n => typeof n === 'number' && Number.isFinite(n) && n > 0;
const figure = (v) => { if (v === null || v === undefined || v === '') return null; const n = Number(v); return Number.isFinite(n) ? n : null; };
const sum = (parts, dp) => (parts.some(p => p === null) ? null : round(parts.reduce((a, p) => a + p, 0), dp));
const parseJson = (v) => { if (v == null) return null; if (typeof v === 'object') return v; try { return JSON.parse(v); } catch { return null; } };

// How suppliers and sheets write a country, beside JFPRO's own list (jfpro.duty_countries).
const COUNTRY_WORDS = Object.freeze({
    china: 'CN', prc: 'CN', 'p.r. china': 'CN', 'p.r.china': 'CN', 'pr china': 'CN', "people's republic of china": 'CN', 'peoples republic of china': 'CN',
    uk: 'GB', 'united kingdom': 'GB', 'great britain': 'GB', england: 'GB', turkey: 'TR', turkiye: 'TR', 'türkiye': 'TR',
    usa: 'US', 'united states': 'US', 'united states of america': 'US', vietnam: 'VN', 'viet nam': 'VN', 'south korea': 'KR', korea: 'KR',
    'hong kong': 'HK', taiwan: 'TW', india: 'IN', italy: 'IT', austria: 'AT', germany: 'DE', malaysia: 'MY', pakistan: 'PK', thailand: 'TH',
});

/** text → ISO code, or null when it cannot be told which country it is. */
async function countryReader(conn) {
    const words = new Map(Object.entries(COUNTRY_WORDS));
    const codes = new Set(Object.values(COUNTRY_WORDS));
    try {
        const [rows] = await conn.query('SELECT code, name FROM jfpro.duty_countries');
        for (const r of rows) {
            const code = String(r.code || '').trim().toUpperCase();
            if (!/^[A-Z]{2}$/.test(code)) continue;
            codes.add(code);
            if (r.name) words.set(nameKey(r.name), code);
        }
    } catch (e) {
        if (!unreadable(e)) throw e;
    }
    return (v) => {
        const key = nameKey(v);
        if (!key) return null;
        if (words.has(key)) return words.get(key);
        const up = key.toUpperCase();
        return /^[A-Z]{2}$/.test(up) && codes.has(up) ? up : null;
    };
}

const SHIPMENT_COLUMNS = 'id, reference, tracking_ref, vessel_name, source_draft_id';
const shipmentOf = r => (r ? { id: Number(r.id), reference: r.reference || null, trackingRef: r.tracking_ref || null, vesselName: r.vessel_name || null, sourceDraftId: r.source_draft_id == null ? null : Number(r.source_draft_id) } : null);

/** The container: by its id, by our number (313), or by the carrier's (FFAU3767679). */
async function findShipment(conn, { shipmentId = null, container = null } = {}) {
    if (shipmentId != null) {
        const [rows] = await conn.query(`SELECT ${SHIPMENT_COLUMNS} FROM shipments WHERE id = ? AND deleted_at IS NULL`, [Number(shipmentId)]);
        return shipmentOf(rows[0]);
    }
    const asTyped = String(container ?? '').trim();
    const key = asTyped.toUpperCase().replace(/[^A-Z0-9]/g, '');
    if (!key) return null;
    const [rows] = await conn.query(
        `SELECT ${SHIPMENT_COLUMNS} FROM shipments
          WHERE deleted_at IS NULL AND merged_into_id IS NULL AND (reference = ? OR REPLACE(UPPER(tracking_ref), ' ', '') = ?)
          ORDER BY id DESC LIMIT 1`,
        [asTyped, key]
    );
    if (rows[0]) return shipmentOf(rows[0]);
    const [lines] = await conn.query(
        `SELECT shipment_id FROM orders
          WHERE deleted_at IS NULL AND shipment_id IS NOT NULL AND REPLACE(UPPER(external_container_number), ' ', '') = ?
          ORDER BY id DESC LIMIT 1`,
        [key]
    );
    return lines[0] ? findShipment(conn, { shipmentId: lines[0].shipment_id }) : null;
}

/** The newest clearance instructions that went (or may have gone) for the container; null when there are none. */
async function sentSheet(conn, shipmentId) {
    try {
        const [rows] = await conn.query(
            `SELECT c.id, c.version, c.container_cost_usd, c.rows_json, n.status, DATE_FORMAT(n.sent_at, '%Y-%m-%d') AS sent_on
               FROM shipment_clearance_instructions c JOIN freight_container_notices n ON n.id = c.notice_id
              WHERE c.shipment_id = ? AND n.status IN ('sent', 'sending')
              ORDER BY (n.status = 'sent') DESC, c.id DESC LIMIT 1`,
            [shipmentId]
        );
        return rows[0] || null;
    } catch (e) {
        if (!unreadable(e)) throw e;
        return null;
    }
}

/**
 * The rows ShipLine would put on the container's clearance instructions today
 * (ShipLine src/components/freight/clearanceInstructions.ts buildClearanceRows,
 * whose rules these are): a supplier's products that share a JFPRO category and
 * a commodity code are one row. Cartons and weights are the supplier's packing
 * list's where one was read for the product, else ours. What is not held stays
 * null and is said in the row's notes.
 */
async function liveRows(conn, shipment, readCountry) {
    const notes = [];
    const [lines] = await conn.query(
        `SELECT o.id, o.jf_code, o.supplier, o.quantity, o.unit_price, o.units_per_carton, o.carton_weight, o.product_name
           FROM orders o
          WHERE o.deleted_at IS NULL AND o.status NOT IN (?)
            AND (o.shipment_id = ? OR o.id IN (SELECT sl.order_id FROM shipment_lines sl WHERE sl.shipment_id = ?))
          ORDER BY o.supplier, o.jf_code, o.id`,
        [INACTIVE_STATUSES, shipment.id, shipment.id]
    );
    // A factory QC sample (JF0197_FQC) is not in the box and not on the sheet.
    const boxed = lines.filter(o => o.jf_code && !/_FQC$/i.test(String(o.jf_code).trim()));
    if (!boxed.length) return { rows: [], notes };
    const codes = [...new Set(boxed.map(o => baseCode(o.jf_code)))];

    // JFPRO: the product (commodity code, net weight of a carton, category), the supplier's country, the duty.
    const products = new Map();
    const suppliers = new Map();
    const duty = new Map();
    try {
        const [ps] = await conn.query(
            `SELECT p.jfcode, p.name, p.commodityCode, p.netCartonWeightKg,
                    (SELECT c.name FROM jfpro.product_categories pc JOIN jfpro.categories c ON c.id = pc.category_id
                      WHERE pc.product_id = p.id ORDER BY pc.category_id LIMIT 1) AS category
               FROM jfpro.products p WHERE p.jfcode IN (?)`,
            [codes]
        );
        for (const p of ps) if (!products.has(baseCode(p.jfcode))) products.set(baseCode(p.jfcode), p);
        const [ss] = await conn.query('SELECT name, countryOfOrigin, country_code FROM jfpro.suppliers');
        for (const s of ss) {
            const code = /^[A-Za-z]{2}$/.test(String(s.country_code || '').trim()) ? String(s.country_code).trim().toUpperCase() : readCountry(s.countryOfOrigin);
            if (!suppliers.has(nameKey(s.name)) || code) suppliers.set(nameKey(s.name), { code: code === 'UK' ? 'GB' : code, text: String(s.countryOfOrigin || '').trim() });
        }
        const commodity = [...new Set([...products.values()].map(p => String(p.commodityCode || '').replace(/\s+/g, '')).filter(Boolean))];
        if (commodity.length) {
            const [ds] = await conn.query(`SELECT commodity_code, origin, duty_percent FROM jfpro.duty_rates WHERE market = 'GB' AND commodity_code IN (?)`, [commodity]);
            for (const d of ds) duty.set(`${d.commodity_code}|${String(d.origin || '').toUpperCase()}`, Number(d.duty_percent));
        }
    } catch (e) {
        if (!unreadable(e)) throw e;
        notes.push('JFPRO could not be read from here, so commodity codes, origins and duty rates are missing from our side.');
    }

    // The suppliers' packing lists for the container, the live version of each.
    const packed = new Map();   // product code → [{ cartons, gross, net }]
    const invoiceOf = new Map();   // product code → Set of invoice numbers
    let lists = [];
    try {
        [lists] = await conn.query(
            `SELECT id, invoice_number, extract_json FROM packing_lists
              WHERE deleted_at IS NULL AND superseded_by_id IS NULL AND status = 'succeeded'
                AND ((container_kind = 'booked' AND container_number = ?) OR (draft_container_id IS NOT NULL AND draft_container_id = ?))`,
            [shipment.reference, shipment.sourceDraftId]
        );
    } catch (e) {
        if (!unreadable(e)) throw e;
    }
    for (const l of lists) {
        for (const row of (parseJson(l.extract_json) || {}).lines || []) {
            const code = baseCode(row.jfCode);
            if (!code || !positive(figure(row.cartons))) continue;
            if (!packed.has(code)) packed.set(code, []);
            packed.get(code).push({ cartons: figure(row.cartons), gross: figure(row.grossWeightPerCarton), net: figure(row.netWeightPerCarton) });
            if (l.invoice_number) { if (!invoiceOf.has(code)) invoiceOf.set(code, new Set()); invoiceOf.get(code).add(String(l.invoice_number).trim()); }
        }
    }

    // One product of one supplier, every order line of it together.
    const items = new Map();
    for (const o of boxed) {
        const jfCode = baseCode(o.jf_code);
        const supplier = String(o.supplier || '').trim();
        const p = products.get(jfCode);
        const quantity = figure(o.quantity) || 0;
        const perCarton = figure(o.units_per_carton);
        const cartons = positive(perCarton) ? Math.ceil(quantity / perCarton) : null;
        const cartonKg = figure(o.carton_weight);
        const netCartonKg = p ? figure(p.netCartonWeightKg) : null;
        const price = figure(o.unit_price);
        const line = {
            cartons,
            grossKg: cartons !== null && positive(cartonKg) ? cartons * cartonKg : null,
            netKg: cartons !== null && positive(netCartonKg) ? cartons * netCartonKg : null,
            valueUsd: positive(price) ? quantity * price : null,
        };
        const id = `${nameKey(supplier)}|${jfCode}`;
        const have = items.get(id);
        if (!have) {
            const from = suppliers.get(nameKey(supplier)) || null;
            items.set(id, {
                supplier, jfCode, type: String((p && (p.category || p.name)) || o.product_name || '').trim(),
                commodityCode: String((p && p.commodityCode) || '').replace(/\s+/g, ''),
                originCode: from ? from.code : null, originText: from ? from.text : '', knownSupplier: Boolean(from), inJfpro: Boolean(p),
                ...line, units: quantity, ours: { cartons: line.cartons, grossKg: line.grossKg, netKg: line.netKg }, fromList: false,
            });
            continue;
        }
        have.units += quantity;
        have.cartons = sum([have.cartons, line.cartons], 0);
        have.grossKg = sum([have.grossKg, line.grossKg], 2);
        have.netKg = sum([have.netKg, line.netKg], 2);
        have.valueUsd = sum([have.valueUsd, line.valueUsd], 2);
        have.ours = { cartons: have.cartons, grossKg: have.grossKg, netKg: have.netKg };
    }
    for (const item of items.values()) {
        const listed = packed.get(item.jfCode);
        if (!listed) continue;
        const cartons = listed.reduce((a, r) => a + r.cartons, 0);
        const weight = (pick, ours) => {
            if (listed.every(r => positive(pick(r)))) return round(listed.reduce((a, r) => a + r.cartons * pick(r), 0), 2);
            return ours !== null && positive(item.ours.cartons) ? round((ours / item.ours.cartons) * cartons, 2) : null;
        };
        item.grossKg = weight(r => r.gross, item.ours.grossKg);
        item.netKg = weight(r => r.net, item.ours.netKg);
        item.cartons = cartons;
        item.fromList = true;
    }

    const grouped = new Map();
    for (const item of items.values()) {
        const id = `${nameKey(item.supplier)}|${item.commodityCode}|${nameKey(item.type)}`;
        if (!grouped.has(id)) grouped.set(id, []);
        grouped.get(id).push(item);
    }
    const rows = [...grouped.values()].map((its) => {
        const first = its[0];
        const originCode = (its.find(i => i.originCode) || first).originCode;
        const rowNotes = [];
        const commodityCode = first.commodityCode;
        let dutyPercent = null;
        if (!commodityCode) rowNotes.push(`${its.filter(i => !i.commodityCode).map(i => i.jfCode).join(', ')} ${its.some(i => !i.inJfpro) ? 'is not a JFPRO product' : 'has no commodity code in JFPRO'}.`);
        if (!first.knownSupplier) rowNotes.push(`${first.supplier || 'The supplier'} is not a JFPRO supplier, so the origin is not known.`);
        else if (!originCode) rowNotes.push(`JFPRO has no country it can read for ${first.supplier}.`);
        if (commodityCode && originCode) {
            const rate = duty.get(`${commodityCode}|${originCode}`);
            if (rate === undefined) rowNotes.push(`No duty in JFPRO for ${commodityCode} from ${originCode}.`);
            else dutyPercent = rate;
        }
        const offList = lists.length ? its.filter(i => !i.fromList).map(i => i.jfCode) : [];
        if (offList.length) rowNotes.push(`${offList.join(', ')} ${offList.length === 1 ? 'is' : 'are'} not on the supplier's packing list for this container: the cartons and weights are our own.`);
        if (its.some(i => i.fromList)) rowNotes.push('Cartons and weights are the supplier\'s packing list\'s.');
        return {
            supplier: first.supplier,
            invoiceNumber: [...new Set(its.flatMap(i => [...(invoiceOf.get(i.jfCode) || [])]))].join(' / '),
            description: [first.type, ...its.map(i => i.jfCode)].filter(Boolean).join(' '),
            origin: (its.find(i => i.originText) || first).originText,
            originCode,
            cartons: sum(its.map(i => i.cartons), 0),
            units: its.reduce((a, i) => a + i.units, 0),
            grossKg: sum(its.map(i => i.grossKg), 2),
            netKg: sum(its.map(i => i.netKg), 2),
            valueUsd: sum(its.map(i => i.valueUsd), 2),
            commodityCode,
            dutyPercent,
            jfCodes: its.map(i => i.jfCode),
            notes: rowNotes,
        };
    });
    if (lists.length) notes.push(`Packing list${lists.length === 1 ? '' : 's'} read for this container: ${lists.length}.`);
    else notes.push('No packing list has been read for this container: cartons and weights are our own carton specs.');
    notes.push('Values are units × our order price, in dollars. Freight is not compared: no clearance instructions hold the container\'s cost.');
    return { rows, notes };
}

/**
 * What we have on file for a container, as rows shaped like the clearance
 * instructions':
 *   kind 'sheet'  the newest clearance instructions sent from ShipLine
 *   kind 'live'   built now from the container's lines, JFPRO and the packing list
 *   kind 'none'   nothing: no lines on the container
 * → { kind, label, sheet, rows, containerCostUsd, notes }
 */
async function loadBasis(conn, shipment) {
    const readCountry = await countryReader(conn);
    const sheet = await sentSheet(conn, shipment.id);
    const sheetRows = sheet ? (parseJson(sheet.rows_json) || []) : [];
    if (sheet && Array.isArray(sheetRows) && sheetRows.length) {
        return {
            kind: 'sheet',
            label: `the clearance instructions v${sheet.version} ${sheet.status === 'sent' ? `sent ${sheet.sent_on || ''}`.trim() : 'handed to Front (not confirmed as sent)'} for container ${shipment.reference}`,
            sheet: { id: Number(sheet.id), version: Number(sheet.version), status: sheet.status, sentOn: sheet.sent_on || null },
            rows: sheetRows.map(r => ({ ...r, originCode: readCountry(r.origin), notes: [] })),
            containerCostUsd: figure(sheet.container_cost_usd),
            notes: [],
        };
    }
    const live = await liveRows(conn, shipment, readCountry);
    if (!live.rows.length) {
        return { kind: 'none', label: `nothing: container ${shipment.reference} has no lines on file`, sheet: null, rows: [], containerCostUsd: null, notes: live.notes };
    }
    return {
        kind: 'live',
        label: `container ${shipment.reference} as our records have it now (its lines, JFPRO's commodity codes and duty rates, the supplier's packing list). No clearance instructions were sent from ShipLine for it`,
        sheet: null,
        rows: live.rows,
        containerCostUsd: null,
        notes: live.notes,
    };
}

// ── A kept check (customs_entry_checks, src/services/customs-entry-routes.js) ──

/**
 * Runs the check a `processing` row asks for and writes what came of it on the
 * row. `fetchFile(row)` → the file's bytes (the route knows the two buckets).
 * Never throws: a failure is the row's (`failed`, with a code). The pool's one
 * connection is never held across the file read or a model call.
 * → { ok: true, verdict } | { failed: code } | { skipped }
 */
async function runCustomsEntryCheck(pool, { id, fetchFile, deadline = null } = {}) {
    const withConn = async (fn) => { const conn = await pool.getConnection(); try { return await fn(conn); } finally { conn.release(); } };
    const fail = async (code, message) => {
        try {
            await withConn(conn => conn.query(
                `UPDATE customs_entry_checks SET status = 'failed', error_code = ?, error_message = ?, analyzed_at = UTC_TIMESTAMP() WHERE id = ?`,
                [String(code).slice(0, 40), String(message || '').slice(0, 3900), id]
            ));
        } catch (e) {
            log.error('[customs-entry-check] could not record failure', { id, error: e.message });
        }
        return { failed: code };
    };

    let row;
    let shipment;
    let basis;
    try {
        ({ row, shipment, basis } = await withConn(async (conn) => {
            const [rows] = await conn.query('SELECT * FROM customs_entry_checks WHERE id = ?', [id]);
            if (!rows[0]) return {};
            const found = await findShipment(conn, { shipmentId: rows[0].shipment_id });
            return { row: rows[0], shipment: found, basis: found ? await loadBasis(conn, found) : null };
        }));
    } catch (e) {
        log.error('[customs-entry-check] could not load', { id, error: e.message });
        return fail('LOAD_FAILED', e.message);
    }
    if (!row) return { skipped: 'not_found' };
    if (row.status !== 'processing') return { skipped: row.status };
    if (!shipment) return fail('NO_SUCH_SHIPMENT', `Shipment ${row.shipment_id} not found.`);

    let bytes;
    try { bytes = await fetchFile(row); }
    catch (e) {
        log.warn('[customs-entry-check] file not read', { id, error: e.message });
        return fail(e.code === 'FILE_REFUSED' ? 'FILE_REFUSED' : 'FILE_UNAVAILABLE', e.message);
    }

    let read;
    let report;
    try {
        read = await readEntry({ bytes, contentType: row.content_type, deadline });
        report = await assess({ entry: read.entry, basis, container: shipment, readModel: read.modelUsed, deadline });
    } catch (e) {
        log.warn('[customs-entry-check] check failed', { id, code: e.code, error: e.message });
        return fail(e.code || 'MODEL_CALL_FAILED', e.message);
    }

    try {
        await withConn(conn => conn.query(
            `UPDATE customs_entry_checks
                SET status = 'succeeded', verdict = ?, mrn = ?, basis_kind = ?, clearance_id = ?, finding_count = ?,
                    model_read = ?, model_judge = ?, extract_json = ?, report_json = ?, error_code = NULL, error_message = NULL, analyzed_at = UTC_TIMESTAMP()
              WHERE id = ?`,
            [report.verdict, read.entry.mrn, basis.kind, basis.sheet ? basis.sheet.id : null, report.findings.length,
                report.models.read || null, report.models.judge || null, JSON.stringify(read.raw), JSON.stringify(report), id]
        ));
    } catch (e) {
        log.error('[customs-entry-check] could not write the report', { id, error: e.message });
        return fail('WRITE_FAILED', e.message);
    }
    return { ok: true, verdict: report.verdict };
}

module.exports = {
    READ_MODEL, JUDGE_MODEL,
    readEntry, judgeEntry, judgeInput, assess, findShipment, loadBasis, runCustomsEntryCheck,
};
