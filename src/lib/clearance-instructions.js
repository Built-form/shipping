'use strict';

// Clearance instructions (2026-11-24; docs/clearance-instructions.md): the
// sheet our team send to the forwarder that handles a container's customs. One
// row per product type of a supplier: the commercial invoice, the goods, their
// origin, cartons, weights, volume, commodity code, duty and value; and the
// container's freight cost shared between the rows by volume.
//
// ShipLine fills the rows from what it holds and staff correct them; this is
// where they are checked and where the file is written, so there is one writer
// and one check that decides. Pure: no database, no clock.
//
// The file is the sheet that was typed by hand until now, laid out to the
// character (tools/test-clearance-instructions-lib.js holds one of them): a
// title row, the heading row with its own words (its misspelling and its stray
// space included: the forwarder knows the sheet by them), the rows, then the
// total volume, the cost per CBM and the total cost. CRLF, no byte order mark,
// as the spreadsheet exported it.

const MAX_ROWS = 200;
const MAX_TEXT = 200;
const COMPANIES = Object.freeze(['JFA', 'HW']);

const HEADING = Object.freeze([
    'Supplier', 'Commerical Invoice Number', 'JFA / HW', ' Description of goods [6/8]', 'Country of Origin', 'Unit Count', 'Unit Type',
    'Total NW (kg)', 'Total GW (kg)', 'Total Carton CBM', 'Commodity Code', 'Duty Percentage', 'Container Cost USD', 'Total Container CBM',
    'Cost of Shipping', 'Value of Goods USD', '',
]);
const COLUMNS = HEADING.length;

const round = (n, dp) => Math.round((n + Number.EPSILON) * 10 ** dp) / 10 ** dp;
const positive = n => typeof n === 'number' && Number.isFinite(n) && n > 0;

/** A cell of text: one line, trimmed, no longer than a cell should be. Spaces inside it are the writer's own. */
const text = v => (typeof v === 'string' || typeof v === 'number' ? String(v).replace(/[\r\n\t]+/g, ' ').trim().slice(0, MAX_TEXT) : '');
/** A figure, or null when it is not one: a number, or text that is exactly a number. */
const figure = (v) => {
    if (typeof v === 'number') return Number.isFinite(v) ? v : null;
    if (typeof v !== 'string' || !/^\s*-?\d+(\.\d+)?\s*$/.test(v)) return null;
    return Number(v);
};
const above = (v, dp) => { const n = figure(v); return n !== null && n > 0 ? round(n, dp) : null; };

const CODE_RE = /^\d{8}(\d{2})?$/;

/**
 * What came in, cleaned, and what stops it being sent. A cell that is not what
 * it should be is cleaned to a blank and named: the file never carries a figure
 * nobody typed. `problems`: [{ row, field, message }], row = its place in `rows`
 * (null for the whole file). None = it can go.
 */
function check(input) {
    const problems = [];
    const list = input && Array.isArray(input.rows) ? input.rows : null;
    if (!list || list.length === 0) problems.push({ row: null, field: 'rows', message: 'There is nothing in this container to declare.' });
    else if (list.length > MAX_ROWS) {
        problems.push({ row: null, field: 'rows', message: `No more than ${MAX_ROWS} rows.` });
        return { rows: [], containerCostUsd: above(input.containerCostUsd, 2), problems };
    }
    const containerCostUsd = above(input ? input.containerCostUsd : null, 2);
    if (containerCostUsd === null) problems.push({ row: null, field: 'containerCostUsd', message: 'The container’s freight cost is not filled in.' });

    const rows = (list || []).map((raw, i) => {
        const r = raw && typeof raw === 'object' ? raw : {};
        const add = (field, message) => problems.push({ row: i, field, message });
        const cartons = (() => { const n = figure(r.cartons); return n !== null && n > 0 && Number.isInteger(n) ? n : null; })();
        const duty = (() => { const n = figure(r.dutyPercent); return n !== null && n >= 0 && n <= 100 ? round(n, 2) : null; })();
        const out = {
            supplier: text(r.supplier), invoiceNumber: text(r.invoiceNumber), company: COMPANIES.includes(r.company) ? r.company : '',
            description: text(r.description), origin: text(r.origin), cartons, unitType: text(r.unitType) || 'Carton',
            netKg: above(r.netKg, 2), grossKg: above(r.grossKg, 2), cbm: above(r.cbm, 3),
            commodityCode: text(r.commodityCode).replace(/\s+/g, ''), dutyPercent: duty, valueUsd: above(r.valueUsd, 2),
            // The products in the row: kept with what was sent, never on the file.
            jfCodes: (Array.isArray(r.jfCodes) ? r.jfCodes : []).filter(c => typeof c === 'string' && c.trim()).map(c => c.trim().toUpperCase().slice(0, 40)).slice(0, 50),
        };
        if (!out.supplier) add('supplier', 'No supplier.');
        if (!out.invoiceNumber) add('invoiceNumber', 'No commercial invoice number.');
        if (!out.company) add('company', 'JFA or HW is not chosen.');
        if (!out.description) add('description', 'No description of the goods.');
        if (!out.origin) add('origin', 'No country of origin.');
        if (out.cartons === null) add('cartons', 'No carton count.');
        if (out.netKg === null) add('netKg', 'No net weight.');
        else if (out.grossKg !== null && out.netKg > out.grossKg) add('netKg', 'Net weight is above gross weight.');
        if (out.grossKg === null) add('grossKg', 'No gross weight.');
        if (out.cbm === null) add('cbm', 'No volume.');
        if (!CODE_RE.test(out.commodityCode)) add('commodityCode', 'The commodity code is not 8 or 10 digits.');
        if (out.dutyPercent === null) add('dutyPercent', 'No duty percentage (0 is a value).');
        if (out.valueUsd === null) add('valueUsd', 'No value of goods.');
        return out;
    });
    return { rows, containerCostUsd, problems };
}

/**
 * The container's cost shared between the rows by volume, as the sheet has
 * always done it: cost ÷ total CBM (to two places) is a rate to four places; a
 * row's share is the rate times its CBM, to the dollar. `shipping` is per row,
 * null where there is nothing to share or the row has no volume.
 */
function totalsOf(rows, containerCostUsd) {
    const totalCbm = round(rows.reduce((a, r) => a + (positive(r.cbm) ? r.cbm : 0), 0), 2);
    if (!positive(containerCostUsd) || !(totalCbm > 0)) return { totalCbm, costPerCbm: null, shipping: rows.map(() => null) };
    const costPerCbm = round(containerCostUsd / totalCbm, 4);
    return { totalCbm, costPerCbm, shipping: rows.map(r => (positive(r.cbm) ? Math.round(costPerCbm * r.cbm) : null)) };
}

const cell = (v) => {
    const s = v === null || v === undefined ? '' : String(v).replace(/[\r\n]+/g, ' ');
    return /[",]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
};
const line = cells => Array.from({ length: COLUMNS }, (_, i) => cell(cells[i])).join(',');
const plain = (n, dp) => (n === null || n === undefined ? '' : String(round(n, dp)));
const fixed = (n, dp) => (n === null || n === undefined ? '' : n.toFixed(dp));
/** A sum of money as the sheet's cost column has it: whole dollars bare, else to the cent. */
const dollars = n => (n === null || n === undefined ? '' : Number.isInteger(n) ? String(n) : n.toFixed(2));

/** The file. `rows` as check() cleaned them; blanks are empty cells. */
function toCsv({ title, rows, containerCostUsd }) {
    const cost = positive(containerCostUsd) ? containerCostUsd : null;
    const t = totalsOf(rows, cost);
    const foot = (label, value) => { const cells = []; cells[13] = label; cells[14] = value; return line(cells); };
    const lines = [
        line([`${title} - Clearance Instructions`]),
        line(HEADING),
        ...rows.map((r, i) => line([
            r.supplier, r.invoiceNumber, r.company, r.description, r.origin, r.cartons, r.unitType,
            plain(r.netKg, 2), plain(r.grossKg, 2), fixed(r.cbm, 3), r.commodityCode,
            r.dutyPercent === null || r.dutyPercent === undefined ? '' : `${r.dutyPercent.toFixed(2)}%`,
            dollars(cost), fixed(t.totalCbm, 2), t.shipping[i], fixed(r.valueUsd, 2), '',
        ])),
        foot('TOTAL CBM', fixed(t.totalCbm, 2)),
        foot('Cost/CBM', t.costPerCbm === null ? '' : String(t.costPerCbm)),
        foot('TOTAL', cost === null ? '' : `USD ${cost.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`),
    ];
    return `${lines.join('\r\n')}\r\n`;
}

/** What the container is called on the file: the carrier's number when it has one, else our own. */
function titleOf({ trackingRef, reference } = {}) {
    return text(trackingRef).toUpperCase() || text(reference) || 'Container';
}

/** The file's name, as the hand-made ones were named; nothing a file name cannot hold. */
const filenameOf = title => `Clearance Instructions - ${String(title).replace(/[\\/:*?"<>|]+/g, '-').trim()}.csv`;

/** The duty each commodity code was sent with, [code, percent], to be offered next time: the last row to say it. */
function dutyRatesOf(rows) {
    const out = new Map();
    for (const r of rows) {
        if (!CODE_RE.test(r.commodityCode || '') || r.dutyPercent === null || r.dutyPercent === undefined) continue;
        out.set(r.commodityCode, r.dutyPercent);
    }
    return [...out.entries()];
}

module.exports = { MAX_ROWS, COMPANIES, HEADING, check, totalsOf, toCsv, titleOf, filenameOf, dutyRatesOf };
