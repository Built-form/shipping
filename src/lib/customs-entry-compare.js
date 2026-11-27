'use strict';

// Customs-entry check (2026-11-27): the sums behind the report.
//
// A forwarder's customs entry declares a container's goods in the broker's own
// items: grouped and ordered as the broker chose, never as we listed them. So
// nothing here goes by position. Two questions are answered instead:
//
//   1. THE END RESULT (byCode). Per commodity code: do the cartons, weights,
//      value, origin and duty rate on the entry add up to ours? This pairs
//      none of their items with our rows, so no regrouping can upset it.
//   2. WHICH IS WHICH (groups). Their items and our rows are tied by the
//      product codes (JF0952 ...) an item's description prints. Items and rows
//      that share a code fall into one group, however many of each, and the
//      group's sums are compared. What no product code ties is tied by
//      commodity code, but never into a group that already balances.
//
// Pure: no database, no model, no clock. Reading the document and wording the
// verdict are the model's (src/services/customs-entry-check.js); every figure
// in the report is computed here, so the model never adds anything up.

// What counts as the same figure. Cartons and duty rates are exact.
const TOLERANCE = Object.freeze({
    kg: Object.freeze({ abs: 0.5, rel: 0.005 }),
    value: Object.freeze({ abs: 1, rel: 0.001 }),
});

// A check that could not be made on one of these stops the verdict being better
// than `incomplete`. Net weight and origin are often not on file: said, not held against it.
const ESSENTIAL = Object.freeze(['commodityCode', 'dutyRate', 'cartons', 'grossKg', 'value']);

const VERDICTS = Object.freeze(['match', 'same_result', 'incomplete', 'differences']);
const HEADLINE = Object.freeze({
    match: 'Matches what we have on file.',
    same_result: 'Grouped or described differently, same end result.',
    incomplete: 'No differences found, but not everything could be compared.',
    differences: 'Differs from what we have on file.',
    no_basis: 'Nothing on file to compare it with.',
    unreadable: 'Could not be read as a customs entry.',
});

const text = v => (typeof v === 'string' || typeof v === 'number' ? String(v).replace(/\s+/g, ' ').trim() : '');
const num = (v) => {
    if (v === null || v === undefined || v === '') return null;
    const n = typeof v === 'number' ? v : Number(String(v).replace(/,/g, '').trim());
    return Number.isFinite(n) ? n : null;
};
const int = (v) => { const n = num(v); return n !== null && Number.isInteger(n) ? n : null; };
const digitsOf = v => text(v).replace(/\D/g, '');
const alnum = v => text(v).toUpperCase().replace(/[^A-Z0-9]/g, '');
const round = (n, dp) => Math.round((n + Number.EPSILON) * 10 ** dp) / 10 ** dp;
const uniq = list => [...new Set(list)];
const strings = v => (Array.isArray(v) ? v.map(text).filter(Boolean) : []);
const country = (v) => { const s = text(v).toUpperCase(); return /^[A-Z]{2}$/.test(s) ? (s === 'UK' ? 'GB' : s) : null; };
/** A sum that is blank as soon as one part is: half a total is worse than none. */
const sumOf = (parts, dp) => (parts.length === 0 || parts.some(p => p === null) ? null : round(parts.reduce((a, p) => a + p, 0), dp));
const within = (a, b, t) => Math.abs(a - b) <= Math.max(t.abs, Math.abs(b) * t.rel) + 1e-9;
const code8 = v => String(v || '').slice(0, 8);

/** Our product codes in a piece of text: "PLASTERS JF0121 jf 0120 HW-0168" → [JF0121, JF0120, HW0168]. */
function productCodesIn(v) {
    const out = [];
    const re = /\b(JF|HW)[\s-]?(\d{3,5})(?!\d)/gi;
    let m;
    while ((m = re.exec(text(v))) !== null) out.push(`${m[1].toUpperCase()}${m[2]}`);
    return uniq(out);
}

/** Two commodity codes are one when the 8 digits agree and, where both print 10, so do the last two. */
function sameCode(a, b) {
    if (a.length < 8 || b.length < 8 || code8(a) !== code8(b)) return false;
    return a.length < 10 || b.length < 10 || a.slice(0, 10) === b.slice(0, 10);
}
const sameCodeSets = (a, b) => a.every(x => b.some(y => sameCode(x, y))) && b.every(y => a.some(x => sameCode(x, y)));
const sameNumberSets = (a, b) => a.every(x => b.some(y => Math.abs(x - y) < 0.001)) && b.every(y => a.some(x => Math.abs(x - y) < 0.001));
const sameSets = (a, b) => a.length === b.length && a.every(x => b.includes(x));

/** A code printed on the entry that is one keystroke from one of ours: one digit off, or two swapped. */
function oneKeystrokeApart(a, b) {
    if (a === b || a.length !== b.length) return false;
    const off = [];
    for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) off.push(i);
    if (off.length === 1) return true;
    return off.length === 2 && off[1] === off[0] + 1 && a[off[0]] === b[off[1]] && a[off[1]] === b[off[0]];
}

/**
 * The entry as the model read it, cleaned: figures that are figures, codes that
 * are digits, and the product codes each description prints. Nothing is
 * calculated: a cell the document did not print stays null.
 */
function normaliseEntry(parsed) {
    const p = parsed && typeof parsed === 'object' ? parsed : {};
    const items = (Array.isArray(p.items) ? p.items : []).map((raw, i) => {
        const it = raw && typeof raw === 'object' ? raw : {};
        const taxes = (Array.isArray(it.taxes) ? it.taxes : []).map(t => ({
            type: text(t && t.type).toUpperCase(), base: num(t && t.base), rate: num(t && t.rate), payable: num(t && t.payable), assessed: num(t && t.assessed),
        })).filter(t => t.type);
        const main = digitsOf(it.commodityCode);
        const duty = taxes.find(t => t.type === 'A00') || null;
        const vat = taxes.find(t => t.type === 'B00') || null;
        return {
            itemNo: int(it.itemNo) ?? i + 1,
            description: text(it.description),
            jfCodes: productCodesIn(it.description),
            packages: int(it.packages),
            packageKind: text(it.packageKind) || null,
            // 8 digits, or 10 where the two TARIC digits are printed beside them.
            commodityCode: main.length === 8 ? `${main}${digitsOf(it.taricCode).slice(0, 2)}` : main.slice(0, 10),
            origin: country(it.originCountry),
            procedure: text(it.procedureCode) || null,
            preference: text(it.preference) || null,
            grossKg: num(it.grossKg),
            netKg: num(it.netKg),
            price: num(it.itemPrice),
            currency: text(it.itemCurrency).toUpperCase() || null,
            statisticalValue: num(it.statisticalValue),
            invoiceNumbers: strings(it.invoiceNumbers),
            dutyRate: duty ? duty.rate : null,
            vatRate: vat ? vat.rate : null,
            taxes,
        };
    });
    return {
        documentType: text(p.documentType) || 'other',
        mrn: alnum(p.mrn) || null,
        lrn: text(p.lrn) || null,
        status: text(p.declarationStatus) || null,
        acceptedAt: text(p.acceptanceDate) || null,
        exporter: text(p.exporterName) || null,
        importer: text(p.importerName) || null,
        importerEori: alnum(p.importerEori) || null,
        declarant: text(p.declarantName) || null,
        vessel: text(p.vessel) || null,
        deliveryTerms: text(p.deliveryTerms) || null,
        containerNumbers: strings(p.containerNumbers),
        totalPackages: int(p.totalPackages),
        totalGrossKg: num(p.totalGrossKg),
        invoiceTotal: num(p.invoiceTotal),
        invoiceCurrency: text(p.invoiceCurrency).toUpperCase() || null,
        invoiceNumbers: strings(p.invoiceNumbers),
        exchangeRates: (Array.isArray(p.exchangeRates) ? p.exchangeRates : [])
            .map(r => ({ currency: text(r && r.currency).toUpperCase(), perGbp: num(r && r.perGbp) })).filter(r => r.currency && r.perGbp),
        additions: (Array.isArray(p.additions) ? p.additions : [])
            .map(a => ({ code: text(a && a.code).toUpperCase(), currency: text(a && a.currency).toUpperCase() || null, amount: num(a && a.amount) })).filter(a => a.amount !== null),
        fiscalReferences: strings(p.fiscalReferences),
        taxTotals: (Array.isArray(p.taxTotals) ? p.taxTotals : [])
            .map(t => ({ type: text(t && t.type).toUpperCase(), payable: num(t && t.payable), assessed: num(t && t.assessed) })).filter(t => t.type),
        items,
    };
}

/**
 * What we hold, cleaned: rows shaped as the clearance instructions' are
 * (src/lib/clearance-instructions.js), with `originCode` (ISO) where the
 * country could be read and `notes` on where a figure came from.
 */
function normaliseOurs(rows) {
    return (Array.isArray(rows) ? rows : []).map((raw, index) => {
        const r = raw && typeof raw === 'object' ? raw : {};
        const listed = strings(r.jfCodes).map(c => c.toUpperCase().replace(/_FQC$/, ''));
        return {
            index,
            supplier: text(r.supplier),
            description: text(r.description),
            jfCodes: uniq(listed.length ? listed : productCodesIn(r.description)),
            commodityCode: digitsOf(r.commodityCode).slice(0, 10),
            origin: text(r.origin) || null,
            originCode: country(r.originCode),
            cartons: int(r.cartons),
            // Units are not on a clearance sheet: held only when the rows were built from the container's lines.
            units: int(r.units),
            grossKg: num(r.grossKg),
            netKg: num(r.netKg),
            valueUsd: num(r.valueUsd),
            dutyPercent: num(r.dutyPercent),
            invoiceNumbers: uniq(text(r.invoiceNumber).split(' / ').map(s => s.trim()).filter(Boolean)),
            notes: strings(r.notes),
        };
    });
}

const usdPerGbp = (entry) => { const r = entry.exchangeRates.find(x => x.currency === 'USD'); return r ? r.perGbp : null; };

/**
 * Roughly what a sum of dollars is worth in duty and VAT, at the document's own
 * rate of exchange: null unless the rate is printed and one duty rate and one
 * VAT rate apply. VAT is charged on the value with its duty.
 */
function approxOf(usd, dutyRates, vatRates, entry) {
    const rate = usdPerGbp(entry);
    if (!rate || usd === null || dutyRates.length !== 1 || vatRates.length !== 1) return null;
    const valueGbp = usd / rate;
    const dutyGbp = valueGbp * dutyRates[0] / 100;
    return { valueGbp: round(valueGbp, 2), dutyGbp: round(dutyGbp, 2), vatGbp: round((valueGbp + dutyGbp) * vatRates[0] / 100, 2) };
}

/**
 * Some of their items beside some of our rows: each side's sums, what differs
 * (`diffs`) and what could not be compared (`notCompared`: a figure one side
 * does not have). One-sided (no items, or no rows): the sums only.
 */
function sides(its, rws, entry) {
    const currencies = uniq(its.map(i => i.currency).filter(Boolean));
    const e = {
        cartons: sumOf(its.map(i => i.packages), 0), grossKg: sumOf(its.map(i => i.grossKg), 3), netKg: sumOf(its.map(i => i.netKg), 3),
        value: sumOf(its.map(i => i.price), 2), currency: currencies.length === 1 ? currencies[0] : null,
        commodityCodes: uniq(its.map(i => i.commodityCode).filter(Boolean)), origins: uniq(its.map(i => i.origin).filter(Boolean)),
        dutyRates: uniq(its.map(i => i.dutyRate).filter(v => v !== null)), vatRates: uniq(its.map(i => i.vatRate).filter(v => v !== null)),
    };
    const o = {
        cartons: sumOf(rws.map(r => r.cartons), 0), grossKg: sumOf(rws.map(r => r.grossKg), 3), netKg: sumOf(rws.map(r => r.netKg), 3),
        value: sumOf(rws.map(r => r.valueUsd), 2), currency: 'USD',
        commodityCodes: uniq(rws.map(r => r.commodityCode).filter(Boolean)), origins: uniq(rws.map(r => r.originCode).filter(Boolean)),
        dutyRates: uniq(rws.map(r => r.dutyPercent).filter(v => v !== null)),
    };
    const diffs = [];
    const notCompared = [];
    if (!its.length || !rws.length) return { entry: e, ours: o, diffs, notCompared };

    const figure = (field, a, b, same) => {
        if (a === null || b === null) { notCompared.push(field); return; }
        if (!same(a, b)) diffs.push({ field, entry: a, ours: b, delta: round(a - b, 3) });
    };
    if (!e.commodityCodes.length || its.some(i => !i.commodityCode) || rws.some(r => !r.commodityCode)) notCompared.push('commodityCode');
    else if (!sameCodeSets(e.commodityCodes, o.commodityCodes)) diffs.push({ field: 'commodityCode', entry: e.commodityCodes, ours: o.commodityCodes });
    if (its.some(i => !i.origin) || rws.some(r => !r.originCode)) notCompared.push('origin');
    else if (!sameSets(e.origins, o.origins)) diffs.push({ field: 'origin', entry: e.origins, ours: o.origins });
    if (its.some(i => i.dutyRate === null) || rws.some(r => r.dutyPercent === null)) notCompared.push('dutyRate');
    else if (!sameNumberSets(e.dutyRates, o.dutyRates)) diffs.push({ field: 'dutyRate', entry: e.dutyRates, ours: o.dutyRates });
    figure('cartons', e.cartons, o.cartons, (a, b) => a === b);
    figure('grossKg', e.grossKg, o.grossKg, (a, b) => within(a, b, TOLERANCE.kg));
    figure('netKg', e.netKg, o.netKg, (a, b) => within(a, b, TOLERANCE.kg));
    if (e.currency !== 'USD') notCompared.push('value');
    else {
        figure('value', e.value, o.value, (a, b) => within(a, b, TOLERANCE.value));
        const d = diffs.find(x => x.field === 'value');
        const approx = d ? approxOf(d.delta, e.dutyRates, e.vatRates, entry) : null;
        if (approx) d.approx = approx;
    }
    return { entry: e, ours: o, diffs, notCompared };
}

const FIGURES = ['cartons', 'grossKg', 'netKg', 'value'];

/** Their items and our rows, tied into groups (see the head of this file). */
function groupsOf(items, rows, entry) {
    const I = i => `i${i}`;
    const R = r => `r${r}`;
    const parent = new Map([...items.map((_, i) => I(i)), ...rows.map((_, r) => R(r))].map(n => [n, n]));
    const find = (n) => { let x = n; while (parent.get(x) !== x) x = parent.get(x); return x; };
    const union = (a, b) => { const x = find(a); const y = find(b); if (x !== y) parent.set(y, x); };
    const components = () => {
        const by = new Map();
        const put = (node, side, idx) => { const k = find(node); if (!by.has(k)) by.set(k, { items: [], rows: [] }); by.get(k)[side].push(idx); };
        items.forEach((_, i) => put(I(i), 'items', i));
        rows.forEach((_, r) => put(R(r), 'rows', r));
        return [...by.values()];
    };

    // 1. By the product codes an item prints.
    const paired = new Set();
    items.forEach((it, i) => rows.forEach((row, r) => {
        if (it.jfCodes.some(c => row.jfCodes.includes(c))) { union(I(i), R(r)); paired.add(I(i)); paired.add(R(r)); }
    }));
    // A group whose figures already agree is left alone by what follows.
    const settled = new Set();
    for (const c of components()) {
        if (!c.items.length || !c.rows.length) continue;
        const s = sides(c.items.map(i => items[i]), c.rows.map(r => rows[r]), entry);
        if (!s.diffs.some(d => FIGURES.includes(d.field))) { c.items.forEach(i => settled.add(I(i))); c.rows.forEach(r => settled.add(R(r))); }
    }
    // 2. What no product code ties: by commodity code, to whatever of that code is not settled.
    const loose = new Set();
    items.forEach((it, i) => {
        if (paired.has(I(i)) || it.commodityCode.length < 8) return;
        rows.forEach((row, r) => { if (!settled.has(R(r)) && code8(row.commodityCode) === code8(it.commodityCode)) { union(I(i), R(r)); loose.add(I(i)); } });
    });
    rows.forEach((row, r) => {
        if (paired.has(R(r)) || row.commodityCode.length < 8) return;
        items.forEach((it, i) => { if (!settled.has(I(i)) && code8(it.commodityCode) === code8(row.commodityCode)) { union(I(i), R(r)); loose.add(R(r)); } });
    });

    return components().map((c) => {
        const its = c.items.map(i => items[i]);
        const rws = c.rows.map(r => rows[r]);
        const both = its.length > 0 && rws.length > 0;
        const printed = uniq(its.flatMap(i => i.jfCodes));
        const held = uniq(rws.flatMap(r => r.jfCodes));
        const onlyEntry = both ? printed.filter(code => !held.includes(code)) : [];
        // Nothing is "missing from the entry" when the entry prints no product codes at all.
        const onlyOurs = both && printed.length ? held.filter(code => !printed.includes(code)) : [];
        const measured = sides(its, rws, entry);
        // Goods of ours that no item covers: what they would have added, at the entry's own VAT rate.
        const notDeclared = !its.length
            ? approxOf(measured.ours.value, measured.ours.dutyRates, uniq(items.map(i => i.vatRate).filter(v => v !== null)), entry)
            : null;
        return {
            items: its.map(i => i.itemNo),
            rows: c.rows.map(r => r + 1),
            how: !both ? 'none' : (c.items.some(i => loose.has(I(i))) || c.rows.some(r => loose.has(R(r)))) ? 'commodity' : 'codes',
            ...measured,
            ...(notDeclared ? { approx: notDeclared } : {}),
            codesOnlyOnEntry: onlyEntry,
            codesOnlyOurs: onlyOurs,
            possibleTypos: onlyEntry.flatMap(p => onlyOurs.filter(o => oneKeystrokeApart(p, o)).map(o => ({ printed: p, ours: o }))),
        };
    }).sort((a, b) => (a.items[0] ?? Infinity) - (b.items[0] ?? Infinity) || (a.rows[0] ?? 0) - (b.rows[0] ?? 0));
}

/** The end result: each commodity code (its 8 digits), their side and ours. */
function byCodeOf(items, rows, entry) {
    const codes = uniq([...items.map(i => code8(i.commodityCode)), ...rows.map(r => code8(r.commodityCode))].filter(c => c.length === 8)).sort();
    return codes.map((code) => {
        const its = items.filter(i => code8(i.commodityCode) === code);
        const rws = rows.filter(r => code8(r.commodityCode) === code);
        return {
            code,
            items: its.map(i => i.itemNo),
            rows: rws.map(r => r.index + 1),
            onlyOn: !rws.length ? 'entry' : !its.length ? 'ours' : null,
            ...sides(its, rws, entry),
        };
    });
}

/** The entry against the container as a whole. `counts`: a failure is a difference; the others are for a person to weigh. */
function headerChecks(entry, rows, { container, containerCostUsd }) {
    const out = [];
    const add = (key, label, counts, theirs, ours, ok) => out.push({ key, label, counts, entry: theirs, ours, ok });

    const ourContainer = alnum(container && container.trackingRef);
    const theirContainers = entry.containerNumbers.map(alnum).filter(Boolean);
    add('container', 'Container number', true, entry.containerNumbers.join(', ') || null, (container && container.trackingRef) || null,
        !ourContainer || !theirContainers.length ? null : theirContainers.includes(ourContainer));

    const ourInvoices = uniq(rows.flatMap(r => r.invoiceNumbers));
    const theirInvoices = uniq([...entry.invoiceNumbers, ...entry.items.flatMap(i => i.invoiceNumbers)]);
    const cited = n => theirInvoices.some(t => alnum(t) === alnum(n));
    add('invoice', 'Commercial invoice', false, theirInvoices.join(', ') || null, ourInvoices.join(', ') || null,
        !ourInvoices.length || !theirInvoices.length ? null : ourInvoices.every(cited) && theirInvoices.every(t => ourInvoices.some(n => alnum(n) === alnum(t))));

    const ourCartons = sumOf(rows.map(r => r.cartons), 0);
    add('packages', 'Total packages', true, entry.totalPackages, ourCartons,
        entry.totalPackages === null || ourCartons === null ? null : entry.totalPackages === ourCartons);
    const ourGross = sumOf(rows.map(r => r.grossKg), 3);
    add('grossKg', 'Total gross weight (kg)', true, entry.totalGrossKg, ourGross,
        entry.totalGrossKg === null || ourGross === null ? null : within(entry.totalGrossKg, ourGross, TOLERANCE.kg));
    const ourValue = sumOf(rows.map(r => r.valueUsd), 2);
    add('value', `Invoice total (${entry.invoiceCurrency || '?'})`, true, entry.invoiceTotal, ourValue,
        entry.invoiceTotal === null || ourValue === null || entry.invoiceCurrency !== 'USD' ? null : within(entry.invoiceTotal, ourValue, TOLERANCE.value));

    // The freight we told them, against whatever the entry adds in dollars. Which addition is the
    // freight is the broker's coding, so a miss is for a person to read, not a difference by itself.
    const cost = num(containerCostUsd);
    const dollars = entry.additions.filter(a => a.currency === 'USD');
    add('freight', 'Freight (USD)', false, dollars.map(a => `${a.code} ${a.amount}`).join(', ') || null, cost,
        cost === null || !dollars.length ? null : dollars.some(a => Math.abs(a.amount - cost) <= 1));
    return out;
}

/** Does the entry add up to its own totals? A miss here is usually a misreading, so it is said apart. */
function selfChecks(entry) {
    const out = [];
    const add = (key, label, printed, summed, ok) => { if (printed !== null && summed !== null) out.push({ key, label, printed, summed, ok }); };
    const packages = sumOf(entry.items.map(i => i.packages), 0);
    add('packages', 'Packages', entry.totalPackages, packages, entry.totalPackages === packages);
    const gross = sumOf(entry.items.map(i => i.grossKg), 3);
    add('grossKg', 'Gross weight (kg)', entry.totalGrossKg, gross, gross !== null && entry.totalGrossKg !== null && Math.abs(entry.totalGrossKg - gross) <= 0.11);
    const price = sumOf(entry.items.map(i => i.price), 2);
    add('value', 'Invoice total', entry.invoiceTotal, price, price !== null && entry.invoiceTotal !== null && Math.abs(entry.invoiceTotal - price) <= 0.06);
    for (const t of entry.taxTotals) {
        if (t.assessed === null) continue;
        const lines = entry.items.map((i) => { const x = i.taxes.find(y => y.type === t.type); return x ? x.assessed : 0; });
        const assessed = sumOf(lines, 2);
        add(`tax:${t.type}`, `${t.type} assessed`, t.assessed, assessed, assessed !== null && Math.abs(t.assessed - assessed) <= 0.06);
    }
    return out;
}

const FIELD_WORDS = Object.freeze({
    commodityCode: 'commodity code', origin: 'origin', dutyRate: 'duty rate', cartons: 'cartons', grossKg: 'gross weight', netKg: 'net weight', value: 'value',
});
const fig = (v, dp = 2) => (v === null || v === undefined ? '—' : Number(v).toLocaleString('en-GB', { minimumFractionDigits: 0, maximumFractionDigits: dp }));
const money = v => (v === null || v === undefined ? '—' : Number(v).toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 }));
/** A value as a sentence or a table prints it; `field` 'value' is money, to the cent. */
const show = (v, field = null) => {
    if (Array.isArray(v)) return v.length ? v.join(', ') : 'none';
    if (v === null || v === undefined) return 'not given';
    if (typeof v === 'number') return field === 'value' ? money(v) : fig(v, 3);
    return String(v);
};

/**
 * The comparison. `entry` from normaliseEntry, `rows` from normaliseOurs.
 * → { verdict, reasons, notCompared, header, byCode, groups, selfCheck }
 * `verdict` is the figures' own: match | same_result | incomplete | differences.
 */
function compare({ entry, rows, container = null, containerCostUsd = null }) {
    const items = entry.items;
    const byCode = byCodeOf(items, rows, entry);
    const groups = groupsOf(items, rows, entry);
    const header = headerChecks(entry, rows, { container, containerCostUsd });
    const selfCheck = selfChecks(entry);

    const reasons = [];
    for (const c of byCode) {
        if (c.onlyOn === 'entry') reasons.push(`Commodity code ${c.code} is declared on the entry (item ${c.items.join(', ')}) and is not in what we hold.`);
        if (c.onlyOn === 'ours') reasons.push(`Commodity code ${c.code} is in what we hold (row ${c.rows.join(', ')}) and is not on the entry.`);
        for (const d of c.diffs) reasons.push(`Commodity code ${c.code}: ${FIELD_WORDS[d.field]} is ${show(d.entry, d.field)} on the entry, ${show(d.ours, d.field)} on file.`);
    }
    for (const g of groups) {
        if (g.how !== 'none') continue;
        if (g.items.length) reasons.push(`Item ${g.items.join(', ')} on the entry matches nothing we hold.`);
        else reasons.push(`Row ${g.rows.join(', ')} of ours is not on the entry.`);
    }
    for (const h of header) if (h.counts && h.ok === false) reasons.push(`${h.label}: ${show(h.entry, h.key)} on the entry, ${show(h.ours, h.key)} on file.`);

    const notCompared = uniq([
        ...byCode.flatMap(c => c.notCompared.map(f => `${FIELD_WORDS[f]} for commodity code ${c.code}`)),
        ...header.filter(h => h.ok === null).map(h => h.label.toLowerCase()),
    ]);
    const essentialMissing = byCode.some(c => c.notCompared.some(f => ESSENTIAL.includes(f)));
    const regrouped = groups.some(g => g.how !== 'codes' || g.items.length !== 1 || g.rows.length !== 1
        || g.diffs.length > 0 || g.codesOnlyOnEntry.length > 0 || g.codesOnlyOurs.length > 0);
    const verdict = reasons.length ? 'differences' : essentialMissing ? 'incomplete' : regrouped ? 'same_result' : 'match';
    return { verdict, reasons: uniq(reasons), notCompared, header, byCode, groups, selfCheck };
}

/** A cell of the model's: its text, or null when it wrote a blank as a word. */
const said = (v) => { const s = text(v); return s && !/^(null|n\/a|-|—)$/i.test(s) ? s : null; };

/** The more serious of two verdicts; one that is not a verdict is ignored. */
function worse(a, b) {
    const ia = VERDICTS.indexOf(a);
    const ib = VERDICTS.indexOf(b);
    if (ia < 0) return ib < 0 ? null : b;
    return ib > ia ? b : a;
}

/**
 * The report on one document: what is kept and shown. `judgement` is the
 * model's (null when it could not be had: the figures then stand alone).
 */
function buildReport({ entry, basis, container, comparison, judgement, models = {} }) {
    const rate = usdPerGbp(entry);
    const tax = type => entry.taxTotals.find(t => t.type === type) || null;
    const document = {
        type: entry.documentType, mrn: entry.mrn, lrn: entry.lrn, status: entry.status, acceptedAt: entry.acceptedAt,
        importer: entry.importer, importerEori: entry.importerEori, declarant: entry.declarant, exporter: entry.exporter,
        containers: entry.containerNumbers, vessel: entry.vessel, deliveryTerms: entry.deliveryTerms,
        totalPackages: entry.totalPackages, totalGrossKg: entry.totalGrossKg, invoiceTotal: entry.invoiceTotal, invoiceCurrency: entry.invoiceCurrency,
        additions: entry.additions, usdPerGbp: rate, fiscalReferences: entry.fiscalReferences,
        duty: tax('A00'), vat: tax('B00'), taxTotals: entry.taxTotals,
    };
    const base = {
        basis: { kind: basis.kind, label: basis.label, notes: basis.notes || [], sheet: basis.sheet || null },
        container: container || null, document, items: entry.items, rows: basis.rows || [], models,
    };
    if (!comparison) {
        const verdict = entry.items.length ? 'no_basis' : 'unreadable';
        return { verdict, verdicts: { numbers: null, ai: null }, headline: HEADLINE[verdict], summary: null, findings: [], ...base };
    }
    const ai = judgement && VERDICTS.includes(judgement.verdict) ? judgement.verdict : null;
    const verdict = worse(comparison.verdict, ai) || comparison.verdict;
    const noted = new Map((judgement && Array.isArray(judgement.items) ? judgement.items : []).map(i => [Number(i.itemNo), i]));
    return {
        verdict,
        verdicts: { numbers: comparison.verdict, ai },
        headline: HEADLINE[verdict],
        summary: (judgement && text(judgement.summary)) || null,
        findings: (judgement && Array.isArray(judgement.findings) ? judgement.findings : []).map(f => ({
            severity: ['critical', 'warning', 'info'].includes(f.severity) ? f.severity : 'info',
            field: said(f.field), itemNo: int(f.itemNo), entry: said(f.entry), ours: said(f.ours),
            explanation: text(f.explanation), effect: said(f.effect),
        })).filter(f => f.explanation),
        reasons: comparison.reasons,
        notCompared: comparison.notCompared,
        checks: comparison.header,
        byCode: comparison.byCode,
        groups: comparison.groups.map(g => ({
            ...g,
            ai: g.items.map(n => noted.get(n)).filter(Boolean).map(n => ({ itemNo: Number(n.itemNo), status: text(n.status) || null, note: text(n.note) || null })),
        })),
        selfCheck: comparison.selfCheck,
        ...base,
    };
}

// ── The report as text (the command line; an email, should one be wanted) ──

const pad = (s, n) => String(s).padEnd(n);
const mark = ok => (ok === true ? 'ok  ' : ok === false ? 'DIFF' : '?   ');

function reportText(report) {
    const d = report.document;
    const L = [];
    L.push(`VERDICT: ${report.verdict.toUpperCase()} — ${report.headline}`);
    if (report.verdicts.numbers || report.verdicts.ai) L.push(`  (the AI says: ${report.verdicts.ai || 'no judgement'}; the figures say: ${report.verdicts.numbers || 'nothing'})`);
    L.push('');
    L.push(`Document:  ${d.type}${d.mrn ? `, MRN ${d.mrn}` : ''}${d.status ? `, ${d.status}` : ''}${d.acceptedAt ? ` ${d.acceptedAt}` : ''}`);
    L.push(`           importer ${d.importer || '—'}${d.importerEori ? ` (${d.importerEori})` : ''}; declarant ${d.declarant || '—'}`);
    L.push(`           container ${d.containers.join(', ') || '—'}; ${fig(d.totalPackages, 0)} packages, ${fig(d.totalGrossKg, 3)} kg; invoice ${money(d.invoiceTotal)} ${d.invoiceCurrency || ''}`);
    if (d.duty || d.vat) L.push(`           duty assessed £${money(d.duty && d.duty.assessed)}, VAT assessed £${money(d.vat && d.vat.assessed)} (payable now: duty £${money(d.duty && d.duty.payable)}, VAT £${money(d.vat && d.vat.payable)})`);
    L.push(`Against:   ${report.basis.label}`);
    for (const n of report.basis.notes) L.push(`           - ${n}`);
    if (report.summary) { L.push(''); L.push(report.summary); }

    if (report.findings.length) {
        L.push('');
        L.push('FINDINGS');
        for (const f of report.findings) {
            L.push(`  [${f.severity.toUpperCase()}]${f.itemNo ? ` item ${f.itemNo}` : ''}${f.field ? ` · ${f.field}` : ''}: ${f.explanation}`);
            if (f.entry || f.ours) L.push(`      entry: ${f.entry || '—'}   |   ours: ${f.ours || '—'}`);
            if (f.effect) L.push(`      effect: ${f.effect}`);
        }
    }
    if (report.byCode) {
        L.push('');
        L.push('END RESULT BY COMMODITY CODE (entry / ours)');
        for (const c of report.byCode) {
            const state = c.onlyOn ? `ONLY ON ${c.onlyOn === 'entry' ? 'THE ENTRY' : 'OUR SIDE'}` : c.diffs.length ? 'DIFF' : c.notCompared.length ? 'ok, part not compared' : 'ok';
            L.push(`  ${pad(c.code, 10)} ${pad(state, 22)} cartons ${fig(c.entry.cartons, 0)} / ${fig(c.ours.cartons, 0)}   gross ${fig(c.entry.grossKg, 3)} / ${fig(c.ours.grossKg, 3)}   net ${fig(c.entry.netKg, 3)} / ${fig(c.ours.netKg, 3)}   value ${money(c.entry.value)} / ${money(c.ours.value)}   duty % ${show(c.entry.dutyRates)} / ${show(c.ours.dutyRates)}`);
        }
        L.push('');
        L.push('ITEM BY ITEM');
        const rowText = n => { const r = report.rows[n - 1]; return r ? `${r.description || r.supplier}` : `row ${n}`; };
        for (const g of report.groups) {
            const left = g.items.length ? `item ${g.items.join('+')}` : '(not on the entry)';
            const right = g.rows.length ? g.rows.map(rowText).join(' + ') : '(nothing of ours)';
            const state = g.how === 'none' ? 'UNMATCHED' : g.diffs.length ? 'DIFF' : 'ok';
            L.push(`  ${pad(left, 20)} ${pad(state, 10)} ${right}${g.how === 'commodity' ? '   [tied by commodity code only]' : ''}`);
            const worth = a => `about £${money(a.valueGbp)} of value: £${money(a.dutyGbp)} duty, £${money(a.vatGbp)} VAT`;
            for (const x of g.diffs) {
                const delta = x.delta === undefined ? '' : ` (${x.delta > 0 ? '+' : ''}${show(x.delta, x.field)})`;
                L.push(`      ${FIELD_WORDS[x.field]}: ${show(x.entry, x.field)} on the entry, ${show(x.ours, x.field)} ours${delta}${x.approx ? `  (${worth(x.approx)})` : ''}`);
            }
            if (g.how === 'none' && g.rows.length) {
                L.push(`      ${fig(g.ours.cartons, 0)} cartons, ${fig(g.ours.grossKg, 3)} kg gross, $${money(g.ours.value)}${g.approx ? `  (not declared: ${worth(g.approx)})` : ''}`);
            }
            if (g.codesOnlyOnEntry.length) L.push(`      printed on the entry, not ours: ${g.codesOnlyOnEntry.join(', ')}`);
            if (g.codesOnlyOurs.length) L.push(`      ours, not printed on the entry: ${g.codesOnlyOurs.join(', ')}`);
            for (const t of g.possibleTypos) L.push(`      ${t.printed} is one keystroke from our ${t.ours}`);
            for (const a of g.ai) if (a.note) L.push(`      AI: ${a.note}`);
        }
        L.push('');
        L.push('WHOLE CONTAINER');
        for (const h of report.checks) L.push(`  ${mark(h.ok)} ${pad(h.label, 26)} entry ${show(h.entry, h.key)}   ours ${show(h.ours, h.key)}`);
        if (report.notCompared.length) { L.push(''); L.push(`NOT COMPARED: ${report.notCompared.join('; ')}.`); }
        const off = report.selfCheck.filter(s => !s.ok);
        if (off.length) {
            L.push('');
            L.push('THE ENTRY DOES NOT ADD UP TO ITS OWN TOTALS (a misreading is likely: look at the document)');
            for (const s of off) L.push(`  ${s.label}: prints ${fig(s.printed, 3)}, its items come to ${fig(s.summed, 3)}`);
        }
    }
    return L.join('\n');
}

module.exports = {
    TOLERANCE, ESSENTIAL, VERDICTS, HEADLINE,
    productCodesIn, sameCode, oneKeystrokeApart, normaliseEntry, normaliseOurs, compare, worse, buildReport, reportText,
};
