'use strict';

// Unit tests for the supplier-portal packing-list matcher (src/lib/pl-match.js)
// and its candidate loader (packing-list-check.js loadSupplierCandidates,
// against a fake connection). The comparisons come from the real
// compareDocuments, so the ratios are what production would see. No
// database, no Gemini.
//   node --test tools/test-pl-match-lib.js

const test = require('node:test');
const assert = require('node:assert/strict');
const PM = require('../src/lib/pl-match');
const P = require('../src/services/packing-list-check');
const PL = require('../src/services/pl-results');

// ── Fixtures ───────────────────────────────────────────────────────────────
const order = (o) => ({
    id: 1, jf_code: 'JF0001', product_name: 'Thing', po_number: 'PO_00001J', lot_number: '0001001', quantity: 100,
    units_per_carton: 50, carton_weight: null, carton_height: null, carton_width: null, carton_depth: null,
    mfg_date: null, exp_date: null, status: 'CONSOLIDATED', supplier: 'Acme', container_number: null,
    order_quantity: 100, po_supplier: 'Acme', allocation_id: null, supplier_group_key: 's:1', supplier_group_id: 1, ...o,
});
const row = (r) => ({
    jfCode: 'JF0001', poNumber: 'PO_00001J', lotNumber: '0001001', mfgDate: null, expDate: null, description: 'Thing',
    cartons: 2, quantity: 100, unit: 'BOX', grossWeightPerCarton: null, netWeightPerCarton: null, cartonDimensionsCm: null, ...r,
});
const extracted = (lines, over = {}) => ({
    invoiceNumber: 'INV-1', documentDate: null, supplierName: 'Acme', containerNumbers: [],
    totalCartons: null, totalQuantity: null, totalGrossWeightKg: null, totalNetWeightKg: null, totalCbm: null, lines, ...over,
});
const booked = ref => ({ kind: 'booked', containerNumber: ref, label: ref });
const draft = (id, name) => ({ kind: 'draft', draftId: id, draftName: name, containerNumber: '328', label: name });

// A candidate for scoreCandidates: the container's lines compared with the document.
const cand = (container, lines, doc, extra = {}) => ({
    container, boxNumbers: [], comparison: P.compareDocuments(lines, [{ packingListId: null, filename: 'pl.pdf', extracted: doc }]), ...extra,
});
const score = (candidates, over = {}) => PM.scoreCandidates({ candidates, printedNumbers: [], ...over });

const BOX = 'MRKU4645188';       // check digit 8 — valid
const BOX2 = 'CSQU3054383';      // the ISO 6346 worked example

// ── ISO 6346 ───────────────────────────────────────────────────────────────
test('iso6346Valid: letter values skip multiples of 11, weights 2^i, mod 11 with 10 → 0', () => {
    assert.equal(PM.LETTER_VALUES.A, 10);
    assert.equal(PM.LETTER_VALUES.K, 21);
    assert.equal(PM.LETTER_VALUES.L, 23, '22 is skipped');
    assert.equal(PM.LETTER_VALUES.V, 34, '33 is skipped');
    assert.equal(PM.LETTER_VALUES.Z, 38);
    assert.equal(PM.iso6346Valid(BOX), true);
    assert.equal(PM.iso6346Valid(BOX2), true);
    assert.equal(PM.iso6346Valid('mrku 464518-8'), true, 'case and separators are ignored');
    assert.equal(PM.iso6346Valid('MRKU4645189'), false, 'a misread last digit');
    assert.equal(PM.iso6346Valid('MRKU4645180'), false);
    assert.equal(PM.iso6346Valid('MRKU464518'), false, 'shape');
    assert.equal(PM.iso6346Valid('324'), false);
    assert.equal(PM.iso6346Valid(null), false);
    // A sum that is a multiple of 11 wants a 0: find one by construction.
    for (let d = 0; d <= 9; d++) {
        const n = `MRKU000000${d}`;
        if (PM.iso6346CheckDigit(n.slice(0, 10)) === 0) { assert.equal(PM.iso6346Valid('MRKU0000000'), true); break; }
    }
});

test('nearBox: edit distance exactly 1', () => {
    assert.equal(PM.nearBox(BOX, 'MRKU4645189'), true, 'substitution');
    assert.equal(PM.nearBox(BOX, 'MRKU464518'), true, 'deletion');
    assert.equal(PM.nearBox(BOX, 'MRKU46451888'), true, 'insertion');
    assert.equal(PM.nearBox(BOX, BOX), false, 'equal is not near');
    assert.equal(PM.nearBox(BOX, 'MRKU4645100'), false, 'two edits');
    assert.equal(PM.nearBox(BOX, BOX2), false);
});

test('containerKey and evidenceFromExtraction', () => {
    assert.equal(PM.containerKey(booked('324')), 'booked:324');
    assert.equal(PM.containerKey(draft(129, 'DRAFT-SEA-260917-173825 - 328')), 'draft:129');
    assert.equal(PM.containerKey({ kind: 'draft', draftId: null, draftName: 'Sunmed box' }), 'draft:Sunmed box');
    const ev = PM.evidenceFromExtraction(extracted([
        row({ jfCode: 'JF0197_FQC', poNumber: 'PO_00001J' }), row({ jfCode: 'jf 0197', poNumber: 'po_00002j' }), row({ jfCode: null, poNumber: null }),
    ], { containerNumbers: [BOX, ' 324 ', null, ''] }));
    assert.deepEqual(ev.productCodes, ['JF0197'], 'base code, FQC folded, spaces dropped');
    assert.deepEqual(ev.poNumbers, ['PO00001J', 'PO00002J']);
    assert.deepEqual(ev.printedNumbers, [BOX, '324'], 'a null entry is not the string "null"');
});

// ── Ratios and the score ───────────────────────────────────────────────────
test('ratiosOf: M, X, P, E and the method weights', () => {
    const lines = [order({ id: 1 }), order({ id: 2, jf_code: 'JF0002', po_number: 'PO_00002J', quantity: 200 })];
    // Row 1 exact (jf_po); row 2 matched on code only with the wrong PO and a short quantity.
    const doc = extracted([row(), row({ jfCode: 'JF0002', poNumber: 'PO_00009J', quantity: 150, cartons: 3 })]);
    const r = PM.ratiosOf(P.compareDocuments(lines, [{ extracted: doc }]));
    // M = 1·100 + 0.85·min(150,200) = 227.5 ; P = 250 ; E = 300 ; X = 100
    assert.equal(r.packedUnits, 250);
    assert.equal(r.expectedUnits, 300);
    assert.equal(r.coverage, 0.91);
    assert.equal(r.exact, 0.4);
    assert.equal(r.recall, 0.7583);
    assert.equal(r.lineHit, 1);
    assert.equal(r.poShare, 0.4, 'only the jf_po row counts');
    assert.equal(r.matched, 2);
    assert.equal(r.unexpected, 0);
});

test('scoreOf: the formula at each weight', () => {
    const zero = { coverage: 0, exact: 0, recall: 0, lineHit: 0, poShare: 0, boxHit: false, refHit: false, replacesHit: false };
    assert.equal(PM.scoreOf(zero), 0);
    assert.equal(PM.scoreOf({ ...zero, coverage: 1 }), PM.SCORE_WEIGHTS.coverage);
    assert.equal(PM.scoreOf({ ...zero, exact: 1 }), PM.SCORE_WEIGHTS.exact);
    assert.equal(PM.scoreOf({ ...zero, recall: 1 }), PM.SCORE_WEIGHTS.recall);
    assert.equal(PM.scoreOf({ ...zero, lineHit: 1 }), PM.SCORE_WEIGHTS.lineHit);
    assert.equal(PM.scoreOf({ ...zero, poShare: 1 }), PM.SCORE_WEIGHTS.poShare);
    assert.equal(PM.scoreOf({ ...zero, boxHit: true }), PM.SCORE_WEIGHTS.boxHit);
    assert.equal(PM.scoreOf({ ...zero, refHit: true }), PM.SCORE_WEIGHTS.refHit);
    assert.equal(PM.scoreOf({ ...zero, replacesHit: true }), PM.SCORE_WEIGHTS.replacesHit);
    assert.equal(PM.scoreOf({ coverage: 1, exact: 1, recall: 1, lineHit: 1, poShare: 1, boxHit: true, refHit: true, replacesHit: true }), 1.6);
});

// ── Existing behaviour ─────────────────────────────────────────────────────
test('the exact container wins', () => {
    const other = [order({ id: 9, jf_code: 'JF0777', po_number: 'PO_00777J' })];
    // With POs printed, the PO rule settles it first.
    const withPo = score([cand(booked('324'), [order()], extracted([row()])), cand(booked('325'), other, extracted([row()]))]);
    assert.equal(withPo.confidence, 'confident');
    assert.equal(withPo.reason, 'po_numbers');
    assert.equal(withPo.winner.key, 'booked:324');
    assert.equal(withPo.winner.score, 1);
    assert.equal(withPo.candidates[1].score, 0);
    assert.deepEqual(withPo.conflicts, []);
    assert.equal(withPo.logFields.rule, '3');
    assert.equal(withPo.logFields.candidateCount, 2);
    // Without (matched on product code alone), the lines do (rule 4).
    const noPo = extracted([row({ poNumber: null })]);
    const byLines = score([cand(booked('324'), [order()], noPo), cand(booked('325'), other, noPo)]);
    assert.equal(byLines.confidence, 'confident');
    assert.equal(byLines.reason, 'lines');
    assert.equal(byLines.winner.key, 'booked:324');
    assert.equal(byLines.winner.coverage, PM.METHOD_WEIGHT.jf_only);
    assert.equal(byLines.winner.poShare, 0);
    assert.equal(byLines.logFields.rule, '4');
});

test('two packing lists in one container are still confident: coverage leads, recall does not', () => {
    const lines = [order({ id: 1 }), order({ id: 2, jf_code: 'JF0002', po_number: 'PO_00002J', quantity: 200 })];
    const doc = extracted([row()]);   // this PL is only the first line; the other PL covers the rest
    const two = score([cand(booked('324'), lines, doc), cand(booked('325'), [order({ id: 9, jf_code: 'JF0777', po_number: 'PO_00777J' })], doc)]);
    assert.equal(two.confidence, 'confident');
    assert.equal(two.winner.coverage, 1);
    assert.equal(two.winner.recall, 0.3333);
    const one = score([cand(booked('324'), lines, doc)]);
    assert.equal(one.confidence, 'confident');
    assert.equal(one.reason, 'single');
    assert.equal(one.winner.recall, 0.3333);
});

test('a PO line split across two drafts: exact decides, or it falls to unsure', () => {
    const X = draft(1, 'DRAFT-SEA-260917-173825 - 328');
    const Y = draft(2, 'DRAFT-SEA-260918-100000 - 329');
    const lx = [order({ quantity: 300, order_quantity: 500, allocation_id: 11 })];
    const ly = [order({ quantity: 200, order_quantity: 500, allocation_id: 12 })];
    const packed300 = extracted([row({ quantity: 300, cartons: 6 })]);
    const decided = score([cand(X, lx, packed300), cand(Y, ly, packed300)]);
    assert.equal(decided.confidence, 'confident');
    assert.equal(decided.reason, 'lines');
    assert.equal(decided.winner.key, 'draft:1');
    assert.equal(decided.winner.exact, 1);
    assert.equal(decided.candidates[1].exact, 0);
    const packed250 = extracted([row({ quantity: 250, cartons: 5 })]);
    const unsure = score([cand(X, lx, packed250), cand(Y, ly, packed250)]);
    assert.equal(unsure.confidence, 'unsure');
    assert.equal(unsure.reason, 'lines');
    assert.equal(unsure.logFields.rule, '6');
    assert.ok(unsure.candidates[0].score - unsure.candidates[1].score < PM.THRESHOLDS.linesMargin);
});

test('quantity-only matches are down-weighted', () => {
    const lines = [order()];
    const withCode = score([cand(booked('324'), lines, extracted([row()]))]);
    assert.equal(withCode.confidence, 'confident');
    const noCode = score([cand(booked('324'), lines, extracted([row({ jfCode: null, poNumber: null })]))]);
    assert.equal(noCode.winner.coverage, PM.METHOD_WEIGHT.no_code);
    assert.equal(noCode.winner.poShare, 0);
    assert.equal(noCode.confidence, 'unsure');
});

test('no candidates gives none', () => {
    const out = score([]);
    assert.equal(out.confidence, 'none');
    assert.equal(out.reason, null);
    assert.equal(out.winner, null);
    assert.deepEqual(out.candidates, []);
    assert.equal(out.logFields.rule, 'none');
    assert.equal(out.logFields.candidateCount, 0);
});

test('thresholds: rule 6 / rule 7 at the coverage floor', () => {
    const lines = [order({ quantity: 1000 })];
    // Packed 1000 but the PL also carries 2000 unexpected units: coverage 1000/3000.
    const doc = extracted([row({ quantity: 1000 }), row({ jfCode: 'JF0999', poNumber: 'PO_00999J', quantity: 2000 })]);
    const unsure = score([cand(booked('324'), lines, doc)]);
    assert.equal(unsure.winner.coverage, 0.3333);
    assert.equal(unsure.confidence, 'unsure');
    const doc2 = extracted([row({ quantity: 1000 }), row({ jfCode: 'JF0999', poNumber: 'PO_00999J', quantity: 3000 })]);
    const none = score([cand(booked('324'), lines, doc2)]);
    assert.equal(none.candidates[0].coverage, 0.25);
    assert.equal(none.confidence, 'none');
    assert.equal(none.winner, null, 'none has no winner');
    assert.equal(none.candidates.length, 1, 'but the candidates are kept for staff');
});

// ── New behaviour ──────────────────────────────────────────────────────────
test('PO-only disambiguation: the same product in A and B under different POs (rule 3)', () => {
    const A = [order({ po_number: 'PO_00001J' })];
    const B = [order({ id: 2, po_number: 'PO_00002J' })];
    const doc = extracted([row({ poNumber: 'PO_00001J' })]);
    const out = score([cand(booked('A'), A, doc), cand(booked('B'), B, doc)]);
    assert.equal(out.confidence, 'confident');
    assert.equal(out.reason, 'po_numbers');
    assert.equal(out.winner.key, 'booked:A');
    assert.equal(out.winner.poShare, 1);
    assert.equal(out.candidates[1].poShare, 0, 'B matched on code only');
    assert.equal(out.candidates[1].coverage, PM.METHOD_WEIGHT.jf_only);
});

test('rule 3 needs every other candidate under poShareLow; between the floors the lines decide', () => {
    // A holds both products under the printed POs; B holds the first one only,
    // under its printed PO, so B's poShare is the first row's share of the PL.
    const A = [order({ po_number: 'PO_00001J' }), order({ id: 2, jf_code: 'JF0002', po_number: 'PO_00002J', quantity: 200 })];
    const B = [order({ id: 3, po_number: 'PO_00001J' })];
    const half = extracted([row({ quantity: 100 }), row({ jfCode: 'JF0002', poNumber: 'PO_00002J', quantity: 100 })]);
    const between = score([cand(booked('A'), A, half), cand(booked('B'), B, half)]);
    assert.equal(between.winner.key, 'booked:A');
    assert.equal(between.winner.poShare, 1);
    assert.equal(between.candidates[1].poShare, 0.5, 'in [poShareLow, poShareHigh)');
    assert.deepEqual(between.conflicts, [], 'under poShareHigh is no conflict');
    assert.equal(between.confidence, 'confident');
    assert.equal(between.reason, 'lines', 'rule 3 does not apply; rule 4 does');
    const fifth = extracted([row({ quantity: 50 }), row({ jfCode: 'JF0002', poNumber: 'PO_00002J', quantity: 200 })]);
    const under = score([cand(booked('A'), A, fifth), cand(booked('B'), B, fifth)]);
    assert.equal(under.candidates[1].poShare, 0.2);
    assert.equal(under.reason, 'po_numbers');
    assert.equal(under.logFields.rule, '3');
});

test('single candidate: confident only with coverage ≥ 0.90 and exact ≥ 0.80 (rule 5)', () => {
    const lines = [order({ quantity: 100 }), order({ id: 2, jf_code: 'JF0002', po_number: 'PO_00002J', quantity: 100 })];
    const clean = score([cand(booked('324'), lines, extracted([row(), row({ jfCode: 'JF0002', poNumber: 'PO_00002J' })]))]);
    assert.equal(clean.reason, 'single');
    // One line short by 30: coverage (100+70)/170 = 1, exact 100/170 = 0.59.
    const off = score([cand(booked('324'), lines, extracted([row(), row({ jfCode: 'JF0002', poNumber: 'PO_00002J', quantity: 70 })]))]);
    assert.equal(off.winner.coverage, 1);
    assert.equal(off.winner.exact, 0.5882);
    assert.equal(off.confidence, 'unsure');
    assert.equal(off.logFields.rule, '6');
});

test('conflict: a box hit on a non-winning candidate caps at unsure', () => {
    const doc = extracted([row()], { containerNumbers: [BOX] });
    const out = score([
        cand(booked('324'), [order()], doc),
        cand(booked('325'), [order({ id: 9, jf_code: 'JF0777', po_number: 'PO_00777J' })], doc, { boxNumbers: [BOX] }),
    ], { printedNumbers: [BOX] });
    assert.equal(out.winner.key, 'booked:324');
    assert.equal(out.confidence, 'unsure');
    assert.equal(out.reason, 'conflict');
    assert.deepEqual(out.conflicts.map(c => c.code), ['box_hit_not_winner']);
    assert.deepEqual(out.candidates[1].conflicts, ['box_hit_not_winner']);
    assert.equal(out.candidates[1].boxHit, true);
    assert.equal(out.logFields.rule, 'conflict');
});

test('conflict: two box hits', () => {
    const doc = extracted([row()]);
    const out = score([
        cand(booked('324'), [order()], doc, { boxNumbers: [BOX] }),
        cand(booked('325'), [order({ id: 9, jf_code: 'JF0777', po_number: 'PO_00777J' })], doc, { boxNumbers: [BOX2] }),
    ], { printedNumbers: [BOX, BOX2] });
    assert.equal(out.confidence, 'unsure');
    assert.deepEqual(out.conflicts.map(c => c.code).sort(), ['box_hit_not_winner', 'multiple_box_hits']);
});

test('conflict: a known box number on no candidate (foreignBox)', () => {
    const doc = extracted([row()]);
    const out = score([cand(booked('324'), [order()], doc)], { printedNumbers: [BOX], foreignBoxes: [BOX] });
    assert.equal(out.confidence, 'unsure');
    assert.deepEqual(out.conflicts.map(c => c.code), ['foreign_box']);
    assert.deepEqual(out.foreignBox, [BOX]);
    // A box that IS on a candidate is not foreign, whatever the loader passed.
    const ok = score([cand(booked('324'), [order()], doc, { boxNumbers: [BOX] })], { printedNumbers: [BOX], foreignBoxes: [BOX] });
    assert.deepEqual(ok.foreignBox, []);
    assert.equal(ok.reason, 'box_number');
    // An invalid printed number is never foreign.
    const bad = score([cand(booked('324'), [order()], doc)], { printedNumbers: ['MRKU4645189'], foreignBoxes: ['MRKU4645189'] });
    assert.deepEqual(bad.conflicts, []);
});

test('conflict: poShare ≥ 0.80 on a non-winner, when the winner lacks the POs', () => {
    const doc = extracted([row(), row({ jfCode: 'JF0002', poNumber: 'PO_00002J' })]);
    // A holds both products under other POs with the right quantities; B holds
    // them under the printed POs, nearly empty.
    const A = [order({ po_number: 'PO_00009J' }), order({ id: 2, jf_code: 'JF0002', po_number: 'PO_00008J' })];
    const B = [order({ id: 3, quantity: 10 }), order({ id: 4, jf_code: 'JF0002', po_number: 'PO_00002J', quantity: 10 })];
    const out = score([cand(booked('A'), A, doc), cand(booked('B'), B, doc)]);
    assert.equal(out.winner.key, 'booked:A');
    assert.equal(out.winner.poShare, 0);
    assert.equal(out.candidates[1].poShare, 1);
    assert.ok(out.candidates[1].score < out.winner.score);
    assert.equal(out.confidence, 'unsure');
    assert.deepEqual(out.conflicts.map(c => c.code), ['po_share_not_winner']);
    // When the winner carries the POs too (a split PO), the POs settle nothing
    // and there is no conflict: the lines decide.
    const split = extracted([row({ quantity: 300, cartons: 6 })]);
    const both = score([cand(booked('A'), [order({ quantity: 300, order_quantity: 500 })], split), cand(booked('B'), [order({ id: 3, quantity: 200, order_quantity: 500 })], split)]);
    assert.equal(both.winner.poShare, 1);
    assert.equal(both.candidates[1].poShare, 1);
    assert.deepEqual(both.conflicts, []);
});

test('conflict: a replacesHit that is not the winner', () => {
    const doc = extracted([row()]);
    const out = score([
        cand(booked('324'), [order()], doc),
        cand(booked('325'), [order({ id: 9, jf_code: 'JF0777', po_number: 'PO_00777J' })], doc),
    ], { replacesContainerKey: 'booked:325' });
    assert.equal(out.confidence, 'unsure');
    assert.deepEqual(out.conflicts.map(c => c.code), ['replaces_not_winner']);
    assert.equal(out.candidates[1].replacesHit, true);
});

test('replaces: the winner with replacesHit and coverage ≥ 0.50 is confident (rule 1, before box and lines)', () => {
    // Half the PL is explained (the rest is new rows): coverage 0.5 exactly.
    const doc = extracted([row(), row({ jfCode: 'JF0555', poNumber: 'PO_00555J' })], { containerNumbers: [BOX] });
    const out = score([cand(booked('324'), [order()], doc, { boxNumbers: [BOX] })], { printedNumbers: [BOX], replacesContainerKey: 'booked:324' });
    assert.equal(out.winner.coverage, 0.5);
    assert.equal(out.winner.replacesHit, true);
    assert.equal(out.winner.boxHit, true);
    assert.equal(out.reason, 'replaces');
    assert.equal(out.logFields.rule, '1');
    const under = score([cand(booked('324'), [order()], extracted([row(), row({ jfCode: 'JF0555', poNumber: 'PO_00555J', quantity: 150 })]))], { replacesContainerKey: 'booked:324' });
    assert.equal(under.winner.coverage, 0.4);
    assert.notEqual(under.reason, 'replaces');
});

test('check digit: a number that fails it is no box hit; a one-edit near-miss is display-only', () => {
    const doc = extracted([row(), row({ jfCode: 'JF0555', poNumber: 'PO_00555J' })]);
    const bad = score([cand(booked('324'), [order()], doc, { boxNumbers: [BOX] })], { printedNumbers: ['MRKU4645189'] });
    assert.equal(bad.winner.boxHit, false);
    assert.deepEqual(bad.winner.nearBox, ['MRKU4645189']);
    assert.equal(bad.winner.score, PM.scoreOf({ ...bad.winner, boxHit: false }), 'nearBox never scores');
    assert.equal(bad.confidence, 'unsure', 'coverage 0.5 alone is not enough');
    assert.deepEqual(bad.conflicts, [], 'a near-miss is no conflict either');
    const good = score([cand(booked('324'), [order()], doc, { boxNumbers: [BOX] })], { printedNumbers: [BOX] });
    assert.equal(good.winner.boxHit, true);
    assert.deepEqual(good.winner.nearBox, []);
    assert.equal(good.reason, 'box_number');
    assert.equal(good.logFields.rule, '2');
    assert.equal(good.winner.score, bad.winner.score + PM.SCORE_WEIGHTS.boxHit);
});

test('refHit: a printed booked reference or the number a draft name reserves', () => {
    const doc = extracted([row()]);
    const out = score([
        cand(booked('324'), [order()], doc),
        cand(draft(129, 'DRAFT-SEA-260917-173825 - 328'), [order({ id: 7, jf_code: 'JF0555', po_number: 'PO_00555J' })], doc),
    ], { printedNumbers: ['324', '328'] });
    assert.equal(out.candidates[0].refHit, true);
    assert.equal(out.candidates[1].refHit, true);
    assert.equal(out.candidates[0].score, 1 + PM.SCORE_WEIGHTS.refHit);
});

test('candidates_json keeps the top 5, ordered by score then key', () => {
    const doc = extracted([row()]);
    const cands = ['F', 'E', 'D', 'C', 'B', 'A', 'G'].map(k => cand(booked(k), [order({ id: 9, jf_code: 'JF0777', po_number: 'PO_00777J' })], doc));
    cands.push(cand(booked('Z'), [order()], doc));
    const out = score(cands);
    assert.equal(out.candidates.length, PM.TOP_CANDIDATES);
    assert.deepEqual(out.candidates.map(c => c.key), ['booked:Z', 'booked:A', 'booked:B', 'booked:C', 'booked:D']);
    assert.equal(out.logFields.candidateCount, 8);
});

// ── The loader (fake connection) ───────────────────────────────────────────
// Booked lines: [{ ref, box?, ...order columns }]. Draft lines: [{ name, draftId, ...}].
// companyOf: supplier id → the lowest id sharing its portal_code (the staff
// grouping, which groupCompanies goes through); the first row's id otherwise.
function fakeLoaderConn({ groupRows, bookedLines = [], draftLines = [], knownBoxes = [], companyOf = {} }) {
    const statements = [];
    const names = new Set(groupRows.map(r => r.name));
    const mine = r => names.has(r.supplier) || names.has(r.po_supplier);
    return {
        statements,
        async query(sql, params = []) {
            const s = sql.replace(/\s+/g, ' ').trim();
            statements.push({ s, params });
            if (/^SELECT .* FROM orders o LEFT JOIN purchase_orders po ON po\.id = o\.purchase_order_id WHERE o\.deleted_at IS NULL AND o\.container_number = \?/.test(s)) {
                return [bookedLines.filter(r => r.ref === params[0]).map(({ ref, box, ...o }) => ({ ...o, container_number: ref }))];
            }
            if (/^SELECT DISTINCT o\.container_number AS ref/.test(s)) {
                const live = new Set(params[0]);
                return [[...new Set(bookedLines.filter(r => mine(r) && live.has(r.status)).map(r => r.ref))].map(ref => ({ ref }))];
            }
            if (/^SELECT .* FROM orders o LEFT JOIN purchase_orders po ON po\.id = o\.purchase_order_id WHERE o\.deleted_at IS NULL AND o\.container_number IN/.test(s)) {
                const refs = new Set(params[0]);
                return [bookedLines.filter(r => mine(r) && refs.has(r.ref)).map(({ ref, box, ...o }) => ({ ...o, container_number: ref }))];
            }
            if (/^SELECT dca\.draft_container_name AS name, MIN\(dc\.id\) AS draft_id/.test(s)) {
                const live = new Set(params[0]);
                const out = new Map();
                for (const r of draftLines) if (mine(r) && live.has(r.status)) out.set(r.name, r.draftId ?? null);
                return [[...out].map(([name, draft_id]) => ({ name, draft_id }))];
            }
            if (/^SELECT .* FROM draft_container_allocations dca JOIN orders o/.test(s)) {
                const want = new Set(params[0]);
                return [draftLines.filter(r => mine(r) && want.has(r.name)).map(({ name, draftId, ...o }) => ({ ...o, draft_name: name, container_number: null }))];
            }
            if (/^SELECT s\.name, \(SELECT MIN\(s2\.id\)/.test(s)) {
                return [groupRows.filter(g => params[0].includes(g.name)).map(g => ({ name: g.name, group_id: companyOf[g.id] ?? groupRows[0].id, id: g.id }))];
            }
            if (/^SELECT container_number AS ref, TRIM\(external_container_number\) AS box/.test(s)) {
                const refs = new Set(params[0]);
                const seen = new Set();
                const out = [];
                for (const r of bookedLines) if (r.box && refs.has(r.ref) && !seen.has(`${r.ref}|${r.box}`)) { seen.add(`${r.ref}|${r.box}`); out.push({ ref: r.ref, box: r.box }); }
                return [out];
            }
            if (/^SELECT DISTINCT UPPER\(TRIM\(external_container_number\)\) AS box/.test(s)) {
                return [knownBoxes.filter(b => params[0].includes(b)).map(box => ({ box }))];
            }
            if (/^INSERT INTO audit_log/.test(s)) return [{ insertId: 1 }];
            throw new Error(`unexpected SQL: ${s.slice(0, 100)}`);
        },
    };
}

const GROUP = [{ id: 1, name: 'Acme' }, { id: 7, name: 'Acme Ltd' }];
// The portal's group (pl-results.js portalGroup): the ids the uploader was
// granted and their live names. No supplierKey, no portal_code.
const PG = { supplierIds: [1, 7], names: ['Acme', 'Acme Ltd'] };
const line = (ref, o = {}) => ({
    ref, id: 1, jf_code: 'JF0001', product_name: 'Thing', po_number: 'PO_00001J', lot_number: null, quantity: 100,
    units_per_carton: 50, carton_weight: null, carton_height: null, carton_width: null, carton_depth: null,
    mfg_date: null, exp_date: null, status: 'CONSOLIDATED', supplier: 'Acme', order_quantity: 100, po_supplier: 'Acme Ltd', ...o,
});

test('loader: the group by name (nameKey), live statuses, and box numbers', async () => {
    const conn = fakeLoaderConn({
        groupRows: GROUP,
        bookedLines: [
            line('324', { box: BOX }),
            line('324', { id: 2, jf_code: 'JF0002', po_number: 'PO_00002J', status: 'RECEIVED', box: BOX2 }),   // not live, but still the container's line
            line('330', { id: 3, status: 'RECEIVED' }),                                            // a delivered container: no live line
            line('325', { id: 4, supplier: 'Other Co', po_supplier: 'Acme' }),                      // the PO name is Acme's: in the group
        ],
        draftLines: [
            { name: 'DRAFT-SEA-260917-173825 - 328', draftId: 129, ...line(null, { id: 5, jf_code: 'JF0003', po_number: 'PO_00003J', allocated: 40 }) },
        ],
    });
    const out = await P.loadSupplierCandidates(conn, PG, { productCodes: [], poNumbers: [], printedNumbers: [BOX] });
    assert.deepEqual(out.group, { supplierIds: [1, 7], names: ['Acme', 'Acme Ltd'] });
    assert.deepEqual(out.candidates.map(c => c.key).sort(), ['booked:324', 'booked:325', 'draft:129']);
    const c324 = out.candidates.find(c => c.key === 'booked:324');
    assert.equal(c324.lines.length, 2, 'every line of the group in a live container, status aside');
    assert.deepEqual(c324.boxNumbers, [BOX2, BOX], 'every box of the container, sorted');
    assert.equal(c324.pinned.box, true);
    assert.equal(out.candidates[0].key, 'booked:324', 'pinned first');
    const d = out.candidates.find(c => c.key === 'draft:129');
    assert.equal(d.container.kind, 'draft');
    assert.equal(d.container.containerNumber, '328');
    assert.equal(d.lines[0].quantity, 100);
    assert.equal(out.codeFilterFallback, false);
    assert.equal(out.capCut, false);
    assert.equal(out.totalContainers, 3);
    // Only this supplier's names ever reach the SQL.
    for (const st of conn.statements.filter(x => /orders o/.test(x.s))) assert.ok(st.params.some(p => Array.isArray(p) && p.includes('Acme')));
});

test('loader: the code filter keeps containers sharing a printed code, and falls back when none does', async () => {
    const make = () => fakeLoaderConn({
        groupRows: GROUP,
        bookedLines: [line('324'), line('325', { id: 2, jf_code: 'JF0002', po_number: 'PO_00002J' }), line('326', { id: 3, jf_code: 'JF0003_FQC', po_number: 'PO_00003J' })],
    });
    const kept = await P.loadSupplierCandidates(make(), PG, { productCodes: ['JF0003'] });
    assert.deepEqual(kept.candidates.map(c => c.key), ['booked:326'], 'the FQC sample counts as its base code');
    assert.equal(kept.codeFilterFallback, false);
    assert.equal(kept.totalContainers, 3);
    const fallback = await P.loadSupplierCandidates(make(), PG, { productCodes: ['JF9999'] });
    assert.deepEqual(fallback.candidates.map(c => c.key), ['booked:324', 'booked:325', 'booked:326']);
    assert.equal(fallback.codeFilterFallback, true);
    // A pinned container survives the filter even without a shared code.
    const pinned = await P.loadSupplierCandidates(make(), PG, { productCodes: ['JF0003'], printedNumbers: ['324'] });
    assert.deepEqual(pinned.candidates.map(c => c.key), ['booked:324', 'booked:326']);
    assert.equal(pinned.candidates[0].pinned.ref, true);
});

test('loader: the replaced filing is pinned, the cap is deterministic and capCut is recorded', async () => {
    const bookedLines = [];
    for (let i = 1; i <= 70; i++) {
        const ref = String(100 + i);
        bookedLines.push(line(ref, { id: i, jf_code: i % 2 ? 'JF0001' : 'JF0002', po_number: i % 3 ? 'PO_00001J' : 'PO_00002J' }));
    }
    bookedLines.push(line('999', { id: 999, jf_code: 'JF0777', po_number: 'PO_00777J' }));   // shares nothing with the PL
    const conn = fakeLoaderConn({ groupRows: GROUP, bookedLines });
    const out = await P.loadSupplierCandidates(conn, PG, {
        productCodes: ['JF0001', 'JF0002'], poNumbers: ['PO00002J'], printedNumbers: [], replacesContainer: booked('999'),
    });
    assert.equal(out.totalContainers, 71);
    assert.equal(out.capCut, true);
    assert.equal(out.candidates.length, P.CANDIDATE_CAP);
    assert.equal(out.candidates[0].key, 'booked:999', 'pinned first, whatever it shares');
    assert.equal(out.candidates[0].pinned.replaces, true);
    assert.equal(out.evidence.replacesContainerKey, 'booked:999');
    // Then by shared codes (all 1 here), shared POs (the multiples of 3), key.
    const rest = out.candidates.slice(1);
    const withPo = rest.filter(c => c.sharedPos === 1).map(c => c.key);
    const without = rest.filter(c => c.sharedPos === 0).map(c => c.key);
    assert.ok(withPo.length > 0 && without.length > 0);
    assert.deepEqual(rest.map(c => c.key), [...withPo, ...without], 'shared POs before none');
    assert.deepEqual(withPo, [...withPo].sort(), 'then key ascending');
    assert.deepEqual(without, [...without].sort());
    // The same input gives the same order.
    const again = await P.loadSupplierCandidates(fakeLoaderConn({ groupRows: GROUP, bookedLines }), PG, {
        productCodes: ['JF0001', 'JF0002'], poNumbers: ['PO00002J'], printedNumbers: [], replacesContainer: booked('999'),
    });
    assert.deepEqual(again.candidates.map(c => c.key), out.candidates.map(c => c.key));
});

test('loader: a replaced filing with no candidate line of its own is not added; a group with no live name loads nothing', async () => {
    const conn = fakeLoaderConn({ groupRows: GROUP, bookedLines: [line('324')] });
    // '999' is not among the live containers; its lines would be loaded through
    // loadGroupLines, which the fake answers with no rows for a booked ref
    // it does not know.
    conn.query = ((orig) => async (sql, params) => {
        if (/WHERE o\.deleted_at IS NULL AND o\.container_number = \?/.test(sql.replace(/\s+/g, ' '))) return [[]];
        return orig(sql, params);
    })(conn.query);
    const out = await P.loadSupplierCandidates(conn, PG, { replacesContainer: booked('999') });
    assert.deepEqual(out.candidates.map(c => c.key), ['booked:324']);
    const none = await P.loadSupplierCandidates(conn, { supplierIds: [1], names: [] }, {});
    assert.deepEqual(none.candidates, []);
    assert.deepEqual(none.group.names, []);
    assert.deepEqual((await P.loadSupplierCandidates(conn, null, {})).candidates, []);
});

test('suggestContainer: loader + comparisons + decision, with the log fields', async () => {
    const conn = fakeLoaderConn({
        groupRows: GROUP,
        bookedLines: [line('324', { box: BOX }), line('325', { id: 2, jf_code: 'JF0002', po_number: 'PO_00002J' })],
    });
    const doc = extracted([row()], { containerNumbers: [BOX] });
    const logs = [];
    const log = require('../src/lib/logger');
    const orig = log.info;
    log.info = (...a) => logs.push(a);
    try {
        const s = await PL.suggestContainer(conn, { group: PG, extracted: doc, filename: 'pl.pdf', submissionId: 42 });
        assert.equal(s.match.confidence, 'confident');
        assert.equal(s.match.reason, 'box_number');
        assert.equal(s.winnerContainer.containerNumber, '324');
        assert.equal(s.logFields.submissionId, 42);
        assert.equal(s.logFields.rule, '2');
        assert.equal(s.logFields.capCut, false);
        assert.equal(s.logFields.codeFilterFallback, false);
        assert.equal(logs.length, 1);
        assert.equal(logs[0][1].winnerCoverage, 1);
        const m = PL.matchColumns(s);
        assert.deepEqual(m.suggested, { kind: 'booked', containerNumber: '324', draftId: null, label: '324' });
        assert.equal('groupSupplierIds' in m, false, 'group_supplier_ids is the portal’s: never among the match columns');
        assert.equal(m.candidates.top.length, 1, '325 holds no printed code: filtered out');
        assert.deepEqual(m.candidates.conflicts, [], 'the matcher’s top-level conflicts are stored with the top 5');
        assert.deepEqual(m.candidates.foreignBox, []);
        assert.equal(s.loaded.totalContainers, 2);
    } finally {
        log.info = orig;
    }
});

// ── The company (a login holding more than one) ────────────────────────────
test('nameWords / sameCompanyName: accents, punctuation and legal forms do not count', () => {
    assert.deepEqual(PM.nameWords('MEDOFFICE SAĞLIK ENDÜSTRİ A.Ş.'), ['medoffice', 'saglik', 'endustri']);
    assert.deepEqual(PM.nameWords('Suzhou Sunmed Co.,Ltd.'), ['suzhou', 'sunmed']);
    assert.equal(PM.sameCompanyName('MEDOFFICE SAĞLIK ENDÜSTRİ A.Ş.', 'Medoffice Saglik Endustri A.S.'), true);
    assert.equal(PM.sameCompanyName('SUZHOU SUNMED CO., LTD.', 'SUNMED'), true, 'the shorter name within the longer');
    assert.equal(PM.sameCompanyName('SUZHOU SUNMED CO., LTD.', 'Medoffice Saglik Endustri A.S.'), false);
    assert.equal(PM.sameCompanyName(null, 'SUNMED'), false);
    assert.equal(PM.sameCompanyName('Co., Ltd.', 'SUNMED'), false, 'nothing but legal forms names no one');
});

test('pickCompany: the lines decide, the printed name breaks a tie, nothing to go on is null', () => {
    const sunmed = { id: 2, names: ['Suzhou Sunmed Co.,Ltd.', 'SUNMED'] };
    const medoffice = { id: 9, names: ['Medoffice Saglik Endustri A.S.'] };
    const pick = (a, b, printedName = null) => PM.pickCompany({ companies: [{ ...sunmed, best: a }, { ...medoffice, best: b }], printedName })?.id ?? null;
    assert.equal(pick(0, 0.45), 9, 'the company whose lines it fits');
    assert.equal(pick(0.6, 0.45, 'MEDOFFICE SAĞLIK ENDÜSTRİ A.Ş.'), 2, 'the lines outweigh the printed name');
    assert.equal(pick(0.45, 0.45, 'MEDOFFICE SAĞLIK ENDÜSTRİ A.Ş.'), 9, 'a tie goes to the printed name');
    assert.equal(pick(0.45, 0.45), null, 'a tie with no name between them');
    assert.equal(pick(0, 0, 'SUZHOU SUNMED CO., LTD.'), 2, 'no line anywhere: the printed name alone');
    assert.equal(pick(0, 0, 'JFA MEDICAL LTD'), null);
    assert.equal(PM.pickCompany({ companies: [{ ...sunmed, best: 0 }] }).id, 2, 'one company is that company');
    assert.equal(PM.pickCompany({ companies: [] }), null);
});

// Acme (ids 1 and 7, one portal_code) and Medco (id 9) on one login.
const TWO = [{ id: 1, name: 'Acme' }, { id: 7, name: 'Acme Ltd' }, { id: 9, name: 'Medco Saglik A.S.' }];
const TWO_PG = { supplierIds: [1, 7, 9], names: TWO.map(g => g.name), suppliers: TWO };
const TWO_COMPANIES = { 1: 1, 7: 1, 9: 9 };
const twoCompanyLines = () => [
    line('324'),
    line('324', { id: 2, jf_code: 'JF0002', po_number: 'PO_00002J', quantity: 300 }),
    line('326', { id: 3, jf_code: 'JF0230', po_number: 'MEDCO-13', quantity: 5454, supplier: 'Medco Saglik A.S.', po_supplier: 'Medco Saglik A.S.' }),
];
// No product code and no PO: the rows only line up by quantity.
const medcoDoc = () => extracted([row({ jfCode: null, poNumber: null, quantity: 5454, cartons: 140 })], { supplierName: 'MEDCO SAĞLIK A.Ş.' });

test('groupCompanies: the ids split by portal_code, keyed and named by the lowest', async () => {
    const conn = fakeLoaderConn({ groupRows: TWO, companyOf: TWO_COMPANIES });
    const companies = await PL.groupCompanies(conn, TWO_PG);
    assert.deepEqual(companies.map(c => [c.key, c.name, c.supplierIds, c.names]), [
        ['s:1', 'Acme', [1, 7], ['Acme', 'Acme Ltd']],
        ['s:9', 'Medco Saglik A.S.', [9], ['Medco Saglik A.S.']],
    ]);
    assert.deepEqual(PL.groupKeys(companies[0]), ['s:1', 's:7']);
    assert.equal(PL.companyForKey(companies, 's:7').key, 's:1', 'an older filing under a granted id');
    assert.equal(PL.companyForKey(companies, 's:5'), null);
    // An id whose code-mate is not granted: keyed by the lowest live id all the same.
    const lone = await PL.groupCompanies(fakeLoaderConn({ groupRows: [{ id: 7, name: 'Acme Ltd' }], companyOf: { 7: 1 } }), { suppliers: [{ id: 7, name: 'Acme Ltd' }] });
    assert.deepEqual(lone.map(c => [c.key, c.name]), [['s:1', 'Acme Ltd']]);
    assert.deepEqual(PL.groupKeys(lone[0]), ['s:1', 's:7']);
});

test('suggestContainer: with two companies only the one the document fits is scored', async () => {
    const conn = fakeLoaderConn({ groupRows: TWO, bookedLines: twoCompanyLines(), companyOf: TWO_COMPANIES });
    const s = await PL.suggestContainer(conn, { group: TWO_PG, extracted: medcoDoc(), quiet: true });
    assert.equal(s.company.key, 's:9');
    assert.deepEqual(s.match.candidates.map(c => c.key), ['booked:326'], 'Acme’s containers are not candidates');
    assert.equal(s.winnerContainer.containerNumber, '326');
    assert.equal(s.logFields.company, 's:9');
    assert.equal(s.logFields.companies, 2);
    assert.equal(PL.shownCompany(TWO_PG, s.company).name, 'Medco Saglik A.S.');
    // Acme's own list stays with Acme.
    const acme = await PL.suggestContainer(fakeLoaderConn({ groupRows: TWO, bookedLines: twoCompanyLines(), companyOf: TWO_COMPANIES }), {
        group: TWO_PG, extracted: extracted([row(), row({ jfCode: 'JF0002', poNumber: 'PO_00002J', quantity: 300 })]), quiet: true,
    });
    assert.equal(acme.company.key, 's:1');
    assert.deepEqual(acme.match.candidates.map(c => c.key), ['booked:324']);
    // One company: that company, and nothing named on the supplier's page.
    const one = { supplierIds: [1, 7], names: ['Acme', 'Acme Ltd'], suppliers: TWO.slice(0, 2) };
    const single = await PL.suggestContainer(fakeLoaderConn({ groupRows: TWO.slice(0, 2), bookedLines: twoCompanyLines(), companyOf: TWO_COMPANIES }), { group: one, extracted: extracted([row()]), quiet: true });
    assert.equal(single.company.key, 's:1');
    assert.equal(PL.shownCompany(one, single.company), null);
});

test('filingCompany: the company whose lines it fits in that container', async () => {
    const conn = fakeLoaderConn({ groupRows: TWO, bookedLines: twoCompanyLines(), companyOf: TWO_COMPANIES });
    assert.equal((await PL.filingCompany(conn, { group: TWO_PG, container: booked('326'), extracted: medcoDoc(), supplierId: 1 })).key, 's:9');
    assert.equal((await PL.filingCompany(conn, { group: TWO_PG, container: booked('324'), extracted: medcoDoc(), supplierId: 1 })).key, 's:1', 'only Acme is on board there');
    assert.equal((await PL.filingCompany(conn, { group: TWO_PG, container: booked('999'), extracted: medcoDoc(), supplierId: 1 })).key, 's:9', 'none on board: the printed name');
    assert.equal((await PL.filingCompany(conn, { group: TWO_PG, container: booked('999'), extracted: null, supplierId: 7 })).key, 's:1', 'nothing to go on: the row’s supplier');
});
