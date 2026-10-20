'use strict';

// Which container does a supplier's packing list belong to? The matcher behind
// the supplier-portal packing-list upload (jportal.pl_submissions, checked by
// src/services/pl-check.js). Staff always confirm the container; this only
// SUGGESTS one, so a wrong confident answer costs more than an unsure one — the
// supplier acknowledges discrepancies against the suggestion.
//
// Pure: no database, no clock. The caller (src/services/pl-results.js) loads
// the candidate containers (packing-list-check.js loadSupplierCandidates), runs
// compareDocuments(supplierRowsInContainer, [thisDocument]) for each, and
// hands the comparisons to scoreCandidates. Per candidate, over the match and
// mismatch lines, with P = packed units and E = expected units:
//   M = Σ w·min(packed, expected), w = METHOD_WEIGHT by how the line matched
//       (a quantity-only no_code match counts half);
//   X = the same over lines with no quantity difference;
//   coverage = M/P   how much of this PL the container explains (one container
//                    can hold several PLs from one supplier, so this leads);
//   exact    = X/P   tie-breaks a PO line split across containers;
//   recall   = M/E;
//   lineHit  = matched groups ÷ (matched + unexpected groups);
//   poShare  = packed units on jf_po-matched lines ÷ P.
// Plus the printed evidence: boxHit (a printed ISO 6346 number — check digit
// verified — that is a candidate's orders.external_container_number), refHit
// (a printed booked reference, or the number a draft's name reserves) and
// replacesHit (the row replaces a submission filed in this candidate). A
// printed number one edit away from a box number is kept as nearBox for staff
// and never scores. A valid printed number known to orders but on no
// candidate is a foreignBox: an old shipment or another supplier's box.
//
// The conflict check runs before the rules and caps the answer at unsure: a
// box hit on a non-winner, two box hits, a foreign box, PO evidence pointing
// away from the winner, or a replacesHit elsewhere. Then the first rule that
// applies decides:
//   1 replaces    the winner has replacesHit and coverage ≥ 0.50
//   2 box number  the winner is the only box hit and has coverage ≥ 0.50
//   3 PO numbers  two or more candidates; only the winner has poShare ≥ 0.80
//                 (with coverage ≥ 0.80) and every other has poShare < 0.30
//   4 lines       two or more candidates; coverage ≥ 0.80 and a score margin
//                 of 0.12 over the second
//   5 single      one candidate; coverage ≥ 0.90 AND exact ≥ 0.80 — nothing
//                 to beat, and the PL may be for a shipment not created yet
//   6 unsure      coverage ≥ 0.30
//   7 none
// Tune from data: every ratio is kept in candidates_json, and one structured
// log line per run.
//
// A portal login may hold more than one company (Sunmed's three spellings and
// Medoffice, say). A packing list is one company's, so the caller scores each
// company's containers apart and pickCompany keeps the one whose order lines
// the document fits best; the supplier name printed on it breaks a tie, and
// decides alone when no line matches anywhere.

// ── Constants (exported so they can be tuned) ────────────────────────────
const METHOD_WEIGHT = { jf_po: 1, jf_only: 0.85, no_code: 0.5 };

const SCORE_WEIGHTS = {
    coverage: 0.35, exact: 0.20, recall: 0.15, lineHit: 0.10, poShare: 0.20,
    boxHit: 0.30, refHit: 0.10, replacesHit: 0.20,
};

const THRESHOLDS = {
    replacesCoverage: 0.50,   // rule 1
    boxCoverage: 0.50,        // rule 2
    poShareHigh: 0.80,        // rule 3 (and the conflict check)
    poShareLow: 0.30,         // rule 3: every other candidate
    poCoverage: 0.80,         // rule 3
    linesCoverage: 0.80,      // rule 4
    linesMargin: 0.12,        // rule 4: best score − second score
    singleCoverage: 0.90,     // rule 5
    singleExact: 0.80,        // rule 5
    unsureCoverage: 0.30,     // rule 6
};

const TOP_CANDIDATES = 5;

const CONFIDENCES = ['confident', 'unsure', 'none'];
const MATCH_REASONS = ['replaces', 'box_number', 'po_numbers', 'lines', 'single', 'conflict'];
const CONFLICT_CODES = {
    box_hit_not_winner: 'A printed container number belongs to a candidate that did not score highest.',
    multiple_box_hits: 'The printed container numbers belong to more than one candidate (a PL that spans two containers?).',
    foreign_box: 'A printed container number is a known box, but not one of the candidates (an old shipment, or another supplier\'s box).',
    po_share_not_winner: 'The printed PO numbers mostly belong to a candidate that did not score highest.',
    replaces_not_winner: 'The submission this one replaces was filed in a candidate that did not score highest.',
};

// ── Normalisers ──────────────────────────────────────────────────────────
const normBox = s => String(s == null ? '' : s).toUpperCase().replace(/[^A-Z0-9]/g, '');
const normRef = s => String(s == null ? '' : s).toUpperCase().replace(/[\s\-_.]/g, '').trim();
const uniq = arr => [...new Set(arr.filter(v => v != null && v !== ''))];
const round4 = v => Math.round(v * 10000) / 10000;

// ── ISO 6346 ─────────────────────────────────────────────────────────────
// Owner code + category (4 letters), 6 digits, 1 check digit. Letters take
// the values 10… skipping multiples of 11 (A=10, B=12, …, K=21, L=23, …,
// U=32, V=34, …, Z=38); each position is weighted 2^i; the sum mod 11 is the
// check digit, a 10 counting as 0. Shipping only had the shape regex before
// (lib/shipments.js CONTAINER_RE), and a misread digit passes that.
const LETTER_VALUES = (() => {
    const out = {};
    let v = 10;
    for (let i = 0; i < 26; i++) {
        while (v % 11 === 0) v += 1;
        out[String.fromCharCode(65 + i)] = v;
        v += 1;
    }
    return out;
})();

function iso6346CheckDigit(ten) {
    let sum = 0;
    for (let i = 0; i < 10; i++) {
        const ch = ten[i];
        const value = i < 4 ? LETTER_VALUES[ch] : Number(ch);
        sum += value * (2 ** i);
    }
    return (sum % 11) % 10;
}

/** True for a well-formed container number whose check digit is right. */
function iso6346Valid(number) {
    const n = normBox(number);
    if (!/^[A-Z]{4}\d{7}$/.test(n)) return false;
    return iso6346CheckDigit(n.slice(0, 10)) === Number(n[10]);
}

// Edit distance exactly 1 (one substitution, insertion or deletion).
function nearBox(a, b) {
    const x = normBox(a);
    const y = normBox(b);
    if (!x || !y || x === y) return false;
    if (Math.abs(x.length - y.length) > 1) return false;
    if (x.length === y.length) {
        let diff = 0;
        for (let i = 0; i < x.length; i++) if (x[i] !== y[i]) diff += 1;
        return diff === 1;
    }
    const [short, long] = x.length < y.length ? [x, y] : [y, x];
    let i = 0;
    let j = 0;
    let skipped = false;
    while (i < short.length && j < long.length) {
        if (short[i] === long[j]) { i += 1; j += 1; continue; }
        if (skipped) return false;
        skipped = true;
        j += 1;
    }
    return true;
}

// ── Candidate identity ───────────────────────────────────────────────────
// A stable key for a resolved container (packing-list-check.js shapes):
// 'booked:324', 'draft:129', or 'draft:<name>' for an allocation name with no
// registry row.
function containerKey(container) {
    if (!container) return null;
    if (container.kind === 'booked') return `booked:${normRef(container.containerNumber)}`;
    if (container.draftId != null) return `draft:${Number(container.draftId)}`;
    return `draft:${String(container.draftName || '').trim()}`;
}

// What the document itself offers as evidence (the extraction's shape is
// packing-list-check.js RESPONSE_SCHEMA): product codes (base form, the
// _FQC sample folded in), PO numbers and the printed container numbers.
function evidenceFromExtraction(extracted) {
    const lines = Array.isArray(extracted?.lines) ? extracted.lines : [];
    const baseJf = s => normRef(String(s ?? '').replace(/_FQC$/i, ''));
    return {
        productCodes: uniq(lines.map(l => baseJf(l?.jfCode))),
        poNumbers: uniq(lines.map(l => normRef(l?.poNumber))),
        printedNumbers: uniq((Array.isArray(extracted?.containerNumbers) ? extracted.containerNumbers : []).filter(n => n != null).map(n => String(n).trim())),
    };
}

// ── Ratios ───────────────────────────────────────────────────────────────
/** The per-candidate ratios from one compareDocuments output. */
function ratiosOf(comparison) {
    const lines = Array.isArray(comparison?.lines) ? comparison.lines : [];
    const P = Number(comparison?.summary?.packedUnits) || 0;
    const E = Number(comparison?.summary?.expectedUnits) || 0;
    let M = 0;
    let X = 0;
    let poUnits = 0;
    let matched = 0;
    let unexpected = 0;
    for (const l of lines) {
        if (l.status === 'unexpected') { unexpected += 1; continue; }
        if (l.status !== 'match' && l.status !== 'mismatch') continue;
        matched += 1;
        const w = METHOD_WEIGHT[l.matchMethod] ?? METHOD_WEIGHT.no_code;
        const packed = Number(l.packed?.quantity) || 0;
        const expected = Number(l.expected?.quantity) || 0;
        const m = w * Math.min(packed, expected);
        M += m;
        if (!(l.differences || []).some(d => d.field === 'quantity')) X += m;
        if (l.matchMethod === 'jf_po') poUnits += packed;
    }
    return {
        coverage: P > 0 ? round4(M / P) : 0,
        exact: P > 0 ? round4(X / P) : 0,
        recall: E > 0 ? round4(M / E) : 0,
        lineHit: matched + unexpected > 0 ? round4(matched / (matched + unexpected)) : 0,
        poShare: P > 0 ? round4(poUnits / P) : 0,
        packedUnits: P,
        expectedUnits: E,
        matched,
        unexpected,
        missing: lines.filter(l => l.status === 'missing').length,
    };
}

function scoreOf(c) {
    const w = SCORE_WEIGHTS;
    return round4(
        w.coverage * c.coverage + w.exact * c.exact + w.recall * c.recall + w.lineHit * c.lineHit + w.poShare * c.poShare
        + w.boxHit * (c.boxHit ? 1 : 0) + w.refHit * (c.refHit ? 1 : 0) + w.replacesHit * (c.replacesHit ? 1 : 0)
    );
}

// ── The decision ─────────────────────────────────────────────────────────
/**
 * Score the candidates and decide.
 *   candidates: [{ container, boxNumbers: [...], reservedRef?, comparison }]
 *     container  — a resolved container (packing-list-check.js);
 *     boxNumbers — the orders.external_container_number(s) of a booked one;
 *     reservedRef — the reference a printed number may name (defaults to
 *                   container.containerNumber: the booked reference, or the
 *                   number a draft's name reserves);
 *     comparison — compareDocuments(supplierRowsInContainer, [thisDocument]).
 *   printedNumbers      — extracted.containerNumbers.
 *   replacesContainerKey — containerKey of where the replaced submission was
 *                          filed, or null.
 *   foreignBoxes        — valid printed numbers the loader found in
 *                         orders.external_container_number; those on no
 *                         candidate are the foreign boxes.
 * → { confidence, reason, winner, candidates, conflicts, logFields }
 *   winner is the top candidate's entry (null for 'none'); candidates are the
 *   top TOP_CANDIDATES by score, each with its ratios, hits, nearBox, score and
 *   the conflict codes it is part of.
 */
function scoreCandidates({ candidates = [], printedNumbers = [], replacesContainerKey = null, foreignBoxes = [] } = {}) {
    const printed = uniq(printedNumbers.map(normBox));
    const validPrinted = printed.filter(iso6346Valid);
    const printedRefs = uniq(printedNumbers.map(normRef));

    const scored = candidates.map((c) => {
        const container = c.container || {};
        const key = c.key || containerKey(container);
        const boxes = uniq((c.boxNumbers || []).map(normBox));
        const ref = normRef(c.reservedRef ?? container.containerNumber);
        const r = ratiosOf(c.comparison);
        const entry = {
            key,
            kind: container.kind || null,
            label: container.label ?? container.draftName ?? container.containerNumber ?? null,
            containerNumber: container.containerNumber ?? null,
            draftContainerId: container.kind === 'draft' ? container.draftId ?? null : null,
            draftName: container.kind === 'draft' ? container.draftName ?? null : null,
            boxNumbers: boxes,
            ...r,
            boxHit: validPrinted.some(n => boxes.includes(n)),
            refHit: Boolean(ref) && printedRefs.includes(ref),
            replacesHit: Boolean(replacesContainerKey) && key === replacesContainerKey,
            nearBox: uniq(printed.filter(n => boxes.some(b => nearBox(n, b)))),
            conflicts: [],
        };
        entry.score = scoreOf(entry);
        return entry;
    });
    // Deterministic: score, then key.
    scored.sort((a, b) => b.score - a.score || String(a.key).localeCompare(String(b.key)));

    const onCandidates = new Set(scored.flatMap(c => c.boxNumbers));
    const foreignBox = uniq(foreignBoxes.map(normBox)).filter(n => iso6346Valid(n) && !onCandidates.has(n));

    const empty = { confidence: 'none', reason: null, winner: null, candidates: [], conflicts: [], foreignBox };
    if (!scored.length) {
        return { ...empty, logFields: logFieldsOf(empty, 'none', 0) };
    }

    const winner = scored[0];
    const conflicts = [];
    const flag = (code, entries) => {
        conflicts.push({ code, message: CONFLICT_CODES[code], candidates: entries.map(e => e.key) });
        for (const e of entries) if (!e.conflicts.includes(code)) e.conflicts.push(code);
    };
    const boxHits = scored.filter(c => c.boxHit);
    if (boxHits.some(c => c !== winner)) flag('box_hit_not_winner', boxHits.filter(c => c !== winner));
    if (boxHits.length >= 2) flag('multiple_box_hits', boxHits);
    if (foreignBox.length) conflicts.push({ code: 'foreign_box', message: CONFLICT_CODES.foreign_box, candidates: [], boxNumbers: foreignBox });
    // PO evidence pointing AWAY from the winner. When the winner carries the
    // POs too (a PO line split across two drafts), the POs settle nothing and
    // the lines decide (rule 4, where `exact` tells the drafts apart).
    const poElsewhere = winner.poShare < THRESHOLDS.poShareHigh ? scored.filter(c => c !== winner && c.poShare >= THRESHOLDS.poShareHigh) : [];
    if (poElsewhere.length) flag('po_share_not_winner', poElsewhere);
    const replacesElsewhere = scored.filter(c => c !== winner && c.replacesHit);
    if (replacesElsewhere.length) flag('replaces_not_winner', replacesElsewhere);

    let confidence;
    let reason;
    let rule;
    const T = THRESHOLDS;
    if (conflicts.length) {
        confidence = 'unsure'; reason = 'conflict'; rule = 'conflict';
    } else if (winner.replacesHit && winner.coverage >= T.replacesCoverage) {
        confidence = 'confident'; reason = 'replaces'; rule = '1';
    } else if (winner.boxHit && boxHits.length === 1 && winner.coverage >= T.boxCoverage) {
        confidence = 'confident'; reason = 'box_number'; rule = '2';
    } else if (
        // Two or more candidates: with one, rule 5's `exact` guard applies
        // (the PL may be for a shipment not created yet).
        scored.length >= 2
        && winner.poShare >= T.poShareHigh && winner.coverage >= T.poCoverage
        && scored.filter(c => c.poShare >= T.poShareHigh).length === 1
        && scored.every(c => c === winner || c.poShare < T.poShareLow)
    ) {
        confidence = 'confident'; reason = 'po_numbers'; rule = '3';
    } else if (scored.length >= 2 && winner.coverage >= T.linesCoverage && winner.score - scored[1].score >= T.linesMargin) {
        confidence = 'confident'; reason = 'lines'; rule = '4';
    } else if (scored.length === 1 && winner.coverage >= T.singleCoverage && winner.exact >= T.singleExact) {
        confidence = 'confident'; reason = 'single'; rule = '5';
    } else if (winner.coverage >= T.unsureCoverage) {
        confidence = 'unsure'; reason = 'lines'; rule = '6';
    } else {
        confidence = 'none'; reason = null; rule = '7';
    }

    const out = {
        confidence,
        reason,
        winner: confidence === 'none' ? null : winner,
        candidates: scored.slice(0, TOP_CANDIDATES),
        conflicts,
        foreignBox,
    };
    out.logFields = logFieldsOf(out, rule, scored.length);
    return out;
}

// ── The company ──────────────────────────────────────────────────────────
// Legal forms and filler, which say nothing about which company it is.
const NAME_NOISE = new Set([
    'co', 'company', 'ltd', 'limited', 'llc', 'inc', 'corp', 'corporation', 'plc', 'gmbh', 'srl', 'spa', 'sa', 'as',
    'sti', 'san', 'tic', 've', 'pvt', 'the', 'and', 'of',
]);

// The words of a company name: accents dropped (Ş → s, İ → i, Ü → u), lower
// case, punctuation as spaces, single letters and legal forms left out.
// 'MEDOFFICE SAĞLIK ENDÜSTRİ A.Ş.' → ['medoffice', 'saglik', 'endustri'].
function nameWords(name) {
    const folded = String(name ?? '').normalize('NFKD').replace(/[̀-ͯ]/g, '').replace(/ı/g, 'i').toLowerCase();
    return uniq(folded.split(/[^a-z0-9]+/).filter(w => w.length > 1 && !NAME_NOISE.has(w)));
}

// Whether a printed name and a supplier name are the same company: every word
// of the shorter is among the longer's ('SUNMED' in 'SUZHOU SUNMED CO., LTD.').
function sameCompanyName(printed, name) {
    const a = nameWords(printed);
    const b = nameWords(name);
    if (!a.length || !b.length) return false;
    const [short, long] = a.length <= b.length ? [a, b] : [b, a];
    return short.every(w => long.includes(w));
}

/**
 * Which of a login's companies a packing list is (see the header).
 *   companies   — [{ id, names, best }]: best is what the document scores
 *                 against that company's lines (its top candidate's score),
 *                 0 when nothing matched;
 *   printedName — extracted.supplierName.
 * → the chosen entry; null when nothing tells them apart (no line matched
 *   and the printed name names none or several, or two companies tie).
 */
function pickCompany({ companies = [], printedName = null } = {}) {
    if (companies.length <= 1) return companies[0] ?? null;
    const named = c => (c.names || []).some(n => sameCompanyName(printedName, n));
    const best = c => Number(c.best) || 0;
    const ranked = [...companies].sort((a, b) => best(b) - best(a) || (named(b) ? 1 : 0) - (named(a) ? 1 : 0));
    const [top, second] = ranked;
    if (best(top) > 0) return best(second) === best(top) && named(second) === named(top) ? null : top;
    const byName = companies.filter(named);
    return byName.length === 1 ? byName[0] : null;
}

// The structured log line's fields (the caller adds submissionId, capCut and
// codeFilterFallback): CloudWatch Insights groups on these.
function logFieldsOf(out, rule, candidateCount) {
    const w = out.winner || (out.candidates && out.candidates[0]) || null;
    return {
        rule,
        confidence: out.confidence,
        reason: out.reason,
        candidateCount,
        conflicts: out.conflicts.map(c => c.code),
        winnerKey: w ? w.key : null,
        winnerCoverage: w ? w.coverage : null,
        winnerPoShare: w ? w.poShare : null,
        winnerScore: w ? w.score : null,
    };
}

module.exports = {
    METHOD_WEIGHT,
    SCORE_WEIGHTS,
    THRESHOLDS,
    TOP_CANDIDATES,
    CONFIDENCES,
    MATCH_REASONS,
    CONFLICT_CODES,
    LETTER_VALUES,
    normBox,
    normRef,
    iso6346CheckDigit,
    iso6346Valid,
    nearBox,
    containerKey,
    evidenceFromExtraction,
    ratiosOf,
    scoreOf,
    scoreCandidates,
    nameWords,
    sameCompanyName,
    pickCompany,
};
