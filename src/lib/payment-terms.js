'use strict';

// Reading a supplier's payment terms: how much of an order is a deposit.
//
// A PORT of ShipLine's parser (Shipline/src/components/payments/paymentsFlowMath.ts:
// normaliseTerms, triggerOf, offsetOf, splitClauses, parsePaymentTerms,
// resolveTermsRule, and resolvePolicy's deposit %), kept line for line so the
// server reads terms exactly as the Payments flow page shows them to staff.
// CHANGE BOTH TOGETHER. Until now only ShipLine read terms; the server needs
// the same answer to decide whether a supplier requires a deposit before a
// line may move into production by itself (src/lib/artwork-confirmations.js,
// docs/artwork-confirmations.md).
//
// Pure: no database. Tested by tools/test-payment-terms-lib.js with the
// vectors documented at the top of ShipLine's file.

const NET_TERMS_ANCHOR = 'bl';

const NUMBER_WORDS = {
    one: '1', seven: '7', ten: '10', fourteen: '14', fifteen: '15', twenty: '20', thirty: '30',
    'forty-five': '45', 'forty five': '45', forty: '40', sixty: '60', ninety: '90',
};

function normaliseTerms(raw) {
    let s = raw.toLowerCase().replace(/\s+/g, ' ').trim();
    s = s.replace(/\bb\s*\/\s*l\b|\bb\.l\.?|\bbill of lad\w*|\bbol\b|\bbl\b/g, ' bl ');
    s = s.replace(/\bt\s*\/\s*t\b|\bt\.t\.?|\btt\b|\btelegraphic transfer\b|\bwire transfer\b/g, ' tt ');
    s = s.replace(/\bd\s*\/\s*p\b|\bd\.p\.?|\bcad\b|\bdocuments? agains\w*( payment)?\b/g, ' dp ');
    s = s.replace(/\bl\s*\/\s*c\b|\bl\.c\.?|\blc\b|\bletter of credit\b/g, ' lc ');
    s = s.replace(/\binadvance\b|\bin advanced?\b|\badvanced payment\b/g, ' in advance ');
    // British spelling: "despatch", "despatched".
    s = s.replace(/\bdespatch/g, 'dispatch');
    s = s.replace(/(\d+(?:\.\d+)?)\s*(%|percent|per cent|pct)/g, '$1%');
    s = s.replace(/\b(forty[- ]five|one|seven|ten|fourteen|fifteen|twenty|thirty|forty|sixty|ninety)\b(?=\s*(day|week|month))/g, m => NUMBER_WORDS[m] ?? m);
    return s.replace(/\s+/g, ' ').trim();
}

const PCT_RE = /(\d+(?:\.\d+)?)%/g;
const DEPOSIT_RE = /\bdeposit|\bdown ?payment|\badvanc|\bupfront|\bup front|\bprepay|\bprepaid|\bpre-?payment|\bwith (the )?order\b|\b(on|upon|against|when) (the )?order\b|\border confirm|\b(before|prior to|to start) production\b|\bfor production\b/;
const BALANCE_WORDS_RE = /\bbalance|\bremain|\bremainder|\brest\b|\bfinal\b|\bblance/;
const B0_BEFORE_RE = /\b(before|prior to|pre)[- ]?(dispatch|shipment|shipping|loading|departure|delivery|release|sailing)/;
const B0_READY_RE = /\b(finished|ready|readiness|completed|completion)\b/;
const B1_BL_RE = /\bbl\b|\bdp\b|\btelex|\bon board|\bshipping doc|\b(copy|set) of (the )?doc|\bcopy doc|\bagainst (the )?doc|\bdocuments\b|\bshipped\b|\b(after|upon|on|from|against)\s+(?:\w+\s+){0,3}(shipment|dispatch(?:ed)?|loading|departure|sailing)\b|\betd\b|\bcopy bill\b|\bbill\b/;
const B2_DELIVERY_RE = /\bdeliver|\breceipt of (the )?goods|\bgoods received|\bwarehouse/;
const B3_ARRIVAL_RE = /\barriv|\beta\b|\bdestination|\blanding|\blanded|\bdischarge|\bport of/;
const B4_ORDER_RE = /\bin advance|\bupfront|\bprepay|\bwith (the )?order\b|\b(on|upon) (the )?order\b/;
const B7_INVOICE_RE = /\b(after|from) (the )?(date of )?(the )?(pi|invoice|proforma)\b|\binvoice date|\bpi date|\bpi issued/;
const B5_BARE_RE = /\bdispatch|\bshipment|\bshipping|\bloading/;
const NET_RE = /\bnet\s*(\d{1,3})(?:\s*days?)?\b/;
const OFFSET_RE = /(\d{1,3})\s*(day|week|month)s?\b/;
// A clause about another event: its "N days" is that event's timing, never a
// payment offset.
const OTHER_EVENT_RE = /\bvalidity\b|\bquotation|\bquote[ds]?\b|\blead[- ]?times?\b|\bdelivery[- ]times?\b|\bproduction[- ]times?\b/;
// A deposit the terms say there is none of: 0 %, never "payment in advance".
const NO_DEPOSIT_RE = /\bno (deposit|down ?payment|advance payment|prepayment)s?\b|\b(deposit|down ?payment|advance payment|prepayment)s? (is |are )?not (required|needed|necessary)\b|\bwithout (a |any )?(deposit|down ?payment|advance payment|prepayment)\b/;

function triggerOf(text, notes) {
    if (B0_BEFORE_RE.test(text) || B0_READY_RE.test(text)) return { trigger: 'before_dispatch', netDays: null };
    if (B1_BL_RE.test(text)) return { trigger: 'bl', netDays: null };
    if (B2_DELIVERY_RE.test(text)) return { trigger: 'delivery', netDays: null };
    if (B3_ARRIVAL_RE.test(text)) return { trigger: 'arrival', netDays: null };
    if (B4_ORDER_RE.test(text)) return { trigger: 'order', netDays: null };
    if (B7_INVOICE_RE.test(text)) return { trigger: 'invoice', netDays: null };
    if (B5_BARE_RE.test(text)) { notes.push('bare_keyword'); return { trigger: 'before_dispatch', netDays: null }; }
    const net = NET_RE.exec(text);
    if (net) { notes.push('net_anchor_assumed'); return { trigger: NET_TERMS_ANCHOR, netDays: Number(net[1]) }; }
    return { trigger: 'unknown', netDays: null };
}

function offsetOf(text) {
    const other = OTHER_EVENT_RE.exec(text);
    const m = OFFSET_RE.exec(other ? text.slice(0, other.index) : text);
    if (!m) return 0;
    const n = Number(m[1]);
    const unit = m[2] === 'week' ? 7 : m[2] === 'month' ? 30 : 1;
    const after = text.slice(m.index + m[0].length);
    const negative = /^\s*(before|prior)/.test(after);
    return negative ? -n * unit : n * unit;
}

function splitClauses(s) {
    const rough = s.split(/[,;\n+]|\.(?!\d)|\band\b|\bthen\b/).map(c => c.trim()).filter(Boolean);
    const out = [];
    for (const c of rough) {
        const marks = [...c.matchAll(PCT_RE)].map(m => m.index ?? 0);
        if (marks.length <= 1) { out.push(c); continue; }
        let start = 0;
        for (let i = 1; i < marks.length; i++) {
            out.push(c.slice(start, marks[i]).trim());
            start = marks[i];
        }
        out.push(c.slice(start).trim());
    }
    return out.filter(Boolean);
}

const EMPTY_RULE = {
    depositPct: null, balanceTrigger: 'unknown', balanceOffsetDays: 0, source: 'none', raw: null, confidence: 'none', notes: [],
};

/** Deterministic free-text → rule. `source` is stamped by resolveTermsRule. */
function parsePaymentTerms(raw) {
    const text = raw == null ? '' : String(raw);
    if (!text.trim()) return { ...EMPTY_RULE, raw: null };
    let s = normaliseTerms(text);
    const notes = [];
    let partial = false;
    let depositPct = null;
    let shorthandBalance = null;

    // JFPRO house shorthand: "30D/70B BOL", "100D", "100B BOL", "50D/50B Before dispatch".
    const sh = /(?:^|\s)(\d{1,3})\s*d\b(?:\s*\/\s*(\d{1,3})\s*b\b)?/.exec(s);
    if (sh) {
        depositPct = Number(sh[1]);
        if (sh[2] != null) shorthandBalance = Number(sh[2]);
        s = s.replace(sh[0], ' ');
    }
    const shb = /(?:^|\s)(\d{1,3})\s*b\b/.exec(s);
    if (shb && shorthandBalance == null) {
        shorthandBalance = Number(shb[1]);
        s = s.replace(shb[0], ' ');
        if (depositPct == null) depositPct = Math.max(0, 100 - shorthandBalance);
    }
    s = s.replace(/\s+/g, ' ').trim();

    const clauses = splitClauses(s).map((c) => {
        const pctMatch = /(\d+(?:\.\d+)?)%/.exec(c);
        let pct = pctMatch ? Number(pctMatch[1]) : null;
        if (pct != null && pct > 100) pct = null;
        return { text: c, pct, role: 'other', trigger: 'unknown', offset: 0, partialNotes: [] };
    });

    // A deposit the terms rule out ("Deposit is NOT required", "no deposit").
    let noDeposit = false;
    for (const c of clauses) {
        if (!NO_DEPOSIT_RE.test(c.text)) continue;
        noDeposit = true;
        c.text = c.text.replace(new RegExp(NO_DEPOSIT_RE.source, 'g'), ' ').replace(/\s+/g, ' ').trim();
    }

    // Orphan offset ("…, 30 days") merges into the preceding balance clause.
    for (let i = clauses.length - 1; i > 0; i--) {
        const c = clauses[i];
        if (c.pct == null && OFFSET_RE.test(c.text) && !OTHER_EVENT_RE.test(c.text) && !DEPOSIT_RE.test(c.text) && triggerOf(c.text, []).trigger === 'unknown') {
            clauses[i - 1].text += ' ' + c.text;
            clauses.splice(i, 1);
        }
    }

    const anyPct = clauses.some(c => c.pct != null) || depositPct != null;
    for (const c of clauses) {
        if (DEPOSIT_RE.test(c.text) && !(c.pct === 100 && B0_BEFORE_RE.test(c.text))) {
            c.role = 'deposit';
            continue;
        }
        const t = triggerOf(c.text, c.partialNotes);
        if (t.trigger !== 'unknown') {
            c.role = 'balance';
            c.trigger = t.trigger;
            c.offset = t.netDays != null ? t.netDays : offsetOf(c.text);
        }
    }

    // Deposit percentage from deposit clauses (several stages → summed).
    const depositClauses = clauses.filter(c => c.role === 'deposit');
    const depositPcts = depositClauses.map(c => c.pct).filter(p => p != null);
    if (depositPcts.length) {
        depositPct = depositPcts.reduce((a, b) => a + b, 0);
        if (depositPcts.length > 1) { notes.push('multi_stage'); partial = true; }
    } else if (depositClauses.length && !anyPct) {
        depositPct = 100; // "payment in advance"
    }
    if (depositPct == null && noDeposit) depositPct = 0;

    // An unclassified pct clause ahead of a balance clause is the deposit stage.
    const balanceClauses = clauses.filter(c => c.role === 'balance');
    if (depositPct == null && balanceClauses.length) {
        const firstBalanceIdx = clauses.indexOf(balanceClauses[0]);
        const lead = clauses.slice(0, firstBalanceIdx).find(c => c.role === 'other' && c.pct != null && c.pct < 100);
        if (lead) { depositPct = lead.pct; lead.role = 'deposit'; notes.push('position_deposit'); }
    }

    // Balance trigger: first balance clause wins.
    let trigger = 'unknown';
    let offset = 0;
    if (balanceClauses.length) {
        trigger = balanceClauses[0].trigger;
        offset = balanceClauses[0].offset;
        for (const n of balanceClauses[0].partialNotes) { notes.push(n); partial = true; }
        const distinct = new Set(balanceClauses.map(c => `${c.trigger}:${c.offset}`));
        if (distinct.size > 1) { notes.push('multi_stage'); partial = true; }
    }

    // Percent bookkeeping.
    const balancePcts = balanceClauses.map(c => c.pct).filter(p => p != null);
    const balanceSum = shorthandBalance != null ? shorthandBalance : balancePcts.reduce((a, b) => a + b, 0);
    if (depositPct == null) {
        if (balancePcts.some(p => p === 100)) depositPct = 0;
        else if (balancePcts.length) depositPct = Math.max(0, 100 - balanceSum);
        else if (!anyPct && balanceClauses.length === 1 && !BALANCE_WORDS_RE.test(balanceClauses[0].text)) depositPct = 0;
        else if (balanceClauses.length) { notes.push('split_unknown'); partial = true; }
    } else if ((balancePcts.length || shorthandBalance != null) && Math.abs(depositPct + balanceSum - 100) > 1) {
        notes.push('pct_mismatch'); partial = true;
    }

    if (depositPct != null && depositPct >= 100) {
        depositPct = 100;
        trigger = 'order';
        offset = 0;
    } else if (depositPct != null && trigger === 'unknown') {
        notes.push('deposit_only'); partial = true;
    }

    if (depositPct == null && trigger === 'unknown') {
        if (/\blc\b/.test(s)) notes.push('lc_terms');
        return { ...EMPTY_RULE, raw: text, notes };
    }
    return {
        depositPct,
        balanceTrigger: trigger,
        balanceOffsetDays: offset,
        source: 'none',
        raw: text,
        confidence: partial || depositPct == null ? 'partial' : 'parsed',
        notes,
    };
}

const CONFIDENCE_RANK = { parsed: 2, partial: 1, none: 0 };

/**
 * Best-understood source wins: a fully parsed reading beats a partial one
 * whichever side it came from; on a tie the newest PI's own terms (dueTerms,
 * then rawTermsText) beat the JFPRO supplier default. `depositPct` is
 * back-filled from the PI's extracted depositPercentage when the text gave no
 * split. `newestPi` = { dueTerms, rawTermsText, depositPercentage } | null.
 */
function resolveTermsRule(newestPi, supplierTerms) {
    const newest = newestPi ?? null;
    const candidates = [];
    if (newest) {
        for (const text of [newest.dueTerms, newest.rawTermsText]) {
            const r = parsePaymentTerms(text);
            if (r.confidence !== 'none') candidates.push({ ...r, source: 'invoice' });
        }
    }
    const fromSupplier = parsePaymentTerms(supplierTerms);
    if (fromSupplier.confidence !== 'none') candidates.push({ ...fromSupplier, source: 'supplier' });
    // Stable sort: equal confidence keeps the PI ahead of the supplier default.
    let rule = candidates.sort((a, b) => CONFIDENCE_RANK[b.confidence] - CONFIDENCE_RANK[a.confidence])[0] ?? null;
    if (!rule) {
        rule = { ...EMPTY_RULE, raw: newest?.dueTerms ?? newest?.rawTermsText ?? supplierTerms ?? null };
    }
    const extractedPct = newest?.depositPercentage;
    if (rule.depositPct == null && extractedPct != null && extractedPct > 0 && extractedPct <= 100) {
        rule = {
            ...rule,
            depositPct: extractedPct,
            source: rule.source === 'none' ? 'invoice' : rule.source,
            confidence: rule.confidence === 'parsed' ? 'parsed' : 'partial',
            notes: [...rule.notes, 'pct_backfilled'],
        };
    }
    return rule;
}

/** Punctuation-blind, single-spaced, lower-cased. "Suzhou Sunmed Co.,Ltd." ≡ "Suzhou Sunmed Co., Ltd". */
const looseName = s => String(s ?? '').trim().toLowerCase().replace(/[^a-z0-9一-鿿]+/g, ' ').trim();

/**
 * The company rule's fixed deposit % for a supplier (resolvePolicy's
 * `depositPct`): the supplier's own payment rule, else the default rule, else
 * null. `rules` are payment_rules rows ({ scope, supplier_name, deposit_pct }).
 */
function ruleDepositPct(rules, supplierNames) {
    const num = v => (v == null ? null : Number(v));
    const def = (rules || []).find(r => r.scope === 'default') ?? null;
    const keys = (supplierNames || []).map(looseName).filter(Boolean);
    const sup = (rules || []).find(r => r.scope === 'supplier' && r.supplier_name && keys.includes(looseName(r.supplier_name))) ?? null;
    return num(sup?.deposit_pct) ?? num(def?.deposit_pct) ?? null;
}

/**
 * The deposit % in force for one purchase order: a company rule's fixed %
 * over the terms (the newest PI's, else the supplier's JFPRO default). Null
 * when nothing says.
 */
function depositPctInForce({ rules, supplierNames, newestPi, supplierTerms }) {
    return ruleDepositPct(rules, supplierNames) ?? resolveTermsRule(newestPi, supplierTerms).depositPct ?? null;
}

module.exports = {
    parsePaymentTerms, resolveTermsRule, looseName, ruleDepositPct, depositPctInForce,
};
