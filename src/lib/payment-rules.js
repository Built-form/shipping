'use strict';

// Payment rules: parsing a request body and serialising a row. Pure — the
// routes and the schema live in src/handlers/orders.js ("Payment rules").

const PAYMENT_RULE_DEPOSIT_TRIGGERS = ['po_sent', 'artwork_confirmed', 'pi_uploaded', 'pi_signed'];
const PAYMENT_RULE_BALANCE_TRIGGERS = ['terms', 'before_dispatch', 'bl', 'telex_release', 'container_document', 'arrival', 'delivery', 'invoice'];

// Each estimate step counts from one event; the anchors allowed per step keep
// the chain acyclic (artwork cannot count from ready, ready cannot count from
// arrival…). Transit is per mode from the ETD.
const PAYMENT_RULE_ESTIMATE_STEPS = {
    artwork: ['po', 'pi', 'pi_signed'],
    pi: ['po', 'artwork'],
    piSigned: ['pi', 'po', 'artwork'],
    ready: ['po', 'pi', 'pi_signed', 'artwork', 'deposit_paid'],
    telex: ['bl', 'etd', 'arrival'],
    document: ['bl', 'etd', 'arrival'],
};
const PAYMENT_RULE_FREIGHT_MODES = ['sea', 'air', 'road'];
const EMPTY_PAYMENT_RULE_ESTIMATES = () => ({
    artwork: null, pi: null, piSigned: null, ready: null, telex: null, document: null,
    transit: { sea: null, air: null, road: null },
});

// { error } or { value: estimates } — always the full shape, nulls for unset.
function parsePaymentRuleEstimates(input) {
    const out = EMPTY_PAYMENT_RULE_ESTIMATES();
    if (input == null) return { value: out };
    if (typeof input !== 'object') return { error: 'estimates must be an object.' };
    const days = (v, name) => {
        const n = Number(v);
        return Number.isInteger(n) && n >= 0 && n <= 365 ? { value: n } : { error: `${name} must be a whole number of days between 0 and 365.` };
    };
    for (const [key, anchors] of Object.entries(PAYMENT_RULE_ESTIMATE_STEPS)) {
        const step = input[key];
        if (step == null || step === '') continue;
        if (typeof step !== 'object') return { error: `estimates.${key} must be { from, days }.` };
        if (!anchors.includes(step.from)) return { error: `estimates.${key}.from must be one of: ${anchors.join(', ')}.` };
        const d = days(step.days, `estimates.${key}.days`);
        if (d.error) return { error: d.error };
        out[key] = { from: step.from, days: d.value };
    }
    const transit = input.transit;
    if (transit != null) {
        if (typeof transit !== 'object') return { error: 'estimates.transit must be { sea, air, road }.' };
        for (const mode of PAYMENT_RULE_FREIGHT_MODES) {
            const v = transit[mode];
            if (v == null || v === '') continue;
            const d = days(v, `estimates.transit.${mode}`);
            if (d.error) return { error: d.error };
            out.transit[mode] = d.value;
        }
    }
    return { value: out };
}

function parseStoredEstimates(raw) {
    if (!raw) return EMPTY_PAYMENT_RULE_ESTIMATES();
    try {
        const parsed = typeof raw === 'string' ? JSON.parse(raw) : raw;
        return parsePaymentRuleEstimates(parsed).value ?? EMPTY_PAYMENT_RULE_ESTIMATES();
    } catch {
        return EMPTY_PAYMENT_RULE_ESTIMATES();
    }
}

function paymentRuleRowToJson(r) {
    if (!r) return null;
    return {
        id: r.id,
        scope: r.scope,
        supplierName: r.scope === 'supplier' ? r.supplier_name : null,
        supplierLabel: r.supplier_label || null,
        depositPct: r.deposit_pct != null ? Number(r.deposit_pct) : null,
        depositTrigger: r.deposit_trigger || null,
        depositGraceDays: Number(r.deposit_grace_days) || 0,
        balanceTrigger: r.balance_trigger || null,
        balanceDocumentType: r.balance_document_type || null,
        balanceOffsetDays: r.balance_offset_days != null ? Number(r.balance_offset_days) : null,
        balanceGraceDays: Number(r.balance_grace_days) || 0,
        depositOffsetDays: r.deposit_offset_days != null ? Number(r.deposit_offset_days) : null,
        estimates: parseStoredEstimates(r.estimates_json),
        // The start date is company-wide: only the default rule carries one.
        airOwedFrom: r.scope === 'default' && r.air_owed_from ? String(r.air_owed_from).slice(0, 10) : null,
        airLimitDays: r.air_limit_days != null ? Number(r.air_limit_days) : null,
        notes: r.notes || null,
        updatedByEmail: r.updated_by_email || null,
        createdAt: r.created_at?.toISOString?.() ?? r.created_at,
        updatedAt: r.updated_at?.toISOString?.() ?? r.updated_at,
    };
}

// Validates and normalises a rule body. Returns { error } or { row }.
function parsePaymentRuleBody(body) {
    const b = body && typeof body === 'object' ? body : {};
    const scope = b.scope === 'supplier' ? 'supplier' : b.scope === 'default' ? 'default' : null;
    if (!scope) return { error: 'scope must be "default" or "supplier".' };
    const label = typeof b.supplierName === 'string' ? b.supplierName.trim() : '';
    if (scope === 'supplier' && !label) return { error: 'supplierName is required for a supplier rule.' };
    const intOrNull = (v, name, min, max) => {
        if (v == null || v === '') return { value: null };
        const n = Number(v);
        if (!Number.isInteger(n) || n < min || n > max) return { error: `${name} must be a whole number between ${min} and ${max}.` };
        return { value: n };
    };
    const pct = b.depositPct == null || b.depositPct === '' ? { value: null } : (() => {
        const n = Number(b.depositPct);
        return Number.isFinite(n) && n >= 0 && n <= 100 ? { value: n } : { error: 'depositPct must be between 0 and 100.' };
    })();
    if (pct.error) return { error: pct.error };
    const depositTrigger = b.depositTrigger == null || b.depositTrigger === '' ? null : String(b.depositTrigger);
    if (depositTrigger && !PAYMENT_RULE_DEPOSIT_TRIGGERS.includes(depositTrigger)) return { error: `depositTrigger must be one of: ${PAYMENT_RULE_DEPOSIT_TRIGGERS.join(', ')}.` };
    const balanceTrigger = b.balanceTrigger == null || b.balanceTrigger === '' ? null : String(b.balanceTrigger);
    if (balanceTrigger && !PAYMENT_RULE_BALANCE_TRIGGERS.includes(balanceTrigger)) return { error: `balanceTrigger must be one of: ${PAYMENT_RULE_BALANCE_TRIGGERS.join(', ')}.` };
    const docType = typeof b.balanceDocumentType === 'string' && b.balanceDocumentType.trim() ? b.balanceDocumentType.trim().slice(0, 64) : null;
    if (balanceTrigger === 'container_document' && !docType) return { error: 'balanceDocumentType is required when the balance is due on a container document.' };
    const depGrace = intOrNull(b.depositGraceDays, 'depositGraceDays', 0, 90);
    const balGrace = intOrNull(b.balanceGraceDays, 'balanceGraceDays', 0, 90);
    const offset = intOrNull(b.balanceOffsetDays, 'balanceOffsetDays', -180, 365);
    const depOffset = intOrNull(b.depositOffsetDays, 'depositOffsetDays', -180, 365);
    for (const r of [depGrace, balGrace, offset, depOffset]) if (r.error) return { error: r.error };
    const est = parsePaymentRuleEstimates(b.estimates);
    if (est.error) return { error: est.error };
    // Air freight delivered on/after this date stays owed until a payment is
    // recorded (company-wide, so default rule only); the limit is how many
    // days after delivery it must be paid by, per rule.
    const airOwedFrom = b.airOwedFrom == null || b.airOwedFrom === '' ? null : String(b.airOwedFrom).trim();
    if (airOwedFrom != null) {
        if (scope !== 'default') return { error: 'airOwedFrom is set on the default rule only.' };
        const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(airOwedFrom);
        const d = m ? new Date(Date.UTC(+m[1], +m[2] - 1, +m[3])) : null;
        if (!d || d.getUTCFullYear() !== +m[1] || d.getUTCMonth() !== +m[2] - 1 || d.getUTCDate() !== +m[3]) {
            return { error: 'airOwedFrom must be a date as YYYY-MM-DD.' };
        }
    }
    const airLimit = intOrNull(b.airLimitDays, 'airLimitDays', 1, 365);
    if (airLimit.error) return { error: airLimit.error };
    const hasEstimate = Object.entries(est.value).some(([k, v]) => (k === 'transit' ? Object.values(v).some(x => x != null) : v != null));
    return {
        row: {
            scope,
            supplier_name: scope === 'supplier' ? label.toLowerCase() : '',
            supplier_label: scope === 'supplier' ? label : null,
            deposit_pct: pct.value,
            deposit_trigger: depositTrigger,
            deposit_grace_days: depGrace.value ?? 0,
            balance_trigger: balanceTrigger,
            balance_document_type: balanceTrigger === 'container_document' ? docType : null,
            balance_offset_days: offset.value,
            balance_grace_days: balGrace.value ?? 0,
            deposit_offset_days: depOffset.value,
            estimates_json: hasEstimate ? JSON.stringify(est.value) : null,
            air_owed_from: airOwedFrom,
            air_limit_days: airLimit.value,
            notes: typeof b.notes === 'string' && b.notes.trim() ? b.notes.trim().slice(0, 500) : null,
        },
    };
}

module.exports = {
    PAYMENT_RULE_DEPOSIT_TRIGGERS,
    PAYMENT_RULE_BALANCE_TRIGGERS,
    parsePaymentRuleEstimates,
    parsePaymentRuleBody,
    paymentRuleRowToJson,
};
