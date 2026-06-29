'use strict';

// ── ShipLine order-movement rules, on the backend ───────────────────────────
//
// The AI status-suggestion system must only ever propose moves a NON-ADMIN
// operator could legitimately make in ShipLine — because the approve API applies
// whatever it's sent with NO transition validation ("the human is the control").
// So the rules that live in the ShipLine frontend (orderHelpers.ts:
// canTransitionStatus / getTransitionBlockers / isBlockedForMissingQcReport /
// isFqcOrder) are mirrored here, and the importer self-respects them.
//
// IMPORTANT — status VALUES. The movement-rules doc labels two stages
// "UNDER_PRODUCTION" and "READY_AT_FACTORY", but the live orders.status column
// stores them as IN_PRODUCTION and READY (confirmed: 0 rows of the doc names,
// 239 IN_PRODUCTION / 69 READY). We use the DB values throughout; the doc names
// are display aliases only.
//
// Admin bypasses are intentionally NOT modelled: suggestions always target the
// non-admin ruleset.

// Pipeline level per status (DB values). Same level => same stage.
const STATUS_LEVEL = {
    SCHEDULED: 0,
    PO_SENT: 1,
    IN_PRODUCTION: 2,        // doc: UNDER_PRODUCTION
    READY_FOR_QC: 3,
    READY: 4,               // doc: READY_AT_FACTORY
    CONSOLIDATED: 5,
    ON_SEA: 6,
    ON_AIR: 6,
    ARRIVED_AT_WAREHOUSE: 7,
    RECEIVED: 8,
    DESTROYED: 99,          // hidden; FQC samples (and QC-failed) only
};

// Canonical status occupying each forward level (ON_SEA is the default for the
// level-6 sea/air fork; freight intent overrides it in nextStepToward).
const STATUS_AT_LEVEL = {
    0: 'SCHEDULED', 1: 'PO_SENT', 2: 'IN_PRODUCTION', 3: 'READY_FOR_QC',
    4: 'READY', 5: 'CONSOLIDATED', 6: 'ON_SEA', 7: 'ARRIVED_AT_WAREHOUSE', 8: 'RECEIVED',
};

const CONTAINER_STAGES = new Set(['CONSOLIDATED', 'ON_SEA', 'ON_AIR']);

// Milestones a supplier email can plausibly assert (a subset of the pipeline,
// factory → freight). RECEIVED is warehouse-internal; DESTROYED is internal.
const INFERRABLE_MILESTONES = [
    'PO_SENT', 'IN_PRODUCTION', 'READY_FOR_QC', 'READY',
    'CONSOLIDATED', 'ON_SEA', 'ON_AIR', 'ARRIVED_AT_WAREHOUSE',
];

// An order whose CURRENT status is one of these is past the point a supplier
// email would advance — never a suggestion candidate.
const CANDIDATE_EXCLUDED_STATUSES = new Set([
    'ARRIVED_AT_WAREHOUSE', 'RECEIVED', 'PARTIALLY_RECEIVED', 'DESTROYED',
]);

function statusLevel(status) {
    return Object.prototype.hasOwnProperty.call(STATUS_LEVEL, status) ? STATUS_LEVEL[status] : null;
}

// FQC sample: asin OR jf_code ends with _FQC (case-insensitive). These are
// inspected then destroyed — never shipped, so they can't pass READY (level 4).
function isFqcOrder(order) {
    const ends = (v) => /_fqc$/i.test(String(v || ''));
    return ends(order && (order.jf_code ?? order.jfCode)) || ends(order && (order.asin ?? order.asinCode ?? order.asin));
}

// Non-admin transition rule (mirrors canTransitionStatus). `to === from` is a
// trivially-allowed no-op (callers treat that as "no change", not a suggestion).
function canTransition(from, to, { isFqc = false } = {}) {
    if (from === to) return true;
    const lf = statusLevel(from);
    const lt = statusLevel(to);
    if (lf == null || lt == null) return false;

    if (to === 'DESTROYED') {
        // FQC: from READY_FOR_QC or READY. Non-FQC: only from READY_FOR_QC.
        return isFqc ? (from === 'READY_FOR_QC' || from === 'READY') : (from === 'READY_FOR_QC');
    }
    // FQC can never enter a stage past READY (no container/shipping/warehouse).
    if (isFqc && lt > 4) return false;

    // Lateral sea ↔ air (same level 6).
    if (lf === 6 && lt === 6) return true;
    // Forward by exactly one level.
    if (lt === lf + 1) return true;
    // The only two permitted backward moves.
    if (from === 'CONSOLIDATED' && to === 'READY') return true;       // un-pack
    if (from === 'READY_FOR_QC' && to === 'IN_PRODUCTION') return true; // QC failed → rework
    // Server-only PARTIALLY_RECEIVED may always finish to RECEIVED.
    if (from === 'PARTIALLY_RECEIVED' && to === 'RECEIVED') return true;
    return false;
}

// Every status a non-admin could move `from` into right now.
function allowedNextStatuses(from, { isFqc = false } = {}) {
    return Object.keys(STATUS_LEVEL).filter(to => to !== from && canTransition(from, to, { isFqc }));
}

// Given the order's current status and the milestone an email asserts, return
// the suggested NON-ADMIN move and a category, or null target when no move fits.
//   freightHint: 'ON_AIR' | 'ON_SEA' — which freight the email implies (level 6).
// Categories:
//   forward            — milestone is exactly the next stage; propose it.
//   lateral            — sea↔air at the same stage.
//   backward_qc_failed — QC failed at READY_FOR_QC → back to IN_PRODUCTION.
//   multi_step         — email implies a FURTHER stage; propose only the next
//                        valid step and note the remaining path.
//   blocked_fqc        — FQC order whose next step would ship it; no move.
//   no_change          — order already at/past the milestone (and not lateral).
function suggestionFor(currentStatus, milestone, { isFqc = false, qcFailed = false, freightHint = null } = {}) {
    // QC-failed is the one supplier-asserted BACKWARD move.
    if (qcFailed && currentStatus === 'READY_FOR_QC') {
        return { target: 'IN_PRODUCTION', category: 'backward_qc_failed', remaining: [] };
    }

    const cl = statusLevel(currentStatus);
    const ml = statusLevel(milestone);
    if (cl == null || ml == null) return { target: null, category: 'no_change', remaining: [] };

    // Same level: lateral sea↔air is the only meaningful move.
    if (ml === cl) {
        if (cl === 6 && milestone !== currentStatus) {
            return { target: milestone, category: 'lateral', remaining: [] };
        }
        return { target: null, category: 'no_change', remaining: [] };
    }
    // Milestone is behind current (and not the QC-fail case) — nothing to do.
    if (ml < cl) return { target: null, category: 'no_change', remaining: [] };

    // Forward. The next single valid step is level cl+1.
    const nextLevel = cl + 1;
    let next = STATUS_AT_LEVEL[nextLevel];
    if (nextLevel === 6 && (freightHint === 'ON_AIR' || milestone === 'ON_AIR')) next = 'ON_AIR';

    // FQC can't be shipped — its next step past READY is blocked.
    if (isFqc && statusLevel(next) > 4) {
        return { target: null, category: 'blocked_fqc', remaining: [] };
    }

    // The stages still to traverse beyond `next` to reach the asserted milestone.
    const remaining = [];
    for (let lvl = nextLevel + 1; lvl <= ml; lvl++) {
        let s = STATUS_AT_LEVEL[lvl];
        if (lvl === 6 && (freightHint === 'ON_AIR' || milestone === 'ON_AIR')) s = 'ON_AIR';
        if (s) remaining.push(s);
    }

    return {
        target: next,
        category: ml === nextLevel ? 'forward' : 'multi_step',
        remaining,
    };
}

// ── Transition gate (required fields to move INTO a status) ──────────────────
// camelCase field names (the suggestion/UI vocabulary) → the orders column.
const FIELD_COLUMN = {
    poNumber: 'po_number', poDate: 'po_date', supplier: 'supplier', unitPrice: 'unit_price',
    artworkConfirmedDate: 'artwork_confirmed_date', lotNumber: 'lot_number',
    mfgDate: 'mfg_date', expDate: 'exp_date',
    estimatedReadyDate: 'estimated_ready_date', actualReadyDate: 'actual_ready_date',
    qcStatus: 'qc_status', qcDate: 'qc_date', qcInvoiceNumber: 'qc_invoice_number',
    containerNumber: 'container_number', externalContainerNumber: 'external_container_number',
    vesselName: 'vessel_name', eta: 'eta',
    estimatedDepartureDate: 'estimated_departure_date', shippedDate: 'shipped_date',
    deliveryDate: 'delivery_date', arrivedDate: 'arrived_date',
};

// Required gate fields to move INTO `target` (§3). READY's gate only applies
// when coming from READY_FOR_QC. Returns camelCase field names.
function gateFieldsFor(target, from) {
    switch (target) {
        case 'PO_SENT': return ['poNumber', 'poDate', 'supplier', 'unitPrice'];
        // The backward QC-failed rework (READY_FOR_QC -> IN_PRODUCTION) is NOT
        // gated (§3: backward moves are never gated); only a forward move into
        // IN_PRODUCTION requires the production setup fields.
        case 'IN_PRODUCTION': return from === 'READY_FOR_QC' ? [] : ['artworkConfirmedDate', 'lotNumber', 'mfgDate', 'expDate'];
        case 'READY_FOR_QC': return ['actualReadyDate'];
        case 'READY': return from === 'READY_FOR_QC' ? ['qcStatus', 'qcDate', 'qcInvoiceNumber'] : [];
        case 'DESTROYED': return ['qcStatus', 'qcDate', 'qcInvoiceNumber'];
        default: return []; // container stages: no field gate (Pack flow handles container details)
    }
}

function isBlank(v) {
    return v == null || (typeof v === 'string' && v.trim() === '');
}

// Fields a milestone email may legitimately fill/update when moving INTO a
// target (gate fields + the compliance/tracking values that milestone carries).
// Deliberately scoped per-target so an email can't update arbitrary order data —
// only what's pertinent to the move it asserts. poNumber is excluded (it's the
// match key, already on the order).
const APPLICABLE_FIELDS = {
    PO_SENT: ['poDate', 'supplier', 'unitPrice'],
    IN_PRODUCTION: ['artworkConfirmedDate', 'lotNumber', 'mfgDate', 'expDate'],
    READY_FOR_QC: ['actualReadyDate'],
    READY: ['qcStatus', 'qcDate', 'qcInvoiceNumber'],
    CONSOLIDATED: ['externalContainerNumber', 'vesselName', 'eta', 'estimatedDepartureDate'],
    ON_SEA: ['externalContainerNumber', 'vesselName', 'eta', 'estimatedDepartureDate', 'shippedDate'],
    ON_AIR: ['externalContainerNumber', 'vesselName', 'eta', 'estimatedDepartureDate', 'shippedDate'],
    ARRIVED_AT_WAREHOUSE: ['arrivedDate', 'deliveryDate'],
};

function applicableFieldsFor(target) {
    return APPLICABLE_FIELDS[target] || [];
}

// Stage-relevant DATA fields that aren't a gate for any move but an email may
// legitimately update at this point in the lifecycle — e.g. an estimated factory
// ready date while still in the factory stages, or a refreshed ETA in transit.
// These surface even when there's no status move (a 'data_update' suggestion).
function applicableDataFields(status) {
    const lvl = statusLevel(status);
    if (lvl == null) return [];
    if (lvl <= 4) return ['estimatedReadyDate'];                                    // SCHEDULED..READY (factory)
    // CONSOLIDATED / in-transit: a forwarder may supply the container/vessel after
    // the stage is already set, so allow those as data updates too (container →
    // external_container_number, the real shipment ref).
    if (lvl === 5) return ['externalContainerNumber', 'vesselName', 'estimatedDepartureDate', 'eta'];        // CONSOLIDATED (packed, awaiting departure)
    if (lvl === 6) return ['externalContainerNumber', 'vesselName', 'estimatedDepartureDate', 'shippedDate', 'eta', 'deliveryDate']; // ON_SEA/ON_AIR (in transit)
    if (lvl === 7) return ['eta', 'deliveryDate', 'arrivedDate'];                     // ARRIVED_AT_WAREHOUSE
    return [];
}

// Non-admins have mfg/exp snapped to the 1st of the month (§8). 'YYYY-MM-DD' →
// 'YYYY-MM-01'. Other dates keep their day.
function snapMonthStart(dateStr) {
    if (!dateStr) return dateStr;
    const m = String(dateStr).match(/^(\d{4})-(\d{2})/);
    return m ? `${m[1]}-${m[2]}-01` : dateStr;
}

// Which gate fields for `target` are missing on `order` (snake_case row), taking
// into account values the suggestion would APPLY (`applied`, camelCase). Adds the
// QC-report hard gate for READY (§3a) and the qcStatus=PASSED requirement.
function missingGateFields(order, target, from, { applied = {}, hasQcReport = true } = {}) {
    const missing = [];
    for (const f of gateFieldsFor(target, from)) {
        const col = FIELD_COLUMN[f];
        const val = applied[f] != null ? applied[f] : (order ? order[col] : null);
        if (isBlank(val)) { missing.push(f); continue; }
        // qcStatus must specifically be PASSED to enter READY (FAILED→DESTROYED only).
        if (f === 'qcStatus' && target === 'READY' && String(val).toUpperCase() !== 'PASSED') {
            missing.push('qcStatus=PASSED');
        }
        if (f === 'qcStatus' && target === 'DESTROYED') {
            const up = String(val).toUpperCase();
            if (up !== 'PASSED' && up !== 'FAILED') missing.push('qcStatus=PASSED|FAILED');
        }
    }
    // Hard gate: a QC inspection report must be attached to reach READY (§3a).
    if (target === 'READY' && hasQcReport === false) missing.push('qcReport(attachment)');
    return missing;
}

module.exports = {
    STATUS_LEVEL,
    STATUS_AT_LEVEL,
    CONTAINER_STAGES,
    INFERRABLE_MILESTONES,
    CANDIDATE_EXCLUDED_STATUSES,
    FIELD_COLUMN,
    statusLevel,
    isFqcOrder,
    canTransition,
    allowedNextStatuses,
    suggestionFor,
    gateFieldsFor,
    missingGateFields,
    applicableFieldsFor,
    applicableDataFields,
    snapMonthStart,
    isBlank,
};
