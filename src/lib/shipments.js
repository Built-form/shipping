'use strict';

// Shipments — the pure half of the shipments entity: vocabulary, reference and
// name parsing, mode inference, stage derivation and the API shape. No DB, no
// pool, no logging.
//
// A shipment is one physical movement of goods (a sea container, an air
// consignment, a truck) through one lifecycle:
//
//   PLANNED -> DRAFT -> BOOKED -> IN_TRANSIT -> ARRIVED -> CLOSED   (or CANCELLED)
//
// Until authority flips (rollout step 4/5) the legacy columns and tables stay
// the source of truth and `shipments` is a derived, rebuildable shadow of them
// (src/services/shipment-sync.js). The sync service, the /api/v1/shipments
// routes, the backfill and the verify tool all parse references, infer modes and
// derive stages through this one module, so they can never disagree.

const { STATUS_LEVEL, isFqcOrder } = require('./order-transitions');

const MODES = ['SEA', 'AIR', 'ROAD'];
const STAGES = ['PLANNED', 'DRAFT', 'BOOKED', 'IN_TRANSIT', 'ARRIVED', 'CLOSED', 'CANCELLED'];
// Before booking: a shipment is a named allocation list mirroring a legacy
// planned / draft container, and holds its name as an open_key.
const OPEN_STAGES = ['PLANNED', 'DRAFT'];
// From booking on, in lifecycle order. Only these have member orders
// (orders.shipment_id) and only these compare by rank.
const BOOKED_STAGES = ['BOOKED', 'IN_TRANSIT', 'ARRIVED', 'CLOSED'];

// Order statuses that end an order's journey. PARTIALLY_RECEIVED is terminal:
// order-receive sets it only once an order is fully reconciled with a shortfall.
const TERMINAL_STATUSES = new Set(['RECEIVED', 'PARTIALLY_RECEIVED', 'DESTROYED']);
const TRANSIT_STATUSES = new Set(['ON_SEA', 'ON_AIR']);

// The order status a shipment's members move to when it departs. ROAD has no
// order status yet (ON_ROAD is deferred): a ROAD shipment going IN_TRANSIT
// stamps departed_at and leaves its orders CONSOLIDATED.
const TRANSIT_STATUS_BY_MODE = { SEA: 'ON_SEA', AIR: 'ON_AIR', ROAD: null };
const ARRIVED_STATUS = 'ARRIVED_AT_WAREHOUSE';

// Milestone column stamped when a shipment reaches each booked stage.
const MILESTONE_BY_STAGE = { BOOKED: 'booked_at', IN_TRANSIT: 'departed_at', ARRIVED: 'arrived_at', CLOSED: 'closed_at' };

// The two running reference sequences: sea containers are bare numbers ('308'),
// air consignments 'NN. Air Freight' ('121. Air Freight').
const SEA_REF_RE = /^(\d{1,4})$/;
const AIR_REF_RE = /^(\d{1,4})\.\s*Air Freight$/i;

// Carrier-reference shapes. AWB = 3-digit airline prefix + 8-digit serial; the
// container shape is the one the sea feeder registers with ShipsGo
// (shipsgo-containers.js CONTAINER_NUMBER_RE).
const AWB_RE = /^\d{3}-?\d{8}$/;
const CONTAINER_RE = /^[A-Z]{4}\d{7}$/i;

// Legacy draft / planned names: 'DRAFT-SEA-260917-173825 - 328',
// 'PLANNED-AIR-260914-145042', 'DRAFT-260610-160022 - Air Freight 2'. The stamp
// carries the mode when the SPA knew it; the label after it is free text.
const NAME_STAMP_RE = /^(DRAFT|PLANNED)(?:-(SEA|AIR|ROAD))?-\d{4,6}-\d{4,6}\s*(?:-\s*)?/i;

function clean(v) {
    if (v == null) return null;
    const s = String(v).trim();
    return s === '' ? null : s;
}

function isMode(v) { return MODES.includes(v); }
function isStage(v) { return STAGES.includes(v); }
function isOpenStage(v) { return OPEN_STAGES.includes(v); }
function isBookedStage(v) { return BOOKED_STAGES.includes(v); }

// 1..4 for the booked stages, 0 for anything else.
function stageRank(stage) {
    return BOOKED_STAGES.indexOf(stage) + 1;
}

// The later of two booked stages; a null/unknown side loses.
function laterStage(a, b) {
    return stageRank(b) > stageRank(a) ? b : a;
}

// The reference in orders.container_number: 'NN' (SEA), 'NN. Air Freight'
// (AIR), or anything else, which is kept verbatim but quarantined
// (seq null, known false) for review — never renumbered, so containerNumber and
// every consumer that reads it keep round-tripping.
function parseReference(raw) {
    const reference = clean(raw);
    if (!reference) return null;
    let m = reference.match(SEA_REF_RE);
    if (m) return { reference, mode: 'SEA', seq: Number(m[1]), known: true };
    m = reference.match(AIR_REF_RE);
    if (m) return { reference, mode: 'AIR', seq: Number(m[1]), known: true };
    return { reference, mode: null, seq: null, known: false };
}

// Canonical reference for a sequence number. ROAD has no sequence.
function formatReference(mode, seq) {
    if (mode === 'SEA') return String(seq);
    if (mode === 'AIR') return `${seq}. Air Freight`;
    return null;
}

// The stamp a legacy name starts with ('DRAFT-SEA-260917-173825'), uppercased,
// else null. The SPA mints it once per draft and a rename keeps it, so two
// names with one stamp are two names of the same draft.
function nameStamp(name) {
    const m = String(name || '').trim().match(/^(DRAFT|PLANNED)(?:-(SEA|AIR|ROAD))?-\d{4,6}-\d{4,6}/i);
    return m ? m[0].toUpperCase() : null;
}

// The mode a legacy name's stamp declares ('DRAFT-SEA-…' -> 'SEA'), else null.
function modeFromName(name) {
    const m = String(name || '').trim().match(NAME_STAMP_RE);
    return m && m[2] ? m[2].toUpperCase() : null;
}

// The reference number a draft / planned name reserves: the leading number of
// its label (the text after the stamp, else after the last ' - ').
//   'DRAFT-SEA-260917-173825 - 328'              -> { seq: 328, mode: 'SEA', reference: '328' }
//   '… - 316 ETD 6 Sep'                          -> 316
//   'DRAFT-AIR-260820-094327 - 121. Air Freight' -> { reference: '121. Air Freight' }
//   'DRAFT-AIR-260916-154300 - 123. Airfreight urgent' -> '123. Air Freight'
//   '… - TAM Container 2', 'Sunmed Container'    -> null
// `stampMode` is the mode the stamp itself declares (null when it declares none)
// so a caller can check it against the shipment a hint points at.
function parseNameHint(name) {
    const full = clean(name);
    if (!full) return null;
    const stamp = full.match(NAME_STAMP_RE);
    let label;
    if (stamp) {
        label = full.slice(stamp[0].length);
    } else {
        const i = full.lastIndexOf(' - ');
        if (i < 0) return null;
        label = full.slice(i + 3);
    }
    label = label.trim();
    const m = label.match(/^(\d{1,4})(?![\d])(\.\s*air\s*freight\b)?/i);
    if (!m) return null;
    // A bare number must end the token ('328', '316 ETD…', '124.' is fine too).
    const next = label.charAt(m[0].length);
    if (!m[2] && next && !/[\s.,;:)(\-]/.test(next)) return null;
    const seq = Number(m[1]);
    const stampMode = stamp && stamp[2] ? stamp[2].toUpperCase() : null;
    const mode = m[2] ? 'AIR' : (stampMode || 'SEA');
    if (mode === 'ROAD') return null;
    return { seq, mode, stampMode, reference: formatReference(mode, seq) };
}

function isAwbShape(ref) { return AWB_RE.test(String(ref || '').trim()); }
function isContainerShape(ref) { return CONTAINER_RE.test(String(ref || '').trim()); }

// Which orders column a carrier reference belongs in — by its SHAPE, not the
// shipment's mode. Only a true AWB goes to awb_number (the hourly air feeder
// registers every non-empty awb_number with ShipsGo, unfiltered, and
// registration is the billed event); everything else — ISO containers, UPS
// '1Z…' and FedEx numbers on air shipments — goes to external_container_number,
// which the sea feeder filters by shape.
function carrierColumnFor(ref) {
    return isAwbShape(ref) ? 'awb_number' : 'external_container_number';
}

// First hit wins: the reference pattern, then member statuses, then the
// tracking reference's shape. ROAD is never inferred.
//   members: [{ status }]
function inferMode({ reference, statuses = [], trackingRef = null } = {}) {
    const parsed = parseReference(reference);
    if (parsed && parsed.mode) return { mode: parsed.mode, source: 'reference' };
    const air = statuses.includes('ON_AIR');
    const sea = statuses.includes('ON_SEA');
    if (air !== sea) return { mode: air ? 'AIR' : 'SEA', source: 'status' };
    const ref = clean(trackingRef);
    if (ref && isAwbShape(ref)) return { mode: 'AIR', source: 'tracking' };
    if (ref && isContainerShape(ref)) return { mode: 'SEA', source: 'tracking' };
    return { mode: null, source: null };
}

// The one carrier reference for a set of member orders. Per order: AIR prefers
// awb_number then external_container_number, everything else the reverse.
// Unanimous across the members that carry one, else the most frequent (ties to
// the first seen) and not unanimous — which the backfill flags for review.
//   rows: [{ external_container_number, awb_number }]
function trackingRefFor(mode, rows = []) {
    const counts = new Map();
    const source = new Map();
    for (const r of rows) {
        const ext = clean(r.external_container_number);
        const awb = clean(r.awb_number);
        const [value, column] = mode === 'AIR'
            ? (awb ? [awb, 'awb_number'] : [ext, 'external_container_number'])
            : (ext ? [ext, 'external_container_number'] : [awb, 'awb_number']);
        if (!value) continue;
        counts.set(value, (counts.get(value) || 0) + 1);
        if (!source.has(value)) source.set(value, column);
    }
    if (!counts.size) return { trackingRef: null, sourceColumn: null, unanimous: true, candidates: [] };
    let best = null;
    for (const [value, n] of counts) if (best === null || n > counts.get(best)) best = value;
    return {
        trackingRef: best,
        sourceColumn: source.get(best),
        unanimous: counts.size === 1,
        candidates: [...counts.keys()],
    };
}

// The stage a shipment's live member orders justify, or null with no members:
// all terminal -> CLOSED; any arrived or terminal -> ARRIVED; any on the water /
// in the air -> IN_TRANSIT; else BOOKED.
function deriveStage(statuses = []) {
    if (!statuses.length) return null;
    if (statuses.every(s => TERMINAL_STATUSES.has(s))) return 'CLOSED';
    if (statuses.some(s => s === ARRIVED_STATUS || TERMINAL_STATUSES.has(s))) return 'ARRIVED';
    if (statuses.some(s => TRANSIT_STATUSES.has(s))) return 'IN_TRANSIT';
    return 'BOOKED';
}

// The stage the API shows: the later of the stored stage (written only by
// explicit shipment-level actions) and the stage the members justify. Neither
// half alone is right — deriving only would downgrade a ROAD shipment whose
// orders stay CONSOLIDATED; storing only goes stale because the ShipsGo sync and
// the receive paths move orders without touching shipments. Because the stored
// half is never ratcheted from derived values, a human correcting order
// statuses backwards self-heals. Open and cancelled stages are never derived.
function effectiveStage(stored, derived) {
    if (!isBookedStage(stored)) return stored;
    return laterStage(stored, derived);
}

// The order status a shipment stage fans out to, or null for none.
function statusForStage(stage, mode) {
    if (stage === 'IN_TRANSIT') return TRANSIT_STATUS_BY_MODE[mode] ?? null;
    if (stage === 'ARRIVED') return ARRIVED_STATUS;
    return null;
}

// Per-order forward-only filter for a stage fan-out: move iff the order's level
// is known, it has been packed (level >= 5), it is below the target, and it is
// not an FQC sample. One ineligible order never blocks the others.
//   order: { status, jf_code?, asin? }
function fanOutDecision(order, targetStatus) {
    const status = order && order.status;
    const level = Object.prototype.hasOwnProperty.call(STATUS_LEVEL, status) ? STATUS_LEVEL[status] : null;
    const target = STATUS_LEVEL[targetStatus];
    if (level == null) return { move: false, reason: 'status_not_in_pipeline' };
    if (isFqcOrder(order)) return { move: false, reason: 'fqc_sample' };
    if (level < 5) return { move: false, reason: 'not_packed' };
    if (level >= target) return { move: false, reason: 'already_at_or_past_target' };
    return { move: true, reason: null };
}

function iso(v) {
    if (v == null) return null;
    if (v instanceof Date) return Number.isNaN(v.getTime()) ? null : v.toISOString();
    return v;
}

function dateOnly(v) {
    if (v == null) return null;
    if (v instanceof Date) return Number.isNaN(v.getTime()) ? null : v.toISOString().slice(0, 10);
    return String(v).slice(0, 10);
}

function num(v) {
    if (v == null || v === '') return null;
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
}

// API shape of a shipments row. When the row comes from the effective-stage
// query (SHIPMENT_SELECT in shipment-sync.js) it carries derived_stage,
// effective_stage and the member aggregates; for a booked shipment the member
// orders' eta / vessel / carrier ref win over the stored copies (those exist
// for rollout step 4).
function rowToShipment(row) {
    if (!row) return null;
    const stored = row.stage;
    const derived = row.derived_stage !== undefined ? (row.derived_stage || null) : null;
    const stage = row.effective_stage || effectiveStage(stored, derived);
    const booked = isBookedStage(stored);
    const memberTracking = booked
        ? (row.mode === 'AIR'
            ? clean(row.member_awb) || clean(row.member_ext)
            : clean(row.member_ext) || clean(row.member_awb))
        : null;
    return {
        id: row.id,
        reference: row.reference || null,
        referenceSeq: row.reference_seq ?? null,
        name: row.name || null,
        mode: row.mode || null,
        modeSource: row.mode_source || null,
        stage,
        storedStage: stored,
        derivedStage: derived,
        trackingRef: memberTracking || row.tracking_ref || null,
        bookingRef: row.booking_ref || null,
        blNumber: row.bl_number || null,
        forwarder: row.forwarder || null,
        vesselName: (booked && clean(row.member_vessel)) || row.vessel_name || null,
        originPort: row.origin_port || null,
        etd: dateOnly(row.etd),
        eta: (booked && dateOnly(row.member_eta)) || dateOnly(row.eta),
        ata: dateOnly(row.ata),
        notes: row.notes || null,
        needsReview: !!Number(row.needs_review || 0),
        reviewNote: row.review_note || null,
        origin: row.origin || null,
        sourceDraftId: row.source_draft_id ?? null,
        mergedIntoId: row.merged_into_id ?? null,
        memberCount: num(row.member_count) || 0,
        lineCount: num(row.line_count) || 0,
        totalUnits: num(row.total_units) || 0,
        createdByEmail: row.created_by_email || null,
        createdAt: iso(row.created_at),
        updatedAt: iso(row.updated_at),
        bookedAt: iso(row.booked_at),
        departedAt: iso(row.departed_at),
        arrivedAt: iso(row.arrived_at),
        closedAt: iso(row.closed_at),
        cancelledAt: iso(row.cancelled_at),
        cancelledReason: row.cancelled_reason || null,
        deletedAt: iso(row.deleted_at),
    };
}

// An order snapshot for audit_log without shipmentId. The read path adds
// shipmentId to every projected order on purpose, but it must never enter an
// order audit row: a 'create' / 'delete' row stores the whole snapshot, and the
// shadow sync writes orders.shipment_id after the route's audit has run.
function auditSnapshot(order) {
    if (!order || typeof order !== 'object' || !('shipmentId' in order)) return order;
    const { shipmentId, ...rest } = order;
    return rest;
}

// A rejected shipment action, mapped to an HTTP response by the routes.
class ShipmentError extends Error {
    constructor(status, code, message, payload = {}) {
        super(message);
        this.status = status;
        this.code = code;
        this.payload = payload;
    }
}

// The compact entry GET /orders carries in its `shipments` side-map.
function shipmentSummary(s) {
    return {
        id: s.id,
        reference: s.reference,
        name: s.name,
        mode: s.mode,
        stage: s.stage,
        trackingRef: s.trackingRef,
        vesselName: s.vesselName,
        etd: s.etd,
        eta: s.eta,
        originPort: s.originPort,
        needsReview: s.needsReview,
    };
}

module.exports = {
    MODES,
    STAGES,
    OPEN_STAGES,
    BOOKED_STAGES,
    TERMINAL_STATUSES,
    TRANSIT_STATUSES,
    TRANSIT_STATUS_BY_MODE,
    ARRIVED_STATUS,
    MILESTONE_BY_STAGE,
    isMode,
    isStage,
    isOpenStage,
    isBookedStage,
    stageRank,
    laterStage,
    parseReference,
    formatReference,
    nameStamp,
    modeFromName,
    parseNameHint,
    isAwbShape,
    isContainerShape,
    carrierColumnFor,
    inferMode,
    trackingRefFor,
    deriveStage,
    effectiveStage,
    statusForStage,
    fanOutDecision,
    rowToShipment,
    shipmentSummary,
    auditSnapshot,
    ShipmentError,
    clean,
};
