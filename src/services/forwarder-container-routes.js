'use strict';

// A forwarder's container page (docs/forwarder-containers.md): what ShipLine
// reads and decides about what a freight forwarder sees and sends on the
// supplier portal for a container.
//
//   GET  /api/v1/shipments/:id/forwarder-portal
//        everything the container card's FORWARDER tab shows: the forwarder
//        and where it comes from, its contacts, whether the container is
//        visible to them and why not, the emails sent about it, what we hold,
//        the forwarder's proposal for its number and dates, and the payments
//        sent to a forwarder that pay this container. Any signed-in user.
//   POST /api/v1/shipments/:id/forwarder-notice  { sendKey, to?, subject?, body? }
//        emails the forwarder's contacts the link to the container's page.
//   GET  /api/v1/forwarder-submissions[?status=all]
//        what forwarders sent and waits for our team, across containers.
//   GET  /api/v1/forwarder-submissions/summary
//        { toReview }: the count for the Freight tab's badge.
//   POST /api/v1/forwarder-details/:id/claim    { expected: { ecn, loadingDate, etd } }
//   POST /api/v1/forwarder-details/:id/finish   { claimToken, ecnReset? }
//   POST /api/v1/forwarder-details/:id/release  { claimToken }
//   POST /api/v1/forwarder-details/:id/reject   { note }
//        accepting, or sending back, a forwarder's proposal of a container's
//        number and dates (below).
// Emailing and deciding: admin and standard users.
//
// Every refusal carries `code`; a 409 that the page can act on also `why`,
// `fields` or `allowedNext`. A 403 is never answered to a role the read told
// it may not decide (`canDecide`): ShipLine treats a 403 as a sign-out.
//
// WHOSE CONTAINER IT IS is decided on rows read under a lock, never on the
// page's own read: lockOwnership takes the shipment row, then its live booking,
// and applies src/lib/forwarder-containers.js ownerOf to them. The routes that
// change ownership write those same rows (a forwarder picked by hand, a delete
// and a merge write the shipment; a booking attached, confirmed, declined or
// cancelled writes the booking, the portal's own confirmation included), so
// each is serialised against a decision here.
//
// ACCEPTING A PROPOSAL is claim, then (in the browser) the ECN, then finish:
//   claim   one transaction under the locks: shipment, live booking, member
//           orders, the container's proposal rows. The proposal is the live one
//           and waiting, no other accept is in progress on the container, it is
//           still that forwarder's, and it holds exactly what the card showed.
//           Only then is anything written anywhere.
//   ECN     ShipLine writes it through its own guard (prepareEcnWrite), which
//           runs only in the browser. That write is not fenced by this server.
//   finish  one transaction under the same locks: the ECN is on every member
//           order, nothing else moved since the claim, the dates are written
//           through PATCH /shipments/:id's own code, and the proposal is marked
//           accepted. Refused, nothing of it is written.
// Inside those transactions the FIRST statement is a locking read, and every
// plain read comes after the locks: under REPEATABLE READ a plain read made
// first would fix the snapshot before the locks were held.
//
// Registered from orders.js; authed through the greedy /api/v1/{proxy+} route.

const crypto = require('crypto');
const FC = require('../lib/forwarder-containers');
const F = require('../lib/freight-quotes');
const N = require('../lib/payment-notices');
const S = require('../lib/shipments');
const poPortalEmail = require('../lib/po-portal-email');
const { jportalSchema } = require('../lib/jportal');
const { escapeHtml, wrapEmail } = require('../lib/email-layout');
const { FREIGHT_CONTAINER_TEMPLATE_KEY } = require('../lib/email-template-defaults');
const sync = require('./shipment-sync');
const { readUploads, readDocuments, listUploads } = require('./forwarder-upload-routes');
const { sendFrontMessage: defaultSend } = require('./front-send');
const { preapprovePortalAddresses: defaultPreapprove } = require('./portal-access');
const { readForwarderEvidence: defaultEvidence } = require('./payment-notices');
const { logSentEmail: defaultLogSentEmail } = require('../lib/email-receipt');
const defaultLog = require('../lib/logger');

const ER_NO_SUCH_TABLE = 1146;
const ER_BAD_DB = 1049;
const ER_LOCK_DEADLOCK = 1213;
const ISO = "'%Y-%m-%dT%H:%i:%sZ'";
const EMAIL_TYPE = 'freight_container_details';
const NOTICE_TABLE = 'freight_container_notices';
const NOT_MIGRATED = { error: 'The forwarder container page is not set up on this server yet: its tables have not been migrated.', code: 'NOT_MIGRATED' };
const LIST_LIMIT = 200;

// What ShipLine says for each reason nobody sees a container.
const WHY_TEXT = Object.freeze({
    gone: 'This container no longer exists.',
    not_real: 'This is not a real container yet: it shows to a forwarder once it is booked.',
    booking_not_confirmed: 'Its booking is still waiting for the forwarder to confirm.',
    no_forwarder: 'No forwarder is on this container.',
    forwarder_not_listed: 'Its forwarder is typed text, not one from the Forwarders list.',
    forwarder_removed: 'Its forwarder has been removed from the Forwarders list.',
});

// What a refusal of a claim, a finish or a send-back says.
const REFUSAL_TEXT = Object.freeze({
    NO_SUCH_PROPOSAL: 'That proposal no longer exists.',
    CLAIM_IN_PROGRESS: 'Someone is accepting a proposal for this container right now. Try again in a moment.',
    ALREADY_DECIDED: 'This proposal was already accepted or sent back.',
    SUPERSEDED: 'The forwarder has sent a newer proposal since this one was shown. Reload to see it.',
    NOT_THEIR_CONTAINER: 'This container is no longer that forwarder’s, so their proposal cannot be accepted. It can be dismissed.',
    NOT_THEIR_DUTY: 'This forwarder no longer does the part of the job this is about, so it cannot be accepted. It can be dismissed.',
    CONTAINER_CHANGED: 'The container has changed since this was shown. Reload and look again.',
    NO_ORDERS: 'This container has no orders to carry what is proposed.',
    BAD_PROPOSAL: 'The number proposed is not a valid one. Send it back.',
    EMPTY_PROPOSAL: 'This proposal asks for nothing. Send it back.',
    DATES_OUT_OF_ORDER: 'The dates would be out of order: loading, then departure, then delivery. Send it back, or set the dates by hand.',
    CLIENT_TOO_OLD: 'This page is older than what the forwarder sent, so it cannot show all of it. Reload ShipLine, then decide.',
    NOT_ON_CONTAINER: 'The proposed container number is not on every order of this container, so the proposal is not accepted yet.',
    CLAIM_LOST: 'This accept is no longer in progress (it took too long, or was started again elsewhere). Reload and accept again.',
});
const ALLOWED_NEXT = Object.freeze({
    NO_SUCH_PROPOSAL: ['reload'], CLAIM_IN_PROGRESS: ['reload'], ALREADY_DECIDED: ['reload'], SUPERSEDED: ['reload'],
    NOT_THEIR_CONTAINER: ['reject'], NOT_THEIR_DUTY: ['reject'], CONTAINER_CHANGED: ['reload'], NO_ORDERS: ['reject'], BAD_PROPOSAL: ['reject'], EMPTY_PROPOSAL: ['reject'],
    DATES_OUT_OF_ORDER: ['reject'], NOT_ON_CONTAINER: ['reload'], CLAIM_LOST: ['reload'], CLIENT_TOO_OLD: ['reload'],
});

class Fail extends Error {
    constructor(status, code, error, extra) {
        super(error);
        this.status = status;
        this.code = code;
        this.error = error;
        this.extra = extra || null;
    }
}
const refuse = (code, extra = {}) => new Fail(code === 'NO_SUCH_PROPOSAL' ? 404 : 409, code, REFUSAL_TEXT[code] || code, { allowedNext: ALLOWED_NEXT[code] || ['reload'], ...extra });
// What a NOT_THEIR_CONTAINER or NOT_THEIR_DUTY refusal of a proposal carries: why the
// duty it needs has no holder (null when another company holds it), and the fields that
// are no longer its forwarder's.
function notTheirs(refusal, duties, row) {
    if (refusal.code !== 'NOT_THEIR_CONTAINER' && refusal.code !== 'NOT_THEIR_DUTY') return {};
    const requested = row ? FC.proposalOf(row.requested_json) : {};
    const needed = [...new Set(Object.keys(requested).map(f => FC.FIELD_DUTY[f]).filter(Boolean))];
    return { why: whyNot(duties, needed), ...(refusal.fields ? { fields: refusal.fields } : {}) };
}

const jsonList = (v) => {
    if (Array.isArray(v)) return v;
    if (v == null || v === '') return [];
    try { const out = JSON.parse(v); return Array.isArray(out) ? out : []; } catch { return []; }
};
const jsonObject = (v) => {
    if (v && typeof v === 'object' && !Array.isArray(v)) return v;
    if (typeof v !== 'string' || v === '') return {};
    try { const out = JSON.parse(v); return out && typeof out === 'object' && !Array.isArray(out) ? out : {}; } catch { return {}; }
};
const portalMissing = e => Boolean(e) && (e.errno === ER_NO_SUCH_TABLE || e.errno === ER_BAD_DB);

const noticeJson = n => ({
    id: Number(n.id),
    status: n.status,
    forwarderId: n.forwarder_id == null ? null : Number(n.forwarder_id),
    sentTo: jsonList(n.sent_to),
    subject: n.subject || null,
    sentByEmail: n.sent_by_email || null,
    createdAt: n.created_at_iso || null,
    sentAt: n.sent_at_iso || null,
});

// ── reads and locks, on a connection ─────────────────────────────────────

// `locking`: read the rows with a shared lock. Inside a decision's transaction
// that is what keeps this from being a plain read (which would fix the
// transaction's snapshot before its later locks are held), and it makes a
// forwarder being removed from the list wait for the decision too.
async function forwardersById(conn, ids, { locking = false } = {}) {
    const wanted = [...new Set(ids.filter(id => id != null).map(Number))];
    if (!wanted.length) return new Map();
    const [rows] = await conn.query(`SELECT id, name, deleted_at FROM forwarders WHERE id IN (?)${locking ? ' ORDER BY id FOR SHARE' : ''}`, [wanted]);
    return new Map(rows.map(r => [Number(r.id), r]));
}

async function contactsOf(conn, forwarderId) {
    if (forwarderId == null) return [];
    const [rows] = await conn.query(
        'SELECT id, name, email FROM forwarder_contacts WHERE forwarder_id = ? AND deleted_at IS NULL ORDER BY id ASC',
        [forwarderId]
    );
    return rows.map(r => ({ name: r.name || null, email: r.email }));
}

// The live bookings of a shipment row as getShipment returns it (at most one
// that covers the freight, and one for the delivery alone), read as dutiesOf
// reads rows.
const bookingsOfRow = row => [
    ...(row && row.bk_id != null && Number(row.bk_count) > 0
        ? [{ id: row.bk_id, status: row.bk_status, scope: row.bk_scope, booking_ref: row.bk_ref, forwarder_id: row.bk_forwarder_id }]
        : []),
    ...(row && row.dbk_id != null
        ? [{ id: row.dbk_id, status: row.dbk_status, scope: 'delivery', booking_ref: row.dbk_ref, forwarder_id: row.dbk_forwarder_id }]
        : []),
];
// Every forwarder a container's rows name: its three picks and its bookings' forwarders.
const namedForwarders = (shipment, bookings) => [
    shipment && shipment.forwarder_id, shipment && shipment.delivery_forwarder_id, shipment && shipment.customs_forwarder_id,
    ...(bookings || []).map(b => b.forwarder_id),
];
const liveIn = forwarders => (id) => { const f = forwarders.get(Number(id)); return Boolean(f) && !f.deleted_at; };
// The view's rows that make a container a forwarder's in full: the freight holder's
// ('booking', 'container') and, since 2026-11-24, a delivery or customs holder's ('duty').
const FULL_SOURCES_SQL = "'booking', 'container', 'duty'";
const { dutiesOfFlags } = FC;
// Why a forwarder cannot have something of its own accepted: the reason the duty it needs has
// no holder (null when another company holds it). `needed`: the duties its fields or file need.
const whyNot = (duties, needed) => {
    for (const duty of (needed.length ? needed : FC.DUTIES)) {
        const h = duties[duty];
        if (h && h.forwarderId == null && h.why) return h.why;
    }
    return null;
};

/**
 * Who holds each duty of a container, on rows read UNDER A LOCK: the shipment
 * row (which carries the three picks), then EVERY live booking of it, then
 * the forwarders any of them names (shared: the picks' and the bookings').
 * Every read here is a locking read. Call inside a transaction, as its FIRST
 * statement.
 * → { shipment, bookings, duties, forwarders, booking, owner, forwarder }: the
 *   last three are the freight duty's (the freight booking, ownerOf's answer
 *   and the freight holder's row), as before 2026-11-24.
 */
async function lockOwnership(conn, shipmentId) {
    const [ships] = await conn.query(
        `SELECT id, reference, mode, stage, deleted_at, merged_into_id, forwarder_id, forwarder, delivery_forwarder_id, customs_forwarder_id
           FROM shipments WHERE id = ? FOR UPDATE`,
        [shipmentId]
    );
    const shipment = ships[0] || null;
    const [bookings] = await conn.query(
        `SELECT fb.id, fb.status, fb.scope, fb.booking_ref, fr.forwarder_id
           FROM freight_bookings fb
           JOIN freight_quote_requests fr ON fr.id = fb.request_id
          WHERE fb.shipment_id = ? AND fb.status IN (?)
          ORDER BY fb.id DESC
            FOR UPDATE OF fb`,
        [shipmentId, FC.LIVE_BOOKING_STATUSES]
    );
    const forwarders = await forwardersById(conn, namedForwarders(shipment, bookings), { locking: true });
    const duties = FC.dutiesOf({ shipment, bookings, forwarderLive: liveIn(forwarders) });
    const owner = duties.freight;
    const booking = owner.bookingId != null
        ? bookings.find(b => Number(b.id) === Number(owner.bookingId)) || null
        : bookings.find(b => (FC.SCOPE_DUTIES[b.scope || FC.DEFAULT_SCOPE] || []).includes('freight')) || null;
    return { shipment, bookings, duties, forwarders, booking, owner, forwarder: owner.forwarderId == null ? null : forwarders.get(owner.forwarderId) };
}

/** The container's member orders, locked: an order joining or leaving it waits too. */
async function lockMembers(conn, shipmentId) {
    // `delivery_at`: what each order holds for delivery, in the one form, straight
    // from SQL (src/lib/forwarder-containers.js deliverySelect): no Date object.
    const [rows] = await conn.query(
        `SELECT id, external_container_number, awb_number, ${FC.deliverySelect('delivery_date')} AS delivery_at FROM orders WHERE shipment_id = ? AND deleted_at IS NULL ORDER BY id FOR UPDATE`,
        [shipmentId]
    );
    return rows;
}

const PROPOSAL_COLUMNS = `d.id, d.shipment_id, d.forwarder_id, d.status, d.requested_json, d.base_json, d.note, d.user_email, d.user_name,
        d.claim_token, d.claim_json, d.claimed_by, d.decided_note, d.decided_by,
        DATE_FORMAT(d.created_at, ${ISO}) AS created_at_iso, DATE_FORMAT(d.claimed_at, ${ISO}) AS claimed_at_iso,
        DATE_FORMAT(d.decided_at, ${ISO}) AS decided_at_iso,
        (d.claimed_at IS NOT NULL AND d.claimed_at < UTC_TIMESTAMP() - INTERVAL ${FC.CLAIM_MINUTES} MINUTE) AS lapsed`;

/** EVERY proposal row of the container (all forwarders), oldest first, locked. */
async function lockProposals(conn, jp, shipmentId) {
    const [rows] = await conn.query(
        `SELECT ${PROPOSAL_COLUMNS} FROM ${jp}.forwarder_container_details d WHERE d.shipment_id = ? ORDER BY d.id FOR UPDATE`,
        [shipmentId]
    );
    return rows;
}

// What we hold of a container, as the card shows it (rowToShipment: the
// member orders win over the stored copies).
function heldOf(row) {
    const api = S.rowToShipment(row);
    // What its orders hold for delivery (shipmentSelect: the distinct values in the
    // one form, and how many orders have none). An unlocked read, for the tab; a
    // decision reads each order's own value under its lock (lockMembers).
    const deliveries = String(row.member_deliveries || '').split('|').filter(Boolean);
    if (Number(row.member_delivery_missing) > 0) deliveries.push(null);
    return {
        ecn: api.trackingRef || null,
        loadingDate: api.loadingDate || null,
        etd: api.etd || null,
        eta: api.eta || null,
        bookingRef: (api.booking && api.booking.bookingRef) || api.bookingRef || null,
        stage: api.stage,
        ...FC.heldDelivery(deliveries),
    };
}
const detailsOf = held => ({ ecn: held.ecn, loadingDate: held.loadingDate, etd: held.etd });
// What a decision compares: the number and dates as the card reads them, and the
// delivery date as every LOCKED member order holds it.
const lockedHeld = (shipRow, members) => ({ ...detailsOf(heldOf(shipRow)), ...FC.heldDelivery(members.map(m => m.delivery_at)) });

async function inTransaction(conn, fn) {
    await conn.beginTransaction();
    try {
        const out = await fn();
        await conn.commit();
        return out;
    } catch (e) {
        await conn.rollback().catch(() => {});
        throw e;
    }
}

// A decision's transaction. The legacy order route takes its locks in another
// order (the order, then the shipment's shadow copy), so a deadlock is possible:
// tried once more, then said as something to retry.
async function inDecision(conn, fn) {
    for (let attempt = 1; ; attempt++) {
        try {
            return await inTransaction(conn, fn);
        } catch (e) {
            if (!e || e.errno !== ER_LOCK_DEADLOCK) throw e;
            if (attempt >= 2) throw new Fail(503, 'RETRY', 'The container was busy. Try again.', { allowedNext: ['retry'] });
        }
    }
}

// The container a proposal is for. A plain read, so BEFORE the transaction.
async function shipmentOfProposal(conn, jp, id) {
    const [rows] = await conn.query(`SELECT d.shipment_id FROM ${jp}.forwarder_container_details d WHERE d.id = ?`, [id]);
    if (!rows.length) throw refuse('NO_SUCH_PROPOSAL');
    return Number(rows[0].shipment_id);
}

/**
 * Staff are about to accept proposal `id`. → { claimToken, expiresInSeconds,
 * requested, held }. Throws a Fail for every refusal; a refusal writes nothing.
 *   expected  what the card showed the container holds: { ecn, loadingDate, etd }
 */
async function claimDetails(conn, { id, expected, actor }, { getShipment = sync.getShipment, env = process.env } = {}) {
    const jp = jportalSchema(env);
    const shipmentId = await shipmentOfProposal(conn, jp, id);
    return inDecision(conn, async () => {
        const { duties } = await lockOwnership(conn, shipmentId);
        const members = await lockMembers(conn, shipmentId);
        const rows = await lockProposals(conn, jp, shipmentId);
        // A claim nobody finished or released is put back, so it is not in the way.
        const lapsed = rows.filter(r => r.status === 'applying' && Number(r.lapsed) === 1);
        if (lapsed.length) {
            await conn.query(
                `UPDATE ${jp}.forwarder_container_details
                    SET status = 'pending', claim_token = NULL, claim_json = NULL, claimed_by = NULL, claimed_at = NULL
                  WHERE id IN (?) AND status = 'applying'`,
                [lapsed.map(r => r.id)]
            );
        }
        const no = FC.claimRefusal({ rows, id, duties });
        if (no) throw refuse(no.code, { ...(no.claimedBy !== undefined ? { claimedBy: no.claimedBy, claimedAt: no.claimedAt } : {}), ...notTheirs(no, duties, rows.find(r => Number(r.id) === Number(id))) });
        const row = rows.find(r => Number(r.id) === Number(id));
        const requested = FC.proposalOf(row.requested_json);
        if (!Object.keys(requested).length) throw refuse('EMPTY_PROPOSAL');
        // A ShipLine build that does not know every field this proposal carries
        // (it names three, and this carries a delivery date) cannot decide it.
        if (!FC.clientKnows(requested, Object.keys(expected || {}))) throw refuse('CLIENT_TOO_OLD');

        // After the locks: the container as the card reads it, and for delivery
        // what each locked member order holds.
        const shipRow = await getShipment(conn, shipmentId);
        const held = lockedHeld(shipRow, members);
        const moved = FC.heldDiff(expected, held);
        if (moved.length) throw refuse('CONTAINER_CHANGED', { fields: moved, held });
        if (requested.ecn) {
            if (!members.length) throw refuse('NO_ORDERS');
            if (FC.ecnProblem(shipRow.mode, requested.ecn)) throw refuse('BAD_PROPOSAL', { fields: ['ecn'] });
        }
        // A delivery date is written onto the orders: it needs one to carry it.
        if (requested.deliveryDate && !members.length) throw refuse('NO_ORDERS');
        const outOfOrder = FC.orderProblem(requested, held);
        if (outOfOrder) throw refuse('DATES_OUT_OF_ORDER', { fields: outOfOrder });

        const claimToken = crypto.randomBytes(16).toString('hex');
        const [done] = await conn.query(
            `UPDATE ${jp}.forwarder_container_details
                SET status = 'applying', claim_token = ?, claim_json = ?, claimed_by = ?, claimed_at = UTC_TIMESTAMP()
              WHERE id = ? AND status = 'pending'`,
            // What the claim saw: the number and dates, WHICH orders are on the container,
            // and what each holds for delivery. finish compares all of it.
            [claimToken, JSON.stringify(FC.claimSnapshot(detailsOf(held), members)), actor.email || 'shipline', id]
        );
        if (done.affectedRows !== 1) throw refuse('ALREADY_DECIDED');
        return { claimToken, expiresInSeconds: FC.CLAIM_MINUTES * 60, requested, held };
        // (`held` carries deliveryDate and deliveryMixed since 2026-11-24.)
    });
}

/**
 * Finish a claimed accept: check the ECN landed and nothing else moved, write
 * the dates, mark the proposal accepted. One transaction; a refusal writes nothing.
 *   ecnReset  { etd } the departure date ShipLine's own ECN reset wrote, or null
 */
async function finishDetails(conn, { id, claimToken, ecnReset, actor }, { getShipment = sync.getShipment, patchShipment, recordAudit, env = process.env } = {}) {
    const jp = jportalSchema(env);
    const shipmentId = await shipmentOfProposal(conn, jp, id);
    return inDecision(conn, async () => {
        const { duties } = await lockOwnership(conn, shipmentId);
        const members = await lockMembers(conn, shipmentId);
        const rows = await lockProposals(conn, jp, shipmentId);
        const row = rows.find(r => Number(r.id) === Number(id));
        if (!row) throw refuse('NO_SUCH_PROPOSAL');
        const mine = row.status === 'applying' && Number(row.lapsed) !== 1 && typeof claimToken === 'string' && claimToken !== '' && row.claim_token === claimToken;
        if (!mine) throw refuse('CLAIM_LOST');
        // Authority is asked again here, on the rows as they are NOW: a pick or a
        // booking's scope that changed since the claim counts only if it took away
        // a duty this proposal needs.
        const lost = FC.dutyRefusal({ row, duties });
        if (lost) throw refuse(lost.code, notTheirs(lost, duties, row));

        const requested = FC.proposalOf(row.requested_json);
        const claim = jsonObject(row.claim_json);
        const shipRow = await getShipment(conn, shipmentId);
        const held = lockedHeld(shipRow, members);
        const memberEcns = members.map(m => S.trackingRefFor(shipRow.mode, [m]).trackingRef);
        const no = FC.finishRefusal({ requested, claim, ecnReset: ecnReset || null, memberEcns, held, members });
        if (no) throw refuse(no.code, { fields: no.fields, held });

        // Written by PATCH /shipments/:id's own code: the loading and departure
        // dates, and the delivery date onto every member order.
        const dates = {};
        if (requested.loadingDate) dates.loadingDate = requested.loadingDate;
        if (requested.etd) dates.etd = requested.etd;
        if (requested.deliveryDate) dates.deliveryDate = requested.deliveryDate;
        if (Object.keys(dates).length) await patchShipment(conn, shipmentId, dates, actor.email || null);

        const [done] = await conn.query(
            `UPDATE ${jp}.forwarder_container_details
                SET status = 'applied', decided_by = ?, decided_at = UTC_TIMESTAMP(), applied_json = ?, claim_token = NULL
              WHERE id = ? AND status = 'applying' AND claim_token = ?`,
            [actor.email || 'shipline', JSON.stringify(requested), id, claimToken]
        );
        if (done.affectedRows !== 1) throw refuse('CLAIM_LOST');
        await recordAudit(conn, {
            entityType: 'forwarder_container_details', entityId: Number(id), action: 'applied',
            before: { shipmentId, forwarderId: Number(row.forwarder_id), held: claim, base: jsonObject(row.base_json) },
            after: { requested, ecnReset: ecnReset || null },
            userEmail: actor.email || null,
        });
        return { ok: true, status: 'applied', requested };
    });
}

/** Give a claim up (the ECN confirmation was cancelled or refused). An old token does nothing. */
async function releaseDetails(conn, { id, claimToken }, { env = process.env } = {}) {
    const jp = jportalSchema(env);
    if (typeof claimToken !== 'string' || claimToken === '') return { ok: true, released: false };
    const [done] = await conn.query(
        `UPDATE ${jp}.forwarder_container_details
            SET status = 'pending', claim_token = NULL, claim_json = NULL, claimed_by = NULL, claimed_at = NULL
          WHERE id = ? AND status = 'applying' AND claim_token = ?`,
        [id, claimToken]
    );
    return { ok: true, released: done.affectedRows === 1 };
}

/** Send a proposal back (or dismiss one that is no longer its forwarder's): the forwarder reads the note. */
async function rejectDetails(conn, { id, note, fields, actor }, { recordAudit, env = process.env } = {}) {
    const jp = jportalSchema(env);
    const shipmentId = await shipmentOfProposal(conn, jp, id);
    return inDecision(conn, async () => {
        const rows = await lockProposals(conn, jp, shipmentId);
        const row = rows.find(r => Number(r.id) === Number(id));
        if (!row) throw refuse('NO_SUCH_PROPOSAL');
        // Sending back is a decision too: a ShipLine build that cannot show a field
        // this proposal carries must name it (`fields`), or it cannot send it back.
        if (!FC.clientKnows(FC.proposalOf(row.requested_json), fields)) throw refuse('CLIENT_TOO_OLD');
        if (row.status === 'applying' && Number(row.lapsed) !== 1) throw refuse('CLAIM_IN_PROGRESS', { claimedBy: row.claimed_by || null, claimedAt: row.claimed_at_iso || null });
        if (!FC.isWaiting(row)) throw refuse('ALREADY_DECIDED');
        if (Number(FC.liveProposal(rows, row.forwarder_id).id) !== Number(row.id)) throw refuse('SUPERSEDED');
        await conn.query(
            `UPDATE ${jp}.forwarder_container_details
                SET status = 'rejected', decided_note = ?, decided_by = ?, decided_at = UTC_TIMESTAMP(),
                    claim_token = NULL, claim_json = NULL, claimed_by = NULL, claimed_at = NULL
              WHERE id = ? AND status IN ('pending', 'applying')`,
            [note, actor.email || 'shipline', id]
        );
        await recordAudit(conn, {
            entityType: 'forwarder_container_details', entityId: Number(id), action: 'rejected',
            before: { shipmentId, forwarderId: Number(row.forwarder_id), requested: FC.proposalOf(row.requested_json) },
            after: { note },
            userEmail: actor.email || null,
        });
        return { ok: true, status: 'rejected' };
    });
}

// A proposal row as the tab shows it. `duties`: who holds what on the container now.
function proposalJson(row, { live, duties, forwarderName }) {
    const claimed = row.status === 'applying' && Number(row.lapsed) !== 1;
    const waiting = FC.isWaiting(row);
    const requested = FC.proposalOf(row.requested_json);
    // The fields it asks for that its forwarder no longer holds the duty for. A
    // proposal is accepted or dismissed whole: one such field makes it dismiss-only.
    const outside = FC.fieldsOutside(requested, FC.heldBy(duties, row.forwarder_id));
    return {
        id: Number(row.id),
        forwarderId: Number(row.forwarder_id),
        forwarderName: forwarderName || null,
        status: claimed ? 'applying' : waiting ? 'pending' : row.status,
        waiting,
        live,
        // Still its forwarder's to ask for: it can be accepted. Otherwise only dismissed.
        actionable: live && (waiting || claimed) && FC.dutyRefusal({ row, duties }) === null,
        outside,
        requested,
        base: FC.proposalOf(row.base_json),
        note: row.note || null,
        byName: row.user_name || null,
        byEmail: row.user_email || null,
        at: row.created_at_iso || null,
        claimedBy: claimed ? (row.claimed_by || null) : null,
        claimedAt: claimed ? (row.claimed_at_iso || null) : null,
        decidedNote: row.decided_note || null,
        decidedBy: row.decided_by || null,
        decidedAt: row.decided_at_iso || null,
    };
}

/**
 * What forwarders sent about a container's number and dates, as the tab shows
 * it. `duties`: who holds what on the container now (dutiesOf).
 * → { unavailable, byForwarder, strays }:
 *   byForwarder  Map of forwarder id → { proposal, history }, for every
 *                forwarder that HOLDS A DUTY on the container: its live proposal
 *                (its newest row, whatever became of it, or null) and its older
 *                ones, newest first. A live proposal that asks for something
 *                outside its forwarder's duties is here too, not actionable.
 *   strays       live, undecided proposals of a forwarder that holds NOTHING on
 *                the container now: they can be dismissed, never accepted
 */
async function readDetails(conn, shipmentId, duties, env = process.env) {
    let rows;
    try {
        [rows] = await conn.query(
            `SELECT ${PROPOSAL_COLUMNS}, f.name AS forwarder_name
               FROM ${jportalSchema(env)}.forwarder_container_details d
               LEFT JOIN forwarders f ON f.id = d.forwarder_id
              WHERE d.shipment_id = ? ORDER BY d.id DESC LIMIT 100`,
            [shipmentId]
        );
    } catch (e) {
        if (!portalMissing(e)) throw e;
        return { unavailable: true, byForwarder: new Map(), strays: [] };
    }
    const out = { unavailable: false, byForwarder: new Map(), strays: [] };
    const seen = new Set();
    for (const row of rows) {
        const fid = Number(row.forwarder_id);
        const live = !seen.has(fid);
        seen.add(fid);
        const json = proposalJson(row, { live, duties, forwarderName: row.forwarder_name });
        if (FC.heldBy(duties, fid).length) {
            if (!out.byForwarder.has(fid)) out.byForwarder.set(fid, { proposal: null, history: [] });
            const mine = out.byForwarder.get(fid);
            if (live) mine.proposal = json; else mine.history.push(json);
        } else if (live && (json.waiting || json.status === 'applying')) {
            out.strays.push(json);
        }
    }
    return out;
}

function registerForwarderContainerRoutes(app, deps) {
    const {
        withConnection, recordAudit, getEmailTemplate, renderTemplate,
        ready = async () => {},
        sendFrontMessage = defaultSend,
        preapprove = defaultPreapprove,
        getShipment = sync.getShipment,
        patchShipment,
        readForwarderEvidence = defaultEvidence,
        logSentEmail = defaultLogSentEmail,
        log = defaultLog,
        env = process.env,
    } = deps;

    const route = (label, fn) => async (req, res) => {
        try {
            await ready();
            await fn(req, res);
        } catch (e) {
            if (res.headersSent) return;
            if (e instanceof Fail) return res.status(e.status).json({ error: e.error, code: e.code, ...(e.extra || {}) });
            if (e instanceof S.ShipmentError) return res.status(e.status).json({ error: e.message, code: e.code, ...(e.payload || {}) });
            if (e && e.errno === ER_NO_SUCH_TABLE) return res.status(503).json(NOT_MIGRATED);
            log.error(`[${label}]`, e);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    };

    const refuseCannotDecide = (req) => {
        if (!FC.mayDecide(req.userType)) {
            throw new Fail(403, 'CANNOT_DECIDE', 'Your role cannot decide what a forwarder sends, or email them: admin and standard users do.');
        }
    };
    const actorOf = req => ({ email: req.userEmail || null, userType: req.userType });

    // The payments SENT to a forwarder that have a charge on this container
    // (the portal's own views: sent payments only, failing closed), each with
    // this container's share and what its forwarder did with it.
    async function paymentsFor(conn, shipmentId) {
        const [lines] = await conn.query(
            `SELECT pl.payment_id, pl.shipment_id, pl.shipment_reference, pl.amount, pl.extra_kind
               FROM portal_forwarder_payment_lines_v pl
              WHERE pl.payment_id IN (SELECT x.payment_id FROM portal_forwarder_payment_lines_v x WHERE x.shipment_id = ?)
              ORDER BY pl.payment_id, pl.line_id`,
            [shipmentId]
        );
        const mine = FC.paymentsOnContainer(lines, shipmentId);
        if (!mine.length) return [];
        const ids = mine.map(p => p.paymentId);
        const [pays] = await conn.query(
            `SELECT p.payment_id, p.forwarder_id, f.name AS forwarder_name, p.amount, p.currency,
                    DATE_FORMAT(p.paid_on, '%Y-%m-%d') AS paid_on, DATE_FORMAT(p.last_sent_at, ${ISO}) AS last_sent_iso, p.revision
               FROM portal_forwarder_payments_v p JOIN forwarders f ON f.id = p.forwarder_id
              WHERE p.payment_id IN (?)`,
            [ids]
        );
        const byId = new Map(pays.map(p => [Number(p.payment_id), p]));
        const read = await readForwarderEvidence(conn, ids, env);
        const out = [];
        for (const m of mine) {
            const p = byId.get(m.paymentId);
            if (!p) continue;
            const forwarderId = Number(p.forwarder_id);
            const evidence = N.forwarderEvidence(read.events.get(m.paymentId) || [], {
                forwarderId, revision: read.revisions.get(`${m.paymentId}:${forwarderId}`) || null,
            });
            out.push({
                paymentId: m.paymentId,
                forwarderId,
                forwarderName: p.forwarder_name || null,
                paidOn: p.paid_on || null,
                amount: Number(p.amount),
                currency: p.currency || null,
                share: m.share,
                lines: m.lines,
                alsoPays: m.alsoPays,
                sentAt: p.last_sent_iso || null,
                opened: evidence.opened,
                acknowledgement: evidence.acknowledgement,
            });
        }
        return out;
    }

    // ── GET /api/v1/shipments/:id/forwarder-portal ──────────────────────────
    app.get('/api/v1/shipments/:id(\\d+)/forwarder-portal', route('GET /shipments/:id/forwarder-portal', async (req, res) => {
        const id = Number(req.params.id);
        const out = await withConnection(async (conn) => {
            const row = await getShipment(conn, id);
            if (!row) throw new Fail(404, 'NO_SUCH_SHIPMENT', `Shipment ${id} not found.`);
            const api = S.rowToShipment(row);
            const bookings = bookingsOfRow(row);
            const forwarders = await forwardersById(conn, namedForwarders(row, bookings));
            // Who holds each duty (2026-11-24): freight, delivery, customs. `owner`
            // is the freight duty, which is all a container's forwarder was before.
            const duties = FC.dutiesOf({ shipment: row, bookings, forwarderLive: liveIn(forwarders) });
            const owner = duties.freight;
            const nameOf = (fid) => { const f = forwarders.get(Number(fid)); return f ? f.name : null; };
            // The forwarder the card shows (the booking's, else the container's
            // own), whether or not it may see the container.
            const forwarder = api.forwarder || api.forwarderId != null
                ? { id: api.forwarderId ?? null, name: api.forwarder || null, source: api.forwarderSource || null }
                : null;
            const contacts = await contactsOf(conn, forwarder ? forwarder.id : null);
            const [notices] = await conn.query(
                `SELECT n.id, n.status, n.forwarder_id, n.sent_to, n.subject, n.sent_by_email,
                        DATE_FORMAT(n.created_at, ${ISO}) AS created_at_iso, DATE_FORMAT(n.sent_at, ${ISO}) AS sent_at_iso
                   FROM ${NOTICE_TABLE} n WHERE n.shipment_id = ? ORDER BY n.id DESC`,
                [id]
            );
            const payments = await paymentsFor(conn, id);
            const held = heldOf(row);
            const paymentOnly = [];
            for (const p of payments) {
                const v = FC.accessFor({ duties, real: FC.isReal(row), forwarderId: p.forwarderId, paidForwarderIds: [p.forwarderId] });
                if (v.mode === 'payment' && !paymentOnly.some(x => x.forwarderId === p.forwarderId)) {
                    paymentOnly.push({ forwarderId: p.forwarderId, forwarderName: p.forwarderName });
                }
            }
            const details = await readDetails(conn, id, duties, env);
            // The files they sent (src/services/forwarder-upload-routes.js), and the documents filed from them.
            const uploads = await readUploads(conn, id, duties, env);
            const documents = await readDocuments(conn, id, { email: req.userEmail || null, userType: req.userType });
            const noticeList = notices.map(noticeJson);
            const noProposals = { proposal: null, history: [] };
            const noFiles = () => ({ invoice: [], loadingPhotos: [], billOfLading: [], customsDraft: [], customsEntry: [] });
            const reviewOf = (d, u) => FC.toReviewCount({
                proposals: [d.proposal].filter(Boolean).map(p => ({ waiting: p.waiting || p.status === 'applying', actionable: p.actionable })),
                uploads: Object.values(u).flat(),
            });
            // One section per forwarder that holds a duty, the freight holder first:
            // who it is, what it does, who can sign in for it, what its email would
            // ask for, and what it sent.
            const sections = [];
            for (const a of FC.attachedOf(duties)) {
                const mine = details.byForwarder.get(a.forwarderId) || noProposals;
                const files = uploads.byForwarder.get(a.forwarderId) || noFiles();
                sections.push({
                    id: a.forwarderId,
                    name: nameOf(a.forwarderId),
                    duties: a.duties,
                    sources: a.sources,
                    visibility: { mode: 'full', why: null },
                    contacts: forwarder && forwarder.id === a.forwarderId ? contacts : await contactsOf(conn, a.forwarderId),
                    asks: FC.containerAsks(held, a.duties),
                    // The newest email sent to it about this container (the list is newest first).
                    lastNotice: noticeList.find(n => n.forwarderId === a.forwarderId) || null,
                    details: { proposal: mine.proposal, history: mine.history },
                    uploads: files,
                    toReview: reviewOf(mine, files),
                });
            }
            // The fields from before 2026-11-24 describe the FREIGHT holder only, so a
            // ShipLine build from before then shows what it showed. Another attached
            // forwarder's things are in `forwarders`, and NEVER in the old `strays`:
            // that build offers a stray for dismissal.
            const freight = owner.forwarderId == null ? null : sections.find(s => s.id === owner.forwarderId) || null;
            const freightFiles = freight ? freight.uploads : noFiles();
            // ... and only what such a build can decide: a stray it has no words for (a draft
            // customs entry, a proposal with a delivery date) is in the new `strays` alone.
            const oldStrays = {
                details: details.strays.filter(p => Object.keys(p.requested).every(f => FC.ORIGINAL_FIELDS.includes(f))),
                uploads: uploads.strays.filter(u => FC.ORIGINAL_KINDS.includes(u.kind)),
            };
            const base = poPortalEmail.portalBaseOf(env);
            return {
                shipmentId: id,
                reference: api.reference,
                mode: api.mode,
                forwarder,
                visibility: owner.forwarderId != null ? { mode: 'full', why: null } : { mode: 'none', why: owner.why },
                whyText: owner.forwarderId != null ? null : (WHY_TEXT[owner.why] || null),
                contacts,
                held,
                asks: FC.containerAsks(held),
                portalUrl: base ? FC.containerUrl(base, id) : null,
                notices: noticeList,
                payments,
                paymentOnly,
                // The portal's own tables are not on this server yet: nothing a forwarder sent can be read.
                unavailable: details.unavailable || uploads.unavailable,
                details: { ...(freight ? freight.details : noProposals), strays: oldStrays.details },
                uploads: { invoice: freightFiles.invoice, loadingPhotos: freightFiles.loadingPhotos, billOfLading: freightFiles.billOfLading, strays: oldStrays.uploads },
                documents: documents.filter(d => d.kind === 'bill_of_lading'),
                // Who holds each duty, or why nobody does; with the pick behind an empty one.
                duties: Object.fromEntries(FC.DUTIES.map((duty) => {
                    const h = duties[duty];
                    const pickId = row[FC.PICK_COLUMN[duty]] ?? null;
                    return [duty, h.forwarderId != null
                        ? { ...h, name: nameOf(h.forwarderId) }
                        : { forwarderId: null, why: h.why, whyText: WHY_TEXT[h.why] || null, picked: pickId == null ? null : { id: Number(pickId), name: nameOf(pickId) } }];
                })),
                forwarders: sections,
                // What a company that holds NOTHING here sent and is still undecided: dismiss only.
                strays: { details: details.strays, uploads: uploads.strays },
                // Every document kept for the container, whatever its kind.
                keptDocuments: documents,
                toReview: sections.reduce((n, s) => n + s.toReview, 0),
                canDecide: FC.mayDecide(req.userType),
            };
        });
        res.json(out);
    }));

    // ── GET /api/v1/forwarder-submissions[?status=all] ──────────────────────
    // What forwarders sent, across containers, newest first: what waits for our
    // team on a container that is still theirs (the default), or everything
    // (`status=all`, with what was decided and what is no longer theirs).
    async function listSubmissions(conn, all) {
        const jp = jportalSchema(env);
        try {
            const [rows] = await conn.query(
                `SELECT d.id, d.shipment_id, d.forwarder_id, d.status, d.requested_json, d.user_email, d.user_name,
                        DATE_FORMAT(d.created_at, ${ISO}) AS created_at_iso, DATE_FORMAT(d.decided_at, ${ISO}) AS decided_at_iso,
                        (d.claimed_at IS NOT NULL AND d.claimed_at < UTC_TIMESTAMP() - INTERVAL ${FC.CLAIM_MINUTES} MINUTE) AS lapsed,
                        COALESCE(c.reference, s.reference, d.container_ref) AS reference, COALESCE(c.mode, s.mode) AS mode, f.name AS forwarder_name,
                        (c.shipment_id IS NOT NULL) AS theirs,
                        c.does_freight, c.does_delivery, c.does_customs,
                        (d.id = (SELECT MAX(x.id) FROM ${jp}.forwarder_container_details x
                                  WHERE x.shipment_id = d.shipment_id AND x.forwarder_id = d.forwarder_id)) AS live
                   FROM ${jp}.forwarder_container_details d
                   LEFT JOIN portal_forwarder_containers_v c
                          ON c.shipment_id = d.shipment_id AND c.forwarder_id = d.forwarder_id AND c.link_source IN (${FULL_SOURCES_SQL})
                   LEFT JOIN shipments s ON s.id = d.shipment_id
                   LEFT JOIN forwarders f ON f.id = d.forwarder_id
                  ${all ? '' : "WHERE d.status IN ('pending', 'applying') HAVING theirs = 1 AND live = 1"}
                  ORDER BY d.id DESC
                  LIMIT ${LIST_LIMIT}`
            );
            const data = rows.map((r) => {
                const waiting = FC.isWaiting(r) || r.status === 'applying';
                // A claim that lapsed reads as waiting again, as on the tab.
                const claimed = r.status === 'applying' && Number(r.lapsed) !== 1;
                const requested = FC.proposalOf(r.requested_json);
                // What its forwarder holds on the container now, as the portal's view says.
                const held = dutiesOfFlags(r);
                // Still its forwarder's to ask for: it holds every duty the fields need.
                const theirs = Number(r.theirs) === 1 && FC.fieldsOutside(requested, held).length === 0;
                return {
                    kind: 'details',
                    id: Number(r.id),
                    shipmentId: Number(r.shipment_id),
                    reference: r.reference || null,
                    mode: r.mode || null,
                    forwarderId: Number(r.forwarder_id),
                    forwarderName: r.forwarder_name || null,
                    duties: held,
                    status: claimed ? 'applying' : FC.isWaiting(r) ? 'pending' : r.status,
                    waiting,
                    // Why it can no longer be acted on: a newer one replaced it (`live`), or its
                    // forwarder no longer holds what it is about on that container (`theirs`).
                    live: Number(r.live) === 1,
                    theirs,
                    actionable: waiting && Number(r.live) === 1 && theirs,
                    requested,
                    byName: r.user_name || null,
                    byEmail: r.user_email || null,
                    at: r.created_at_iso || null,
                    decidedAt: r.decided_at_iso || null,
                };
            });
            // With the files they uploaded, the newest first.
            const files = await listUploads(conn, all, env);
            const merged = [...(all ? data : data.filter(d => d.actionable)), ...files]
                .sort((a, b) => (String(b.at || '') > String(a.at || '') ? 1 : String(b.at || '') < String(a.at || '') ? -1 : b.id - a.id));
            return { data: merged.slice(0, LIST_LIMIT), unavailable: false };
        } catch (e) {
            if (!portalMissing(e)) throw e;
            return { data: [], unavailable: true };
        }
    }

    app.get('/api/v1/forwarder-submissions/summary', route('GET /forwarder-submissions/summary', async (req, res) => {
        const out = await withConnection(conn => listSubmissions(conn, false));
        res.json({ toReview: out.data.length, unavailable: out.unavailable });
    }));

    app.get('/api/v1/forwarder-submissions', route('GET /forwarder-submissions', async (req, res) => {
        const out = await withConnection(conn => listSubmissions(conn, req.query.status === 'all'));
        res.json({ ...out, canDecide: FC.mayDecide(req.userType) });
    }));

    // ── A proposal of a container's number and dates ────────────────────────
    const proposalId = (req) => {
        const id = Number(req.params.id);
        if (!Number.isSafeInteger(id) || id <= 0) throw new Fail(400, 'BAD_FIELD', 'Not a proposal id.', { field: 'id' });
        return id;
    };

    app.post('/api/v1/forwarder-details/:id(\\d+)/claim', route('POST /forwarder-details/:id/claim', async (req, res) => {
        refuseCannotDecide(req);
        const id = proposalId(req);
        const expected = (req.body && typeof req.body.expected === 'object' && req.body.expected) || null;
        if (!expected) throw new Fail(400, 'BAD_FIELD', 'expected is what the card showed the container holds: { ecn, loadingDate, etd }.', { field: 'expected' });
        res.json(await withConnection(conn => claimDetails(conn, { id, expected, actor: actorOf(req) }, { getShipment, env })));
    }));

    app.post('/api/v1/forwarder-details/:id(\\d+)/finish', route('POST /forwarder-details/:id/finish', async (req, res) => {
        refuseCannotDecide(req);
        const id = proposalId(req);
        const { claimToken, ecnReset } = req.body || {};
        if (typeof claimToken !== 'string' || !/^[a-f0-9]{32}$/.test(claimToken)) throw new Fail(400, 'BAD_FIELD', 'claimToken is what the claim answered.', { field: 'claimToken' });
        const reset = ecnReset && typeof ecnReset === 'object' && Object.prototype.hasOwnProperty.call(ecnReset, 'etd') ? { etd: ecnReset.etd } : null;
        res.json(await withConnection(conn => finishDetails(conn, { id, claimToken, ecnReset: reset, actor: actorOf(req) }, { getShipment, patchShipment, recordAudit, env })));
    }));

    app.post('/api/v1/forwarder-details/:id(\\d+)/release', route('POST /forwarder-details/:id/release', async (req, res) => {
        refuseCannotDecide(req);
        const id = proposalId(req);
        res.json(await withConnection(conn => releaseDetails(conn, { id, claimToken: (req.body || {}).claimToken }, { env })));
    }));

    app.post('/api/v1/forwarder-details/:id(\\d+)/reject', route('POST /forwarder-details/:id/reject', async (req, res) => {
        refuseCannotDecide(req);
        const id = proposalId(req);
        const problem = FC.noteProblem((req.body || {}).note);
        if (problem) {
            throw new Fail(400, 'NOTE_REQUIRED', problem === 'long' ? `The note must be ${FC.NOTE_MAX} characters or fewer.` : `Say what is wrong in at least ${FC.NOTE_MIN} characters: the forwarder reads it.`, { field: 'note' });
        }
        const named = Array.isArray(req.body.fields) ? req.body.fields.filter(f => typeof f === 'string') : undefined;
        res.json(await withConnection(conn => rejectDetails(conn, { id, note: FC.cleanNote(req.body.note), fields: named, actor: actorOf(req) }, { recordAudit, env })));
    }));

    // ── POST /api/v1/shipments/:id/forwarder-notice ─────────────────────────
    // Body { sendKey, to?, subject?, body? }. Emails the forwarder's contacts
    // the link to the container's page. `to` may only untick contacts: an
    // address that is not a live contact of the container's forwarder is
    // refused. `sendKey` is made by the dialog and sent with every attempt:
    // a key that has a row is reported as it stands and is never emailed
    // again, except one Front refused (the freight quote request's rule).
    // The row is written `sending` and COMMITTED before Front is called, so
    // a send whose answer never came is known as that, not as nothing.
    // → { ok, outcome: 'sent' | 'already_sent' | 'send_unconfirmed', noticeId }
    app.post('/api/v1/shipments/:id(\\d+)/forwarder-notice', route('POST /shipments/:id/forwarder-notice', async (req, res) => {
        refuseCannotDecide(req);
        const id = Number(req.params.id);
        const { sendKey, to, subject, body, forwarderId: wanted } = req.body || {};
        if (!FC.isSendKey(sendKey)) throw new Fail(400, 'BAD_FIELD', 'sendKey must be the key the dialog made.', { field: 'sendKey' });
        // Which of the container's forwarders to email (2026-11-24). Left out: the
        // only one it has, else the one that carries the freight.
        const wantedId = wanted === undefined || wanted === null ? null : Number(wanted);
        if (wantedId !== null && !(Number.isSafeInteger(wantedId) && wantedId > 0)) {
            throw new Fail(400, 'BAD_FIELD', 'forwarderId must be a forwarder id.', { field: 'forwarderId' });
        }
        const portalBase = poPortalEmail.portalBaseOf(env);
        if (!portalBase) {
            throw new Fail(409, 'PORTAL_URL_UNSET', 'The supplier portal address (SUPPLIER_PORTAL_URL) is not configured, so the container link cannot be sent.');
        }

        const out = await withConnection(async (conn) => {
            const row = await getShipment(conn, id);
            if (!row) throw new Fail(404, 'NO_SUCH_SHIPMENT', `Shipment ${id} not found.`);

            const claimed = await inTransaction(conn, async () => {
                const { shipment, booking, owner, duties, forwarders } = await lockOwnership(conn, id);
                // Whom to email, on the locked rows: a company that holds a duty here.
                const attached = FC.attachedOf(duties);
                const target = wantedId !== null
                    ? attached.find(a => a.forwarderId === wantedId) || null
                    : (attached.length === 1 ? attached[0] : attached.find(a => a.duties.includes('freight')) || null);
                if (!target) {
                    if (wantedId !== null && attached.length) {
                        throw new Fail(409, 'NOT_VISIBLE', 'That forwarder does nothing on this container, so it has no page to link to.', { why: null });
                    }
                    throw new Fail(409, 'NOT_VISIBLE', `This container is not on a forwarder's page, so there is nothing to link to. ${WHY_TEXT[owner.why] || ''}`.trim(), { why: owner.why });
                }
                const forwarder = forwarders.get(target.forwarderId) || null;
                const contacts = await contactsOf(conn, target.forwarderId);
                let addresses = contacts.map(c => String(c.email).trim().toLowerCase());
                if (to !== undefined && to !== null) {
                    const parsed = poPortalEmail.parseRecipients(to);
                    if (parsed.invalid.length) {
                        throw new Fail(400, 'BAD_FIELD', `Not a valid email address: ${parsed.invalid.map(a => `"${a}"`).join(', ')}`, { field: 'to', invalid: parsed.invalid });
                    }
                    const who = N.recipientsAreContacts(parsed.addresses, contacts);
                    if (who.notContacts.length) {
                        throw new Fail(400, 'NOT_A_CONTACT', `Not a contact of this forwarder: ${who.notContacts.join(', ')}. Add them on Freight > Forwarders first.`, { field: 'to', notContacts: who.notContacts });
                    }
                    addresses = who.to;
                }
                if (!addresses.length) throw new Fail(409, 'NO_CONTACTS', 'There is nobody to email: this forwarder has no contact, or none is ticked. Add one on Freight > Forwarders.');

                const [existing] = await conn.query(
                    `SELECT id, shipment_id, forwarder_id, status FROM ${NOTICE_TABLE} WHERE send_key = ? FOR UPDATE`,
                    [sendKey]
                );
                const state = FC.noticeState(existing[0] || null, { shipmentId: id, forwarderId: target.forwarderId });
                if (state === 'clash') throw new Fail(409, 'SEND_KEY_CLASH', 'This send key belongs to another container or forwarder. Open the email dialog again.');
                if (state === 'already_sent' || state === 'send_unconfirmed') return { replay: state, noticeId: Number(existing[0].id) };

                let noticeId;
                if (state === 'retry') {
                    noticeId = Number(existing[0].id);
                    await conn.query(`UPDATE ${NOTICE_TABLE} SET status = 'sending', sent_by_email = ? WHERE id = ?`, [req.userEmail || null, noticeId]);
                } else {
                    const [ins] = await conn.query(
                        `INSERT INTO ${NOTICE_TABLE} (shipment_id, shipment_reference, forwarder_id, send_key, status, sent_by_email)
                         VALUES (?, ?, ?, ?, 'sending', ?)`,
                        [id, shipment.reference || null, target.forwarderId, sendKey, req.userEmail || null]
                    );
                    noticeId = Number(ins.insertId);
                }
                const carriesFreight = target.duties.includes('freight');
                return {
                    noticeId, addresses, forwarderId: target.forwarderId,
                    forwarderName: (forwarder && forwarder.name) || '',
                    reference: shipment.reference || '',
                    // The freight booking's reference is the freight forwarder's own: no other company is told it.
                    bookingRef: carriesFreight ? ((booking && booking.booking_ref) || row.booking_ref || null) : null,
                    // What this company is asked for: only the things of the duties it holds.
                    duties: target.duties,
                };
            });
            if (claimed.replay) return { ok: true, outcome: claimed.replay, noticeId: claimed.noticeId };

            await preapprove(conn, { emails: claimed.addresses, actorEmail: req.userEmail || null, note: `Forwarder container ${claimed.reference}` });

            const held = heldOf(row);
            const tpl = await getEmailTemplate(conn, FREIGHT_CONTAINER_TEMPLATE_KEY);
            const vars = { forwarder: claimed.forwarderName, containerName: claimed.reference, bookingRef: claimed.bookingRef || '' };
            const finalSubject = ((typeof subject === 'string' && subject.trim())
                ? renderTemplate(subject.trim(), vars)
                : (renderTemplate(tpl.subject, vars) || `Container ${claimed.reference} – details needed`))
                .replace(/[\r\n]+/g, ' ').slice(0, 500);
            const escaped = Object.fromEntries(Object.entries(vars).map(([k, val]) => [k, escapeHtml(val)]));
            const inner = renderTemplate((typeof body === 'string' && body.trim()) ? body : tpl.bodyHtml, escaped);
            const html = wrapEmail({
                title: finalSubject,
                bodyHtml: inner + F.containerBlockHtml({
                    containerName: claimed.reference, bookingRef: claimed.bookingRef, asks: FC.containerAsks(held, claimed.duties),
                    url: FC.containerUrl(portalBase, id), registerUrl: poPortalEmail.registerUrl(portalBase),
                }),
            });

            const sent = await sendFrontMessage({ to: claimed.addresses, subject: finalSubject, html });
            if (!sent.ok) {
                await conn.query(`UPDATE ${NOTICE_TABLE} SET status = 'failed' WHERE id = ? AND status = 'sending'`, [claimed.noticeId]);
                if (sent.reason === 'front_not_configured') {
                    throw new Fail(500, 'FRONT_NOT_CONFIGURED', 'Front is not configured (FRONT_API_TOKEN, FRONT_CHANNEL_ID).');
                }
                throw new Fail(502, 'FRONT_ERROR', 'Front rejected the message.', { frontStatus: sent.status ?? null, frontBody: sent.detail ?? null });
            }
            await conn.query(
                `UPDATE ${NOTICE_TABLE} SET status = 'sent', sent_at = UTC_TIMESTAMP(), front_message_uid = ?, front_conversation_id = ?, sent_to = ?, subject = ?
                  WHERE id = ?`,
                [sent.frontMessageUid || null, sent.frontConversationId || null, JSON.stringify(claimed.addresses), finalSubject, claimed.noticeId]
            );
            await logSentEmail(conn, {
                emailType: EMAIL_TYPE, sentTo: claimed.addresses, subject: finalSubject,
                confirmVia: 'portal', sendTable: NOTICE_TABLE, sendId: claimed.noticeId,
            });
            await recordAudit(conn, {
                entityType: 'freight_container_notice', entityId: claimed.noticeId, action: 'sent',
                before: null,
                after: {
                    shipmentId: id, reference: claimed.reference, forwarderId: claimed.forwarderId, forwarder: claimed.forwarderName,
                    to: claimed.addresses, subject: finalSubject,
                    frontMessageUid: sent.frontMessageUid || null, frontConversationId: sent.frontConversationId || null,
                },
                userEmail: req.userEmail,
            });
            return { ok: true, outcome: 'sent', noticeId: claimed.noticeId };
        });
        res.json(out);
    }));
}

module.exports = {
    registerForwarderContainerRoutes, WHY_TEXT, REFUSAL_TEXT, EMAIL_TYPE, NOTICE_TABLE, Fail,
    // The decisions, each on a connection of the caller's: the routes above,
    // and tools/test-forwarder-container-races.js on two connections at once.
    lockOwnership, lockMembers, lockProposals, claimDetails, finishDetails, releaseDetails, rejectDetails, readDetails,
};
