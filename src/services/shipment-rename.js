'use strict';

// What a booked shipment's change of reference (308 -> 309) carries with it.
//
// orders.container_number follows in the PATCH route itself. Everything here
// names the container as TEXT, so nothing else would move it and it would stay
// behind under the old number:
//   shipment_payments, shipment_payment_documents, payment_extras
//                                     shipment_reference
//   payment_reviews, payment_assignees
//                                     'balance:<currency>:<CONTAINER>|<supplier>' keys
//   payment_due_dates                 those keys, and 'item:…:<container>' row keys
//   packing_lists, packing_list_sign_offs, packing_approvals, container_photos
//                                     container_number
//   draft_containers                  container_number of the draft that became it
//   daily_alerts                      'container_eta:c:<container>'
// Left alone on purpose: payment_alert_dismissals (the key hashes the alert's
// wording, so a dismissed alert comes back under the new number, as that table
// intends) and supplier_payment_lines (they point at rows by id).
//
// A money record never changes amount or status here, its updated_at is
// pinned, and each one gets an audit row. The caller owns the transaction.

const S = require('../lib/shipments');
const R = require('../lib/payment-reviews');
const D = require('../lib/payment-due-dates');

// payment_extras.shipment_reference is VARCHAR(64); the others take 100.
const REFERENCE_MAX = 64;

const MONEY_TABLES = [
    { table: 'shipment_payments', count: 'balances', entityType: 'shipment_payment', pin: ', updated_at = updated_at' },
    { table: 'shipment_payment_documents', count: 'paymentDocuments', entityType: 'shipment_payment_document', pin: '' },
    { table: 'payment_extras', count: 'extras', entityType: 'payment_extra', pin: ', updated_at = updated_at' },
];
const PAPER_TABLES = [
    { table: 'packing_lists', count: 'packingLists', live: ' AND deleted_at IS NULL' },
    { table: 'packing_list_sign_offs', count: 'packingSignOffs', live: '' },
    { table: 'packing_approvals', count: 'packingApprovals', live: ' AND withdrawn_at IS NULL' },
    { table: 'container_photos', count: 'photos', live: ' AND deleted_at IS NULL' },
];
const COUNT_KEYS = [...MONEY_TABLES.map(t => t.count), 'signOffs', 'assignees', 'dueDates', ...PAPER_TABLES.map(t => t.count)];

function total(records) {
    return COUNT_KEYS.reduce((n, k) => n + (Number(records && records[k]) || 0), 0);
}

async function scalar(conn, sql, params) {
    const [[row]] = await conn.query(sql, params);
    return Number(row.n) || 0;
}

// Balance-key rows (payment_assignees has no container column: the key is all there is).
async function assigneesOf(conn, reference, { lock = false } = {}) {
    const [rows] = await conn.query(
        `SELECT id, payment_key FROM payment_assignees WHERE payment_key LIKE 'balance:%'${lock ? ' FOR UPDATE' : ''}`
    );
    return rows.filter(r => R.rekeyBalanceKey(r.payment_key, reference, reference) !== null);
}

// Due dates set by hand under the container: on its balance payments, or on
// one row of them (the key is all there is).
async function dueDatesOf(conn, reference, { lock = false } = {}) {
    const [rows] = await conn.query(
        `SELECT id, target_key FROM payment_due_dates WHERE target_key LIKE 'balance:%' OR target_key LIKE 'item:%'${lock ? ' FOR UPDATE' : ''}`
    );
    return rows.filter(r => D.rekeyTargetKey(r.target_key, reference, reference) !== null);
}

// The live records kept under `reference`: by the text, and by `shipmentId`
// too when one is given. This is what a rename moves, and what a confirm shows.
async function recordsUnder(conn, reference, { shipmentId = null } = {}) {
    const out = {};
    for (const t of MONEY_TABLES) {
        out[t.count] = shipmentId != null
            ? await scalar(conn, `SELECT COUNT(*) AS n FROM ${t.table} WHERE (shipment_id = ? OR shipment_reference = ?) AND deleted_at IS NULL`, [shipmentId, reference])
            : await scalar(conn, `SELECT COUNT(*) AS n FROM ${t.table} WHERE shipment_reference = ? AND deleted_at IS NULL`, [reference]);
    }
    out.signOffs = await scalar(conn,
        `SELECT COUNT(*) AS n FROM payment_reviews WHERE kind = 'balance' AND UPPER(container_ref) = ? AND revoked_at IS NULL`, [reference.toUpperCase()]);
    out.assignees = (await assigneesOf(conn, reference)).length;
    out.dueDates = (await dueDatesOf(conn, reference)).length;
    for (const t of PAPER_TABLES) {
        out[t.count] = await scalar(conn,
            `SELECT COUNT(*) AS n FROM ${t.table} WHERE container_kind = 'booked' AND container_number = ?${t.live}`, [reference]);
    }
    return out;
}

// Drafts that only NAME the old number ('… - 308', never closed as converted
// into it) and hold packing rows or photos: the booked container finds those
// through the name, so they stop following it once it is renumbered.
async function strandedDrafts(conn, from) {
    const [reg] = await conn.query(
        `SELECT id, name FROM draft_containers WHERE name LIKE ? AND NOT (closed_reason <=> 'converted' AND container_number <=> ?)`,
        [`%${from}%`, from]
    );
    const candidates = reg.filter(r => (S.parseNameHint(r.name) || {}).reference === from);
    if (!candidates.length) return [];
    const ids = candidates.map(r => r.id);
    const holding = new Set();
    for (const t of PAPER_TABLES) {
        const [rows] = await conn.query(`SELECT DISTINCT draft_container_id AS id FROM ${t.table} WHERE draft_container_id IN (?)`, [ids]);
        for (const r of rows) holding.add(Number(r.id));
    }
    return candidates.filter(r => holding.has(Number(r.id))).map(r => r.name);
}

// Move everything from `from` to `to`. Returns { moved, warnings }.
async function carryRecords(conn, { shipmentId, from, to, userEmail = null, recordAudit }) {
    const moved = {};
    const warnings = [];

    for (const t of MONEY_TABLES) {
        const [rows] = await conn.query(
            `SELECT id, shipment_reference FROM ${t.table} WHERE shipment_id = ? OR shipment_reference = ? ORDER BY id FOR UPDATE`,
            [shipmentId, from]
        );
        moved[t.count] = 0;
        for (const row of rows) {
            if (row.shipment_reference === to) continue;
            await conn.query(`UPDATE ${t.table} SET shipment_reference = ?${t.pin} WHERE id = ?`, [to, row.id]);
            await recordAudit(conn, {
                entityType: t.entityType, entityId: row.id, action: 'update',
                before: { shipmentReference: row.shipment_reference }, after: { shipmentReference: to }, userEmail,
            });
            moved[t.count]++;
        }
    }

    const toKeyRef = to.toUpperCase();
    const [reviews] = await conn.query(
        `SELECT id, payment_key, container_ref FROM payment_reviews WHERE kind = 'balance' AND UPPER(container_ref) = ? ORDER BY id FOR UPDATE`,
        [from.toUpperCase()]
    );
    moved.signOffs = 0;
    for (const row of reviews) {
        const key = R.rekeyBalanceKey(row.payment_key, from, to);
        if (!key) continue;
        await conn.query(`UPDATE payment_reviews SET payment_key = ?, container_ref = ? WHERE id = ?`, [key, toKeyRef, row.id]);
        await recordAudit(conn, {
            entityType: 'payment_review', entityId: row.id, action: 'update',
            before: { paymentKey: row.payment_key, containerRef: row.container_ref }, after: { paymentKey: key, containerRef: toKeyRef }, userEmail,
        });
        moved.signOffs++;
    }
    moved.assignees = 0;
    for (const row of await assigneesOf(conn, from, { lock: true })) {
        const key = R.rekeyBalanceKey(row.payment_key, from, to);
        await conn.query(`UPDATE payment_assignees SET payment_key = ?, updated_at = updated_at WHERE id = ?`, [key, row.id]);
        await recordAudit(conn, {
            entityType: 'payment_assignee', entityId: row.id, action: 'update',
            before: { paymentKey: row.payment_key }, after: { paymentKey: key }, userEmail,
        });
        moved.assignees++;
    }
    moved.dueDates = 0;
    for (const row of await dueDatesOf(conn, from, { lock: true })) {
        const key = D.rekeyTargetKey(row.target_key, from, to);
        await conn.query(`UPDATE payment_due_dates SET target_key = ?, updated_at = updated_at WHERE id = ?`, [key, row.id]);
        await recordAudit(conn, {
            entityType: 'payment_due_date', entityId: row.id, action: 'update',
            before: { key: row.target_key }, after: { key }, userEmail,
        });
        moved.dueDates++;
    }

    // Booked rows by the number; a draft's own rows as well when the draft
    // became this shipment (they are found through the draft, the number on
    // them is only what it was uploaded against).
    for (const t of PAPER_TABLES) {
        const [res] = await conn.query(
            `UPDATE ${t.table} SET container_number = ?
              WHERE container_number = ?
                AND (container_kind = 'booked' OR draft_container_id IN (SELECT id FROM draft_containers WHERE shipment_id = ?))`,
            [to, from, shipmentId]
        );
        moved[t.count] = res.affectedRows || 0;
    }
    const [reg] = await conn.query(
        `UPDATE draft_containers SET container_number = ? WHERE closed_reason = 'converted' AND container_number = ?`, [to, from]
    );
    moved.drafts = reg.affectedRows || 0;

    // The standing ETA alert: follow, unless one already stands under the new number.
    const oldAlert = `container_eta:c:${from}`;
    const newAlert = `container_eta:c:${to}`;
    const taken = await scalar(conn, `SELECT COUNT(*) AS n FROM daily_alerts WHERE dedup_key = ?`, [newAlert]);
    if (!taken) {
        const [al] = await conn.query(`UPDATE daily_alerts SET dedup_key = ?, entity_id = ? WHERE dedup_key = ?`, [newAlert, to, oldAlert]);
        moved.alerts = al.affectedRows || 0;
    } else {
        moved.alerts = 0;
    }

    const stranded = await strandedDrafts(conn, from);
    if (stranded.length) {
        warnings.push(`Packing lists or photos uploaded on ${stranded.length === 1 ? 'the draft' : 'the drafts'} ${stranded.map(n => `"${n}"`).join(', ')} `
            + `were matched to this container by the number in the draft's name, so they stay listed under ${from}.`);
    }
    return { moved, warnings };
}

module.exports = { REFERENCE_MAX, recordsUnder, carryRecords, total };
