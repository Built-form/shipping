'use strict';

// Shipments shadow sync: every write that keeps `shipments` and
// `shipment_lines` in step with the legacy tables, the read queries built on
// them, and the backfill + verify that seed and check them.
//
// Through rollout step 3 the legacy columns and tables stay the source of truth
// (orders.container_number and the carrier refs, draft_container_allocations,
// planned_container_allocations, draft_containers). `shipments` is a derived,
// rebuildable shadow of them. Legacy routes keep their bodies and call one
// fail-soft hook through shadow(). The /api/v1/shipments routes write through to
// the legacy tables in the same transaction. A shadow failure never fails the
// legacy write: it lands in shipment_sync_failures (one coalescing row per key)
// and tools/verify-shipments.js repairs the drift.
//
// Every function takes a connection and never acquires one. Callers own their
// transactions, except shadow() in autocommit mode, which owns exactly its own.
//
// Rules the primitives rely on:
// - Unique keys are released on exit. Any move to CANCELLED, any soft delete
//   and any merge loser sets reference = NULL and open_key = NULL in the same
//   UPDATE, and every finder filters deleted_at IS NULL AND merged_into_id IS
//   NULL.
// - One normalisation: a reference is TRIM(orders.container_number), '' is no
//   reference, and references are matched SQL-side (the keys are
//   case-insensitive; JavaScript string equality is not).
// - Lock order: shipments rows first, then orders rows. Inside a transaction
//   every read that guards a write is a locking read, and find-or-create is
//   INSERT ... ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id).
// - Every UPDATE of orders here is addressed by primary key and pins
//   last_updated (ON UPDATE CURRENT_TIMESTAMP, and JFPRO displays it), so the
//   shadow never makes an order look edited.
// - The stored stage is written only by explicit shipment-level actions. Order
//   status changes never ratchet it (see effectiveStage in lib/shipments.js).

const log = require('../lib/logger');
const { recordAudit } = require('../lib/audit');
const S = require('../lib/shipments');

const BACKFILL_MARKER = 'shipments_backfill_v1';
const KILL_SWITCH = 'shipments_shadow_off';
const KILL_SWITCH_TTL_MS = 60_000;

const BOOKED_IN = `'BOOKED','IN_TRANSIT','ARRIVED','CLOSED'`;
const LIVE = 's.deleted_at IS NULL AND s.merged_into_id IS NULL';

// ── Arming + kill switch ─────────────────────────────────────────────────
// The hooks do nothing until the backfill has written its marker into
// app_migrations; inserting the kill-switch row there turns them off again
// without a redeploy. Only the armed state is cached (for the life of the
// process): a negative answer is re-asked on every call, so a warm Lambda can
// never stay inert after arming. The kill switch is re-read at most every 60 s.
let armedSeen = false;
let killSwitch = { on: false, checkedAt: 0 };

async function shadowArmed(conn) {
    const now = Date.now();
    if (armedSeen && now - killSwitch.checkedAt < KILL_SWITCH_TTL_MS) return !killSwitch.on;
    const [rows] = await conn.query(
        `SELECT name FROM app_migrations WHERE name IN (?, ?)`,
        [BACKFILL_MARKER, KILL_SWITCH]
    );
    const names = new Set(rows.map(r => r.name));
    killSwitch = { on: names.has(KILL_SWITCH), checkedAt: now };
    if (names.has(BACKFILL_MARKER)) armedSeen = true;
    return armedSeen && !killSwitch.on;
}

// For the unit suite.
function resetFlagCache() {
    armedSeen = false;
    killSwitch = { on: false, checkedAt: 0 };
}

// Errors after which the enclosing legacy transaction is gone (MySQL rolls a
// deadlocked transaction back whole) or the connection is unusable.
function isFatal(err) {
    if (!err) return false;
    if (err.fatal) return true;
    return ['ER_LOCK_DEADLOCK', 'PROTOCOL_CONNECTION_LOST', 'ECONNRESET', 'EPIPE', 'ETIMEDOUT'].includes(err.code);
}

// Test seam for the local dev server only (IS_OFFLINE is never set in a
// Lambda): SHIPMENTS_SHADOW_FAULT=<site>[,<site>...] or '*' makes shadow()
// throw after `fn` has written, so a suite can prove a failure leaves no
// partial shadow rows and never fails the legacy request.
function injectedFault(site) {
    const spec = process.env.IS_OFFLINE ? process.env.SHIPMENTS_SHADOW_FAULT : null;
    if (!spec) return null;
    const sites = spec.split(',').map(s => s.trim()).filter(Boolean);
    return sites.includes('*') || sites.includes(site) ? new Error(`injected shadow fault at ${site}`) : null;
}

// One unresolved row per (site, key_kind, key_value): a repeat bumps
// `occurrences` instead of adding a row. Resolving a row NULLs dedup_key, so
// the next failure for the same key starts a fresh row. Best-effort itself.
// Returns the insert's own error, if any, so an in-transaction caller can tell
// whether the legacy transaction survived it.
async function recordFailure(conn, { site, keyKind, keyValue }, err) {
    const siteS = String(site || 'unknown').slice(0, 64);
    const kind = String(keyKind || 'none').slice(0, 16);
    const value = String(keyValue == null ? '' : keyValue).slice(0, 255);
    const message = `${err && err.code ? err.code + ': ' : ''}${(err && err.message) || err}`.slice(0, 500);
    log.warn('[shipments] shadow sync failed', { site: siteS, keyKind: kind, keyValue: value, error: message });
    try {
        await conn.query(
            `INSERT INTO shipment_sync_failures (site, key_kind, key_value, error, dedup_key, occurrences, last_seen_at)
             VALUES (?, ?, ?, ?, ?, 1, NOW())
             ON DUPLICATE KEY UPDATE occurrences = occurrences + 1, error = VALUES(error), last_seen_at = NOW()`,
            [siteS, kind, value, message, `${siteS}|${kind}|${value}`.slice(0, 340)]
        );
        return null;
    } catch (e) {
        log.warn('[shipments] could not record a sync failure', { site: siteS, error: e.message });
        return e;
    }
}

// Run `fn(conn)` as a fail-soft shadow of the legacy write that just happened
// on `conn`. Returns fn's result, or null when inert or failed.
//
//   inTx: true   the legacy route is inside a transaction: fn runs inside a
//                SAVEPOINT; a failure rolls back to it and is recorded, and the
//                legacy transaction carries on. ER_LOCK_DEADLOCK and fatal
//                connection errors are rethrown, because MySQL has already
//                rolled the legacy transaction back.
//   inTx: false  the legacy write has already autocommitted: fn gets its own
//                transaction, always rolled back if not committed (a leaked
//                open transaction on the single pooled connection would leave
//                every later write in that warm Lambda uncommitted). Every
//                error is swallowed, deadlock included, and recorded after the
//                rollback so the record is not rolled back with it.
//
// Call it after the route's recordAudit / recordPoAttachmentChange /
// firePoSentWebhookIfNeeded, so nothing it changes can enter an audit diff and
// no shadow transaction ever wraps the webhook's HTTP call.
async function shadow(conn, { site, inTx = false, keyKind = 'none', keyValue = '' } = {}, fn) {
    const key = { site, keyKind, keyValue };
    let armed = false;
    try {
        armed = await shadowArmed(conn);
    } catch (err) {
        if (inTx && isFatal(err)) throw err;
        log.warn('[shipments] arming check failed; shadow skipped', { site, error: err.message });
        return null;
    }
    if (!armed) return null;
    const fault = injectedFault(site);

    if (inTx) {
        try {
            await conn.query('SAVEPOINT shipments_shadow');
        } catch (err) {
            if (isFatal(err)) throw err;
            const e2 = await recordFailure(conn, key, err);
            if (isFatal(e2)) throw e2;
            return null;
        }
        try {
            const result = await fn(conn);
            if (fault) throw fault;
            await conn.query('RELEASE SAVEPOINT shipments_shadow');
            return result;
        } catch (err) {
            if (isFatal(err)) throw err;
            // Cannot restore the legacy transaction to a known state: fail the
            // request rather than commit half a shadow write.
            await conn.query('ROLLBACK TO SAVEPOINT shipments_shadow');
            const e2 = await recordFailure(conn, key, err);
            if (isFatal(e2)) throw e2;
            return null;
        }
    }

    let failure = null;
    let result = null;
    try {
        await conn.query('START TRANSACTION');
        let committed = false;
        try {
            result = await fn(conn);
            if (fault) throw fault;
            await conn.query('COMMIT');
            committed = true;
        } finally {
            if (!committed) {
                try { await conn.query('ROLLBACK'); } catch (_) { /* connection already gone */ }
            }
        }
    } catch (err) {
        failure = err;
    }
    if (failure) {
        await recordFailure(conn, key, failure);
        return null;
    }
    return result;
}

// ── Small helpers ────────────────────────────────────────────────────────
function str(v, max) {
    const s = S.clean(v);
    return s == null ? null : s.slice(0, max);
}

function ph(list) {
    return list.map(() => '?').join(',');
}

function intIds(list) {
    return [...new Set((list || []).map(Number).filter(n => Number.isInteger(n) && n > 0))].sort((a, b) => a - b);
}

async function shipmentAudit(conn, { shipmentId, action, before = null, after = null, userEmail = null }) {
    await recordAudit(conn, { entityType: 'shipment', entityId: shipmentId, action, before, after, userEmail });
}

async function lockShipment(conn, id) {
    const [rows] = await conn.query(`SELECT * FROM shipments WHERE id = ? FOR UPDATE`, [id]);
    return rows[0] || null;
}

async function lockOpenShipment(conn, openKey) {
    const [rows] = await conn.query(
        `SELECT * FROM shipments s WHERE s.open_key = ? AND ${LIVE} FOR UPDATE`,
        [openKey]
    );
    return rows[0] || null;
}

async function lockByReference(conn, reference) {
    const [rows] = await conn.query(
        `SELECT * FROM shipments s WHERE s.reference = ? AND ${LIVE} FOR UPDATE`,
        [reference]
    );
    return rows[0] || null;
}

// Follow merged_into_id to the live survivor (bounded, merges do not chain long).
async function followToLive(conn, row) {
    let cur = row;
    for (let i = 0; cur && i < 10; i++) {
        if (!cur.deleted_at && !cur.merged_into_id) return cur;
        if (!cur.merged_into_id) return null;
        cur = await lockShipment(conn, cur.merged_into_id);
    }
    return null;
}

// ── Find-or-create a booked shipment by reference ────────────────────────
// Race-safe and lock-taking: the ON DUPLICATE KEY path returns the live
// holder's id with an exclusive lock on it, never a stale snapshot.
async function findOrCreateByReference(conn, reference, {
    origin = 'legacy_route', userEmail = null, name = null, sourceDraftId = null, mode = null, modeSource = null,
} = {}) {
    const parsed = S.parseReference(reference);
    if (!parsed) throw new Error('findOrCreateByReference: empty reference');
    const ref = parsed.reference.slice(0, 100);
    const m = mode || parsed.mode;
    const source = mode ? (modeSource || 'user') : (parsed.mode ? 'reference' : 'default');
    const note = !parsed.known ? 'reference outside the known sequences' : null;
    const [r] = await conn.query(
        `INSERT INTO shipments
            (reference, reference_seq, name, mode, mode_source, stage, needs_review, review_note,
             origin, source_draft_id, created_by_email, booked_at)
         VALUES (?, ?, ?, ?, ?, 'BOOKED', ?, ?, ?, ?, ?, NOW())
         ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id)`,
        [ref, parsed.seq, str(name, 100), m, source, parsed.known && m ? 0 : 1, note,
         origin, sourceDraftId, userEmail || null]
    );
    return r.insertId;
}

async function upsertLine(conn, shipmentId, orderId, quantity, userEmail = null) {
    await conn.query(
        `INSERT INTO shipment_lines (shipment_id, order_id, quantity, created_by_email)
         VALUES (?, ?, ?, ?)
         ON DUPLICATE KEY UPDATE quantity = VALUES(quantity)`,
        [shipmentId, orderId, Number(quantity) || 0, userEmail || null]
    );
}

// Remove `orderIds` from every booked manifest except `keepShipmentId`'s.
// Draft / planned lines (allocation mirrors) are never touched here.
async function leaveBookedManifests(conn, orderIds, keepShipmentId = null) {
    const ids = intIds(orderIds);
    if (!ids.length) return;
    await conn.query(
        `DELETE sl FROM shipment_lines sl
           JOIN shipments s ON s.id = sl.shipment_id
          WHERE sl.order_id IN (${ph(ids)}) AND s.stage IN (${BOOKED_IN})
            AND NOT (sl.shipment_id <=> ?)`,
        [...ids, keepShipmentId]
    );
}

// A booked shipment's manifest == its live member orders, at their quantities.
async function rebuildBookedLines(conn, shipmentId) {
    const [members] = await conn.query(
        `SELECT id, quantity FROM orders WHERE shipment_id = ? AND deleted_at IS NULL ORDER BY id FOR SHARE`,
        [shipmentId]
    );
    const keep = members.map(m => m.id);
    await conn.query(
        `DELETE FROM shipment_lines WHERE shipment_id = ?${keep.length ? ` AND order_id NOT IN (${ph(keep)})` : ''}`,
        [shipmentId, ...keep]
    );
    for (const m of members) await upsertLine(conn, shipmentId, m.id, m.quantity);
}

// Refresh the stored eta / vessel / carrier-ref copies of booked shipments
// from their live members (never the stage), and infer a still-unknown mode.
// Reads the members without locking: the copies are advisory (the read path
// takes them from the member orders directly) and a locking read here would
// queue behind any legacy transaction holding those orders.
async function refreshBookedCopies(conn, shipmentIds) {
    for (const id of intIds(shipmentIds)) {
        const [[s]] = await conn.query(
            `SELECT id, reference, stage, mode, mode_source, tracking_ref, eta, vessel_name, needs_review, review_note
               FROM shipments WHERE id = ?`,
            [id]
        );
        if (!s || !S.isBookedStage(s.stage)) continue;
        const [members] = await conn.query(
            `SELECT status, eta, vessel_name, external_container_number, awb_number
               FROM orders WHERE shipment_id = ? AND deleted_at IS NULL`,
            [id]
        );
        if (!members.length) continue;
        const sets = [];
        const vals = [];
        let mode = s.mode;
        if (!mode && s.mode_source !== 'user') {
            const inferred = S.inferMode({
                reference: s.reference,
                statuses: members.map(m => m.status),
                trackingRef: S.trackingRefFor(null, members).trackingRef,
            });
            if (inferred.mode) {
                mode = inferred.mode;
                sets.push('mode = ?', 'mode_source = ?');
                vals.push(inferred.mode, inferred.source);
            }
        }
        const tracking = S.trackingRefFor(mode, members).trackingRef;
        const eta = members.map(m => m.eta).filter(Boolean).sort().pop() || null;
        const vessel = mostFrequent(members.map(m => S.clean(m.vessel_name)));
        if (tracking && tracking !== s.tracking_ref) { sets.push('tracking_ref = ?'); vals.push(tracking.slice(0, 255)); }
        if (eta && eta !== s.eta) { sets.push('eta = ?'); vals.push(eta); }
        if (vessel && vessel !== s.vessel_name) { sets.push('vessel_name = ?'); vals.push(vessel.slice(0, 255)); }
        if (!sets.length) continue;
        await conn.query(`UPDATE shipments SET ${sets.join(', ')} WHERE id = ?`, [...vals, id]);
    }
}

function mostFrequent(values) {
    const counts = new Map();
    for (const v of values) if (v) counts.set(v, (counts.get(v) || 0) + 1);
    let best = null;
    for (const [v, n] of counts) if (best === null || n > counts.get(best)) best = v;
    return best;
}

// ── Order membership ─────────────────────────────────────────────────────
// Bring `orderIds` into line with their container numbers: each live order
// with a reference belongs to the live shipment holding it (created BOOKED on
// first sight), its manifest line carries its quantity, and it leaves every
// other booked manifest. A live order without a reference belongs to none. A
// soft-deleted order leaves every booked manifest but keeps its shipment_id as
// history (draft and planned lines stay, mirroring legacy). The previous
// shipment is read from orders.shipment_id, which nothing else writes. A
// hand-edited containerNumber follows the same rule, so a typo creates a
// needs_review shipment that verify reports as a memberless orphan once fixed.
// Returns { [orderId]: shipmentId | null }.
async function syncOrderMembership(conn, orderIds, { userEmail = null, origin = 'legacy_route' } = {}) {
    const ids = intIds(orderIds);
    const out = {};
    if (!ids.length) return out;

    // Discover the references without locking so the shipment rows can be
    // locked (created) before the order rows.
    const [peek] = await conn.query(
        `SELECT id, NULLIF(TRIM(container_number), '') AS ref, shipment_id FROM orders WHERE id IN (${ph(ids)})`,
        ids
    );
    const refToId = new Map();
    for (const ref of [...new Set(peek.map(r => r.ref).filter(Boolean))].sort()) {
        refToId.set(ref, await findOrCreateByReference(conn, ref, { origin, userEmail }));
    }
    const targets = new Set(refToId.values());
    const previous = intIds(peek.map(r => r.shipment_id)).filter(id => !targets.has(id));
    if (previous.length) {
        await conn.query(`SELECT id FROM shipments WHERE id IN (${ph(previous)}) ORDER BY id FOR UPDATE`, previous);
    }

    const [rows] = await conn.query(
        `SELECT id, NULLIF(TRIM(container_number), '') AS ref, quantity, deleted_at, shipment_id
           FROM orders WHERE id IN (${ph(ids)}) ORDER BY id FOR UPDATE`,
        ids
    );
    const touched = new Set([...targets, ...previous]);
    for (const row of rows) {
        if (row.shipment_id) touched.add(row.shipment_id);
        if (row.deleted_at) {
            await leaveBookedManifests(conn, [row.id], null);
            out[row.id] = row.shipment_id ?? null;
            continue;
        }
        let target = null;
        if (row.ref) {
            // The number changed between the discovery read and the lock.
            if (!refToId.has(row.ref)) refToId.set(row.ref, await findOrCreateByReference(conn, row.ref, { origin, userEmail }));
            target = refToId.get(row.ref);
            touched.add(target);
        }
        if ((row.shipment_id ?? null) !== target) {
            await conn.query(
                `UPDATE orders SET shipment_id = ?, last_updated = last_updated WHERE id = ?`,
                [target, row.id]
            );
        }
        if (target) await upsertLine(conn, target, row.id, row.quantity, userEmail);
        await leaveBookedManifests(conn, [row.id], target);
        out[row.id] = target;
    }
    await refreshBookedCopies(conn, [...touched]);
    return out;
}

// ── Drafts ───────────────────────────────────────────────────────────────
async function lockRegistryRow(conn, name) {
    const [rows] = await conn.query(
        `SELECT id, name, shipment_id, closed_reason, container_number FROM draft_containers WHERE name = ? FOR UPDATE`,
        [name]
    );
    return rows[0] || null;
}

async function lockAllocations(conn, kind, name) {
    const [rows] = kind === 'draft'
        ? await conn.query(
            `SELECT order_id, allocated FROM draft_container_allocations WHERE draft_container_name = ? ORDER BY id FOR SHARE`,
            [name])
        : await conn.query(
            `SELECT order_id, allocated FROM planned_container_allocations WHERE planned_container_name = ? ORDER BY id FOR SHARE`,
            [name]);
    return rows;
}

// An open shipment's lines == the legacy allocation rows for its name, one to
// one, including rows whose order has since been soft-deleted (legacy keeps
// them and hides them with an INNER JOIN; reads here do the same).
async function mirrorAllocationLines(conn, shipmentId, allocs, userEmail = null) {
    const [cur] = await conn.query(
        `SELECT order_id, quantity FROM shipment_lines WHERE shipment_id = ? FOR UPDATE`,
        [shipmentId]
    );
    const have = new Map(cur.map(l => [Number(l.order_id), Number(l.quantity)]));
    const want = new Map(allocs.map(a => [Number(a.order_id), Number(a.allocated) || 0]));
    for (const [orderId, qty] of want) {
        if (have.get(orderId) !== qty) await upsertLine(conn, shipmentId, orderId, qty, userEmail);
    }
    const gone = [...have.keys()].filter(id => !want.has(id));
    if (gone.length) {
        await conn.query(
            `DELETE FROM shipment_lines WHERE shipment_id = ? AND order_id IN (${ph(gone)})`,
            [shipmentId, ...gone]
        );
    }
}

async function stampDraftDocuments(conn, name, shipmentId) {
    await conn.query(
        `UPDATE draft_container_documents SET shipment_id = ? WHERE draft_container_name = ? AND shipment_id IS NULL`,
        [shipmentId, name]
    );
    await conn.query(
        `UPDATE quality_assurance_documents SET shipment_id = ? WHERE draft_container_name = ? AND shipment_id IS NULL`,
        [shipmentId, name]
    );
}

async function createOpenShipment(conn, { stage, name, origin, sourceDraftId = null, userEmail = null, mode = null, modeSource = null }) {
    const openKey = `${stage === 'PLANNED' ? 'P' : 'D'}:${name}`;
    const m = mode || S.modeFromName(name);
    const [r] = await conn.query(
        `INSERT INTO shipments (name, open_key, mode, mode_source, stage, origin, source_draft_id, created_by_email)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id)`,
        [name, openKey, m, m ? (modeSource || (mode ? 'user' : 'draft_name')) : 'default', stage, origin, sourceDraftId, userEmail || null]
    );
    const row = await lockShipment(conn, r.insertId);
    // Callers only get here after a locking read found no open shipment for the
    // key, so this is a creation (a concurrent creator winning the race is the
    // one exception, and costs a duplicate 'create' row at worst).
    await shipmentAudit(conn, {
        shipmentId: row.id, action: 'create', after: { stage, name, mode: row.mode, origin }, userEmail,
    });
    return row;
}

// Resolve the open DRAFT shipment of a draft name, applying the generation
// rule. A registry row belongs to a NAME and is reused (and silently reopened)
// forever; a shipment belongs to one GENERATION of that name. So the open
// shipment is found by open_key 'D:<name>', never by following the registry
// pointer blindly. With no open shipment, the registry's pointer decides:
//   CANCELLED     -> revive it to DRAFT and restore the open_key
//   DRAFT         -> adopt it (the draft was renamed while nothing watched)
//   BOOKED+       -> a new generation of the name: with live allocations (or
//                    createIfMissing) a fresh DRAFT shipment, and the registry
//                    is repointed (earlier documents keep their own stamp);
//                    with none (a QA sheet raised for a converted draft, which
//                    legacy "reopens") the documents are stamped onto the
//                    booked shipment and nothing else changes.
// Returns { shipment } | { stampOnto: shipmentId } | null.
async function resolveOpenDraft(conn, name, { reg, allocs, origin, userEmail, createIfMissing }) {
    const openKey = `D:${name}`;
    let open = await lockOpenShipment(conn, openKey);
    if (open) {
        if (open.open_key !== openKey || open.name !== name) {
            await conn.query(`UPDATE shipments SET open_key = ?, name = ? WHERE id = ?`, [openKey, name, open.id]);
            open = { ...open, open_key: openKey, name };
        }
        return { shipment: open };
    }
    if (reg && reg.shipment_id) {
        const pointed = await followToLive(conn, await lockShipment(conn, reg.shipment_id));
        if (pointed) {
            if (pointed.stage === 'CANCELLED') {
                await conn.query(
                    `UPDATE shipments
                        SET stage = 'DRAFT', open_key = ?, name = ?, cancelled_at = NULL, cancelled_reason = NULL
                      WHERE id = ?`,
                    [openKey, name, pointed.id]
                );
                await shipmentAudit(conn, {
                    shipmentId: pointed.id, action: 'reopened',
                    before: { stage: 'CANCELLED' }, after: { stage: 'DRAFT', name }, userEmail,
                });
                return { shipment: { ...pointed, stage: 'DRAFT', open_key: openKey, name } };
            }
            // Adopt only a draft that came from this registry row: a pointer at
            // another name's open draft must never move that draft's key.
            if (pointed.stage === 'DRAFT' && (pointed.source_draft_id == null || pointed.source_draft_id === reg.id)) {
                await conn.query(`UPDATE shipments SET open_key = ?, name = ? WHERE id = ?`, [openKey, name, pointed.id]);
                return { shipment: { ...pointed, open_key: openKey, name } };
            }
            if (S.isBookedStage(pointed.stage) && !allocs.length && !createIfMissing) {
                return { stampOnto: pointed.id };
            }
        }
    }
    // Nothing open to mirror: no allocation rows, and the name is unregistered
    // or its registry row is closed (converted / deleted). A registered, open,
    // empty draft (every line removed one at a time) is mirrored as an empty
    // DRAFT, exactly as the registry reports it.
    if (!allocs.length && !createIfMissing && (!reg || reg.closed_reason)) return null;
    const created = await createOpenShipment(conn, {
        stage: 'DRAFT', name, origin, sourceDraftId: reg ? reg.id : null, userEmail,
    });
    return { shipment: created };
}

// Mirror one legacy draft name into its open DRAFT shipment: lines, label,
// registry pointer and the name's unstamped documents. `syncDraft` never
// mirrors lines onto a booked shipment. Returns { shipmentId, stampedOnly } or
// null when the name has nothing to mirror.
async function syncDraft(conn, rawName, { userEmail = null, origin = 'legacy_route', createIfMissing = false } = {}) {
    const asked = str(rawName, 100);
    if (!asked) return null;
    // Names match case-insensitively (the keys' collation); the registry holds
    // the canonical spelling, so a case-only rename is never undone by a
    // follow-up sync of the old spelling.
    const reg = await lockRegistryRow(conn, asked);
    const name = reg ? reg.name : asked;
    const allocs = await lockAllocations(conn, 'draft', name);
    const resolved = await resolveOpenDraft(conn, name, { reg, allocs, origin, userEmail, createIfMissing });
    if (!resolved) return null;
    if (resolved.stampOnto) {
        await stampDraftDocuments(conn, name, resolved.stampOnto);
        return { shipmentId: resolved.stampOnto, stampedOnly: true };
    }
    const ship = resolved.shipment;
    await mirrorAllocationLines(conn, ship.id, allocs, userEmail);
    if (!ship.mode && ship.mode_source !== 'user') {
        const m = S.modeFromName(name);
        if (m) await conn.query(`UPDATE shipments SET mode = ?, mode_source = 'draft_name' WHERE id = ?`, [m, ship.id]);
    }
    if (reg && reg.shipment_id !== ship.id) {
        await conn.query(`UPDATE draft_containers SET shipment_id = ? WHERE id = ?`, [ship.id, reg.id]);
    }
    if (reg && ship.source_draft_id == null) {
        await conn.query(`UPDATE shipments SET source_draft_id = ? WHERE id = ?`, [reg.id, ship.id]);
    }
    await stampDraftDocuments(conn, name, ship.id);
    return { shipmentId: ship.id, stampedOnly: false };
}

// ── Planned ──────────────────────────────────────────────────────────────
// Planned containers have no registry: a planned container IS its allocation
// rows. The shadow is a PLANNED shipment held by open_key 'P:<name>'; when the
// last row goes, the shipment is cancelled and its key released.
async function syncPlanned(conn, rawName, { userEmail = null, origin = 'legacy_route', createIfMissing = false } = {}) {
    const asked = str(rawName, 100);
    if (!asked) return null;
    // Canonical spelling: the oldest allocation row's (names match
    // case-insensitively, so the rows of one planned container can differ).
    const [spelled] = await conn.query(
        `SELECT planned_container_name AS name FROM planned_container_allocations
          WHERE planned_container_name = ? ORDER BY id LIMIT 1`,
        [asked]
    );
    const name = spelled.length ? spelled[0].name : asked;
    const openKey = `P:${name}`;
    let ship = await lockOpenShipment(conn, openKey);
    const allocs = await lockAllocations(conn, 'planned', name);
    if (!allocs.length && !createIfMissing) {
        if (!ship) return null;
        await cancelShipment(conn, ship, { reason: 'emptied', userEmail });
        return { shipmentId: ship.id, cancelled: true };
    }
    if (!ship) {
        ship = await createOpenShipment(conn, { stage: 'PLANNED', name, origin, userEmail });
    } else if (ship.open_key !== openKey || ship.name !== name) {
        await conn.query(`UPDATE shipments SET open_key = ?, name = ? WHERE id = ?`, [openKey, name, ship.id]);
    }
    await mirrorAllocationLines(conn, ship.id, allocs, userEmail);
    return { shipmentId: ship.id, cancelled: false };
}

// Move an open shipment to CANCELLED, releasing its keys and dropping its lines.
async function cancelShipment(conn, ship, { reason, userEmail = null, softDelete = false }) {
    await conn.query(
        `UPDATE shipments
            SET stage = 'CANCELLED', reference = NULL, open_key = NULL,
                cancelled_at = COALESCE(cancelled_at, NOW()), cancelled_reason = ?${softDelete ? ', deleted_at = NOW()' : ''}
          WHERE id = ?`,
        [String(reason || 'cancelled').slice(0, 32), ship.id]
    );
    await conn.query(`DELETE FROM shipment_lines WHERE shipment_id = ?`, [ship.id]);
    await shipmentAudit(conn, {
        shipmentId: ship.id, action: softDelete ? 'deleted' : 'cancelled',
        before: { stage: ship.stage, name: ship.name || null },
        after: { stage: 'CANCELLED', reason: reason || null },
        userEmail,
    });
}

// ── Merge ────────────────────────────────────────────────────────────────
const MERGE_COALESCE = [
    'name', 'tracking_ref', 'booking_ref', 'bl_number', 'forwarder', 'vessel_name', 'origin_port',
    'etd', 'eta', 'ata', 'notes', 'source_draft_id', 'booked_at', 'departed_at', 'arrived_at', 'closed_at',
];

// Fold `loser` into `survivor`: the survivor keeps its non-null header fields
// and fills the gaps from the loser, keeps the later stage, and takes over the
// loser's member orders, documents and registry pointers. The loser releases
// its keys and is marked merged + deleted. `reference`, when given, is taken by
// the survivor after the loser has released it. A merge never changes a booked
// survivor's reference. Rebuilds the survivor's booked manifest when it ends
// up booked.
async function mergeShipments(conn, survivorId, loserId, { reference = null, userEmail = null } = {}) {
    const [first, second] = survivorId < loserId ? [survivorId, loserId] : [loserId, survivorId];
    const a = await lockShipment(conn, first);
    const b = await lockShipment(conn, second);
    const survivor = a && a.id === survivorId ? a : b;
    const loser = a && a.id === loserId ? a : b;
    if (!survivor || !loser) throw new Error(`mergeShipments: missing ${survivor ? loserId : survivorId}`);

    await conn.query(
        `UPDATE shipments SET reference = NULL, open_key = NULL, merged_into_id = ?, deleted_at = NOW() WHERE id = ?`,
        [survivor.id, loser.id]
    );

    const sets = [];
    const vals = [];
    for (const col of MERGE_COALESCE) {
        if (survivor[col] == null && loser[col] != null) { sets.push(`${col} = ?`); vals.push(loser[col]); }
    }
    if (!survivor.mode && loser.mode) {
        sets.push('mode = ?', 'mode_source = ?');
        vals.push(loser.mode, loser.mode_source || 'default');
    }
    const stage = S.isBookedStage(loser.stage) || S.isBookedStage(survivor.stage)
        ? S.laterStage(S.isBookedStage(survivor.stage) ? survivor.stage : 'BOOKED', loser.stage)
        : survivor.stage;
    if (stage !== survivor.stage) { sets.push('stage = ?'); vals.push(stage); }
    if (S.isBookedStage(stage)) sets.push('open_key = NULL');
    if (reference && !(S.isBookedStage(survivor.stage) && survivor.reference)) {
        const parsed = S.parseReference(reference);
        sets.push('reference = ?', 'reference_seq = ?');
        vals.push(parsed.reference.slice(0, 100), parsed.seq);
    }
    if (sets.length) await conn.query(`UPDATE shipments SET ${sets.join(', ')} WHERE id = ?`, [...vals, survivor.id]);

    const [moved] = await conn.query(
        `SELECT id FROM orders WHERE shipment_id = ? ORDER BY id FOR UPDATE`,
        [loser.id]
    );
    for (const o of moved) {
        await conn.query(`UPDATE orders SET shipment_id = ?, last_updated = last_updated WHERE id = ?`, [survivor.id, o.id]);
    }
    await conn.query(`DELETE FROM shipment_lines WHERE shipment_id = ?`, [loser.id]);
    await conn.query(`UPDATE draft_container_documents SET shipment_id = ? WHERE shipment_id = ?`, [survivor.id, loser.id]);
    await conn.query(`UPDATE quality_assurance_documents SET shipment_id = ? WHERE shipment_id = ?`, [survivor.id, loser.id]);
    await conn.query(`UPDATE draft_containers SET shipment_id = ? WHERE shipment_id = ?`, [survivor.id, loser.id]);
    if (S.isBookedStage(stage)) await rebuildBookedLines(conn, survivor.id);

    await shipmentAudit(conn, {
        shipmentId: survivor.id, action: 'merged',
        before: { mergedId: loser.id, mergedStage: loser.stage, mergedReference: loser.reference || null, mergedName: loser.name || null },
        after: { stage, reference: reference || survivor.reference || null, movedOrders: moved.length },
        userEmail,
    });
    return { survivorId: survivor.id, loserId: loser.id, stage, movedOrders: moved.length };
}

// ── The close hook ───────────────────────────────────────────────────────
// Called after draftAudit.closeDraft succeeded with neither notFound nor
// alreadyClosed (a repeat close with a different reason is a legacy no-op and
// is one here). 'deleted' cancels the draft's open shipment. 'converted' into
// number N resolves D = the draft's open shipment and B = the live holder of N:
//   - B survives when it pre-dates this conversion (created by the API or the
//     backfill, stored stage past BOOKED, or holding members beyond the orders
//     this conversion packed), so a top-up pack into container 308 never
//     retires 308's id;
//   - otherwise D survives and takes N.
// Pack-then-close and close-then-pack converge on the same single shipment.
// The conversion form's etd / port / vessel / eta / carrier ref fill gaps in
// the survivor's header; they are not written to orders.
async function syncDraftClose(conn, { name: rawName, reason, details = {}, closeResult = {}, userEmail = null }) {
    const name = str(rawName, 100);
    if (!name) return null;
    const reg = closeResult.id ? (await conn.query(
        `SELECT id, name, shipment_id FROM draft_containers WHERE id = ? FOR UPDATE`, [closeResult.id]
    ))[0][0] || null : await lockRegistryRow(conn, name);

    let draft = await lockOpenShipment(conn, `D:${name}`);
    if (!draft && reg && reg.shipment_id) {
        const pointed = await followToLive(conn, await lockShipment(conn, reg.shipment_id));
        if (pointed && pointed.stage === 'DRAFT') draft = pointed;
    }

    if (reason === 'deleted') {
        if (!draft) return null;
        await cancelShipment(conn, draft, { reason: 'deleted', userEmail });
        if (reg && reg.shipment_id !== draft.id) {
            await conn.query(`UPDATE draft_containers SET shipment_id = ? WHERE id = ?`, [draft.id, reg.id]);
        }
        await stampDraftDocuments(conn, name, draft.id);
        return { shipmentId: draft.id, cancelled: true };
    }
    if (reason !== 'converted') return null;

    const parsed = S.parseReference(details.containerNumber);
    if (!parsed) return null;
    const holder = await lockByReference(conn, parsed.reference);
    let survivorId;
    let merged = null;
    if (!draft && !holder) {
        survivorId = await findOrCreateByReference(conn, parsed.reference, {
            origin: 'legacy_route', userEmail, name, sourceDraftId: reg ? reg.id : null,
            mode: freightMode(details.freightType) && !parsed.mode ? freightMode(details.freightType) : null,
            modeSource: 'freight_type',
        });
    } else if (!draft) {
        survivorId = holder.id;
    } else if (!holder) {
        survivorId = draft.id;
        await conn.query(
            `UPDATE shipments
                SET reference = ?, reference_seq = ?, stage = 'BOOKED', open_key = NULL,
                    booked_at = COALESCE(booked_at, NOW())
              WHERE id = ?`,
            [parsed.reference.slice(0, 100), parsed.seq, draft.id]
        );
        await rebuildBookedLines(conn, draft.id);
    } else {
        const predates = await holderPredatesConversion(conn, holder, closeResult, details);
        const [keep, fold] = predates ? [holder, draft] : [draft, holder];
        merged = await mergeShipments(conn, keep.id, fold.id, { reference: parsed.reference, userEmail });
        survivorId = keep.id;
        if (!predates) {
            await conn.query(
                `UPDATE shipments SET stage = IF(stage IN (${BOOKED_IN}), stage, 'BOOKED'), open_key = NULL,
                        booked_at = COALESCE(booked_at, NOW()) WHERE id = ?`,
                [survivorId]
            );
            await rebuildBookedLines(conn, survivorId);
        }
    }

    await applyConversionDetails(conn, survivorId, { name, parsed, details, reg });
    if (reg) await conn.query(`UPDATE draft_containers SET shipment_id = ? WHERE id = ?`, [survivorId, reg.id]);
    await stampDraftDocuments(conn, name, survivorId);
    await refreshBookedCopies(conn, [survivorId]);
    await shipmentAudit(conn, {
        shipmentId: survivorId, action: 'converted',
        before: { draftName: name, draftShipmentId: draft ? draft.id : null },
        after: { reference: parsed.reference, mergedShipmentId: merged ? merged.loserId : null },
        userEmail,
    });
    return { shipmentId: survivorId, mergedShipmentId: merged ? merged.loserId : null };
}

function freightMode(freightType) {
    const f = String(freightType || '').trim().toUpperCase();
    if (f.includes('AIR')) return 'AIR';
    if (f.includes('SEA') || f.includes('OCEAN')) return 'SEA';
    if (f.includes('ROAD') || f.includes('TRUCK')) return 'ROAD';
    return null;
}

// Did the live holder of the conversion's number exist before this conversion?
// It did when the API or the backfill created it, when its stored stage is past
// BOOKED, or when it holds a member this conversion does not explain. The
// conversion explains the draft's lines and the close form's packs, plus the
// split children a partial pack just created: recent rows copying one of those
// orders' (jf_code, po_number, purchase_order_id). Getting this wrong only
// changes which id survives the merge, never the membership.
async function holderPredatesConversion(conn, holder, closeResult, details) {
    if (holder.origin !== 'legacy_route') return true;
    if (S.stageRank(holder.stage) > 1) return true;
    const explained = intIds([
        ...(Array.isArray(closeResult.lines) ? closeResult.lines.map(l => l && l.orderId) : []),
        ...(Array.isArray(details.packs) ? details.packs.map(p => p && p.orderId) : []),
    ]);
    if (!explained.length) {
        const [[any]] = await conn.query(
            `SELECT COUNT(*) AS n FROM orders WHERE shipment_id = ? AND deleted_at IS NULL`, [holder.id]
        );
        return Number(any.n) > 0;
    }
    const [[row]] = await conn.query(
        `SELECT COUNT(*) AS n FROM orders m
          WHERE m.shipment_id = ? AND m.deleted_at IS NULL
            AND m.id NOT IN (${ph(explained)})
            AND NOT (m.created_at >= NOW() - INTERVAL 30 MINUTE
                     AND EXISTS (SELECT 1 FROM orders x
                                  WHERE x.id IN (${ph(explained)})
                                    AND x.jf_code <=> m.jf_code
                                    AND x.po_number <=> m.po_number
                                    AND x.purchase_order_id <=> m.purchase_order_id))`,
        [holder.id, ...explained, ...explained]
    );
    return Number(row.n) > 0;
}

async function applyConversionDetails(conn, shipmentId, { name, parsed, details, reg }) {
    const [[s]] = await conn.query(`SELECT * FROM shipments WHERE id = ?`, [shipmentId]);
    if (!s) return;
    const sets = [];
    const vals = [];
    const fill = (col, value) => {
        if (s[col] == null && value != null && value !== '') { sets.push(`${col} = ?`); vals.push(value); }
    };
    fill('name', str(name, 100));
    fill('source_draft_id', reg ? reg.id : null);
    fill('etd', validDate(details.etd));
    fill('eta', validDate(details.eta));
    fill('origin_port', str(details.port, 255));
    fill('vessel_name', str(details.vesselName, 255));
    const mode = s.mode || parsed.mode || freightMode(details.freightType);
    const carrier = mode === 'AIR'
        ? str(details.awbNumber, 255) || str(details.externalContainerNumber, 255)
        : str(details.externalContainerNumber, 255) || str(details.awbNumber, 255);
    fill('tracking_ref', carrier);
    if (!s.mode && mode) {
        sets.push('mode = ?', 'mode_source = ?');
        vals.push(mode, parsed.mode ? 'reference' : 'freight_type');
    }
    if (s.mode && parsed.mode && s.mode !== parsed.mode) {
        sets.push('needs_review = 1', 'review_note = ?');
        vals.push(`mode ${s.mode} contradicts reference ${parsed.reference}`.slice(0, 255));
    }
    if (sets.length) await conn.query(`UPDATE shipments SET ${sets.join(', ')} WHERE id = ?`, [...vals, shipmentId]);
}

function validDate(v) {
    const s = S.clean(v);
    if (!s) return null;
    const m = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (!m) return null;
    const d = new Date(`${m[1]}-${m[2]}-${m[3]}T00:00:00Z`);
    return Number.isNaN(d.getTime()) ? null : `${m[1]}-${m[2]}-${m[3]}`;
}

// ── Reads ────────────────────────────────────────────────────────────────
// One query computes every shipment's effective stage: the later of the stored
// stage and the stage its live members justify (lib/shipments.js
// effectiveStage), plus the member-derived eta / vessel / carrier refs that the
// API shows for booked shipments. Open shipments count their lines the way the
// legacy Draft tab does: INNER JOIN to a live order.
function shipmentSelect({ where = [], effectiveWhere = [], orderBy = 'COALESCE(z.booked_at, z.created_at) DESC, z.id DESC', limit = null, idFilter = null } = {}) {
    const memberScope = idFilter ? ` AND o.shipment_id IN (${idFilter})` : '';
    const lineScope = idFilter ? ` WHERE sl.shipment_id IN (${idFilter})` : '';
    return `
        SELECT z.* FROM (
          SELECT y.*,
                 CASE WHEN y.stage IN (${BOOKED_IN})
                      THEN ELT(GREATEST(FIELD(y.stage, ${BOOKED_IN}),
                                        FIELD(COALESCE(y.derived_stage, ''), ${BOOKED_IN})),
                               ${BOOKED_IN})
                      ELSE y.stage END AS effective_stage
            FROM (
              SELECT s.*,
                     COALESCE(m.member_count, 0) AS member_count,
                     m.member_eta, m.member_etd, m.member_vessel, m.member_ext, m.member_awb,
                     COALESCE(l.line_count, 0) AS line_count,
                     CASE WHEN s.stage IN (${BOOKED_IN}) THEN COALESCE(m.member_units, 0)
                          ELSE COALESCE(l.line_units, 0) END AS total_units,
                     CASE WHEN s.stage NOT IN (${BOOKED_IN}) OR COALESCE(m.member_count, 0) = 0 THEN NULL
                          WHEN m.n_terminal = m.member_count THEN 'CLOSED'
                          WHEN m.n_arrived + m.n_terminal > 0 THEN 'ARRIVED'
                          WHEN m.n_transit > 0 THEN 'IN_TRANSIT'
                          ELSE 'BOOKED' END AS derived_stage
                FROM shipments s
                LEFT JOIN (
                    SELECT o.shipment_id,
                           COUNT(*) AS member_count,
                           COALESCE(SUM(o.quantity), 0) AS member_units,
                           SUM(o.status IN ('RECEIVED', 'PARTIALLY_RECEIVED', 'DESTROYED')) AS n_terminal,
                           SUM(o.status = 'ARRIVED_AT_WAREHOUSE') AS n_arrived,
                           SUM(o.status IN ('ON_SEA', 'ON_AIR')) AS n_transit,
                           MAX(o.eta) AS member_eta,
                           MAX(o.estimated_departure_date) AS member_etd,
                           MAX(NULLIF(TRIM(o.vessel_name), '')) AS member_vessel,
                           MAX(NULLIF(TRIM(o.external_container_number), '')) AS member_ext,
                           MAX(NULLIF(TRIM(o.awb_number), '')) AS member_awb
                      FROM orders o
                     WHERE o.deleted_at IS NULL AND o.shipment_id IS NOT NULL${memberScope}
                     GROUP BY o.shipment_id
                ) m ON m.shipment_id = s.id
                LEFT JOIN (
                    SELECT sl.shipment_id, COUNT(*) AS line_count, COALESCE(SUM(sl.quantity), 0) AS line_units
                      FROM shipment_lines sl
                      JOIN orders lo ON lo.id = sl.order_id AND lo.deleted_at IS NULL${lineScope}
                     GROUP BY sl.shipment_id
                ) l ON l.shipment_id = s.id
               ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
            ) y
        ) z
        ${effectiveWhere.length ? 'WHERE ' + effectiveWhere.join(' AND ') : ''}
        ORDER BY ${orderBy}
        ${limit ? `LIMIT ${Number(limit)}` : ''}
    `;
}

// Filtered list, as rows (map with S.rowToShipment). Every filter is optional.
//   stages: effective stages; ids: explicit ids (then deleted / merged rows
//   are included, so a stale pointer still resolves).
async function selectShipments(conn, {
    ids = null, stages = null, mode = null, q = null, reference = null, trackingRef = null,
    name = null, needsReview = null, limit = null, includeInactive = false,
} = {}) {
    const where = [];
    const params = [];
    let idFilter = null;
    const idList = ids ? intIds(ids) : null;
    if (idList) {
        if (!idList.length) return [];
        idFilter = idList.join(',');
        where.push(`s.id IN (${idFilter})`);
    }
    if (!idList && !includeInactive) where.push(LIVE);
    if (mode) { where.push('s.mode = ?'); params.push(mode); }
    if (reference) { where.push('s.reference = ?'); params.push(String(reference).trim()); }
    if (trackingRef) { where.push('s.tracking_ref = ?'); params.push(String(trackingRef).trim()); }
    if (name) { where.push('s.name = ?'); params.push(String(name).trim()); }
    if (needsReview != null) { where.push('s.needs_review = ?'); params.push(needsReview ? 1 : 0); }
    if (q) {
        const like = `%${String(q).trim()}%`;
        where.push('(s.reference LIKE ? OR s.name LIKE ? OR s.tracking_ref LIKE ? OR s.booking_ref LIKE ? OR s.bl_number LIKE ?)');
        params.push(like, like, like, like, like);
    }
    const effectiveWhere = [];
    const stageList = (stages || []).filter(S.isStage);
    if (stages && stages.length) {
        if (!stageList.length) return [];
        effectiveWhere.push(`z.effective_stage IN (${ph(stageList)})`);
        params.push(...stageList);
    }
    const [rows] = await conn.query(shipmentSelect({ where, effectiveWhere, limit, idFilter }), params);
    return rows;
}

async function getShipment(conn, id, { includeInactive = true } = {}) {
    const rows = await selectShipments(conn, { ids: [id], includeInactive });
    return rows[0] || null;
}

// GET /orders side-map: every shipment the listed orders point at, keyed by id,
// in the compact summary shape. Never throws: a missing table or column yields {}.
async function loadShipmentsForOrders(conn, orders) {
    try {
        const ids = intIds((orders || []).map(o => o && o.shipmentId));
        if (!ids.length) return {};
        const rows = await selectShipments(conn, { ids });
        const out = {};
        for (const r of rows) {
            const s = S.rowToShipment(r);
            out[s.id] = S.shipmentSummary(s);
        }
        return out;
    } catch (err) {
        log.warn('[shipments] side-map unavailable', { error: err.message });
        return {};
    }
}

// Lines of one shipment with their order summaries. Lines on soft-deleted
// orders are hidden, like the legacy allocation reads.
async function loadLines(conn, shipmentId) {
    return (await loadLinesFor(conn, [shipmentId])).get(Number(shipmentId)) || [];
}

// Lines of many shipments in one query: Map shipmentId -> lines.
async function loadLinesFor(conn, shipmentIds) {
    const ids = intIds(shipmentIds);
    const out = new Map(ids.map(id => [id, []]));
    if (!ids.length) return out;
    const [rows] = await conn.query(
        `SELECT sl.id, sl.shipment_id, sl.order_id, sl.quantity, sl.created_at, sl.updated_at,
                o.jf_code, o.asin, o.product_name, o.quantity AS order_quantity, o.status AS order_status,
                o.supplier, o.po_number, o.purchase_order_id, o.container_number, o.eta
           FROM shipment_lines sl
           INNER JOIN orders o ON o.id = sl.order_id AND o.deleted_at IS NULL
          WHERE sl.shipment_id IN (${ph(ids)})
          ORDER BY sl.shipment_id ASC, sl.id ASC`,
        ids
    );
    for (const r of rows) out.get(r.shipment_id).push(lineToJson(r));
    return out;
}

function lineToJson(r) {
    return {
        id: r.id,
        orderId: r.order_id,
        quantity: Number(r.quantity),
        overAllocated: r.order_quantity != null && Number(r.quantity) > Number(r.order_quantity),
        order: {
            id: r.order_id,
            jfCode: r.jf_code || null,
            asin: r.asin || null,
            productName: r.product_name || null,
            quantity: r.order_quantity != null ? Number(r.order_quantity) : null,
            status: r.order_status || null,
            supplier: r.supplier || null,
            poNumber: r.po_number || null,
            purchaseOrderId: r.purchase_order_id ?? null,
            containerNumber: r.container_number || null,
            eta: r.eta ? String(r.eta).slice(0, 10) : null,
        },
        createdAt: r.created_at?.toISOString?.() ?? r.created_at,
        updatedAt: r.updated_at?.toISOString?.() ?? r.updated_at,
    };
}

// ── Reference allocation ─────────────────────────────────────────────────
// Advisory next number per sequence: one past the highest of (a) the numbers
// live orders carry — so a memberless shipment (a typo, a test's 9001) cannot
// poison the sequence — and (b) the numbers open draft / planned names reserve
// ('... - 328'). ROAD has no sequence (null).
async function nextReference(conn, mode) {
    if (mode !== 'SEA' && mode !== 'AIR') return null;
    const [refRows] = await conn.query(
        `SELECT DISTINCT TRIM(container_number) AS ref FROM orders
          WHERE deleted_at IS NULL AND container_number IS NOT NULL AND TRIM(container_number) <> ''`
    );
    let maxBooked = 0;
    for (const r of refRows) {
        const p = S.parseReference(r.ref);
        if (p && p.mode === mode && p.seq > maxBooked) maxBooked = p.seq;
    }
    const [nameRows] = await conn.query(
        `SELECT DISTINCT draft_container_name AS name FROM draft_container_allocations
         UNION
         SELECT DISTINCT planned_container_name FROM planned_container_allocations`
    );
    let maxReserved = 0;
    const reservedBy = [];
    for (const r of nameRows) {
        const hint = S.parseNameHint(r.name);
        if (!hint || hint.mode !== mode) continue;
        reservedBy.push({ name: r.name, seq: hint.seq });
        if (hint.seq > maxReserved) maxReserved = hint.seq;
    }
    const seq = Math.max(maxBooked, maxReserved) + 1;
    reservedBy.sort((a, b) => b.seq - a.seq);
    return { mode, seq, reference: S.formatReference(mode, seq), maxBooked, maxReserved, reservedBy };
}

// Is `reference` free to take: no live shipment holds it and no live order
// carries it (the second guards against a stale shadow). Advisory, plain
// reads: the real guard is uk_reference, so a caller takes a reference with an
// UPDATE and treats ER_DUP_ENTRY as "taken" (a locking read here would gap-lock
// the key and deadlock two concurrent bookings of the same next number).
async function referenceIsFree(conn, reference, { exceptShipmentId = null } = {}) {
    const [s] = await conn.query(
        `SELECT id FROM shipments s WHERE s.reference = ? AND ${LIVE} AND NOT (s.id <=> ?) LIMIT 1`,
        [reference, exceptShipmentId]
    );
    if (s.length) return false;
    const [o] = await conn.query(
        `SELECT id FROM orders WHERE TRIM(container_number) = ? AND deleted_at IS NULL
            AND NOT (shipment_id <=> ?) LIMIT 1`,
        [reference, exceptShipmentId]
    );
    return !o.length;
}

// ── Backfill (step 1) ────────────────────────────────────────────────────
// Seeds the shadow from the legacy tables. Dry-run unless `apply`. Idempotent
// on natural keys. Each phase runs in its own short transaction(s): never one
// long lock on orders in production.
//   1 booked    one shipment per distinct TRIM(container_number) of a live order
//   2 drafts    registry rows -> DRAFT shipments, name-hint links, documents-only
//   3 planned   one PLANNED shipment per planned name
//   4 documents stamp shipment_id from the registry row
//   5 orders    link orders.shipment_id in id batches, then the booked manifests
//   6 marker    write shipments_backfill_v1, which arms the dual-write hooks
async function backfillAll(conn, {
    apply = false, linkByNameHint = false, excludeHintIds = [], rearm = false, batchSize = 200, userEmail = null,
} = {}) {
    const report = {
        apply, linkByNameHint, references: [], anomalies: {}, drafts: [], planned: [], documents: {},
        next: {}, writes: { shipmentsCreated: 0, draftsSynced: 0, registryLinked: 0, documentsOnly: 0,
            plannedSynced: 0, documentsStamped: 0, qaDocumentsStamped: 0, ordersLinked: 0, ordersCleared: 0,
            linesUpserted: 0, linesRemoved: 0 },
    };
    const inTx = async (fn) => {
        await conn.query('START TRANSACTION');
        try {
            const r = await fn();
            await conn.query('COMMIT');
            return r;
        } catch (err) {
            try { await conn.query('ROLLBACK'); } catch (_) { /* gone */ }
            throw err;
        }
    };
    const excluded = new Set(intIds(excludeHintIds));

    // ── Phase 1: booked ─────────────────────────────────────────────────
    // Group live orders by reference SQL-side (the collation decides which
    // spellings are the same reference), then pair each group with its
    // existing shipment, also SQL-side.
    const [memberRows] = await conn.query(`
        SELECT o.id, g.ref, o.status, o.quantity, o.external_container_number, o.awb_number,
               o.eta, o.vessel_name, o.shipped_date, o.arrived_date, o.created_at, o.dates,
               s.id AS shipment_id
          FROM orders o
          JOIN (SELECT MIN(TRIM(container_number)) AS ref
                  FROM orders
                 WHERE deleted_at IS NULL AND container_number IS NOT NULL AND TRIM(container_number) <> ''
                 GROUP BY TRIM(container_number)) g ON g.ref = TRIM(o.container_number)
          LEFT JOIN shipments s ON s.reference = g.ref AND ${LIVE}
         WHERE o.deleted_at IS NULL
         ORDER BY g.ref, o.id`);
    const groups = new Map();
    for (const r of memberRows) {
        if (!groups.has(r.ref)) groups.set(r.ref, { ref: r.ref, shipmentId: r.shipment_id, members: [] });
        groups.get(r.ref).members.push(r);
    }
    const airSeqs = [];
    const seaSeqs = [];
    for (const g of groups.values()) {
        const p = S.parseReference(g.ref);
        if (p.mode === 'AIR') airSeqs.push(p.seq);
        if (p.mode === 'SEA') seaSeqs.push(p.seq);
    }
    const airRange = airSeqs.length ? [Math.min(...airSeqs), Math.max(...airSeqs)] : null;

    const carrierToRefs = new Map();
    const planned1 = [];
    for (const g of groups.values()) {
        const parsed = S.parseReference(g.ref);
        const statuses = g.members.map(m => m.status);
        const inferred = S.inferMode({
            reference: g.ref, statuses,
            trackingRef: S.trackingRefFor(null, g.members).trackingRef,
        });
        const tracking = S.trackingRefFor(inferred.mode, g.members);
        const derived = S.deriveStage(statuses);
        const statusMix = {};
        for (const st of statuses) statusMix[st] = (statusMix[st] || 0) + 1;
        const units = g.members.reduce((sum, m) => sum + Number(m.quantity || 0), 0);
        const reviewNotes = [];
        if (!parsed.known) reviewNotes.push('reference outside the known sequences');
        if (!inferred.mode) reviewNotes.push('mode unknown');
        if (!tracking.unanimous) reviewNotes.push(`members carry ${tracking.candidates.length} carrier refs`);
        if (parsed.mode === 'SEA' && airRange && parsed.seq >= airRange[0] && parsed.seq <= airRange[1]) {
            reviewNotes.push(`number falls inside the AIR sequence range ${airRange[0]}-${airRange[1]}`);
        }
        for (const c of tracking.candidates) {
            if (!carrierToRefs.has(c)) carrierToRefs.set(c, new Set());
            carrierToRefs.get(c).add(g.ref);
        }
        const conflicts = g.members
            .filter(m => S.clean(m.external_container_number) && S.clean(m.awb_number)
                && S.clean(m.external_container_number) !== S.clean(m.awb_number))
            .map(m => ({ orderId: m.id, external: S.clean(m.external_container_number), awb: S.clean(m.awb_number) }));
        const entry = {
            reference: g.ref,
            class: parsed.known ? parsed.mode.toLowerCase() : 'junk',
            referenceSeq: parsed.seq,
            mode: inferred.mode, modeSource: inferred.source,
            derivedStage: derived,
            statusMix,
            orders: g.members.length,
            units,
            trackingRef: tracking.trackingRef,
            trackingSource: tracking.sourceColumn,
            carrierCandidates: tracking.candidates,
            carrierConflicts: conflicts,
            needsReview: reviewNotes.length > 0,
            reviewNote: reviewNotes.join('; ') || null,
            shipmentId: g.shipmentId || null,
            action: g.shipmentId ? 'exists' : 'create',
        };
        report.references.push(entry);
        planned1.push({ g, entry });
    }
    report.references.sort((a, b) => (a.class === b.class
        ? (a.referenceSeq ?? 0) - (b.referenceSeq ?? 0) || a.reference.localeCompare(b.reference)
        : a.class.localeCompare(b.class)));
    report.anomalies.junk = report.references.filter(r => r.class === 'junk').map(r => r.reference);
    report.anomalies.mixedStatus = report.references.filter(r => Object.keys(r.statusMix).length > 1)
        .map(r => ({ reference: r.reference, statusMix: r.statusMix }));
    report.anomalies.multipleCarrierRefs = report.references.filter(r => r.carrierCandidates.length > 1)
        .map(r => ({ reference: r.reference, candidates: r.carrierCandidates }));
    report.anomalies.carrierRefOnManyReferences = [...carrierToRefs.entries()].filter(([, refs]) => refs.size > 1)
        .map(([carrier, refs]) => ({ carrier, references: [...refs] }));
    report.anomalies.externalVsAwb = report.references.filter(r => r.carrierConflicts.length)
        .map(r => ({ reference: r.reference, conflicts: r.carrierConflicts }));
    report.anomalies.sequenceOutliers = report.references
        .filter(r => r.reviewNote && r.reviewNote.includes('AIR sequence range')).map(r => r.reference);
    report.anomalies.unknownMode = report.references.filter(r => !r.mode).map(r => r.reference);

    if (apply) {
        await inTx(async () => {
            for (const { g, entry } of planned1) {
                const milestones = milestonesFor(g.members, entry.derivedStage);
                if (!g.shipmentId) {
                    const [r] = await conn.query(
                        `INSERT INTO shipments
                            (reference, reference_seq, mode, mode_source, stage, tracking_ref, vessel_name, eta,
                             needs_review, review_note, origin, created_by_email,
                             booked_at, departed_at, arrived_at, closed_at)
                         VALUES (?, ?, ?, ?, 'BOOKED', ?, ?, ?, ?, ?, 'backfill', ?, ?, ?, ?, ?)
                         ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id)`,
                        [g.ref.slice(0, 100), entry.referenceSeq, entry.mode, entry.modeSource || 'default',
                         entry.trackingRef ? entry.trackingRef.slice(0, 255) : null,
                         str(mostFrequent(g.members.map(m => S.clean(m.vessel_name))), 255),
                         g.members.map(m => m.eta).filter(Boolean).sort().pop() || null,
                         entry.needsReview ? 1 : 0, entry.reviewNote ? entry.reviewNote.slice(0, 255) : null,
                         userEmail, milestones.booked, milestones.departed, milestones.arrived, milestones.closed]
                    );
                    g.shipmentId = r.insertId;
                    entry.shipmentId = r.insertId;
                    report.writes.shipmentsCreated++;
                }
            }
        });
    }

    // ── Phase 2: drafts ─────────────────────────────────────────────────
    const [regRows] = await conn.query(`
        SELECT dc.id, dc.name, dc.shipment_id, dc.closed_reason, dc.container_number, dc.last_activity_at,
               (SELECT COUNT(*) FROM draft_container_allocations a WHERE a.draft_container_name = dc.name) AS alloc_rows,
               (SELECT COUNT(*) FROM draft_container_allocations a
                  JOIN orders o ON o.id = a.order_id AND o.deleted_at IS NULL
                 WHERE a.draft_container_name = dc.name) AS live_lines,
               (SELECT COUNT(*) FROM draft_container_documents d WHERE d.draft_container_name = dc.name) AS docs,
               (SELECT COUNT(*) FROM quality_assurance_documents q WHERE q.draft_container_name = dc.name) AS qa_docs
          FROM draft_containers dc
         ORDER BY dc.id`);
    const [orphanNames] = await conn.query(`
        SELECT DISTINCT a.draft_container_name AS name FROM draft_container_allocations a
         WHERE NOT EXISTS (SELECT 1 FROM draft_containers dc WHERE dc.name = a.draft_container_name)`);
    // Stamps of the drafts that are open now. A documents-only registry row
    // sharing one is an old name of that draft (renamed before the whole-draft
    // rename route kept documents with the draft), so the number in its name
    // describes the draft, not a booked container: never link it by number.
    const openStamps = new Map();
    for (const reg of regRows) {
        const stamp = S.nameStamp(reg.name);
        if (stamp && Number(reg.alloc_rows) > 0) openStamps.set(stamp, reg.name);
    }

    for (const reg of regRows) {
        const hint = S.parseNameHint(reg.name);
        const entry = {
            registryId: reg.id, name: reg.name, allocationRows: Number(reg.alloc_rows), liveLines: Number(reg.live_lines),
            documents: Number(reg.docs), qaDocuments: Number(reg.qa_docs), closedReason: reg.closed_reason || null,
            hint: hint ? hint.reference : null, hintMode: hint ? hint.stampMode : null,
            action: null, target: null, note: null,
        };
        report.drafts.push(entry);
        if (entry.allocationRows > 0) {
            entry.action = 'draft';
            if (apply) {
                const r = await inTx(() => syncDraft(conn, reg.name, { origin: 'backfill', userEmail }));
                entry.target = r ? r.shipmentId : null;
                report.writes.draftsSynced++;
            }
            continue;
        }
        if (reg.shipment_id) { entry.action = 'already-linked'; entry.target = reg.shipment_id; continue; }
        const hasDocs = entry.documents + entry.qaDocuments > 0;
        if (reg.closed_reason === 'converted' && S.clean(reg.container_number)) {
            const holder = await findBookedByReference(conn, reg.container_number, groups);
            if (holder) {
                entry.action = 'link-converted';
                entry.target = holder.ref;
                if (apply) await linkRegistry(conn, inTx, reg.id, holder.ref, report);
                continue;
            }
        }
        if (!hasDocs) { entry.action = 'none'; continue; }
        const sibling = openStamps.get(S.nameStamp(reg.name));
        if (sibling) entry.note = `old name of open draft "${sibling}"`;
        if (!reg.closed_reason && hint && !sibling) {
            const holder = await findBookedByReference(conn, hint.reference, groups);
            const holderMode = holder ? S.parseReference(holder.ref).mode : null;
            const contradicts = holder && hint.stampMode && holderMode && hint.stampMode !== holderMode;
            if (holder && !contradicts && !excluded.has(reg.id)) {
                entry.target = holder.ref;
                if (linkByNameHint) {
                    entry.action = 'link-hint';
                    if (apply) await linkRegistry(conn, inTx, reg.id, holder.ref, report);
                } else {
                    entry.action = 'pending-hint';
                    entry.note = 'links only with --link-by-name-hint';
                }
                continue;
            }
            if (holder && contradicts) entry.note = `hint mode ${hint.stampMode} contradicts ${holder.ref}`;
            if (holder && excluded.has(reg.id)) entry.note = 'excluded from name-hint linking';
            if (!holder) entry.note = `no booked shipment ${hint.reference}`;
        }
        entry.action = reg.closed_reason === 'deleted' ? 'own-cancelled' : 'own-closed';
        if (apply) {
            await inTx(async () => {
                const cancelled = reg.closed_reason === 'deleted';
                const [ins] = await conn.query(
                    `INSERT INTO shipments
                        (name, mode, mode_source, stage, review_note, origin, source_draft_id, created_by_email,
                         closed_at, cancelled_at, cancelled_reason)
                     VALUES (?, ?, ?, ?, ?, 'backfill', ?, ?, ?, ?, ?)`,
                    [reg.name, S.modeFromName(reg.name), S.modeFromName(reg.name) ? 'draft_name' : 'default',
                     cancelled ? 'CANCELLED' : 'CLOSED',
                     cancelled ? 'legacy draft, deleted' : 'legacy draft, documents only',
                     reg.id, userEmail,
                     cancelled ? null : reg.last_activity_at, cancelled ? reg.last_activity_at : null,
                     cancelled ? 'deleted' : null]
                );
                await conn.query(`UPDATE draft_containers SET shipment_id = ? WHERE id = ? AND shipment_id IS NULL`, [ins.insertId, reg.id]);
                entry.target = ins.insertId;
            });
            report.writes.documentsOnly++;
        }
    }
    for (const r of orphanNames) {
        report.drafts.push({ registryId: null, name: r.name, action: 'draft', note: 'allocations without a registry row' });
        if (apply) {
            await inTx(() => syncDraft(conn, r.name, { origin: 'backfill', userEmail }));
            report.writes.draftsSynced++;
        }
    }

    // ── Phase 3: planned ────────────────────────────────────────────────
    const [plannedNames] = await conn.query(`
        SELECT p.planned_container_name AS name, COUNT(*) AS n,
               SUM(o.id IS NULL OR o.deleted_at IS NOT NULL) AS dead
          FROM planned_container_allocations p
          LEFT JOIN orders o ON o.id = p.order_id
         GROUP BY p.planned_container_name
         ORDER BY p.planned_container_name`);
    for (const p of plannedNames) {
        const entry = { name: p.name, lines: Number(p.n), deadLines: Number(p.dead), hint: (S.parseNameHint(p.name) || {}).reference || null };
        report.planned.push(entry);
        if (apply) {
            const r = await inTx(() => syncPlanned(conn, p.name, { origin: 'backfill', userEmail }));
            entry.shipmentId = r ? r.shipmentId : null;
            report.writes.plannedSynced++;
        }
    }

    // Allocation health, for the report.
    const [deadLines] = await conn.query(`
        SELECT 'draft' AS kind, a.draft_container_name AS name, a.order_id, a.allocated
          FROM draft_container_allocations a LEFT JOIN orders o ON o.id = a.order_id
         WHERE o.id IS NULL OR o.deleted_at IS NOT NULL
        UNION ALL
        SELECT 'planned', p.planned_container_name, p.order_id, p.allocated
          FROM planned_container_allocations p LEFT JOIN orders o ON o.id = p.order_id
         WHERE o.id IS NULL OR o.deleted_at IS NOT NULL`);
    report.anomalies.linesOnDeletedOrders = deadLines;
    const [overAlloc] = await conn.query(`
        SELECT 'draft' AS kind, a.draft_container_name AS name, a.order_id, a.allocated, o.quantity
          FROM draft_container_allocations a JOIN orders o ON o.id = a.order_id AND o.deleted_at IS NULL
         WHERE a.allocated > o.quantity
        UNION ALL
        SELECT 'planned', p.planned_container_name, p.order_id, p.allocated, o.quantity
          FROM planned_container_allocations p JOIN orders o ON o.id = p.order_id AND o.deleted_at IS NULL
         WHERE p.allocated > o.quantity`);
    report.anomalies.overAllocations = overAlloc;

    // ── Phase 4: documents ──────────────────────────────────────────────
    const [[docCounts]] = await conn.query(`
        SELECT (SELECT COUNT(*) FROM draft_container_documents d
                  JOIN draft_containers dc ON dc.name = d.draft_container_name
                 WHERE d.shipment_id IS NULL AND dc.shipment_id IS NOT NULL) AS docs_stampable,
               (SELECT COUNT(*) FROM draft_container_documents d WHERE d.shipment_id IS NULL) AS docs_unstamped,
               (SELECT COUNT(*) FROM quality_assurance_documents q
                  JOIN draft_containers dc ON dc.name = q.draft_container_name
                 WHERE q.shipment_id IS NULL AND dc.shipment_id IS NOT NULL) AS qa_stampable,
               (SELECT COUNT(*) FROM quality_assurance_documents q
                 WHERE q.shipment_id IS NULL AND q.draft_container_name IS NOT NULL) AS qa_unstamped,
               (SELECT COUNT(*) FROM quality_assurance_documents q WHERE q.draft_container_name IS NULL) AS qa_untagged`);
    report.documents = {
        documentsUnstamped: Number(docCounts.docs_unstamped),
        documentsStampableNow: Number(docCounts.docs_stampable),
        qaDocumentsUnstamped: Number(docCounts.qa_unstamped),
        qaDocumentsStampableNow: Number(docCounts.qa_stampable),
        qaDocumentsUntagged: Number(docCounts.qa_untagged),
    };
    if (apply) {
        await inTx(async () => {
            const [d] = await conn.query(`
                UPDATE draft_container_documents d
                  JOIN draft_containers dc ON dc.name = d.draft_container_name
                   SET d.shipment_id = dc.shipment_id
                 WHERE d.shipment_id IS NULL AND dc.shipment_id IS NOT NULL`);
            const [q] = await conn.query(`
                UPDATE quality_assurance_documents q
                  JOIN draft_containers dc ON dc.name = q.draft_container_name
                   SET q.shipment_id = dc.shipment_id
                 WHERE q.shipment_id IS NULL AND dc.shipment_id IS NOT NULL`);
            report.writes.documentsStamped += d.affectedRows || 0;
            report.writes.qaDocumentsStamped += q.affectedRows || 0;
        });
    }

    // ── Phase 5: orders + booked manifests ──────────────────────────────
    const [[bounds]] = await conn.query(`SELECT MIN(id) AS lo, MAX(id) AS hi FROM orders`);
    // Orders (live or soft-deleted) carrying a number that some live order
    // carries, and not yet pointing at that number's shipment.
    const [[toLink]] = await conn.query(`
        SELECT COUNT(*) AS n FROM orders o
         WHERE o.container_number IS NOT NULL AND TRIM(o.container_number) <> ''
           AND EXISTS (SELECT 1 FROM orders l
                        WHERE l.deleted_at IS NULL AND TRIM(l.container_number) = TRIM(o.container_number))
           AND NOT EXISTS (SELECT 1 FROM shipments s
                            WHERE s.reference = TRIM(o.container_number) AND ${LIVE} AND s.id = o.shipment_id)`);
    report.writes.ordersToLink = Number(toLink.n);
    if (apply && bounds.lo != null) {
        for (let lo = Number(bounds.lo); lo <= Number(bounds.hi); lo += batchSize) {
            const hi = lo + batchSize - 1;
            await inTx(async () => {
                // Derive inside the statement: a concurrent production edit can
                // never be overwritten from a stale map. last_updated is pinned.
                const [linked] = await conn.query(`
                    UPDATE orders o
                      JOIN shipments s ON s.reference = TRIM(o.container_number) AND s.deleted_at IS NULL AND s.merged_into_id IS NULL
                       SET o.shipment_id = s.id, o.last_updated = o.last_updated
                     WHERE o.id BETWEEN ? AND ? AND NOT (o.shipment_id <=> s.id)`, [lo, hi]);
                const [cleared] = await conn.query(`
                    UPDATE orders o
                       SET o.shipment_id = NULL, o.last_updated = o.last_updated
                     WHERE o.id BETWEEN ? AND ? AND o.deleted_at IS NULL AND o.shipment_id IS NOT NULL
                       AND (o.container_number IS NULL OR TRIM(o.container_number) = '')`, [lo, hi]);
                const [lines] = await conn.query(`
                    INSERT INTO shipment_lines (shipment_id, order_id, quantity)
                    SELECT o.shipment_id, o.id, o.quantity
                      FROM orders o
                      JOIN shipments s ON s.id = o.shipment_id AND s.stage IN (${BOOKED_IN}) AND ${LIVE}
                      LEFT JOIN shipment_lines sl ON sl.shipment_id = o.shipment_id AND sl.order_id = o.id
                     WHERE o.deleted_at IS NULL AND o.id BETWEEN ? AND ?
                       AND (sl.id IS NULL OR sl.quantity <> o.quantity)
                    ON DUPLICATE KEY UPDATE quantity = VALUES(quantity)`, [lo, hi]);
                report.writes.ordersLinked += linked.affectedRows || 0;
                report.writes.ordersCleared += cleared.affectedRows || 0;
                report.writes.linesUpserted += lines.affectedRows || 0;
            });
        }
        await inTx(async () => {
            const [removed] = await conn.query(`
                DELETE sl FROM shipment_lines sl
                  JOIN shipments s ON s.id = sl.shipment_id AND s.stage IN (${BOOKED_IN})
                  LEFT JOIN orders o ON o.id = sl.order_id AND o.deleted_at IS NULL AND o.shipment_id = sl.shipment_id
                 WHERE o.id IS NULL`);
            report.writes.linesRemoved += removed.affectedRows || 0;
        });
        const [bookedIds] = await conn.query(`SELECT s.id FROM shipments s WHERE s.stage IN (${BOOKED_IN}) AND ${LIVE}`);
        for (let i = 0; i < bookedIds.length; i += 50) {
            await inTx(() => refreshBookedCopies(conn, bookedIds.slice(i, i + 50).map(r => r.id)));
        }
    }

    report.next.SEA = await nextReference(conn, 'SEA');
    report.next.AIR = await nextReference(conn, 'AIR');

    // ── Phase 6: marker ─────────────────────────────────────────────────
    const [flags] = await conn.query(`SELECT name FROM app_migrations WHERE name IN (?, ?)`, [BACKFILL_MARKER, KILL_SWITCH]);
    const flagNames = new Set(flags.map(f => f.name));
    report.armed = flagNames.has(BACKFILL_MARKER);
    report.killSwitch = flagNames.has(KILL_SWITCH);
    if (apply) {
        await conn.query(`INSERT IGNORE INTO app_migrations (name) VALUES (?)`, [BACKFILL_MARKER]);
        report.armed = true;
        if (rearm && report.killSwitch) {
            await conn.query(`DELETE FROM app_migrations WHERE name = ?`, [KILL_SWITCH]);
            report.killSwitch = false;
            report.rearmed = true;
        }
    }
    return report;
}

// Informational milestones from the member orders.
function milestonesFor(members, derived) {
    const dates = members.map(m => {
        try { return typeof m.dates === 'string' ? JSON.parse(m.dates) || {} : (m.dates || {}); } catch { return {}; }
    });
    const first = list => list.filter(Boolean).map(v => String(v).slice(0, 19).replace('T', ' ')).sort()[0] || null;
    const last = list => list.filter(Boolean).map(v => String(v).slice(0, 19).replace('T', ' ')).sort().pop() || null;
    const asDay = v => (v ? `${String(v).slice(0, 10)} 00:00:00` : null);
    return {
        booked: first(dates.map(d => d.consolidated)),
        departed: first(members.map(m => asDay(m.shipped_date))),
        arrived: first(members.map(m => asDay(m.arrived_date))),
        closed: derived === 'CLOSED' ? last(dates.map(d => d.received)) : null,
    };
}

// The group (phase 1) or live booked shipment whose reference equals `ref`,
// compared SQL-side. Returns { ref } or null.
async function findBookedByReference(conn, ref, groups) {
    const want = S.clean(ref);
    if (!want) return null;
    const [rows] = await conn.query(
        `SELECT MIN(TRIM(container_number)) AS ref FROM orders
          WHERE deleted_at IS NULL AND TRIM(container_number) = ?`,
        [want]
    );
    const hit = rows[0] && rows[0].ref;
    if (hit && groups.has(hit)) return { ref: hit };
    const [ship] = await conn.query(
        `SELECT s.reference AS ref FROM shipments s WHERE s.reference = ? AND s.stage IN (${BOOKED_IN}) AND ${LIVE}`,
        [want]
    );
    return ship[0] ? { ref: ship[0].ref } : null;
}

async function linkRegistry(conn, inTx, registryId, ref, report) {
    await inTx(async () => {
        const [rows] = await conn.query(
            `SELECT s.id FROM shipments s WHERE s.reference = ? AND ${LIVE} FOR UPDATE`, [ref]
        );
        if (!rows.length) return;
        const [r] = await conn.query(
            `UPDATE draft_containers SET shipment_id = ? WHERE id = ? AND shipment_id IS NULL`,
            [rows[0].id, registryId]
        );
        report.writes.registryLinked += r.affectedRows || 0;
    });
}

// ── Verify ───────────────────────────────────────────────────────────────
// Re-derives what the shadow should say from the legacy tables and diffs it.
// `hard` findings fail the gate; `info` findings are reported only. With
// `fix`, re-runs the sync primitives for the drifted keys only (each key in its
// own transaction), re-verifies, and when the re-verify is clean marks the
// unresolved sync failures resolved.
async function verifyAll(conn, { fix = false, userEmail = null } = {}) {
    const hard = {};
    const info = {};

    const [memberDrift] = await conn.query(`
        SELECT o.id, TRIM(o.container_number) AS ref, o.shipment_id, s.id AS expected
          FROM orders o
          LEFT JOIN shipments s ON s.reference = TRIM(o.container_number) AND ${LIVE}
         WHERE o.deleted_at IS NULL AND o.container_number IS NOT NULL AND TRIM(o.container_number) <> ''
           AND (s.id IS NULL OR NOT (o.shipment_id <=> s.id))`);
    hard.orderWithoutMatchingShipment = memberDrift;

    const [strayMembers] = await conn.query(`
        SELECT o.id, o.shipment_id, NULLIF(TRIM(o.container_number), '') AS ref,
               s.reference, s.stage, s.deleted_at IS NOT NULL AS deleted, s.merged_into_id
          FROM orders o
          LEFT JOIN shipments s ON s.id = o.shipment_id
         WHERE o.deleted_at IS NULL AND o.shipment_id IS NOT NULL
           AND (s.id IS NULL OR s.deleted_at IS NOT NULL OR s.merged_into_id IS NOT NULL
                OR s.stage NOT IN (${BOOKED_IN})
                OR NOT (s.reference <=> NULLIF(TRIM(o.container_number), '')))`);
    hard.shipmentIdWithoutMatchingNumber = strayMembers;

    const [draftLineDrift] = await conn.query(`
        SELECT a.draft_container_name AS name, a.order_id, a.allocated, s.id AS shipment_id, sl.quantity
          FROM draft_container_allocations a
          LEFT JOIN shipments s ON s.open_key = CONCAT('D:', a.draft_container_name) AND ${LIVE}
          LEFT JOIN shipment_lines sl ON sl.shipment_id = s.id AND sl.order_id = a.order_id
         WHERE s.id IS NULL OR sl.id IS NULL OR sl.quantity <> a.allocated`);
    const [plannedLineDrift] = await conn.query(`
        SELECT p.planned_container_name AS name, p.order_id, p.allocated, s.id AS shipment_id, sl.quantity
          FROM planned_container_allocations p
          LEFT JOIN shipments s ON s.open_key = CONCAT('P:', p.planned_container_name) AND ${LIVE}
          LEFT JOIN shipment_lines sl ON sl.shipment_id = s.id AND sl.order_id = p.order_id
         WHERE s.id IS NULL OR sl.id IS NULL OR sl.quantity <> p.allocated`);
    const [openExtraLines] = await conn.query(`
        SELECT s.id AS shipment_id, s.stage, s.name, sl.order_id, sl.quantity
          FROM shipments s
          JOIN shipment_lines sl ON sl.shipment_id = s.id
          LEFT JOIN draft_container_allocations a
                 ON s.stage = 'DRAFT' AND s.open_key = CONCAT('D:', a.draft_container_name) AND a.order_id = sl.order_id
          LEFT JOIN planned_container_allocations p
                 ON s.stage = 'PLANNED' AND s.open_key = CONCAT('P:', p.planned_container_name) AND p.order_id = sl.order_id
         WHERE s.stage IN ('DRAFT', 'PLANNED') AND ${LIVE} AND a.id IS NULL AND p.id IS NULL`);
    hard.draftLinesMismatch = draftLineDrift;
    hard.plannedLinesMismatch = plannedLineDrift;
    hard.openLinesWithoutAllocation = openExtraLines;

    const [bookedMissing] = await conn.query(`
        SELECT o.id AS order_id, o.shipment_id, o.quantity, sl.quantity AS line_quantity
          FROM orders o
          JOIN shipments s ON s.id = o.shipment_id AND s.stage IN (${BOOKED_IN}) AND ${LIVE}
          LEFT JOIN shipment_lines sl ON sl.shipment_id = o.shipment_id AND sl.order_id = o.id
         WHERE o.deleted_at IS NULL AND (sl.id IS NULL OR sl.quantity <> o.quantity)`);
    const [bookedExtra] = await conn.query(`
        SELECT sl.shipment_id, sl.order_id, sl.quantity
          FROM shipment_lines sl
          JOIN shipments s ON s.id = sl.shipment_id AND s.stage IN (${BOOKED_IN})
          LEFT JOIN orders o ON o.id = sl.order_id AND o.deleted_at IS NULL AND o.shipment_id = sl.shipment_id
         WHERE o.id IS NULL`);
    const [deadShipmentLines] = await conn.query(`
        SELECT sl.shipment_id, COUNT(*) AS n
          FROM shipment_lines sl JOIN shipments s ON s.id = sl.shipment_id
         WHERE s.deleted_at IS NOT NULL OR s.merged_into_id IS NOT NULL OR s.stage = 'CANCELLED'
         GROUP BY sl.shipment_id`);
    hard.bookedMemberWithoutLine = bookedMissing;
    hard.bookedLineWithoutMember = bookedExtra;
    hard.linesOnInactiveShipments = deadShipmentLines;

    const [keyLeaks] = await conn.query(`
        SELECT id, reference, open_key, stage, deleted_at IS NOT NULL AS deleted, merged_into_id
          FROM shipments
         WHERE (reference IS NOT NULL OR open_key IS NOT NULL)
           AND (deleted_at IS NOT NULL OR merged_into_id IS NOT NULL OR stage = 'CANCELLED')`);
    const [openKeyShape] = await conn.query(`
        SELECT id, stage, open_key FROM shipments s
         WHERE ${LIVE} AND ((s.stage IN ('DRAFT', 'PLANNED')) <> (s.open_key IS NOT NULL))`);
    hard.keysNotReleased = keyLeaks;
    hard.openKeyStageMismatch = openKeyShape;

    hard.documentsOnUnlinkedShipments = await documentLinkDrift(conn);

    // ── informational
    const [stageAhead] = await conn.query(`
        SELECT z.id, z.reference, z.stage, z.derived_stage
          FROM (${shipmentSelect({ where: [LIVE], orderBy: 'z.id' })}) z
         WHERE z.derived_stage IS NOT NULL
           AND FIELD(z.stage, ${BOOKED_IN}) > FIELD(z.derived_stage, ${BOOKED_IN})
           AND NOT EXISTS (SELECT 1 FROM audit_log al
                            WHERE al.entity_type = 'shipment' AND al.entity_id = z.id
                              AND al.action IN ('stage_changed', 'booked'))`);
    info.storedStageAheadWithoutTransition = stageAhead;
    const [copyDrift] = await conn.query(`
        SELECT z.id, z.reference, z.tracking_ref, z.member_ext, z.member_awb, z.eta, z.member_eta
          FROM (${shipmentSelect({ where: [LIVE, `s.stage IN (${BOOKED_IN})`], orderBy: 'z.id' })}) z
         WHERE z.member_count > 0
           AND ((COALESCE(z.member_eta, z.eta) <> z.eta)
                OR (z.tracking_ref IS NOT NULL
                    AND NOT (z.tracking_ref <=> IF(z.mode = 'AIR', COALESCE(z.member_awb, z.member_ext), COALESCE(z.member_ext, z.member_awb)))
                    AND COALESCE(z.member_ext, z.member_awb) IS NOT NULL))`);
    info.storedCopiesDiffer = copyDrift;
    const [orphans] = await conn.query(`
        SELECT s.id, s.reference, s.stage, s.origin, s.name FROM shipments s
         WHERE ${LIVE} AND s.stage IN (${BOOKED_IN})
           AND NOT EXISTS (SELECT 1 FROM orders o WHERE o.shipment_id = s.id AND o.deleted_at IS NULL)
           AND s.origin <> 'backfill' AND s.reference IS NOT NULL`);
    info.memberlessBookedShipments = orphans;
    const [pointer] = await conn.query(`
        SELECT s.id AS shipment_id, s.name, dc.id AS registry_id, dc.shipment_id AS registry_points_at
          FROM shipments s JOIN draft_containers dc ON CONCAT('D:', dc.name) = s.open_key
         WHERE ${LIVE} AND s.stage = 'DRAFT' AND NOT (dc.shipment_id <=> s.id)`);
    info.registryPointerMismatch = pointer;
    const [failures] = await conn.query(`
        SELECT id, site, key_kind, key_value, error, occurrences, created_at, last_seen_at
          FROM shipment_sync_failures WHERE resolved_at IS NULL ORDER BY last_seen_at DESC LIMIT 200`);
    info.unresolvedSyncFailures = failures;
    const [review] = await conn.query(`
        SELECT id, reference, name, mode, review_note FROM shipments s WHERE ${LIVE} AND s.needs_review = 1 ORDER BY id`);
    info.needsReview = review;

    const hardCount = Object.values(hard).reduce((n, list) => n + list.length, 0);
    const result = { ok: hardCount === 0, hardCount, hard, info };
    if (!fix || result.ok) return result;

    // ── fix: re-sync drifted keys only
    const inTx = async (fn) => {
        await conn.query('START TRANSACTION');
        try { const r = await fn(); await conn.query('COMMIT'); return r; }
        catch (err) { try { await conn.query('ROLLBACK'); } catch (_) { /* gone */ } throw err; }
    };
    const fixed = { orders: 0, drafts: 0, planned: 0, inactiveLines: 0, keys: 0, documents: 0 };
    const orderIds = intIds([
        ...memberDrift.map(r => r.id), ...strayMembers.map(r => r.id),
        ...bookedMissing.map(r => r.order_id), ...bookedExtra.map(r => r.order_id),
    ]);
    for (let i = 0; i < orderIds.length; i += 50) {
        await inTx(() => syncOrderMembership(conn, orderIds.slice(i, i + 50), { userEmail }));
    }
    fixed.orders = orderIds.length;
    const draftNames = [...new Set([...draftLineDrift.map(r => r.name),
        ...openExtraLines.filter(r => r.stage === 'DRAFT').map(r => r.name)].filter(Boolean))];
    for (const n of draftNames) { await inTx(() => syncDraft(conn, n, { userEmail })); fixed.drafts++; }
    const plannedNames = [...new Set([...plannedLineDrift.map(r => r.name),
        ...openExtraLines.filter(r => r.stage === 'PLANNED').map(r => r.name)].filter(Boolean))];
    for (const n of plannedNames) { await inTx(() => syncPlanned(conn, n, { userEmail })); fixed.planned++; }
    for (const r of deadShipmentLines) {
        await conn.query(`DELETE FROM shipment_lines WHERE shipment_id = ?`, [r.shipment_id]);
        fixed.inactiveLines++;
    }
    for (const r of keyLeaks) {
        await conn.query(`UPDATE shipments SET reference = NULL, open_key = NULL WHERE id = ?`, [r.id]);
        fixed.keys++;
    }
    for (const d of hard.documentsOnUnlinkedShipments) {
        if (!d.registryShipmentId) continue;
        const table = d.kind === 'qa' ? 'quality_assurance_documents' : 'draft_container_documents';
        await conn.query(`UPDATE ${table} SET shipment_id = ? WHERE id = ?`, [d.registryShipmentId, d.id]);
        fixed.documents++;
    }
    const after = await verifyAll(conn, { fix: false });
    if (after.ok) {
        const [r] = await conn.query(
            `UPDATE shipment_sync_failures SET resolved_at = NOW(), dedup_key = NULL WHERE resolved_at IS NULL`
        );
        after.failuresResolved = r.affectedRows || 0;
    }
    after.fixed = fixed;
    return after;
}

// Every stamped document must point at a shipment that is, or was, linked to
// its registry row: the registry's current pointer, a shipment sourced from
// the row, or anything those were merged into.
async function documentLinkDrift(conn) {
    const [shipRows] = await conn.query(`SELECT id, source_draft_id, merged_into_id FROM shipments`);
    const [regRows] = await conn.query(`SELECT id, shipment_id FROM draft_containers`);
    const mergedInto = new Map(shipRows.map(s => [s.id, s.merged_into_id]));
    const bySource = new Map();
    for (const s of shipRows) {
        if (s.source_draft_id == null) continue;
        if (!bySource.has(s.source_draft_id)) bySource.set(s.source_draft_id, []);
        bySource.get(s.source_draft_id).push(s.id);
    }
    const linked = new Map();
    for (const r of regRows) {
        const set = new Set();
        const queue = [r.shipment_id, ...(bySource.get(r.id) || [])].filter(Boolean);
        while (queue.length) {
            const id = queue.pop();
            if (set.has(id)) continue;
            set.add(id);
            const next = mergedInto.get(id);
            if (next) queue.push(next);
        }
        linked.set(r.id, { set, pointer: r.shipment_id });
    }
    const out = [];
    for (const [kind, table] of [['document', 'draft_container_documents'], ['qa', 'quality_assurance_documents']]) {
        const [docs] = await conn.query(`
            SELECT d.id, d.shipment_id, dc.id AS registry_id
              FROM ${table} d
              LEFT JOIN draft_containers dc ON dc.name = d.draft_container_name
             WHERE d.shipment_id IS NOT NULL`);
        for (const d of docs) {
            const l = d.registry_id != null ? linked.get(d.registry_id) : null;
            if (l && l.set.has(d.shipment_id)) continue;
            out.push({ kind, id: d.id, shipmentId: d.shipment_id, registryId: d.registry_id ?? null, registryShipmentId: l ? l.pointer : null });
        }
    }
    return out;
}

module.exports = {
    BACKFILL_MARKER,
    KILL_SWITCH,
    shadowArmed,
    resetFlagCache,
    isFatal,
    recordFailure,
    shadow,
    lockShipment,
    lockOpenShipment,
    findOrCreateByReference,
    upsertLine,
    leaveBookedManifests,
    rebuildBookedLines,
    refreshBookedCopies,
    syncOrderMembership,
    resolveOpenDraft,
    syncDraft,
    syncPlanned,
    cancelShipment,
    mergeShipments,
    syncDraftClose,
    stampDraftDocuments,
    freightMode,
    validDate,
    shipmentSelect,
    selectShipments,
    getShipment,
    loadShipmentsForOrders,
    loadLines,
    loadLinesFor,
    nextReference,
    referenceIsFree,
    backfillAll,
    verifyAll,
    mostFrequent,
    intIds,
};
