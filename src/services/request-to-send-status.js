'use strict';

// Request to send staleness, read side. Compares each supplier's latest Request
// to send (draft_container_documents type 'supplier-quote') with the lines the
// container has now, read exactly as the PDF reads them (quoteLineSource), for
// one shipment's Documents list and for every live shipment at once (the
// container cards). Rules: src/lib/request-to-send-drift.js.

const S = require('../lib/shipments');
const R = require('../lib/request-to-send-drift');
const { quoteLineSourceMany } = require('../lib/quote-line-source');

// Flagged until the goods arrive (user rule, 2026-10-26): a draft, or a booked
// container whose effective stage (stored vs what its orders show,
// S.effectiveStage) is still BOOKED or IN_TRANSIT. The stored stage alone stays
// BOOKED after the goods sail or land, so it can't decide this by itself.
// Stored ARRIVED / CLOSED can only be later, so they are not even read.
const LIVE_STAGES = ['DRAFT', 'BOOKED', 'IN_TRANSIT'];
const FLAGGED_STAGES = new Set(['DRAFT', 'BOOKED', 'IN_TRANSIT']);
const LINE_COLS = 'o.jf_code, o.asin, o.product_name, o.lot_number, o.supplier, o.status';
const lc = s => String(s || '').trim().toLowerCase();

// Current lines per source, keyed by the lower-cased draft name or reference
// (both columns compare case-insensitively in MySQL), with the member order
// statuses the effective stage is derived from.
async function loadLinesBySource(conn, kind, keys) {
    const out = new Map();
    if (!keys.length) return out;
    const src = quoteLineSourceMany(kind, keys);
    const [rows] = await conn.query(
        `SELECT ${src.sourceKey}, ${src.allocated}, ${LINE_COLS}
           FROM ${src.from}
          WHERE ${src.where}
          ORDER BY ${src.orderBy}`,
        src.params
    );
    const grouped = new Map();
    for (const r of rows) {
        const k = lc(r.source_key);
        if (!grouped.has(k)) grouped.set(k, []);
        grouped.get(k).push(r);
    }
    for (const [k, rs] of grouped) {
        out.set(k, { lines: R.currentRequestLines(rs), statuses: rs.map(r => r.status).filter(Boolean) });
    }
    return out;
}

// Whether a shipment's Requests to send are still worth flagging.
function flaggable(ship, statuses) {
    if (ship.stage === 'DRAFT') return true;
    return FLAGGED_STAGES.has(S.effectiveStage(ship.stage, S.deriveStage(statuses || [])));
}

// Each supplier's latest Request to send, from rows ordered newest first. The
// latest decides even when it has no snapshot (then: no baseline, no flag),
// so an older snapshot is never compared instead.
function latestPerSupplier(docRows) {
    const seen = new Set();
    const out = [];
    for (const d of docRows) {
        if (d.type !== 'supplier-quote') continue;
        const k = lc(d.supplier);
        if (seen.has(k)) continue;
        seen.add(k);
        out.push(d);
    }
    return out;
}

// Where a shipment's documents read their lines: { kind, key } or null.
function sourceOf(ship) {
    let s;
    try { s = S.documentSource(ship); } catch { return null; }
    const key = s.kind === 'draft' ? S.clean(s.name) : s.reference;
    return key ? { kind: s.kind, key } : null;
}

// { stale, changes, baseline } for one document, or null without a snapshot.
function driftFor(doc, lines) {
    const snap = R.parseRequestLines(doc.lines_json);
    if (!snap) return null;
    const mine = (lines || []).filter(l => R.sameSupplier(l.supplier, doc.supplier));
    return { ...R.requestDrift(snap.lines, mine), baseline: snap.source };
}

// One shipment's Documents list: Map docId → drift, for each supplier's latest
// Request to send. `docRows` = that list's rows, newest first, lines_json
// included. Empty once the goods have arrived (same rule as the cards).
async function requestDriftForShipment(conn, ship, docRows) {
    const out = new Map();
    const latest = latestPerSupplier(docRows).filter(d => d.lines_json != null);
    if (!latest.length) return out;
    const src = sourceOf(ship);
    if (!src) return out;
    const current = (await loadLinesBySource(conn, src.kind, [src.key])).get(lc(src.key)) || { lines: [], statuses: [] };
    if (!flaggable(ship, current.statuses)) return out;
    for (const d of latest) out.set(d.id, driftFor(d, current.lines));
    return out;
}

// Every flaggable shipment with at least one stale Request to send, stale suppliers
// only: [{ shipmentId, name, reference, stage, suppliers: [{ supplier,
// documentId, version, generatedAt, changeCount, baseline }] }].
async function requestToSendStatus(conn) {
    const [ships] = await conn.query(
        `SELECT * FROM shipments WHERE deleted_at IS NULL AND merged_into_id IS NULL AND stage IN (?)`,
        [LIVE_STAGES]
    );
    if (!ships.length) return [];
    const byId = new Map(ships.map(s => [Number(s.id), s]));
    // An open draft also owns documents made under its name that no hook has
    // stamped with its shipment id yet (as GET /shipments/:id/documents).
    const draftByName = new Map(ships.filter(s => s.stage === 'DRAFT' && S.clean(s.name)).map(s => [lc(s.name), s]));

    const [docs] = await conn.query(
        `SELECT * FROM draft_container_documents
          WHERE type = 'supplier-quote' AND deleted_at IS NULL
            AND (shipment_id IN (?) OR (shipment_id IS NULL AND draft_container_name IN (?)))
          ORDER BY generated_at DESC, id DESC`,
        [[...byId.keys()], draftByName.size ? [...draftByName.keys()] : ['']]
    );
    const docsByShip = new Map();
    for (const d of docs) {
        const ship = d.shipment_id != null ? byId.get(Number(d.shipment_id)) : draftByName.get(lc(d.draft_container_name));
        if (!ship) continue;
        if (!docsByShip.has(ship.id)) docsByShip.set(ship.id, []);
        docsByShip.get(ship.id).push(d);
    }

    const work = [];
    for (const [shipId, rows] of docsByShip) {
        const latest = latestPerSupplier(rows).filter(d => d.lines_json != null);
        const src = latest.length ? sourceOf(byId.get(Number(shipId))) : null;
        if (src) work.push({ ship: byId.get(Number(shipId)), latest, src });
    }
    if (!work.length) return [];

    const keysOf = kind => [...new Set(work.filter(w => w.src.kind === kind).map(w => w.src.key))];
    const lines = {
        draft: await loadLinesBySource(conn, 'draft', keysOf('draft')),
        booked: await loadLinesBySource(conn, 'booked', keysOf('booked')),
    };

    const out = [];
    for (const { ship, latest, src } of work) {
        const current = lines[src.kind].get(lc(src.key)) || { lines: [], statuses: [] };
        if (!flaggable(ship, current.statuses)) continue;
        const suppliers = [];
        for (const d of latest) {
            const drift = driftFor(d, current.lines);
            if (!drift || !drift.stale) continue;
            suppliers.push({
                supplier: d.supplier,
                documentId: d.id,
                version: d.version,
                generatedAt: d.generated_at?.toISOString?.() ?? d.generated_at,
                changeCount: drift.changes.length,
                baseline: drift.baseline,
            });
        }
        if (suppliers.length) {
            out.push({ shipmentId: ship.id, name: ship.name || null, reference: ship.reference || null, stage: ship.stage, suppliers });
        }
    }
    return out;
}

module.exports = { LIVE_STAGES, flaggable, latestPerSupplier, requestDriftForShipment, requestToSendStatus };
