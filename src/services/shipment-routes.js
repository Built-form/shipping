'use strict';

// /api/v1/shipments: the new address for the state the legacy container,
// draft-container and planned-container routes already manage.
//
// Through rollout step 3 the legacy tables stay the source of truth, so every
// write here goes through to them in the same transaction (the registry and
// allocation rows with their legacy audit payloads, the orders columns) and the
// old SPA, JFPRO and cashboard-ui keep seeing exactly what they see today. See
// docs/shipments-frontend.md for the contract.
//
// Registered at the bottom of orders.js (registerShipmentRoutes), after every
// constant these handlers close over exists. All authed through the greedy
// /api/v1/{proxy+} route, so serverless.yml needs no change. Names containing
// '/' and spaces are looked up with ?name=, never in the path.

const S = require('../lib/shipments');
const sync = require('./shipment-sync');
const { makeBookShipment } = require('./shipment-book');
const { makeTransitionShipment } = require('./shipment-stage');
const { SplitError } = require('./order-split');

const { ShipmentError } = S;

// Same columns draftAudit.lineSnapshot expects (the legacy line-event payload).
const DRAFT_LINE_SNAPSHOT_SELECT = `
    SELECT dca.id, dca.order_id, dca.allocated, dca.draft_container_name,
           orders.jf_code, orders.asin, orders.product_name, orders.supplier, orders.po_number,
           orders.quantity AS order_quantity
      FROM draft_container_allocations dca
      LEFT JOIN orders ON orders.id = dca.order_id`;

function registerShipmentRoutes(app, deps) {
    const {
        withConnection, ready, ORDER_SELECT, rowToOrder, recordAudit, draftAudit,
        generateDraftDocumentSet, publicS3Url, poBucket, rowToContainer, rowToAirShipment, log,
    } = deps;
    const bookShipment = makeBookShipment(deps);
    const { transitionShipment, cancelOpen } = makeTransitionShipment(deps);

    function sendError(res, err, label) {
        if (err instanceof ShipmentError || err instanceof SplitError) {
            return res.status(err.status).json({ error: err.message, code: err.code, ...(err.payload || {}) });
        }
        log.error(label, err);
        return res.status(500).json({ error: 'An internal error occurred.' });
    }

    async function inTransaction(conn, fn) {
        await conn.beginTransaction();
        try {
            const r = await fn();
            await conn.commit();
            return r;
        } catch (err) {
            try { await conn.rollback(); } catch (_) { /* connection already gone */ }
            throw err;
        }
    }

    function idParam(req) {
        return Number(req.params.id);
    }

    async function lockLive(conn, id) {
        const ship = await sync.lockShipment(conn, id);
        if (!ship || ship.deleted_at) throw new ShipmentError(404, 'NOT_FOUND', `Shipment ${id} not found.`);
        if (ship.merged_into_id) {
            throw new ShipmentError(409, 'MERGED', `Shipment ${id} was merged into ${ship.merged_into_id}.`, { mergedIntoId: ship.merged_into_id });
        }
        return ship;
    }

    // Ids of shipments merged (directly or not) into `id`.
    async function mergedInto(conn, id) {
        const out = [];
        let frontier = [id];
        for (let depth = 0; frontier.length && depth < 5; depth++) {
            const [rows] = await conn.query(
                `SELECT id FROM shipments WHERE merged_into_id IN (${frontier.map(() => '?').join(',')})`, frontier
            );
            frontier = rows.map(r => r.id).filter(x => !out.includes(x));
            out.push(...frontier);
        }
        return out;
    }

    async function trackingBlock(conn, s) {
        const ref = s.trackingRef;
        if (!ref) return null;
        const container = async () => {
            const [rows] = await conn.query('SELECT * FROM containers WHERE container_number = ?', [ref]);
            return rows[0] ? { provider: 'shipsgo', kind: 'container', data: rowToContainer(rows[0]) } : null;
        };
        const air = async () => {
            const [rows] = await conn.query('SELECT * FROM air_shipments WHERE awb_number = ?', [ref]);
            return rows[0] ? { provider: 'shipsgo', kind: 'air', data: rowToAirShipment(rows[0]) } : null;
        };
        try {
            return s.mode === 'AIR' ? (await air()) || (await container()) : (await container()) || (await air());
        } catch (err) {
            if (err.code !== 'ER_NO_SUCH_TABLE') log.warn('[shipments] tracking block unavailable', { id: s.id, error: err.message });
            return null;
        }
    }

    async function shipmentDetail(conn, id) {
        const row = await sync.getShipment(conn, id);
        if (!row) return null;
        const s = S.rowToShipment(row);
        s.lines = await sync.loadLines(conn, id);
        s.mergedFrom = await mergedInto(conn, id);
        if (S.isBookedStage(row.stage)) {
            const [orders] = await conn.query(`${ORDER_SELECT} AND orders.shipment_id = ? ORDER BY orders.id`, [id]);
            s.orders = orders.map(rowToOrder);
        } else {
            s.orders = [];
        }
        s.tracking = await trackingBlock(conn, s);
        return s;
    }

    function parseLines(raw) {
        if (raw === undefined || raw === null) return [];
        if (!Array.isArray(raw)) throw new ShipmentError(400, 'BAD_LINES', 'lines must be an array of { orderId, quantity }.');
        const seen = new Set();
        return raw.map((l) => {
            const orderId = Number(l && l.orderId);
            const quantity = Number(l && (l.quantity ?? l.allocated));
            if (!Number.isInteger(orderId) || orderId <= 0) throw new ShipmentError(400, 'BAD_LINES', 'Each line needs a positive integer orderId.');
            if (!Number.isFinite(quantity) || quantity < 0) throw new ShipmentError(400, 'BAD_LINES', `Line for order ${orderId}: quantity must be a non-negative number.`);
            if (seen.has(orderId)) throw new ShipmentError(400, 'BAD_LINES', `Order ${orderId} appears twice.`);
            seen.add(orderId);
            return { orderId, quantity };
        });
    }

    function stampName(stage, mode) {
        const d = new Date();
        const p = n => String(n).padStart(2, '0');
        return `${stage}-${mode}-${String(d.getUTCFullYear()).slice(2)}${p(d.getUTCMonth() + 1)}${p(d.getUTCDate())}`
            + `-${p(d.getUTCHours())}${p(d.getUTCMinutes())}${p(d.getUTCSeconds())}`;
    }

    // Write one legacy allocation (insert or change) plus its legacy draft
    // event, as POST / PUT /draft-containers do. Returns the warning, if any.
    async function upsertDraftAllocation(conn, name, orderId, quantity, userEmail) {
        const [existing] = await conn.query(
            `${DRAFT_LINE_SNAPSHOT_SELECT} WHERE dca.draft_container_name = ? AND dca.order_id = ? FOR UPDATE`,
            [name, orderId]
        );
        if (existing.length) {
            const before = existing[0];
            if (Number(before.allocated) === quantity) return;
            await conn.query(`UPDATE draft_container_allocations SET allocated = ? WHERE id = ?`, [quantity, before.id]);
            const [after] = await conn.query(`${DRAFT_LINE_SNAPSHOT_SELECT} WHERE dca.id = ?`, [before.id]);
            const reg = await draftAudit.ensureDraftRegistered(conn, name, userEmail);
            await draftAudit.recordDraftAudit(conn, {
                draftId: reg.id, action: 'line_updated',
                before: { draftName: reg.name, ...draftAudit.lineSnapshot(before) },
                after: { draftName: reg.name, ...draftAudit.lineSnapshot(after[0]) },
                userEmail,
            });
            return;
        }
        const [ins] = await conn.query(
            `INSERT INTO draft_container_allocations (order_id, draft_container_name, allocated) VALUES (?, ?, ?)`,
            [orderId, name, quantity]
        );
        const [rows] = await conn.query(`${DRAFT_LINE_SNAPSHOT_SELECT} WHERE dca.id = ?`, [ins.insertId]);
        const reg = await draftAudit.ensureDraftRegistered(conn, name, userEmail);
        await draftAudit.recordDraftAudit(conn, {
            draftId: reg.id, action: 'line_added',
            after: { draftName: reg.name, ...draftAudit.lineSnapshot(rows[0]) },
            userEmail,
        });
    }

    async function liveOrder(conn, orderId) {
        const [rows] = await conn.query(
            `SELECT id, quantity FROM orders WHERE id = ? AND deleted_at IS NULL FOR SHARE`, [orderId]
        );
        if (!rows.length) throw new ShipmentError(404, 'ORDER_NOT_FOUND', `Order ${orderId} not found.`, { orderId });
        return rows[0];
    }

    function overAllocationWarning(orderId, quantity, orderQty) {
        return quantity > Number(orderQty)
            ? `Order ${orderId}: ${quantity} allocated exceeds its quantity (${orderQty}).`
            : null;
    }

    // ── GET /shipments ────────────────────────────────────────────────────
    app.get('/api/v1/shipments', async (req, res) => {
        try {
            await ready();
            const { stage, mode, q, reference, trackingRef, name, needsReview, include, limit } = req.query;
            const stages = stage ? String(stage).split(',').map(s => s.trim().toUpperCase()).filter(Boolean) : null;
            if (stages && stages.some(s => !S.isStage(s))) {
                return res.status(400).json({ error: `stage must be a comma-separated list of ${S.STAGES.join(', ')}.` });
            }
            const m = mode ? String(mode).trim().toUpperCase() : null;
            if (m && !S.isMode(m)) return res.status(400).json({ error: 'mode must be SEA, AIR or ROAD.' });
            let lim = parseInt(limit, 10);
            if (!Number.isFinite(lim) || lim <= 0) lim = 500;
            if (lim > 2000) lim = 2000;
            const nr = needsReview === undefined ? null : ['1', 'true', 'yes'].includes(String(needsReview).toLowerCase());
            const wantLines = String(include || '').split(',').map(s => s.trim()).includes('lines');
            const data = await withConnection(async (conn) => {
                const rows = await sync.selectShipments(conn, { stages, mode: m, q, reference, trackingRef, name, needsReview: nr, limit: lim });
                const out = rows.map(S.rowToShipment);
                if (wantLines && out.length) {
                    const lines = await sync.loadLinesFor(conn, out.map(s => s.id));
                    for (const s of out) s.lines = lines.get(s.id) || [];
                }
                return out;
            });
            res.json({ data });
        } catch (error) {
            sendError(res, error, '[GET /shipments]');
        }
    });

    // ── GET /shipments/next-reference?mode= ───────────────────────────────
    app.get('/api/v1/shipments/next-reference', async (req, res) => {
        try {
            await ready();
            const mode = String(req.query.mode || '').trim().toUpperCase();
            if (!S.isMode(mode)) return res.status(400).json({ error: 'mode must be SEA, AIR or ROAD.' });
            if (mode === 'ROAD') {
                return res.status(422).json({ error: 'ROAD shipments have no reference sequence; give an explicit reference when booking.', code: 'NO_SEQUENCE' });
            }
            const next = await withConnection(conn => sync.nextReference(conn, mode));
            res.json(next);
        } catch (error) {
            sendError(res, error, '[GET /shipments/next-reference]');
        }
    });

    // ── POST /shipments ───────────────────────────────────────────────────
    // { mode, stage?: 'DRAFT' | 'PLANNED', name?, reference?, originPort?, notes?, lines? }
    app.post('/api/v1/shipments', async (req, res) => {
        try {
            await ready();
            const b = req.body || {};
            const mode = String(b.mode || '').trim().toUpperCase();
            if (!S.isMode(mode)) return res.status(400).json({ error: 'mode must be SEA, AIR or ROAD.' });
            const stage = String(b.stage || 'DRAFT').trim().toUpperCase();
            if (stage !== 'DRAFT' && stage !== 'PLANNED') return res.status(400).json({ error: "stage must be 'DRAFT' or 'PLANNED'." });
            const lines = parseLines(b.lines);
            if (stage === 'PLANNED' && !lines.length) {
                return res.status(422).json({ error: 'A planned shipment needs at least one line (a planned container is its lines).', code: 'LINES_REQUIRED' });
            }
            let name = S.clean(b.name) || stampName(stage, mode);
            const reference = S.clean(b.reference);
            if (reference) {
                const parsed = S.parseReference(reference);
                if (parsed.mode && parsed.mode !== mode) {
                    return res.status(422).json({ error: `Reference "${reference}" belongs to the ${parsed.mode} sequence.`, code: 'REFERENCE_MODE_MISMATCH' });
                }
                const hint = S.parseNameHint(name);
                if (!hint || hint.reference.toLowerCase() !== parsed.reference.toLowerCase()) name = `${name} - ${parsed.reference}`;
            }
            if (name.length > 100) return res.status(400).json({ error: 'name must be 100 characters or fewer.' });
            const originPort = S.clean(b.originPort) ? S.clean(b.originPort).slice(0, 255) : null;
            const notes = S.clean(b.notes);

            const result = await withConnection(conn => inTransaction(conn, async () => {
                const kind = stage === 'PLANNED' ? 'P' : 'D';
                if (await sync.lockOpenShipment(conn, `${kind}:${name}`)) {
                    throw new ShipmentError(409, 'NAME_IN_USE', `An open ${stage.toLowerCase()} named "${name}" already exists.`);
                }
                const table = stage === 'PLANNED' ? 'planned_container_allocations' : 'draft_container_allocations';
                const column = stage === 'PLANNED' ? 'planned_container_name' : 'draft_container_name';
                const [[taken]] = await conn.query(`SELECT COUNT(*) AS n FROM ${table} WHERE ${column} = ?`, [name]);
                if (Number(taken.n) > 0) {
                    throw new ShipmentError(409, 'NAME_IN_USE', `An open ${stage.toLowerCase()} named "${name}" already exists.`);
                }
                if (reference) {
                    const parsed = S.parseReference(reference);
                    if (!(await sync.referenceIsFree(conn, parsed.reference))) {
                        throw new ShipmentError(409, 'REFERENCE_IN_USE', `Reference "${parsed.reference}" is already in use.`);
                    }
                }
                const warnings = [];
                for (const l of lines) {
                    const order = await liveOrder(conn, l.orderId);
                    const w = overAllocationWarning(l.orderId, l.quantity, order.quantity);
                    if (w) warnings.push(w);
                }
                let synced;
                if (stage === 'DRAFT') {
                    await draftAudit.ensureDraftRegistered(conn, name, req.userEmail);
                    for (const l of lines) await upsertDraftAllocation(conn, name, l.orderId, l.quantity, req.userEmail);
                    synced = await sync.syncDraft(conn, name, { origin: 'api', userEmail: req.userEmail, createIfMissing: true });
                } else {
                    for (const l of lines) {
                        await conn.query(
                            `INSERT INTO planned_container_allocations (order_id, planned_container_name, allocated) VALUES (?, ?, ?)`,
                            [l.orderId, name, l.quantity]
                        );
                    }
                    synced = await sync.syncPlanned(conn, name, { origin: 'api', userEmail: req.userEmail, createIfMissing: true });
                }
                if (!synced || !synced.shipmentId || synced.stampedOnly) throw new Error('shipment not created');
                await conn.query(
                    `UPDATE shipments
                        SET mode = ?, mode_source = 'user', origin_port = COALESCE(?, origin_port), notes = COALESCE(?, notes),
                            created_by_email = COALESCE(created_by_email, ?)
                      WHERE id = ?`,
                    [mode, originPort, notes, req.userEmail || null, synced.shipmentId]
                );
                return { id: synced.shipmentId, warnings };
            }));
            const detail = await withConnection(conn => shipmentDetail(conn, result.id));
            res.status(201).json({ ...detail, ...(result.warnings.length ? { warnings: result.warnings } : {}) });
        } catch (error) {
            sendError(res, error, '[POST /shipments]');
        }
    });

    // ── GET /shipments/:id ────────────────────────────────────────────────
    app.get('/api/v1/shipments/:id(\\d+)', async (req, res) => {
        try {
            await ready();
            const detail = await withConnection(conn => shipmentDetail(conn, idParam(req)));
            if (!detail) return res.status(404).json({ error: `Shipment ${req.params.id} not found.` });
            res.json(detail);
        } catch (error) {
            sendError(res, error, '[GET /shipments/:id]');
        }
    });

    // ── PATCH /shipments/:id ──────────────────────────────────────────────
    // Header fields. On a booked shipment reference / trackingRef / vesselName
    // / eta / etd write through to every member order (with per-order audit).
    const HEADER_FIELDS = {
        ata: { col: 'ata', type: 'date' },
        originPort: { col: 'origin_port', max: 255 },
        bookingRef: { col: 'booking_ref', max: 100 },
        blNumber: { col: 'bl_number', max: 100 },
        forwarder: { col: 'forwarder', max: 255 },
        notes: { col: 'notes', max: 65535 },
        reviewNote: { col: 'review_note', max: 255 },
    };
    const WRITE_THROUGH = {
        vesselName: { col: 'vessel_name', orderCol: 'vessel_name', max: 255 },
        eta: { col: 'eta', orderCol: 'eta', type: 'date' },
        etd: { col: 'etd', orderCol: 'estimated_departure_date', type: 'date' },
    };

    function fieldValue(key, spec, raw) {
        if (raw === null || raw === '') return null;
        if (spec.type === 'date') {
            const d = sync.validDate(raw);
            if (!d) throw new ShipmentError(400, 'BAD_DATE', `${key} must be a YYYY-MM-DD date.`);
            return d;
        }
        const s = S.clean(raw);
        return s == null ? null : s.slice(0, spec.max);
    }

    app.patch('/api/v1/shipments/:id(\\d+)', async (req, res) => {
        try {
            await ready();
            const b = req.body || {};
            const id = idParam(req);
            const result = await withConnection(conn => inTransaction(conn, async () => {
                const ship = await lockLive(conn, id);
                const booked = S.isBookedStage(ship.stage);
                const sets = [];
                const vals = [];
                const orderSets = {};
                const warnings = [];
                const before = {};
                const after = {};
                const change = (key, col, value) => {
                    before[key] = ship[col] instanceof Date ? ship[col].toISOString() : (ship[col] ?? null);
                    after[key] = value;
                    sets.push(`${col} = ?`);
                    vals.push(value);
                };

                if (b.name !== undefined) {
                    const newName = S.clean(b.name);
                    if (!newName) throw new ShipmentError(400, 'BAD_NAME', 'name cannot be empty.');
                    if (newName.length > 100) throw new ShipmentError(400, 'BAD_NAME', 'name must be 100 characters or fewer.');
                    if (newName !== ship.name) {
                        if (ship.stage === 'DRAFT') {
                            const r = await draftAudit.renameDraft(conn, { from: ship.name, to: newName, userEmail: req.userEmail });
                            if (r.conflict) throw new ShipmentError(409, 'NAME_IN_USE', `A draft named "${newName}" already exists.`);
                            change('name', 'name', newName);
                            sets.push('open_key = ?');
                            vals.push(`D:${newName}`);
                        } else if (ship.stage === 'PLANNED') {
                            if (newName.toLowerCase() !== String(ship.name).toLowerCase()) {
                                const [[taken]] = await conn.query(
                                    `SELECT COUNT(*) AS n FROM planned_container_allocations WHERE planned_container_name = ?`, [newName]
                                );
                                if (Number(taken.n) > 0) throw new ShipmentError(409, 'NAME_IN_USE', `A planned container named "${newName}" already exists.`);
                            }
                            await conn.query(
                                `UPDATE planned_container_allocations SET planned_container_name = ? WHERE planned_container_name = ?`,
                                [newName, ship.name]
                            );
                            change('name', 'name', newName);
                            sets.push('open_key = ?');
                            vals.push(`P:${newName}`);
                        } else {
                            // Booked: a label only. renameDraft here would reopen
                            // the closed registry row and wipe what the draft became.
                            change('name', 'name', newName);
                        }
                    }
                }
                if (b.mode !== undefined) {
                    const m = String(b.mode || '').trim().toUpperCase();
                    if (!S.isMode(m)) throw new ShipmentError(400, 'BAD_MODE', 'mode must be SEA, AIR or ROAD.');
                    if (m !== ship.mode || ship.mode_source !== 'user') {
                        change('mode', 'mode', m);
                        sets.push(`mode_source = 'user'`);
                    }
                }
                if (b.needsReview !== undefined) change('needsReview', 'needs_review', b.needsReview ? 1 : 0);
                for (const [key, spec] of Object.entries(HEADER_FIELDS)) {
                    if (b[key] === undefined) continue;
                    change(key, spec.col, fieldValue(key, spec, b[key]));
                }
                for (const [key, spec] of Object.entries(WRITE_THROUGH)) {
                    if (b[key] === undefined) continue;
                    const v = fieldValue(key, spec, b[key]);
                    change(key, spec.col, v);
                    if (booked) orderSets[spec.orderCol] = v;
                }

                let newReference = null;
                if (b.reference !== undefined) {
                    if (!booked) throw new ShipmentError(422, 'REFERENCE_AT_BOOKING', 'A reference is assigned when the shipment is booked.');
                    const parsed = S.parseReference(b.reference);
                    if (!parsed) throw new ShipmentError(400, 'BAD_REFERENCE', 'reference cannot be empty.');
                    if (parsed.reference !== ship.reference) {
                        const mode = after.mode || ship.mode;
                        if (parsed.mode && mode && parsed.mode !== mode) {
                            throw new ShipmentError(422, 'REFERENCE_MODE_MISMATCH', `Reference "${parsed.reference}" belongs to the ${parsed.mode} sequence.`);
                        }
                        if (!(await sync.referenceIsFree(conn, parsed.reference, { exceptShipmentId: ship.id }))) {
                            throw new ShipmentError(409, 'REFERENCE_IN_USE', `Reference "${parsed.reference}" is already in use.`);
                        }
                        change('reference', 'reference', parsed.reference.slice(0, 100));
                        sets.push('reference_seq = ?');
                        vals.push(parsed.seq);
                        newReference = parsed.reference.slice(0, 100);
                        orderSets.container_number = newReference;
                    }
                }

                let trackingChange;
                if (b.trackingRef !== undefined) {
                    const v = S.clean(b.trackingRef) ? S.clean(b.trackingRef).slice(0, 255) : null;
                    change('trackingRef', 'tracking_ref', v);
                    if (booked) trackingChange = { value: v };
                }

                if (!sets.length && !Object.keys(orderSets).length && trackingChange === undefined) {
                    return { id, warnings, unchanged: true };
                }
                if (sets.length) {
                    try {
                        await conn.query(`UPDATE shipments SET ${sets.join(', ')} WHERE id = ?`, [...vals, ship.id]);
                    } catch (e) {
                        if (e.code === 'ER_DUP_ENTRY') throw new ShipmentError(409, 'CONFLICT', 'The new name or reference is already in use.');
                        throw e;
                    }
                }

                // Write-through to the member orders, one audited UPDATE each.
                if (booked && (Object.keys(orderSets).length || trackingChange !== undefined)) {
                    const [members] = await conn.query(
                        `SELECT * FROM orders WHERE shipment_id = ? AND deleted_at IS NULL ORDER BY id FOR UPDATE`, [ship.id]
                    );
                    const mode = after.mode || ship.mode;
                    for (const m of members) {
                        const oSets = { ...orderSets };
                        if (trackingChange !== undefined) {
                            const prev = S.trackingRefFor(mode, [m]);
                            if (trackingChange.value) {
                                const col = S.carrierColumnFor(trackingChange.value);
                                oSets[col] = trackingChange.value;
                                if (prev.sourceColumn && prev.sourceColumn !== col) oSets[prev.sourceColumn] = null;
                            } else if (prev.sourceColumn) {
                                oSets[prev.sourceColumn] = null;
                            }
                        }
                        const cols = Object.keys(oSets).filter(c => !sameValue(m[c], oSets[c]));
                        if (!cols.length) continue;
                        const [beforeRows] = await conn.query(`${ORDER_SELECT} AND orders.id = ?`, [m.id]);
                        await conn.query(
                            `UPDATE orders SET ${cols.map(c => `${c} = ?`).join(', ')} WHERE id = ?`,
                            [...cols.map(c => oSets[c]), m.id]
                        );
                        const [afterRows] = await conn.query(`${ORDER_SELECT} AND orders.id = ?`, [m.id]);
                        await recordAudit(conn, {
                            entityType: 'order', entityId: m.id, action: 'update',
                            before: S.auditSnapshot(rowToOrder(beforeRows[0])), after: S.auditSnapshot(rowToOrder(afterRows[0])),
                            userEmail: req.userEmail,
                        });
                    }
                    if (orderSets.eta !== undefined || orderSets.vessel_name !== undefined) {
                        const ref = trackingChange ? trackingChange.value : (S.clean(ship.tracking_ref) || null);
                        if (ref && await hasShipsgoCache(conn, ref)) {
                            warnings.push(`ShipsGo tracks ${ref}: its hourly sync overwrites eta and vessel on these orders.`);
                        }
                    }
                }
                await recordAudit(conn, {
                    entityType: 'shipment', entityId: ship.id, action: 'update', before, after, userEmail: req.userEmail,
                });
                return { id, warnings };
            }));
            const detail = await withConnection(conn => shipmentDetail(conn, result.id));
            res.json({ ...detail, ...(result.warnings.length ? { warnings: result.warnings } : {}) });
        } catch (error) {
            sendError(res, error, '[PATCH /shipments/:id]');
        }
    });

    function sameValue(a, b) {
        if (a == null && b == null) return true;
        if (a == null || b == null) return false;
        const av = a instanceof Date ? a.toISOString().slice(0, 10) : String(a);
        return av === String(b);
    }

    async function hasShipsgoCache(conn, ref) {
        try {
            const [c] = await conn.query(`SELECT 1 FROM containers WHERE container_number = ? LIMIT 1`, [ref]);
            if (c.length) return true;
            const [a] = await conn.query(`SELECT 1 FROM air_shipments WHERE awb_number = ? LIMIT 1`, [ref]);
            return a.length > 0;
        } catch (_) {
            return false;
        }
    }

    // ── DELETE /shipments/:id ─────────────────────────────────────────────
    // Soft delete, keys released. PLANNED / DRAFT / CANCELLED, or a BOOKED
    // shipment with no live members. Writes through, or the old SPA would keep
    // showing it: a draft closes 'deleted', a planned container's rows go.
    app.delete('/api/v1/shipments/:id(\\d+)', async (req, res) => {
        try {
            await ready();
            const id = idParam(req);
            await withConnection(conn => inTransaction(conn, async () => {
                const ship = await lockLive(conn, id);
                if (S.isOpenStage(ship.stage)) {
                    await cancelOpen(conn, ship, { userEmail: req.userEmail, reason: 'deleted' });
                    await conn.query(`UPDATE shipments SET deleted_at = NOW() WHERE id = ?`, [ship.id]);
                } else if (ship.stage === 'CANCELLED' || ship.stage === 'BOOKED') {
                    if (ship.stage === 'BOOKED') {
                        const [[m]] = await conn.query(
                            `SELECT COUNT(*) AS n FROM orders WHERE shipment_id = ? AND deleted_at IS NULL`, [ship.id]
                        );
                        if (Number(m.n) > 0) {
                            throw new ShipmentError(409, 'HAS_MEMBERS', `Shipment ${id} still has ${m.n} order(s); move them out first.`);
                        }
                    }
                    await conn.query(
                        `UPDATE shipments SET deleted_at = NOW(), reference = NULL, open_key = NULL WHERE id = ?`, [ship.id]
                    );
                    await conn.query(`DELETE FROM shipment_lines WHERE shipment_id = ?`, [ship.id]);
                } else {
                    throw new ShipmentError(409, 'NOT_DELETABLE', `A ${ship.stage} shipment cannot be deleted.`);
                }
                await recordAudit(conn, {
                    entityType: 'shipment', entityId: ship.id, action: 'delete',
                    before: { stage: ship.stage, reference: ship.reference || null, name: ship.name || null }, after: null,
                    userEmail: req.userEmail,
                });
            }));
            res.json({ ok: true, id });
        } catch (error) {
            sendError(res, error, '[DELETE /shipments/:id]');
        }
    });

    // ── PUT / DELETE /shipments/:id/lines/:orderId ────────────────────────
    // Natural-key upsert of one line, PLANNED / DRAFT only. Over-allocation is
    // a warning, not a rejection (legacy parity).
    app.put('/api/v1/shipments/:id(\\d+)/lines/:orderId(\\d+)', async (req, res) => {
        try {
            await ready();
            const id = idParam(req);
            const orderId = Number(req.params.orderId);
            const quantity = Number((req.body || {}).quantity);
            if (!Number.isFinite(quantity) || quantity < 0) return res.status(400).json({ error: 'quantity must be a non-negative number.' });
            const warnings = await withConnection(conn => inTransaction(conn, async () => {
                const ship = await lockLive(conn, id);
                if (!S.isOpenStage(ship.stage)) throw new ShipmentError(409, 'NOT_OPEN', `Lines of a ${ship.stage} shipment follow its orders; book or edit orders instead.`);
                const order = await liveOrder(conn, orderId);
                if (ship.stage === 'DRAFT') {
                    await upsertDraftAllocation(conn, ship.name, orderId, quantity, req.userEmail);
                    await sync.syncDraft(conn, ship.name, { userEmail: req.userEmail, origin: 'api' });
                } else {
                    await conn.query(
                        `INSERT INTO planned_container_allocations (order_id, planned_container_name, allocated) VALUES (?, ?, ?)
                         ON DUPLICATE KEY UPDATE allocated = VALUES(allocated)`,
                        [orderId, ship.name, quantity]
                    );
                    await sync.syncPlanned(conn, ship.name, { userEmail: req.userEmail, origin: 'api' });
                    await recordAudit(conn, {
                        entityType: 'shipment', entityId: ship.id, action: 'line_set',
                        before: null, after: { orderId, quantity }, userEmail: req.userEmail,
                    });
                }
                const w = overAllocationWarning(orderId, quantity, order.quantity);
                return w ? [w] : [];
            }));
            const lines = await withConnection(conn => sync.loadLines(conn, id));
            res.json({ line: lines.find(l => l.orderId === orderId) || null, lines, ...(warnings.length ? { warnings } : {}) });
        } catch (error) {
            sendError(res, error, '[PUT /shipments/:id/lines/:orderId]');
        }
    });

    app.delete('/api/v1/shipments/:id(\\d+)/lines/:orderId(\\d+)', async (req, res) => {
        try {
            await ready();
            const id = idParam(req);
            const orderId = Number(req.params.orderId);
            const result = await withConnection(conn => inTransaction(conn, async () => {
                const ship = await lockLive(conn, id);
                if (!S.isOpenStage(ship.stage)) throw new ShipmentError(409, 'NOT_OPEN', `Lines of a ${ship.stage} shipment follow its orders.`);
                if (ship.stage === 'DRAFT') {
                    const [rows] = await conn.query(
                        `${DRAFT_LINE_SNAPSHOT_SELECT} WHERE dca.draft_container_name = ? AND dca.order_id = ? FOR UPDATE`,
                        [ship.name, orderId]
                    );
                    if (!rows.length) throw new ShipmentError(404, 'LINE_NOT_FOUND', `Order ${orderId} is not a line of shipment ${id}.`);
                    await conn.query(`DELETE FROM draft_container_allocations WHERE id = ?`, [rows[0].id]);
                    const reg = await draftAudit.ensureDraftRegistered(conn, ship.name, req.userEmail);
                    await draftAudit.recordDraftAudit(conn, {
                        draftId: reg.id, action: 'line_removed',
                        before: { draftName: reg.name, ...draftAudit.lineSnapshot(rows[0]) }, userEmail: req.userEmail,
                    });
                    await sync.syncDraft(conn, ship.name, { userEmail: req.userEmail, origin: 'api' });
                    return { cancelled: false };
                }
                const [del] = await conn.query(
                    `DELETE FROM planned_container_allocations WHERE planned_container_name = ? AND order_id = ?`, [ship.name, orderId]
                );
                if (!del.affectedRows) throw new ShipmentError(404, 'LINE_NOT_FOUND', `Order ${orderId} is not a line of shipment ${id}.`);
                await recordAudit(conn, {
                    entityType: 'shipment', entityId: ship.id, action: 'line_removed',
                    before: { orderId }, after: null, userEmail: req.userEmail,
                });
                const r = await sync.syncPlanned(conn, ship.name, { userEmail: req.userEmail, origin: 'api' });
                return { cancelled: !!(r && r.cancelled) };
            }));
            res.json({ ok: true, id, orderId, ...(result.cancelled ? { shipmentCancelled: true } : {}) });
        } catch (error) {
            sendError(res, error, '[DELETE /shipments/:id/lines/:orderId]');
        }
    });

    // ── POST /shipments/:id/book ──────────────────────────────────────────
    app.post('/api/v1/shipments/:id(\\d+)/book', async (req, res) => {
        try {
            await ready();
            const id = idParam(req);
            const r = await withConnection(conn => inTransaction(conn,
                () => bookShipment(conn, { shipmentId: id, body: req.body || {}, userEmail: req.userEmail })));
            const detail = await withConnection(conn => shipmentDetail(conn, id));
            if (r.alreadyBooked) return res.status(200).json({ ...detail, alreadyBooked: true });
            res.status(200).json({
                ...detail,
                booking: { reference: r.reference, referenceSource: r.referenceSource, packedOrderIds: r.packedOrderIds, draft: r.draft },
            });
        } catch (error) {
            sendError(res, error, '[POST /shipments/:id/book]');
        }
    });

    // ── POST /shipments/:id/transition ────────────────────────────────────
    app.post('/api/v1/shipments/:id(\\d+)/transition', async (req, res) => {
        try {
            await ready();
            const id = idParam(req);
            const b = req.body || {};
            const r = await withConnection(conn => inTransaction(conn, () => transitionShipment(conn, {
                shipmentId: id, stage: b.stage, note: b.note, force: b.force === true,
                isAdmin: req.userType === 'admin', userEmail: req.userEmail,
            })));
            const detail = await withConnection(conn => shipmentDetail(conn, id));
            res.json({ ...r, shipment: detail });
        } catch (error) {
            sendError(res, error, '[POST /shipments/:id/transition]');
        }
    });

    // ── Documents ─────────────────────────────────────────────────────────
    // Listed by shipment_id, so they survive renames and span every
    // generation merged into this shipment.
    app.get('/api/v1/shipments/:id(\\d+)/documents', async (req, res) => {
        try {
            await ready();
            const id = idParam(req);
            const out = await withConnection(async (conn) => {
                const row = await sync.getShipment(conn, id);
                if (!row) return null;
                const ids = [id, ...(await mergedInto(conn, id))];
                const idPh = ids.map(() => '?').join(',');
                const [docs] = await conn.query(
                    `SELECT id, draft_container_name, version, type, supplier, s3_key, public_url, file_size,
                            csv_s3_key, csv_public_url, csv_file_size, batch_id, generated_by_email, generated_at, shipment_id
                       FROM draft_container_documents
                      WHERE shipment_id IN (${idPh}) AND deleted_at IS NULL
                      ORDER BY type ASC, generated_at DESC, id DESC`, ids);
                const [qa] = await conn.query(
                    `SELECT id, ref, version, draft_container_name, order_ids, qc_units, s3_key, public_url, file_size,
                            csv_s3_key, csv_public_url, csv_file_size, comments, generated_by_email, generated_at, shipment_id
                       FROM quality_assurance_documents
                      WHERE shipment_id IN (${idPh}) AND deleted_at IS NULL
                      ORDER BY generated_at DESC, id DESC`, ids);
                const docSends = await sendsFor(conn, 'draft_container_document_sends', 'draft_container_document_id', docs.map(d => d.id));
                const qaSends = await sendsFor(conn, 'quality_assurance_document_sends', 'quality_assurance_document_id', qa.map(d => d.id));
                return { docs, qa, docSends, qaSends };
            });
            if (!out) return res.status(404).json({ error: `Shipment ${id} not found.` });
            const parseJson = v => (typeof v === 'string' ? JSON.parse(v) : v);
            res.json({
                data: out.docs.map(r => ({
                    id: r.id,
                    shipmentId: r.shipment_id,
                    draftContainerName: r.draft_container_name,
                    version: r.version,
                    type: r.type || 'quote',
                    supplier: r.supplier || null,
                    batchId: r.batch_id || null,
                    fileSize: r.file_size,
                    url: r.public_url || publicS3Url(r.s3_key),
                    csvUrl: r.csv_public_url || (r.csv_s3_key ? publicS3Url(r.csv_s3_key) : null),
                    csvFileSize: r.csv_file_size ?? null,
                    generatedByEmail: r.generated_by_email || null,
                    generatedAt: r.generated_at?.toISOString?.() ?? r.generated_at,
                    sends: (out.docSends.get(r.id) || []).map(sendToJson),
                })),
                qaDocuments: out.qa.map(r => {
                    const orderIds = parseJson(r.order_ids) || [];
                    return {
                        id: r.id,
                        shipmentId: r.shipment_id,
                        ref: r.ref,
                        version: r.version,
                        draftContainerName: r.draft_container_name || null,
                        orderIds,
                        qcUnits: parseJson(r.qc_units) || {},
                        rowCount: orderIds.length,
                        comments: r.comments || null,
                        fileSize: r.file_size,
                        url: r.public_url || publicS3Url(r.s3_key),
                        csvUrl: r.csv_public_url || (r.csv_s3_key ? publicS3Url(r.csv_s3_key) : null),
                        csvFileSize: r.csv_file_size ?? null,
                        generatedByEmail: r.generated_by_email || null,
                        generatedAt: r.generated_at?.toISOString?.() ?? r.generated_at,
                        sends: (out.qaSends.get(r.id) || []).map(sendToJson),
                    };
                }),
            });
        } catch (error) {
            sendError(res, error, '[GET /shipments/:id/documents]');
        }
    });

    async function sendsFor(conn, table, fk, ids) {
        const out = new Map();
        if (!ids.length) return out;
        const [rows] = await conn.query(
            `SELECT id, ${fk} AS parent_id, sent_to, subject, front_message_uid, front_conversation_id, sent_by_email, sent_at
               FROM ${table} WHERE ${fk} IN (${ids.map(() => '?').join(',')}) ORDER BY sent_at DESC`, ids
        );
        for (const r of rows) {
            if (!out.has(r.parent_id)) out.set(r.parent_id, []);
            out.get(r.parent_id).push(r);
        }
        return out;
    }

    function sendToJson(s) {
        return {
            id: s.id,
            sentTo: typeof s.sent_to === 'string' ? JSON.parse(s.sent_to) : s.sent_to,
            subject: s.subject || null,
            frontMessageUid: s.front_message_uid || null,
            frontConversationId: s.front_conversation_id || null,
            sentByEmail: s.sent_by_email || null,
            sentAt: s.sent_at?.toISOString?.() ?? s.sent_at,
        };
    }

    // Generate: the same document set the legacy generate route produces (the
    // per-supplier split, the shared batch id the email route attaches by, and
    // the one document_generated event), then stamped with this shipment.
    app.post('/api/v1/shipments/:id(\\d+)/documents', async (req, res) => {
        try {
            await ready();
            if (!poBucket()) return res.status(500).json({ error: 'PO_DOCS_BUCKET env var not configured.' });
            const id = idParam(req);
            const { comments, type, supplier } = req.body || {};
            const docType = type === undefined || type === null || type === '' ? 'quote' : String(type);
            if (docType !== 'quote' && docType !== 'forwarder-quote' && docType !== 'supplier-quote') {
                return res.status(400).json({ error: "type must be 'quote', 'forwarder-quote' or 'supplier-quote'." });
            }
            const supplierName = docType === 'supplier-quote' ? String(supplier || '').trim() : null;
            if (docType === 'supplier-quote' && !supplierName) {
                return res.status(400).json({ error: "supplier is required for type 'supplier-quote'." });
            }
            const wantSplit = docType === 'forwarder-quote' && (req.body || {}).splitBySupplier !== false;

            const result = await withConnection(async (conn) => {
                const [[ship]] = await conn.query(`SELECT * FROM shipments WHERE id = ?`, [id]);
                if (!ship || ship.deleted_at) throw new ShipmentError(404, 'NOT_FOUND', `Shipment ${id} not found.`);
                if (ship.merged_into_id) throw new ShipmentError(409, 'MERGED', `Shipment ${id} was merged into ${ship.merged_into_id}.`, { mergedIntoId: ship.merged_into_id });
                if (ship.stage !== 'DRAFT') throw new ShipmentError(409, 'NOT_A_DRAFT', 'Documents are generated while the shipment is a DRAFT.');
                const set = await generateDraftDocumentSet(conn, {
                    name: ship.name, docType, supplierName, comments, wantSplit, userEmail: req.userEmail,
                });
                if (set.noLines) return { noLines: true, name: ship.name };
                const docIds = [set.main.documentId, ...(set.supplierDocuments || []).map(d => d.documentId)];
                await conn.query(
                    `UPDATE draft_container_documents SET shipment_id = ? WHERE id IN (${docIds.map(() => '?').join(',')})`,
                    [id, ...docIds]
                );
                return set;
            });
            if (result.noLines) {
                const msg = docType === 'supplier-quote'
                    ? `Shipment ${id} has no lines for supplier "${supplierName}".`
                    : `Shipment ${id} has no lines.`;
                return res.status(400).json({ error: msg });
            }
            const { main } = result;
            const payload = {
                shipmentId: id,
                documentId: main.documentId,
                draftContainerName: main.draftContainerName,
                type: docType,
                supplier: supplierName,
                version: main.version,
                fileSize: main.fileSize,
                url: main.url,
                csvUrl: main.csvUrl,
            };
            if (wantSplit) {
                payload.batchId = result.batchId;
                payload.supplierDocuments = (result.supplierDocuments || []).map(d => ({
                    documentId: d.documentId, type: d.type, supplier: d.supplier, version: d.version,
                    fileSize: d.fileSize, url: d.url, csvUrl: d.csvUrl,
                }));
                if ((result.failedSuppliers || []).length) payload.failedSuppliers = result.failedSuppliers;
            }
            res.status(201).json(payload);
        } catch (error) {
            sendError(res, error, '[POST /shipments/:id/documents]');
        }
    });

    // ── GET /shipments/:id/history ────────────────────────────────────────
    // The shipment's own events plus the draft events of every registry row
    // linked to it (and to the shipments merged into it), newest first.
    app.get('/api/v1/shipments/:id(\\d+)/history', async (req, res) => {
        try {
            await ready();
            const id = idParam(req);
            let limit = parseInt(req.query.limit, 10);
            if (!Number.isFinite(limit) || limit <= 0) limit = 500;
            if (limit > 2000) limit = 2000;
            const rows = await withConnection(async (conn) => {
                const row = await sync.getShipment(conn, id);
                if (!row) return null;
                const shipIds = [id, ...(await mergedInto(conn, id))];
                const idPh = shipIds.map(() => '?').join(',');
                const [regs] = await conn.query(
                    `SELECT id FROM draft_containers WHERE shipment_id IN (${idPh})
                     UNION
                     SELECT source_draft_id FROM shipments WHERE id IN (${idPh}) AND source_draft_id IS NOT NULL`,
                    [...shipIds, ...shipIds]
                );
                const regIds = regs.map(r => r.id).filter(x => x != null);
                const [events] = await conn.query(
                    `SELECT id, entity_type, entity_id, action, before_json, after_json, user_email, created_at
                       FROM audit_log
                      WHERE (entity_type = 'shipment' AND entity_id IN (${idPh}))
                         ${regIds.length ? `OR (entity_type = 'draft_container' AND entity_id IN (${regIds.map(() => '?').join(',')}))` : ''}
                      ORDER BY id DESC
                      LIMIT ${limit}`,
                    [...shipIds, ...regIds]
                );
                return events;
            });
            if (!rows) return res.status(404).json({ error: `Shipment ${id} not found.` });
            const parse = v => (v == null ? null : typeof v === 'string' ? (() => { try { return JSON.parse(v); } catch { return v; } })() : v);
            res.json({
                data: rows.map(r => ({
                    id: r.id,
                    entityType: r.entity_type,
                    entityId: r.entity_id,
                    action: r.action,
                    before: parse(r.before_json),
                    after: parse(r.after_json),
                    userEmail: r.user_email || null,
                    createdAt: r.created_at?.toISOString?.() ?? r.created_at,
                })),
            });
        } catch (error) {
            sendError(res, error, '[GET /shipments/:id/history]');
        }
    });
}

module.exports = { registerShipmentRoutes };
