'use strict';

// /api/v1/forwarders, /api/v1/freight-delivery-options, /api/v1/freight-quotes,
// /api/v1/freight-bookings — asking freight forwarders for sailings and prices,
// booking one, and sending the container's contents (docs/freight-quotes.md).
//
// Staff keep a list of forwarders (name + contacts). A request goes to one or
// more of them for a date range: each gets its own row and its own no-login
// link into the supplier portal, <SUPPLIER_PORTAL_URL>/freight/<token>, where
// the forwarder lists its options; a contact who signs in to the portal sees
// the same requests. The portal writes the answer into THIS schema (column
// grants, 2026-11-16_11), so nothing is pushed here: staff see it on the next
// read. Staff request a booking on one option; the forwarder confirms it on
// the same page with a booking reference (or staff mark it). A draft container
// is attached to the booking and its contents, by supplier, are emailed.
//
// A request may carry stock movements (collections from a factory, with CBM
// and weight): rows of their own (freight_stock_movements, 2026-11-17_20),
// each priced by the forwarder on its own line, apart from the sailing. Staff
// can add one at any time while the link is not closed, after a quote and
// after a booking too, and the forwarder is emailed to price it.
//
// The pure rules are in src/lib/freight-quotes.js. Registered from orders.js;
// authed through the greedy /api/v1/{proxy+} route. Reads are open to every
// staff user; writes need an editor role, as the QC routes do.

const crypto = require('crypto');
const { GetObjectCommand } = require('@aws-sdk/client-s3');
const F = require('../lib/freight-quotes');
const QV = require('../lib/qc-visits');
const S = require('../lib/shipments');
const poPortalEmail = require('../lib/po-portal-email');
const { escapeHtml, wrapEmail } = require('../lib/email-layout');
const {
    FREIGHT_QUOTE_REQUEST_TEMPLATE_KEY, FREIGHT_BOOKING_REQUEST_TEMPLATE_KEY, FREIGHT_BOOKING_CONTENTS_TEMPLATE_KEY,
    FREIGHT_MOVEMENT_REQUEST_TEMPLATE_KEY,
} = require('../lib/email-template-defaults');
const { sendFrontMessage: defaultSend } = require('./front-send');
const { preapprovePortalAddresses: defaultPreapprove } = require('./portal-access');
const { logSentEmail, recordReminderSend } = require('../lib/email-receipt');
const { londonToday: defaultLondonToday } = require('./daily-alerts');
const defaultLog = require('../lib/logger');

// The tables are not there yet (the migration has not run on this server).
const TABLE_MISSING = 1146;
const DUPLICATE = 1062;

// Requests whose range ended longer ago than this are left out of the board
// unless the caller asks for a range.
const BOARD_DAYS_BACK = 120;
const BOARD_LIMIT = 500;
// A booking, or a cancelled one, keeps the link open at least this long.
const BOOKING_EXTENDS_DAYS = 30;

class Fail extends Error {
    constructor(status, code, error, extra) {
        super(error);
        this.status = status;
        this.code = code;
        this.error = error;
        this.extra = extra || null;
    }
}

const iso = v => (v instanceof Date ? v.toISOString() : (v == null ? null : String(v)));
const LIVE = F.LIVE_BOOKING_STATUSES;
const isLive = b => Boolean(b) && LIVE.includes(b.status);

const REQUEST_COLUMNS = `r.id, r.token, r.batch_id, r.forwarder_id, r.forwarder_name, r.window_start_date,
        r.window_end_date, r.origin, r.destination, r.container_size, r.services, r.staff_note, r.sent_to,
        r.subject, r.created_by_email, r.created_at, r.sent_at, r.expires_at, r.revoked_at, r.revoke_reason,
        r.answer_count, r.answered_at, r.answered_by_name, r.answered_by_email, r.answer_source, r.answer_note,
        r.declined, r.row_version, r.reminded_at, r.reminder_count, (r.expires_at <= UTC_TIMESTAMP()) AS expired`;

function optionJson(o) {
    return {
        id: Number(o.id),
        requestId: Number(o.request_id),
        lineNo: Number(o.line_no),
        loadingDate: F.toYmd(o.loading_date),
        sailingDate: F.toYmd(o.sailing_date),
        transitDays: Number(o.transit_days),
        arrivalDate: F.arrivalDate(o.sailing_date, Number(o.transit_days)),
        portFrom: o.port_from,
        portTo: o.port_to,
        service: o.service,
        containerSize: o.container_size,
        price: Number(o.price),
        currency: o.currency,
        carrier: o.carrier || null,
        validUntil: F.toYmd(o.valid_until),
        note: o.note || null,
    };
}

// A stock movement as the board carries it. `price` null = not priced yet.
function movementJson(m) {
    return {
        id: Number(m.id),
        requestId: Number(m.request_id),
        from: m.factory,
        cbm: Number(m.cbm),
        weightKg: Number(m.weight_kg),
        addedAt: iso(m.created_at),
        addedByEmail: m.created_by_email || null,
        price: m.price === null || m.price === undefined ? null : Number(m.price),
        currency: m.currency || null,
        priceNote: m.price_note || null,
        pricedAt: iso(m.priced_at),
        pricedBy: m.priced_by_email || m.priced_by_name || null,
        priceSource: m.price_source || null,
        rowVersion: Number(m.row_version),
    };
}

// The request's own facts, as the emails and the validators read them.
function requestFactsOf(row) {
    return {
        windowStart: F.toYmd(row.window_start_date),
        windowEnd: F.toYmd(row.window_end_date),
        origin: row.origin || '',
        destination: row.destination || '',
        containerSize: row.container_size,
        // The delivery options asked about, as text (an old request holds the four codes).
        services: F.servicesOf(row.services),
        note: row.staff_note || null,
    };
}

function registerFreightQuoteRoutes(app, deps) {
    const {
        withConnection, recordAudit, getEmailTemplate, renderTemplate,
        loadForwarderQuoteForPdf, generateDraftDocumentSet, s3,
        poBucket = () => null,
        ready = async () => {},
        sendFrontMessage = defaultSend,
        preapprove = defaultPreapprove,
        londonToday = defaultLondonToday,
        log = defaultLog,
    } = deps;

    const route = (label, fn) => async (req, res) => {
        try {
            await ready();
            await fn(req, res);
        } catch (e) {
            if (res.headersSent) return;
            if (e instanceof Fail) return res.status(e.status).json({ error: e.error, code: e.code, ...(e.extra || {}) });
            if (e instanceof S.ShipmentError) return res.status(e.status).json({ error: e.message, code: e.code });
            log.error(`[${label}]`, e);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    };

    function refuseReadOnly(req) {
        if (!QV.canEdit(req.userType)) throw new Fail(403, 'READ_ONLY', 'Read only.');
    }

    const invalid = v => new Fail(400, v.code || 'BAD_FIELD', v.error, v.field ? { field: v.field } : null);

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

    // ── forwarders ──────────────────────────────────────────────────────────

    async function loadForwarders(conn, ids = null) {
        if (ids && !ids.length) return [];
        const [rows] = await conn.query(
            `SELECT id, name, notes, created_by_email, created_at, updated_at
               FROM forwarders
              WHERE deleted_at IS NULL ${ids ? 'AND id IN (?)' : ''}
              ORDER BY name ASC, id ASC`,
            ids ? [ids] : []
        );
        if (!rows.length) return [];
        const [contacts] = await conn.query(
            `SELECT id, forwarder_id, name, email
               FROM forwarder_contacts
              WHERE deleted_at IS NULL AND forwarder_id IN (?)
              ORDER BY id ASC`,
            [rows.map(r => r.id)]
        );
        return rows.map(r => ({
            id: Number(r.id),
            name: r.name,
            notes: r.notes || null,
            contacts: contacts.filter(c => Number(c.forwarder_id) === Number(r.id))
                .map(c => ({ id: Number(c.id), name: c.name || null, email: c.email })),
            createdAt: iso(r.created_at),
            updatedAt: iso(r.updated_at),
        }));
    }

    async function loadForwarder(conn, id) {
        const [found] = await loadForwarders(conn, [id]);
        if (!found) throw new Fail(404, 'NOT_FOUND', `Forwarder ${id} not found.`);
        return found;
    }

    // The contacts as the form sent them: a contact no longer listed is removed
    // (soft: its address stops matching on the portal at once), a new one is
    // added, a kept one takes the name typed.
    async function saveContacts(conn, forwarderId, contacts) {
        const [existing] = await conn.query(
            'SELECT id, email FROM forwarder_contacts WHERE forwarder_id = ? AND deleted_at IS NULL',
            [forwarderId]
        );
        const wanted = new Map(contacts.map(c => [c.email, c]));
        for (const row of existing) {
            const keep = wanted.get(row.email);
            if (!keep) {
                await conn.query('UPDATE forwarder_contacts SET deleted_at = UTC_TIMESTAMP() WHERE id = ?', [row.id]);
            } else {
                await conn.query('UPDATE forwarder_contacts SET name = ? WHERE id = ?', [keep.name, row.id]);
                wanted.delete(row.email);
            }
        }
        for (const c of wanted.values()) {
            await conn.query('INSERT INTO forwarder_contacts (forwarder_id, name, email) VALUES (?, ?, ?)', [forwarderId, c.name, c.email]);
        }
    }

    // ── GET /api/v1/forwarders ──────────────────────────────────────────────
    // { forwarders: [{ id, name, notes, contacts: [{ id, name, email }], createdAt, updatedAt }] }
    app.get('/api/v1/forwarders', route('GET /forwarders', async (req, res) => {
        const forwarders = await withConnection(async (conn) => {
            try {
                return await loadForwarders(conn);
            } catch (e) {
                if (e.errno !== TABLE_MISSING) throw e;
                return [];
            }
        });
        res.json({ forwarders });
    }));

    // ── POST /api/v1/forwarders ─────────────────────────────────────────────
    // Body { name, notes?, contacts: [{ name?, email }] } → { forwarder }
    app.post('/api/v1/forwarders', route('POST /forwarders', async (req, res) => {
        refuseReadOnly(req);
        const v = F.validateForwarder(req.body);
        if (v.code) throw invalid(v);
        const forwarder = await withConnection(conn => inTransaction(conn, async () => {
            const [ins] = await conn.query(
                'INSERT INTO forwarders (name, notes, created_by_email) VALUES (?, ?, ?)',
                [v.value.name, v.value.notes, req.userEmail || null]
            );
            const id = Number(ins.insertId);
            await saveContacts(conn, id, v.value.contacts);
            await recordAudit(conn, {
                entityType: 'forwarder', entityId: id, action: 'create',
                before: null, after: v.value, userEmail: req.userEmail,
            });
            return loadForwarder(conn, id);
        }));
        res.status(201).json({ forwarder });
    }));

    // ── PATCH /api/v1/forwarders/:id ────────────────────────────────────────
    // The whole forwarder again: { name, notes?, contacts } → { forwarder }
    app.patch('/api/v1/forwarders/:id(\\d+)', route('PATCH /forwarders/:id', async (req, res) => {
        refuseReadOnly(req);
        const id = Number(req.params.id);
        const v = F.validateForwarder(req.body);
        if (v.code) throw invalid(v);
        const forwarder = await withConnection(conn => inTransaction(conn, async () => {
            const before = await loadForwarder(conn, id);
            await conn.query('UPDATE forwarders SET name = ?, notes = ? WHERE id = ?', [v.value.name, v.value.notes, id]);
            await saveContacts(conn, id, v.value.contacts);
            await recordAudit(conn, {
                entityType: 'forwarder', entityId: id, action: 'update',
                before: { name: before.name, notes: before.notes, contacts: before.contacts.map(c => c.email) },
                after: { name: v.value.name, notes: v.value.notes, contacts: v.value.contacts.map(c => c.email) },
                userEmail: req.userEmail,
            });
            return loadForwarder(conn, id);
        }));
        res.json({ forwarder });
    }));

    // ── DELETE /api/v1/forwarders/:id ───────────────────────────────────────
    // Soft. Its contacts go with it (no portal access through them any more) and
    // its open links are closed as `forwarder_removed`. Refused while one of its
    // bookings is in progress: cancel that first. A finished booking (confirmed,
    // arrived more than a fortnight ago) does not stop it. → { ok, revoked }
    app.delete('/api/v1/forwarders/:id(\\d+)', route('DELETE /forwarders/:id', async (req, res) => {
        refuseReadOnly(req);
        const id = Number(req.params.id);
        const out = await withConnection(conn => inTransaction(conn, async () => {
            const before = await loadForwarder(conn, id);
            // A booking still asked for, or confirmed and not yet finished (its
            // sailing arrived more than a fortnight ago: F.bookingFinished).
            const [[live]] = await conn.query(
                `SELECT COUNT(*) AS n
                   FROM freight_bookings b
                   JOIN freight_quote_requests r ON r.id = b.request_id
                   LEFT JOIN freight_quote_options o ON o.id = b.option_id
                  WHERE r.forwarder_id = ?
                    AND (b.status = 'requested'
                         OR (b.status = 'confirmed'
                             AND (o.id IS NULL OR DATE_ADD(o.sailing_date, INTERVAL (o.transit_days + ?) DAY) >= ?)))`,
                [id, F.FINISHED_AFTER_DAYS, londonToday()]
            );
            if (Number(live.n) > 0) {
                throw new Fail(409, 'HAS_BOOKING', `${before.name} has a booking in progress. Cancel it before removing the forwarder.`);
            }
            await conn.query('UPDATE forwarders SET deleted_at = UTC_TIMESTAMP() WHERE id = ?', [id]);
            await conn.query('UPDATE forwarder_contacts SET deleted_at = UTC_TIMESTAMP() WHERE forwarder_id = ? AND deleted_at IS NULL', [id]);
            const [upd] = await conn.query(
                `UPDATE freight_quote_requests
                    SET revoked_at = UTC_TIMESTAMP(), revoke_reason = 'forwarder_removed', row_version = row_version + 1
                  WHERE forwarder_id = ? AND revoked_at IS NULL`,
                [id]
            );
            await recordAudit(conn, {
                entityType: 'forwarder', entityId: id, action: 'delete',
                before: { name: before.name }, after: { revokedRequests: upd.affectedRows || 0 }, userEmail: req.userEmail,
            });
            return { ok: true, revoked: upd.affectedRows || 0 };
        }));
        res.json(out);
    }));

    // ── delivery options ────────────────────────────────────────────────────
    // The list a request's delivery options are ticked from: free text, kept by
    // staff. A request stores the text of the ones chosen, so rewording or
    // removing an entry here never changes a request already sent.

    async function loadDeliveryOptions(conn) {
        const [rows] = await conn.query(
            'SELECT id, label FROM freight_delivery_options WHERE deleted_at IS NULL ORDER BY id ASC'
        );
        return rows.map(r => ({ id: Number(r.id), label: r.label }));
    }

    // The same text twice (in any case) would be two boxes nobody can tell apart.
    function refuseDuplicate(list, label, exceptId = null) {
        const key = label.toLowerCase();
        if (list.some(o => o.id !== exceptId && o.label.toLowerCase() === key)) {
            throw new Fail(409, 'DUPLICATE', `"${label}" is already in the list.`, { field: 'label' });
        }
    }

    // ── GET /api/v1/freight-delivery-options ────────────────────────────────
    // { deliveryOptions: [{ id, label }] }, in the order they were added.
    app.get('/api/v1/freight-delivery-options', route('GET /freight-delivery-options', async (req, res) => {
        const deliveryOptions = await withConnection(async (conn) => {
            try {
                return await loadDeliveryOptions(conn);
            } catch (e) {
                if (e.errno !== TABLE_MISSING) throw e;
                return [];
            }
        });
        res.json({ deliveryOptions });
    }));

    // ── POST /api/v1/freight-delivery-options ───────────────────────────────
    // Body { label } → { deliveryOptions } (the whole list again).
    app.post('/api/v1/freight-delivery-options', route('POST /freight-delivery-options', async (req, res) => {
        refuseReadOnly(req);
        const v = F.validateDeliveryOption(req.body);
        if (v.code) throw invalid(v);
        const deliveryOptions = await withConnection(conn => inTransaction(conn, async () => {
            const list = await loadDeliveryOptions(conn);
            if (list.length >= F.MAX_DELIVERY_OPTIONS) {
                throw new Fail(409, 'LIST_FULL', `The list holds at most ${F.MAX_DELIVERY_OPTIONS} delivery options. Remove one first.`);
            }
            refuseDuplicate(list, v.value.label);
            const [ins] = await conn.query(
                'INSERT INTO freight_delivery_options (label, created_by_email) VALUES (?, ?)',
                [v.value.label, req.userEmail || null]
            );
            await recordAudit(conn, {
                entityType: 'freight_delivery_option', entityId: Number(ins.insertId), action: 'create',
                before: null, after: v.value, userEmail: req.userEmail,
            });
            return loadDeliveryOptions(conn);
        }));
        res.status(201).json({ deliveryOptions });
    }));

    // ── PATCH /api/v1/freight-delivery-options/:id ──────────────────────────
    // Body { label } → { deliveryOptions }. Requests already sent keep the old text.
    app.patch('/api/v1/freight-delivery-options/:id(\\d+)', route('PATCH /freight-delivery-options/:id', async (req, res) => {
        refuseReadOnly(req);
        const id = Number(req.params.id);
        const v = F.validateDeliveryOption(req.body);
        if (v.code) throw invalid(v);
        const deliveryOptions = await withConnection(conn => inTransaction(conn, async () => {
            const list = await loadDeliveryOptions(conn);
            const before = list.find(o => o.id === id);
            if (!before) throw new Fail(404, 'NOT_FOUND', `Delivery option ${id} not found.`);
            refuseDuplicate(list, v.value.label, id);
            if (before.label !== v.value.label) {
                await conn.query('UPDATE freight_delivery_options SET label = ? WHERE id = ?', [v.value.label, id]);
                await recordAudit(conn, {
                    entityType: 'freight_delivery_option', entityId: id, action: 'update',
                    before: { label: before.label }, after: v.value, userEmail: req.userEmail,
                });
            }
            return loadDeliveryOptions(conn);
        }));
        res.json({ deliveryOptions });
    }));

    // ── DELETE /api/v1/freight-delivery-options/:id ─────────────────────────
    // Soft. Requests already sent keep the text. Removing one twice is a no-op.
    // → { deliveryOptions }
    app.delete('/api/v1/freight-delivery-options/:id(\\d+)', route('DELETE /freight-delivery-options/:id', async (req, res) => {
        refuseReadOnly(req);
        const id = Number(req.params.id);
        const deliveryOptions = await withConnection(conn => inTransaction(conn, async () => {
            const before = (await loadDeliveryOptions(conn)).find(o => o.id === id);
            if (before) {
                await conn.query('UPDATE freight_delivery_options SET deleted_at = UTC_TIMESTAMP() WHERE id = ?', [id]);
                await recordAudit(conn, {
                    entityType: 'freight_delivery_option', entityId: id, action: 'delete',
                    before: { label: before.label }, after: null, userEmail: req.userEmail,
                });
            }
            return loadDeliveryOptions(conn);
        }));
        res.json({ deliveryOptions });
    }));

    // ── reading requests, options and bookings ──────────────────────────────

    // When a booking is confirmed, the other forwarders' links from the same
    // send are closed as `filled`, so nobody quotes for a container that is
    // booked. Not earlier: until then a decline must leave the next forwarder
    // bookable. The forwarder's own confirmation is written by the portal,
    // whose login cannot revoke, so this runs before the staff reads below (and
    // after a staff confirmation). Never fails a read.
    async function syncFilled(conn) {
        try {
            const [rows] = await conn.query(
                `SELECT r.id
                   FROM freight_quote_requests r
                  WHERE r.revoked_at IS NULL
                    AND EXISTS (SELECT 1
                                  FROM freight_bookings b
                                  JOIN freight_quote_requests br ON br.id = b.request_id
                                 WHERE b.status = 'confirmed' AND br.batch_id = r.batch_id AND br.id <> r.id)
                    AND NOT EXISTS (SELECT 1 FROM freight_bookings lb
                                     WHERE lb.request_id = r.id AND lb.status IN (?))`,
                [LIVE]
            );
            if (!rows.length) return 0;
            const [upd] = await conn.query(
                `UPDATE freight_quote_requests
                    SET revoked_at = UTC_TIMESTAMP(), revoke_reason = 'filled', row_version = row_version + 1
                  WHERE id IN (?) AND revoked_at IS NULL`,
                [rows.map(r => r.id)]
            );
            return upd.affectedRows || 0;
        } catch (e) {
            if (e.errno !== TABLE_MISSING) log.warn('[freight-quotes] closing filled requests failed', { error: e.message, errno: e.errno });
            return 0;
        }
    }

    // A confirmed booking was cancelled: the links that were closed as `filled`
    // because of it open again (unless another booking of the send is confirmed).
    async function reopenFilled(conn, batchId) {
        const [[still]] = await conn.query(
            `SELECT COUNT(*) AS n
               FROM freight_bookings b
               JOIN freight_quote_requests r ON r.id = b.request_id
              WHERE r.batch_id = ? AND b.status = 'confirmed'`,
            [batchId]
        );
        if (Number(still.n) > 0) return 0;
        const [upd] = await conn.query(
            `UPDATE freight_quote_requests
                SET revoked_at = NULL, revoke_reason = NULL, row_version = row_version + 1,
                    expires_at = GREATEST(expires_at, UTC_TIMESTAMP() + INTERVAL ? DAY)
              WHERE batch_id = ? AND revoke_reason = 'filled'`,
            [BOOKING_EXTENDS_DAYS, batchId]
        );
        return upd.affectedRows || 0;
    }

    // The lines of the container attached to a booking, read as the forwarder
    // quote PDF reads them. → { ship, source, lines } or throws Fail.
    async function loadContainerLines(conn, shipmentId) {
        const [[ship]] = await conn.query(
            'SELECT id, name, reference, stage, deleted_at, merged_into_id FROM shipments WHERE id = ?',
            [shipmentId]
        );
        if (!ship || ship.deleted_at) throw new Fail(409, 'NO_CONTAINER', 'The attached container no longer exists. Attach another.');
        if (ship.merged_into_id) throw new Fail(409, 'NO_CONTAINER', 'The attached container was merged into another. Attach that one.');
        const source = S.documentSource(ship);
        const { lines } = await loadForwarderQuoteForPdf(conn, source.kind === 'booked' ? { reference: source.reference } : source.name);
        return { ship, source, lines };
    }

    function bookingJson(b, { option, request, shipment, drift, finished }) {
        return {
            id: Number(b.id),
            requestId: Number(b.request_id),
            optionId: Number(b.option_id),
            status: b.status,
            // Confirmed, and due to arrive more than a fortnight ago: history.
            finished: Boolean(finished),
            forwarderId: request ? Number(request.forwarder_id) : null,
            forwarderName: request ? request.forwarder_name || null : null,
            option: option ? optionJson(option) : null,
            requestedByEmail: b.requested_by_email || null,
            requestedAt: iso(b.requested_at),
            requestNote: b.request_note || null,
            sentTo: F.jsonArray(b.sent_to).map(String),
            shipmentId: b.shipment_id != null ? Number(b.shipment_id) : null,
            containerName: shipment ? (shipment.name || shipment.reference || null) : (b.draft_container_name || null),
            containerStage: shipment ? shipment.stage : null,
            bookingRef: b.booking_ref || null,
            responseNote: b.response_note || null,
            respondedAt: iso(b.responded_at),
            respondedBy: b.responded_by_email || b.responded_by_name || null,
            responseSource: b.response_source || null,
            cancelledAt: iso(b.cancelled_at),
            cancelReason: b.cancel_reason || null,
            contentsSentAt: iso(b.contents_sent_at),
            contentsSentTo: F.jsonArray(b.contents_sent_to).map(String),
            // null = nothing sent yet, or the container cannot be read.
            contentsChanged: drift ? drift.stale : null,
            rowVersion: Number(b.row_version),
        };
    }

    function requestJson(row, { options, movements, liveBooking, bookingFinished = false, lastBooking = null, portalBase }) {
        const state = F.requestState(row, Boolean(liveBooking), bookingFinished);
        const facts = requestFactsOf(row);
        return {
            id: Number(row.id),
            batchId: row.batch_id,
            forwarderId: Number(row.forwarder_id),
            forwarderName: row.forwarder_name || null,
            state,
            revokeReason: row.revoke_reason || null,
            // A booking on it is requested or confirmed (finished or not): no
            // other option of it can be booked, whatever `state` says.
            hasLiveBooking: Boolean(liveBooking),
            // The newest booking's status: `declined` tells a request the
            // forwarder turned down apart from one never booked.
            lastBookingStatus: lastBooking ? lastBooking.status : null,
            ...facts,
            // In the order added, each with the forwarder's price once given.
            stockMovements: movements.map(movementJson),
            sentTo: F.jsonArray(row.sent_to).map(String),
            subject: row.subject || null,
            createdAt: iso(row.created_at),
            // null = Front never confirmed the email (the call was cut short).
            sentAt: iso(row.sent_at),
            sentByEmail: row.created_by_email || null,
            // Reminders sent by staff (POST …/remind): the last one, and how many.
            remindedAt: iso(row.reminded_at),
            reminderCount: Number(row.reminder_count) || 0,
            expiresAt: iso(row.expires_at),
            revokedAt: iso(row.revoked_at),
            answerCount: Number(row.answer_count),
            answeredAt: iso(row.answered_at),
            answeredBy: row.answered_by_email || row.answered_by_name || null,
            answerSource: row.answer_source || null,
            answerNote: row.answer_note || null,
            declined: Number(row.declined) === 1,
            rowVersion: Number(row.row_version),
            // The link is the permission: only while the page still does something.
            url: state !== 'revoked' && state !== 'expired' && portalBase ? F.freightUrl(portalBase, row.token) : null,
            options: options.map(optionJson),
        };
    }

    // The stock movements of some requests that staff have not removed, oldest
    // first. A server where 2026-11-17_20 has not run has none.
    async function loadMovements(conn, requestIds) {
        if (!requestIds.length) return [];
        try {
            const [rows] = await conn.query(
                'SELECT * FROM freight_stock_movements WHERE request_id IN (?) AND deleted_at IS NULL ORDER BY request_id ASC, id ASC',
                [requestIds]
            );
            return rows;
        } catch (e) {
            if (e.errno !== TABLE_MISSING) throw e;
            return [];
        }
    }

    // Requests (newest first) with their current options, their stock
    // movements and every booking. `where` filters freight_quote_requests r.
    async function readBoard(conn, where, params, { withDrift = true } = {}) {
        const portalBase = poPortalEmail.portalBaseOf();
        const [rows] = await conn.query(
            `SELECT ${REQUEST_COLUMNS}
               FROM freight_quote_requests r
              WHERE ${where}
              ORDER BY r.created_at DESC, r.id DESC
              LIMIT ${BOARD_LIMIT}`,
            params
        );
        if (!rows.length) return { enabled: portalBase !== '', requests: [], bookings: [] };
        const ids = rows.map(r => r.id);
        const [bookings] = await conn.query('SELECT * FROM freight_bookings WHERE request_id IN (?) ORDER BY id ASC', [ids]);
        // The current options of each request, and the option of each booking
        // (which may belong to an older answer).
        const bookedIds = [...new Set(bookings.map(b => Number(b.option_id)))];
        const [options] = await conn.query(
            `SELECT o.*, (o.answer_no = r.answer_count) AS is_current
               FROM freight_quote_options o
               JOIN freight_quote_requests r ON r.id = o.request_id
              WHERE o.request_id IN (?) AND (o.answer_no = r.answer_count ${bookedIds.length ? 'OR o.id IN (?)' : ''})
              ORDER BY o.request_id ASC, o.line_no ASC, o.id ASC`,
            bookedIds.length ? [ids, bookedIds] : [ids]
        );
        const movements = await loadMovements(conn, ids);
        const shipmentIds = [...new Set(bookings.map(b => b.shipment_id).filter(v => v != null).map(Number))];
        const [shipments] = shipmentIds.length
            ? await conn.query('SELECT id, name, reference, stage FROM shipments WHERE id IN (?) AND deleted_at IS NULL', [shipmentIds])
            : [[]];

        const rowById = new Map(rows.map(r => [Number(r.id), r]));
        const optionById = new Map(options.map(o => [Number(o.id), o]));
        const shipmentById = new Map(shipments.map(s => [Number(s.id), s]));
        const liveByRequest = new Map(bookings.filter(isLive).map(b => [Number(b.request_id), b]));
        // The rows come oldest first, so the last one set is the newest.
        const lastByRequest = new Map(bookings.map(b => [Number(b.request_id), b]));
        const today = londonToday();
        const finishedIds = new Set(bookings
            .filter(b => F.bookingFinished(b, optionById.get(Number(b.option_id)), today))
            .map(b => Number(b.id)));

        const bookingsOut = [];
        for (const b of bookings) {
            let drift = null;
            // A finished booking's container has long arrived: nothing to compare.
            if (withDrift && isLive(b) && !finishedIds.has(Number(b.id)) && b.contents_json && b.shipment_id != null) {
                try {
                    const { lines } = await loadContainerLines(conn, Number(b.shipment_id));
                    drift = F.contentsDrift(b.contents_json, lines);
                } catch (e) {
                    if (!(e instanceof Fail) && !(e instanceof S.ShipmentError)) {
                        log.warn('[freight-quotes] contents could not be compared', { bookingId: b.id, error: e.message });
                    }
                }
            }
            bookingsOut.push(bookingJson(b, {
                option: optionById.get(Number(b.option_id)) || null,
                request: rowById.get(Number(b.request_id)) || null,
                shipment: b.shipment_id != null ? shipmentById.get(Number(b.shipment_id)) || null : null,
                drift,
                finished: finishedIds.has(Number(b.id)),
            }));
        }
        return {
            enabled: portalBase !== '',
            requests: rows.map((r) => {
                const liveBooking = liveByRequest.get(Number(r.id)) || null;
                return requestJson(r, {
                    options: options.filter(o => Number(o.request_id) === Number(r.id) && Number(o.is_current) === 1),
                    movements: movements.filter(m => Number(m.request_id) === Number(r.id)),
                    liveBooking,
                    bookingFinished: Boolean(liveBooking) && finishedIds.has(Number(liveBooking.id)),
                    lastBooking: lastByRequest.get(Number(r.id)) || null,
                    portalBase,
                });
            }),
            // Newest first, as the requests are.
            bookings: bookingsOut.reverse(),
        };
    }

    // The default board: everything whose range has not long passed.
    function boardFilter(req) {
        const { from, to } = req.query || {};
        const where = [];
        const params = [];
        if (from !== undefined && from !== '') {
            if (!F.isYmd(String(from))) throw new Fail(400, 'BAD_FIELD', 'from must be a date (YYYY-MM-DD).', { field: 'from' });
            where.push('r.window_end_date >= ?');
            params.push(String(from));
        } else {
            where.push('r.window_end_date >= ?');
            params.push(F.addDays(londonToday(), -BOARD_DAYS_BACK));
        }
        if (to !== undefined && to !== '') {
            if (!F.isYmd(String(to))) throw new Fail(400, 'BAD_FIELD', 'to must be a date (YYYY-MM-DD).', { field: 'to' });
            where.push('r.window_start_date <= ?');
            params.push(String(to));
        }
        return { where: where.join(' AND '), params };
    }

    const defaultBoard = (conn) => readBoard(conn, 'r.window_end_date >= ?', [F.addDays(londonToday(), -BOARD_DAYS_BACK)]);

    // ── GET /api/v1/freight-quotes?from=&to= ────────────────────────────────
    // { enabled, requests: [{ …, state, options: [...] }], bookings: [...] }
    // `enabled` is false when the portal address is not configured or the
    // tables are not on this server: the page then says so instead of sending.
    app.get('/api/v1/freight-quotes', route('GET /freight-quotes', async (req, res) => {
        const filter = boardFilter(req);
        const board = await withConnection(async (conn) => {
            try {
                await syncFilled(conn);
                return await readBoard(conn, filter.where, filter.params);
            } catch (e) {
                if (e.errno !== TABLE_MISSING) throw e;
                return { enabled: false, requests: [], bookings: [] };
            }
        });
        res.json(board);
    }));

    // ── GET /api/v1/freight-quotes/summary ──────────────────────────────────
    // { answered, contentsDue, total }: what waits on staff (the FREIGHT tab's
    // badge shows `total`).
    //   answered     sends with a quote in and nothing booked yet. A send counts
    //                once however many forwarders answered; a forwarder that
    //                cannot quote, a closed or expired link and a removed
    //                forwarder do not count. A booking the forwarder declined
    //                puts its send back here.
    //   contentsDue  bookings the forwarder confirmed whose contents have not
    //                been emailed yet (a finished booking does not count).
    // Bookings whose contents changed since they were sent are not counted
    // here: that needs every container's lines read, which the board does.
    app.get('/api/v1/freight-quotes/summary', route('GET /freight-quotes/summary', async (req, res) => {
        const counts = await withConnection(async (conn) => {
            try {
                await syncFilled(conn);
                const [[due]] = await conn.query(
                    `SELECT COUNT(*) AS n
                       FROM freight_bookings b
                       JOIN freight_quote_requests r ON r.id = b.request_id
                       JOIN freight_quote_options o ON o.id = b.option_id
                      WHERE b.status = 'confirmed' AND b.contents_sent_at IS NULL AND r.revoked_at IS NULL
                        AND DATE_ADD(o.sailing_date, INTERVAL (o.transit_days + ?) DAY) >= ?`,
                    [F.FINISHED_AFTER_DAYS, londonToday()]
                );
                const [[row]] = await conn.query(
                    `SELECT COUNT(DISTINCT r.batch_id) AS n
                       FROM freight_quote_requests r
                       JOIN forwarders f ON f.id = r.forwarder_id AND f.deleted_at IS NULL
                      WHERE r.revoked_at IS NULL AND r.expires_at > UTC_TIMESTAMP()
                        AND r.answered_at IS NOT NULL AND r.declined = 0
                        AND NOT EXISTS (SELECT 1
                                          FROM freight_bookings b
                                          JOIN freight_quote_requests br ON br.id = b.request_id
                                         WHERE br.batch_id = r.batch_id AND b.status IN (?))`,
                    [LIVE]
                );
                return { answered: Number(row.n) || 0, contentsDue: Number(due.n) || 0 };
            } catch (e) {
                if (e.errno !== TABLE_MISSING) throw e;
                return { answered: 0, contentsDue: 0 };
            }
        });
        res.json({ ...counts, total: counts.answered + counts.contentsDue });
    }));

    // ── POST /api/v1/freight-quotes ─────────────────────────────────────────
    // Body { forwarderIds, batchId?, windowStart, windowEnd, origin?, destination?,
    //        containerSize?, services, stockMovements?, note?, expiryDays?,
    //        subject?, body? }. `services` are the delivery options asked about,
    //        as text; `stockMovements` [{ from, cbm, weightKg }], each priced by
    //        the forwarder on its own line; `subject` and `body` the email as
    //        staff edited it in the dialog ({tokens} still filled in per
    //        forwarder), the template's when left out.
    // One request row, one link and one email per forwarder, all sharing
    // `batchId` (the send dialog makes it once). SAFE TO RETRY with the same
    // batchId: a forwarder that already has a row for it is reported as it
    // stands and never emailed again. A Front failure revokes that forwarder's
    // request (`send_failed`), so no dead link is left open; the others still
    // go. That row reached nobody, so the same batchId sent again TRIES IT
    // AGAIN, as the dialog now has it, instead of reporting the failure for
    // ever. A batchId that already exists may also carry a forwarder not asked
    // before: ShipLine's "Ask another forwarder" adds one to a send that way.
    // → { batchId, results: [{ forwarderId, forwarderName, outcome, requestId?, error? }], …board }
    //   outcome: sent | already_sent | send_unconfirmed | failed | no_contacts
    app.post('/api/v1/freight-quotes', route('POST /freight-quotes', async (req, res) => {
        refuseReadOnly(req);
        const v = F.validateRequest(req.body, londonToday());
        if (v.code) throw invalid(v);
        const want = v.value;
        const { subject, body } = req.body || {};
        const portalBase = poPortalEmail.portalBaseOf();
        if (!portalBase) {
            throw new Fail(409, 'PORTAL_URL_UNSET', 'The supplier portal address (SUPPLIER_PORTAL_URL) is not configured, so the quote link cannot be sent.');
        }
        const batchId = want.batchId || crypto.randomUUID();

        const out = await withConnection(async (conn) => {
            const forwarders = await loadForwarders(conn, want.forwarderIds);
            const missing = want.forwarderIds.filter(id => !forwarders.some(f => f.id === id));
            if (missing.length) throw new Fail(400, 'BAD_FIELD', `Forwarder ${missing.join(', ')} not found.`, { field: 'forwarderIds' });

            const tpl = await getEmailTemplate(conn, FREIGHT_QUOTE_REQUEST_TEMPLATE_KEY);
            const expiresOn = F.addDays(new Date().toISOString().slice(0, 10), want.expiryDays);
            const results = [];

            for (const fw of forwarders) {
                const base = { forwarderId: fw.id, forwarderName: fw.name };
                const [[existing]] = await conn.query(
                    'SELECT id, token, sent_at, revoked_at, revoke_reason FROM freight_quote_requests WHERE batch_id = ? AND forwarder_id = ?',
                    [batchId, fw.id]
                );
                // Front refused the first attempt, so nobody was emailed: go again.
                const retrying = Boolean(existing) && existing.revoke_reason === 'send_failed';
                if (existing && !retrying) {
                    results.push({
                        ...base, requestId: Number(existing.id),
                        outcome: existing.revoked_at ? 'failed' : (existing.sent_at ? 'already_sent' : 'send_unconfirmed'),
                    });
                    continue;
                }
                const addresses = fw.contacts.map(c => c.email);
                if (!addresses.length) { results.push({ ...base, outcome: 'no_contacts' }); continue; }

                // So a contact can register on the portal with no approval to
                // wait for. The email goes whether or not this works.
                try {
                    await preapprove(conn, { emails: addresses, actorEmail: req.userEmail, note: `Freight forwarder contact: ${fw.name}` });
                } catch (e) {
                    log.warn('[freight-quotes] portal pre-approval failed', { forwarderId: fw.id, error: e.message, errno: e.errno });
                }

                const vars = {
                    forwarder: fw.name, windowStart: want.windowStart, windowEnd: want.windowEnd,
                    origin: want.origin, destination: want.destination, containerSize: want.containerSize,
                    services: want.services.map(F.serviceName).join(', '),
                };
                const finalSubject = ((typeof subject === 'string' && subject.trim())
                    ? renderTemplate(subject.trim(), vars)
                    : (renderTemplate(tpl.subject, vars) || `Freight quote request – ${want.windowStart} to ${want.windowEnd}`))
                    .replace(/[\r\n]+/g, ' ').slice(0, 500);

                const token = retrying ? existing.token : F.newToken();
                let requestId;
                if (retrying) {
                    // The same row, as the dialog has it now, open again. Guarded
                    // on the reason, so two retries at once cannot both send.
                    requestId = Number(existing.id);
                    const [reopened] = await conn.query(
                        `UPDATE freight_quote_requests
                            SET forwarder_name = ?, window_start_date = ?, window_end_date = ?, origin = ?, destination = ?,
                                container_size = ?, services = ?, staff_note = ?, sent_to = ?, subject = ?,
                                created_by_email = ?, sent_at = NULL, revoked_at = NULL, revoke_reason = NULL,
                                expires_at = UTC_TIMESTAMP() + INTERVAL ? DAY, row_version = row_version + 1
                          WHERE id = ? AND revoke_reason = 'send_failed'`,
                        [
                            fw.name, want.windowStart, want.windowEnd, want.origin, want.destination,
                            want.containerSize, JSON.stringify(want.services), want.note, JSON.stringify(addresses),
                            finalSubject, req.userEmail || null, want.expiryDays, requestId,
                        ]
                    );
                    if (reopened.affectedRows !== 1) { results.push({ ...base, requestId, outcome: 'send_unconfirmed' }); continue; }
                    // The movements of the attempt that failed give way to the ones listed now.
                    await conn.query('DELETE FROM freight_stock_movements WHERE request_id = ? AND add_key = ?', [requestId, batchId]);
                    await insertMovements(conn, requestId, batchId, want.stockMovements, req.userEmail);
                } else {
                    try {
                        const [ins] = await conn.query(
                            `INSERT INTO freight_quote_requests
                                (token, batch_id, forwarder_id, forwarder_name, window_start_date, window_end_date, origin,
                                 destination, container_size, services, staff_note, sent_to, subject,
                                 created_by_email, expires_at)
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP() + INTERVAL ? DAY)`,
                            [
                                token, batchId, fw.id, fw.name, want.windowStart, want.windowEnd, want.origin,
                                want.destination, want.containerSize, JSON.stringify(want.services), want.note,
                                JSON.stringify(addresses), finalSubject, req.userEmail || null, want.expiryDays,
                            ]
                        );
                        requestId = Number(ins.insertId);
                        // Each forwarder prices the movements itself, so each request
                        // has its own rows. The send's batch id is their add key.
                        await insertMovements(conn, requestId, batchId, want.stockMovements, req.userEmail);
                    } catch (e) {
                        // The same send, running twice at once: the other call has this forwarder.
                        if (e.errno !== DUPLICATE) throw e;
                        results.push({ ...base, outcome: 'send_unconfirmed' });
                        continue;
                    }
                }

                const escaped = Object.fromEntries(Object.entries(vars).map(([k, val]) => [k, escapeHtml(val)]));
                const inner = renderTemplate((typeof body === 'string' && body.trim()) ? body : tpl.bodyHtml, escaped);
                const html = wrapEmail({
                    title: finalSubject,
                    bodyHtml: inner + F.quoteBlockHtml({
                        request: { ...want, note: want.note },
                        url: F.freightUrl(portalBase, token),
                        expiresOn,
                        registerUrl: poPortalEmail.registerUrl(portalBase),
                    }),
                });

                const sent = await sendFrontMessage({ to: addresses, subject: finalSubject, html });
                if (!sent.ok) {
                    await conn.query(
                        `UPDATE freight_quote_requests
                            SET revoked_at = UTC_TIMESTAMP(), revoke_reason = 'send_failed', row_version = row_version + 1
                          WHERE id = ?`,
                        [requestId]
                    );
                    results.push({
                        ...base, requestId, outcome: 'failed',
                        error: sent.reason === 'front_not_configured'
                            ? 'Front is not configured (FRONT_API_TOKEN, FRONT_CHANNEL_ID).'
                            : 'Front rejected the message.',
                        frontStatus: sent.status ?? null, frontBody: sent.detail ?? null,
                    });
                    continue;
                }
                await conn.query(
                    `UPDATE freight_quote_requests
                        SET sent_at = UTC_TIMESTAMP(), front_message_uid = ?, front_conversation_id = ?
                      WHERE id = ?`,
                    [sent.frontMessageUid, sent.frontConversationId, requestId]
                );
                // On ShipLine's Email Receipts page as a portal email: the link
                // is the one thing asked, and an answer confirms it
                // (src/lib/email-receipt.js).
                await logSentEmail(conn, {
                    emailType: 'freight_quote_request', sentTo: addresses, subject: finalSubject,
                    confirmVia: 'portal', sendTable: 'freight_quote_requests', sendId: requestId,
                });
                // Never the token: whoever reads the audit log must not get the link.
                await recordAudit(conn, {
                    entityType: 'freight_quote_request', entityId: requestId, action: 'requested',
                    before: null,
                    after: {
                        batchId, forwarderId: fw.id, forwarder: fw.name, to: addresses, subject: finalSubject,
                        windowStart: want.windowStart, windowEnd: want.windowEnd, origin: want.origin,
                        destination: want.destination, containerSize: want.containerSize, services: want.services,
                        stockMovements: want.stockMovements, expiresOn, frontMessageUid: sent.frontMessageUid, frontConversationId: sent.frontConversationId,
                    },
                    userEmail: req.userEmail,
                });
                results.push({ ...base, requestId, outcome: 'sent' });
            }
            return { batchId, results, ...(await defaultBoard(conn)) };
        });
        res.json(out);
    }));

    // Stock movement rows for one request, unpriced. `addKey` names the call
    // that adds them: with the line number it is unique per request, so the
    // same call made twice can never add twice.
    async function insertMovements(conn, requestId, addKey, movements, userEmail) {
        if (!movements.length) return;
        await conn.query(
            `INSERT INTO freight_stock_movements
                (request_id, add_key, line_no, factory, cbm, weight_kg, created_by_email)
             VALUES ${movements.map(() => '(?, ?, ?, ?, ?, ?, ?)').join(', ')}`,
            movements.flatMap((m, i) => [requestId, addKey, i + 1, m.from, m.cbm, m.weightKg, userEmail || null])
        );
    }

    async function lockRequest(conn, id) {
        const [[row]] = await conn.query(
            `SELECT ${REQUEST_COLUMNS} FROM freight_quote_requests r WHERE r.id = ? FOR UPDATE`,
            [id]
        );
        if (!row) throw new Fail(404, 'NOT_FOUND', `Quote request ${id} not found.`);
        return row;
    }

    async function liveBookingOf(conn, requestId) {
        const [[row]] = await conn.query(
            'SELECT * FROM freight_bookings WHERE request_id = ? AND status IN (?) LIMIT 1',
            [requestId, LIVE]
        );
        return row || null;
    }

    // Whether a live booking is finished (F.bookingFinished), read with its option.
    async function isFinished(conn, booking) {
        if (booking.status !== 'confirmed') return false;
        const [[option]] = await conn.query('SELECT * FROM freight_quote_options WHERE id = ?', [booking.option_id]);
        return F.bookingFinished(booking, option || null, londonToday());
    }

    // ── POST /api/v1/freight-quotes/:id/revoke ──────────────────────────────
    // Closes a link. The options already given stay readable in ShipLine.
    // Refused while a booking on it is in progress (the forwarder still needs
    // the page to confirm): cancel the booking first. A finished booking does
    // not stop it. Revoking twice is a no-op. → the board.
    app.post('/api/v1/freight-quotes/:id(\\d+)/revoke', route('POST /freight-quotes/:id/revoke', async (req, res) => {
        refuseReadOnly(req);
        const id = Number(req.params.id);
        const board = await withConnection(async (conn) => {
            await inTransaction(conn, async () => {
                const row = await lockRequest(conn, id);
                // A finished booking (arrived more than a fortnight ago) needs the page no longer.
                const held = await liveBookingOf(conn, id);
                if (held && !(await isFinished(conn, held))) {
                    throw new Fail(409, 'HAS_BOOKING', 'This request has a booking in progress. Cancel the booking before closing the link.');
                }
                if (row.revoked_at) return;
                await conn.query(
                    `UPDATE freight_quote_requests
                        SET revoked_at = UTC_TIMESTAMP(), revoke_reason = 'staff', row_version = row_version + 1
                      WHERE id = ?`,
                    [id]
                );
                await recordAudit(conn, {
                    entityType: 'freight_quote_request', entityId: id, action: 'revoked',
                    before: null, after: { reason: 'staff' }, userEmail: req.userEmail,
                });
            });
            return defaultBoard(conn);
        });
        res.json(board);
    }));

    // ── POST /api/v1/freight-quotes/:id/remind ──────────────────────────────
    // Body { reminderCount, to?, subject?, body? }. Emails the forwarder the
    // SAME link again, by hand (nothing here is automatic). What it asks for is
    // what is still waiting:
    //   kind 'quote'      no answer yet: the quote request's template and block;
    //   kind 'movements'  answered, but stock movements have no price: the price
    //                     request's template, with the movements still unpriced.
    // 409 NOTHING_WAITING when neither is true; 409 REVOKED on a closed link.
    // An expired link is opened again: the link is kept open at least another
    // REMINDER_EXTENDS_DAYS. A request whose first email Front never confirmed
    // (`sent_at` NULL) counts as sent once a reminder has gone.
    // `reminderCount` is the request's as last read: the call claims it before
    // sending, so a retry of a call that did send is refused (409 STALE)
    // instead of sending twice. A Front failure gives the claim back.
    // The request's row_version is NOT bumped: the forwarder's open form must
    // not be refused because of a reminder.
    // On the Email Receipts page the reminder is listed under the email it
    // repeats (email_receipt_reminders); with no such row, as an email of its own.
    // → { ok, kind, …board }
    app.post('/api/v1/freight-quotes/:id(\\d+)/remind', route('POST /freight-quotes/:id/remind', async (req, res) => {
        refuseReadOnly(req);
        const id = Number(req.params.id);
        const { reminderCount, to, subject, body } = req.body || {};
        if (!Number.isSafeInteger(reminderCount) || reminderCount < 0) {
            throw new Fail(400, 'BAD_FIELD', 'reminderCount must be the request\'s reminderCount.', { field: 'reminderCount' });
        }
        const portalBase = poPortalEmail.portalBaseOf();
        if (!portalBase) {
            throw new Fail(409, 'PORTAL_URL_UNSET', 'The supplier portal address (SUPPLIER_PORTAL_URL) is not configured, so the link cannot be sent.');
        }

        const out = await withConnection(async (conn) => {
            const made = await inTransaction(conn, async () => {
                const request = await lockRequest(conn, id);
                if (request.revoked_at) throw new Fail(409, 'REVOKED', 'This request was closed, so there is no link to remind the forwarder of.');
                const movements = (await loadMovements(conn, [id]))
                    .map(m => ({ from: m.factory, cbm: Number(m.cbm), weightKg: Number(m.weight_kg), priced: m.price !== null && m.price !== undefined }));
                const unpriced = movements.filter(m => !m.priced);
                const kind = !request.answered_at ? 'quote' : (unpriced.length ? 'movements' : null);
                if (!kind) {
                    throw new Fail(409, 'NOTHING_WAITING', 'This forwarder has answered and priced everything asked, so there is nothing to remind them of.');
                }
                if ((Number(request.reminder_count) || 0) !== reminderCount) {
                    throw new Fail(409, 'STALE', 'A reminder has gone since this was shown. Reload and check before sending another.');
                }
                const addresses = await recipientsFor(conn, to, Number(request.forwarder_id));
                // The claim: under the row lock, so only one call per count gets past here.
                await conn.query(
                    `UPDATE freight_quote_requests
                        SET reminder_count = reminder_count + 1,
                            expires_at = GREATEST(expires_at, UTC_TIMESTAMP() + INTERVAL ? DAY)
                      WHERE id = ?`,
                    [F.REMINDER_EXTENDS_DAYS, id]
                );

                const facts = requestFactsOf(request);
                const tpl = await getEmailTemplate(conn, kind === 'quote' ? FREIGHT_QUOTE_REQUEST_TEMPLATE_KEY : FREIGHT_MOVEMENT_REQUEST_TEMPLATE_KEY);
                const vars = {
                    forwarder: request.forwarder_name || '', windowStart: facts.windowStart, windowEnd: facts.windowEnd,
                    origin: facts.origin, destination: facts.destination, containerSize: facts.containerSize,
                    services: facts.services.map(F.serviceName).join(', '), count: String(unpriced.length),
                };
                const fallback = kind === 'quote'
                    ? `Freight quote request – ${facts.windowStart} to ${facts.windowEnd}`
                    : 'Stock movement price request';
                const finalSubject = ((typeof subject === 'string' && subject.trim())
                    ? renderTemplate(subject.trim(), vars)
                    : `Reminder: ${renderTemplate(tpl.subject, vars) || fallback}`)
                    .replace(/[\r\n]+/g, ' ').slice(0, 500);
                const escaped = Object.fromEntries(Object.entries(vars).map(([k, val]) => [k, escapeHtml(val)]));
                const inner = renderTemplate((typeof body === 'string' && body.trim()) ? body : tpl.bodyHtml, escaped);
                // The link lasts at least as long as it did, and at least the reminder's days from now.
                const floor = F.addDays(new Date().toISOString().slice(0, 10), F.REMINDER_EXTENDS_DAYS);
                const was = iso(request.expires_at);
                const expiresOn = was && was.slice(0, 10) > floor ? was.slice(0, 10) : floor;
                const url = F.freightUrl(portalBase, request.token);
                const block = kind === 'quote'
                    ? F.quoteBlockHtml({ request: { ...facts, stockMovements: movements }, url, expiresOn, registerUrl: poPortalEmail.registerUrl(portalBase) })
                    : F.movementBlockHtml({ movements: unpriced, url, expiresOn });
                return { kind, request, addresses, finalSubject, html: wrapEmail({ title: finalSubject, bodyHtml: inner + block }) };
            });

            const sent = await sendFrontMessage({ to: made.addresses, subject: made.finalSubject, html: made.html });
            if (!sent.ok) {
                await conn.query('UPDATE freight_quote_requests SET reminder_count = reminder_count - 1 WHERE id = ? AND reminder_count > 0', [id]);
                throw frontFailure(sent);
            }
            await conn.query(
                'UPDATE freight_quote_requests SET reminded_at = UTC_TIMESTAMP(), sent_at = COALESCE(sent_at, UTC_TIMESTAMP()) WHERE id = ?',
                [id]
            );
            // Under the email it repeats on the Email Receipts page; the email is
            // sent, so a failure here is logged and never fails the call.
            const emailType = made.kind === 'quote' ? 'freight_quote_request' : 'freight_movement_request';
            try {
                const [[receipt]] = await conn.query(
                    `SELECT id, reminder_count FROM email_receipts
                      WHERE send_table = 'freight_quote_requests' AND send_id = ? AND email_type = ?
                      ORDER BY id DESC LIMIT 1`,
                    [id, emailType]
                );
                if (receipt) {
                    await recordReminderSend(conn, receipt.id, {
                        reminderNumber: Number(receipt.reminder_count || 0) + 1, sentTo: made.addresses, subject: made.finalSubject,
                        frontMessageUid: sent.frontMessageUid, frontConversationId: sent.frontConversationId, sentByEmail: req.userEmail || null,
                    });
                } else {
                    await logSentEmail(conn, {
                        emailType, sentTo: made.addresses, subject: made.finalSubject,
                        confirmVia: 'portal', sendTable: 'freight_quote_requests', sendId: id,
                    });
                }
            } catch (e) {
                log.warn('[freight-quotes] a reminder was not recorded on the Email Receipts page', { requestId: id, error: e.message });
            }
            await recordAudit(conn, {
                entityType: 'freight_quote_request', entityId: id, action: 'reminded',
                before: null,
                after: {
                    kind: made.kind, forwarder: made.request.forwarder_name || null, to: made.addresses, subject: made.finalSubject,
                    reminderNumber: reminderCount + 1,
                    frontMessageUid: sent.frontMessageUid, frontConversationId: sent.frontConversationId,
                },
                userEmail: req.userEmail,
            });
            return { ok: true, kind: made.kind, ...(await defaultBoard(conn)) };
        });
        res.json(out);
    }));

    async function insertOptions(conn, requestId, answerNo, options) {
        if (!options.length) return;
        await conn.query(
            `INSERT INTO freight_quote_options
                (request_id, answer_no, line_no, loading_date, sailing_date, transit_days, port_from, port_to,
                 service, container_size, price, currency, carrier, valid_until, note)
             VALUES ${options.map(() => '(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)').join(', ')}`,
            options.flatMap((o, i) => [
                requestId, answerNo, i + 1, o.loadingDate, o.sailingDate, o.transitDays, o.portFrom, o.portTo,
                o.service, o.containerSize, o.price, o.currency, o.carrier, o.validUntil, o.note,
            ])
        );
    }

    // ── POST /api/v1/freight-quotes/:id/options ─────────────────────────────
    // Staff type in the answer of a forwarder who replied by email:
    // { options: [...], declined?, note? }, the same rules as the portal's form.
    // A new answer, replacing the one shown; the earlier rows stay. Allowed on an
    // expired link (the email may come late), never on a closed one or under a
    // live booking. → the board.
    app.post('/api/v1/freight-quotes/:id(\\d+)/options', route('POST /freight-quotes/:id/options', async (req, res) => {
        refuseReadOnly(req);
        const id = Number(req.params.id);
        const board = await withConnection(async (conn) => {
            await inTransaction(conn, async () => {
                const row = await lockRequest(conn, id);
                if (row.revoked_at) throw new Fail(409, 'REVOKED', 'This request was closed.');
                if (await liveBookingOf(conn, id)) {
                    throw new Fail(409, 'HAS_BOOKING', 'This request has a booking in progress, so its options cannot change.');
                }
                const v = F.validateStaffAnswer(req.body, requestFactsOf(row));
                if (v.code) throw invalid(v);
                const answerNo = Number(row.answer_count) + 1;
                await conn.query(
                    `UPDATE freight_quote_requests
                        SET answer_count = ?, answered_at = UTC_TIMESTAMP(), answered_by_name = NULL,
                            answered_by_email = ?, answer_source = 'staff', answer_note = ?, declined = ?,
                            row_version = row_version + 1
                      WHERE id = ?`,
                    [answerNo, req.userEmail || null, v.value.note, v.value.declined ? 1 : 0, id]
                );
                await insertOptions(conn, id, answerNo, v.value.options);
                await recordAudit(conn, {
                    entityType: 'freight_quote_request', entityId: id, action: 'answer_entered',
                    before: { answerCount: Number(row.answer_count) },
                    after: { answerCount: answerNo, options: v.value.options.length, declined: v.value.declined },
                    userEmail: req.userEmail,
                });
            });
            return defaultBoard(conn);
        });
        res.json(board);
    }));

    // ── bookings ────────────────────────────────────────────────────────────

    // A container a booking may carry: a live shipment that documents can be
    // generated for (a DRAFT, or one already booked), not held by another live
    // booking.
    async function attachableShipment(conn, shipmentId, exceptBookingId = null) {
        const [[ship]] = await conn.query(
            'SELECT id, name, reference, stage, deleted_at, merged_into_id FROM shipments WHERE id = ?',
            [shipmentId]
        );
        if (!ship || ship.deleted_at || ship.merged_into_id) {
            throw new Fail(400, 'BAD_FIELD', `Container ${shipmentId} not found.`, { field: 'shipmentId' });
        }
        S.documentSource(ship); // throws a ShipmentError for a PLANNED or CANCELLED one
        const [[taken]] = await conn.query(
            'SELECT id FROM freight_bookings WHERE shipment_id = ? AND status IN (?) AND id <> ? LIMIT 1',
            [shipmentId, LIVE, exceptBookingId || 0]
        );
        if (taken) throw new Fail(409, 'CONTAINER_TAKEN', `${ship.name || ship.reference} is already attached to another booking.`, { bookingId: Number(taken.id) });
        return ship;
    }

    // One live booking per container is a unique key (2026-11-20,
    // uq_freight_bookings_live_shipment): the check above runs under the
    // booking's lock, not the container's, so two attaches at once meet here.
    const isContainerTaken = e => /uq_freight_bookings_live_shipment/.test((e && e.message) || '');
    const containerTaken = () => new Fail(409, 'CONTAINER_TAKEN', 'That container was attached to another booking a moment ago.');

    async function contactAddresses(conn, forwarderId) {
        const [rows] = await conn.query(
            'SELECT email FROM forwarder_contacts WHERE forwarder_id = ? AND deleted_at IS NULL ORDER BY id ASC',
            [forwarderId]
        );
        return rows.map(r => r.email);
    }

    // The addresses an email goes to: the dialog's, or the forwarder's contacts.
    async function recipientsFor(conn, to, forwarderId) {
        if (to !== undefined && to !== null && to !== '') {
            const { addresses, invalid: wrong } = poPortalEmail.parseRecipients(to);
            if (wrong.length) throw new Fail(400, 'BAD_FIELD', `Not a valid email address: ${wrong.map(a => `"${a}"`).join(', ')}`, { field: 'to', invalid: wrong });
            if (addresses.length) return addresses;
        }
        const addresses = await contactAddresses(conn, forwarderId);
        if (!addresses.length) throw new Fail(409, 'NO_CONTACTS', 'This forwarder has no contact to email. Add one on the Forwarders tab.');
        return addresses;
    }

    function frontFailure(sent) {
        if (sent.reason === 'front_not_configured') {
            return new Fail(500, 'FRONT_NOT_CONFIGURED', 'Front is not configured (FRONT_API_TOKEN, FRONT_CHANNEL_ID).');
        }
        return new Fail(502, 'FRONT_ERROR', 'Front rejected the message.', { frontStatus: sent.status ?? null, frontBody: sent.detail ?? null });
    }

    // ── POST /api/v1/freight-quotes/options/:optionId/book ──────────────────
    // Body { note?, shipmentId?, to?, subject?, body? }. Asks the forwarder to
    // book one of its current options: the booking row, then an email with the
    // link to the same page, where they confirm with a booking reference or
    // decline. One live booking per request (409 ALREADY_BOOKED), which also
    // makes a retry harmless. The request's row_version is bumped in the same
    // transaction, so a forwarder saving a new answer at that moment loses and
    // is shown the booking instead. A Front failure cancels the booking.
    // → { ok, bookingId, …board }
    app.post('/api/v1/freight-quotes/options/:optionId(\\d+)/book', route('POST /freight-quotes/options/:optionId/book', async (req, res) => {
        refuseReadOnly(req);
        const optionId = Number(req.params.optionId);
        const { note, shipmentId, to, subject, body } = req.body || {};
        const noteText = F.optionalText(note, 'note', F.NOTE_MAX);
        if (noteText.code) throw invalid(noteText);
        const portalBase = poPortalEmail.portalBaseOf();
        if (!portalBase) {
            throw new Fail(409, 'PORTAL_URL_UNSET', 'The supplier portal address (SUPPLIER_PORTAL_URL) is not configured, so the booking link cannot be sent.');
        }

        const out = await withConnection(async (conn) => {
            const made = await inTransaction(conn, async () => {
                const [[option]] = await conn.query('SELECT * FROM freight_quote_options WHERE id = ?', [optionId]);
                if (!option) throw new Fail(404, 'NOT_FOUND', `Option ${optionId} not found.`);
                const request = await lockRequest(conn, Number(option.request_id));
                if (request.revoked_at) throw new Fail(409, 'REVOKED', 'This request was closed, so it cannot be booked.');
                if (Number(option.answer_no) !== Number(request.answer_count)) {
                    throw new Fail(409, 'STALE_OPTION', 'The forwarder has changed its answer since this option was shown. Reload and choose again.');
                }
                if (await liveBookingOf(conn, Number(request.id))) {
                    throw new Fail(409, 'ALREADY_BOOKED', 'A booking is already in progress on this request.');
                }
                const addresses = await recipientsFor(conn, to, Number(request.forwarder_id));
                const ship = shipmentId === undefined || shipmentId === null || shipmentId === ''
                    ? null : await attachableShipment(conn, Number(shipmentId));

                const o = optionJson(option);
                const tpl = await getEmailTemplate(conn, FREIGHT_BOOKING_REQUEST_TEMPLATE_KEY);
                const vars = {
                    forwarder: request.forwarder_name || '', sailingDate: o.sailingDate, loadingDate: o.loadingDate,
                    portFrom: o.portFrom, portTo: o.portTo, service: F.serviceName(o.service),
                    containerSize: o.containerSize, price: F.money(o.price, o.currency),
                };
                const finalSubject = ((typeof subject === 'string' && subject.trim())
                    ? renderTemplate(subject.trim(), vars)
                    : (renderTemplate(tpl.subject, vars) || `Booking request – sailing ${o.sailingDate}`))
                    .replace(/[\r\n]+/g, ' ').slice(0, 500);

                let bookingId;
                try {
                    const [ins] = await conn.query(
                        `INSERT INTO freight_bookings
                            (request_id, option_id, status, requested_by_email, requested_at, request_note, sent_to,
                             subject, shipment_id, draft_container_name)
                         VALUES (?, ?, 'requested', ?, UTC_TIMESTAMP(), ?, ?, ?, ?, ?)`,
                        [
                            request.id, optionId, req.userEmail || null, noteText.value, JSON.stringify(addresses),
                            finalSubject, ship ? ship.id : null, ship ? (ship.name || ship.reference || null) : null,
                        ]
                    );
                    bookingId = Number(ins.insertId);
                } catch (e) {
                    if (e.errno !== DUPLICATE) throw e;
                    if (isContainerTaken(e)) throw containerTaken();
                    throw new Fail(409, 'ALREADY_BOOKED', 'A booking is already in progress on this request.');
                }
                await conn.query(
                    `UPDATE freight_quote_requests
                        SET row_version = row_version + 1,
                            expires_at = GREATEST(expires_at, UTC_TIMESTAMP() + INTERVAL ? DAY)
                      WHERE id = ?`,
                    [BOOKING_EXTENDS_DAYS, request.id]
                );
                const escaped = Object.fromEntries(Object.entries(vars).map(([k, val]) => [k, escapeHtml(val)]));
                const inner = renderTemplate((typeof body === 'string' && body.trim()) ? body : tpl.bodyHtml, escaped);
                const html = wrapEmail({
                    title: finalSubject,
                    bodyHtml: inner + F.bookingBlockHtml({ option: o, url: F.freightUrl(portalBase, request.token), note: noteText.value }),
                });
                return { bookingId, requestId: Number(request.id), batchId: request.batch_id, addresses, finalSubject, html, option: o, forwarder: request.forwarder_name };
            });

            const sent = await sendFrontMessage({ to: made.addresses, subject: made.finalSubject, html: made.html });
            if (!sent.ok) {
                await conn.query(
                    `UPDATE freight_bookings
                        SET status = 'cancelled', cancel_reason = 'send_failed', cancelled_at = UTC_TIMESTAMP(),
                            row_version = row_version + 1
                      WHERE id = ?`,
                    [made.bookingId]
                );
                throw frontFailure(sent);
            }
            await conn.query(
                'UPDATE freight_bookings SET front_message_uid = ?, front_conversation_id = ? WHERE id = ?',
                [sent.frontMessageUid, sent.frontConversationId, made.bookingId]
            );
            await logSentEmail(conn, {
                emailType: 'freight_booking_request', sentTo: made.addresses, subject: made.finalSubject,
                confirmVia: 'portal', sendTable: 'freight_bookings', sendId: made.bookingId,
            });
            await recordAudit(conn, {
                entityType: 'freight_booking', entityId: made.bookingId, action: 'requested',
                before: null,
                after: {
                    requestId: made.requestId, optionId, forwarder: made.forwarder, to: made.addresses,
                    subject: made.finalSubject, sailingDate: made.option.sailingDate, portFrom: made.option.portFrom,
                    portTo: made.option.portTo, price: made.option.price, currency: made.option.currency,
                    shipmentId: shipmentId != null && shipmentId !== '' ? Number(shipmentId) : null,
                    frontMessageUid: sent.frontMessageUid, frontConversationId: sent.frontConversationId,
                },
                userEmail: req.userEmail,
            });
            return { ok: true, bookingId: made.bookingId, ...(await defaultBoard(conn)) };
        });
        res.json(out);
    }));

    async function lockBooking(conn, id) {
        const [[row]] = await conn.query('SELECT * FROM freight_bookings WHERE id = ? FOR UPDATE', [id]);
        if (!row) throw new Fail(404, 'NOT_FOUND', `Booking ${id} not found.`);
        return row;
    }

    // ── PATCH /api/v1/freight-bookings/:id ──────────────────────────────────
    // Any of, in one call:
    //   { shipmentId: <id> | null }                attach, change or detach the container
    //   { status: 'confirmed', bookingRef, note? } staff confirm for a forwarder who answered
    //                                              by email or phone (also corrects the
    //                                              reference of a confirmed booking)
    //   { status: 'cancelled' }                    staff cancel
    // These routes sit behind the staff sign-in and never use the forwarder's
    // token, so marking a booking needs no override: it records
    // response_source = 'staff'. Cancelling reopens the request (its state is
    // derived), keeps its link open another 30 days, and reopens the links a
    // confirmation had closed as `filled`. → the board.
    app.patch('/api/v1/freight-bookings/:id(\\d+)', route('PATCH /freight-bookings/:id', async (req, res) => {
        refuseReadOnly(req);
        const id = Number(req.params.id);
        const patch = req.body || {};
        const has = key => Object.prototype.hasOwnProperty.call(patch, key);
        if (!has('shipmentId') && !has('status')) {
            throw new Fail(400, 'BAD_FIELD', 'Nothing to change: send shipmentId or status.', { field: 'body' });
        }
        if (has('status') && patch.status !== 'confirmed' && patch.status !== 'cancelled') {
            throw new Fail(400, 'BAD_FIELD', "status must be 'confirmed' or 'cancelled'.", { field: 'status' });
        }

        const board = await withConnection(async (conn) => {
            const confirmed = await inTransaction(conn, async () => {
                const b = await lockBooking(conn, id);
                if (!isLive(b)) throw new Fail(409, 'NOT_LIVE', `This booking is ${b.status} and can no longer be changed.`);
                const wasConfirmed = b.status === 'confirmed';
                const sets = [];
                const params = [];
                const before = {};
                const after = {};

                if (has('shipmentId')) {
                    const wanted = patch.shipmentId === null || patch.shipmentId === '' ? null : Number(patch.shipmentId);
                    if (wanted !== null && (!Number.isSafeInteger(wanted) || wanted <= 0)) {
                        throw new Fail(400, 'BAD_FIELD', 'shipmentId must be a container id, or null.', { field: 'shipmentId' });
                    }
                    const current = b.shipment_id != null ? Number(b.shipment_id) : null;
                    if (wanted !== current) {
                        const ship = wanted === null ? null : await attachableShipment(conn, wanted, id);
                        // Another container: what was sent about the old one says nothing about this one.
                        sets.push('shipment_id = ?', 'draft_container_name = ?', 'contents_sent_at = NULL', 'contents_sent_to = NULL', 'contents_json = NULL', 'contents_document_ids = NULL');
                        params.push(wanted, ship ? (ship.name || ship.reference || null) : null);
                        before.shipmentId = current;
                        after.shipmentId = wanted;
                        after.container = ship ? (ship.name || ship.reference || null) : null;
                    }
                }

                let becameConfirmed = false;
                if (patch.status === 'confirmed') {
                    const ref = F.validateBookingRef(patch.bookingRef);
                    if (ref.code) throw invalid(ref);
                    const noteText = F.optionalText(patch.note, 'note', F.NOTE_MAX);
                    if (noteText.code) throw invalid(noteText);
                    sets.push("status = 'confirmed'", 'booking_ref = ?', 'response_note = ?', 'responded_at = UTC_TIMESTAMP()',
                        'responded_by_name = NULL', 'responded_by_email = ?', "response_source = 'staff'");
                    params.push(ref.value, noteText.value, req.userEmail || null);
                    before.status = b.status;
                    before.bookingRef = b.booking_ref || null;
                    after.status = 'confirmed';
                    after.bookingRef = ref.value;
                    becameConfirmed = true;
                } else if (patch.status === 'cancelled') {
                    sets.push("status = 'cancelled'", "cancel_reason = 'staff'", 'cancelled_at = UTC_TIMESTAMP()', 'cancelled_by_email = ?');
                    params.push(req.userEmail || null);
                    before.status = b.status;
                    after.status = 'cancelled';
                }

                if (!sets.length) return false;
                try {
                    await conn.query(
                        `UPDATE freight_bookings SET ${sets.join(', ')}, row_version = row_version + 1 WHERE id = ?`,
                        [...params, id]
                    );
                } catch (e) {
                    if (e.errno === DUPLICATE && isContainerTaken(e)) throw containerTaken();
                    throw e;
                }
                if (patch.status === 'cancelled') {
                    const request = await lockRequest(conn, Number(b.request_id));
                    await conn.query(
                        `UPDATE freight_quote_requests
                            SET row_version = row_version + 1,
                                expires_at = GREATEST(expires_at, UTC_TIMESTAMP() + INTERVAL ? DAY)
                          WHERE id = ?`,
                        [BOOKING_EXTENDS_DAYS, request.id]
                    );
                    if (wasConfirmed) after.reopened = await reopenFilled(conn, request.batch_id);
                }
                await recordAudit(conn, {
                    entityType: 'freight_booking', entityId: id,
                    action: patch.status === 'cancelled' ? 'cancelled' : (becameConfirmed ? 'confirmed' : 'update'),
                    before, after, userEmail: req.userEmail,
                });
                return becameConfirmed;
            });
            if (confirmed) await syncFilled(conn);
            return defaultBoard(conn);
        });
        res.json(board);
    }));

    // The booking, its request and option, for the contents routes.
    async function loadBookingContext(conn, id) {
        const [[b]] = await conn.query('SELECT * FROM freight_bookings WHERE id = ?', [id]);
        if (!b) throw new Fail(404, 'NOT_FOUND', `Booking ${id} not found.`);
        const [[request]] = await conn.query(`SELECT ${REQUEST_COLUMNS} FROM freight_quote_requests r WHERE r.id = ?`, [b.request_id]);
        const [[option]] = await conn.query('SELECT * FROM freight_quote_options WHERE id = ?', [b.option_id]);
        return { b, request: request || null, option: option || null };
    }

    // ── GET /api/v1/freight-bookings/:id/contents ───────────────────────────
    // The attached container's contents by supplier, as the email will list
    // them, and whether they have changed since the last contents email.
    // → { containerName, groups, totals, missingLots, sentAt, changedSinceSent, changes }
    app.get('/api/v1/freight-bookings/:id(\\d+)/contents', route('GET /freight-bookings/:id/contents', async (req, res) => {
        const id = Number(req.params.id);
        const out = await withConnection(async (conn) => {
            const { b } = await loadBookingContext(conn, id);
            if (b.shipment_id == null) throw new Fail(409, 'NO_CONTAINER', 'No container is attached to this booking yet.');
            const { source, lines } = await loadContainerLines(conn, Number(b.shipment_id));
            const drift = F.contentsDrift(b.contents_json, lines);
            return {
                containerName: source.label,
                ...F.groupContentsBySupplier(lines),
                // Lines with no LOT number yet: the page warns before sending.
                missingLots: lines.filter(l => !String(l.lot || '').trim()).length,
                sentAt: iso(b.contents_sent_at),
                changedSinceSent: drift ? drift.stale : null,
                changes: drift ? drift.changes : [],
            };
        });
        res.json(out);
    }));

    // The forwarder quote PDFs and CSVs of one generate run, as attachments.
    async function documentAttachments(conn, batchId, mainDocumentId) {
        const [docs] = await conn.query(
            `SELECT id, s3_key, csv_s3_key, supplier
               FROM draft_container_documents
              WHERE ${batchId ? 'batch_id = ?' : 'id = ?'} AND type = 'forwarder-quote' AND deleted_at IS NULL
              ORDER BY (supplier IS NULL) DESC, supplier ASC, id ASC`,
            [batchId || mainDocumentId]
        );
        const attachments = [];
        for (const d of docs) {
            for (const [key, contentType] of [[d.s3_key, 'application/pdf'], [d.csv_s3_key, 'text/csv']]) {
                if (!key) continue;
                const obj = await s3.send(new GetObjectCommand({ Bucket: poBucket(), Key: key }));
                attachments.push({
                    bytes: Buffer.from(await obj.Body.transformToByteArray()),
                    filename: key.split('/').pop(),
                    contentType,
                });
            }
        }
        return { docs, attachments };
    }

    // ── POST /api/v1/freight-bookings/:id/contents-email ────────────────────
    // Body { rowVersion, to?, subject?, body?, attachDocuments? }. Emails the
    // forwarder the attached container's contents by supplier: a table per
    // supplier in the body, and (unless attachDocuments is false) a freshly
    // generated forwarder quote, the combined PDF and CSV plus one pair per
    // supplier, attached. The lines listed are kept on the booking
    // (contents_json), so a later change to the container shows as
    // "changed since sent", and so are the ids of the documents attached
    // (contents_document_ids): the supplier portal lists those on the
    // forwarder's page. `rowVersion` is the booking's as last read: the
    // call claims it before sending, so a retry of a call that did send is
    // refused (409 STALE) instead of sending twice. → the board.
    app.post('/api/v1/freight-bookings/:id(\\d+)/contents-email', route('POST /freight-bookings/:id/contents-email', async (req, res) => {
        refuseReadOnly(req);
        const id = Number(req.params.id);
        const { rowVersion, to, subject, body, attachDocuments } = req.body || {};
        if (!Number.isSafeInteger(rowVersion) || rowVersion < 0) {
            throw new Fail(400, 'BAD_FIELD', 'rowVersion must be the booking\'s rowVersion.', { field: 'rowVersion' });
        }
        const wantDocuments = attachDocuments !== false;
        if (wantDocuments && !poBucket()) throw new Fail(500, 'NO_BUCKET', 'PO_DOCS_BUCKET env var not configured.');

        const board = await withConnection(async (conn) => {
            const { b, request, option } = await loadBookingContext(conn, id);
            if (!isLive(b)) throw new Fail(409, 'NOT_LIVE', `This booking is ${b.status}.`);
            if (b.shipment_id == null) throw new Fail(409, 'NO_CONTAINER', 'Attach a container to this booking first.');
            if (!request || !option) throw new Fail(409, 'NOT_LIVE', 'This booking has lost its request.');
            const addresses = await recipientsFor(conn, to, Number(request.forwarder_id));
            const { ship, source, lines } = await loadContainerLines(conn, Number(b.shipment_id));
            if (!lines.length) throw new Fail(409, 'NO_LINES', `${source.label} has no lines to send.`);

            // The claim: only one call per rowVersion gets past here.
            const [claim] = await conn.query(
                'UPDATE freight_bookings SET row_version = row_version + 1 WHERE id = ? AND row_version = ? AND status IN (?)',
                [id, rowVersion, LIVE]
            );
            if (claim.affectedRows !== 1) {
                throw new Fail(409, 'STALE', 'This booking changed since it was shown (the contents may already have been sent). Reload and check.');
            }

            let docs = [];
            let attachments = [];
            if (wantDocuments) {
                const set = await generateDraftDocumentSet(conn, {
                    name: ship.name, docType: 'forwarder-quote', supplierName: null, comments: null,
                    wantSplit: true, userEmail: req.userEmail,
                    booked: source.kind === 'booked' ? { shipmentId: Number(ship.id), reference: source.reference } : null,
                });
                if (!set.noLines) {
                    const docIds = [set.main.documentId, ...(set.supplierDocuments || []).map(d => d.documentId)];
                    await conn.query('UPDATE draft_container_documents SET shipment_id = ? WHERE id IN (?)', [ship.id, docIds]);
                    ({ docs, attachments } = await documentAttachments(conn, set.batchId, set.main.documentId));
                }
            }

            const o = optionJson(option);
            const tpl = await getEmailTemplate(conn, FREIGHT_BOOKING_CONTENTS_TEMPLATE_KEY);
            const vars = {
                forwarder: request.forwarder_name || '', bookingRef: b.booking_ref || '',
                sailingDate: o.sailingDate, containerName: source.label,
            };
            const finalSubject = ((typeof subject === 'string' && subject.trim())
                ? renderTemplate(subject.trim(), vars)
                : (renderTemplate(tpl.subject, vars) || `Container contents – ${source.label}`))
                .replace(/[\r\n]+/g, ' ').slice(0, 500);
            const escaped = Object.fromEntries(Object.entries(vars).map(([k, val]) => [k, escapeHtml(val)]));
            const inner = renderTemplate((typeof body === 'string' && body.trim()) ? body : tpl.bodyHtml, escaped);
            const contents = F.groupContentsBySupplier(lines);
            const html = wrapEmail({
                title: finalSubject,
                bodyHtml: inner + F.contentsHtml(contents, { containerName: source.label, option: o, bookingRef: b.booking_ref || null }),
            });

            const sent = await sendFrontMessage({ to: addresses, subject: finalSubject, html, attachments });
            if (!sent.ok) throw frontFailure(sent);

            // contents_document_ids: the documents attached, in the order attached
            // (the combined one first). The supplier portal lists them on the
            // forwarder's page. NULL when the email went without documents.
            const documentIds = docs.map(d => Number(d.id));
            await conn.query(
                `UPDATE freight_bookings
                    SET contents_sent_at = UTC_TIMESTAMP(), contents_sent_to = ?, contents_json = ?,
                        contents_document_ids = ?,
                        row_version = row_version + 1
                  WHERE id = ?`,
                [JSON.stringify(addresses), JSON.stringify(F.contentsSnapshot(lines)), documentIds.length ? JSON.stringify(documentIds) : null, id]
            );
            // Each attached PDF shows this email in its own sends history, as a
            // forwarder quote emailed from the draft does. `delivery` (2026-11-13_10):
            // the files were attached, as every forwarder quote's are.
            for (const d of docs) {
                await conn.query(
                    `INSERT INTO draft_container_document_sends
                        (draft_container_document_id, sent_to, subject, front_message_uid, front_conversation_id, sent_by_email, delivery)
                     VALUES (?, ?, ?, ?, ?, ?, 'attached')`,
                    [d.id, JSON.stringify(addresses), finalSubject, sent.frontMessageUid, sent.frontConversationId, req.userEmail || null]
                );
            }
            await logSentEmail(conn, {
                emailType: 'freight_booking_contents', sentTo: addresses, subject: finalSubject,
                confirmVia: 'none', sendTable: 'freight_bookings', sendId: id,
            });
            await recordAudit(conn, {
                entityType: 'freight_booking', entityId: id, action: 'contents_sent',
                before: null,
                after: {
                    to: addresses, subject: finalSubject, container: source.label, shipmentId: Number(ship.id),
                    suppliers: contents.groups.length, lines: contents.totals.lines, cartons: contents.totals.cartons,
                    attachments: attachments.map(a => a.filename),
                    frontMessageUid: sent.frontMessageUid, frontConversationId: sent.frontConversationId,
                },
                userEmail: req.userEmail,
            });
            return defaultBoard(conn);
        });
        res.json(board);
    }));

    // ── stock movements ─────────────────────────────────────────────────────

    // ── POST /api/v1/freight-quotes/:id/stock-movements ─────────────────────
    // Body { stockMovements: [{ from, cbm, weightKg }], addKey?, notify?, to?,
    //        subject?, body? }. Adds stock movements to a request that is
    // already out, whatever it has reached: waiting, quoted, booking requested
    // or confirmed, or expired (never a closed link). Unless `notify` is
    // false, the forwarder is emailed the new movements and the link to the
    // same page, where each is priced on its own. The link is kept open at
    // least another 30 days, so an expired one works again.
    // The request's row_version is NOT bumped: nothing the forwarder may be
    // typing (its options) is affected, so its open form must not be refused.
    // SAFE TO RETRY with the same `addKey` (the dialog makes it once): a call
    // whose rows are already there adds and emails nothing (`alreadyAdded`).
    // A Front failure takes the rows back out, so nothing unasked-for is left
    // on the forwarder's page and the same call can simply be made again.
    // → { ok, added, alreadyAdded, …board }
    app.post('/api/v1/freight-quotes/:id(\\d+)/stock-movements', route('POST /freight-quotes/:id/stock-movements', async (req, res) => {
        refuseReadOnly(req);
        const id = Number(req.params.id);
        const v = F.validateMovementAdd(req.body);
        if (v.code) throw invalid(v);
        const { notify, to, subject, body } = req.body || {};
        const wantEmail = notify !== false;
        const portalBase = poPortalEmail.portalBaseOf();
        if (wantEmail && !portalBase) {
            throw new Fail(409, 'PORTAL_URL_UNSET', 'The supplier portal address (SUPPLIER_PORTAL_URL) is not configured, so the link cannot be sent.');
        }
        const addKey = v.value.addKey || crypto.randomUUID();
        const added = v.value.stockMovements;

        const out = await withConnection(async (conn) => {
            const made = await inTransaction(conn, async () => {
                const request = await lockRequest(conn, id);
                if (request.revoked_at) throw new Fail(409, 'REVOKED', 'This request was closed, so nothing can be added to it.');
                const [[counts]] = await conn.query(
                    `SELECT COALESCE(SUM(add_key = ?), 0) AS same_call, COALESCE(SUM(deleted_at IS NULL), 0) AS live
                       FROM freight_stock_movements WHERE request_id = ?`,
                    [addKey, id]
                );
                if (Number(counts.same_call) > 0) return null;
                if (Number(counts.live) + added.length > F.MAX_MOVEMENTS) {
                    throw new Fail(409, 'TOO_MANY', `A request carries at most ${F.MAX_MOVEMENTS} stock movements; this one has ${Number(counts.live)}.`);
                }
                const addresses = wantEmail ? await recipientsFor(conn, to, Number(request.forwarder_id)) : [];
                await insertMovements(conn, id, addKey, added, req.userEmail);
                await conn.query(
                    `UPDATE freight_quote_requests
                        SET expires_at = GREATEST(expires_at, UTC_TIMESTAMP() + INTERVAL ? DAY)
                      WHERE id = ?`,
                    [BOOKING_EXTENDS_DAYS, id]
                );
                if (!wantEmail) return { request, addresses, finalSubject: null, html: null };

                const facts = requestFactsOf(request);
                const tpl = await getEmailTemplate(conn, FREIGHT_MOVEMENT_REQUEST_TEMPLATE_KEY);
                const vars = {
                    forwarder: request.forwarder_name || '', count: String(added.length),
                    windowStart: facts.windowStart, windowEnd: facts.windowEnd, origin: facts.origin,
                    destination: facts.destination, containerSize: facts.containerSize,
                };
                const finalSubject = ((typeof subject === 'string' && subject.trim())
                    ? renderTemplate(subject.trim(), vars)
                    : (renderTemplate(tpl.subject, vars) || 'Stock movement price request'))
                    .replace(/[\r\n]+/g, ' ').slice(0, 500);
                const escaped = Object.fromEntries(Object.entries(vars).map(([k, val]) => [k, escapeHtml(val)]));
                const inner = renderTemplate((typeof body === 'string' && body.trim()) ? body : tpl.bodyHtml, escaped);
                // The link lasts at least as long as it did, and at least 30 days from now.
                const floor = F.addDays(new Date().toISOString().slice(0, 10), BOOKING_EXTENDS_DAYS);
                const was = iso(request.expires_at);
                const expiresOn = was && was.slice(0, 10) > floor ? was.slice(0, 10) : floor;
                const html = wrapEmail({
                    title: finalSubject,
                    bodyHtml: inner + F.movementBlockHtml({ movements: added, url: F.freightUrl(portalBase, request.token), expiresOn }),
                });
                return { request, addresses, finalSubject, html };
            });
            if (!made) return { ok: true, added: 0, alreadyAdded: true, ...(await defaultBoard(conn)) };

            let sent = null;
            if (wantEmail) {
                sent = await sendFrontMessage({ to: made.addresses, subject: made.finalSubject, html: made.html });
                if (!sent.ok) {
                    await conn.query('DELETE FROM freight_stock_movements WHERE request_id = ? AND add_key = ?', [id, addKey]);
                    throw frontFailure(sent);
                }
                await logSentEmail(conn, {
                    emailType: 'freight_movement_request', sentTo: made.addresses, subject: made.finalSubject,
                    confirmVia: 'portal', sendTable: 'freight_quote_requests', sendId: id,
                });
            }
            await recordAudit(conn, {
                entityType: 'freight_quote_request', entityId: id, action: 'movements_added',
                before: null,
                after: {
                    forwarder: made.request.forwarder_name || null, stockMovements: added,
                    to: made.addresses, subject: made.finalSubject,
                    frontMessageUid: sent ? sent.frontMessageUid : null, frontConversationId: sent ? sent.frontConversationId : null,
                },
                userEmail: req.userEmail,
            });
            return { ok: true, added: added.length, alreadyAdded: false, ...(await defaultBoard(conn)) };
        });
        res.json(out);
    }));

    async function lockMovement(conn, id) {
        const [[row]] = await conn.query('SELECT * FROM freight_stock_movements WHERE id = ? FOR UPDATE', [id]);
        if (!row || row.deleted_at) throw new Fail(404, 'NOT_FOUND', `Stock movement ${id} not found.`);
        return row;
    }

    // ── PATCH /api/v1/freight-stock-movements/:id ───────────────────────────
    // Body { price, currency, note? }: staff type in the price a forwarder
    // gave by email or phone (price_source = 'staff'), or correct one.
    // { price: null } takes a price back off. Allowed whatever the request has
    // reached, a closed link included: the price is a record of what was said.
    // → the board.
    app.patch('/api/v1/freight-stock-movements/:id(\\d+)', route('PATCH /freight-stock-movements/:id', async (req, res) => {
        refuseReadOnly(req);
        const id = Number(req.params.id);
        const v = F.validateMovementPrice(req.body);
        if (v.code) throw invalid(v);
        const board = await withConnection(async (conn) => {
            await inTransaction(conn, async () => {
                const m = await lockMovement(conn, id);
                const priced = v.value.price !== null;
                await conn.query(
                    `UPDATE freight_stock_movements
                        SET price = ?, currency = ?, price_note = ?, priced_at = ${priced ? 'UTC_TIMESTAMP()' : 'NULL'},
                            priced_by_name = NULL, priced_by_email = ?, price_source = ?, price_ip = NULL,
                            row_version = row_version + 1
                      WHERE id = ?`,
                    [v.value.price, v.value.currency, v.value.note, priced ? (req.userEmail || null) : null, priced ? 'staff' : null, id]
                );
                await recordAudit(conn, {
                    entityType: 'freight_stock_movement', entityId: id, action: priced ? 'price_entered' : 'price_cleared',
                    before: { price: m.price === null ? null : Number(m.price), currency: m.currency || null, source: m.price_source || null },
                    after: { requestId: Number(m.request_id), factory: m.factory, price: v.value.price, currency: v.value.currency, note: v.value.note },
                    userEmail: req.userEmail,
                });
            });
            return defaultBoard(conn);
        });
        res.json(board);
    }));

    // ── GET /api/v1/shipments/:id/forwarder-charges ─────────────────────────
    // What the container's live freight booking prices (2026-11-20), for
    // ShipLine's forwarder payment editor to offer: the booked option's
    // all-in price and each stock movement's own price. A movement's price is
    // never inside the option's, so the two add up. Each carries the live
    // payment extra already priced from it (payment_extras.freight_booking_id /
    // freight_movement_id). { booking: null, movements: [] } with none.
    app.get('/api/v1/shipments/:id(\\d+)/forwarder-charges', route('GET /shipments/:id/forwarder-charges', async (req, res) => {
        const shipmentId = Number(req.params.id);
        const out = await withConnection(async (conn) => {
            const [[b]] = await conn.query(
                `SELECT b.id, b.status, b.booking_ref, b.request_id, b.option_id, r.forwarder_id, r.forwarder_name
                   FROM freight_bookings b
                   JOIN freight_quote_requests r ON r.id = b.request_id
                  WHERE b.shipment_id = ? AND b.status IN (?)
                  ORDER BY b.id DESC LIMIT 1`,
                [shipmentId, LIVE]
            );
            if (!b) return { booking: null, movements: [] };
            const [[o]] = await conn.query('SELECT * FROM freight_quote_options WHERE id = ?', [b.option_id]);
            const [[f]] = b.forwarder_id != null
                ? await conn.query('SELECT id, name, deleted_at FROM forwarders WHERE id = ?', [b.forwarder_id])
                : [[null]];
            const [moves] = await conn.query(
                'SELECT id, factory, cbm, price, currency FROM freight_stock_movements WHERE request_id = ? AND deleted_at IS NULL ORDER BY id ASC',
                [b.request_id]
            );
            const [held] = await conn.query(
                `SELECT id, freight_booking_id, freight_movement_id, amount, currency FROM payment_extras
                  WHERE deleted_at IS NULL AND (freight_booking_id = ? OR freight_movement_id IN (?))`,
                [b.id, moves.length ? moves.map(m => m.id) : [0]]
            );
            const holder = (match) => {
                const x = held.find(match);
                return x ? { extraId: Number(x.id), extraAmount: Number(x.amount), extraCurrency: x.currency } : { extraId: null, extraAmount: null, extraCurrency: null };
            };
            const opt = o ? optionJson(o) : null;
            return {
                booking: {
                    id: Number(b.id), status: b.status, bookingRef: b.booking_ref || null,
                    forwarderId: b.forwarder_id != null ? Number(b.forwarder_id) : null,
                    forwarderName: b.forwarder_name || null,
                    forwarderLive: Boolean(f && !f.deleted_at),
                    option: opt ? {
                        price: opt.price, currency: opt.currency, service: opt.service, portFrom: opt.portFrom, portTo: opt.portTo,
                        sailingDate: opt.sailingDate, arrivalDate: opt.arrivalDate, containerSize: opt.containerSize,
                    } : null,
                    ...holder(x => x.freight_booking_id != null && Number(x.freight_booking_id) === Number(b.id)),
                },
                movements: moves.map(m => ({
                    id: Number(m.id), from: m.factory, cbm: Number(m.cbm),
                    price: m.price == null ? null : Number(m.price), currency: m.currency || null,
                    ...holder(x => x.freight_movement_id != null && Number(x.freight_movement_id) === Number(m.id)),
                })),
            };
        });
        res.json(out);
    }));

    // ── DELETE /api/v1/freight-stock-movements/:id ──────────────────────────
    // Staff remove a movement that is no longer wanted. Soft: the row and any
    // price on it stay in the table, and it leaves the forwarder's page.
    // Removing one twice is a no-op. → the board.
    app.delete('/api/v1/freight-stock-movements/:id(\\d+)', route('DELETE /freight-stock-movements/:id', async (req, res) => {
        refuseReadOnly(req);
        const id = Number(req.params.id);
        const board = await withConnection(async (conn) => {
            await inTransaction(conn, async () => {
                const [[m]] = await conn.query('SELECT * FROM freight_stock_movements WHERE id = ? FOR UPDATE', [id]);
                if (!m) throw new Fail(404, 'NOT_FOUND', `Stock movement ${id} not found.`);
                if (m.deleted_at) return;
                await conn.query(
                    `UPDATE freight_stock_movements
                        SET deleted_at = UTC_TIMESTAMP(), deleted_by_email = ?, row_version = row_version + 1
                      WHERE id = ?`,
                    [req.userEmail || null, id]
                );
                await recordAudit(conn, {
                    entityType: 'freight_stock_movement', entityId: id, action: 'removed',
                    before: {
                        requestId: Number(m.request_id), factory: m.factory, cbm: Number(m.cbm), weightKg: Number(m.weight_kg),
                        price: m.price === null ? null : Number(m.price), currency: m.currency || null,
                    },
                    after: null, userEmail: req.userEmail,
                });
            });
            return defaultBoard(conn);
        });
        res.json(board);
    }));
}

module.exports = { registerFreightQuoteRoutes };
