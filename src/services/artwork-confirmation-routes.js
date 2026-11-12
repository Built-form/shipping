'use strict';

// /api/v1/purchase-orders/:id/artwork-confirmation — asking a supplier to
// confirm final artwork per PO line, and reading what they confirmed
// (docs/artwork-confirmations.md).
//
// Staff send the "Artwork Confirmed" email from a PO in ShipLine. It carries a
// no-login link into the supplier portal, <SUPPLIER_PORTAL_URL>/artwork/<token>,
// where the supplier ticks each item; a signed-in portal user ticks the same
// items on the order's page. The portal inserts artwork_confirmations rows in
// THIS schema and invokes jportal-artwork-confirmed-<env>
// (src/handlers/artwork-confirmed.js), which fills
// orders.artwork_confirmed_by_supplier and, when the supplier requires no
// deposit or it has been paid (src/services/deposit-gate.js), moves a PO_SENT
// line into production. The only write to orders here is staff clearing a
// supplier's confirmation.
//
// The pure rules are in src/lib/artwork-confirmations.js. Registered from
// orders.js; authed through the greedy /api/v1/{proxy+} route. Reads are open to
// every staff user; writes need an editor role, as the QC routes do.

const A = require('../lib/artwork-confirmations');
const QV = require('../lib/qc-visits');
const poPortalEmail = require('../lib/po-portal-email');
const { escapeHtml, wrapEmail } = require('../lib/email-layout');
const { ARTWORK_CONFIRMATION_TEMPLATE_KEY } = require('../lib/email-template-defaults');
const { sendFrontMessage: defaultSend } = require('./front-send');
const { logSentEmail } = require('../lib/email-receipt');
const { loadDepositGate } = require('./deposit-gate');
const { londonToday: defaultLondonToday } = require('./daily-alerts');
const defaultLog = require('../lib/logger');

// The tables are not there yet (the migration has not run on this server).
const TABLE_MISSING = 1146;

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

const jsonArray = (value) => {
    if (Array.isArray(value)) return value;
    try { const parsed = JSON.parse(value); return Array.isArray(parsed) ? parsed : []; } catch { return []; }
};

const REQUEST_COLUMNS = `id, token, purchase_order_id, order_ids, sent_to, subject, created_by_email,
        created_at, expires_at, revoked_at, revoke_reason, (expires_at <= UTC_TIMESTAMP()) AS expired`;

function registerArtworkConfirmationRoutes(app, deps) {
    const {
        withConnection, recordAudit, getEmailTemplate, renderTemplate,
        ready = async () => {},
        sendFrontMessage = defaultSend,
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
            log.error(`[${label}]`, e);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    };

    function refuseReadOnly(req) {
        if (!QV.canEdit(req.userType)) throw new Fail(403, 'READ_ONLY', 'Read only.');
    }

    async function loadPo(conn, id) {
        const [rows] = await conn.query(
            'SELECT id, po_number, supplier FROM purchase_orders WHERE id = ? AND deleted_at IS NULL',
            [id]
        );
        if (!rows.length) throw new Fail(404, 'NOT_FOUND', `Purchase order ${id} not found.`);
        return rows[0];
    }

    async function loadLines(conn, poId) {
        const [rows] = await conn.query(
            `SELECT id, jf_code, asin, product_name, quantity, status, artwork_confirmed_date,
                    artwork_confirmed_by_supplier
               FROM orders
              WHERE purchase_order_id = ? AND deleted_at IS NULL
              ORDER BY jf_code ASC, id ASC`,
            [poId]
        );
        return rows;
    }

    // The newest confirmation per line, and every request of the PO (newest
    // first). Empty where the tables do not exist yet.
    async function loadPortalRows(conn, poId) {
        try {
            const [confirmations] = await conn.query(
                `SELECT c.id, c.order_id, c.request_id, c.source, c.confirmed_by_email, c.confirmed_by_name,
                        c.confirmed_on, c.created_at, c.applied_at
                   FROM artwork_confirmations c
                  WHERE c.purchase_order_id = ?
                  ORDER BY c.id ASC`,
                [poId]
            );
            const [requests] = await conn.query(
                `SELECT ${REQUEST_COLUMNS} FROM artwork_confirmation_requests
                  WHERE purchase_order_id = ?
                  ORDER BY created_at DESC, id DESC`,
                [poId]
            );
            return { confirmations, requests, missing: false };
        } catch (e) {
            if (e.errno !== TABLE_MISSING) throw e;
            return { confirmations: [], requests: [], missing: true };
        }
    }

    function requestJson(row, lineById, pendingOrderIds, portalBase, fqcIds = new Set()) {
        const orderIds = A.parseOrderIds(row.order_ids).filter(id => !fqcIds.has(id));
        const live = orderIds.map(id => lineById.get(id)).filter(Boolean)
            .map(l => ({ confirmed: A.isConfirmed(l) || pendingOrderIds.has(Number(l.id)) }));
        const state = A.requestState(row, live);
        return {
            id: Number(row.id),
            state,
            revokeReason: row.revoke_reason || null,
            orderIds,
            sentTo: jsonArray(row.sent_to).map(String),
            subject: row.subject || null,
            sentAt: iso(row.created_at),
            sentByEmail: row.created_by_email || null,
            expiresAt: iso(row.expires_at),
            revokedAt: iso(row.revoked_at),
            // The link is the permission: only while it still takes ticks.
            url: state === 'open' && portalBase ? A.artworkUrl(portalBase, row.token) : null,
        };
    }

    // Whether a tick would move this PO's PO_SENT lines into production: only with
    // no deposit required, or the deposit paid. Null when it cannot be read.
    async function readGate(conn, po) {
        try {
            return await loadDepositGate(conn, { id: po.id, supplier: po.supplier });
        } catch (e) {
            log.warn('[artwork-confirmation] deposit gate could not be read', { purchaseOrderId: po.id, error: e.message, errno: e.errno });
            return null;
        }
    }

    async function readPanel(conn, poId) {
        const po = await loadPo(conn, poId);
        const allLines = await loadLines(conn, poId);
        // _FQC samples have no artwork of their own: not listed, not counted, and dropped
        // from a request that named one before they were left out.
        const fqcIds = new Set(allLines.filter(l => A.isFqcLine(l)).map(l => Number(l.id)));
        const lines = allLines.filter(l => !fqcIds.has(Number(l.id)));
        const gate = await readGate(conn, po);
        const { confirmations, requests, missing } = await loadPortalRows(conn, poId);
        const portalBase = poPortalEmail.portalBaseOf();
        const sentToByRequest = new Map(requests.map(r => [Number(r.id), jsonArray(r.sent_to)]));
        const latest = new Map();
        const pendingOrderIds = new Set();
        for (const c of confirmations) {
            latest.set(Number(c.order_id), c);
            if (!c.applied_at) pendingOrderIds.add(Number(c.order_id));
        }
        const lineById = new Map(lines.map(l => [Number(l.id), l]));
        return {
            enabled: portalBase !== '' && !missing,
            // { depositPct, depositPaid, clear, reason }: `clear` = a tick moves a
            // PO_SENT line into production; otherwise it only records the confirmation.
            deposit: gate,
            lines: lines.map((l) => {
                const c = latest.get(Number(l.id)) || null;
                return {
                    orderId: Number(l.id),
                    jfCode: l.jf_code || null,
                    productName: l.product_name || null,
                    quantity: Number(l.quantity || 0),
                    status: l.status || null,
                    // The supplier's confirmation (its own field), and staff's own date beside it.
                    artworkConfirmedBySupplier: A.toYmd(l.artwork_confirmed_by_supplier),
                    artworkConfirmedDate: A.toYmd(l.artwork_confirmed_date),
                    tickable: A.isTickable(l) && !pendingOrderIds.has(Number(l.id)),
                    confirmation: c ? {
                        confirmedOn: A.toYmd(c.confirmed_on),
                        source: c.source,
                        by: A.confirmerOf(c, sentToByRequest.get(Number(c.request_id))),
                        requestId: c.request_id != null ? Number(c.request_id) : null,
                        confirmedAt: iso(c.created_at),
                        appliedAt: iso(c.applied_at),
                    } : null,
                };
            }),
            requests: requests.map(r => requestJson(r, lineById, pendingOrderIds, portalBase, fqcIds)),
        };
    }

    // ── GET /api/v1/purchase-orders/:id/artwork-confirmation ────────────────
    // { enabled, deposit, lines: [{ orderId, jfCode, productName, quantity, status,
    //   artworkConfirmedBySupplier, artworkConfirmedDate, tickable, confirmation }],
    //   requests: [...] }
    app.get('/api/v1/purchase-orders/:id(\\d+)/artwork-confirmation', route('GET /purchase-orders/:id/artwork-confirmation', async (req, res) => {
        const id = Number(req.params.id);
        res.json(await withConnection(conn => readPanel(conn, id)));
    }));

    // ── POST /api/v1/purchase-orders/:id/artwork-confirmation/email ─────────
    // Body { to, subject?, body?, orderIds? }. Creates the request (its token is
    // the link) and emails it through Front: the stored `artwork_confirmation`
    // template, or the body the send dialog edited, with the fixed block (items,
    // button) appended. A Front failure revokes the request, so no dead link is
    // left open. → { ok, requestId, sentTo, subject, panel }
    app.post('/api/v1/purchase-orders/:id(\\d+)/artwork-confirmation/email', route('POST /purchase-orders/:id/artwork-confirmation/email', async (req, res) => {
        refuseReadOnly(req);
        const id = Number(req.params.id);
        const { to, subject, body, orderIds } = req.body || {};

        const { addresses, invalid } = poPortalEmail.parseRecipients(to);
        if (invalid.length) {
            throw new Fail(400, 'BAD_FIELD', `Not a valid email address: ${invalid.map(a => `"${a}"`).join(', ')}`, { field: 'to', invalid });
        }
        if (!addresses.length) throw new Fail(400, 'BAD_FIELD', 'A valid `to` email address (or array of addresses) is required.', { field: 'to' });
        const portalBase = poPortalEmail.portalBaseOf();
        if (!portalBase) {
            throw new Fail(409, 'PORTAL_URL_UNSET', 'The supplier portal address (SUPPLIER_PORTAL_URL) is not configured, so the artwork link cannot be sent.');
        }

        const out = await withConnection(async (conn) => {
            const po = await loadPo(conn, id);
            const allLines = await loadLines(conn, id);
            const lines = allLines.filter(l => !A.isFqcLine(l)); // never asked about, never listed
            const { confirmations } = await loadPortalRows(conn, id);
            const pending = new Set(confirmations.filter(c => !c.applied_at).map(c => Number(c.order_id)));
            const chosen = A.chooseLines(lines.filter(l => !pending.has(Number(l.id))), orderIds);
            if (!chosen.orderIds) {
                throw new Fail(chosen.code === 'NOTHING_TO_CONFIRM' ? 409 : 400, chosen.code, chosen.error, chosen.field ? { field: chosen.field } : null);
            }
            const chosenLines = lines.filter(l => chosen.orderIds.includes(Number(l.id)));

            const tpl = await getEmailTemplate(conn, ARTWORK_CONFIRMATION_TEMPLATE_KEY);
            const subjectVars = { poNumber: po.po_number, supplier: po.supplier || '' };
            const finalSubject = ((typeof subject === 'string' && subject.trim())
                ? renderTemplate(subject.trim(), subjectVars)
                : (renderTemplate(tpl.subject, subjectVars) || `Artwork confirmation – ${po.po_number}`))
                .replace(/[\r\n]+/g, ' ').slice(0, 500);

            const token = A.newToken();
            const expiresOn = new Date(Date.now() + A.EXPIRY_DAYS * 86400000).toISOString().slice(0, 10);
            const [ins] = await conn.query(
                `INSERT INTO artwork_confirmation_requests
                    (token, purchase_order_id, po_number, supplier_name, order_ids, sent_to, subject,
                     created_by_email, expires_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP() + INTERVAL ? DAY)`,
                [
                    token, id, po.po_number || null, po.supplier || null,
                    JSON.stringify(chosen.orderIds), JSON.stringify(addresses), finalSubject,
                    req.userEmail || null, A.EXPIRY_DAYS,
                ]
            );
            const requestId = Number(ins.insertId);

            const url = A.artworkUrl(portalBase, token);
            const bodyVars = {
                poNumber: escapeHtml(po.po_number),
                supplier: escapeHtml(po.supplier || ''),
                lines: A.linesHtml(chosenLines),
            };
            const inner = renderTemplate((typeof body === 'string' && body.trim()) ? body : tpl.bodyHtml, bodyVars);
            const html = wrapEmail({
                title: finalSubject,
                bodyHtml: inner + A.artworkBlockHtml({ poNumber: po.po_number, url, lines: chosenLines, expiresOn }),
            });

            const sent = await sendFrontMessage({ to: addresses, subject: finalSubject, html });
            if (!sent.ok) {
                await conn.query(
                    `UPDATE artwork_confirmation_requests
                        SET revoked_at = UTC_TIMESTAMP(), revoke_reason = 'send_failed'
                      WHERE id = ?`,
                    [requestId]
                );
                if (sent.reason === 'front_not_configured') {
                    throw new Fail(500, 'FRONT_NOT_CONFIGURED', 'Front is not configured (FRONT_API_TOKEN, FRONT_CHANNEL_ID).');
                }
                throw new Fail(502, 'FRONT_ERROR', 'Front rejected the message.', { frontStatus: sent.status ?? null, frontBody: sent.detail ?? null });
            }

            await conn.query(
                'UPDATE artwork_confirmation_requests SET front_message_uid = ?, front_conversation_id = ? WHERE id = ?',
                [sent.frontMessageUid, sent.frontConversationId, requestId]
            );
            // On ShipLine's Email Receipts page as a portal email: the link is
            // the one thing asked (no "Confirm receipt" button), and a tick on
            // one of its items confirms it (src/lib/email-receipt.js).
            await logSentEmail(conn, {
                emailType: 'artwork_confirmation', sentTo: addresses, subject: finalSubject,
                confirmVia: 'portal', sendTable: 'artwork_confirmation_requests', sendId: requestId,
            });

            // Sending the email IS staff confirming the artwork: each line it covers
            // gets the staff Artwork Confirmed date (artwork_confirmed_date) of today,
            // UK time. A line that already has one keeps it. Only once Front has
            // taken the message: a failed send dates nothing. An _FQC sample line is
            // never in the email, but it is the same artwork as its product: when
            // JF0787 is in the email, JF0787_FQC gets the date too. A sample whose
            // product is not in this email is left alone.
            const artworkDate = londonToday();
            const toDate = [...chosenLines, ...A.fqcSamplesOf(chosenLines, allLines)]
                .filter(l => A.toYmd(l.artwork_confirmed_date) === null).map(l => Number(l.id));
            let datedOrderIds = [];
            if (toDate.length) {
                await conn.query(
                    `UPDATE orders SET artwork_confirmed_date = ?
                      WHERE id IN (?) AND purchase_order_id = ? AND artwork_confirmed_date IS NULL AND deleted_at IS NULL`,
                    [artworkDate, toDate, id]
                );
                const [now] = await conn.query(
                    'SELECT id FROM orders WHERE id IN (?) AND artwork_confirmed_date = ?',
                    [toDate, artworkDate]
                );
                datedOrderIds = now.map(r => Number(r.id));
                for (const orderId of datedOrderIds) {
                    await recordAudit(conn, {
                        entityType: 'order', entityId: orderId, action: 'update',
                        before: { artworkConfirmedDate: null },
                        after: { artworkConfirmedDate: artworkDate, note: 'set when the artwork confirmation email was sent' },
                        userEmail: req.userEmail,
                    });
                }
            }

            // Never the token: whoever reads the audit log must not get the link.
            await recordAudit(conn, {
                entityType: 'purchase_order', entityId: id, action: 'artwork_confirmation_requested',
                before: null,
                after: {
                    requestId, orderIds: chosen.orderIds, to: addresses, subject: finalSubject,
                    frontMessageUid: sent.frontMessageUid, frontConversationId: sent.frontConversationId, expiresOn,
                    artworkConfirmedDate: datedOrderIds.length ? artworkDate : null, datedOrderIds,
                },
                userEmail: req.userEmail,
            });
            return {
                ok: true, requestId, sentTo: addresses, subject: finalSubject,
                datedOrderIds, artworkConfirmedDate: datedOrderIds.length ? artworkDate : null,
                panel: await readPanel(conn, id),
            };
        });
        res.json(out);
    }));

    // ── POST /api/v1/purchase-orders/:id/artwork-confirmation/requests/:rid/revoke ──
    // Closes a link. Ticks already saved stay. Revoking twice is a no-op. → the panel.
    app.post('/api/v1/purchase-orders/:id(\\d+)/artwork-confirmation/requests/:rid(\\d+)/revoke', route('POST /purchase-orders/:id/artwork-confirmation/requests/:rid/revoke', async (req, res) => {
        refuseReadOnly(req);
        const id = Number(req.params.id);
        const rid = Number(req.params.rid);
        const panel = await withConnection(async (conn) => {
            await loadPo(conn, id);
            const [rows] = await conn.query(
                'SELECT id, revoked_at FROM artwork_confirmation_requests WHERE id = ? AND purchase_order_id = ?',
                [rid, id]
            );
            if (!rows.length) throw new Fail(404, 'NOT_FOUND', `Artwork request ${rid} not found on this purchase order.`);
            const [upd] = await conn.query(
                `UPDATE artwork_confirmation_requests
                    SET revoked_at = UTC_TIMESTAMP(), revoke_reason = 'staff'
                  WHERE id = ? AND revoked_at IS NULL`,
                [rid]
            );
            if (upd.affectedRows) {
                await recordAudit(conn, {
                    entityType: 'purchase_order', entityId: id, action: 'artwork_confirmation_revoked',
                    before: null, after: { requestId: rid, reason: 'staff' }, userEmail: req.userEmail,
                });
            }
            return readPanel(conn, id);
        });
        res.json(panel);
    }));

    // ── POST /api/v1/purchase-orders/:id/artwork-confirmation/lines/:orderId/clear ──
    // Staff take a supplier's confirmation off a line (the artwork changed, or
    // it was ticked by mistake): artwork_confirmed_by_supplier back to NULL, and
    // any tick still waiting for the line dropped, so the supplier can confirm
    // again. The status is not moved back. Clearing a line that is not confirmed
    // is a no-op. → the panel.
    app.post('/api/v1/purchase-orders/:id(\\d+)/artwork-confirmation/lines/:orderId(\\d+)/clear', route('POST /purchase-orders/:id/artwork-confirmation/lines/:orderId/clear', async (req, res) => {
        refuseReadOnly(req);
        const id = Number(req.params.id);
        const orderId = Number(req.params.orderId);
        const panel = await withConnection(async (conn) => {
            await loadPo(conn, id);
            const [rows] = await conn.query(
                `SELECT id, artwork_confirmed_by_supplier FROM orders
                  WHERE id = ? AND purchase_order_id = ? AND deleted_at IS NULL`,
                [orderId, id]
            );
            if (!rows.length) throw new Fail(404, 'NOT_FOUND', `Line ${orderId} not found on this purchase order.`);
            const before = A.toYmd(rows[0].artwork_confirmed_by_supplier);
            let dropped = 0;
            try {
                const [upd] = await conn.query(
                    'UPDATE artwork_confirmations SET applied_at = UTC_TIMESTAMP() WHERE order_id = ? AND applied_at IS NULL',
                    [orderId]
                );
                dropped = upd.affectedRows || 0;
            } catch (e) {
                if (e.errno !== TABLE_MISSING) throw e;
            }
            if (before !== null) {
                await conn.query('UPDATE orders SET artwork_confirmed_by_supplier = NULL WHERE id = ?', [orderId]);
            }
            if (before !== null || dropped) {
                await recordAudit(conn, {
                    entityType: 'order', entityId: orderId, action: 'update',
                    before: { artworkConfirmedBySupplier: before },
                    after: { artworkConfirmedBySupplier: null, note: 'supplier artwork confirmation cleared' },
                    userEmail: req.userEmail,
                });
            }
            return readPanel(conn, id);
        });
        res.json(panel);
    }));
}

module.exports = { registerArtworkConfirmationRoutes };
