'use strict';

// "Send to supplier" on a recorded payment (docs/payment-proof-send.md): the
// database side and the send itself. The rules are in src/lib/payment-notices.js,
// the routes in src/services/payment-notice-routes.js.
//
// Two layers:
//   sendNotice / resolveSend / previewOf   what happens, in order, written
//       against a `store` so that every outcome of Front can be exercised
//       (tools/test-payment-notices-send.js);
//   createStore                            that store on MySQL: the notice's
//       own reads of a payment, the send log's guarded transitions.
//
// The send, in one line each:
//   1. the sendKey is looked up first: a retried click is answered from its row;
//   2. under the locks an edit of the payment takes, the rules are run and ONE
//      row is written as 'sending', then committed. That commit is the moment
//      the send exists: nothing is emailed that has no row;
//   3. outside any transaction: the recipients are pre-approved on the
//      portal. A failure that leaves nobody to show it to is a definite
//      'failed'; nothing is ever attached instead (the user, 2026-11-17);
//   4. Front is called once, and only while the row's deadline leaves room;
//   5. accepted -> 'sent' (one finaliser, also used when a person says "it
//      went"); refused -> 'failed'; no answer -> 'unknown', which is never
//      retried: a person checks Front and says which.

const log = require('../lib/logger');
const N = require('../lib/payment-notices');
const P = require('../lib/po-portal-email');
const R = require('../lib/email-receipt');
const { jportalSchema, isSourceMissing } = require('../lib/jportal');
const { wrapEmail } = require('../lib/email-layout');
const { preapprovePortalUsers } = require('./portal-access');

const ER_DUP_ENTRY = 1062;
const ER_NO_SUCH_TABLE = 1146;
const ER_BAD_FIELD = 1054;
const BIN = 'COLLATE utf8mb4_0900_bin';
const ISO = "'%Y-%m-%dT%H:%i:%sZ'";
const SEND_KEY_RE = /^[A-Za-z0-9_-]{8,64}$/;

const result = (status, body) => ({ status, body });
const refused = r => { const { status, ...body } = r; return result(status, body); };
const bad = error => result(400, { error, code: 'BAD_REQUEST' });

// {token} in a subject or body, from the payment itself. An unknown token is
// left as written, so a typo shows rather than blanking part of the email.
const fill = (text, tokens) => String(text == null ? '' : text).replace(/\{(\w+)\}/g, (m, k) => (tokens[k] != null ? String(tokens[k]) : m));
const oneLine = s => String(s || '').replace(/[\r\n]+/g, ' ').trim();

/** A send row as the API gives it. Its status is the one that holds NOW. */
function sendJson(row, now = new Date()) {
    if (!row) return null;
    return {
        id: row.id,
        kind: row.noticeKind,
        delivery: row.delivery,
        status: N.effectiveStatus(row, now),
        supplierName: row.supplierName,
        sentTo: row.sentTo || [],
        subject: row.subject || null,
        sentByEmail: row.sentByEmail || null,
        sentAt: row.sentAt || null,
        deadlineAt: row.deadlineAt || null,
        sendError: row.sendError || null,
        resolvedAt: row.resolvedAt || null,
        resolvedByEmail: row.resolvedByEmail || null,
    };
}

/**
 * What the send dialog shows for a transfer: its figures, its proofs, how it
 * goes out, and each notice with the wording to start from, where its links
 * lead, its state and its sends. `previewToken` goes back with the send.
 * `source` and `delivery` are the store's reads; `sends` the transfer's rows.
 */
function previewOf(source, delivery, sends = [], now = new Date()) {
    const built = N.buildNotices(source);
    // What is wrong with the payment itself first; then whether the portal can
    // show it to anyone. It goes out as a portal link or not at all.
    const blocked = built.blocked || N.portalRefusal(delivery, source.payment.supplierName);
    const states = N.noticeStates(built.notices.map(n => n.kind), sends, source.payment.supplierName, now);
    return {
        paymentId: source.payment.id,
        supplierName: source.payment.supplierName,
        sendable: !blocked,
        blocked,
        transfer: built.transfer,
        proofs: built.proofs,
        delivery: { delivery: delivery.delivery, reason: delivery.reason, portalSupplier: (delivery.suppliers || []).length > 0 },
        previewToken: N.previewToken(built, delivery.delivery),
        notices: built.notices.map((n, i) => ({
            kind: n.kind,
            amount: n.amount,
            lines: n.lines,
            paidFor: n.paidFor,
            unnamed: n.unnamed,
            blocked: n.blocked,
            templateKey: N.TEMPLATE_KEYS[n.kind],
            tokens: N.tokensFor(built, n),
            // null when it cannot be sent (resolveNoticeDelivery).
            portalUrl: delivery.portalUrl,
            // What the email asks for, with a button to it: a deposit's orders on the
            // portal, for their ready dates; a balance's containers, for their telex release.
            readyDates: N.readyDatesFor(n, delivery),
            telex: N.telexContainersOf(n),
            state: states[i].state,
            lastSentAt: states[i].lastSentAt,
            sends: sends.filter(s => s.noticeKind === n.kind).map(s => sendJson(s, now)),
        })),
    };
}

// What a recorded outcome answers a retried key with.
function replay(row, now) {
    const send = sendJson(row, now);
    switch (send.status) {
        case 'sent': return result(200, { send, delivery: row.delivery, portalAccess: null, replayed: true });
        case 'sending': return refused(N.refusal('SEND_IN_PROGRESS', { send }));
        case 'unknown': return refused(N.refusal('SEND_OUTCOME_UNKNOWN', { send }));
        default:
            return refused(N.refusal(String(row.sendError || '').startsWith('front') ? 'FRONT_REFUSED' : 'SEND_FAILED', { send, reason: row.sendError || null }));
    }
}

/**
 * Sends one notice of a transfer. ctx: { store, sendFront, now, apiBaseUrl }.
 * input: { paymentId, kind, actor: { email, userType }, body: { sendKey, to,
 * subject, body, delivery, previewToken } }. → { status, body }, never throws
 * for anything the sender can act on.
 */
async function sendNotice(ctx, { paymentId, kind, actor, body }) {
    const { store, sendFront } = ctx;
    const now = ctx.now || (() => new Date());
    const b = body || {};

    if (!N.maySend(actor && actor.userType)) return refused(N.refusal('CANNOT_SEND'));
    if (!N.NOTICE_KINDS.includes(kind)) return bad('The part of the payment to send must be "deposit" or "balance".');
    if (typeof b.sendKey !== 'string' || !SEND_KEY_RE.test(b.sendKey)) return bad('`sendKey` must be 8 to 64 letters, digits, - or _.');
    if (!N.DELIVERIES.includes(b.delivery)) return bad('`delivery` must be "portal": the way the dialog showed.');
    if (typeof b.previewToken !== 'string' || !b.previewToken) return bad('`previewToken` is required: what the dialog showed.');
    const { addresses, invalid } = P.parseRecipients(b.to);
    if (invalid.length) return bad(`Not an email address: ${invalid.join(', ')}`);
    if (!addresses.length) return bad('At least one recipient is required.');

    const requestHash = N.requestHash({ to: addresses, subject: b.subject, body: b.body, delivery: b.delivery, previewToken: b.previewToken });

    // 1. The key first: whatever the payment looks like now, a click already
    //    recorded is answered from its row.
    const seen = await store.findByKey(b.sendKey);
    if (seen) {
        if (seen.paymentId !== paymentId || seen.noticeKind !== kind || seen.requestHash !== requestHash) return refused(N.refusal('SEND_KEY_REUSED'));
        return replay(seen, now());
    }

    // Wording left blank falls back to the stored template for this notice.
    const wording = { subject: oneLine(b.subject), body: String(b.body || '').trim() };
    if (!wording.subject || !wording.body) {
        const tpl = await store.template(N.TEMPLATE_KEYS[kind]);
        if (!wording.subject) wording.subject = oneLine(tpl.subject);
        if (!wording.body) wording.body = tpl.bodyHtml || '';
    }

    // 2. Under the payment's locks: the rules, then the row.
    const decide = (source, delivery, sends) => {
        const built = N.buildNotices(source);
        if (built.blocked) return { refusal: built.blocked };
        const cannot = N.portalRefusal(delivery, source.payment.supplierName);
        if (cannot) return { refusal: cannot };
        const conflict = N.deliveryConflict(b.delivery);
        if (conflict) return { refusal: conflict };
        const notice = built.notices.find(n => n.kind === kind);
        if (!notice) return { refusal: N.refusal('NO_SUCH_NOTICE') };
        if (notice.blocked) return { refusal: notice.blocked };
        if (N.previewToken(built, delivery.delivery) !== b.previewToken) return { refusal: N.refusal('PAYMENT_CHANGED') };
        const at = now();
        for (const s of sends.filter(x => x.noticeKind === kind)) {
            const status = N.effectiveStatus(s, at);
            if (status === 'sending') return { refusal: N.refusal('SEND_IN_PROGRESS', { sendId: s.id }) };
            if (status === 'unknown') return { refusal: N.refusal('UNRESOLVED_SEND', { sendId: s.id }) };
        }
        const tokens = N.tokensFor(built, notice);
        return { built, notice, tokens, subject: oneLine(fill(wording.subject, tokens)).slice(0, 500), bodyHtml: fill(wording.body, tokens) };
    };
    const row = d => ({
        noticeKind: kind,
        delivery: N.DELIVERY,
        supplierName: d.built.supplierName,
        sendKey: b.sendKey,
        requestHash,
        deadlineAt: new Date(now().getTime() + N.DEADLINE_SECONDS * 1000).toISOString(),
        sentTo: addresses,
        subject: d.subject,
        sentByEmail: (actor && actor.email) || null,
    });
    const reserved = await store.reserve({ paymentId, decide, row });
    if (reserved.notFound) return result(404, { error: `Supplier payment ${paymentId} not found.`, code: 'NOT_FOUND' });
    if (reserved.refusal) return refused(reserved.refusal);
    if (reserved.duplicate) {
        const d = reserved.duplicate;
        if (d.paymentId !== paymentId || d.noticeKind !== kind || d.requestHash !== requestHash) return refused(N.refusal('SEND_KEY_REUSED'));
        return replay(d, now());
    }
    const { send, source, delivery, notice, subject, bodyHtml } = reserved;

    // 3. Outside any transaction. Whatever fails from here until Front is
    //    called is a definite "nothing was sent".
    const giveUp = async (reason, answer) => {
        await store.fail(send.id, String(reason).slice(0, 500));
        return answer;
    };
    let portalAccess = null;
    let html;
    try {
        try {
            portalAccess = await store.preapprove({
                recipients: addresses,
                // The portal lists the payment to users linked to a supplier
                // of exactly this name, so that name is all they are given.
                poSupplier: source.payment.supplierName,
                actorEmail: actor.email,
                note: N.preapprovalNote({ paymentId, kind, actor: actor.email }),
            });
        } catch (e) {
            log.error('[payment-notices] portal pre-approval failed', { paymentId, error: e.message, errno: e.errno });
            portalAccess = { status: 'error', suppliers: [], poSupplierMatched: false, users: [] };
        }
        // The portal has nobody to show it to after all (the supplier record or
        // the portal went between the check and here): nothing is sent, and
        // nothing is attached instead.
        if (portalAccess.status === 'no_supplier' || portalAccess.status === 'unavailable') {
            const reason = portalAccess.status === 'no_supplier' ? 'no_supplier' : 'portal_unavailable';
            return giveUp(`no portal delivery: ${reason}`, refused(N.portalRefusal({ delivery: null, reason }, source.payment.supplierName)));
        }
        const preapproved = portalAccess.status === 'ok' && portalAccess.users.every(u => P.GRANTED_OUTCOMES.has(u.outcome));
        const block = N.portalBlockHtml({
            kind,
            url: delivery.portalUrl,
            registerUrl: delivery.registerUrl,
            preapproved,
            readyDates: N.readyDatesFor(notice, delivery),
            telex: N.telexContainersOf(notice),
        });
        html = wrapEmail({ title: subject, bodyHtml: `${bodyHtml}${block}` });
    } catch (e) {
        log.error('[payment-notices] a send could not be prepared', { paymentId, sendId: send.id, error: e.message });
        return giveUp(`prepare: ${e.message}`, refused(N.refusal('SEND_FAILED', { send: sendJson({ ...send, status: 'failed' }, now()) })));
    }

    // 4. Front, once, and only while the deadline leaves room for the call to
    //    end inside it: past the deadline a person may say what happened.
    if (!N.mayCallFront(send.deadlineAt, now())) {
        return giveUp('deadline: too little time left to call Front', refused(N.refusal('SEND_FAILED', { send: sendJson({ ...send, status: 'failed' }, now()) })));
    }
    let front;
    try {
        front = await sendFront({ to: addresses, subject, html });
    } catch (e) {
        front = { ok: false, reason: 'network_error', detail: e && e.message };
    }

    // 5. What Front said.
    const current = async fallback => sendJson((await store.getSend(send.id).catch(() => null)) || fallback, now());
    if (!front || !front.ok) {
        const reason = (front && front.reason) || 'network_error';
        if (reason === 'network_error') {
            // It may have gone. Its receipt is kept for whoever says it did.
            await store.unknown(send.id, 'front: no answer').catch(e => log.error('[payment-notices] an unknown outcome was not recorded', { sendId: send.id, error: e.message }));
            return refused(N.refusal('SEND_OUTCOME_UNKNOWN', { send: await current({ ...send, status: 'unknown' }) }));
        }
        const said = front.status != null ? `${reason}: ${front.status}` : reason;
        return giveUp(said, refused(N.refusal('FRONT_REFUSED', { frontStatus: front.status ?? null, reason, send: sendJson({ ...send, status: 'failed', sendError: said }, now()) })));
    }
    let done;
    try {
        done = await store.finalise(send.id, { front, byExecutor: true });
    } catch (e) {
        log.error('[payment-notices] an email went out but could not be recorded', { paymentId, sendId: send.id, error: e.message });
        await store.unknown(send.id, 'sent, not recorded').catch(() => {});
        return refused(N.refusal('SEND_OUTCOME_UNKNOWN', { send: await current({ ...send, status: 'sending' }) }));
    }
    if (!done.ok && !(done.send && done.send.status === 'sent')) {
        return refused(N.refusal('SEND_OUTCOME_UNKNOWN', { send: sendJson(done.send || send, now()) }));
    }
    await store.logReceipt({
        emailType: N.EMAIL_TYPE, sentTo: addresses, subject, confirmVia: 'portal', actionUrl: delivery.portalUrl,
        sendTable: N.SEND_TABLE, sendId: send.id,
    });
    return result(200, { send: sendJson(done.send, now()), delivery: N.DELIVERY, portalAccess, replayed: false });
}

/**
 * A person says what became of a send whose outcome is unknown: 'sent' (it is
 * in Front) or 'not_sent'. Only an unknown send: one still going cannot be
 * declared dead, one that went or failed is already settled.
 */
async function resolveSend(ctx, { sendId, outcome, actor }) {
    const { store } = ctx;
    const now = ctx.now || (() => new Date());
    if (!N.maySend(actor && actor.userType)) return refused(N.refusal('CANNOT_SEND'));
    if (!['sent', 'not_sent'].includes(outcome)) return bad('`outcome` must be "sent" or "not_sent".');
    const row = await store.getSend(sendId);
    if (!row) return result(404, { error: `Send ${sendId} not found.`, code: 'NOT_FOUND' });
    if (N.effectiveStatus(row, now()) !== 'unknown') return refused(N.refusal('NOT_UNKNOWN', { send: sendJson(row, now()) }));
    if (outcome === 'sent') {
        const done = await store.finalise(sendId, { resolvedBy: actor.email });
        if (!done.ok) return refused(N.refusal('NOT_UNKNOWN', { send: sendJson(done.send, now()) }));
        if (row.delivery === 'portal') {
            await store.logReceipt({
                emailType: N.EMAIL_TYPE, sentTo: row.sentTo, subject: row.subject, confirmVia: 'portal',
                actionUrl: row.portalUrl || null, sendTable: N.SEND_TABLE, sendId,
            });
        }
        return result(200, { send: sendJson(done.send, now()) });
    }
    const done = await store.resolveNotSent(sendId, { resolvedBy: actor.email });
    if (!done.ok) return refused(N.refusal('NOT_UNKNOWN', { send: sendJson(done.send, now()) }));
    if (row.receiptToken) await store.discardReceipt(row.receiptToken);
    return result(200, { send: sendJson(done.send, now()) });
}

// ── The store on MySQL ────────────────────────────────────────────────────

const SEND_COLUMNS = `s.*,
        DATE_FORMAT(s.deadline_at, ${ISO}) AS deadline_at_iso,
        DATE_FORMAT(s.sent_at, ${ISO}) AS sent_at_iso,
        DATE_FORMAT(s.resolved_at, ${ISO}) AS resolved_at_iso`;

function jsonList(v) {
    if (Array.isArray(v)) return v;
    if (typeof v !== 'string') return [];
    try { const p = JSON.parse(v); return Array.isArray(p) ? p : []; } catch { return []; }
}

const sendRow = (r, env) => ({
    id: Number(r.id),
    paymentId: Number(r.supplier_payment_id),
    noticeKind: r.notice_kind,
    delivery: r.delivery,
    supplierName: r.supplier_name,
    sendKey: r.send_key,
    requestHash: r.request_hash,
    status: r.status,
    deadlineAt: r.deadline_at_iso || null,
    sentTo: jsonList(r.sent_to),
    subject: r.subject || null,
    receiptToken: r.receipt_token || null,
    frontMessageUid: r.front_message_uid || null,
    sendError: r.send_error || null,
    sentByEmail: r.sent_by_email || null,
    sentAt: r.sent_at_iso || null,
    resolvedAt: r.resolved_at_iso || null,
    resolvedByEmail: r.resolved_by_email || null,
    // Where a portal send's reminder button leads: the payment's own page.
    portalUrl: r.delivery === 'portal' && P.portalBaseOf(env) ? N.paymentPortalUrl(P.portalBaseOf(env), r.supplier_payment_id) : null,
});

// 'YYYY-MM-DD HH:MM:SS' (UTC) for a DATETIME column, from an ISO instant.
const sqlUtc = iso => new Date(iso).toISOString().slice(0, 19).replace('T', ' ');

// A pi line's paid_with_json as mysql2 hands it back: parsed, or text.
function paidWithOf(v) {
    const list = jsonList(v);
    const ok = list.filter(e => e && typeof e.shipmentReference === 'string' && Number.isFinite(Number(e.amount)))
        .map(e => ({ shipmentReference: e.shipmentReference, amount: Number(e.amount) }));
    return ok.length ? ok : null;
}

// What a line points at, or null when that is deleted or gone: then the
// payment cannot be sent until it is edited (LINE_TARGET_MISSING).
function targetOf(r) {
    const kind = r.target_kind;
    const hasPo = r.po_id != null;
    const present = kind === 'po_deposit' ? hasPo
        : kind === 'pi' ? (r.pip_id != null && hasPo)
            : kind === 'balance' ? r.bal_id != null
                : kind === 'extra' ? r.ex_id != null
                    : (r.order_id != null && hasPo);   // qc, repackaged
    if (!present) return null;
    return {
        paymentType: r.payment_type || null,
        poId: hasPo ? Number(r.po_id) : null,
        poNumber: r.po_number || null,
        poSupplier: r.po_supplier ?? null,
        shipmentId: r.shipment_id != null ? Number(r.shipment_id) : null,
        shipmentReference: r.shipment_reference || null,
        // The record that ties the container to a supplier: the balance
        // record, or the extra itself.
        shipmentSupplier: kind === 'balance' ? (r.bal_supplier ?? null) : (r.ex_shipment_id != null ? (r.extra_supplier ?? null) : null),
        invoiceNumber: r.invoice_number || null,
        topUp: r.bal_source === 'top_up',
        jfCode: r.jf_code || null,
        extraKind: r.extra_kind || null,
        ridesWith: r.rides_with || null,
        extraSupplier: r.extra_supplier ?? null,
    };
}

// The lines of one transfer with what each points at: the same joins as the
// portal's own view of a line (2026-11-16_21 portal_payment_lines_v), so the
// dialog and the portal cannot disagree about what a line is.
const linesSql = ({ paidWith = true, extras = true } = {}) => `
    SELECT l.id, l.target_kind, l.target_id, l.amount, ${paidWith ? 'l.paid_with_json' : 'NULL AS paid_with_json'},
           pip.id AS pip_id, pip.payment_type,
           po.id AS po_id, po.po_number, po.supplier AS po_supplier,
           o.id AS order_id, o.jf_code,
           ${extras ? 'COALESCE(bal.shipment_id, ex.shipment_id)' : 'bal.shipment_id'} AS shipment_id,
           ${extras ? 'COALESCE(sh.reference, bal.shipment_reference, ex.shipment_reference)' : 'COALESCE(sh.reference, bal.shipment_reference)'} AS shipment_reference,
           bal.id AS bal_id, bal.supplier_name AS bal_supplier, bal.invoice_number, bal.source AS bal_source,
           ${extras ? 'ex.id AS ex_id, ex.kind AS extra_kind, ex.rides_with, ex.supplier_name AS extra_supplier, ex.shipment_id AS ex_shipment_id'
        : 'NULL AS ex_id, NULL AS extra_kind, NULL AS rides_with, NULL AS extra_supplier, NULL AS ex_shipment_id'}
      FROM supplier_payment_lines l
      LEFT JOIN purchase_order_invoice_payments pip
             ON l.target_kind = 'pi' AND pip.id = l.target_id
            AND EXISTS (SELECT 1 FROM purchase_order_invoices i
                         WHERE i.id = pip.purchase_order_invoice_id AND i.deleted_at IS NULL)
      LEFT JOIN orders o
             ON l.target_kind IN ('qc', 'repackaged') AND o.id = l.target_id AND o.deleted_at IS NULL
      LEFT JOIN shipment_payments bal
             ON l.target_kind = 'balance' AND bal.id = l.target_id AND bal.deleted_at IS NULL
      ${extras ? `LEFT JOIN payment_extras ex
             ON l.target_kind = 'extra' AND ex.id = l.target_id AND ex.deleted_at IS NULL` : ''}
      LEFT JOIN purchase_orders po
             ON po.deleted_at IS NULL
            AND po.id = CASE l.target_kind
                          WHEN 'po_deposit' THEN l.target_id
                          WHEN 'pi'         THEN pip.purchase_order_id
                          WHEN 'qc'         THEN o.purchase_order_id
                          WHEN 'repackaged' THEN o.purchase_order_id
                          ${extras ? "WHEN 'extra'      THEN ex.purchase_order_id" : ''}
                        END
      LEFT JOIN shipments sh ON sh.id = ${extras ? 'COALESCE(bal.shipment_id, ex.shipment_id)' : 'bal.shipment_id'}
     WHERE l.payment_id = ?
     ORDER BY l.id`;

/**
 * A live transfer as the notice rules read it: { payment, lines, documents },
 * or null. Its own reads, not the API's line JSON: that one carries no
 * supplier of a line's target and no JF code. Never the note or the bank
 * reference.
 */
async function loadNoticeSource(conn, paymentId) {
    const [pays] = await conn.query(
        `SELECT id, supplier_name, supplier_key, amount, currency, DATE_FORMAT(paid_on, '%Y-%m-%d') AS paid_on
           FROM supplier_payments WHERE id = ? AND deleted_at IS NULL`,
        [paymentId]
    );
    if (!pays.length) return null;
    const p = pays[0];
    // A server from before paid_with_json (2026-10-19_10) or payment_extras
    // (2026-09-29_10) still answers, with less.
    const shape = { paidWith: true, extras: true };
    let rows;
    for (;;) {
        try {
            [rows] = await conn.query(linesSql(shape), [paymentId]);
            break;
        } catch (e) {
            if (e.errno === ER_BAD_FIELD && shape.paidWith) shape.paidWith = false;
            else if (e.errno === ER_NO_SUCH_TABLE && shape.extras) shape.extras = false;
            else throw e;
        }
    }
    const [docs] = await conn.query(
        `SELECT id, doc_kind, supplier_name, filename, public_url, s3_key, content_type, file_size
           FROM shipment_payment_documents WHERE deleted_at IS NULL AND supplier_payment_id = ? ORDER BY id`,
        [paymentId]
    );
    return {
        payment: {
            id: Number(p.id), supplierName: p.supplier_name, supplierKey: p.supplier_key,
            amount: Number(p.amount), currency: p.currency, paidOn: p.paid_on ? String(p.paid_on).slice(0, 10) : null,
        },
        lines: rows.map(r => ({
            id: Number(r.id), kind: r.target_kind, targetId: Number(r.target_id), amount: Number(r.amount),
            paidWith: paidWithOf(r.paid_with_json), target: targetOf(r),
        })),
        documents: docs.map(d => ({
            id: Number(d.id), docKind: d.doc_kind, supplierName: d.supplier_name ?? null, filename: d.filename,
            url: d.public_url || null, s3Key: d.s3_key || null, contentType: d.content_type || null, fileSize: Number(d.file_size) || 0,
        })),
    };
}

/**
 * Which way a transfer's notices go out now: { delivery, reason, portalBase,
 * portalUrl, registerUrl, suppliers }. `suppliers` are the live rows named
 * EXACTLY as the transfer's supplier: who the portal shows the payment to and
 * who its recipients are linked to. The portal is "there" when its own
 * payment_views table is: the table its Payments page came with.
 */
async function resolveNoticeDelivery(conn, payment, env = process.env) {
    const portalBase = P.portalBaseOf(env);
    const [suppliers] = await conn.query(
        `SELECT s.id, s.name
           FROM suppliers s
          WHERE s.deleted_at IS NULL
            AND CONVERT(s.name USING utf8mb4) ${BIN} = CONVERT(? USING utf8mb4) ${BIN}
          ORDER BY s.id`,
        [String(payment.supplierName)]
    );
    let portalAvailable = true;
    try {
        await conn.query(`SELECT 1 AS ok FROM ${jportalSchema(env)}.payment_views LIMIT 1`);
    } catch (e) {
        if (!isSourceMissing(e)) throw e;
        portalAvailable = false;
    }
    const out = N.deliveryFor({ portalBase, switchOn: N.portalSwitchOn(env), portalAvailable, supplierMatched: suppliers.length > 0 });
    const portal = out.delivery === 'portal';
    return {
        ...out,
        portalBase,
        portalUrl: portal ? N.paymentPortalUrl(portalBase, payment.id) : null,
        registerUrl: portal ? P.registerUrl(portalBase) : null,
        suppliers: suppliers.map(s => ({ id: Number(s.id), name: s.name })),
    };
}

/** The sends of these transfers, oldest first. null where the table is not migrated. */
async function loadSends(conn, paymentIds, env = process.env) {
    if (!paymentIds.length) return [];
    try {
        const [rows] = await conn.query(
            `SELECT ${SEND_COLUMNS}
               FROM supplier_payment_sends s WHERE s.supplier_payment_id IN (${paymentIds.map(() => '?').join(', ')})
              ORDER BY s.id`,
            paymentIds
        );
        return rows.map(r => sendRow(r, env));
    } catch (e) {
        if (e.errno !== ER_NO_SUCH_TABLE) throw e;
        return null;
    }
}

/**
 * The store sendNotice / resolveSend work against, on MySQL. deps:
 * { withConnection, lockSupplierPaymentTargets, getEmailTemplate, recordAudit,
 *   preapprove?, env? }.
 */
function createStore(deps) {
    const { withConnection, lockSupplierPaymentTargets, getEmailTemplate, recordAudit } = deps;
    const env = deps.env || process.env;
    const preapprove = deps.preapprove || preapprovePortalUsers;
    const jp = () => jportalSchema(env);

    const readSend = async (conn, where, params) => {
        const [rows] = await conn.query(`SELECT ${SEND_COLUMNS} FROM supplier_payment_sends s WHERE ${where}`, params);
        return rows.length ? sendRow(rows[0], env) : null;
    };
    const guardedUpdate = (sql, params) => withConnection(async conn => {
        const [r] = await conn.query(sql, params);
        return r.affectedRows === 1;
    });
    // A person may resolve only what is unknown: said so, or left 'sending' past its deadline.
    const UNKNOWN_NOW = `(status = 'unknown' OR (status = 'sending' AND deadline_at < UTC_TIMESTAMP()))`;

    const store = {
        findByKey: key => withConnection(conn => readSend(conn, 's.send_key = ?', [key])),
        getSend: id => withConnection(conn => readSend(conn, 's.id = ?', [id])),
        template: key => withConnection(conn => getEmailTemplate(conn, key)),
        preapprove: args => withConnection(conn => preapprove(conn, args)),
        // For a send from before 2026-11-17 that went out with the proof attached and is
        // being resolved "not sent": its receipt (with a confirm button) is taken back.
        discardReceipt: token => withConnection(conn => R.discardEmailReceipt(conn, token)),
        logReceipt: fields => withConnection(conn => R.logSentEmail(conn, fields)),

        // Who opened the payment's proof on the portal (jportal.payment_views:
        // the portal records a view for the proof and for nothing else). A
        // server without the portal answers as nobody.
        views: paymentId => withConnection(async conn => {
            try {
                const [rows] = await conn.query(
                    `SELECT v.user_email, DATE_FORMAT(v.first_viewed_at, ${ISO}) AS first_iso,
                            DATE_FORMAT(v.last_viewed_at, ${ISO}) AS last_iso, v.view_count
                       FROM ${jp()}.payment_views v WHERE v.supplier_payment_id = ?
                      ORDER BY v.first_viewed_at, v.user_email`,
                    [paymentId]
                );
                return rows.map(r => ({ email: r.user_email, firstViewedAt: r.first_iso, lastViewedAt: r.last_iso, count: Number(r.view_count) || 0 }));
            } catch (e) {
                if (!isSourceMissing(e)) throw e;
                return [];
            }
        }),

        // What the dialog is built from: no lock, nothing written.
        forPreview: paymentId => withConnection(async conn => {
            const source = await loadNoticeSource(conn, paymentId);
            if (!source) return null;
            const delivery = await resolveNoticeDelivery(conn, source.payment, env);
            const sends = await loadSends(conn, [paymentId], env);
            return { source, delivery, sends: sends || [], migrated: sends !== null };
        }),

        /**
         * Step 2 of the send. Locks the payment and what its lines point at,
         * the way PUT /supplier-payments/:id does, reads the payment again
         * under the lock (starting over once if a line appeared that the lock
         * did not cover), asks `decide`, and inserts the row `row(decision)`
         * describes as 'sending'.
         * → { send, source, delivery, ...decision } | { refusal } | { notFound } | { duplicate }
         */
        reserve: ({ paymentId, decide, row }) => withConnection(async conn => {
            let planned = await loadNoticeSource(conn, paymentId);
            if (!planned) return { notFound: true };
            const keyOf = l => `${l.kind}:${l.targetId}`;
            for (let attempt = 1; ; attempt += 1) {
                const lockLines = planned.lines.map(l => ({ kind: l.kind, id: l.targetId }));
                await conn.beginTransaction();
                try {
                    const lock = await lockSupplierPaymentTargets(conn, { paymentId, lines: lockLines });
                    if (!lock.paymentLive) { await conn.rollback(); return { notFound: true }; }
                    const source = await loadNoticeSource(conn, paymentId);
                    if (!source) { await conn.rollback(); return { notFound: true }; }
                    const locked = new Set(planned.lines.map(keyOf));
                    if (!source.lines.every(l => locked.has(keyOf(l)))) {
                        await conn.rollback();
                        if (attempt >= 2) return { refusal: N.refusal('PAYMENT_CHANGED') };
                        planned = source;
                        continue;
                    }
                    const delivery = await resolveNoticeDelivery(conn, source.payment, env);
                    const sends = (await loadSends(conn, [paymentId], env)) || [];
                    const decision = decide(source, delivery, sends);
                    if (decision.refusal) { await conn.rollback(); return { refusal: decision.refusal }; }
                    const f = row(decision);
                    let insertId;
                    try {
                        const [r] = await conn.query(
                            `INSERT INTO supplier_payment_sends
                                    (supplier_payment_id, notice_kind, delivery, supplier_name, send_key, request_hash, status, deadline_at, sent_to, subject, sent_by_email)
                             VALUES (?, ?, ?, ?, ?, ?, 'sending', ?, ?, ?, ?)`,
                            [paymentId, f.noticeKind, f.delivery, f.supplierName, f.sendKey, f.requestHash, sqlUtc(f.deadlineAt), JSON.stringify(f.sentTo), f.subject, f.sentByEmail]
                        );
                        insertId = r.insertId;
                    } catch (e) {
                        if (e.errno !== ER_DUP_ENTRY) throw e;
                        // The same key, a moment earlier: that request owns the send.
                        await conn.rollback();
                        return { duplicate: await readSend(conn, 's.send_key = ?', [f.sendKey]) };
                    }
                    await conn.commit();
                    const send = await readSend(conn, 's.id = ?', [insertId]);
                    return { send: send || { id: insertId, paymentId, status: 'sending', ...f }, source, delivery, ...decision };
                } catch (e) {
                    await conn.rollback().catch(() => {});
                    throw e;
                }
            }
        }),

        fail: (id, reason) => guardedUpdate(
            `UPDATE supplier_payment_sends SET status = 'failed', send_error = ? WHERE id = ? AND status = 'sending'`, [reason, id]),
        unknown: (id, reason) => guardedUpdate(
            `UPDATE supplier_payment_sends SET status = 'unknown', send_error = ? WHERE id = ? AND status = 'sending'`, [reason, id]),

        /**
         * The one way a send becomes 'sent': by the request that holds Front's
         * answer (`byExecutor`, with `front`), or by a person (`resolvedBy`).
         * One transaction: the guarded UPDATE, an attached send's receipt
         * pointed at the row, the audit. → { ok, send }.
         *
         * The request's own answer outranks a person's "it did not go": it is
         * Front's. A person never outranks anything: only an unknown send.
         */
        finalise: (id, { front = null, byExecutor = false, resolvedBy = null } = {}) => withConnection(async conn => {
            await conn.beginTransaction();
            try {
                const [r] = byExecutor
                    ? await conn.query(
                        `UPDATE supplier_payment_sends
                            SET status = 'sent', sent_at = COALESCE(sent_at, UTC_TIMESTAMP()), front_message_uid = ?, front_conversation_id = ?,
                                send_error = NULL, resolved_at = NULL, resolved_by_email = NULL
                          WHERE id = ? AND (status IN ('sending', 'unknown') OR (? = 1 AND status = 'failed' AND resolved_at IS NOT NULL))`,
                        [(front && front.frontMessageUid) || null, (front && front.frontConversationId) || null, id, 1]
                    )
                    : await conn.query(
                        `UPDATE supplier_payment_sends
                            SET status = 'sent', sent_at = COALESCE(sent_at, deadline_at - INTERVAL ${N.DEADLINE_SECONDS} SECOND),
                                send_error = NULL, resolved_at = UTC_TIMESTAMP(), resolved_by_email = ?
                          WHERE id = ? AND ${UNKNOWN_NOW}`,
                        [resolvedBy, id]
                    );
                if (r.affectedRows !== 1) {
                    await conn.rollback();
                    return { ok: false, send: await readSend(conn, 's.id = ?', [id]) };
                }
                const send = await readSend(conn, 's.id = ?', [id]);
                if (send.receiptToken) {
                    await conn.query(`UPDATE email_receipts SET send_table = ?, send_id = ? WHERE token = ?`, [N.SEND_TABLE, id, send.receiptToken]);
                }
                await recordAudit(conn, {
                    entityType: 'supplier_payment', entityId: send.paymentId, action: resolvedBy ? 'notice_resolved' : 'notice_sent', before: null,
                    after: { sendId: id, kind: send.noticeKind, delivery: send.delivery, to: send.sentTo, frontMessageUid: send.frontMessageUid, ...(resolvedBy ? { outcome: 'sent' } : {}) },
                    userEmail: resolvedBy || send.sentByEmail,
                });
                await conn.commit();
                return { ok: true, send };
            } catch (e) {
                await conn.rollback().catch(() => {});
                throw e;
            }
        }),

        resolveNotSent: (id, { resolvedBy }) => withConnection(async conn => {
            const [r] = await conn.query(
                `UPDATE supplier_payment_sends
                    SET status = 'failed', send_error = 'resolved: not sent', resolved_at = UTC_TIMESTAMP(), resolved_by_email = ?
                  WHERE id = ? AND ${UNKNOWN_NOW}`,
                [resolvedBy, id]
            );
            const send = await readSend(conn, 's.id = ?', [id]);
            if (r.affectedRows !== 1) return { ok: false, send };
            await recordAudit(conn, {
                entityType: 'supplier_payment', entityId: send.paymentId, action: 'notice_resolved', before: null,
                after: { sendId: id, kind: send.noticeKind, outcome: 'not_sent' }, userEmail: resolvedBy,
            });
            return { ok: true, send };
        }),
    };
    return store;
}

/**
 * `notices` for each transfer GET /api/v1/supplier-payments lists:
 * Map(payment id -> [{ kind, state, lastSentAt, delivery }]), or null where
 * the send log is not migrated (ShipLine then hides the feature). `payments`
 * are the API's own transfer JSON.
 */
async function noticeSummaries(conn, payments, env = process.env, now = new Date()) {
    const sends = await loadSends(conn, payments.map(p => p.id), env);
    if (sends === null) return null;
    const byPayment = new Map();
    for (const s of sends) {
        if (!byPayment.has(s.paymentId)) byPayment.set(s.paymentId, []);
        byPayment.get(s.paymentId).push(s);
    }
    const out = new Map();
    for (const p of payments) out.set(p.id, N.noticeStates(N.noticeKindsOf(p.lines), byPayment.get(p.id) || [], p.supplierName, now));
    return out;
}

/**
 * Whether a manual reminder of a payment email must be refused: the payment
 * has been deleted, or is no longer the supplier's the email went to (the
 * reminder would point them at something they cannot open, or at someone
 * else's payment). null when it may go. `receipt` needs send_table / send_id.
 */
async function reminderRefusal(conn, receipt) {
    if (!receipt || receipt.send_table !== N.SEND_TABLE || !receipt.send_id) return null;
    let rows;
    try {
        [rows] = await conn.query(
            `SELECT (sp.id IS NOT NULL
                     AND CONVERT(sp.supplier_name USING utf8mb4) ${BIN} = CONVERT(s.supplier_name USING utf8mb4) ${BIN}) AS current
               FROM supplier_payment_sends s
               LEFT JOIN supplier_payments sp ON sp.id = s.supplier_payment_id AND sp.deleted_at IS NULL
              WHERE s.id = ?`,
            [receipt.send_id]
        );
    } catch (e) {
        if (e.errno !== ER_NO_SUCH_TABLE) throw e;
        return null;
    }
    if (!rows.length || Number(rows[0].current) !== 1) return N.refusal('PAYMENT_NOT_CURRENT');
    return null;
}

module.exports = {
    sendNotice, resolveSend, previewOf, sendJson,
    createStore, loadNoticeSource, resolveNoticeDelivery, loadSends,
    noticeSummaries, reminderRefusal,
};
