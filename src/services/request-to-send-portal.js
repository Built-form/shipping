'use strict';

// The database side of sending a Request to send as a link to the supplier
// portal (docs/request-to-send-portal.md). The rules are in
// src/lib/request-to-send-portal.js; pre-approving the recipients is the PO
// send's own code (src/services/portal-access.js).
//
// draft_container_document_sends gained delivery / request_etd / request_port
// (2026-11-13_10). Every statement here that names them answers a server where
// that migration has not run yet (1054) as if they were empty, so a send is
// never lost to it.

const log = require('../lib/logger');
const { jportalSchema, isSourceMissing } = require('../lib/jportal');
const { wrapEmail } = require('../lib/email-layout');
const P = require('../lib/po-portal-email');
const L = require('../lib/request-to-send-portal');
const { preapprovePortalUsers } = require('./portal-access');

const ER_BAD_FIELD_ERROR = 1054;

const NONE = { portalUrl: null, registerUrl: null, suppliers: [] };

/**
 * Which way `doc` (a draft_container_documents row: id, type, supplier) goes
 * out now: { delivery, reason, portalUrl, registerUrl, suppliers }. `suppliers`
 * are the live rows named EXACTLY as the document's supplier, the ones the
 * portal shows it to and its recipients are linked to. An attached delivery
 * carries no link. Asked when the send dialog opens (the preview route) and
 * again when the email is sent.
 */
async function resolveDelivery(conn, doc, env = process.env) {
    const portalBase = P.portalBaseOf(env);
    const base = { docType: doc.type, supplier: doc.supplier, portalBase };
    // Nothing to look up for a document that never goes to the portal.
    const early = L.deliveryFor({ ...base, supplierMatched: true, portalAvailable: true });
    if (early.delivery !== 'portal') return { ...early, ...NONE };

    const BIN = 'COLLATE utf8mb4_0900_bin';
    const [suppliers] = await conn.query(
        `SELECT s.id, s.name
           FROM suppliers s
          WHERE s.deleted_at IS NULL
            AND CONVERT(s.name USING utf8mb4) ${BIN} = CONVERT(? USING utf8mb4) ${BIN}
          ORDER BY s.id`,
        [String(doc.supplier)]
    );

    let portalAvailable = true;
    try {
        await conn.query(`SELECT 1 AS ok FROM ${jportalSchema(env)}.supplier_users LIMIT 1`);
    } catch (e) {
        if (!isSourceMissing(e)) throw e;
        portalAvailable = false;
    }

    const out = L.deliveryFor({ ...base, supplierMatched: suppliers.length > 0, portalAvailable });
    if (out.delivery !== 'portal') return { ...out, ...NONE };
    return {
        ...out,
        portalUrl: L.requestPortalUrl(portalBase, doc.id),
        registerUrl: P.registerUrl(portalBase),
        suppliers: suppliers.map(s => ({ id: Number(s.id), name: s.name })),
    };
}

/**
 * Everything a portal delivery does before Front is called: pre-approves the
 * recipients on the portal for the document's supplier (the PO send's own
 * rules, in their own committed transaction, so the account is there by the
 * time the supplier follows the link), and builds the email: the body, the
 * portal block under it, inside the portal emails' frame. No "Confirm
 * receipt" button: opening the request on the portal is what confirms it.
 *
 * `plan` is resolveDelivery's answer (delivery 'portal'); `fields` is
 * parseSentFields' ({ etd, port }).
 * → { portalAccess, preapproved, emailHtml }, or { conflict } (a 409 body)
 *   when pre-approval found that the supplier row or the portal has gone since
 *   the plan was made: nobody could open the link, so nothing is sent as one.
 * A pre-approval that FAILS never stops the email: it goes out as a link
 * without the "already approved" wording, and `portalAccess.status` is 'error'.
 */
async function preparePortalSend(conn, { doc, plan, recipients, fields, bodyHtml, subject, actorEmail, update = null }, { preapprove = preapprovePortalUsers, logError = (m, meta) => log.error(m, meta) } = {}) {
    let portalAccess;
    try {
        portalAccess = await preapprove(conn, {
            recipients,
            // The portal lists the request to users linked to a supplier of
            // exactly this name, so that name is all the recipients are given.
            poSupplier: doc.supplier,
            actorEmail,
            note: L.preapprovalNote({ ref: doc.draft_container_name, version: doc.version, actor: actorEmail }),
        });
    } catch (e) {
        logError('[draft-container-documents/email] portal pre-approval failed', { documentId: doc.id, error: e.message, errno: e.errno });
        portalAccess = { status: 'error', suppliers: [], poSupplierMatched: false, users: [] };
    }
    if (portalAccess.status === 'no_supplier' || portalAccess.status === 'unavailable') {
        return {
            conflict: L.deliveryConflict('portal', {
                delivery: 'attached',
                reason: portalAccess.status === 'no_supplier' ? 'no_supplier' : 'portal_unavailable',
            }),
        };
    }
    const preapproved = portalAccess.status === 'ok'
        && portalAccess.users.every(u => P.GRANTED_OUTCOMES.has(u.outcome));
    const block = L.requestPortalBlockHtml({
        version: doc.version,
        etd: fields && fields.etd,
        port: fields && fields.port,
        url: plan.portalUrl,
        registerUrl: plan.registerUrl,
        preapproved,
    });
    // An update says so first: that it replaces the earlier version and what
    // changed, above the way to the portal.
    const updateBlock = update ? L.updateBlockHtml({ version: doc.version, ...update }) : '';
    return { portalAccess, preapproved, emailHtml: wrapEmail({ title: subject, bodyHtml: `${bodyHtml || ''}${updateBlock}${block}` }) };
}

/**
 * Whether sending `doc` (id, type, version, supplier, draft_container_name,
 * shipment_id, lines_json) is an UPDATE: an earlier version of the same request
 * has already been emailed. → L.requestUpdate's answer ({ replacesVersion,
 * changes }), or null for the first version anyone was sent, and for anything
 * that is not a Request to send.
 *
 * "The same request" is the same supplier's request for the same container:
 * the same name, or (a draft that was renamed, or booked since) the same
 * shipment. A version that was generated and never emailed replaces nothing:
 * the supplier never had it.
 */
async function resolveUpdate(conn, doc) {
    if (doc.type !== 'supplier-quote') return null;
    const [rows] = await conn.query(
        `SELECT p.id, p.version, p.lines_json
           FROM draft_container_documents p
          WHERE p.type = 'supplier-quote' AND p.deleted_at IS NULL
            AND p.id <> ? AND p.version < ?
            AND p.supplier <=> ?
            AND (p.draft_container_name = ? OR (p.shipment_id IS NOT NULL AND p.shipment_id = ?))
            AND EXISTS (SELECT 1 FROM draft_container_document_sends s WHERE s.draft_container_document_id = p.id)
          ORDER BY p.version DESC, p.id DESC
          LIMIT 1`,
        [doc.id, doc.version, doc.supplier ?? null, doc.draft_container_name, doc.shipment_id ?? null]
    );
    return L.requestUpdate(rows[0] || null, doc);
}

/**
 * Keeps what an update's email said on its send row (request_update, JSON:
 * { replacesVersion, changes }), which the portal shows beside the PDF. Not an
 * update: nothing is written. A table without the column yet (2026-11-13_30):
 * the send stands without it.
 */
async function saveSendUpdate(conn, sendId, update) {
    if (!update || !sendId) return;
    try {
        await conn.query(
            'UPDATE draft_container_document_sends SET request_update = ? WHERE id = ?',
            [JSON.stringify(update), sendId]
        );
    } catch (e) {
        if (e.errno !== ER_BAD_FIELD_ERROR) throw e;
    }
}

/** Map(sendId -> { replacesVersion, changes }) for the sends of these documents that were updates. */
async function loadSendUpdates(conn, documentIds) {
    const out = new Map();
    const ids = [...new Set((documentIds || []).filter(Boolean))];
    if (!ids.length) return out;
    let rows;
    try {
        [rows] = await conn.query(
            `SELECT id, request_update FROM draft_container_document_sends
              WHERE draft_container_document_id IN (${ids.map(() => '?').join(',')})`,
            ids
        );
    } catch (e) {
        if (e.errno === ER_BAD_FIELD_ERROR) return out;
        throw e;
    }
    for (const r of rows) {
        const update = L.storedUpdate(r.request_update);
        if (update) out.set(Number(r.id), update);
    }
    return out;
}

/**
 * Writes one send row and returns its id: how it went out (`delivery`) and what
 * the email said about the departure (`etd` 'YYYY-MM-DD', `port`), which the
 * portal shows beside the PDF.
 */
async function insertSend(conn, { documentId, sentTo, subject, frontMessageUid, frontConversationId, sentByEmail, delivery, etd, port }) {
    const common = [
        documentId, JSON.stringify(sentTo), subject, frontMessageUid || null, frontConversationId || null, sentByEmail || null,
    ];
    try {
        const [ins] = await conn.query(
            `INSERT INTO draft_container_document_sends
                (draft_container_document_id, sent_to, subject, front_message_uid, front_conversation_id, sent_by_email, delivery, request_etd, request_port)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            [...common, delivery === 'portal' ? 'portal' : 'attached', etd || null, port || null]
        );
        return ins.insertId;
    } catch (e) {
        if (e.errno !== ER_BAD_FIELD_ERROR) throw e;
    }
    const [ins] = await conn.query(
        `INSERT INTO draft_container_document_sends
            (draft_container_document_id, sent_to, subject, front_message_uid, front_conversation_id, sent_by_email)
         VALUES (?, ?, ?, ?, ?, ?)`,
        common
    );
    return ins.insertId;
}

/**
 * Map(sendId -> { delivery, etd, port }) for every send of these documents.
 * A send with no entry (the columns are not there yet) went out attached.
 */
async function loadSendFacts(conn, documentIds) {
    const out = new Map();
    const ids = [...new Set((documentIds || []).filter(Boolean))];
    if (!ids.length) return out;
    let rows;
    try {
        [rows] = await conn.query(
            `SELECT id, delivery, DATE_FORMAT(request_etd, '%Y-%m-%d') AS request_etd, request_port
               FROM draft_container_document_sends
              WHERE draft_container_document_id IN (${ids.map(() => '?').join(',')})`,
            ids
        );
    } catch (e) {
        if (e.errno === ER_BAD_FIELD_ERROR) return out;
        throw e;
    }
    for (const r of rows) out.set(Number(r.id), L.sendFactsOf(r));
    return out;
}

module.exports = { resolveDelivery, resolveUpdate, preparePortalSend, insertSend, loadSendFacts, saveSendUpdate, loadSendUpdates };
