'use strict';

// Unit tests for src/lib/payment-documents.js — who may delete an uploaded
// payment document (a supplier's invoice, or a proof of payment), where it is
// read, and refiling a balance invoice between a shipment (a commercial
// invoice) and the supplier alone (a QC invoice). No database.
//   node --test tools/test-payment-documents-lib.js

const test = require('node:test');
const assert = require('node:assert/strict');
const L = require('../src/lib/payment-documents');

// A proof of payment filed from Record payment that no payment uses.
const proof = (over = {}) => ({
    id: 70, doc_kind: 'remittance', supplier_payment_id: null, payment_id: null, uploaded_by_email: 'Accounts@Built-Form.co.uk', ...over,
});

test('an admin may delete any upload', () => {
    assert.equal(L.deleteRefusal({ userType: 'admin', userEmail: 'boss@x.com', doc: proof() }), null);
    assert.equal(L.deleteRefusal({ userType: 'admin', userEmail: 'boss@x.com', doc: proof({ doc_kind: 'balance_invoice', payment_id: 5 }) }), null);
    assert.equal(L.deleteRefusal({ userType: 'admin', userEmail: null, doc: proof({ supplier_payment_id: 9 }) }), null);
});

test('whoever uploaded a proof no payment uses may take it back — the email compared whatever its case', () => {
    assert.equal(L.deleteRefusal({ userType: 'accountant', userEmail: 'accounts@built-form.co.uk', doc: proof() }), null);
    assert.equal(L.deleteRefusal({ userType: 'standard', userEmail: ' ACCOUNTS@built-form.co.uk ', doc: proof() }), null);
});

test('someone else\'s proof: admin only', () => {
    const r = L.deleteRefusal({ userType: 'accountant', userEmail: 'other@built-form.co.uk', doc: proof() });
    assert.deepEqual(r, { status: 403, code: 'ADMIN_ONLY', error: 'Admin access required — only an admin, or whoever uploaded a proof that no payment uses, can delete it.' });
    assert.equal(L.deleteRefusal({ userType: 'accountant', userEmail: null, doc: proof() }).code, 'ADMIN_ONLY');
    assert.equal(L.deleteRefusal({ userType: 'accountant', userEmail: 'accounts@built-form.co.uk', doc: proof({ uploaded_by_email: null }) }).code, 'ADMIN_ONLY');
    assert.equal(L.deleteRefusal({ userType: undefined, userEmail: '', doc: proof({ uploaded_by_email: '' }) }).code, 'ADMIN_ONLY');
});

test('a proof a payment uses is not the uploader\'s to delete: it is that payment\'s evidence', () => {
    const who = { userType: 'accountant', userEmail: 'accounts@built-form.co.uk' };
    assert.equal(L.deleteRefusal({ ...who, doc: proof({ supplier_payment_id: 9 }) }).code, 'ADMIN_ONLY');
    assert.equal(L.deleteRefusal({ ...who, doc: proof({ payment_id: 4 }) }).code, 'ADMIN_ONLY');
});

test('only a proof of payment: a supplier\'s invoice the uploader filed stays admin-only', () => {
    const who = { userType: 'accountant', userEmail: 'accounts@built-form.co.uk' };
    assert.equal(L.deleteRefusal({ ...who, doc: proof({ doc_kind: 'balance_invoice' }) }).code, 'ADMIN_ONLY');
    assert.equal(L.deleteRefusal({ ...who, doc: proof({ doc_kind: 'other' }) }).code, 'ADMIN_ONLY');
});

// ── Where an upload was filed decides how it is read (user, 2026-11-02) ──
// On a shipment: that container's invoice. On the supplier alone through
// "Upload QC invoice": a QC invoice. On the balance of a PO's combined /
// repackaged goods (no container will ever bill them): their balance invoice,
// the PO named. A proof of payment on the supplier stays a proof.
test('the upload place: a shipment, the repackaged goods of a PO, QC units, or the supplier alone', () => {
    assert.equal(L.readPlaceOf({ shipment_id: 307, doc_kind: 'balance_invoice', purchase_order_id: null }), 'shipment');
    assert.equal(L.readPlaceOf({ shipment_id: null, doc_kind: 'balance_invoice', purchase_order_id: 500 }), 'repackaged');
    assert.equal(L.readPlaceOf({ shipment_id: null, doc_kind: 'balance_invoice', purchase_order_id: null }), 'qc');
    assert.equal(L.readPlaceOf({ shipment_id: null, doc_kind: 'balance_invoice' }), 'qc');
    assert.equal(L.readPlaceOf({ shipment_id: null, doc_kind: 'remittance', purchase_order_id: null }), 'supplier');
    assert.equal(L.readPlaceOf({ shipment_id: null, doc_kind: 'remittance', purchase_order_id: 500 }), 'supplier');
});

// What an upload naming a PO must be: the balance invoice for goods on it
// that are marked Combined / Repackaged, with no shipment.
const repack = (over = {}) => ({
    docKind: 'balance_invoice', hasShipment: false, purchaseOrderId: 500,
    po: { id: 500, poNumber: 'PO_00394J', supplier: 'Roosin Medical Co., Ltd.' }, repackagedLines: 1, ...over,
});

test('a balance invoice for a PO\'s combined / repackaged goods is allowed', () => {
    assert.equal(L.repackagedUploadRefusal(repack()), null);
    assert.equal(L.repackagedUploadRefusal(repack({ purchaseOrderId: '500' })), null);
});

test('an upload naming a PO: a balance invoice, no shipment, a live PO with goods marked Combined / Repackaged', () => {
    assert.deepEqual(L.repackagedUploadRefusal(repack({ purchaseOrderId: 'x' })), { status: 400, code: 'BAD_FIELD', error: 'purchaseOrderId must be a purchase order id.' });
    assert.deepEqual(L.repackagedUploadRefusal(repack({ purchaseOrderId: 0 })), { status: 400, code: 'BAD_FIELD', error: 'purchaseOrderId must be a purchase order id.' });
    assert.deepEqual(L.repackagedUploadRefusal(repack({ hasShipment: true })), { status: 400, code: 'BAD_FIELD', error: 'An invoice for combined / repackaged goods names a purchase order, not a shipment — send one or the other.' });
    assert.deepEqual(L.repackagedUploadRefusal(repack({ docKind: 'remittance' })), { status: 400, code: 'BAD_FIELD', error: 'Only a balance invoice is filed against a purchase order\'s combined / repackaged goods.' });
    assert.deepEqual(L.repackagedUploadRefusal(repack({ po: null })), { status: 404, code: 'NOT_FOUND', error: 'Purchase order 500 not found.' });
    assert.deepEqual(L.repackagedUploadRefusal(repack({ repackagedLines: 0 })), { status: 422, code: 'NOT_REPACKAGED', error: 'PO_00394J has no goods marked Combined / Repackaged — upload its invoice on the container it ships in.' });
});

// ── Refiling: a commercial invoice ↔ a QC invoice ──────────────────────────
// An invoice imported from the supplier portal (or uploaded by staff) in the
// wrong place: the place decides how it is read, so moving it is the fix.
const invoice = (over = {}) => ({
    id: 90, doc_kind: 'balance_invoice', shipment_id: 61, shipment_reference: '324', supplier_name: 'Acme Ltd', supplier_key: 'acme ltd',
    purchase_order_id: null, payment_id: null, supplier_payment_id: null, uploaded_by_email: 'sales@acme.example', deleted_at: null, ...over,
});
const qcInvoice = (over = {}) => invoice({ shipment_id: null, shipment_reference: null, ...over });
const SHIP = { id: 62, reference: '325', stage: 'BOOKED' };

test('REFILE_KINDS, and who may refile: the import deciders (admin and standard)', () => {
    assert.deepEqual(L.REFILE_KINDS, ['commercial', 'qc']);
    assert.deepEqual(L.REFILE_ROLES, require('../src/lib/pi-submissions').DECIDER_ROLES);
    const r = L.refileRefusal({ userType: 'readonly', doc: invoice(), kind: 'qc', supplierName: 'Acme Ltd' });
    assert.deepEqual([r.status, r.code], [403, 'CANNOT_REFILE']);
    assert.equal(L.refileRefusal({ userType: 'accountant', doc: invoice(), kind: 'qc', supplierName: 'Acme Ltd' }).code, 'CANNOT_REFILE');
    assert.equal(L.refileRefusal({ userType: 'admin', doc: invoice(), kind: 'qc', supplierName: 'Acme Ltd' }), null);
    assert.equal(L.refileRefusal({ userType: 'standard', doc: invoice(), kind: 'qc', supplierName: 'Acme Ltd' }), null);
});

test('parseRefileBody: the kind, and a shipment only for a commercial invoice', () => {
    assert.deepEqual(L.parseRefileBody({ kind: 'qc' }).value, { kind: 'qc', shipmentId: null, shipmentReference: null, supplierName: null, force: false });
    assert.deepEqual(L.parseRefileBody({ kind: 'qc', supplierName: ' Acme Ltd ', force: true }).value, { kind: 'qc', shipmentId: null, shipmentReference: null, supplierName: 'Acme Ltd', force: true });
    assert.deepEqual(L.parseRefileBody({ kind: 'commercial', shipmentId: '62' }).value, { kind: 'commercial', shipmentId: 62, shipmentReference: null, supplierName: null, force: false });
    assert.deepEqual(L.parseRefileBody({ kind: 'commercial', shipmentReference: ' 325 ' }).value.shipmentReference, '325');
    assert.match(L.parseRefileBody({}).error, /kind must be commercial or qc/);
    assert.match(L.parseRefileBody({ kind: 'remittance' }).error, /kind must be commercial or qc/);
    assert.match(L.parseRefileBody({ kind: 'commercial' }).error, /shipmentId or shipmentReference/);
    assert.match(L.parseRefileBody({ kind: 'commercial', shipmentId: 0 }).error, /positive integer/);
    assert.match(L.parseRefileBody({ kind: 'qc', shipmentId: 62 }).error, /QC invoice.*not.*shipment/i);
    assert.match(L.parseRefileBody({ kind: 'qc', supplierName: 9 }).error, /supplierName/);
    assert.match(L.parseRefileBody({ kind: 'qc', force: 'yes' }).error, /force/);
});

test('refileRefusal: only a balance invoice that is nobody\'s evidence yet moves', () => {
    const who = { userType: 'admin', kind: 'qc', supplierName: 'Acme Ltd' };
    assert.equal(L.refileRefusal({ ...who, doc: invoice({ doc_kind: 'remittance' }) }).code, 'NOT_AN_INVOICE');
    assert.equal(L.refileRefusal({ ...who, doc: invoice({ doc_kind: 'other' }) }).status, 422);
    const repack = L.refileRefusal({ ...who, doc: qcInvoice({ purchase_order_id: 394 }) });
    assert.deepEqual([repack.status, repack.code], [422, 'NOT_REFILABLE']);
    const used = L.refileRefusal({ ...who, doc: invoice({ supplier_payment_id: 12 }) });
    assert.deepEqual([used.status, used.code, used.payload], [409, 'DOCUMENT_IN_USE', { supplierPaymentId: 12, paymentId: null }]);
    assert.deepEqual(L.refileRefusal({ ...who, doc: invoice({ payment_id: 5 }) }).payload, { supplierPaymentId: null, paymentId: 5 });
    assert.equal(L.refileRefusal({ ...who, doc: invoice({ supplier_payment_id: 12 }), force: true }).code, 'DOCUMENT_IN_USE', 'force never overrides a payment');
});

test('refileRefusal: current sign-offs are two people\'s work — confirmed with force', () => {
    const who = { userType: 'standard', kind: 'commercial', shipment: SHIP, supplierName: 'Acme Ltd' };
    const signed = L.refileRefusal({ ...who, doc: qcInvoice(), signOffs: 2 });
    assert.deepEqual([signed.status, signed.code, signed.payload], [409, 'DOCUMENT_SIGNED_OFF', { signOffs: 2 }]);
    assert.equal(L.refileRefusal({ ...who, doc: qcInvoice(), signOffs: 2, force: true }), null);
    assert.equal(L.refileRefusal({ ...who, doc: qcInvoice(), signOffs: 0 }), null);
});

test('refileRefusal: nothing to change', () => {
    const sameQc = L.refileRefusal({ userType: 'admin', doc: qcInvoice(), kind: 'qc', supplierName: 'ACME LTD' });
    assert.deepEqual([sameQc.status, sameQc.code], [409, 'NO_CHANGE'], 'the same supplier, whatever the case');
    assert.equal(L.refileRefusal({ userType: 'admin', doc: qcInvoice(), kind: 'qc', supplierName: 'Medoffice' }), null, 'another supplier is a change');
    const sameBox = L.refileRefusal({ userType: 'admin', doc: invoice(), kind: 'commercial', shipment: { id: 61, reference: '324' }, supplierName: 'Acme Ltd' });
    assert.equal(sameBox.code, 'NO_CHANGE');
    assert.equal(L.refileRefusal({ userType: 'admin', doc: invoice(), kind: 'commercial', shipment: SHIP, supplierName: 'Acme Ltd' }), null, 'another shipment');
    assert.equal(L.refileRefusal({ userType: 'admin', doc: invoice(), kind: 'commercial', shipment: { id: 61, reference: '324' }, supplierName: 'Other Co' }), null, 'the same box, another supplier');
    const noShip = L.refileRefusal({ userType: 'admin', doc: qcInvoice(), kind: 'commercial', shipment: null, supplierName: 'Acme Ltd' });
    assert.deepEqual([noShip.status, noShip.code], [400, 'BAD_FIELD']);
    const noName = L.refileRefusal({ userType: 'admin', doc: invoice({ supplier_name: null }), kind: 'qc', supplierName: null });
    assert.deepEqual([noName.status, noName.code], [400, 'BAD_FIELD'], 'a QC invoice is filed under a supplier');
});

test('refilePlace: what the row becomes', () => {
    assert.deepEqual(L.refilePlace({ kind: 'qc', shipment: null, supplierName: 'Acme Ltd' }), { shipmentId: null, shipmentReference: null, supplierName: 'Acme Ltd' });
    assert.deepEqual(L.refilePlace({ kind: 'commercial', shipment: SHIP, supplierName: 'Acme Ltd' }), { shipmentId: 62, shipmentReference: '325', supplierName: 'Acme Ltd' });
    assert.deepEqual(L.refilePlace({ kind: 'commercial', shipment: { id: 70, reference: null }, supplierName: 'Acme Ltd' }).shipmentReference, '70', 'a shipment with no reference: its id, as a staff upload writes it');
});

// ── The refile route, against a fake connection ────────────────────────────
const { registerPaymentDocumentRefileRoutes } = require('../src/services/payment-document-refile-routes');

const SHIP_325 = { id: 62, reference: '325', name: null, stage: 'BOOKED', mode: 'SEA', merged_into_id: null, deleted_at: null, tracking_ref: null, bl_number: null, booking_ref: null, etd: null, eta: null };
const posOn = (shipmentId, ...pos) => pos.map(([id, poNumber, supplier]) => ({ shipment_id: shipmentId, id, po_number: poNumber, supplier, currency: 'USD', line_count: 1, units_in_shipment: 10, value_in_shipment: 1000 }));

function refileWorld({ doc = invoice(), shipment = SHIP_325, memberPos = posOn(62, [301, 'PO_00301J', 'Acme Ltd']), signOffs = 0, duplicate = null, portalGrant = true } = {}) {
    const state = { doc: { ...doc }, audits: [], updates: [], portal: [], extractions: [], tx: [] };
    const conn = {
        async beginTransaction() { state.tx.push('begin'); },
        async commit() { state.tx.push('commit'); },
        async rollback() { state.tx.push('rollback'); },
        async query(sql, params = []) {
            const q = sql.replace(/\s+/g, ' ');
            if (/FROM shipment_payment_documents WHERE id = \? AND deleted_at IS NULL FOR UPDATE/.test(q)) return [Number(params[0]) === state.doc.id && !state.doc.deleted_at ? [{ ...state.doc }] : []];
            if (/FROM shipments WHERE id = \?/.test(q)) return [Number(params[0]) === shipment.id ? [shipment] : []];
            if (/FROM shipments WHERE reference = \?/.test(q)) return [String(params[0]) === shipment.reference ? [shipment] : []];
            if (/FROM shipments s WHERE s\.id = \?/.test(q)) return [[shipment]];
            if (/FROM orders o JOIN purchase_orders po/.test(q)) return [memberPos];
            if (/FROM payment_reviews WHERE payment_key = \?/.test(q)) return [[{ n: params[0] === `qc:${state.doc.id}` ? signOffs : 0 }]];
            if (/FROM shipment_payment_documents WHERE deleted_at IS NULL AND id <> \?/.test(q)) return [duplicate ? [{ id: duplicate }] : []];
            if (/UPDATE shipment_payment_documents SET shipment_id/.test(q)) {
                state.updates.push(params);
                Object.assign(state.doc, { shipment_id: params[0], shipment_reference: params[1], supplier_name: params[2], supplier_key: params[3], extract_status: 'processing', extract_error: null });
                return [{ affectedRows: 1 }];
            }
            if (/UPDATE `jportal`\.ci_submissions SET confirmed_shipment_id/.test(q)) {
                if (!portalGrant) throw Object.assign(new Error('access denied'), { errno: 1142 });
                state.portal.push(params);
                return [{ affectedRows: 1 }];
            }
            if (/SELECT \* FROM shipment_payment_documents WHERE id = \?/.test(q)) return [[{ ...state.doc }]];
            throw new Error(`unexpected SQL: ${q.slice(0, 100)}`);
        },
    };
    const handlers = {};
    const app = { post: (p, h) => { handlers[`POST ${p}`] = h; } };
    registerPaymentDocumentRefileRoutes(app, {
        withConnection: fn => fn(conn), pool: {}, ready: async () => {},
        recordAudit: async (_c, a) => { state.audits.push(a); },
        shipmentPaymentDocumentRowToJson: r => ({ id: r.id, shipmentId: r.shipment_id ?? null, shipmentReference: r.shipment_reference ?? null, supplierName: r.supplier_name, extractStatus: r.extract_status }),
        runShipmentPaymentExtraction: async (_pool, args) => { state.extractions.push(args); return { ok: true }; },
        log: { error() {}, warn(msg) { state.warned = msg; }, info() {} },
    });
    const call = async (body, { userType = 'standard' } = {}) => {
        const res = { statusCode: 200, body: null, headersSent: false, status(c) { this.statusCode = c; return this; }, json(b) { this.body = b; this.headersSent = true; return this; } };
        await handlers['POST /api/v1/shipment-payment-documents/:id(\\d+)/refile']({ params: { id: '90' }, body, userType, userEmail: 'ops@example.com' }, res);
        return res;
    };
    return { state, call };
}

test('refile route: a commercial invoice becomes a QC invoice — no shipment, the read reset and run again, the portal row following', async () => {
    const { state, call } = refileWorld();
    const res = await call({ kind: 'qc' });
    assert.equal(res.statusCode, 202, JSON.stringify(res.body));
    assert.deepEqual(res.body.refiled, {
        from: { kind: 'commercial', shipmentId: 61, shipmentReference: '324', supplierName: 'Acme Ltd' },
        to: { kind: 'qc', shipmentId: null, shipmentReference: null, supplierName: 'Acme Ltd' },
    });
    assert.deepEqual(res.body.document, { id: 90, shipmentId: null, shipmentReference: null, supplierName: 'Acme Ltd', extractStatus: 'processing' });
    assert.deepEqual(state.updates, [[null, null, 'Acme Ltd', 'acme ltd', 90]]);
    assert.deepEqual(state.portal, [[null, null, 90]], 'the supplier portal row says "Filed" with no shipment now');
    assert.equal(res.body.portalRows, 1);
    assert.deepEqual(state.tx, ['begin', 'commit']);
    assert.equal(state.audits.length, 1);
    assert.deepEqual([state.audits[0].action, state.audits[0].before.kind, state.audits[0].after.kind], ['refile', 'commercial', 'qc']);
    await new Promise(r => setImmediate(r));
    assert.deepEqual(state.extractions, [{ documentId: 90, userEmail: 'ops@example.com' }]);
});

test('refile route: a QC invoice goes on a shipment, filed under the supplier on it', async () => {
    const { state, call } = refileWorld({ doc: qcInvoice() });
    const res = await call({ kind: 'commercial', shipmentId: 62 });
    assert.equal(res.statusCode, 202, JSON.stringify(res.body));
    assert.deepEqual(res.body.refiled.to, { kind: 'commercial', shipmentId: 62, shipmentReference: '325', supplierName: 'Acme Ltd' });
    assert.deepEqual(state.portal, [[62, '325', 90]]);
    assert.equal(state.audits[0].after.onShipment, true);

    // Another supplier's box: who is on it, or force.
    const other = refileWorld({ doc: qcInvoice(), memberPos: posOn(62, [305, 'PO_00305J', 'Other Co']) });
    const refused = await other.call({ kind: 'commercial', shipmentId: 62 });
    assert.deepEqual([refused.statusCode, refused.body.code], [422, 'SUPPLIER_NOT_ON_SHIPMENT']);
    assert.deepEqual(refused.body.suppliers.map(x => x.supplierName), ['Other Co']);
    assert.deepEqual(other.state.tx, ['begin', 'rollback']);
    const picked = await other.call({ kind: 'commercial', shipmentId: 62, supplierName: 'other co' });
    assert.equal(picked.statusCode, 202);
    assert.equal(picked.body.refiled.to.supplierName, 'Other Co', 'the PO\'s own spelling');
    const forced = await refileWorld({ doc: qcInvoice(), memberPos: posOn(62, [305, 'PO_00305J', 'Other Co']) }).call({ kind: 'commercial', shipmentId: 62, force: true });
    assert.equal(forced.statusCode, 202);
    assert.equal(forced.body.refiled.to.supplierName, 'Acme Ltd', 'forced: kept under its own supplier');
});

test('refile route: refusals, and a portal schema it cannot reach', async () => {
    assert.deepEqual([(await refileWorld().call({ kind: 'qc' }, { userType: 'accountant' })).statusCode, (await refileWorld().call({ kind: 'qc' }, { userType: 'accountant' })).body.code], [403, 'CANNOT_REFILE']);
    const bad = await refileWorld().call({ kind: 'commercial' });
    assert.deepEqual([bad.statusCode, bad.body.code], [400, 'BAD_FIELD']);
    const gone = await refileWorld({ doc: invoice({ id: 91 }) }).call({ kind: 'qc' });
    assert.deepEqual([gone.statusCode, gone.body.code], [404, 'NOT_FOUND']);
    const same = await refileWorld({ doc: qcInvoice() }).call({ kind: 'qc' });
    assert.deepEqual([same.statusCode, same.body.code], [409, 'NO_CHANGE']);
    const used = await refileWorld({ doc: invoice({ supplier_payment_id: 4 }) }).call({ kind: 'qc' });
    assert.deepEqual([used.statusCode, used.body.code, used.body.supplierPaymentId], [409, 'DOCUMENT_IN_USE', 4]);
    const signed = await refileWorld({ doc: qcInvoice(), signOffs: 2 }).call({ kind: 'commercial', shipmentId: 62 });
    assert.deepEqual([signed.statusCode, signed.body.code, signed.body.signOffs], [409, 'DOCUMENT_SIGNED_OFF', 2]);
    assert.equal((await refileWorld({ doc: qcInvoice(), signOffs: 2 }).call({ kind: 'commercial', shipmentId: 62, force: true })).statusCode, 202);
    const dup = await refileWorld({ duplicate: 41 }).call({ kind: 'qc' });
    assert.deepEqual([dup.statusCode, dup.body.code, dup.body.existingId], [409, 'DUPLICATE_DOCUMENT', 41]);
    const proof = await refileWorld({ doc: invoice({ doc_kind: 'remittance' }) }).call({ kind: 'qc' });
    assert.deepEqual([proof.statusCode, proof.body.code], [422, 'NOT_AN_INVOICE']);
    const draft = await refileWorld({ doc: qcInvoice(), shipment: { ...SHIP_325, stage: 'DRAFT' } }).call({ kind: 'commercial', shipmentId: 62 });
    assert.deepEqual([draft.statusCode, draft.body.code], [422, 'NOT_BOOKED']);
    const merged = await refileWorld({ doc: qcInvoice(), shipment: { ...SHIP_325, merged_into_id: 70 } }).call({ kind: 'commercial', shipmentId: 62 });
    assert.deepEqual([merged.statusCode, merged.body.code, merged.body.mergedIntoId], [409, 'MERGED', 70]);
    const missing = await refileWorld({ doc: qcInvoice() }).call({ kind: 'commercial', shipmentId: 999 });
    assert.deepEqual([missing.statusCode, missing.body.code], [404, 'SHIPMENT_NOT_FOUND']);

    // No grant on the portal schema (a staff upload on a database without jportal): the refile still goes through.
    const noPortal = refileWorld({ portalGrant: false });
    const ok = await noPortal.call({ kind: 'qc' });
    assert.equal(ok.statusCode, 202);
    assert.equal(ok.body.portalRows, 0);
    assert.deepEqual(noPortal.state.tx, ['begin', 'commit']);
    assert.match(String(noPortal.state.warned), /portal row not updated/);
});
