#!/usr/bin/env node
'use strict';

// HTTP suite for forwarder links (2026-11-20, src/lib/forwarder-links.js,
// src/lib/payment-extras.js): a container's forwarder from the Forwarders list
// and from its live freight booking, forwarder payments by forwarder id, and
// forwarder payments priced from a booking or a stock movement.
//
// Against a running local server on the TEST database (guard() refuses
// anything else), normally a scratch one on :3041 so the user's :3031 is left
// alone:
//   ORDERS_PORT=3041 npm run dev:local     (in another shell)
//   TEST_BASE_URL=http://localhost:3041 node tools/test-forwarder-links-http.js
//
// Needs the migration 2026-11-20_10_forwarder_links.js applied to TEST.
// Fixtures: a forwarder, a draft container, a freight request / option /
// booking / stock movements inserted directly (no email is sent), and the
// payment extras the suite adds. All stamped with this run and removed again.

const crypto = require('crypto');
const H = require('./shipments-test-helpers');
const { api, check, section, sql, STAMP } = H;

const FWD = `SHIPTEST FWD ${STAMP}`;
const FWD2 = `SHIPTEST FWD2 ${STAMP}`;
const fx = { forwarders: [], requests: [], options: [], bookings: [], movements: [], extras: new Set(), payments: new Set() };

async function insertForwarder(name) {
    const r = await sql('INSERT INTO forwarders (name, notes) VALUES (?, ?)', [name, 'forwarder links test']);
    fx.forwarders.push(r.insertId);
    return r.insertId;
}
async function insertBooking({ forwarderId, forwarderName, shipmentId, shipmentName, status = 'confirmed', price = 1850, currency = 'USD' }) {
    const req = await sql(
        `INSERT INTO freight_quote_requests (token, batch_id, forwarder_id, forwarder_name, window_start_date, window_end_date,
                                             container_size, services, expires_at, answer_count)
         VALUES (?, ?, ?, ?, '2026-12-01', '2026-12-31', '40HQ', '["Door to door"]', UTC_TIMESTAMP() + INTERVAL 30 DAY, 1)`,
        [crypto.randomBytes(24).toString('hex'), crypto.randomUUID(), forwarderId, forwarderName]
    );
    fx.requests.push(req.insertId);
    const opt = await sql(
        `INSERT INTO freight_quote_options (request_id, answer_no, line_no, loading_date, sailing_date, transit_days, port_from, port_to,
                                            service, container_size, price, currency)
         VALUES (?, 1, 1, '2026-12-05', '2026-12-10', 32, 'Ningbo', 'Felixstowe', 'Door to door', '40HQ', ?, ?)`,
        [req.insertId, price, currency]
    );
    fx.options.push(opt.insertId);
    const bk = await sql(
        `INSERT INTO freight_bookings (request_id, option_id, status, requested_at, shipment_id, draft_container_name)
         VALUES (?, ?, ?, UTC_TIMESTAMP(), ?, ?)`,
        [req.insertId, opt.insertId, status, shipmentId, shipmentName]
    );
    fx.bookings.push(bk.insertId);
    return { requestId: req.insertId, optionId: opt.insertId, bookingId: bk.insertId };
}
async function insertMovement(requestId, lineNo, price, currency) {
    const r = await sql(
        `INSERT INTO freight_stock_movements (request_id, add_key, line_no, factory, cbm, weight_kg, price, currency)
         VALUES (?, ?, ?, ?, 1.5, 200, ?, ?)`,
        [requestId, `test-${STAMP}`, lineNo, `Factory ${lineNo}`, price, currency]
    );
    fx.movements.push(r.insertId);
    return r.insertId;
}
const addExtra = async (body) => {
    const r = await api.post('/api/v1/payment-extras', body);
    if (r.status === 201 && r.data && r.data.id) fx.extras.add(r.data.id);
    return r;
};

async function cleanupFixtures() {
    const extraIds = [...fx.extras, ...(await sql('SELECT id FROM payment_extras WHERE supplier_name IN (?, ?)', [FWD, FWD2])).map(r => r.id)];
    const pays = [...fx.payments, ...(await sql('SELECT id FROM supplier_payments WHERE supplier_name IN (?, ?)', [FWD, FWD2])).map(r => r.id)];
    if (pays.length) {
        await sql(`DELETE FROM audit_log WHERE entity_type = 'supplier_payment' AND entity_id IN (?)`, [pays]);
        await sql('DELETE FROM supplier_payment_lines WHERE payment_id IN (?)', [pays]);
        await sql('DELETE FROM supplier_payments WHERE id IN (?)', [pays]);
    }
    if (extraIds.length) {
        await sql(`DELETE FROM audit_log WHERE entity_type = 'payment_extra' AND entity_id IN (?)`, [extraIds]);
        await sql('DELETE FROM payment_extras WHERE id IN (?)', [extraIds]);
    }
    if (fx.movements.length) await sql('DELETE FROM freight_stock_movements WHERE id IN (?)', [fx.movements]);
    if (fx.bookings.length) await sql('DELETE FROM freight_bookings WHERE id IN (?)', [fx.bookings]);
    if (fx.options.length) await sql('DELETE FROM freight_quote_options WHERE id IN (?)', [fx.options]);
    if (fx.requests.length) await sql('DELETE FROM freight_quote_requests WHERE id IN (?)', [fx.requests]);
    if (fx.forwarders.length) await sql('DELETE FROM forwarders WHERE id IN (?)', [fx.forwarders]);
}

(async () => {
    await H.guard();
    const ready = await sql(`SELECT COUNT(*) AS n FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'payment_extras' AND COLUMN_NAME = 'freight_booking_id'`);
    if (!Number(ready[0].n)) {
        console.error('Refusing to run: 2026-11-20_10_forwarder_links.js is not applied to this database.');
        process.exit(2);
    }
    try {
        const fwd = await insertForwarder(FWD);
        const fwd2 = await insertForwarder(FWD2);
        const name = H.draftName('forwarder links');
        const made = await api.post('/api/v1/shipments', { mode: 'SEA', stage: 'DRAFT', name });
        check('a draft container to work on', made.status === 201 || made.status === 200, made.data);
        const shipId = made.data && (made.data.id || (made.data.shipment && made.data.shipment.id));
        H.track(shipId);
        const get = async () => (await api.get(`/api/v1/shipments/${shipId}`)).data;

        section('The container\'s own forwarder');
        let r = await api.patch(`/api/v1/shipments/${shipId}`, { forwarderId: fwd, forwarder: 'ignored text' });
        check('PATCH forwarderId: the list name, linked', r.status === 200 && r.data.forwarder === FWD && r.data.forwarderId === fwd && r.data.forwarderSource === 'container', r.data);
        r = await api.patch(`/api/v1/shipments/${shipId}`, { forwarder: 'Someone Typed' });
        check('PATCH text no forwarder carries: unlinked', r.status === 200 && r.data.forwarder === 'Someone Typed' && r.data.forwarderId === null, r.data);
        r = await api.patch(`/api/v1/shipments/${shipId}`, { forwarder: `  ${FWD.toLowerCase()} ` });
        check('PATCH text a forwarder carries: linked, the list spelling', r.status === 200 && r.data.forwarder === FWD && r.data.forwarderId === fwd, r.data);
        r = await api.patch(`/api/v1/shipments/${shipId}`, { forwarderId: null, forwarder: 'On Purpose' });
        check('PATCH forwarderId null + text: unlinked on purpose', r.status === 200 && r.data.forwarder === 'On Purpose' && r.data.forwarderId === null, r.data);
        r = await api.patch(`/api/v1/shipments/${shipId}`, { forwarderId: 999999999 });
        check('PATCH an id not on the list: 400 BAD_FIELD forwarderId', r.status === 400 && r.data.code === 'BAD_FIELD' && r.data.field === 'forwarderId', r.data);
        await api.patch(`/api/v1/shipments/${shipId}`, { forwarderId: fwd2 });

        section('A live freight booking wins');
        const { requestId, bookingId } = await insertBooking({ forwarderId: fwd, forwarderName: FWD, shipmentId: shipId, shipmentName: name });
        let s = await get();
        check('the booking\'s forwarder, from booking', s.forwarder === FWD && s.forwarderId === fwd && s.forwarderSource === 'booking' && s.booking && s.booking.id === bookingId, s);
        const list = (await api.get('/api/v1/shipments', { params: { stage: 'DRAFT', limit: 2000 } })).data.data.find(x => x.id === shipId);
        check('the list says the same', list && list.forwarderSource === 'booking' && list.forwarderId === fwd, list);
        r = await api.patch(`/api/v1/shipments/${shipId}`, { notes: `note ${STAMP}` });
        check('a PATCH response re-reads: still from booking', r.status === 200 && r.data.forwarderSource === 'booking', r.data);
        let dup = null;
        try { await insertBooking({ forwarderId: fwd2, forwarderName: FWD2, shipmentId: shipId, shipmentName: name }); } catch (e) { dup = e; }
        check('a second live booking on the container: refused by the unique key', dup && dup.code === 'ER_DUP_ENTRY' && /uq_freight_bookings_live_shipment/.test(dup.message), dup && dup.message);
        const charges = (await api.get(`/api/v1/shipments/${shipId}/forwarder-charges`)).data;
        check('forwarder charges: the booking and its price', charges.booking && charges.booking.id === bookingId && charges.booking.option.price === 1850 && charges.booking.option.currency === 'USD' && charges.booking.extraId === null, charges);

        section('Forwarder payments by forwarder id');
        r = await addExtra({ supplierName: 'typed anyhow', currency: 'GBP', amount: 240, kind: 'customs', ridesWith: 'shipment', shipmentId: shipId, forwarderId: fwd2 });
        check('POST forwarderId: the list name is the payee', r.status === 201 && r.data.supplierName === FWD2 && r.data.forwarderId === fwd2, r.data);
        const plain = r.data;
        r = await addExtra({ supplierName: FWD, currency: 'GBP', amount: 10, kind: 'other', ridesWith: 'shipment', shipmentId: shipId, forwarderId: 999999999 });
        check('POST an id not on the list: 400 BAD_FIELD forwarderId', r.status === 400 && r.data.field === 'forwarderId', r.data);
        r = await addExtra({ supplierName: FWD.toUpperCase(), currency: 'GBP', amount: 11, kind: 'other', ridesWith: 'shipment', shipmentId: shipId });
        check('POST no id (an older client): linked by name, the name kept as sent', r.status === 201 && r.data.forwarderId === fwd && r.data.supplierName === FWD.toUpperCase(), r.data);
        const base = { supplierName: plain.supplierName, currency: 'GBP', amount: 240, kind: 'customs', ridesWith: 'shipment', shipmentId: shipId, shipmentReference: plain.shipmentReference };
        r = await api.put(`/api/v1/payment-extras/${plain.id}`, { ...base, note: 'old client' });
        check('PUT no id, same name: the link stays', r.status === 200 && r.data.forwarderId === fwd2, r.data);
        await sql('UPDATE forwarders SET name = ? WHERE id = ?', [`${FWD2} RENAMED`, fwd2]);
        r = await api.put(`/api/v1/payment-extras/${plain.id}`, { ...base, forwarderId: fwd2, note: 'after a rename' });
        check('PUT the same id after a rename: the recorded name stays', r.status === 200 && r.data.supplierName === FWD2 && r.data.forwarderId === fwd2, r.data);
        await sql('UPDATE forwarders SET name = ? WHERE id = ?', [FWD2, fwd2]);
        r = await api.put(`/api/v1/payment-extras/${plain.id}`, { ...base, forwarderId: null });
        check('PUT forwarderId null: unlinked', r.status === 200 && r.data.forwarderId === null, r.data);

        section('Priced from the booking and a stock movement');
        const m1 = await insertMovement(requestId, 1, 120, 'GBP');
        const m2 = await insertMovement(requestId, 2, null, null);
        r = await addExtra({ supplierName: 'x', currency: 'USD', amount: 1850, kind: 'freight', ridesWith: 'shipment', shipmentId: shipId, freightSource: { kind: 'booking', id: bookingId } });
        check('POST from the booking: 201, the booking\'s forwarder', r.status === 201 && r.data.forwarderId === fwd && r.data.supplierName === FWD && r.data.freightSource && r.data.freightSource.id === bookingId, r.data);
        const fromBooking = r.data;
        r = await addExtra({ supplierName: 'x', currency: 'USD', amount: 1, kind: 'freight', ridesWith: 'shipment', shipmentId: shipId, freightSource: { kind: 'booking', id: bookingId } });
        check('the same booking price again: 409 ALREADY_ADDED', r.status === 409 && r.data.code === 'ALREADY_ADDED' && r.data.extraId === fromBooking.id, r.data);
        const race = await Promise.all([1, 2].map(() => addExtra({ supplierName: 'x', currency: 'GBP', amount: 120, kind: 'freight', ridesWith: 'shipment', shipmentId: shipId, freightSource: { kind: 'movement', id: m1 } })));
        check('two adds of one movement price at once: one 201, one 409', race.map(x => x.status).sort().join(',') === '201,409', race.map(x => [x.status, x.data && x.data.code]));
        r = await addExtra({ supplierName: 'x', currency: 'GBP', amount: 5, kind: 'freight', ridesWith: 'shipment', shipmentId: shipId, freightSource: { kind: 'movement', id: m2 } });
        check('an unpriced movement: 409 UNPRICED', r.status === 409 && r.data.code === 'UNPRICED', r.data);
        r = await addExtra({ supplierName: 'x', currency: 'USD', amount: 5, kind: 'freight', ridesWith: 'shipment', shipmentId: shipId, freightSource: { kind: 'booking', id: bookingId }, sourceKind: 'shipment_document', sourceId: 1 });
        check('freightSource with an invoice source: 400', r.status === 400, r.data);
        const fb = { supplierName: FWD, currency: 'USD', amount: 1850, kind: 'freight', ridesWith: 'shipment', shipmentId: shipId, shipmentReference: fromBooking.shipmentReference };
        r = await api.put(`/api/v1/payment-extras/${fromBooking.id}`, { ...fb, sourceKind: 'shipment_document', sourceId: 12345, note: 'old editor' });
        const [stored] = await sql('SELECT source_kind, source_id, freight_booking_id FROM payment_extras WHERE id = ?', [fromBooking.id]);
        check('an older editor\'s PUT: 200, source untouched, no invoice source written', r.status === 200 && stored.source_kind === null && stored.source_id === null && stored.freight_booking_id === bookingId, [r.status, r.data && r.data.code, stored]);
        r = await api.put(`/api/v1/payment-extras/${fromBooking.id}`, { ...fb, ridesWith: 'balance' });
        check('PUT ridesWith balance on it: 422 SOURCE_LOCKED', r.status === 422 && r.data.code === 'SOURCE_LOCKED', r.data);
        r = await api.put(`/api/v1/payment-extras/${fromBooking.id}`, { ...fb, forwarderId: fwd2 });
        check('PUT another forwarder on it: 422 SOURCE_LOCKED', r.status === 422 && r.data.code === 'SOURCE_LOCKED', r.data);
        const after = (await api.get(`/api/v1/shipments/${shipId}/forwarder-charges`)).data;
        check('forwarder charges show what is added', after.booking.extraId === fromBooking.id && after.movements.find(m => m.id === m1).extraId != null && after.movements.find(m => m.id === m2).price === null, after);

        section('After the booking is cancelled');
        await sql(`UPDATE freight_bookings SET status = 'cancelled', cancelled_at = UTC_TIMESTAMP(), cancel_reason = 'staff' WHERE id = ?`, [bookingId]);
        r = await api.put(`/api/v1/payment-extras/${fromBooking.id}`, { ...fb, amount: 1900, note: 'invoice said 1900' });
        check('its payment stays editable: amount 1900', r.status === 200 && r.data.amount === 1900 && r.data.freightSource.id === bookingId, r.data);
        r = await addExtra({ supplierName: 'x', currency: 'USD', amount: 1850, kind: 'freight', ridesWith: 'shipment', shipmentId: shipId, freightSource: { kind: 'booking', id: bookingId } });
        check('a new price from it: 409 (ALREADY_ADDED or NOT_LIVE)', r.status === 409, r.data);
        s = await get();
        check('the container shows its own forwarder again', s.forwarderSource === 'container' && s.forwarderId === fwd2, s);

        section('Record payment to the forwarder still settles a linked extra');
        const pay = await api.post('/api/v1/supplier-payments', {
            supplierName: FWD, amount: 1900, currency: 'USD', paidOn: new Date().toISOString().slice(0, 10),
            lines: [{ kind: 'extra', id: fromBooking.id, amount: 1900 }],
        });
        if (pay.status === 201 && pay.data && pay.data.id) fx.payments.add(pay.data.id);
        const [settled] = await sql('SELECT status FROM payment_extras WHERE id = ?', [fromBooking.id]);
        check('the transfer pays it', pay.status === 201 && settled.status === 'paid', [pay.status, pay.data && pay.data.code, settled]);
    } catch (e) {
        H.fail('suite crashed', e);
    }
    try { await cleanupFixtures(); } catch (e) { console.log(`  fixture cleanup error: ${e.message}`); }
    await H.finish();
})();
