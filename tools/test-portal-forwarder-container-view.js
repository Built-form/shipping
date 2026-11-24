'use strict';
// The portal's view of a forwarder's containers, against a real database
// (portal_forwarder_containers_v as 2026-11-24_11 re-created it for three
// duties, and portal_forwarder_payment_lines_v of 2026-11-23_11;
// docs/forwarder-containers.md).
//
// What only a database can show: which containers the view lists for which
// forwarder and with which duties (freight, delivery, customs), that it agrees
// with the rule the routes apply to locked rows (src/lib/forwarder-containers.js
// dutiesOf) and with the API's own read of a container (stage, ECN, dates,
// vessel, duties), the delivery date in its one form, when a container is
// finished, what a payment-only row carries, and that no column says anything
// about a supplier, a line or a price.
//
// It WRITES: fixture rows of its own (forwarders, shipments, orders, freight
// requests, options and bookings, charges, payments, lines, sends), every one
// deleted by id at the end, pass or fail. TEST only: it refuses a host that
// does not look like TEST and needs the host said back to it. The fixture
// orders carry container numbers that are not real ones (lower case), so the
// tracking feeder never registers them.
//
//   node tools/test-portal-forwarder-container-view.js --confirm-host <DB_HOST>
//
// Needs the migrations of 2026-11-23 and 2026-11-24 applied here. PART OF THE
// RELEASE GATE of the duties work: run it on TEST after its migration, before
// anything is deployed.
require('dotenv').config();
const crypto = require('crypto');
const { getPool, closePool } = require('../src/db');
const FC = require('../src/lib/forwarder-containers');
const S = require('../src/lib/shipments');
const sync = require('../src/services/shipment-sync');

const host = process.env.DB_HOST || '';
const confirm = process.argv[process.argv.indexOf('--confirm-host') + 1];
const HEX = crypto.randomBytes(4).toString('hex');
const TAG = `ZZ fwd-container-test ${HEX}`;

let passed = 0;
let failed = 0;
const check = (name, ok, detail = '') => {
    if (ok) { passed++; console.log(`  ok    ${name}`); } else { failed++; console.log(`  FAIL  ${name}${detail ? `  [${detail}]` : ''}`); }
};

// The 20 columns of 2026-11-23_11, then the four of 2026-11-24_11, added at the end.
const COLUMNS = ['shipment_id', 'forwarder_id', 'link_source', 'reference', 'mode', 'stage', 'ecn', 'loading_date', 'etd', 'eta',
    'arrived_on', 'finished', 'vessel_name', 'origin_port', 'booking_ref', 'sailing_date', 'port_from', 'port_to', 'container_size', 'freight_request_id',
    'does_freight', 'does_delivery', 'does_customs', 'delivery_date'];
const FLAGS = ['does_freight', 'does_delivery', 'does_customs'];
// Everything but who and which container: NULL on a payment-only row (its duty flags are 0).
const CONTAINER_FACTS = COLUMNS.filter(c => !['shipment_id', 'forwarder_id', 'link_source', 'reference', 'mode', ...FLAGS].includes(c));
// A row's duty flags as "1/0/0" (freight / delivery / customs).
const flagsOf = r => FLAGS.map(f => Number(r[f])).join('/');

(async () => {
    if (!/test/i.test(host)) { console.error(`Refusing: ${host || '(DB_HOST unset)'} does not look like the TEST database.`); process.exit(2); }
    if (confirm !== host) { console.error(`Refusing to write fixtures: pass --confirm-host ${host}`); process.exit(2); }
    const pool = getPool();
    const conn = await pool.getConnection();
    const made = {
        forwarders: [], shipments: [], orders: [], freight_quote_requests: [], freight_quote_options: [], freight_bookings: [],
        payment_extras: [], supplier_payments: [], supplier_payment_lines: [], supplier_payment_sends: [],
    };
    const q = async (sql, params) => (await conn.query({ sql, dateStrings: true }, params))[0];
    const ins = async (table, row) => {
        const cols = Object.keys(row);
        const r = await q(`INSERT INTO ${table} (${cols.join(', ')}) VALUES (${cols.map(() => '?').join(', ')})`, cols.map(c => row[c]));
        made[table].push(r.insertId);
        return r.insertId;
    };
    let n = 0;
    const forwarder = name => ins('forwarders', { name: `${TAG} ${name}`, created_by_email: 'view-test@tools' });
    const shipment = (over = {}) => {
        n += 1;
        return ins('shipments', { reference: `ZZ${HEX}-${n}`, name: `${TAG} ${n}`, mode: 'SEA', stage: 'BOOKED', origin: 'api', created_by_email: 'view-test@tools', ...over });
    };
    const order = (shipmentId, over = {}) => ins('orders', {
        jf_code: 'ZZVIEWTEST', product_name: 'Forwarder container view test', quantity: 1, unit_price: 0, status: 'CONSOLIDATED', shipment_id: shipmentId, ...over,
    });
    const booking = async (shipmentId, forwarderId, status, over = {}) => {
        const request = await ins('freight_quote_requests', {
            token: crypto.randomBytes(24).toString('hex'), batch_id: crypto.randomUUID(), forwarder_id: forwarderId, forwarder_name: TAG,
            window_start_date: '2026-12-01', window_end_date: '2026-12-31', container_size: '40HQ', services: '["Door to door"]',
            expires_at: '2027-12-31 00:00:00', answer_count: 1,
        });
        const option = await ins('freight_quote_options', {
            request_id: request, answer_no: 1, line_no: 1, loading_date: '2026-12-10', sailing_date: '2026-12-15', transit_days: 30,
            port_from: 'Ningbo', port_to: 'Felixstowe', service: 'Door to door', container_size: '40HQ', price: 1850, currency: 'USD',
        });
        const id = await ins('freight_bookings', { request_id: request, option_id: option, status, requested_at: '2026-11-20 10:00:00', shipment_id: shipmentId, booking_ref: 'BK-VIEW', ...over });
        return { id, request, option };
    };
    const charge = (forwarderId, over = {}) => ins('payment_extras', {
        supplier_name: TAG, supplier_key: TAG.toLowerCase(), currency: 'USD', amount: 100, kind: 'freight', rides_with: 'shipment',
        description: 'INTERNAL-DESCRIPTION', note: 'INTERNAL-NOTE', forwarder_id: forwarderId, created_by_email: 'view-test@tools', ...over,
    });
    const send = (paymentId, forwarderId, over = {}) => ins('supplier_payment_sends', {
        supplier_payment_id: paymentId, notice_kind: 'forwarder', delivery: 'portal', supplier_name: TAG, forwarder_id: forwarderId,
        send_key: `vt-${crypto.randomBytes(12).toString('hex')}`, request_hash: 'x'.repeat(64), status: 'sent',
        deadline_at: '2026-11-21 10:01:00', sent_to: '[]', sent_at: '2026-11-21 10:00:00', ...over,
    });
    // A payment that is one forwarder's, one charge per container given, sent to it unless told not to.
    const paymentOn = async (forwarderId, charges, { sent = true } = {}) => {
        const id = await ins('supplier_payments', { supplier_name: TAG, supplier_key: TAG.toLowerCase(), amount: 100 * charges.length, currency: 'USD', paid_on: '2026-11-21', created_by_email: 'view-test@tools' });
        const extras = [];
        for (const c of charges) {
            const e = await charge(forwarderId, c);
            extras.push(e);
            await ins('supplier_payment_lines', { payment_id: id, target_kind: 'extra', target_id: e, amount: 100 });
        }
        if (sent) await send(id, forwarderId);
        return { id, extras };
    };
    const rowsOf = shipmentId => q('SELECT * FROM portal_forwarder_containers_v WHERE shipment_id = ? ORDER BY forwarder_id', [shipmentId]);
    const rowFor = async (shipmentId, forwarderId) => (await q('SELECT * FROM portal_forwarder_containers_v WHERE shipment_id = ? AND forwarder_id = ?', [shipmentId, forwarderId]))[0] || null;
    const today = (await q('SELECT DATE_FORMAT(UTC_DATE(), \'%Y-%m-%d\') AS d'))[0].d;
    const daysAgo = async d => (await q('SELECT DATE_FORMAT(DATE_SUB(UTC_DATE(), INTERVAL ? DAY), \'%Y-%m-%d\') AS d', [d]))[0].d;

    try {
        console.log(`Database: ${process.env.DB_USER || '?'}@${host}/${process.env.DB_NAME || '?'}   fixtures tagged "${TAG}"   UTC date ${today}`);
        const A = await forwarder('A');
        const B = await forwarder('B');
        const C = await forwarder('C removed');
        await q('UPDATE forwarders SET deleted_at = NOW() WHERE id = ?', [C]);
        const liveForwarders = [A, B];

        console.log('\nWho a container is shown to, in full');
        const byHand = await shipment({ forwarder_id: A, forwarder: `${TAG} A` });
        let rows = await rowsOf(byHand);
        check('a forwarder picked by hand, no booking: one row, theirs, source "container"', rows.length === 1 && Number(rows[0].forwarder_id) === A && rows[0].link_source === 'container', JSON.stringify(rows));
        check('the view has exactly its documented columns, the four new ones at the end', rows.length === 1 && Object.keys(rows[0]).join() === COLUMNS.join(), rows[0] && Object.keys(rows[0]).join());
        check('a freight forwarder and nobody else: it does freight alone (1/0/0), as every row did before duties', rows.length === 1 && flagsOf(rows[0]) === '1/0/0', rows[0] && flagsOf(rows[0]));
        check('no column says anything of a supplier, a line, a quantity or a price', !COLUMNS.some(c => /supplier|line|unit|quantity|price|amount|cost|note/.test(c)));

        const booked = await shipment({ forwarder_id: B, forwarder: `${TAG} B` });
        const bk = await booking(booked, A, 'confirmed');
        rows = await rowsOf(booked);
        check('a confirmed booking: its forwarder, source "booking", and the forwarder picked by hand has no row', rows.length === 1 && Number(rows[0].forwarder_id) === A && rows[0].link_source === 'booking', JSON.stringify(rows.map(r => [r.forwarder_id, r.link_source])));
        check('the booking\'s reference, option and request are on the row', rows.length === 1 && rows[0].booking_ref === 'BK-VIEW' && rows[0].sailing_date === '2026-12-15'
            && rows[0].port_from === 'Ningbo' && rows[0].port_to === 'Felixstowe' && rows[0].container_size === '40HQ' && Number(rows[0].freight_request_id) === bk.request, JSON.stringify(rows[0]));

        const waiting = await shipment({ forwarder_id: B, forwarder: `${TAG} B` });
        await booking(waiting, A, 'requested');
        check('a booking still "requested": shown to nobody, the forwarder picked by hand included', (await rowsOf(waiting)).length === 0);

        const declined = await shipment();
        await booking(declined, A, 'declined');
        check('a declined booking and no forwarder on the container: nobody', (await rowsOf(declined)).length === 0);
        const cancelled = await shipment({ forwarder_id: B, forwarder: `${TAG} B` });
        await booking(cancelled, A, 'cancelled');
        rows = await rowsOf(cancelled);
        check('a cancelled booking is no booking: the forwarder picked by hand', rows.length === 1 && Number(rows[0].forwarder_id) === B && rows[0].link_source === 'container', JSON.stringify(rows.map(r => [r.forwarder_id, r.link_source])));

        const removed = await shipment({ forwarder_id: C, forwarder: `${TAG} C` });
        check('a forwarder taken off the list: nobody', (await rowsOf(removed)).length === 0);
        const typed = await shipment({ forwarder: 'Typed Freight Ltd' });
        check('a forwarder typed as text, not from the list: nobody', (await rowsOf(typed)).length === 0);

        const notReal = [];
        notReal.push(['a draft', await shipment({ stage: 'DRAFT', reference: null, open_key: `D:${TAG} d`, forwarder_id: A })]);
        notReal.push(['a planned one', await shipment({ stage: 'PLANNED', reference: null, open_key: `P:${TAG} p`, forwarder_id: A })]);
        notReal.push(['a cancelled one', await shipment({ stage: 'CANCELLED', forwarder_id: A })]);
        notReal.push(['a deleted one', await shipment({ forwarder_id: A, deleted_at: '2026-11-01 10:00:00' })]);
        const survivor = await shipment();
        notReal.push(['one merged away', await shipment({ forwarder_id: A, merged_into_id: survivor })]);
        notReal.push(['one with no number', await shipment({ reference: null, forwarder_id: A })]);
        for (const [what, id] of notReal) check(`${what}: not a row`, (await rowsOf(id)).length === 0);
        for (const stage of ['IN_TRANSIT', 'ARRIVED', 'CLOSED']) {
            const id = await shipment({ stage, forwarder_id: A });
            check(`stored stage ${stage}: a row`, (await rowsOf(id)).length === 1);
        }

        console.log('\nThree duties, one holder each: freight, delivery, customs');
        const said = list => JSON.stringify(list.map(r => [Number(r.forwarder_id), r.link_source, flagsOf(r)]));
        const is = (r, forwarderId, source, flags) => Boolean(r) && Number(r.forwarder_id) === forwarderId && r.link_source === source && flagsOf(r) === flags;

        // Port to port by booking (a booking made without a scope is port to port), another company for delivery and customs.
        const split = await shipment({ delivery_forwarder_id: B, customs_forwarder_id: B });
        const splitBk = await booking(split, A, 'confirmed');
        rows = await rowsOf(split);
        check('port to port + a delivery forwarder that also does customs: two rows, the freight holder "booking" 1/0/0 and the other "duty" 0/1/1',
            rows.length === 2 && is(rows.find(r => Number(r.forwarder_id) === A), A, 'booking', '1/0/0') && is(rows.find(r => Number(r.forwarder_id) === B), B, 'duty', '0/1/1'), said(rows));
        const freightRow = rows.find(r => Number(r.forwarder_id) === A);
        const otherRow = rows.find(r => Number(r.forwarder_id) === B);
        check('the booking\'s reference and its quote request are on the freight holder\'s row only', Boolean(freightRow && otherRow)
            && freightRow.booking_ref === 'BK-VIEW' && Number(freightRow.freight_request_id) === splitBk.request && otherRow.booking_ref === null && otherRow.freight_request_id === null,
            JSON.stringify([freightRow && [freightRow.booking_ref, freightRow.freight_request_id], otherRow && [otherRow.booking_ref, otherRow.freight_request_id]]));
        check('the sailing, the ports and the size are on both: the delivery company needs the port', Boolean(freightRow && otherRow)
            && ['sailing_date', 'port_from', 'port_to', 'container_size', 'stage', 'reference'].every(c => freightRow[c] === otherRow[c]) && otherRow.port_to === 'Felixstowe', JSON.stringify(otherRow));

        // End to end by booking: the booking's forwarder delivers too, and outranks the delivery pick.
        const endToEnd = await shipment({ delivery_forwarder_id: B });
        await booking(endToEnd, A, 'confirmed', { scope: 'end_to_end' });
        rows = await rowsOf(endToEnd);
        check('an end-to-end booking: its forwarder does freight and delivery (1/1/0), and the delivery pick has no row', rows.length === 1 && is(rows[0], A, 'booking', '1/1/0'), said(rows));

        // The same booking still "requested": freight and delivery have nobody, the delivery pick included. Customs is not the booking's.
        const e2eWaiting = await shipment({ forwarder_id: B, forwarder: `${TAG} B`, delivery_forwarder_id: B, customs_forwarder_id: B });
        await booking(e2eWaiting, A, 'requested', { scope: 'end_to_end' });
        rows = await rowsOf(e2eWaiting);
        check('an end-to-end booking still "requested": nobody carries or delivers it, the company picked for customs keeps customs alone (0/0/1)', rows.length === 1 && is(rows[0], B, 'duty', '0/0/1'), said(rows));

        // A port-to-port booking still "requested" leaves FREIGHT with nobody; delivery is not its to hold up.
        const p2pWaiting = await shipment({ delivery_forwarder_id: B });
        await booking(p2pWaiting, A, 'requested');
        rows = await rowsOf(p2pWaiting);
        check('a port-to-port booking still "requested": only the freight waits, the delivery forwarder has its row (0/1/0)', rows.length === 1 && is(rows[0], B, 'duty', '0/1/0'), said(rows));

        // End to end by hand: the same forwarder in both picks.
        const handE2e = await shipment({ forwarder_id: A, forwarder: `${TAG} A`, delivery_forwarder_id: A });
        rows = await rowsOf(handE2e);
        check('the same forwarder picked for freight and for delivery: one row, "container", 1/1/0', rows.length === 1 && is(rows[0], A, 'container', '1/1/0'), said(rows));

        // A customs broker that carries nothing, on a container with no freight forwarder at all.
        const brokerOnly = await shipment({ customs_forwarder_id: B });
        rows = await rowsOf(brokerOnly);
        check('a customs broker alone: one row, "duty", 0/0/1, and never a booking reference', rows.length === 1 && is(rows[0], B, 'duty', '0/0/1') && rows[0].booking_ref === null && rows[0].freight_request_id === null, said(rows));

        // All three, three companies would need a third live forwarder: A carries, B delivers, A does customs.
        const mixed = await shipment({ forwarder_id: A, forwarder: `${TAG} A`, delivery_forwarder_id: B, customs_forwarder_id: A });
        rows = await rowsOf(mixed);
        check('freight and customs with one company, delivery with another: 1/0/1 and 0/1/0', rows.length === 2
            && is(rows.find(r => Number(r.forwarder_id) === A), A, 'container', '1/0/1') && is(rows.find(r => Number(r.forwarder_id) === B), B, 'duty', '0/1/0'), said(rows));

        // A pick that names a forwarder taken off the list holds nothing; the others are not affected.
        const removedPick = await shipment({ forwarder_id: A, forwarder: `${TAG} A`, delivery_forwarder_id: C, customs_forwarder_id: C });
        rows = await rowsOf(removedPick);
        check('a delivery and customs pick taken off the list: no row for it, the freight forwarder unchanged (1/0/0)', rows.length === 1 && is(rows[0], A, 'container', '1/0/0'), said(rows));

        // A duty on a container that is not real is no duty.
        const draftPicks = await shipment({ stage: 'DRAFT', reference: null, open_key: `D:${TAG} picks`, delivery_forwarder_id: B, customs_forwarder_id: B });
        check('a delivery and customs pick on a draft: nobody yet', (await rowsOf(draftPicks)).length === 0);

        // A forwarder that holds a duty and was sent a payment on the same container has ONE row: the full one.
        await paymentOn(B, [{ shipment_id: split }]);
        rows = await rowsOf(split);
        check('the delivery forwarder paid on that container: still two rows, its own the "duty" one and never a "payment" one',
            rows.length === 2 && is(rows.find(r => Number(r.forwarder_id) === B), B, 'duty', '0/1/1'), said(rows));

        console.log('\nThe rule the routes apply to locked rows says the same');
        for (const id of made.shipments) {
            const [raw] = await q('SELECT id, reference, stage, deleted_at, merged_into_id, forwarder_id, forwarder, delivery_forwarder_id, customs_forwarder_id FROM shipments WHERE id = ?', [id]);
            const live = await q(
                `SELECT fb.id, fb.status, fb.scope, fr.forwarder_id FROM freight_bookings fb JOIN freight_quote_requests fr ON fr.id = fb.request_id
                  WHERE fb.shipment_id = ? AND fb.status IN ('requested','confirmed')`, [id]);
            const duties = FC.dutiesOf({ shipment: raw, bookings: live, forwarderLive: fid => liveForwarders.includes(Number(fid)) });
            // What the rule says each attached forwarder's row is: who, link_source, flags.
            const want = FC.attachedOf(duties)
                .map(a => [a.forwarderId, FC.linkSourceOf(duties, a.forwarderId), FLAGS.map(f => (a.duties.includes(f.slice(5)) ? 1 : 0)).join('/')])
                .sort((x, y) => x[0] - y[0]);
            const got = (await rowsOf(id)).filter(r => r.link_source !== 'payment').map(r => [Number(r.forwarder_id), r.link_source, flagsOf(r)]).sort((x, y) => x[0] - y[0]);
            if (JSON.stringify(want) !== JSON.stringify(got)) check(`dutiesOf agrees with the view for shipment ${id}`, false, `rule ${JSON.stringify(want)} vs view ${JSON.stringify(got)}`);
            // The freight duty is what a container's one forwarder was before: ownerOf still says it.
            const owner = FC.ownerOf({ shipment: raw, booking: live.find(b => (FC.SCOPE_DUTIES[b.scope || FC.DEFAULT_SCOPE] || []).includes('freight')) || null, forwarderLive: fid => liveForwarders.includes(Number(fid)) });
            if ((owner.forwarderId ?? null) !== (duties.freight.forwarderId ?? null)) check(`ownerOf is the freight duty for shipment ${id}`, false, `${JSON.stringify(owner)} vs ${JSON.stringify(duties.freight)}`);
        }
        check(`dutiesOf agrees with the view, row for row and flag for flag, for all ${made.shipments.length} fixture containers`, true);

        console.log('\nWhat it says of the container: the member orders win, as on the container card');
        const sea = await shipment({ forwarder_id: A, tracking_ref: 'stored-ref', etd: '2026-11-01', eta: '2026-12-01', vessel_name: 'Stored Vessel', loading_date: '2026-11-28', origin_port: 'Ningbo', booking_ref: 'OWN-REF' });
        await order(sea, { status: 'ON_SEA', external_container_number: 'zz-view-ecn-1', estimated_departure_date: '2026-11-05', eta: '2026-12-09', vessel_name: 'Member Vessel' });
        await order(sea, { status: 'CONSOLIDATED', external_container_number: 'zz-view-ecn-1', estimated_departure_date: '2026-11-03', eta: '2026-12-07' });
        let v = await rowFor(sea, A);
        check('ECN, departure, ETA and vessel come from the orders; loading date, port and reference from the container', v && v.ecn === 'zz-view-ecn-1' && v.etd === '2026-11-05' && v.eta === '2026-12-09'
            && v.vessel_name === 'Member Vessel' && v.loading_date === '2026-11-28' && v.origin_port === 'Ningbo' && v.booking_ref === 'OWN-REF', JSON.stringify(v));
        check('a stored BOOKED container whose order has sailed reads IN_TRANSIT', v && v.stage === 'IN_TRANSIT', v && v.stage);
        const air = await shipment({ mode: 'AIR', forwarder_id: A });
        await order(air, { status: 'ON_AIR', external_container_number: 'zz-view-ext', awb_number: 'zz-view-awb' });
        v = await rowFor(air, A);
        check('AIR: the air waybill number before the container number', v && v.ecn === 'zz-view-awb' && v.mode === 'AIR', JSON.stringify(v && [v.ecn, v.mode]));
        const empty = await shipment({ forwarder_id: A, tracking_ref: 'stored-ref', etd: '2026-11-01' });
        v = await rowFor(empty, A);
        check('no orders: the stored copies, stage as stored', v && v.ecn === 'stored-ref' && v.etd === '2026-11-01' && v.stage === 'BOOKED', JSON.stringify(v && [v.ecn, v.etd, v.stage]));
        const arrivedOrders = await shipment({ forwarder_id: A });
        await order(arrivedOrders, { status: 'ARRIVED_AT_WAREHOUSE', arrived_date: await daysAgo(3) });
        v = await rowFor(arrivedOrders, A);
        check('arrived by its orders: stage ARRIVED, arrived_on the latest order\'s', v && v.stage === 'ARRIVED' && v.arrived_on === await daysAgo(3), JSON.stringify(v && [v.stage, v.arrived_on]));

        // What GET /shipments/:id says of the duties is what the view lists, on the containers of every arrangement above.
        for (const id of [byHand, booked, split, endToEnd, e2eWaiting, p2pWaiting, handE2e, brokerOnly, mixed, removedPick]) {
            const api = S.rowToShipment(await sync.getShipment(conn, id));
            const want = FC.attachedOf(api.duties).map(a => [a.forwarderId, a.duties.join()]).sort((x, y) => x[0] - y[0]);
            const got = (await rowsOf(id)).filter(r => r.link_source !== 'payment')
                .map(r => [Number(r.forwarder_id), FC.dutiesOfFlags(r).join()]).sort((x, y) => x[0] - y[0]);
            check(`the duties GET /shipments/${id} gives are the view's`, JSON.stringify(want) === JSON.stringify(got), `api ${JSON.stringify(want)} vs view ${JSON.stringify(got)}`);
        }

        for (const id of [byHand, booked, sea, air, empty, arrivedOrders]) {
            const api = S.rowToShipment(await sync.getShipment(conn, id));
            const [row] = (await rowsOf(id)).filter(r => r.link_source !== 'payment');
            const same = row && api.stage === row.stage && (api.trackingRef || null) === (row.ecn || null) && (api.etd || null) === (row.etd || null)
                && (api.eta || null) === (row.eta || null) && (api.vesselName || null) === (row.vessel_name || null)
                && (api.loadingDate || null) === (row.loading_date || null) && Number(api.forwarderId) === Number(row.forwarder_id);
            check(`the same as GET /shipments/${id} on stage, ECN, dates, vessel and forwarder`, Boolean(same), JSON.stringify({ api: [api.stage, api.trackingRef, api.etd, api.eta, api.vesselName, api.loadingDate, api.forwarderId], view: row && [row.stage, row.ecn, row.etd, row.eta, row.vessel_name, row.loading_date, row.forwarder_id] }));
        }

        console.log('\nFinished: more than 14 days after it arrived');
        const d14 = await shipment({ forwarder_id: A, ata: await daysAgo(14) });
        const d15 = await shipment({ forwarder_id: A, ata: await daysAgo(15) });
        const notArrived = await shipment({ forwarder_id: A });
        check('the 14th day: not finished', Number((await rowFor(d14, A)).finished) === 0);
        check('the 15th day: finished', Number((await rowFor(d15, A)).finished) === 1);
        check('not arrived: not finished', Number((await rowFor(notArrived, A)).finished) === 0 && (await rowFor(notArrived, A)).arrived_on === null);
        const byOrders = await shipment({ forwarder_id: A });
        await order(byOrders, { status: 'RECEIVED', arrived_date: await daysAgo(20) });
        check('arrival taken from the orders when the container holds none: finished', Number((await rowFor(byOrders, A)).finished) === 1);
        check('the library\'s isFinished says the same for both days', FC.isFinished(await daysAgo(14), today) === false && FC.isFinished(await daysAgo(15), today) === true);

        console.log('\nThe delivery date: one form, held by every order or not at all');
        const delivered = async (values, over = {}) => {
            const id = await shipment({ forwarder_id: A, forwarder: `${TAG} A`, delivery_forwarder_id: B, ...over });
            for (const v of values) await order(id, { delivery_date: v });
            return id;
        };
        const dateOf = async id => (await rowFor(id, B)).delivery_date;
        check('every order at midnight: the day alone', await dateOf(await delivered(['2026-12-20 00:00:00', '2026-12-20 00:00:00'])) === '2026-12-20');
        check('every order at 09:30: the day and the time, to the minute', await dateOf(await delivered(['2026-12-20 09:30:00', '2026-12-20 09:30:00'])) === '2026-12-20T09:30');
        check('seconds do not make two orders differ', await dateOf(await delivered(['2026-12-20 09:30:00', '2026-12-20 09:30:45'])) === '2026-12-20T09:30');
        check('orders that differ: no date', await dateOf(await delivered(['2026-12-20 09:30:00', '2026-12-21 09:30:00'])) === null);
        check('one order with a date and one without: no date', await dateOf(await delivered(['2026-12-20 00:00:00', null])) === null);
        check('no order holds one: no date', await dateOf(await delivered([null, null])) === null);
        check('no orders at all: no date', await dateOf(await delivered([])) === null);
        const bothSee = await delivered(['2026-12-20 09:30:00']);
        check('the freight holder is shown the same delivery date as the company that delivers', (await rowFor(bothSee, A)).delivery_date === '2026-12-20T09:30' && (await rowFor(bothSee, B)).delivery_date === '2026-12-20T09:30');
        for (const values of [['2026-12-20 00:00:00', '2026-12-20 00:00:00'], ['2026-12-20 09:30:00', '2026-12-21 09:30:00'], ['2026-12-20 00:00:00', null]]) {
            const id = await delivered(values);
            const held = FC.heldDelivery((await q(`SELECT ${FC.deliverySelect('delivery_date')} AS d FROM orders WHERE shipment_id = ? AND deleted_at IS NULL`, [id])).map(r => r.d));
            check(`the library's heldDelivery says what the view says for ${JSON.stringify(values)}`, (held.deliveryDate ?? null) === await dateOf(id), JSON.stringify(held));
        }

        console.log('\nFinished, when someone delivers it: 14 days after the later of the arrival and the delivery');
        const finishedOf = async id => Number((await rowFor(id, B)).finished);
        const lateDelivery = await delivered([`${await daysAgo(10)} 00:00:00`], { ata: await daysAgo(20) });
        check('arrived 20 days ago, delivered 10 days ago: not finished', await finishedOf(lateDelivery) === 0);
        const oldDelivery = await delivered([`${await daysAgo(15)} 09:30:00`], { ata: await daysAgo(20) });
        check('arrived 20 days ago, delivered 15 days ago: finished', await finishedOf(oldDelivery) === 1);
        check('one answer for every row of the container: the freight holder\'s row says the same', Number((await rowFor(oldDelivery, A)).finished) === 1 && Number((await rowFor(lateDelivery, A)).finished) === 0);
        const awaiting = await shipment({ forwarder_id: A, forwarder: `${TAG} A`, delivery_forwarder_id: B, ata: await daysAgo(30) });
        await order(awaiting, { status: 'ARRIVED_AT_WAREHOUSE' });
        check('arrived 30 days ago, a delivery forwarder and no delivery date yet: not finished, so it can still send one', await finishedOf(awaiting) === 0);
        const closedNoDate = await shipment({ forwarder_id: A, forwarder: `${TAG} A`, delivery_forwarder_id: B, ata: await daysAgo(30) });
        await order(closedNoDate, { status: 'RECEIVED' });
        check('the same, every order received (CLOSED): finished by the arrival', await finishedOf(closedNoDate) === 1);
        const noDeliverer = await shipment({ forwarder_id: A, forwarder: `${TAG} A`, ata: await daysAgo(30) });
        await order(noDeliverer, { status: 'ARRIVED_AT_WAREHOUSE' });
        check('nobody delivers it and no delivery date: finished by the arrival, as before duties', Number((await rowFor(noDeliverer, A)).finished) === 1);
        check('the library\'s isFinished says the same for each', FC.isFinished(await daysAgo(20), today, { deliveryOn: await daysAgo(10) }) === false
            && FC.isFinished(await daysAgo(20), today, { deliveryOn: await daysAgo(15) }) === true
            && FC.isFinished(await daysAgo(30), today, { awaitingDelivery: true, closed: false }) === false
            && FC.isFinished(await daysAgo(30), today, { awaitingDelivery: true, closed: true }) === true
            && FC.isFinished(await daysAgo(30), today) === true);

        console.log('\nA container known only through a payment that was sent');
        const paidOnly = await shipment();
        const p1 = await paymentOn(A, [{ shipment_id: paidOnly }]);
        rows = await rowsOf(paidOnly);
        check('a sent payment on a container with no forwarder: one row, that forwarder\'s, source "payment"', rows.length === 1 && Number(rows[0].forwarder_id) === A && rows[0].link_source === 'payment', JSON.stringify(rows.map(r => [r.forwarder_id, r.link_source])));
        check('a payment-only row carries the ids, the number and the mode and NULL in every other column', rows.length === 1 && rows[0].reference && rows[0].mode === 'SEA' && CONTAINER_FACTS.every(c => rows[0][c] === null), JSON.stringify(rows[0]));
        check('and it does nothing on the container: 0/0/0', rows.length === 1 && flagsOf(rows[0]) === '0/0/0', rows[0] && flagsOf(rows[0]));
        const lineCols = await q('SELECT * FROM portal_forwarder_payment_lines_v WHERE payment_id = ?', [p1.id]);
        check('the payment\'s line names the container by id', lineCols.length === 1 && Number(lineCols[0].shipment_id) === paidOnly
            && Object.keys(lineCols[0]).sort().join() === 'amount,extra_kind,line_id,payment_id,shipment_id,shipment_reference', JSON.stringify(lineCols[0]));
        check('nothing typed on a charge is in any column', !JSON.stringify(lineCols).includes('INTERNAL'));

        const unsentOn = await shipment();
        await paymentOn(A, [{ shipment_id: unsentOn }], { sent: false });
        check('a payment that was never sent shows its container to nobody', (await rowsOf(unsentOn)).length === 0);

        await paymentOn(B, [{ shipment_id: booked }]);
        rows = await rowsOf(booked);
        check('another forwarder paid on a booked container: the owner keeps the full row, the other gets a payment-only row', rows.length === 2
            && rows.some(r => Number(r.forwarder_id) === A && r.link_source === 'booking') && rows.some(r => Number(r.forwarder_id) === B && r.link_source === 'payment'), JSON.stringify(rows.map(r => [r.forwarder_id, r.link_source])));
        const payRow = rows.find(r => r.link_source === 'payment');
        check('and that other forwarder learns nothing of the container', Boolean(payRow) && CONTAINER_FACTS.every(c => payRow[c] === null));

        await paymentOn(A, [{ shipment_id: byHand }]);
        rows = await rowsOf(byHand);
        check('the owner paid on its own container: one row, the full one', rows.length === 1 && rows[0].link_source === 'container', JSON.stringify(rows.map(r => [r.forwarder_id, r.link_source])));

        const goneContainer = await shipment({ deleted_at: '2026-11-01 10:00:00' });
        const draftContainer = await shipment({ stage: 'DRAFT', reference: null, open_key: `D:${TAG} paid` });
        await paymentOn(A, [{ shipment_id: goneContainer }, { shipment_id: draftContainer }]);
        check('a payment on a deleted container, or on one not yet booked: no row for either', (await rowsOf(goneContainer)).length === 0 && (await rowsOf(draftContainer)).length === 0);

        const byRef = await shipment();
        const [{ reference: refOnly }] = await q('SELECT reference FROM shipments WHERE id = ?', [byRef]);
        const p5 = await paymentOn(A, [{ shipment_id: null, shipment_reference: refOnly }]);
        const refLine = await q('SELECT shipment_id, shipment_reference FROM portal_forwarder_payment_lines_v WHERE payment_id = ?', [p5.id]);
        check('a charge that names its container by number only is found by that number', refLine.length === 1 && Number(refLine[0].shipment_id) === byRef && refLine[0].shipment_reference === refOnly, JSON.stringify(refLine));
        check('and that container is shown for the payment', (await rowsOf(byRef)).some(r => Number(r.forwarder_id) === A && r.link_source === 'payment'));

        const two = await shipment();
        const three = await shipment();
        const p6 = await paymentOn(A, [{ shipment_id: two }, { shipment_id: three }]);
        check('one transfer for two containers shows on both', (await rowsOf(two)).length === 1 && (await rowsOf(three)).length === 1);
        await q('UPDATE payment_extras SET forwarder_id = ? WHERE id = ?', [B, p6.extras[0]]);
        check('a payment that is no longer one forwarder\'s shows neither container', (await rowsOf(two)).length === 0 && (await rowsOf(three)).length === 0);
        await q('UPDATE payment_extras SET forwarder_id = ? WHERE id = ?', [B, p6.extras[1]]);
        check('moved whole to another forwarder it was never sent to: still neither', (await rowsOf(two)).length === 0 && (await rowsOf(three)).length === 0);
        await send(p6.id, B);
        rows = [...await rowsOf(two), ...await rowsOf(three)];
        check('sent to that forwarder: both containers are theirs for the payment, and not the first forwarder\'s', rows.length === 2 && rows.every(r => Number(r.forwarder_id) === B && r.link_source === 'payment'), JSON.stringify(rows.map(r => [r.shipment_id, r.forwarder_id, r.link_source])));

        console.log('\nOne forwarder never sees another\'s');
        const mineA = await q('SELECT shipment_id FROM portal_forwarder_containers_v WHERE forwarder_id = ?', [A]);
        const mineB = await q('SELECT shipment_id, link_source FROM portal_forwarder_containers_v WHERE forwarder_id = ?', [B]);
        check('every row a forwarder is listed has that forwarder\'s id and one of this run\'s containers', mineA.length > 0 && mineB.length > 0
            && [...mineA, ...mineB].every(r => made.shipments.includes(Number(r.shipment_id))));
        check('the removed forwarder is listed nothing', (await q('SELECT 1 FROM portal_forwarder_containers_v WHERE forwarder_id = ?', [C])).length === 0);
        const pairs = await q('SELECT shipment_id, forwarder_id, COUNT(*) AS n FROM portal_forwarder_containers_v WHERE shipment_id IN (?) GROUP BY shipment_id, forwarder_id HAVING n > 1', [made.shipments]);
        check('never two rows for one container and forwarder', pairs.length === 0, JSON.stringify(pairs));
        const sources = await q('SELECT DISTINCT link_source FROM portal_forwarder_containers_v WHERE shipment_id IN (?)', [made.shipments]);
        check('link_source is one of booking, container, duty, payment', sources.every(r => ['booking', 'container', 'duty', 'payment'].includes(r.link_source)), JSON.stringify(sources));
        const odd = await q(
            `SELECT shipment_id, forwarder_id, link_source, does_freight, does_delivery, does_customs FROM portal_forwarder_containers_v
              WHERE shipment_id IN (?) AND NOT (
                    (link_source IN ('booking', 'container') AND does_freight = 1)
                 OR (link_source = 'duty' AND does_freight = 0 AND does_delivery + does_customs >= 1)
                 OR (link_source = 'payment' AND does_freight + does_delivery + does_customs = 0))`, [made.shipments]);
        check('"booking" and "container" are the freight holder\'s rows and nobody else\'s: what an older portal build may write for', odd.length === 0, JSON.stringify(odd));
    } catch (e) {
        failed++;
        console.error('\nTHE RUN STOPPED:', e);
    } finally {
        const order = ['supplier_payment_sends', 'supplier_payment_lines', 'supplier_payments', 'payment_extras', 'freight_bookings', 'freight_quote_options',
            'freight_quote_requests', 'orders', 'shipments', 'forwarders'];
        for (const table of order) {
            if (!made[table].length) continue;
            try { await conn.query(`DELETE FROM ${table} WHERE id IN (?)`, [made[table]]); } catch (e) { failed++; console.error(`COULD NOT DELETE fixtures from ${table} (ids ${made[table].join(', ')}):`, e.message); }
        }
        conn.release();
        await closePool();
    }
    console.log(`\n${passed} passed, ${failed} failed. Fixtures tagged "${TAG}" removed.`);
    process.exit(failed ? 1 : 0);
})().catch(async (e) => { console.error('FAILED:', e); process.exit(1); });
