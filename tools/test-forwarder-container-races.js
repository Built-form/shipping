'use strict';
// A forwarder's container page (docs/forwarder-containers.md): what only two
// connections on a real database can show. That a change of ownership and a
// write that depends on ownership can never both go through on stale reads:
//
//   - THE PORTAL'S WRITES are single INSERT ... SELECT statements through the
//     view portal_forwarder_containers_v (jportal's src/handlers/
//     forwarderContainers.js). Under REPEATABLE READ such a statement reads the
//     rows it selects with shared locks, so it WAITS for an uncommitted change
//     (a forwarder picked on the container, a booking being confirmed, a
//     contact being removed) and then sees its result. The whole design of
//     those writes rests on this; it is checked here on the real view.
//
//   - SHIPPING'S DECISIONS (claim, finish, accept) lock the shipment row, its
//     live booking and, for a claim or a finish, its member orders, and read
//     ownership on those locked rows. Checked here in both orders: a decision
//     that meets an uncommitted change waits and then reads its result (so
//     its read of the container comes AFTER its locks), and a change that
//     meets a decision in progress waits for it. Also a whole accept, a
//     claim that lapses, and a deadlock tried once more.
//
//   - A FORWARDER'S FILES: the accept route itself against the portal's own
//     withdrawal, in each order (the accept waits for an uncommitted
//     withdrawal; a withdrawal waits for an accept that holds its locks; one
//     that lands between our copy and the decision), against a change of
//     forwarder, and two accepts at once on two connections (filed once, both
//     told the same row). The two buckets are a stand-in: no file is sent.
//
//   - THREE DUTIES (2026-11-24): a container's freight, delivery and customs can
//     be three companies'. The same two things, for a company that delivers: its
//     write waits for a change of the delivery pick, of a booking's scope, of its
//     own place on the Forwarders list; a decision on a delivery date waits for
//     them too, and a finish is refused when the duty moved after the claim, a
//     delivery date was typed on an order, or an order joined or left. Also: a
//     person who is a contact of two companies on one container writes only for
//     the one the page's block names; a portal build from BEFORE duties, with its
//     own statement, cannot write for a company that does not carry the freight;
//     and a ShipLine build from before the delivery date cannot decide one.
//
// It WRITES: fixture rows of its own (forwarders, contacts, shipments, orders,
// freight requests, options and bookings, rows of the portal's two tables, and
// the forwarder documents its accepts file),
// every one deleted by id at the end, pass or fail. TEST only: it refuses a
// host that does not look like TEST and needs the host said back to it. The
// fixture orders carry container numbers that are not real ones.
//
//   node tools/test-forwarder-container-races.js --confirm-host <DB_HOST>
//
// Needs the migrations of 2026-11-23 and 2026-11-24 applied here, and the
// portal's (jportal's 2026-11-23_jportal_forwarder_containers.sql). PART OF THE
// RELEASE GATE of the duties work: run it on TEST after its migration, before
// anything is deployed. If the portal's statement does not WAIT on the regrouped
// view, the release stops there.
require('dotenv').config();
const crypto = require('crypto');
const mysql = require('mysql2/promise');
const { getPool, closePool } = require('../src/db');
const { jportalSchema } = require('../src/lib/jportal');
const S = require('../src/lib/shipments');
const FC = require('../src/lib/forwarder-containers');
const FL = require('../src/lib/forwarder-links');
const sync = require('../src/services/shipment-sync');
const R = require('../src/services/forwarder-container-routes');
const U = require('../src/services/forwarder-upload-routes');
const SW = require('../src/lib/forwarder-sweep');

const host = process.env.DB_HOST || '';
const confirm = process.argv[process.argv.indexOf('--confirm-host') + 1];
const HEX = crypto.randomBytes(4).toString('hex');
const TAG = `ZZ fwd-race-test ${HEX}`;
const wait = ms => new Promise(r => setTimeout(r, ms));

let passed = 0;
let failed = 0;
const check = (name, ok, detail = '') => {
    if (ok) { passed++; console.log(`  ok    ${name}`); } else { failed++; console.log(`  FAIL  ${name}${detail ? `  [${detail}]` : ''}`); }
};

(async () => {
    if (!/test/i.test(host)) { console.error(`Refusing: ${host || '(DB_HOST unset)'} does not look like the TEST database.`); process.exit(2); }
    if (confirm !== host) { console.error(`Refusing to write fixtures: pass --confirm-host ${host}`); process.exit(2); }
    const pool = getPool();
    const conn = await pool.getConnection();
    // A connection of its own for the other side of a race: the app's pool holds one
    // connection only (src/db/index.js), so a second one asked of it would wait for ever.
    const other = await mysql.createConnection({
        host: process.env.DB_HOST, user: process.env.DB_USER, password: process.env.DB_PASSWORD, database: process.env.DB_NAME,
        port: Number(process.env.DB_PORT || 3306), ssl: { rejectUnauthorized: false },
    });
    const jp = jportalSchema();
    const made = { forwarders: [], forwarder_contacts: [], shipments: [], orders: [], freight_quote_requests: [], freight_quote_options: [], freight_bookings: [] };
    const madeDetails = [];
    const q = async (sql, params) => (await conn.query({ sql, dateStrings: true }, params))[0];
    const ins = async (table, row) => {
        const cols = Object.keys(row);
        const r = await q(`INSERT INTO ${table} (${cols.join(', ')}) VALUES (${cols.map(() => '?').join(', ')})`, cols.map(c => row[c]));
        made[table].push(r.insertId);
        return r.insertId;
    };
    let n = 0;
    const forwarder = name => ins('forwarders', { name: `${TAG} ${name}`, created_by_email: 'race-test@tools' });
    const contactOf = async (forwarderId) => {
        n += 1;
        const email = `race.${HEX}.${n}@race-test.example`;
        await ins('forwarder_contacts', { forwarder_id: forwarderId, name: 'Race Test', email });
        return email;
    };
    const shipment = (over = {}) => {
        n += 1;
        return ins('shipments', { reference: `ZZR${HEX}-${n}`, name: `${TAG} ${n}`, mode: 'SEA', stage: 'BOOKED', origin: 'api', created_by_email: 'race-test@tools', ...over });
    };
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
        return ins('freight_bookings', { request_id: request, option_id: option, status, requested_at: '2026-11-20 10:00:00', shipment_id: shipmentId, ...over });
    };

    // The portal's own statement for a container's number and dates (jportal
    // src/handlers/forwarderContainers.js proposalSql.insert), word for word in
    // its WHERE: the row exists only if, in this statement's own read, the
    // container is in full the company's the page's block is for (the handler
    // finds that company from the block's key, among the caller's own rows), that
    // company holds the duties the fields sent need, the container takes
    // submissions, and the caller is one of that company's contacts.
    const PORTAL_INSERT = `
        INSERT INTO ${jp}.forwarder_container_details
               (shipment_id, container_ref, forwarder_id, submit_key, user_id, user_email, user_name, base_json, requested_json, note)
        SELECT c.shipment_id, c.reference, c.forwarder_id, ?, 0, ?, 'Race Test', '{}', ?, NULL
          FROM portal_forwarder_containers_v c
         WHERE c.shipment_id = ?
           AND c.link_source IN ('booking', 'container', 'duty')
           AND c.finished = 0
           AND c.forwarder_id = ?
           AND (? = 0 OR c.does_freight = 1)
           AND (? = 0 OR c.does_delivery = 1)
           AND c.forwarder_id IN (SELECT fc.forwarder_id FROM forwarder_contacts fc WHERE fc.email = ? AND fc.deleted_at IS NULL)`;
    // That statement's parameters: who sends, for which company, and what the fields sent need.
    const portalParams = ({ email, shipmentId, forwarderId, requested = { ecn: 'ZZZU0000000' } }) => {
        const needs = duty => (Object.keys(requested).some(f => FC.FIELD_DUTY[f] === duty) ? 1 : 0);
        return [crypto.randomUUID(), email, JSON.stringify(requested), shipmentId, forwarderId, needs('freight'), needs('delivery'), email];
    };
    // The statement of a portal build from BEFORE duties (2026-11-23), which runs
    // against the new view until that build is replaced: it names no company and
    // knows two kinds of full row. Params: key, email, requested, shipmentId, email.
    const PORTAL_INSERT_OLD = `
        INSERT INTO ${jp}.forwarder_container_details
               (shipment_id, container_ref, forwarder_id, submit_key, user_id, user_email, user_name, base_json, requested_json, note)
        SELECT c.shipment_id, c.reference, c.forwarder_id, ?, 0, ?, 'Race Test', '{}', ?, NULL
          FROM portal_forwarder_containers_v c
         WHERE c.shipment_id = ?
           AND c.link_source IN ('booking', 'container')
           AND c.finished = 0
           AND c.forwarder_id IN (SELECT fc.forwarder_id FROM forwarder_contacts fc WHERE fc.email = ? AND fc.deleted_at IS NULL)`;
    // Runs the portal's write on `conn` while `other` holds an uncommitted change.
    // → { waited, rows }: whether it was still waiting after `holdMs`, and what it wrote once released.
    const portalWriteDuring = async ({ shipmentId, email, forwarderId, requested, change, release, holdMs = 1500 }) => {
        await other.query('START TRANSACTION');
        await change();
        let settled = false;
        const write = conn.query(PORTAL_INSERT, portalParams({ email, shipmentId, forwarderId, requested })).then((r) => { settled = true; return r[0]; });
        await wait(holdMs);
        const waited = !settled;
        await other.query(release);
        const res = await write;
        if (res.insertId) madeDetails.push(res.insertId);
        return { waited, rows: res.affectedRows };
    };

    try {
        console.log(`Database: ${process.env.DB_USER || '?'}@${host}/${process.env.DB_NAME || '?'}   fixtures tagged "${TAG}"`);
        const [[iso]] = await conn.query('SELECT @@transaction_isolation AS level');
        check('the server runs REPEATABLE READ, which is what the waits below rest on', iso.level === 'REPEATABLE-READ', iso.level);
        await conn.query('SET SESSION innodb_lock_wait_timeout = 20');
        await other.query('SET SESSION innodb_lock_wait_timeout = 20');

        const A = await forwarder('A');
        const B = await forwarder('B');
        const emailA = await contactOf(A);

        console.log('\nThe portal\'s write against a change of ownership that is not committed yet');
        const s1 = await shipment({ forwarder_id: A, forwarder: `${TAG} A` });
        const plain = await conn.query(PORTAL_INSERT, portalParams({ email: emailA, shipmentId: s1, forwarderId: A }));
        if (plain[0].insertId) madeDetails.push(plain[0].insertId);
        check('with nothing in the way, the container\'s own forwarder writes one row', plain[0].affectedRows === 1);

        let r = await portalWriteDuring({
            shipmentId: s1, email: emailA, forwarderId: A,
            change: () => other.query('UPDATE shipments SET forwarder_id = ?, forwarder = ? WHERE id = ?', [B, `${TAG} B`, s1]),
            release: 'COMMIT',
        });
        check('it WAITS while the container is being given to another forwarder', r.waited);
        check('and once that is committed it writes nothing', r.rows === 0, `rows ${r.rows}`);

        const s2 = await shipment({ forwarder_id: A, forwarder: `${TAG} A` });
        r = await portalWriteDuring({
            shipmentId: s2, email: emailA, forwarderId: A,
            change: () => other.query('UPDATE shipments SET forwarder_id = ?, forwarder = ? WHERE id = ?', [B, `${TAG} B`, s2]),
            release: 'ROLLBACK',
        });
        check('a change that is rolled back: it waited, then wrote its row', r.waited && r.rows === 1, JSON.stringify(r));

        const s3 = await shipment({ forwarder_id: A, forwarder: `${TAG} A` });
        r = await portalWriteDuring({
            shipmentId: s3, email: emailA, forwarderId: A,
            change: () => other.query('UPDATE shipments SET deleted_at = UTC_TIMESTAMP() WHERE id = ?', [s3]),
            release: 'COMMIT',
        });
        check('a container being deleted: it waits, then writes nothing', r.waited && r.rows === 0, JSON.stringify(r));

        const s4 = await shipment();
        const b4 = await booking(s4, A, 'requested');
        const before = await conn.query(PORTAL_INSERT, portalParams({ email: emailA, shipmentId: s4, forwarderId: A }));
        check('a booking not confirmed yet: nothing can be written', before[0].affectedRows === 0);
        r = await portalWriteDuring({
            shipmentId: s4, email: emailA, forwarderId: A,
            change: () => other.query('UPDATE freight_bookings SET status = \'confirmed\' WHERE id = ?', [b4]),
            release: 'COMMIT',
        });
        check('a booking being confirmed: once it is committed the write goes through', r.rows === 1, JSON.stringify(r));

        const s5 = await shipment();
        const b5 = await booking(s5, A, 'confirmed');
        r = await portalWriteDuring({
            shipmentId: s5, email: emailA, forwarderId: A,
            change: () => other.query('UPDATE freight_bookings SET status = \'cancelled\' WHERE id = ?', [b5]),
            release: 'COMMIT',
        });
        check('a confirmed booking being cancelled: it waits, then writes nothing', r.waited && r.rows === 0, JSON.stringify(r));

        const s6 = await shipment({ forwarder_id: A, forwarder: `${TAG} A` });
        const emailGone = await contactOf(A);
        r = await portalWriteDuring({
            shipmentId: s6, email: emailGone, forwarderId: A,
            change: () => other.query('UPDATE forwarder_contacts SET deleted_at = UTC_TIMESTAMP() WHERE email = ?', [emailGone]),
            release: 'COMMIT',
        });
        check('a contact being removed: it waits, then writes nothing', r.waited && r.rows === 0, JSON.stringify(r));

        const s7 = await shipment({ forwarder_id: A, forwarder: `${TAG} A` });
        r = await portalWriteDuring({
            shipmentId: s7, email: emailA, forwarderId: A,
            change: () => other.query('UPDATE forwarders SET deleted_at = UTC_TIMESTAMP() WHERE id = ?', [B]),
            release: 'COMMIT',
        });
        check('a change to ANOTHER forwarder does not stop it writing', r.rows === 1, JSON.stringify(r));
        // ── shipping's decisions: claim, finish, send back ─────────────────
        // The decisions themselves (src/services/forwarder-container-routes.js),
        // run on `conn` as the routes run them, against `other`. The dates a
        // finish writes go through a stand-in here (plain UPDATEs of the same
        // rows): PATCH /shipments/:id's own body needs the whole app around it.
        const C = await forwarder('C');
        const D = await forwarder('D');
        const emailC = await contactOf(C);
        const actor = { email: 'race-test@tools', userType: 'admin' };
        const order = (shipmentId, over = {}) => ins('orders', {
            jf_code: 'ZZRACETEST', product_name: 'Forwarder container race test', quantity: 1, unit_price: 0, status: 'CONSOLIDATED', shipment_id: shipmentId, ...over,
        });
        const ownedByC = (over = {}) => shipment({ forwarder_id: C, forwarder: `${TAG} C`, ...over });
        const propose = async (shipmentId, requested, forwarderId = C) => {
            const res = await q(
                `INSERT INTO ${jp}.forwarder_container_details
                        (shipment_id, container_ref, forwarder_id, submit_key, user_id, user_email, user_name, base_json, requested_json)
                 VALUES (?, 'ZZR', ?, ?, 0, ?, 'Race Test', '{}', ?)`,
                [shipmentId, forwarderId, crypto.randomUUID(), emailC, JSON.stringify(requested)]
            );
            madeDetails.push(res.insertId);
            return res.insertId;
        };
        // What a ShipLine build from before the delivery date names in a claim: the three fields.
        const heldNow = async (shipmentId) => {
            const api = S.rowToShipment(await sync.getShipment(conn, shipmentId));
            return { ecn: api.trackingRef || null, loadingDate: api.loadingDate || null, etd: api.etd || null };
        };
        // The delivery value each order of a container holds, in the one form.
        const deliveriesOf = async shipmentId => (await q(`SELECT ${FC.deliverySelect('delivery_date')} AS d FROM orders WHERE shipment_id = ? AND deleted_at IS NULL ORDER BY id`, [shipmentId])).map(x => x.d);
        // What the present build names: those, and the delivery date as every order holds it.
        const heldAll = async shipmentId => ({ ...(await heldNow(shipmentId)), ...FC.heldDelivery(await deliveriesOf(shipmentId)) });
        // What the FORWARDER tab reads of one attached forwarder's proposals: { proposal, history }.
        const tabOf = async (shipmentId, forwarderId) => {
            const duties = FL.dutiesOfRow(await sync.getShipment(conn, shipmentId));
            const read = await R.readDetails(conn, shipmentId, duties);
            return read.byForwarder.get(Number(forwarderId)) || { proposal: null, history: [] };
        };
        const rowOf = async id => (await q(`SELECT status, claim_token, claimed_by, decided_by, decided_note, applied_json FROM ${jp}.forwarder_container_details WHERE id = ?`, [id]))[0];
        const audits = [];
        const recordAudit = async (c, entry) => { audits.push(entry); };
        const patchShipment = async (c, id, body) => {
            if (body.loadingDate) await c.query('UPDATE shipments SET loading_date = ? WHERE id = ?', [body.loadingDate, id]);
            if (body.etd) {
                await c.query('UPDATE shipments SET etd = ? WHERE id = ?', [body.etd, id]);
                await c.query('UPDATE orders SET estimated_departure_date = ? WHERE shipment_id = ? AND deleted_at IS NULL', [body.etd, id]);
            }
            // A delivery date lives on the member orders alone, to the minute.
            if (body.deliveryDate) await c.query('UPDATE orders SET delivery_date = ? WHERE shipment_id = ? AND deleted_at IS NULL', [FC.deliverySql(body.deliveryDate), id]);
        };
        const claim = (id, expected, deps = {}) => R.claimDetails(conn, { id, expected, actor }, deps);
        const finish = (id, claimToken, ecnReset = null) => R.finishDetails(conn, { id, claimToken, ecnReset, actor }, { patchShipment, recordAudit });
        const outcome = p => p.then(result => ({ result }), error => ({ error }));
        const codeOf = o => (o.error ? (o.error.code || String(o.error.message).slice(0, 80)) : 'went through');
        // A decision on `conn` while `other` holds a change it has not committed.
        const decideDuring = async ({ decide, change, release, holdMs = 1500 }) => {
            await other.query('START TRANSACTION');
            await change();
            let settled = false;
            const run = outcome(decide()).finally(() => { settled = true; });
            await wait(holdMs);
            const waited = !settled;
            await other.query(release);
            return { waited, ...(await run) };
        };
        // A change on `other` while a claim on `conn` holds its locks: the claim is
        // slowed down AFTER its locks (its read of the container), so the change
        // arrives in the middle of it. → { blocked, claim, change }
        const changeDuringClaim = async ({ id, expected, change }) => {
            const slow = { getShipment: async (c, sid) => { await wait(1800); return sync.getShipment(c, sid); } };
            const claiming = outcome(claim(id, expected, slow));
            await wait(500);
            let settled = false;
            const changing = outcome(change()).finally(() => { settled = true; });
            await wait(600);
            const blocked = !settled;
            return { blocked, claim: await claiming, change: await changing };
        };

        console.log('\nA claim against a change that is not committed yet');
        const c1 = await ownedByC();
        await order(c1, { estimated_departure_date: '2026-12-20' });
        const p1 = await propose(c1, { loadingDate: '2026-12-15' });
        r = await decideDuring({
            decide: async () => claim(p1, await heldNow(c1)),
            change: () => other.query('UPDATE shipments SET forwarder_id = ?, forwarder = ? WHERE id = ?', [D, `${TAG} D`, c1]),
            release: 'COMMIT',
        });
        check('a claim WAITS while the container is being given to another forwarder', r.waited);
        check('and once that is committed it is refused: no longer theirs', codeOf(r) === 'NOT_THEIR_CONTAINER', codeOf(r));
        check('and the proposal is still waiting, with no claim on it', (await rowOf(p1)).status === 'pending' && (await rowOf(p1)).claim_token === null);

        const c2 = await ownedByC();
        await order(c2, { estimated_departure_date: '2026-12-20' });
        const p2 = await propose(c2, { loadingDate: '2026-12-15' });
        r = await decideDuring({
            decide: async () => claim(p2, await heldNow(c2)),
            change: () => other.query('UPDATE shipments SET forwarder_id = ?, forwarder = ? WHERE id = ?', [D, `${TAG} D`, c2]),
            release: 'ROLLBACK',
        });
        check('a change that is rolled back: it waited, then claimed', r.waited && r.result && /^[a-f0-9]{32}$/.test(r.result.claimToken), codeOf(r));
        check('the claim is on the row, with who made it', (await rowOf(p2)).status === 'applying' && (await rowOf(p2)).claimed_by === 'race-test@tools');
        let again = await outcome(claim(p2, await heldNow(c2)));
        check('a second claim while that one stands is refused, and says who holds it', codeOf(again) === 'CLAIM_IN_PROGRESS' && again.error.extra.claimedBy === 'race-test@tools', codeOf(again));
        const wrong = await R.releaseDetails(conn, { id: p2, claimToken: 'f'.repeat(32) });
        check('another token releases nothing', wrong.released === false && (await rowOf(p2)).status === 'applying');
        const let2 = await R.releaseDetails(conn, { id: p2, claimToken: r.result.claimToken });
        check('its own token releases it', let2.released === true && (await rowOf(p2)).status === 'pending' && (await rowOf(p2)).claim_token === null);

        const c3 = await ownedByC();
        const o3 = await order(c3, { estimated_departure_date: '2026-12-20' });
        const p3 = await propose(c3, { loadingDate: '2026-12-15' });
        const seen3 = await heldNow(c3);
        r = await decideDuring({
            decide: () => claim(p3, seen3),
            change: () => other.query('UPDATE orders SET external_container_number = ? WHERE id = ?', ['ZZ-RACE-NOT-REAL', o3]),
            release: 'COMMIT',
        });
        check('a claim WAITS while an order of the container is being given a number', r.waited);
        check('and then reads the container as it is AFTER that commit: refused as changed, in ecn', codeOf(r) === 'CONTAINER_CHANGED' && JSON.stringify(r.error.extra.fields) === '["ecn"]',
            `${codeOf(r)} ${r.error ? JSON.stringify(r.error.extra) : ''}`);

        const c4 = await ownedByC();
        await order(c4, { estimated_departure_date: '2026-12-20' });
        const stray4 = await order(null, { estimated_departure_date: '2027-01-05' });
        const p4 = await propose(c4, { loadingDate: '2026-12-15' });
        const seen4 = await heldNow(c4);
        r = await decideDuring({
            decide: () => claim(p4, seen4),
            change: () => other.query('UPDATE orders SET shipment_id = ? WHERE id = ?', [c4, stray4]),
            release: 'COMMIT',
        });
        check('a claim WAITS while an order is joining the container', r.waited);
        check('and then sees it: refused as changed, in the departure date', codeOf(r) === 'CONTAINER_CHANGED' && JSON.stringify(r.error.extra.fields) === '["etd"]',
            `${codeOf(r)} ${r.error ? JSON.stringify(r.error.extra) : ''}`);

        const c5 = await ownedByC();
        const b5c = await booking(c5, C, 'confirmed');
        await order(c5, { estimated_departure_date: '2026-12-20' });
        const p5 = await propose(c5, { loadingDate: '2026-12-15' });
        const seen5 = await heldNow(c5);
        r = await decideDuring({
            decide: () => claim(p5, seen5),
            change: () => other.query('UPDATE freight_bookings SET status = \'cancelled\' WHERE id = ?', [b5c]),
            release: 'ROLLBACK',
        });
        check('a claim WAITS while the container\'s booking is being cancelled, and goes through when that is rolled back', r.waited && Boolean(r.result), codeOf(r));
        await R.releaseDetails(conn, { id: p5, claimToken: r.result ? r.result.claimToken : '' });

        console.log('\nA change that arrives while a claim holds its locks');
        const c6 = await ownedByC();
        const o6 = await order(c6, { estimated_departure_date: '2026-12-20' });
        const p6 = await propose(c6, { loadingDate: '2026-12-15' });
        let d = await changeDuringClaim({
            id: p6, expected: await heldNow(c6),
            change: () => other.query('UPDATE shipments SET forwarder_id = ?, forwarder = ? WHERE id = ?', [D, `${TAG} D`, c6]),
        });
        check('giving the container to another forwarder WAITS for the claim', d.blocked);
        check('the claim, first, goes through; the change after it', Boolean(d.claim.result) && !d.change.error, `${codeOf(d.claim)} / ${codeOf(d.change)}`);
        let f = await outcome(finish(p6, d.claim.result ? d.claim.result.claimToken : ''));
        check('and the finish of that claim is refused: the container is no longer theirs', codeOf(f) === 'NOT_THEIR_CONTAINER', codeOf(f));
        const [[ship6]] = await conn.query({ sql: 'SELECT loading_date FROM shipments WHERE id = ?', dateStrings: true }, [c6]);
        check('with no date written', ship6.loading_date === null, String(ship6.loading_date));

        const c7 = await ownedByC();
        const o7 = await order(c7, { estimated_departure_date: '2026-12-20' });
        const stray7 = await order(null, { estimated_departure_date: '2027-01-05' });
        const p7 = await propose(c7, { loadingDate: '2026-12-15' });
        d = await changeDuringClaim({ id: p7, expected: await heldNow(c7), change: () => other.query('UPDATE orders SET shipment_id = ? WHERE id = ?', [c7, stray7]) });
        check('an order joining the container WAITS for the claim', d.blocked && Boolean(d.claim.result) && !d.change.error, `${d.blocked} ${codeOf(d.claim)} / ${codeOf(d.change)}`);
        f = await outcome(finish(p7, d.claim.result ? d.claim.result.claimToken : ''));
        check('and the finish then sees the order that joined: refused as changed, in the departure date', codeOf(f) === 'CONTAINER_CHANGED' && JSON.stringify(f.error.extra.fields) === '["etd"]', codeOf(f));

        const c8 = await ownedByC();
        const o8 = await order(c8, { estimated_departure_date: '2026-12-20' });
        const p8 = await propose(c8, { loadingDate: '2026-12-15' });
        d = await changeDuringClaim({ id: p8, expected: await heldNow(c8), change: () => other.query('UPDATE orders SET estimated_departure_date = ? WHERE id = ?', ['2026-12-28', o8]) });
        check('an edit of a member order WAITS for the claim', d.blocked && Boolean(d.claim.result) && !d.change.error, `${d.blocked} ${codeOf(d.claim)} / ${codeOf(d.change)}`);

        const c9 = await ownedByC();
        await order(c9, { estimated_departure_date: '2026-12-20' });
        const p9 = await propose(c9, { loadingDate: '2026-12-15' });
        let newer = null;
        d = await changeDuringClaim({
            id: p9, expected: await heldNow(c9),
            change: async () => {
                const [res] = await other.query(PORTAL_INSERT, portalParams({ email: emailC, shipmentId: c9, forwarderId: C, requested: { loadingDate: '2026-12-16' } }));
                newer = res;
                if (res.insertId) madeDetails.push(res.insertId);
            },
        });
        check('a newer proposal sent from the portal during a claim WAITS for it, then is written', d.blocked && newer && newer.affectedRows === 1, `${d.blocked} ${JSON.stringify(newer)} ${codeOf(d.change)}`);
        f = await outcome(finish(p9, d.claim.result ? d.claim.result.claimToken : ''));
        check('the claimed proposal still finishes', Boolean(f.result) && (await rowOf(p9)).status === 'applied', codeOf(f));
        const tab9 = await tabOf(c9, C);
        check('and the newer one is the live proposal, waiting', Boolean(newer) && tab9.proposal && tab9.proposal.id === newer.insertId && tab9.proposal.waiting === true && tab9.history.some(h => h.id === p9 && h.status === 'applied'),
            JSON.stringify(tab9.proposal && { id: tab9.proposal.id, waiting: tab9.proposal.waiting }));
        again = await outcome(claim(p9, await heldNow(c9)));
        check('the older one cannot be claimed again', codeOf(again) === 'ALREADY_DECIDED', codeOf(again));

        console.log('\nA whole accept, for an air shipment (its number is a made-up air waybill, never a container number)');
        // Upper case: a proposal's number is kept as the portal sends it, in upper case.
        const AWB = `ZZ-RACE-${HEX.toUpperCase()}`;
        // MySQL keeps a JSON object's keys in its own order.
        const sameObject = (a, b) => JSON.stringify(Object.entries(a || {}).sort()) === JSON.stringify(Object.entries(b || {}).sort());
        const c10 = await ownedByC({ mode: 'AIR' });
        const o10a = await order(c10);
        const o10b = await order(c10);
        const asked10 = { ecn: AWB, loadingDate: '2026-12-15', etd: '2026-12-20' };
        const p10 = await propose(c10, asked10);
        const seen10 = await heldNow(c10);
        const got10 = await outcome(claim(p10, seen10));
        check('claimed, with what the container held', Boolean(got10.result) && JSON.stringify(got10.result.held) === JSON.stringify(seen10) && got10.result.expiresInSeconds === 600, codeOf(got10));
        const token10 = got10.result ? got10.result.claimToken : '';
        f = await outcome(finish(p10, token10));
        check('finish before the number is on the orders: refused', codeOf(f) === 'NOT_ON_CONTAINER', codeOf(f));
        await q('UPDATE orders SET awb_number = ? WHERE id = ?', [AWB, o10a]);
        f = await outcome(finish(p10, token10));
        check('finish with the number on one order of two: refused', codeOf(f) === 'NOT_ON_CONTAINER', codeOf(f));
        check('a refused finish leaves the claim standing and writes no date', (await rowOf(p10)).status === 'applying' && (await heldNow(c10)).loadingDate === null);
        await q('UPDATE orders SET awb_number = ? WHERE id = ?', [AWB, o10b]);
        f = await outcome(finish(p10, 'f'.repeat(32)));
        check('finish with another token: refused', codeOf(f) === 'CLAIM_LOST', codeOf(f));
        f = await outcome(finish(p10, token10));
        check('finish with the number on every order: accepted', Boolean(f.result) && f.result.status === 'applied', codeOf(f));
        const after10 = await heldNow(c10);
        check('the container holds the number and both dates', JSON.stringify(after10) === JSON.stringify(asked10), JSON.stringify(after10));
        const row10 = await rowOf(p10);
        check('the proposal is accepted, by whom, with what was applied, and its token is gone',
            row10.status === 'applied' && row10.decided_by === 'race-test@tools' && row10.claim_token === null
                && sameObject(typeof row10.applied_json === 'string' ? JSON.parse(row10.applied_json) : row10.applied_json, asked10),
            JSON.stringify(row10));
        check('and audited once', audits.filter(a => a.entityId === p10 && a.action === 'applied').length === 1);
        f = await outcome(finish(p10, token10));
        check('the same finish again changes nothing', codeOf(f) === 'CLAIM_LOST', codeOf(f));

        console.log('\nThe last order leaves between claim and finish');
        const c11 = await ownedByC({ mode: 'AIR' });
        const o11 = await order(c11, { awb_number: AWB });
        const p11 = await propose(c11, { loadingDate: '2026-12-15' });
        const got11 = await outcome(claim(p11, await heldNow(c11)));
        await q('UPDATE orders SET shipment_id = NULL WHERE id = ?', [o11]);
        f = await outcome(finish(p11, got11.result ? got11.result.claimToken : ''));
        check('refused: what the container holds changed (its number went with the order)', codeOf(f) === 'CONTAINER_CHANGED', codeOf(f));

        console.log('\nA claim nobody finished');
        const c12 = await ownedByC();
        await order(c12, { estimated_departure_date: '2026-12-20' });
        const p12 = await propose(c12, { loadingDate: '2026-12-15' });
        const got12 = await outcome(claim(p12, await heldNow(c12)));
        await q(`UPDATE ${jp}.forwarder_container_details SET claimed_at = UTC_TIMESTAMP() - INTERVAL 9 MINUTE WHERE id = ?`, [p12]);
        again = await outcome(claim(p12, await heldNow(c12)));
        check('nine minutes old: it still stands', codeOf(again) === 'CLAIM_IN_PROGRESS', codeOf(again));
        await q(`UPDATE ${jp}.forwarder_container_details SET claimed_at = UTC_TIMESTAMP() - INTERVAL 11 MINUTE WHERE id = ?`, [p12]);
        f = await outcome(finish(p12, got12.result ? got12.result.claimToken : ''));
        check('eleven minutes old: its finish is refused', codeOf(f) === 'CLAIM_LOST', codeOf(f));
        const tab12 = await tabOf(c12, C);
        check('the tab reads it as waiting again', tab12.proposal && tab12.proposal.status === 'pending' && tab12.proposal.waiting === true && tab12.proposal.claimedBy === null);
        again = await outcome(claim(p12, await heldNow(c12)));
        check('and it can be claimed again, with a new token', Boolean(again.result) && again.result.claimToken !== got12.result.claimToken, codeOf(again));
        const sentBack = await outcome(R.rejectDetails(conn, { id: p12, note: 'Race test', actor }, { recordAudit }));
        check('while that claim stands it cannot be sent back', codeOf(sentBack) === 'CLAIM_IN_PROGRESS', codeOf(sentBack));
        await R.releaseDetails(conn, { id: p12, claimToken: again.result ? again.result.claimToken : '' });
        const back12 = await outcome(R.rejectDetails(conn, { id: p12, note: 'Race test: wrong date', actor }, { recordAudit }));
        const row12 = await rowOf(p12);
        check('released, it is sent back with the note and who decided', Boolean(back12.result) && row12.status === 'rejected' && row12.decided_note === 'Race test: wrong date' && row12.decided_by === 'race-test@tools', codeOf(back12));

        console.log('\nA deadlock');
        const c13 = await ownedByC();
        const members13 = [];
        for (let i = 0; i < 6; i++) members13.push(await order(c13, { estimated_departure_date: '2026-12-20' }));
        const p13 = await propose(c13, { loadingDate: '2026-12-15' });
        const seen13 = await heldNow(c13);
        let begins = 0;
        const realBegin = conn.beginTransaction.bind(conn);
        conn.beginTransaction = async (...args) => { begins += 1; return realBegin(...args); };
        // `other` holds the orders (and has written the most, so it is not the one
        // InnoDB rolls back); the claim takes the shipment and waits for the orders;
        // `other` then asks for the shipment: a cycle.
        await other.query('START TRANSACTION');
        for (let i = 0; i < 4; i++) await other.query('UPDATE orders SET notes = ? WHERE id IN (?)', [`race ${i}`, members13]);
        const claiming13 = outcome(claim(p13, seen13));
        await wait(600);
        const cycle = outcome(other.query('UPDATE shipments SET notes = ? WHERE id = ?', ['race', c13]));
        await wait(1200);
        await other.query('ROLLBACK').catch(() => {});
        const got13 = await claiming13;
        const cycled = await cycle;
        conn.beginTransaction = realBegin;
        check('the claim was the one rolled back, and tried once more', begins === 2 && !cycled.error, `transactions ${begins}, other: ${codeOf(cycled)}`);
        check('and then went through', Boolean(got13.result), codeOf(got13));
        if (got13.result) await R.releaseDetails(conn, { id: p13, claimToken: got13.result.claimToken });

        console.log('\nWhat waits, across containers');
        const [waiting] = await conn.query(
            `SELECT d.id, (c.shipment_id IS NOT NULL) AS theirs
               FROM ${jp}.forwarder_container_details d
               LEFT JOIN portal_forwarder_containers_v c
                      ON c.shipment_id = d.shipment_id AND c.forwarder_id = d.forwarder_id AND c.link_source IN ('booking', 'container', 'duty')
              WHERE d.id IN (?)`,
            [[p1, p5, newer ? newer.insertId : 0]]
        );
        const theirs = Object.fromEntries(waiting.map(w => [Number(w.id), Number(w.theirs)]));
        check('the list\'s join: a proposal on a container given to another forwarder is not theirs; one on their own is', theirs[p1] === 0 && theirs[p5] === 1 && theirs[newer ? newer.insertId : 0] === 1, JSON.stringify(theirs));

        // ── three duties: freight, delivery, customs (2026-11-24) ──────────
        // C carries the container (picked by hand, or by booking); G delivers it
        // from the UK port; H is another company that could.
        console.log('\nThree duties: the portal\'s write for a company that delivers');
        const G = await forwarder('G delivers');
        const H = await forwarder('H delivers');
        const emailG = await contactOf(G);
        // One person, a contact of both G and H (the same group's two offices, listed apart).
        const emailBoth = await contactOf(G);
        await ins('forwarder_contacts', { forwarder_id: H, name: 'Race Test', email: emailBoth });
        const DELIVERY = { deliveryDate: '2026-12-28T09:30' };
        const portalWrite = async (params) => {
            const [res] = await conn.query(PORTAL_INSERT, portalParams(params));
            if (res.insertId) madeDetails.push(res.insertId);
            return res.affectedRows;
        };
        const oldPortalWrite = async ({ email, shipmentId, requested = { ecn: 'ZZZU0000000' } }) => {
            const [res] = await conn.query(PORTAL_INSERT_OLD, [crypto.randomUUID(), email, JSON.stringify(requested), shipmentId, email]);
            if (res.insertId) madeDetails.push(res.insertId);
            return res.affectedRows;
        };
        const split = (over = {}) => shipment({ forwarder_id: C, forwarder: `${TAG} C`, delivery_forwarder_id: G, ...over });
        const deliveryTo = (shipmentId, forwarderId) => other.query('UPDATE shipments SET delivery_forwarder_id = ? WHERE id = ?', [forwarderId, shipmentId]);
        const lastSender = async shipmentId => Number((await q(`SELECT forwarder_id FROM ${jp}.forwarder_container_details WHERE shipment_id = ? ORDER BY id DESC LIMIT 1`, [shipmentId]))[0].forwarder_id);

        const d1 = await split();
        check('the company that delivers writes its delivery date', await portalWrite({ email: emailG, shipmentId: d1, forwarderId: G, requested: DELIVERY }) === 1 && await lastSender(d1) === G);
        check('and cannot write the container\'s number: that is the freight forwarder\'s to send', await portalWrite({ email: emailG, shipmentId: d1, forwarderId: G }) === 0);
        check('the freight forwarder cannot write a delivery date', await portalWrite({ email: emailC, shipmentId: d1, forwarderId: C, requested: DELIVERY }) === 0);
        check('nor can the company that delivers write as the freight forwarder: it is not its contact', await portalWrite({ email: emailG, shipmentId: d1, forwarderId: C }) === 0);
        check('a portal build from before duties, with its own statement, writes NOTHING for the company that delivers', await oldPortalWrite({ email: emailG, shipmentId: d1 }) === 0);
        check('and still writes for the freight forwarder, under that forwarder, as before', await oldPortalWrite({ email: emailC, shipmentId: d1 }) === 1 && await lastSender(d1) === C);

        const d2 = await split();
        r = await portalWriteDuring({ shipmentId: d2, email: emailG, forwarderId: G, requested: DELIVERY, change: () => deliveryTo(d2, H), release: 'COMMIT' });
        check('a delivery date WAITS while delivery is being given to another company', r.waited);
        check('and once that is committed it writes nothing', r.rows === 0, `rows ${r.rows}`);
        const d3 = await split();
        r = await portalWriteDuring({ shipmentId: d3, email: emailG, forwarderId: G, requested: DELIVERY, change: () => deliveryTo(d3, H), release: 'ROLLBACK' });
        check('that change rolled back: it waited, then wrote its row', r.waited && r.rows === 1, JSON.stringify(r));

        const d4 = await split();
        await q('UPDATE shipments SET delivery_forwarder_id = ? WHERE id = ?', [H, d4]);
        check('a contact of both companies, sending as the one that no longer delivers: nothing is written', await portalWrite({ email: emailBoth, shipmentId: d4, forwarderId: G, requested: DELIVERY }) === 0);
        check('sending as the one that now does: one row, under that company', await portalWrite({ email: emailBoth, shipmentId: d4, forwarderId: H, requested: DELIVERY }) === 1 && await lastSender(d4) === H);

        const gone = await forwarder('gone delivers');
        const emailOfGone = await contactOf(gone);
        const d5 = await shipment({ forwarder_id: C, forwarder: `${TAG} C`, delivery_forwarder_id: gone });
        r = await portalWriteDuring({
            shipmentId: d5, email: emailOfGone, forwarderId: gone, requested: DELIVERY,
            change: () => other.query('UPDATE forwarders SET deleted_at = UTC_TIMESTAMP() WHERE id = ?', [gone]), release: 'COMMIT',
        });
        check('the company that delivers being taken off the Forwarders list: it waits, then writes nothing', r.waited && r.rows === 0, JSON.stringify(r));

        const d6 = await shipment();
        const b6 = await booking(d6, C, 'confirmed', { scope: 'end_to_end' });
        check('an end-to-end booking: its forwarder writes the delivery date too', await portalWrite({ email: emailC, shipmentId: d6, forwarderId: C, requested: DELIVERY }) === 1);
        r = await portalWriteDuring({
            shipmentId: d6, email: emailC, forwarderId: C, requested: DELIVERY,
            change: () => other.query('UPDATE freight_bookings SET scope = \'port_to_port\' WHERE id = ?', [b6]), release: 'COMMIT',
        });
        check('that booking being set back to port to port: a delivery date WAITS, then writes nothing', r.waited && r.rows === 0, JSON.stringify(r));
        check('and its departure date still goes through: it still carries the freight', await portalWrite({ email: emailC, shipmentId: d6, forwarderId: C, requested: { etd: '2026-12-21' } }) === 1);

        const d7 = await shipment();
        const b7 = await booking(d7, C, 'requested', { scope: 'end_to_end' });
        check('an end-to-end booking not confirmed yet: no delivery date can be written', await portalWrite({ email: emailC, shipmentId: d7, forwarderId: C, requested: DELIVERY }) === 0);
        r = await portalWriteDuring({
            shipmentId: d7, email: emailC, forwarderId: C, requested: DELIVERY,
            change: () => other.query('UPDATE freight_bookings SET status = \'confirmed\' WHERE id = ?', [b7]), release: 'COMMIT',
        });
        check('that booking being confirmed: once it is committed the delivery date goes through', r.rows === 1, JSON.stringify(r));

        console.log('\nThree duties: a decision on a delivery date');
        const noDeliveryOn = async shipmentId => (await deliveriesOf(shipmentId)).every(v => v === null);
        const e1 = await split();
        await order(e1, { estimated_departure_date: '2026-12-20' });
        const q1 = await propose(e1, DELIVERY, G);
        r = await decideDuring({ decide: async () => claim(q1, await heldAll(e1)), change: () => deliveryTo(e1, H), release: 'COMMIT' });
        check('a claim of a delivery date WAITS while delivery is being given to another company', r.waited);
        check('and is then refused: that company does nothing on the container any more', codeOf(r) === 'NOT_THEIR_CONTAINER', codeOf(r));

        const e2 = await split({ customs_forwarder_id: G });
        await order(e2, { estimated_departure_date: '2026-12-20' });
        const q2 = await propose(e2, DELIVERY, G);
        r = await decideDuring({ decide: async () => claim(q2, await heldAll(e2)), change: () => deliveryTo(e2, H), release: 'COMMIT' });
        check('the same when it still handles customs: refused as no longer its duty, naming the delivery date',
            r.waited && codeOf(r) === 'NOT_THEIR_DUTY' && JSON.stringify(r.error.extra.fields) === '["deliveryDate"]', `${codeOf(r)} ${r.error ? JSON.stringify(r.error.extra) : ''}`);
        const tab2 = await tabOf(e2, G);
        check('the tab still lists it under that company, not to be accepted, with the field that is no longer its own',
            Boolean(tab2.proposal) && tab2.proposal.id === q2 && tab2.proposal.actionable === false && JSON.stringify(tab2.proposal.outside) === '["deliveryDate"]', JSON.stringify(tab2.proposal && [tab2.proposal.actionable, tab2.proposal.outside]));

        // A whole accept, on a container whose two orders differ: one holds a delivery date, one none.
        const e3 = await split();
        await order(e3, { estimated_departure_date: '2026-12-20' });
        await order(e3, { estimated_departure_date: '2026-12-20', delivery_date: '2026-12-24 00:00:00' });
        const q3 = await propose(e3, DELIVERY, G);
        again = await outcome(claim(q3, await heldNow(e3)));
        check('a ShipLine build that names no delivery date cannot claim a proposal that carries one', codeOf(again) === 'CLIENT_TOO_OLD', codeOf(again));
        const old3 = await outcome(R.rejectDetails(conn, { id: q3, note: 'Race test: sent back', actor }, { recordAudit }));
        check('nor send it back without naming it', codeOf(old3) === 'CLIENT_TOO_OLD' && (await rowOf(q3)).status === 'pending', codeOf(old3));
        const seen3d = await heldAll(e3);
        check('orders that differ are held as each distinct value, an order with none included',
            seen3d.deliveryDate === null && JSON.stringify([...(seen3d.deliveryMixed || [])].sort()) === JSON.stringify(['2026-12-24', null].sort()), JSON.stringify(seen3d));
        const got3 = await outcome(claim(q3, seen3d));
        check('claimed, with the delivery date named as the card showed it', Boolean(got3.result), codeOf(got3));
        f = await outcome(finish(q3, got3.result ? got3.result.claimToken : ''));
        check('and finished: accepted', Boolean(f.result) && f.result.status === 'applied', codeOf(f));
        check('every order of the container now holds the one delivery date, to the minute', JSON.stringify(await deliveriesOf(e3)) === JSON.stringify(['2026-12-28T09:30', '2026-12-28T09:30']), JSON.stringify(await deliveriesOf(e3)));
        check('and the view gives the company that delivers that date', (await q('SELECT delivery_date FROM portal_forwarder_containers_v WHERE shipment_id = ? AND forwarder_id = ?', [e3, G]))[0].delivery_date === '2026-12-28T09:30');

        const e4 = await split({ customs_forwarder_id: G });
        await order(e4, { estimated_departure_date: '2026-12-20' });
        const q4 = await propose(e4, DELIVERY, G);
        d = await changeDuringClaim({ id: q4, expected: await heldAll(e4), change: () => deliveryTo(e4, H) });
        check('giving delivery to another company WAITS for a claim in progress', d.blocked && Boolean(d.claim.result) && !d.change.error, `${d.blocked} ${codeOf(d.claim)} / ${codeOf(d.change)}`);
        f = await outcome(finish(q4, d.claim.result ? d.claim.result.claimToken : ''));
        check('and the finish of that claim is refused: delivery is no longer that company\'s duty', codeOf(f) === 'NOT_THEIR_DUTY', codeOf(f));
        check('with no delivery date written', await noDeliveryOn(e4));

        const e5 = await split();
        await order(e5, { estimated_departure_date: '2026-12-20' });
        const q5 = await propose(e5, DELIVERY, G);
        const got5 = await outcome(claim(q5, await heldAll(e5)));
        await q('UPDATE shipments SET customs_forwarder_id = ? WHERE id = ?', [H, e5]);
        f = await outcome(finish(q5, got5.result ? got5.result.claimToken : ''));
        check('a change of who handles customs between claim and finish does not undo a delivery date\'s accept: that company still delivers', Boolean(f.result) && f.result.status === 'applied', codeOf(f));

        const e6 = await split();
        const o6a = await order(e6, { estimated_departure_date: '2026-12-20' });
        await order(e6, { estimated_departure_date: '2026-12-20' });
        const q6 = await propose(e6, DELIVERY, G);
        const got6 = await outcome(claim(q6, await heldAll(e6)));
        await q('UPDATE orders SET delivery_date = ? WHERE id = ?', ['2026-12-25 14:00:00', o6a]);
        f = await outcome(finish(q6, got6.result ? got6.result.claimToken : ''));
        check('a delivery date typed on one order between claim and finish: refused as changed, in the delivery date',
            codeOf(f) === 'CONTAINER_CHANGED' && JSON.stringify(f.error.extra.fields) === '["deliveryDate"]', `${codeOf(f)} ${f.error ? JSON.stringify(f.error.extra) : ''}`);
        check('and the other order was given nothing', JSON.stringify(await deliveriesOf(e6)) === JSON.stringify(['2026-12-25T14:00', null]), JSON.stringify(await deliveriesOf(e6)));

        const e7 = await split();
        await order(e7, { estimated_departure_date: '2026-12-20' });
        const joining = await order(null, { estimated_departure_date: '2026-12-20' });
        const q7 = await propose(e7, DELIVERY, G);
        d = await changeDuringClaim({ id: q7, expected: await heldAll(e7), change: () => other.query('UPDATE orders SET shipment_id = ? WHERE id = ?', [e7, joining]) });
        check('an order joining the container WAITS for a claim of a delivery date', d.blocked && Boolean(d.claim.result) && !d.change.error, `${d.blocked} ${codeOf(d.claim)} / ${codeOf(d.change)}`);
        f = await outcome(finish(q7, d.claim.result ? d.claim.result.claimToken : ''));
        check('and the finish then sees it: refused as changed, in its orders, so the order that joined is not given a date nobody saw it get',
            codeOf(f) === 'CONTAINER_CHANGED' && JSON.stringify(f.error.extra.fields) === '["members"]' && await noDeliveryOn(e7), `${codeOf(f)} ${f.error ? JSON.stringify(f.error.extra) : ''}`);

        // The container holds the departure date itself too, so that only its orders change when the order leaves.
        const e8 = await split({ etd: '2026-12-20' });
        const o8d = await order(e8, { estimated_departure_date: '2026-12-20' });
        const q8 = await propose(e8, DELIVERY, G);
        const got8 = await outcome(claim(q8, await heldAll(e8)));
        await q('UPDATE orders SET shipment_id = NULL WHERE id = ?', [o8d]);
        f = await outcome(finish(q8, got8.result ? got8.result.claimToken : ''));
        check('the last order leaving between claim and finish: refused as changed, in its orders', codeOf(f) === 'CONTAINER_CHANGED' && JSON.stringify(f.error.extra.fields) === '["members"]', `${codeOf(f)} ${f.error ? JSON.stringify(f.error.extra) : ''}`);
        await R.releaseDetails(conn, { id: q8, claimToken: got8.result ? got8.result.claimToken : '' });
        again = await outcome(claim(q8, await heldAll(e8)));
        check('and a container with no orders cannot take a delivery date at all', codeOf(again) === 'NO_ORDERS', codeOf(again));

        const e9 = await split();
        await order(e9, { estimated_departure_date: '2026-12-30' });
        const q9 = await propose(e9, DELIVERY, G);
        again = await outcome(claim(q9, await heldAll(e9)));
        check('a delivery date before the departure the container holds: refused, naming both dates',
            codeOf(again) === 'DATES_OUT_OF_ORDER' && JSON.stringify(again.error.extra.fields) === '["etd","deliveryDate"]', `${codeOf(again)} ${again.error ? JSON.stringify(again.error.extra) : ''}`);
        const back9 = await outcome(R.rejectDetails(conn, { id: q9, note: 'Race test: before it leaves', fields: ['deliveryDate'], actor }, { recordAudit }));
        check('and it is sent back by a page that names the delivery date', Boolean(back9.result) && (await rowOf(q9)).status === 'rejected', codeOf(back9));

        // ── a forwarder's files: accept against a withdrawal, and two accepts at once ──
        // The accept route itself (src/services/forwarder-upload-routes.js), run on
        // these two connections, with a stand-in for both buckets: no file goes
        // anywhere. What it files is a bill of lading (a row of
        // shipment_forwarder_documents), removed with the fixtures.
        console.log('\nA forwarder\'s file: an accept against a withdrawal, a change of forwarder, and another accept');
        const BYTES = Buffer.from('%PDF-1.4\n% forwarder container race test\n%%EOF\n', 'latin1');
        const bucket = { puts: [], beforePut: null, getDelay: 0 };
        const fakeS3 = {
            send: async (cmd) => {
                const kind = cmd.constructor.name;
                if (kind === 'GetObjectCommand') {
                    if (bucket.getDelay) await wait(bucket.getDelay);
                    return { ContentLength: BYTES.length, Body: { transformToByteArray: async () => new Uint8Array(BYTES), destroy() {} } };
                }
                if (kind === 'PutObjectCommand') {
                    const hook = bucket.beforePut;
                    bucket.beforePut = null;
                    if (hook) await hook();
                    bucket.puts.push(cmd.input.Key);
                    return {};
                }
                throw new Error(`unexpected bucket call ${kind}`);
            },
        };
        const routeErrors = [];
        const inAudit = { hook: null };
        const ACCEPT = 'POST /api/v1/forwarder-uploads/:id(\\d+)/accept';
        const acceptOn = (c) => {
            const routes = {};
            const app = Object.fromEntries(['get', 'post', 'delete'].map(m => [m, (path, fn) => { routes[`${m.toUpperCase()} ${path}`] = fn; }]));
            U.registerForwarderUploadRoutes(app, {
                withConnection: fn => fn(c),
                recordAudit: async (cc, entry) => { audits.push(entry); const hook = inAudit.hook; inAudit.hook = null; if (hook) await hook(); },
                s3: fakeS3, poBucket: 'race-test-no-bucket', publicS3Url: key => `https://race-test.invalid/${key}`,
                log: { error: (...a) => routeErrors.push(a.map(x => (x && x.message) || String(x)).join(' ')), warn() {} },
                env: { ...process.env, PI_UPLOADS_BUCKET: 'race-test-no-bucket' },
            });
            return async (id) => {
                let status = 200;
                let body = null;
                const res = { headersSent: false, status(s) { status = s; return this; }, json(b) { body = b; this.headersSent = true; return this; } };
                await routes[ACCEPT]({ query: {}, params: { id: String(id) }, body: {}, userEmail: 'race-test@tools', userType: 'admin' }, res);
                return { status, body, code: (body && body.code) || null };
            };
        };
        const accept = acceptOn(conn);
        const acceptOther = acceptOn(other);
        const said = a => `${a.status} ${a.code || ''} ${routeErrors.join(' | ')}`.trim();
        const upload = async (shipmentId, forwarderId = C) => {
            const token = crypto.randomBytes(16).toString('hex');
            const res = await q(
                `INSERT INTO ${jp}.forwarder_uploads
                        (shipment_id, container_ref, forwarder_id, kind, user_id, user_email, user_name, filename, content_type, file_size, in_key, s3_etag, upload_status, finalise_token, sent_at)
                 VALUES (?, 'ZZR', ?, 'bill_of_lading', 0, ?, 'Race Test', 'race test.pdf', 'application/pdf', ?, ?, '"race"', 'sent', ?, UTC_TIMESTAMP())`,
                [shipmentId, forwarderId, emailC, BYTES.length, `files/in/${crypto.randomUUID()}.pdf`, token]
            );
            await q(`UPDATE ${jp}.forwarder_uploads SET final_key = ? WHERE id = ?`, [`files/fwd/${res.insertId}/${token}.pdf`, res.insertId]);
            return res.insertId;
        };
        const uploadRow = async id => (await q(`SELECT upload_status, review_status, filed_key, filed_id, decided_by, decided_at, withdrawn_at FROM ${jp}.forwarder_uploads WHERE id = ?`, [id]))[0];
        const filedFor = id => q('SELECT id, shipment_id, forwarder_id, s3_key, accepted_by_email FROM shipment_forwarder_documents WHERE forwarder_upload_id = ?', [id]);
        // The portal's own statement for taking a file back (jportal
        // src/handlers/forwarderUploads.js, uploadSql.withdraw), word for word in its WHERE.
        const PORTAL_WITHDRAW = `
            UPDATE ${jp}.forwarder_uploads u
               SET u.upload_status = 'withdrawn', u.withdrawn_at = NOW(), u.row_version = u.row_version + 1
             WHERE u.id = ? AND u.upload_status = 'sent' AND u.review_status = 'open'
               AND EXISTS (SELECT 1 FROM portal_forwarder_containers_v c
                            WHERE c.shipment_id = u.shipment_id AND c.forwarder_id = u.forwarder_id
                              AND c.link_source IN ('booking', 'container', 'duty')
                              AND c.forwarder_id IN (SELECT fc.forwarder_id FROM forwarder_contacts fc WHERE fc.email = ? AND fc.deleted_at IS NULL))`;
        const withdrawOn = async (c, id) => (await c.query(PORTAL_WITHDRAW, [id, emailC]))[0].affectedRows;

        const u1c = await ownedByC();
        const u1 = await upload(u1c);
        let a = await accept(u1);
        let urow = await uploadRow(u1);
        let docs = await filedFor(u1);
        const filed1 = a.body && a.body.filed ? a.body.filed : {};
        check('with nothing in the way: accepted, and filed once as that container\'s bill of lading from that forwarder',
            a.status === 200 && a.body.replayed === false && filed1.as === 'document' && docs.length === 1 && Number(docs[0].id) === filed1.id
                && Number(docs[0].shipment_id) === u1c && Number(docs[0].forwarder_id) === C && docs[0].accepted_by_email === 'race-test@tools', said(a));
        check('the upload is received, by whom, and points at what it was filed as', urow.review_status === 'received' && urow.decided_by === 'race-test@tools' && Number(urow.filed_id) === filed1.id, JSON.stringify(urow));
        check('our copy was written once, at the key kept on the row', bucket.puts.length === 1 && bucket.puts[0] === urow.filed_key && docs.length === 1 && docs[0].s3_key === urow.filed_key, JSON.stringify(bucket.puts));
        const again1 = await accept(u1);
        check('asked again: the same filed row, and nothing copied', again1.status === 200 && again1.body.replayed === true && again1.body.filed.id === filed1.id && bucket.puts.length === 1, said(again1));
        check('and the forwarder can no longer take it back', (await withdrawOn(conn, u1)) === 0);

        const u2c = await ownedByC();
        const u2 = await upload(u2c);
        bucket.puts.length = 0;
        r = await decideDuring({ decide: () => accept(u2), change: () => other.query(PORTAL_WITHDRAW, [u2, emailC]), release: 'COMMIT' });
        check('an accept WAITS while the forwarder is taking the file back', r.waited);
        check('and once that is committed it is refused as withdrawn: nothing filed, nothing copied',
            Boolean(r.result) && r.result.status === 409 && r.result.code === 'WITHDRAWN' && (await filedFor(u2)).length === 0 && bucket.puts.length === 0, r.result ? said(r.result) : codeOf(r));

        const u3c = await ownedByC();
        const u3 = await upload(u3c);
        r = await decideDuring({ decide: () => accept(u3), change: () => other.query(PORTAL_WITHDRAW, [u3, emailC]), release: 'ROLLBACK' });
        check('a withdrawal that is rolled back: the accept waited, then went through', r.waited && Boolean(r.result) && r.result.status === 200 && (await filedFor(u3)).length === 1, r.result ? said(r.result) : codeOf(r));

        const u4c = await ownedByC();
        const u4 = await upload(u4c);
        let late = null;
        let lateSettled = false;
        let lateBlocked = null;
        inAudit.hook = async () => {
            // Inside the accept's own transaction, after it has filed the document.
            late = withdrawOn(other, u4).then((rows) => { lateSettled = true; return rows; });
            await wait(1200);
            lateBlocked = !lateSettled;
        };
        a = await accept(u4);
        const lateRows = late ? await late : null;
        urow = await uploadRow(u4);
        check('a withdrawal that arrives while an accept holds its locks WAITS for it', lateBlocked === true);
        check('the accept, first, goes through; the withdrawal after it changes nothing', a.status === 200 && lateRows === 0 && urow.upload_status === 'sent' && urow.review_status === 'received', `${said(a)} rows ${lateRows} ${urow.upload_status}/${urow.review_status}`);

        const u5c = await ownedByC();
        const u5 = await upload(u5c);
        bucket.puts.length = 0;
        // Between our copy and the decision no lock is held: a withdrawal lands there.
        bucket.beforePut = async () => { await withdrawOn(other, u5); };
        a = await accept(u5);
        urow = await uploadRow(u5);
        check('taken back between our copy and the decision: refused as withdrawn, with nothing filed', a.status === 409 && a.code === 'WITHDRAWN' && (await filedFor(u5)).length === 0 && urow.upload_status === 'withdrawn', said(a));
        const now5 = Date.now();
        check('the copy it left is the one the sweep removes: not now, only a week after the withdrawal',
            bucket.puts.length === 1 && bucket.puts[0] === urow.filed_key && SW.copySweepable(urow, now5) === false && SW.copySweepable(urow, now5 + 8 * 86400000) === true, JSON.stringify({ puts: bucket.puts.length, key: Boolean(urow.filed_key) }));

        const u6c = await ownedByC();
        const u6 = await upload(u6c);
        r = await decideDuring({
            decide: () => accept(u6),
            change: () => other.query('UPDATE shipments SET forwarder_id = ?, forwarder = ? WHERE id = ?', [D, `${TAG} D`, u6c]),
            release: 'COMMIT',
        });
        check('an accept WAITS while the container is being given to another forwarder', r.waited);
        check('and then is refused as no longer theirs, to be sent back: nothing filed, the upload still open',
            Boolean(r.result) && r.result.status === 409 && r.result.code === 'NOT_THEIR_CONTAINER' && JSON.stringify(r.result.body.allowedNext) === '["reject"]'
                && (await filedFor(u6)).length === 0 && (await uploadRow(u6)).review_status === 'open', r.result ? said(r.result) : codeOf(r));

        const u7c = await ownedByC();
        const u7 = await upload(u7c);
        bucket.puts.length = 0;
        // Slow reads from the bucket, so both requests are past their first read before either decides.
        bucket.getDelay = 500;
        const both = await Promise.all([accept(u7), acceptOther(u7)]);
        bucket.getDelay = 0;
        docs = await filedFor(u7);
        check('two accepts at once, on two connections: both are answered 200', both.every(b => b.status === 200), both.map(said).join(' / '));
        check('the file is filed once', docs.length === 1, `rows ${docs.length}`);
        check('both are told the same filed row, one of them as already received',
            docs.length === 1 && both.every(b => b.body && b.body.filed && b.body.filed.id === Number(docs[0].id)) && both.filter(b => b.body.replayed === true).length === 1,
            JSON.stringify(both.map(b => b.body && { id: b.body.filed && b.body.filed.id, replayed: b.body.replayed })));
        check('and every copy went to the one key kept on the row', bucket.puts.length >= 1 && docs.length === 1 && bucket.puts.every(k => k === docs[0].s3_key), JSON.stringify(bucket.puts));
        check('no accept ended in an error of its own', routeErrors.length === 0, routeErrors.join(' | '));
    } catch (e) {
        failed++;
        console.error('\nTHE RUN STOPPED:', e);
        await other.query('ROLLBACK').catch(() => {});
    } finally {
        try { if (madeDetails.length) await conn.query(`DELETE FROM ${jp}.forwarder_container_details WHERE id IN (?)`, [madeDetails]); } catch (e) { failed++; console.error('COULD NOT DELETE the portal rows made:', e.message); }
        try { await conn.query(`DELETE FROM ${jp}.forwarder_container_details WHERE shipment_id IN (?)`, [made.shipments.length ? made.shipments : [0]]); } catch (e) { failed++; console.error('COULD NOT DELETE the portal rows of the fixture containers:', e.message); }
        try { await conn.query('DELETE FROM shipment_forwarder_documents WHERE shipment_id IN (?)', [made.shipments.length ? made.shipments : [0]]); } catch (e) { failed++; console.error('COULD NOT DELETE the forwarder documents of the fixture containers:', e.message); }
        try { await conn.query(`DELETE FROM ${jp}.forwarder_uploads WHERE shipment_id IN (?)`, [made.shipments.length ? made.shipments : [0]]); } catch (e) { failed++; console.error('COULD NOT DELETE the uploads of the fixture containers:', e.message); }
        const order = ['freight_bookings', 'freight_quote_options', 'freight_quote_requests', 'orders', 'shipments', 'forwarder_contacts', 'forwarders'];
        for (const table of order) {
            if (!made[table].length) continue;
            try { await conn.query(`DELETE FROM ${table} WHERE id IN (?)`, [made[table]]); } catch (e) { failed++; console.error(`COULD NOT DELETE fixtures from ${table} (ids ${made[table].join(', ')}):`, e.message); }
        }
        await other.end().catch(() => {});
        conn.release();
        await closePool();
    }
    console.log(`\n${passed} passed, ${failed} failed. Fixtures tagged "${TAG}" removed.`);
    process.exit(failed ? 1 : 0);
})().catch(async (e) => { console.error('FAILED:', e); process.exit(1); });
