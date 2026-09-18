'use strict';

// Unit tests for the shipments entity: src/lib/shipments.js (pure) and the
// shadow primitives in src/services/shipment-sync.js, against a scripted fake
// connection (no DB, no server):
//
//   node --test tools/test-shipments-lib.js
//
// What a fake connection cannot show (savepoint and autocommit behaviour, the
// real coalescing of failure rows) is covered against MySQL by
// tools/test-shipments-shadow-mysql.js.

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');

const S = require('../src/lib/shipments');
const sync = require('../src/services/shipment-sync');

function fakeConn(respond) {
    const calls = [];
    return {
        calls,
        async query(sql, params) {
            calls.push({ sql: sql.replace(/\s+/g, ' ').trim(), params: params || [] });
            return respond(sql.replace(/\s+/g, ' ').trim(), params || [], calls.length);
        },
    };
}
const has = (calls, fragment) => calls.filter(c => c.sql.includes(fragment));
const indexOf = (calls, fragment) => calls.findIndex(c => c.sql.includes(fragment));

// app_migrations responder: which flags are present.
function flags({ armed = true, kill = false } = {}) {
    return [[...(armed ? [{ name: sync.BACKFILL_MARKER }] : []), ...(kill ? [{ name: sync.KILL_SWITCH }] : [])]];
}

// ── references and names, on the real live shapes ────────────────────────
test('parseReference: the two sequences, quarantined junk, normalisation', () => {
    assert.deepEqual(S.parseReference('308'), { reference: '308', mode: 'SEA', seq: 308, known: true });
    assert.deepEqual(S.parseReference(' 122. Air Freight '), { reference: '122. Air Freight', mode: 'AIR', seq: 122, known: true });
    assert.deepEqual(S.parseReference('96. air freight'), { reference: '96. air freight', mode: 'AIR', seq: 96, known: true });
    for (const junk of ['UPS', 'Austria', 'test', '12345', '124. Airfreight']) {
        const p = S.parseReference(junk);
        assert.equal(p.known, false, junk);
        assert.equal(p.seq, null, junk);
        assert.equal(p.reference, junk, 'kept verbatim, never renumbered');
    }
    assert.equal(S.parseReference(''), null);
    assert.equal(S.parseReference('   '), null, "'' is no reference (six live READY orders carry one)");
    assert.equal(S.parseReference(null), null);
    assert.equal(S.formatReference('SEA', 329), '329');
    assert.equal(S.formatReference('AIR', 125), '125. Air Freight');
    assert.equal(S.formatReference('ROAD', 1), null);
});

test('parseNameHint: the leading number of the label after the stamp', () => {
    const hint = n => { const h = S.parseNameHint(n); return h && { reference: h.reference, mode: h.mode, stampMode: h.stampMode }; };
    assert.deepEqual(hint('DRAFT-SEA-260917-173825 - 328'), { reference: '328', mode: 'SEA', stampMode: 'SEA' });
    assert.deepEqual(hint('DRAFT-SEA-260820-151031 - 316 ETD 6 Sep'), { reference: '316', mode: 'SEA', stampMode: 'SEA' });
    assert.deepEqual(hint('DRAFT-SEA-260914-131303 -  320 TAM Container 1 ETD 27th Sep'), { reference: '320', mode: 'SEA', stampMode: 'SEA' });
    assert.deepEqual(hint('DRAFT-AIR-260820-094327 - 121. Air Freight'), { reference: '121. Air Freight', mode: 'AIR', stampMode: 'AIR' });
    assert.deepEqual(hint('DRAFT-AIR-260710-165859- 115. Air Freight'), { reference: '115. Air Freight', mode: 'AIR', stampMode: 'AIR' });
    assert.deepEqual(hint('DRAFT-AIR-260916-154300 - 123. Airfreight urgent'), { reference: '123. Air Freight', mode: 'AIR', stampMode: 'AIR' });
    assert.deepEqual(hint('DRAFT-SEA-AUDIT-1758200000000 - 9001'), { reference: '9001', mode: 'SEA', stampMode: null });
    for (const none of [
        'DRAFT-SEA-260714-121033 - TAM Container 1', 'Sunmed Container', 'DRAFT-260610-160022 - Air Freight 2',
        'PLANNED-AIR-260914-145042', 'DRAFT-SEA-260917-101510 - Test', 'DRAFT-001', 'DRAFT-260518-1515',
        'DRAFT-SEA-260917-101510 - 3280abc', 'DRAFT_PDF_1779107571303',
        'PLANNED-SEA-260914-144831 - Shanghai Container Sep End (In Production)',
    ]) {
        assert.equal(S.parseNameHint(none), null, none);
    }
    // One stamp = one draft: a rename keeps it (row 88 '… - 319 Sunmed' is an
    // old name of open draft '… - 324 Sunmed ETD 24th Sep', not container 319).
    assert.equal(S.nameStamp('DRAFT-SEA-260903-172536 - 319 Sunmed'), S.nameStamp('DRAFT-SEA-260903-172536 - 324 Sunmed ETD 24th Sep'));
    assert.equal(S.nameStamp('DRAFT-SEA-260914-131303-  TAM Container 1 ETD 27th Sep'), 'DRAFT-SEA-260914-131303');
    assert.equal(S.nameStamp('Sunmed Container'), null);
    assert.equal(S.modeFromName('DRAFT-AIR-260916-154300 - x'), 'AIR');
    assert.equal(S.modeFromName('PLANNED-SEA-260914-144831'), 'SEA');
    assert.equal(S.modeFromName('Shanghai Airfreight'), null);
});

test('carrierColumnFor: by shape, never by mode — only a true AWB goes to awb_number', () => {
    assert.equal(S.carrierColumnFor('112-06580361'), 'awb_number');
    assert.equal(S.carrierColumnFor('61853679054'), 'awb_number');
    assert.equal(S.carrierColumnFor('1ZG07Y500412959338'), 'external_container_number', 'UPS 1Z never reaches the unfiltered air feeder');
    assert.equal(S.carrierColumnFor('1Z9W52A30494300553'), 'external_container_number');
    assert.equal(S.carrierColumnFor('873237176762'), 'external_container_number', 'a 12-digit FedEx number is not an AWB');
    assert.equal(S.carrierColumnFor('MRKU4645188'), 'external_container_number');
});

test('inferMode: reference, then member statuses, then tracking shape; ROAD never inferred', () => {
    assert.deepEqual(S.inferMode({ reference: '308' }), { mode: 'SEA', source: 'reference' });
    assert.deepEqual(S.inferMode({ reference: '121. Air Freight', statuses: ['ON_SEA'] }), { mode: 'AIR', source: 'reference' });
    assert.deepEqual(S.inferMode({ reference: 'UPS', statuses: ['RECEIVED', 'ON_AIR'] }), { mode: 'AIR', source: 'status' });
    assert.deepEqual(S.inferMode({ reference: 'UPS', statuses: ['ON_AIR', 'ON_SEA'], trackingRef: 'MRKU4645188' }), { mode: 'SEA', source: 'tracking' });
    assert.deepEqual(S.inferMode({ reference: 'test', trackingRef: '618-53679054' }), { mode: 'AIR', source: 'tracking' });
    assert.deepEqual(S.inferMode({ reference: 'Austria', statuses: ['RECEIVED'] }), { mode: null, source: null });
    for (const r of ['UPS', 'Truck 1', 'ROAD']) assert.notEqual(S.inferMode({ reference: r }).mode, 'ROAD');
});

test('trackingRefFor: AIR prefers awb_number, others external; unanimity across members', () => {
    const rows = [
        { external_container_number: '112-06580361', awb_number: '112-06580361' },
        { external_container_number: null, awb_number: ' 112-06580361 ' },
    ];
    assert.deepEqual(S.trackingRefFor('AIR', rows), { trackingRef: '112-06580361', sourceColumn: 'awb_number', unanimous: true, candidates: ['112-06580361'] });
    const sea = S.trackingRefFor('SEA', [{ external_container_number: 'MRKU4645188', awb_number: null }, { external_container_number: '', awb_number: null }]);
    assert.equal(sea.trackingRef, 'MRKU4645188');
    assert.equal(sea.sourceColumn, 'external_container_number');
    const split = S.trackingRefFor('SEA', [
        { external_container_number: 'AAAU1111111' }, { external_container_number: 'BBBU2222222' }, { external_container_number: 'BBBU2222222' },
    ]);
    assert.equal(split.trackingRef, 'BBBU2222222', 'most frequent');
    assert.equal(split.unanimous, false);
    assert.equal(S.trackingRefFor('SEA', []).trackingRef, null);
});

test("deriveStage: 300's mix is CLOSED; arrived / terminal / transit / booked", () => {
    const three100 = [...Array(20).fill('RECEIVED'), 'PARTIALLY_RECEIVED'];
    assert.equal(S.deriveStage(three100), 'CLOSED');
    assert.equal(S.deriveStage(['DESTROYED']), 'CLOSED');
    assert.equal(S.deriveStage(['RECEIVED', 'ON_SEA']), 'ARRIVED');
    assert.equal(S.deriveStage(['ARRIVED_AT_WAREHOUSE', 'CONSOLIDATED']), 'ARRIVED');
    assert.equal(S.deriveStage(['ON_AIR', 'CONSOLIDATED']), 'IN_TRANSIT');
    assert.equal(S.deriveStage(['CONSOLIDATED']), 'BOOKED');
    assert.equal(S.deriveStage([]), null);
});

test('effectiveStage never returns the earlier of stored and derived; open stages are never derived', () => {
    for (const stored of S.BOOKED_STAGES) {
        for (const derived of [null, ...S.BOOKED_STAGES]) {
            const eff = S.effectiveStage(stored, derived);
            assert.ok(S.stageRank(eff) >= S.stageRank(stored), `${stored}/${derived}`);
            assert.ok(S.stageRank(eff) >= S.stageRank(derived), `${stored}/${derived}`);
        }
    }
    assert.equal(S.effectiveStage('IN_TRANSIT', 'BOOKED'), 'IN_TRANSIT', 'a ROAD shipment is not downgraded by its CONSOLIDATED orders');
    assert.equal(S.effectiveStage('BOOKED', 'IN_TRANSIT'), 'IN_TRANSIT', 'a legacy-flow shipment follows its orders');
    for (const open of ['PLANNED', 'DRAFT', 'CANCELLED']) assert.equal(S.effectiveStage(open, 'CLOSED'), open);
});

test('statusForStage + TRANSIT_STATUS_BY_MODE: ROAD moves no order', () => {
    assert.equal(S.statusForStage('IN_TRANSIT', 'SEA'), 'ON_SEA');
    assert.equal(S.statusForStage('IN_TRANSIT', 'AIR'), 'ON_AIR');
    assert.equal(S.statusForStage('IN_TRANSIT', 'ROAD'), null);
    assert.equal(S.statusForStage('ARRIVED', 'ROAD'), 'ARRIVED_AT_WAREHOUSE');
    assert.equal(S.statusForStage('CLOSED', 'SEA'), null);
});

test('fanOutDecision: forward-only, packed, known level, never FQC', () => {
    assert.deepEqual(S.fanOutDecision({ status: 'CONSOLIDATED' }, 'ON_SEA'), { move: true, reason: null });
    assert.equal(S.fanOutDecision({ status: 'ON_AIR' }, 'ON_SEA').reason, 'already_at_or_past_target');
    assert.equal(S.fanOutDecision({ status: 'READY' }, 'ON_SEA').reason, 'not_packed');
    assert.equal(S.fanOutDecision({ status: 'PARTIALLY_RECEIVED' }, 'ARRIVED_AT_WAREHOUSE').reason, 'status_not_in_pipeline');
    assert.equal(S.fanOutDecision({ status: 'CONSOLIDATED', jf_code: 'JF1_FQC' }, 'ON_SEA').reason, 'fqc_sample');
    assert.equal(S.fanOutDecision({ status: 'RECEIVED' }, 'ARRIVED_AT_WAREHOUSE').reason, 'already_at_or_past_target');
    assert.equal(S.fanOutDecision({ status: 'ON_SEA' }, 'ARRIVED_AT_WAREHOUSE').move, true);
});

test('auditSnapshot strips shipmentId and nothing else', () => {
    const o = { id: 1, status: 'READY', shipmentId: 7 };
    assert.deepEqual(S.auditSnapshot(o), { id: 1, status: 'READY' });
    assert.equal(o.shipmentId, 7, 'the response object keeps it');
    assert.equal(S.auditSnapshot(null), null);
    const plain = { id: 2 };
    assert.equal(S.auditSnapshot(plain), plain);
});

test('rowToShipment: booked shipments read etd / eta / vessel / carrier ref from their members', () => {
    const s = S.rowToShipment({
        id: 5, reference: '308', stage: 'BOOKED', derived_stage: 'IN_TRANSIT', effective_stage: 'IN_TRANSIT', mode: 'SEA',
        tracking_ref: 'OLDU0000000', member_ext: 'MRKU4645188', member_awb: null, eta: '2026-09-01', member_eta: '2026-09-20',
        etd: null, member_etd: '2026-08-19',
        vessel_name: null, member_vessel: 'EVER GIVEN', needs_review: 0, member_count: 26, total_units: '1000',
    });
    assert.equal(s.stage, 'IN_TRANSIT');
    assert.equal(s.storedStage, 'BOOKED');
    assert.equal(s.trackingRef, 'MRKU4645188');
    assert.equal(s.etd, '2026-08-19');
    assert.equal(s.eta, '2026-09-20');
    assert.equal(s.vesselName, 'EVER GIVEN');
    assert.equal(s.totalUnits, 1000);
    const d = S.rowToShipment({ id: 6, stage: 'DRAFT', open_key: 'D:x', tracking_ref: 'X', member_ext: 'Y', etd: '2026-10-01', member_etd: '2026-11-01' });
    assert.equal(d.trackingRef, 'X', 'a draft has no members to read from');
    assert.equal(d.etd, '2026-10-01', 'a draft keeps its own etd');
});

// ── shadow(): arming, kill switch, both transaction modes ─────────────────
test('shadow: inert without the marker, and a negative answer is never cached', async () => {
    sync.resetFlagCache();
    let armed = false;
    const conn = fakeConn((sql) => (sql.includes('FROM app_migrations') ? flags({ armed }) : [{ affectedRows: 1 }]));
    let ran = 0;
    assert.equal(await sync.shadow(conn, { site: 't' }, async () => { ran++; return 1; }), null);
    assert.equal(ran, 0);
    armed = true;
    assert.equal(await sync.shadow(conn, { site: 't' }, async () => { ran++; return 'ok'; }), 'ok', 'arming takes effect on the next call');
    assert.equal(ran, 1);
    assert.equal(has(conn.calls, 'FROM app_migrations').length, 2);
});

test('shadow: the kill switch turns the hooks off, re-read at most every 60 s', async () => {
    sync.resetFlagCache();
    let kill = true;
    const conn = fakeConn((sql) => (sql.includes('FROM app_migrations') ? flags({ armed: true, kill }) : [{ affectedRows: 1 }]));
    const realNow = Date.now;
    let now = 1_000_000;
    Date.now = () => now;
    try {
        assert.equal(await sync.shadow(conn, { site: 't' }, async () => 'x'), null);
        kill = false;
        now += 30_000;
        assert.equal(await sync.shadow(conn, { site: 't' }, async () => 'x'), null, 'kill state cached inside the TTL');
        assert.equal(has(conn.calls, 'FROM app_migrations').length, 1);
        now += 31_000;
        assert.equal(await sync.shadow(conn, { site: 't' }, async () => 'x'), 'x', 're-read after 60 s');
    } finally {
        Date.now = realNow;
    }
});

test('shadow (autocommit): own transaction, rollback in finally, failure recorded AFTER the rollback', async () => {
    sync.resetFlagCache();
    const conn = fakeConn((sql) => (sql.includes('FROM app_migrations') ? flags() : [{ affectedRows: 1 }]));
    const r = await sync.shadow(conn, { site: 'PUT /orders/:id', inTx: false, keyKind: 'reference', keyValue: '308' }, async (c) => {
        await c.query('INSERT INTO shipments (x) VALUES (1)');
        const err = new Error('Deadlock found');
        err.code = 'ER_LOCK_DEADLOCK';
        throw err;
    });
    assert.equal(r, null, 'deadlock swallowed: the legacy write already committed');
    const start = indexOf(conn.calls, 'START TRANSACTION');
    const rollback = indexOf(conn.calls, 'ROLLBACK');
    const failure = indexOf(conn.calls, 'INSERT INTO shipment_sync_failures');
    assert.ok(start >= 0 && rollback > start && failure > rollback, 'begin -> rollback -> failure row');
    assert.equal(has(conn.calls, 'COMMIT').length, 0);
    const f = conn.calls[failure];
    assert.ok(f.sql.includes('ON DUPLICATE KEY UPDATE occurrences = occurrences + 1'), 'coalesces');
    assert.deepEqual(f.params.slice(0, 3), ['PUT /orders/:id', 'reference', '308']);
    assert.equal(f.params[4], 'PUT /orders/:id|reference|308', 'dedup_key');
});

test('shadow (autocommit): success commits and returns the result', async () => {
    sync.resetFlagCache();
    const conn = fakeConn((sql) => (sql.includes('FROM app_migrations') ? flags() : [{ affectedRows: 1 }]));
    assert.deepEqual(await sync.shadow(conn, { site: 's' }, async () => ({ 1: 9 })), { 1: 9 });
    assert.ok(indexOf(conn.calls, 'COMMIT') > indexOf(conn.calls, 'START TRANSACTION'));
    assert.equal(has(conn.calls, 'ROLLBACK').length, 0);
});

test('shadow (in transaction): savepoint; a failure rolls back to it and is recorded; deadlock is rethrown', async () => {
    sync.resetFlagCache();
    const conn = fakeConn((sql) => (sql.includes('FROM app_migrations') ? flags() : [{ affectedRows: 1 }]));
    const r = await sync.shadow(conn, { site: 'POST /containers/pack', inTx: true, keyKind: 'reference', keyValue: '308' }, async () => {
        throw new Error('boom');
    });
    assert.equal(r, null);
    assert.ok(indexOf(conn.calls, 'SAVEPOINT shipments_shadow') >= 0);
    assert.ok(indexOf(conn.calls, 'ROLLBACK TO SAVEPOINT shipments_shadow') > indexOf(conn.calls, 'SAVEPOINT shipments_shadow'));
    assert.ok(indexOf(conn.calls, 'INSERT INTO shipment_sync_failures') > indexOf(conn.calls, 'ROLLBACK TO SAVEPOINT'));
    assert.equal(has(conn.calls, 'START TRANSACTION').length, 0, 'never opens its own transaction inside the legacy one');

    const dl = fakeConn((sql) => (sql.includes('FROM app_migrations') ? flags() : [{ affectedRows: 1 }]));
    const err = new Error('Deadlock found');
    err.code = 'ER_LOCK_DEADLOCK';
    await assert.rejects(() => sync.shadow(dl, { site: 'x', inTx: true }, async () => { throw err; }), /Deadlock/);
    assert.equal(has(dl.calls, 'ROLLBACK TO SAVEPOINT').length, 0, 'the legacy transaction is already gone');

    const ok = fakeConn((sql) => (sql.includes('FROM app_migrations') ? flags() : [{ affectedRows: 1 }]));
    assert.equal(await sync.shadow(ok, { site: 'x', inTx: true }, async () => 42), 42);
    assert.ok(indexOf(ok.calls, 'RELEASE SAVEPOINT shipments_shadow') > 0);
});

test('shadow: a failing failure-insert never fails the route', async () => {
    sync.resetFlagCache();
    const conn = fakeConn((sql) => {
        if (sql.includes('FROM app_migrations')) return flags();
        if (sql.includes('INSERT INTO shipment_sync_failures')) throw new Error("Table 'shipment_sync_failures' doesn't exist");
        return [{ affectedRows: 1 }];
    });
    assert.equal(await sync.shadow(conn, { site: 'x' }, async () => { throw new Error('no table'); }), null);
});

test('recordFailure: one key per (site, kind, value), truncated to the column sizes', async () => {
    const conn = fakeConn(() => [{ affectedRows: 1 }]);
    await sync.recordFailure(conn, { site: 's'.repeat(100), keyKind: 'reference', keyValue: 'v'.repeat(400) }, new Error('e'.repeat(900)));
    const p = conn.calls[0].params;
    assert.equal(p[0].length, 64);
    assert.equal(p[2].length, 255);
    assert.equal(p[3].length, 500);
    assert.ok(p[4].length <= 340);
});

// ── primitives ───────────────────────────────────────────────────────────
test('syncOrderMembership: shipment locked (find-or-create) before the order rows; PK-addressed, last_updated pinned', async () => {
    const conn = fakeConn((sql, params) => {
        if (sql.startsWith('SELECT id, NULLIF(TRIM(container_number)') && !sql.includes('FOR UPDATE')) {
            return [[{ id: 10, ref: '308', shipment_id: null }, { id: 11, ref: null, shipment_id: 4 }]];
        }
        if (sql.includes('INSERT INTO shipments')) return [{ insertId: 7, affectedRows: 1 }];
        if (sql.includes('SELECT id FROM shipments WHERE id IN')) return [[{ id: 4 }]];
        if (sql.includes('FROM orders WHERE id IN') && sql.includes('FOR UPDATE')) {
            return [[
                { id: 10, ref: '308', quantity: 50, deleted_at: null, shipment_id: null },
                { id: 11, ref: null, quantity: 5, deleted_at: null, shipment_id: 4 },
            ]];
        }
        if (sql.includes('FROM shipments WHERE id = ?')) return [[{ id: params[0], stage: 'BOOKED', reference: '308', mode: 'SEA' }]];
        if (sql.includes('FROM orders WHERE shipment_id = ?')) return [[]];
        return [{ affectedRows: 1 }];
    });
    const out = await sync.syncOrderMembership(conn, [11, 10, 10]);
    assert.deepEqual(out, { 10: 7, 11: null });
    assert.ok(indexOf(conn.calls, 'INSERT INTO shipments') < indexOf(conn.calls, 'ORDER BY id FOR UPDATE'), 'shipments before orders');
    const updates = has(conn.calls, 'UPDATE orders');
    assert.equal(updates.length, 2);
    for (const u of updates) {
        assert.ok(u.sql.includes('last_updated = last_updated'), u.sql);
        assert.ok(/WHERE id = \?$/.test(u.sql), u.sql);
    }
    assert.deepEqual(has(conn.calls, 'INSERT INTO shipment_lines')[0].params.slice(0, 3), [7, 10, 50]);
});

test('every UPDATE of orders in the shadow is PK-addressed and pins last_updated', () => {
    const src = fs.readFileSync(path.join(__dirname, '..', 'src', 'services', 'shipment-sync.js'), 'utf8');
    const statements = [...src.matchAll(/UPDATE orders[\s\S]*?(?=`)/g)].map(m => m[0].replace(/\s+/g, ' '));
    assert.ok(statements.length >= 4, `found ${statements.length}`);
    for (const s of statements) {
        assert.ok(/last_updated = (o\.)?last_updated/.test(s), `pins last_updated: ${s}`);
        assert.ok(/WHERE (o\.)?id (= \?|BETWEEN \? AND \?|IN)/.test(s), `PK-addressed: ${s}`);
    }
});

test('syncDraft: a converted name with no live lines stamps onto the booked shipment and touches nothing else', async () => {
    const conn = fakeConn((sql) => {
        if (sql.includes('FROM draft_containers WHERE name = ?')) return [[{ id: 3, name: 'D1', shipment_id: 40, closed_reason: null }]];
        if (sql.includes('FROM draft_container_allocations WHERE draft_container_name')) return [[]];
        if (sql.includes('s.open_key = ?')) return [[]];
        if (sql.includes('FROM shipments WHERE id = ? FOR UPDATE')) return [[{ id: 40, stage: 'BOOKED', deleted_at: null, merged_into_id: null }]];
        return [{ affectedRows: 1 }];
    });
    const r = await sync.syncDraft(conn, 'D1');
    assert.deepEqual(r, { shipmentId: 40, stampedOnly: true });
    assert.equal(has(conn.calls, 'INSERT INTO shipments').length, 0, 'no new generation without lines');
    assert.equal(has(conn.calls, 'shipment_lines').length, 0, 'never mirrors lines onto a booked shipment');
    assert.equal(has(conn.calls, 'UPDATE draft_container_documents SET shipment_id').length, 1);
});

test('syncDraft: a converted name with new lines starts a new generation and repoints the registry', async () => {
    const conn = fakeConn((sql, params) => {
        if (sql.includes('FROM draft_containers WHERE name = ?')) return [[{ id: 3, name: 'D1', shipment_id: 40, closed_reason: null }]];
        if (sql.includes('FROM draft_container_allocations WHERE draft_container_name')) return [[{ order_id: 9, allocated: 5 }]];
        if (sql.includes('s.open_key = ?')) return [[]];
        if (sql.includes('FROM shipments WHERE id = ? FOR UPDATE')) {
            return params[0] === 40
                ? [[{ id: 40, stage: 'BOOKED', deleted_at: null, merged_into_id: null }]]
                : [[{ id: params[0], stage: 'DRAFT', name: 'D1', open_key: 'D:D1', source_draft_id: 3, deleted_at: null, merged_into_id: null }]];
        }
        if (sql.includes('INSERT INTO shipments')) return [{ insertId: 41, affectedRows: 1 }];
        if (sql.includes('FROM shipment_lines WHERE shipment_id = ? FOR UPDATE')) return [[]];
        return [{ affectedRows: 1 }];
    });
    const r = await sync.syncDraft(conn, 'D1');
    assert.equal(r.shipmentId, 41);
    const ins = has(conn.calls, 'INSERT INTO shipments')[0];
    assert.equal(ins.params[1], 'D:D1', 'open_key');
    assert.deepEqual(has(conn.calls, 'UPDATE draft_containers SET shipment_id')[0].params, [41, 3]);
    assert.deepEqual(has(conn.calls, 'INSERT INTO shipment_lines')[0].params.slice(0, 3), [41, 9, 5]);
});

test('syncDraft: a cancelled pointer is revived with its open_key restored', async () => {
    const conn = fakeConn((sql) => {
        if (sql.includes('FROM draft_containers WHERE name = ?')) return [[{ id: 3, name: 'D1', shipment_id: 40 }]];
        if (sql.includes('FROM draft_container_allocations WHERE draft_container_name')) return [[{ order_id: 9, allocated: 5 }]];
        if (sql.includes('s.open_key = ?')) return [[]];
        if (sql.includes('FROM shipments WHERE id = ? FOR UPDATE')) return [[{ id: 40, stage: 'CANCELLED', deleted_at: null, merged_into_id: null }]];
        if (sql.includes('FROM shipment_lines WHERE shipment_id = ? FOR UPDATE')) return [[]];
        return [{ affectedRows: 1 }];
    });
    const r = await sync.syncDraft(conn, 'D1');
    assert.equal(r.shipmentId, 40);
    const revive = has(conn.calls, "SET stage = 'DRAFT', open_key = ?")[0];
    assert.deepEqual(revive.params, ['D:D1', 'D1', 40]);
    assert.equal(has(conn.calls, 'INSERT INTO shipments').length, 0);
});

test('syncPlanned: the last line gone cancels the shipment and releases its key', async () => {
    const conn = fakeConn((sql) => {
        if (sql.includes('SELECT planned_container_name AS name')) return [[]];
        if (sql.includes('s.open_key = ?')) return [[{ id: 12, stage: 'PLANNED', name: 'P1', open_key: 'P:P1' }]];
        if (sql.includes('FROM planned_container_allocations WHERE planned_container_name')) return [[]];
        return [{ affectedRows: 1 }];
    });
    const r = await sync.syncPlanned(conn, 'P1');
    assert.deepEqual(r, { shipmentId: 12, cancelled: true });
    const cancel = has(conn.calls, "SET stage = 'CANCELLED'")[0];
    assert.ok(cancel.sql.includes('reference = NULL, open_key = NULL'), 'keys released in the same UPDATE');
    assert.equal(has(conn.calls, 'DELETE FROM shipment_lines WHERE shipment_id = ?').length, 1);
});

test('mergeShipments: the loser releases its keys before the survivor takes the reference', async () => {
    const rows = {
        5: { id: 5, stage: 'DRAFT', reference: null, name: 'D', mode: 'SEA', origin: 'legacy_route', etd: '2026-10-01' },
        9: { id: 9, stage: 'BOOKED', reference: '330', name: null, mode: 'SEA', origin: 'legacy_route', etd: null },
    };
    const conn = fakeConn((sql, params) => {
        if (sql.includes('FROM shipments WHERE id = ? FOR UPDATE')) return [[rows[params[0]]]];
        if (sql.includes('FROM orders WHERE shipment_id = ? ORDER BY id FOR UPDATE')) return [[{ id: 77 }]];
        if (sql.includes('FROM orders WHERE shipment_id = ? AND deleted_at IS NULL ORDER BY id FOR SHARE')) return [[{ id: 77, quantity: 3 }]];
        return [{ affectedRows: 1 }];
    });
    const r = await sync.mergeShipments(conn, 5, 9, { reference: '330' });
    assert.equal(r.stage, 'BOOKED');
    const release = indexOf(conn.calls, 'SET reference = NULL, open_key = NULL, merged_into_id = ?');
    const take = conn.calls.findIndex(c => c.sql.startsWith('UPDATE shipments SET') && c.sql.includes('reference = ?'));
    assert.ok(release >= 0 && take > release, 'release, then take');
    assert.ok(conn.calls[take].params.includes('330'));
    assert.deepEqual(has(conn.calls, 'UPDATE orders SET shipment_id = ?')[0].params, [5, 77]);
});

test('findOrCreateByReference: race-safe upsert that returns the live holder, quarantining junk', async () => {
    const conn = fakeConn(() => [{ insertId: 33, affectedRows: 1 }]);
    assert.equal(await sync.findOrCreateByReference(conn, ' UPS '), 33);
    const c = conn.calls[0];
    assert.ok(c.sql.includes('ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id)'));
    assert.equal(c.params[0], 'UPS');
    assert.equal(c.params[1], null, 'no sequence number');
    assert.equal(c.params[5], 1, 'needs_review');
    await sync.findOrCreateByReference(conn, '121. Air Freight');
    assert.deepEqual(conn.calls[1].params.slice(0, 6), ['121. Air Freight', 121, null, 'AIR', 'reference', 0]);
});
