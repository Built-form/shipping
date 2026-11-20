'use strict';

// Unit tests for src/lib/forwarder-links.js — a container's forwarder as a
// row of ShipLine's Forwarders list (2026-11-20): which forwarder a container
// shows (its live freight booking wins), what a write to the container's own
// forwarder does, how a merge keeps one, how old typed names are matched for
// the backfill, and the guard against re-seeding shipments under live links.
// No database.
//   node --test tools/test-forwarder-links-lib.js

const test = require('node:test');
const assert = require('node:assert/strict');
const FL = require('../src/lib/forwarder-links');

test('nameKey: trimmed, spaces collapsed, lower case (the same key as supplierKey)', () => {
    assert.equal(FL.nameKey('  DCG   Logistics '), 'dcg logistics');
    assert.equal(FL.nameKey(null), '');
});

test('which forwarder a container shows: its live booking, else its own, else nothing', () => {
    // A live booking on it: the booking's forwarder, as the request recorded it.
    assert.deepEqual(FL.effectiveForwarder({
        forwarder: 'Typed Ltd', forwarder_id: 3,
        bk_count: 1, bk_id: 41, bk_status: 'confirmed', bk_ref: 'BK-1', bk_forwarder_id: 7, bk_forwarder_name: 'DCG',
    }), { forwarder: 'DCG', forwarderId: 7, forwarderSource: 'booking', booking: { id: 41, status: 'confirmed', bookingRef: 'BK-1' } });
    // Requested (not yet confirmed) counts: it holds the container.
    assert.equal(FL.effectiveForwarder({ bk_count: 1, bk_id: 42, bk_status: 'requested', bk_ref: null, bk_forwarder_id: 7, bk_forwarder_name: 'DCG' }).forwarderSource, 'booking');
    // No live booking: the container's own pair.
    assert.deepEqual(FL.effectiveForwarder({ forwarder: 'DCG', forwarder_id: 7, bk_count: 0 }),
        { forwarder: 'DCG', forwarderId: 7, forwarderSource: 'container', booking: null });
    // Typed text only (from before the list): still the container's.
    assert.deepEqual(FL.effectiveForwarder({ forwarder: 'Old Name', forwarder_id: null, bk_count: null }),
        { forwarder: 'Old Name', forwarderId: null, forwarderSource: 'container', booking: null });
    // Nothing at all.
    assert.deepEqual(FL.effectiveForwarder({ forwarder: null, forwarder_id: null, bk_count: 0 }),
        { forwarder: null, forwarderId: null, forwarderSource: null, booking: null });
});

test('which forwarder a container shows, read without the booking join: its own pair, no booking claim', () => {
    // A raw row (a locked SELECT * FROM shipments): the route re-reads before answering.
    assert.deepEqual(FL.effectiveForwarder({ forwarder: 'DCG', forwarder_id: 7 }),
        { forwarder: 'DCG', forwarderId: 7, forwarderSource: 'container' });
    assert.equal('booking' in FL.effectiveForwarder({ forwarder: 'DCG', forwarder_id: 7 }), false);
});

test('a write to the container forwarder: every case of forwarderId and the text', () => {
    const stored = { id: 7, name: 'DCG' };
    const d = (body) => FL.decideShipmentForwarder({ stored, body });
    // Nothing sent.
    assert.deepEqual(d({}), { action: 'untouched' });
    // Text only (an older client, the booking modal, the copy button).
    assert.deepEqual(d({ forwarder: 'Savino' }), { action: 'byText', text: 'Savino' });
    assert.deepEqual(d({ forwarder: ' dcg ' }), { action: 'untouched' }); // the stored name, however typed: the pair stays
    assert.deepEqual(d({ forwarder: null }), { action: 'clear' });
    assert.deepEqual(d({ forwarder: '' }), { action: 'clear' });
    assert.deepEqual(d({ forwarder: '   ' }), { action: 'clear' });
    // An id: that forwarder; the text is ignored. The stored one stays as stored.
    assert.deepEqual(d({ forwarderId: 8, forwarder: 'whatever' }), { action: 'byId', id: 8 });
    assert.deepEqual(d({ forwarderId: '8' }), { action: 'byId', id: 8 });
    assert.deepEqual(d({ forwarderId: 7, forwarder: 'DCG Global' }), { action: 'untouched' });
    // null id: unlinked on purpose, the text kept as typed; or nothing at all.
    assert.deepEqual(d({ forwarderId: null, forwarder: ' Someone Else ' }), { action: 'set', id: null, name: 'Someone Else' });
    assert.deepEqual(d({ forwarderId: null }), { action: 'clear' });
    assert.deepEqual(d({ forwarderId: null, forwarder: null }), { action: 'clear' });
    assert.deepEqual(d({ forwarderId: null, forwarder: '' }), { action: 'clear' });
    // A bad id.
    for (const bad of [0, -2, 1.5, 'x', true]) {
        assert.deepEqual(d({ forwarderId: bad }), { error: 'forwarderId must be a forwarder id, or null.' }, String(bad));
    }
    // Text too long for the column.
    assert.deepEqual(d({ forwarder: 'x'.repeat(256) }), { error: 'forwarder must be 255 characters or fewer.' });
    // Nothing stored yet: same text rules.
    assert.deepEqual(FL.decideShipmentForwarder({ stored: { id: null, name: null }, body: { forwarder: 'DCG' } }), { action: 'byText', text: 'DCG' });
});

test('the booking modal: a forwarder typed there changes the pair only when it is a different name', () => {
    const stored = { id: 7, name: 'DCG' };
    assert.deepEqual(FL.decideBookForwarder({ stored, text: null }), { action: 'untouched' });
    assert.deepEqual(FL.decideBookForwarder({ stored, text: undefined }), { action: 'untouched' });
    assert.deepEqual(FL.decideBookForwarder({ stored, text: '  ' }), { action: 'untouched' });
    assert.deepEqual(FL.decideBookForwarder({ stored, text: 'dcg' }), { action: 'untouched' });
    assert.deepEqual(FL.decideBookForwarder({ stored, text: 'Savino' }), { action: 'byText', text: 'Savino' });
});

test('text to a forwarder: exactly one live forwarder with that name, else no link', () => {
    const list = [{ id: 7, name: 'DCG', deleted_at: null }, { id: 8, name: 'Savino', deleted_at: null }, { id: 9, name: 'Gone Ltd', deleted_at: new Date() }];
    // Linked: the container carries the list's spelling.
    assert.deepEqual(FL.pairFromText('  dcg ', list), { id: 7, name: 'DCG' });
    assert.deepEqual(FL.pairFromText('Nobody', list), { id: null, name: 'Nobody' });
    // A removed forwarder is never linked to.
    assert.deepEqual(FL.pairFromText('Gone Ltd', list), { id: null, name: 'Gone Ltd' });
    // Two live ones with the same name: neither.
    assert.deepEqual(FL.pairFromText('DCG', [...list, { id: 10, name: 'dcg', deleted_at: null }]), { id: null, name: 'DCG' });
});

test('merging two containers: the survivor keeps its own forwarder pair whole, else takes the other one whole', () => {
    // Survivor has typed text, loser a linked forwarder: never mixed into { Alpha, 7 }.
    assert.deepEqual(FL.mergeForwarderPair({ forwarder: 'Alpha', forwarder_id: null }, { forwarder: 'Beta', forwarder_id: 7 }), { forwarder: 'Alpha', forwarder_id: null });
    assert.deepEqual(FL.mergeForwarderPair({ forwarder: null, forwarder_id: null }, { forwarder: 'Beta', forwarder_id: 7 }), { forwarder: 'Beta', forwarder_id: 7 });
    assert.deepEqual(FL.mergeForwarderPair({ forwarder: '  ', forwarder_id: null }, { forwarder: 'Beta', forwarder_id: null }), { forwarder: 'Beta', forwarder_id: null });
    // An id with no text (should not happen, but the id still makes it the survivor's).
    assert.deepEqual(FL.mergeForwarderPair({ forwarder: null, forwarder_id: 3 }, { forwarder: 'Beta', forwarder_id: 7 }), { forwarder: null, forwarder_id: 3 });
    assert.deepEqual(FL.mergeForwarderPair({ forwarder: null, forwarder_id: null }, { forwarder: null, forwarder_id: null }), { forwarder: null, forwarder_id: null });
});

test('the backfill match: exact name first, a looser one only when it names exactly one, removed names always skipped', () => {
    const list = [
        { id: 7, name: 'DCG Logistics', deleted_at: null },
        { id: 8, name: 'Savino Del Bene', deleted_at: null },
        { id: 9, name: 'Old Freight', deleted_at: new Date() },
        { id: 10, name: 'Old Freight', deleted_at: null },
    ];
    const same = (a, b) => FL.nameKey(a).includes(FL.nameKey(b)) || FL.nameKey(b).includes(FL.nameKey(a));
    assert.deepEqual(FL.matchForBackfill('dcg logistics', list, same), { result: 'linked', id: 7, name: 'DCG Logistics', how: 'exact' });
    assert.deepEqual(FL.matchForBackfill('Savino', list, same), { result: 'linked', id: 8, name: 'Savino Del Bene', how: 'similar' });
    // A removed forwarder of that name exists too: skipped for review, even though a live one matches exactly.
    assert.deepEqual(FL.matchForBackfill('Old Freight', list, same), { result: 'collision', ids: [10], deletedIds: [9] });
    // A loose match to two live forwarders.
    const two = [...list, { id: 11, name: 'DCG Logistics UK', deleted_at: null }];
    assert.deepEqual(FL.matchForBackfill('DCG', two, same), { result: 'ambiguous', ids: [7, 11] });
    assert.deepEqual(FL.matchForBackfill('Nobody', list, same), { result: 'unmatched' });
    assert.deepEqual(FL.matchForBackfill('  ', list, same), { result: 'unmatched' });
});

// The migration 2026-11-20_10_forwarder_links.js against a stand-in for
// information_schema: what it adds, that a re-run adds nothing, and that it
// refuses while two live bookings hold one container.
function fakeSchema({ have = [], dupes = [] } = {}) {
    const present = new Set(have);
    const ran = [];
    const conn = {
        async query(sql, params = []) {
            const s = sql.replace(/\s+/g, ' ').trim();
            if (s.startsWith('SELECT shipment_id, GROUP_CONCAT(id')) return [dupes];
            if (s.startsWith('SELECT COUNT(*) AS n FROM information_schema.COLUMNS')) return [[{ n: present.has(`col:${params[0]}.${params[1]}`) ? 1 : 0 }]];
            if (s.startsWith('SELECT COUNT(*) AS n FROM information_schema.STATISTICS')) return [[{ n: present.has(`idx:${params[0]}.${params[1]}`) ? 1 : 0 }]];
            if (s.startsWith('SELECT COUNT(*) AS n FROM information_schema.TABLE_CONSTRAINTS')) return [[{ n: present.has(`chk:${params[0]}.${params[1]}`) ? 1 : 0 }]];
            if (s.startsWith('ALTER TABLE')) {
                ran.push(s);
                const t = s.match(/^ALTER TABLE (\w+)/)[1];
                for (const m of s.matchAll(/ADD COLUMN (\w+)/g)) present.add(`col:${t}.${m[1]}`);
                for (const m of s.matchAll(/ADD (?:UNIQUE )?KEY (\w+)/g)) present.add(`idx:${t}.${m[1]}`);
                for (const m of s.matchAll(/ADD CONSTRAINT (\w+) CHECK/g)) present.add(`chk:${t}.${m[1]}`);
                return [{}];
            }
            throw new Error(`unexpected SQL: ${s.slice(0, 80)}`);
        },
    };
    return { conn, ran, present };
}
const migration = require('../src/db/migrate/2026-11-20_10_forwarder_links');
const quiet = { log: () => {} };

test('migration: adds the forwarder links, one freight source per extra and one live booking per container', async () => {
    const { conn, ran } = fakeSchema();
    await migration.up(conn, quiet);
    const all = ran.join('\n');
    assert.match(all, /ALTER TABLE shipments ADD COLUMN forwarder_id INT NULL AFTER forwarder/);
    assert.match(all, /ALTER TABLE shipments ADD KEY idx_shipments_forwarder \(forwarder_id\)/);
    assert.match(all, /ALTER TABLE payment_extras ADD COLUMN forwarder_id INT NULL AFTER supplier_key/);
    assert.match(all, /ALTER TABLE payment_extras ADD COLUMN freight_booking_id INT NULL AFTER source_id/);
    assert.match(all, /ALTER TABLE payment_extras ADD COLUMN freight_movement_id INT NULL AFTER freight_booking_id/);
    assert.match(all, /ADD COLUMN freight_source_key VARCHAR\(24\) GENERATED ALWAYS AS \(CASE WHEN deleted_at IS NOT NULL THEN NULL WHEN freight_booking_id IS NOT NULL THEN CONCAT\('b:', freight_booking_id\) WHEN freight_movement_id IS NOT NULL THEN CONCAT\('m:', freight_movement_id\) END\) STORED/);
    assert.match(all, /ADD UNIQUE KEY uq_payment_extras_freight_source \(freight_source_key\)/);
    assert.match(all, /ADD CONSTRAINT chk_payment_extras_one_freight_source CHECK \(freight_booking_id IS NULL OR freight_movement_id IS NULL\)/);
    assert.match(all, /ADD CONSTRAINT chk_payment_extras_freight_source_alone CHECK \(\(freight_booking_id IS NULL AND freight_movement_id IS NULL\) OR \(source_kind IS NULL AND source_id IS NULL AND rides_with = 'shipment'\)\)/);
    assert.match(all, /ALTER TABLE freight_bookings ADD COLUMN live_shipment_id INT GENERATED ALWAYS AS \(IF\(status IN \('requested', 'confirmed'\), shipment_id, NULL\)\) STORED/);
    assert.match(all, /ALTER TABLE freight_bookings ADD UNIQUE KEY uq_freight_bookings_live_shipment \(live_shipment_id\)/);
    assert.match(all, /ALTER TABLE payment_extras ADD KEY idx_payment_extras_forwarder \(forwarder_id\)/);
    assert.match(all, /ALTER TABLE payment_extras ADD KEY idx_payment_extras_freight_booking \(freight_booking_id\)/);
    assert.match(all, /ALTER TABLE payment_extras ADD KEY idx_payment_extras_freight_movement \(freight_movement_id\)/);
    assert.equal(ran.length, 14);
});

test('migration: a re-run adds nothing', async () => {
    const s = fakeSchema();
    await migration.up(s.conn, quiet);
    const again = fakeSchema({ have: [...s.present] });
    await migration.up(again.conn, quiet);
    assert.deepEqual(again.ran, []);
});

test('migration: refuses while two live bookings hold one container, and changes nothing', async () => {
    const { conn, ran } = fakeSchema({ dupes: [{ shipment_id: 62, ids: '41,44' }] });
    await assert.rejects(migration.up(conn, quiet),
        /Container 62 has 2 live freight bookings \(41,44\)\. Cancel all but one on ShipLine's Freight page, then run the migration again\./);
    assert.deepEqual(ran, []);
});

test('verify: a live booking still names its container — by name, by reference, or by the number its draft name reserved', () => {
    const m = FL.bookingNameMatches;
    assert.equal(m({ bookedName: 'Draft 12', name: 'draft 12 ', reference: null, hintReference: null }), true);
    // Booked since: the draft "… - 325" is container 325 now.
    assert.equal(m({ bookedName: 'DRAFT-SEA-261101-101010 - 325', name: 'DRAFT-SEA-261101-101010 - 325 x', reference: '325', hintReference: '325' }), true);
    assert.equal(m({ bookedName: '325', name: null, reference: '325', hintReference: null }), true);
    // Another container entirely (an id handed on after a re-seed): flagged.
    assert.equal(m({ bookedName: 'DRAFT-SEA-261101-101010 - 325', name: 'Draft 9', reference: '331', hintReference: '325' }), false);
    // Nothing recorded at attach time: nothing to compare.
    assert.equal(m({ bookedName: null, name: 'Draft 9', reference: '331', hintReference: null }), true);
});

test('backfill --apply: a reviewed row is written only while it and its forwarder are as reviewed', () => {
    const entry = { table: 'payment_extras', id: 31, value: 'DCG', forwarderId: 7, forwarderName: 'DCG Logistics' };
    const live = { id: 7, name: 'DCG Logistics', deleted_at: null };
    assert.equal(FL.revalidateLink({ entry, row: { forwarder_id: null, value: 'DCG' }, forwarder: live }), null);
    assert.equal(FL.revalidateLink({ entry, row: null, forwarder: live }), 'the row is gone');
    assert.equal(FL.revalidateLink({ entry, row: { forwarder_id: 8, value: 'DCG' }, forwarder: live }), 'the row is linked already');
    assert.equal(FL.revalidateLink({ entry, row: { forwarder_id: null, value: 'DCG Ltd' }, forwarder: live }), 'its name changed since the review');
    assert.equal(FL.revalidateLink({ entry, row: { forwarder_id: null, value: 'DCG' }, forwarder: null }), 'the forwarder is gone');
    assert.equal(FL.revalidateLink({ entry, row: { forwarder_id: null, value: 'DCG' }, forwarder: { ...live, deleted_at: new Date() } }), 'the forwarder was removed');
    assert.equal(FL.revalidateLink({ entry, row: { forwarder_id: null, value: 'DCG' }, forwarder: { ...live, name: 'DCG Global' } }), 'the forwarder was renamed since the review');
});

test('backfill --repair: a reviewed linked row is relinked only while it is as reviewed', () => {
    const entry = { table: 'shipments', id: 12, value: 'Savino', forwarderId: 7 };
    assert.equal(FL.revalidateRepair({ entry, row: { forwarder_id: 7, value: 'Savino' } }), null);
    assert.equal(FL.revalidateRepair({ entry, row: null }), 'the row is gone');
    assert.equal(FL.revalidateRepair({ entry, row: { forwarder_id: 8, value: 'Savino' } }), 'its link changed since the review');
    assert.equal(FL.revalidateRepair({ entry, row: { forwarder_id: 7, value: 'DCG' } }), 'its name changed since the review');
});

test('the re-seed guard: no seeding an empty shipments table while other rows point at shipment ids', () => {
    assert.equal(FL.reseedGuard({ shipmentCount: 120, dependants: { freight_bookings: 4, payment_extras: 9 } }), null);
    assert.equal(FL.reseedGuard({ shipmentCount: 0, dependants: { freight_bookings: 0, payment_extras: 0 } }), null);
    assert.equal(FL.reseedGuard({ shipmentCount: 0, dependants: { freight_bookings: 4, payment_extras: 0 } }),
        'shipments is empty but rows still point at shipment ids (freight_bookings: 4). Seeding would hand those ids to other containers: remap them first.');
    assert.equal(FL.reseedGuard({ shipmentCount: 0, dependants: { freight_bookings: 4, payment_extras: 2 } }),
        'shipments is empty but rows still point at shipment ids (freight_bookings: 4, payment_extras: 2). Seeding would hand those ids to other containers: remap them first.');
});
