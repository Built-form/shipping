'use strict';

// Unit tests for src/lib/freight-quotes.js — freight quotes
// (docs/freight-quotes.md): the token and link, what a forwarder, a request and
// an answer may say, a request's state, the emails' fixed blocks, a container's
// contents by supplier and whether they changed since they were sent — plus
// sendEmail's SES message and attachments (src/services/email-send.js). No database.
//   node --test tools/test-freight-quotes-lib.js

const test = require('node:test');
const assert = require('node:assert/strict');
const F = require('../src/lib/freight-quotes');
const { sendEmail } = require('../src/services/email-send');
const {
    DEFAULT_EMAIL_TEMPLATES, FREIGHT_TEMPLATES, FREIGHT_QUOTE_REQUEST_TEMPLATE_KEY,
    FREIGHT_BOOKING_REQUEST_TEMPLATE_KEY, FREIGHT_BOOKING_CONTENTS_TEMPLATE_KEY,
} = require('../src/lib/email-template-defaults');

const REQUEST = { windowStart: '2026-12-01', windowEnd: '2026-12-31', services: ['door_to_door', 'door_to_port'], containerSize: '40HQ' };
const OPTION = {
    loadingDate: '2026-12-05', sailingDate: '2026-12-10', transitDays: 32, portFrom: 'Ningbo', portTo: 'Felixstowe',
    service: 'door_to_port', price: '1850', currency: 'USD',
};

test('token and link', () => {
    const t = F.newToken();
    assert.match(t, F.TOKEN_RE);
    assert.notEqual(t, F.newToken());
    assert.equal(F.isToken('abc'), false);
    assert.equal(F.isToken(t.toUpperCase()), false);
    assert.equal(F.freightUrl('https://p.example', t), `https://p.example/freight/${t}`);
});

test('the four templates are known to the in-code fallback', () => {
    const keys = [
        FREIGHT_QUOTE_REQUEST_TEMPLATE_KEY, FREIGHT_BOOKING_REQUEST_TEMPLATE_KEY, FREIGHT_BOOKING_CONTENTS_TEMPLATE_KEY,
        'freight_movement_request',
    ];
    assert.deepEqual(FREIGHT_TEMPLATES.map(t => t.key), keys);
    for (const key of keys) {
        const tpl = DEFAULT_EMAIL_TEMPLATES.find(x => x.key === key);
        assert.equal(tpl.category, 'Freight', key);
        assert.match(tpl.bodyHtml, /^<div[^>]*><p>Greetings,<\/p>/, key);
        assert.match(tpl.bodyHtml, /您好/, key);
    }
});

test('dates: real days, arrival = sailing + transit', () => {
    assert.equal(F.isYmd('2026-02-28'), true);
    assert.equal(F.isYmd('2026-02-30'), false);
    assert.equal(F.isYmd('2026-2-3'), false);
    assert.equal(F.addDays('2026-12-30', 3), '2027-01-02');
    assert.equal(F.daysBetween('2026-12-01', '2026-12-31'), 30);
    assert.equal(F.arrivalDate('2026-12-10', 32), '2027-01-11');
    assert.equal(F.arrivalDate(null, 32), null);
    assert.equal(F.arrivalDate('2026-12-10', 1.5), null);
});

test('validateForwarder: a name and at least one plain address, lower-cased, duplicates dropped', () => {
    const ok = F.validateForwarder({ name: '  DCG Logistics ', notes: '', contacts: [
        { name: 'Amy', email: 'Amy@DCG.com' }, { email: 'amy@dcg.com' }, { email: 'Ops <ops@dcg.com>' },
    ] });
    assert.deepEqual(ok.value, {
        name: 'DCG Logistics', notes: null,
        contacts: [{ name: 'Amy', email: 'amy@dcg.com' }, { name: null, email: 'ops@dcg.com' }],
    });
    assert.equal(F.validateForwarder({ name: '', contacts: [{ email: 'a@b.com' }] }).field, 'name');
    assert.equal(F.validateForwarder({ name: 'X', contacts: [] }).field, 'contacts');
    assert.equal(F.validateForwarder({ name: 'X', contacts: [{ email: 'not an address' }] }).field, 'contacts[0].email');
    assert.equal(F.validateForwarder({ name: 'X', contacts: [{ email: 'a@b.com, c@d.com' }] }).field, 'contacts[0].email');
    assert.equal(F.validateForwarder(null).field, 'body');
});

test('validateRequest: forwarders, a range, services; defaults filled in', () => {
    const body = {
        forwarderIds: [3, 3, 7], batchId: 'A1B2C3D4-0000-4000-8000-000000000001',
        windowStart: '2026-12-01', windowEnd: '2026-12-31', services: ['port_to_port', 'door_to_door'],
    };
    const ok = F.validateRequest(body, '2026-11-16');
    assert.deepEqual(ok.value, {
        // A request made with no kind is a freight request.
        kind: 'freight',
        forwarderIds: [3, 7], batchId: 'a1b2c3d4-0000-4000-8000-000000000001',
        windowStart: '2026-12-01', windowEnd: '2026-12-31', origin: 'China', destination: 'UK',
        // The delivery options as text, in the order chosen; no stock movements.
        containerSize: '40HQ', services: ['port_to_port', 'door_to_door'], stockMovements: [], note: null, expiryDays: 30,
    });
    const bad = (patch, today = '2026-11-16') => F.validateRequest({ ...body, ...patch }, today).field;
    assert.equal(bad({ forwarderIds: [] }), 'forwarderIds');
    assert.equal(bad({ forwarderIds: ['x'] }), 'forwarderIds');
    assert.equal(bad({ forwarderIds: Array.from({ length: 11 }, (_, i) => i + 1) }), 'forwarderIds');
    assert.equal(bad({ batchId: 'nope' }), 'batchId');
    assert.equal(bad({ windowStart: '2026-13-01' }), 'windowStart');
    assert.equal(bad({ windowEnd: '2026-11-30' }), 'windowEnd');           // before the start
    assert.equal(bad({ windowEnd: '2027-04-01' }), 'windowEnd');           // longer than 92 days
    assert.equal(bad({}, '2027-01-05'), 'windowEnd');                      // wholly in the past
    assert.equal(bad({ containerSize: '45HQ' }), 'containerSize');
    assert.equal(bad({ services: [] }), 'services');
    assert.equal(bad({ services: [42] }), 'services');
    assert.equal(bad({ services: ['two\nlines'] }), 'services');
    assert.equal(bad({ services: Array.from({ length: 13 }, (_, i) => `Option ${i}`) }), 'services');
    assert.equal(bad({ expiryDays: 3 }), 'expiryDays');
    assert.equal(F.validateRequest({ ...body, batchId: undefined }, '2026-11-16').value.batchId, null);
});

test('delivery options are free text; the four old services keep their translation', () => {
    assert.deepEqual(F.validateServices([' Door to port ', 'Deliver to Unit 5', 'DOOR TO PORT']).value, ['Door to port', 'Deliver to Unit 5']);
    assert.deepEqual(F.validateDeliveryOption({ label: '  Port to door, tail-lift ' }).value, { label: 'Port to door, tail-lift' });
    assert.equal(F.validateDeliveryOption({ label: '' }).field, 'label');
    assert.equal(F.validateDeliveryOption({ label: 'x'.repeat(101) }).field, 'label');
    assert.equal(F.validateDeliveryOption(null).field, 'body');
    assert.deepEqual(F.DEFAULT_DELIVERY_OPTIONS, ['Door to door', 'Door to port', 'Port to port', 'Port to door']);

    // A code from an old request, and its English name typed as text, read the same.
    assert.equal(F.serviceCode('door_to_port'), 'door_to_port');
    assert.equal(F.serviceCode(' Door to Port '), 'door_to_port');
    assert.equal(F.serviceCode('Door to port (Felixstowe)'), null);
    assert.equal(F.serviceName('door_to_port'), 'Door to port');
    assert.equal(F.serviceName('Deliver to Unit 5'), 'Deliver to Unit 5');
    assert.equal(F.serviceLabel('Door to port'), 'Door to port / 门到港');
    assert.equal(F.serviceLabel('Deliver to Unit 5'), 'Deliver to Unit 5');
    assert.deepEqual(F.servicesOf('["door_to_door","Deliver to Unit 5",""]'), ['door_to_door', 'Deliver to Unit 5']);

    // An option names one of the request's own delivery options, exactly.
    const request = { ...REQUEST, services: ['Deliver to Unit 5'] };
    assert.equal(F.validateOptions([{ ...OPTION, service: 'Deliver to Unit 5' }], request).options[0].service, 'Deliver to Unit 5');
    assert.equal(F.validateOptions([{ ...OPTION, service: 'deliver to unit 5' }], request).field, 'options[0].service');
    assert.equal(F.validateOptions([{ ...OPTION, service: 7 }], request).field, 'options[0].service');
});

test('stock movements: a factory, CBM and weight; no "to"', () => {
    const ok = F.validateMovements([
        // A "to" from an older client is ignored, not refused.
        { from: ' Acme Medical ', to: 'Ningbo port', cbm: '12.5', weightKg: 2400 },
        { from: 'Best Gloves', cbm: 3.255, weightKg: '610.55' },
    ]);
    assert.deepEqual(ok.value, [
        { from: 'Acme Medical', cbm: 12.5, weightKg: 2400 },
        { from: 'Best Gloves', cbm: 3.255, weightKg: 610.55 },
    ]);
    assert.deepEqual(F.movementTotals(ok.value), { count: 2, cbm: 15.755, weightKg: 3010.55 });
    assert.deepEqual(F.validateMovements(undefined).value, []);
    assert.deepEqual(F.validateMovements([]).value, []);

    const good = { from: 'A', cbm: 1, weightKg: 1 };
    const field = patch => F.validateMovements([good, { ...good, ...patch }]).field;
    assert.equal(field({ from: ' ' }), 'stockMovements[1].from');
    assert.equal(field({ cbm: 0 }), 'stockMovements[1].cbm');
    assert.equal(field({ cbm: '1.2345' }), 'stockMovements[1].cbm');
    assert.equal(field({ cbm: -2 }), 'stockMovements[1].cbm');
    assert.equal(field({ weightKg: '' }), 'stockMovements[1].weightKg');
    assert.equal(field({ weightKg: '12.345' }), 'stockMovements[1].weightKg');
    assert.equal(F.validateMovements('no').field, 'stockMovements');
    assert.equal(F.validateMovements(Array.from({ length: 31 }, () => good)).field, 'stockMovements');
    assert.equal(F.validateMovements([null]).field, 'stockMovements[0]');

    assert.equal(F.movementsHtml([]), '');
    const one = F.movementsHtml([{ from: 'Acme <Ningbo>', cbm: 12.5, weightKg: 2400 }]);
    assert.match(one, /Acme &lt;Ningbo&gt;/);
    assert.match(one, />12\.5</);
    assert.match(one, /each priced separately/);
    assert.doesNotMatch(one, /Total/);                           // one row needs no total
    assert.doesNotMatch(one, /port or consolidation/i);          // no "to" column
    const two = F.movementsHtml(ok.value);
    assert.match(two, /Total \/ 合计/);
    assert.match(two, />15\.755</);
    assert.match(two, />3010\.55</);
});

test('stock movements: added later, priced one by one, totals by currency', () => {
    const key = 'B2C3D4E5-0000-4000-8000-000000000002';
    const add = F.validateMovementAdd({ addKey: key, stockMovements: [{ from: 'Late', cbm: 2, weightKg: 300 }] });
    assert.deepEqual(add.value, { addKey: key.toLowerCase(), stockMovements: [{ from: 'Late', cbm: 2, weightKg: 300 }] });
    assert.equal(F.validateMovementAdd({ stockMovements: [{ from: 'Late', cbm: 2, weightKg: 300 }] }).value.addKey, null);
    assert.equal(F.validateMovementAdd({ stockMovements: [] }).field, 'stockMovements');
    assert.equal(F.validateMovementAdd({ addKey: 'nope', stockMovements: [{ from: 'Late', cbm: 2, weightKg: 300 }] }).field, 'addKey');
    assert.equal(F.validateMovementAdd(null).field, 'body');
    assert.equal(F.validateMovementAdd({ stockMovements: [{ from: 'Late', cbm: 0, weightKg: 300 }] }).field, 'stockMovements[0].cbm');

    assert.deepEqual(F.validateMovementPrice({ price: '120.5', currency: 'USD', note: ' incl. tolls ' }).value, { price: '120.50', currency: 'USD', note: 'incl. tolls' });
    assert.deepEqual(F.validateMovementPrice({ price: 80, currency: 'CNY' }).value, { price: '80.00', currency: 'CNY', note: null });
    // No price takes one back off, whatever else is sent.
    assert.deepEqual(F.validateMovementPrice({ price: null, currency: 'USD', note: 'x' }).value, { price: null, currency: null, note: null });
    assert.deepEqual(F.validateMovementPrice({ price: '' }).value, { price: null, currency: null, note: null });
    assert.equal(F.validateMovementPrice({ price: '0', currency: 'USD' }).field, 'price');
    assert.equal(F.validateMovementPrice({ price: '1.234', currency: 'USD' }).field, 'price');
    assert.equal(F.validateMovementPrice({ price: 'free', currency: 'USD' }).field, 'price');
    assert.equal(F.validateMovementPrice({ price: '10', currency: 'JPY' }).field, 'currency');
    assert.equal(F.validateMovementPrice({ price: '10', currency: 'USD', note: 'x'.repeat(501) }).field, 'note');
    assert.equal(F.validateMovementPrice([]).field, 'body');

    // Never added across currencies; cents are summed whole.
    assert.deepEqual(F.movementPriceTotals([
        { price: 0.1, currency: 'CNY' }, { price: 120.5, currency: 'USD' }, { price: null, currency: null },
        { price: 0.2, currency: 'CNY' }, { price: 30, currency: 'USD' },
    ]), { priced: 4, unpriced: 1, totals: [{ currency: 'USD', amount: 150.5 }, { currency: 'CNY', amount: 0.3 }] });
    assert.deepEqual(F.movementPriceTotals([]), { priced: 0, unpriced: 0, totals: [] });

    const block = F.movementBlockHtml({
        movements: [{ from: 'Late <Factory>', cbm: 2, weightKg: 300 }], url: 'https://p.example/freight/abc', expiresOn: '2026-12-17',
    });
    assert.match(block, /Late &lt;Factory&gt;/);
    assert.match(block, /href="https:\/\/p\.example\/freight\/abc"/);
    assert.match(block, /Price the stock movements/);
    assert.match(block, /2026-12-17/);
});

test('bookingFinished: confirmed, and due to arrive more than a fortnight ago', () => {
    const option = { sailing_date: '2026-10-01', transit_days: 30 }; // arrives 2026-10-31
    const confirmed = { status: 'confirmed' };
    assert.equal(F.bookingFinished(confirmed, option, '2026-11-14'), false); // the fourteenth day
    assert.equal(F.bookingFinished(confirmed, option, '2026-11-15'), true);
    // The option as JSON reads the same.
    assert.equal(F.bookingFinished(confirmed, { sailingDate: '2026-10-01', transitDays: 30 }, '2026-11-15'), true);
    // Only a confirmed booking finishes; one whose option cannot be read never does.
    assert.equal(F.bookingFinished({ status: 'requested' }, option, '2027-01-01'), false);
    assert.equal(F.bookingFinished({ status: 'cancelled' }, option, '2027-01-01'), false);
    assert.equal(F.bookingFinished(confirmed, null, '2027-01-01'), false);
    assert.equal(F.bookingFinished(confirmed, { sailing_date: null, transit_days: 30 }, '2027-01-01'), false);
    assert.equal(F.bookingFinished(confirmed, option, 'soon'), false);
});

test('requestState: revoked > booked > expired > answered > open', () => {
    const at = new Date();
    assert.equal(F.requestState({ revoked_at: at, expired: 1, answered_at: at }, true), 'revoked');
    // A booking that stands outranks the link's expiry: the forwarder still needs the page.
    assert.equal(F.requestState({ revoked_at: null, expired: 1, answered_at: at }, true), 'booked');
    assert.equal(F.requestState({ revoked_at: null, expired: 1, answered_at: at }, false), 'expired');
    // A finished booking no longer holds the link open, but still holds the request while the link lasts.
    assert.equal(F.requestState({ revoked_at: null, expired: 1, answered_at: at }, true, true), 'expired');
    assert.equal(F.requestState({ revoked_at: null, expired: 0, answered_at: at }, true, true), 'booked');
    assert.equal(F.requestState({ revoked_at: null, expired: 0, answered_at: at }, true), 'booked');
    assert.equal(F.requestState({ revoked_at: null, expired: 0, answered_at: at }, false), 'answered');
    assert.equal(F.requestState({ revoked_at: null, expired: 0, answered_at: null }, false), 'open');
    // A cancelled or declined booking is not live: the request is simply answered again.
    assert.equal(F.isEditable('answered'), true);
    assert.equal(F.isEditable('open'), true);
    assert.equal(F.isEditable('booked'), false);
    assert.equal(F.isEditable('revoked'), false);
});

test('validateOptions: one good option, size defaulted, price to two decimals', () => {
    const { options } = F.validateOptions([OPTION], REQUEST);
    assert.deepEqual(options, [{
        ...OPTION, containerSize: '40HQ', price: '1850.00', carrier: null, validUntil: null, note: null,
    }]);
    // The forwarder may offer another size, and a number for the price.
    const other = F.validateOptions([{ ...OPTION, containerSize: '40GP', price: 1799.5 }], REQUEST).options[0];
    assert.equal(other.containerSize, '40GP');
    assert.equal(other.price, '1799.50');
});

test('validateOptions: what it refuses, by field', () => {
    const field = patch => F.validateOptions([{ ...OPTION, ...patch }], REQUEST).field;
    assert.equal(field({ loadingDate: '2026-12-11' }), 'options[0].loadingDate');      // after the sailing
    assert.equal(field({ loadingDate: 'soon' }), 'options[0].loadingDate');
    assert.equal(field({ sailingDate: '2027-02-01' }), 'options[0].sailingDate');      // far outside the range
    assert.equal(field({ transitDays: 0 }), 'options[0].transitDays');
    assert.equal(field({ transitDays: '32.5' }), 'options[0].transitDays');
    assert.equal(field({ transitDays: true }), 'options[0].transitDays');
    assert.equal(field({ portFrom: '  ' }), 'options[0].portFrom');
    assert.equal(field({ portTo: 'x'.repeat(101) }), 'options[0].portTo');
    assert.equal(field({ service: 'port_to_port' }), 'options[0].service');            // not one asked for
    assert.equal(field({ containerSize: '45HQ' }), 'options[0].containerSize');
    assert.equal(field({ price: '0' }), 'options[0].price');
    assert.equal(field({ price: '12.345' }), 'options[0].price');
    assert.equal(field({ price: '-5' }), 'options[0].price');
    assert.equal(field({ price: undefined }), 'options[0].price');
    assert.equal(field({ currency: 'JPY' }), 'options[0].currency');
    assert.equal(field({ validUntil: '2026-02-30' }), 'options[0].validUntil');
    assert.equal(field({ note: 'x'.repeat(501) }), 'options[0].note');
    assert.equal(F.validateOptions('no', REQUEST).field, 'options');
    assert.equal(F.validateOptions(Array.from({ length: 41 }, () => OPTION), REQUEST).field, 'options');
    assert.equal(F.validateOptions([OPTION, null], REQUEST).field, 'options[1]');
});

test('validateOptions: a sailing just outside the range is accepted', () => {
    const late = F.validateOptions([{ ...OPTION, loadingDate: '2026-12-30', sailingDate: '2027-01-10' }], REQUEST);
    assert.equal(late.options.length, 1);
    const tooLate = F.validateOptions([{ ...OPTION, loadingDate: '2026-12-30', sailingDate: '2027-01-15' }], REQUEST);
    assert.equal(tooLate.field, 'options[0].sailingDate');
});

test('validateStaffAnswer: options, or "cannot quote", never both or neither', () => {
    assert.equal(F.validateStaffAnswer({ options: [OPTION], note: ' by email ' }, REQUEST).value.note, 'by email');
    assert.deepEqual(F.validateStaffAnswer({ declined: true }, REQUEST).value, { options: [], declined: true, note: null });
    assert.equal(F.validateStaffAnswer({ options: [OPTION], declined: true }, REQUEST).field, 'options');
    assert.equal(F.validateStaffAnswer({ options: [] }, REQUEST).field, 'options');
    assert.equal(F.validateBookingRef('  BK-123 ').value, 'BK-123');
    assert.equal(F.validateBookingRef('').field, 'bookingRef');
});

test('money and the emails\' fixed blocks', () => {
    assert.equal(F.money('1850', 'USD'), 'USD 1,850.00');
    assert.equal(F.money(1234567.5, 'GBP'), 'GBP 1,234,567.50');

    const quote = F.quoteBlockHtml({
        request: {
            ...REQUEST, origin: 'Ningbo <factory>', destination: 'UK', note: 'Two pallets\nextra',
            stockMovements: [{ from: 'Acme Medical', cbm: 12.5, weightKg: 2400 }],
        },
        url: 'https://p.example/freight/abc', expiresOn: '2026-12-16', registerUrl: 'https://p.example/register',
    });
    assert.match(quote, /Stock movements, each priced separately/);
    assert.match(quote, /separate from the price of the sailing/);
    assert.match(quote, /Acme Medical/);
    assert.ok(quote.indexOf('Acme Medical') < quote.indexOf('Enter your quote'));   // above the button
    assert.match(quote, /href="https:\/\/p\.example\/freight\/abc"/);
    assert.match(quote, /Ningbo &lt;factory&gt;/);           // escaped
    assert.match(quote, /Two pallets<br>extra/);
    assert.match(quote, /Door to door \/ 门到门/);
    assert.match(quote, /2026-12-16/);
    assert.match(quote, /https:\/\/p\.example\/register/);

    const { options: [option] } = F.validateOptions([OPTION], REQUEST);
    const booking = F.bookingBlockHtml({ option, url: 'https://p.example/freight/abc', note: null });
    assert.match(booking, /USD 1,850\.00/);
    assert.match(booking, /arrives 2027-01-11/);
    assert.match(booking, /Confirm or decline the booking/);
});

const LINES = [
    { sku: 'Gauze', jfCode: 'JF0001', lot: 'L1', poNumber: 'PO-1', orderedUnits: 1000, noOfCartons: 10, totalCbm: 1.235, cartonWeight: 12.5, supplier: 'Acme', port: 'Ningbo' },
    { sku: 'Tape', jfCode: 'JF0002', lot: '', poNumber: 'PO-2', orderedUnits: 500, noOfCartons: 5, totalCbm: 0.5, cartonWeight: 8, supplier: ' acme ', port: 'Ningbo' },
    { sku: 'Mask', jfCode: 'JF0003', lot: 'L3', poNumber: 'PO-9', orderedUnits: 200, noOfCartons: 2, totalCbm: 0.25, cartonWeight: 4, supplier: 'Bolt', port: 'Shanghai' },
    { sku: 'Loose', jfCode: 'JF0004', lot: 'L4', poNumber: '', orderedUnits: 10, noOfCartons: 1, totalCbm: 0.01, cartonWeight: 1, supplier: '', port: '' },
];

test('groupContentsBySupplier: one group per supplier (any case), nameless last, totals', () => {
    const { groups, totals } = F.groupContentsBySupplier(LINES);
    assert.deepEqual(groups.map(g => g.supplier), ['Acme', 'Bolt', '(no supplier)']);
    assert.deepEqual(groups[0].poNumbers, ['PO-1', 'PO-2']);
    assert.deepEqual(groups[0].ports, ['Ningbo']);
    assert.deepEqual(groups[0].totals, { lines: 2, units: 1500, cartons: 15, cbm: 1.735, weightKg: 165 });
    assert.deepEqual(totals, { lines: 4, units: 1710, cartons: 18, cbm: 1.995, weightKg: 174 });
    assert.deepEqual(F.groupContentsBySupplier([]), { groups: [], totals: { lines: 0, units: 0, cartons: 0, cbm: 0, weightKg: 0 } });
});

test('contentsHtml: a table per supplier, escaped, with subtotals', () => {
    const html = F.contentsHtml(F.groupContentsBySupplier([{ ...LINES[0], supplier: 'A & B <Co>' }]), {
        containerName: 'Draft 12', bookingRef: 'BK-1',
        option: { sailingDate: '2026-12-10', portFrom: 'Ningbo', portTo: 'Felixstowe' },
    });
    assert.match(html, /A &amp; B &lt;Co&gt;/);
    assert.match(html, /Draft 12/);
    assert.match(html, /BK-1/);
    assert.match(html, /Subtotal/);
    assert.match(html, /1\.235/);           // CBM to three places
    assert.match(html, /125\.0/);           // 10 cartons at 12.5 kg
});

test('contentsDrift: nothing sent → null; then only what the forwarder acts on counts', () => {
    assert.equal(F.contentsDrift(null, LINES), null);
    const snapshot = JSON.stringify(F.contentsSnapshot(LINES));
    assert.equal(F.contentsDrift(snapshot, LINES).stale, false);
    // Carton spec and CBM are not a change; units and a new line are.
    assert.equal(F.contentsDrift(snapshot, LINES.map(l => ({ ...l, totalCbm: 9, cartonWeight: 99 }))).stale, false);
    const moved = F.contentsDrift(snapshot, [{ ...LINES[0], orderedUnits: 900 }, ...LINES.slice(1)]);
    assert.equal(moved.stale, true);
    assert.deepEqual(moved.changes, [{ kind: 'units', item: 'JF0001', lot: 'L1', unitsWas: 1000, unitsNow: 900 }]);
    assert.equal(F.contentsDrift(snapshot, LINES.slice(0, 3)).changes[0].kind, 'removed');
});

test('sendEmail: attachments ride along on the SES message; none by default', async () => {
    const calls = [];
    const sendImpl = async (region, input) => { calls.push(input); return { MessageId: 'm1' }; };
    const env = { SES_FROM_ARN: 'arn:aws:ses:eu-north-1:123456789012:identity/portal@example.com' };
    const plain = await sendEmail({ to: 'a@b.com', subject: 's', html: '<p>x</p>' }, { sendImpl, env });
    assert.deepEqual(plain, { ok: true, messageId: 'm1' });
    assert.equal(calls[0].Content.Simple.Attachments, undefined);
    assert.equal(calls[0].ReplyToAddresses, undefined, 'no EMAIL_REPLY_TO, no Reply-To');

    await sendEmail({
        to: 'a@b.com', subject: 's', html: '<p>x</p>',
        attachments: [{ bytes: Buffer.from('%PDF'), filename: 'q.pdf', contentType: 'application/pdf' }, null, { filename: 'no-bytes.csv' }],
    }, { sendImpl, env });
    const files = calls[1].Content.Simple.Attachments;
    assert.equal(files.length, 1);
    assert.equal(files[0].FileName, 'q.pdf');
    assert.equal(files[0].ContentType, 'application/pdf');
    assert.equal(files[0].ContentDisposition, 'ATTACHMENT');
});

test('sendEmail: not configured, a refusal and no answer are reported, never thrown', async () => {
    const env = { SES_FROM_ARN: 'arn:aws:ses:eu-north-1:123456789012:identity/portal@example.com' };
    assert.deepEqual(await sendEmail({ to: 'a@b.com', subject: 's', html: 'x' }, { env: {} }), { ok: false, reason: 'ses_not_configured' });
    assert.deepEqual(await sendEmail({ to: 'a@b.com', subject: 's', html: 'x' }, { env: { SES_FROM_ARN: 'arn:aws:ses:eu-north-1:123456789012:identity/example.com' } }), { ok: false, reason: 'ses_not_configured' }, 'a domain identity has no address');
    assert.deepEqual(await sendEmail({ to: [], subject: 's', html: 'x' }, { env }), { ok: false, reason: 'no_recipients' });
    const sesError = (name, status, message = name) => Object.assign(new Error(message), { name, $metadata: { httpStatusCode: status } });
    const refused = await sendEmail({ to: 'a@b.com', subject: 's', html: 'x' }, { env, sendImpl: async () => { throw sesError('MessageRejected', 400, 'not verified'); } });
    assert.deepEqual(refused, { ok: false, reason: 'ses_error', status: 400, retryable: false, detail: 'MessageRejected: not verified' });
    const busy = await sendEmail({ to: 'a@b.com', subject: 's', html: 'x' }, { env, sendImpl: async () => { throw sesError('TooManyRequestsException', 429); } });
    assert.equal(busy.retryable, true);
    const dead = await sendEmail({ to: 'a@b.com', subject: 's', html: 'x' }, { env, sendImpl: async () => { throw Object.assign(new Error('socket hang up'), { code: 'ECONNRESET' }); } });
    assert.deepEqual(dead, { ok: false, reason: 'network_error', detail: 'socket hang up' });
});

// ── a delivery request: a UK haulier, port to warehouse (2026-11-24) ──────

test('validateRequest: a request has a kind, freight unless it says delivery', () => {
    const body = { forwarderIds: [3], windowStart: '2026-12-01', windowEnd: '2026-12-10', services: ['UK port to warehouse'] };
    assert.equal(F.validateRequest(body, '2026-11-24').value.kind, 'freight');
    assert.equal(F.validateRequest({ ...body, kind: 'freight' }, '2026-11-24').value.kind, 'freight');
    assert.equal(F.validateRequest({ ...body, kind: 'delivery' }, '2026-11-24').value.kind, 'delivery');
    for (const kind of ['air', '', null, 7]) assert.equal(F.validateRequest({ ...body, kind }, '2026-11-24').field, 'kind', String(kind));
});

const DELIVERY_REQUEST = { kind: 'delivery', windowStart: '2026-12-01', windowEnd: '2026-12-10', services: ['UK port to warehouse'], containerSize: '40HQ' };
const DELIVERY_OPTION = { sailingDate: '2026-12-04', transitDays: 0, portFrom: 'Felixstowe', portTo: 'Unit 5, Leeds', service: 'UK port to warehouse', price: '650', currency: 'GBP' };

test('validateOptions: a delivery option is a collection day and the days to deliver; it has no loading date of its own', () => {
    const { options } = F.validateOptions([DELIVERY_OPTION], DELIVERY_REQUEST);
    // Stored in the same columns: the collection day is the sailing date, and the loading date is that day too.
    assert.deepEqual(options[0], {
        loadingDate: '2026-12-04', sailingDate: '2026-12-04', transitDays: 0, portFrom: 'Felixstowe', portTo: 'Unit 5, Leeds',
        service: 'UK port to warehouse', containerSize: '40HQ', price: '650.00', currency: 'GBP', carrier: null, validUntil: null, note: null,
    });
    // Delivered the same day is 0; a number of days is as it is.
    assert.equal(F.validateOptions([{ ...DELIVERY_OPTION, transitDays: 2 }], DELIVERY_REQUEST).options[0].transitDays, 2);
    assert.equal(F.arrivalDate('2026-12-04', 0), '2026-12-04');
    // A loading date sent with it is not kept: the collection day is.
    assert.equal(F.validateOptions([{ ...DELIVERY_OPTION, loadingDate: '2026-11-20' }], DELIVERY_REQUEST).options[0].loadingDate, '2026-12-04');
    const field = patch => F.validateOptions([{ ...DELIVERY_OPTION, ...patch }], DELIVERY_REQUEST).field;
    assert.equal(field({ sailingDate: 'soon' }), 'options[0].sailingDate');
    assert.equal(field({ sailingDate: '2027-02-01' }), 'options[0].sailingDate', 'the collection day is inside the range asked for, give or take the margin');
    assert.equal(field({ transitDays: -1 }), 'options[0].transitDays');
    assert.equal(field({ transitDays: 1.5 }), 'options[0].transitDays');
    assert.equal(field({ transitDays: '' }), 'options[0].transitDays');
    assert.equal(field({ portFrom: '' }), 'options[0].portFrom');
    assert.equal(field({ portTo: '' }), 'options[0].portTo');
    assert.match(F.validateOptions([{ ...DELIVERY_OPTION, sailingDate: 'soon' }], DELIVERY_REQUEST).error, /collection date/);
    assert.match(F.validateOptions([{ ...DELIVERY_OPTION, transitDays: -1 }], DELIVERY_REQUEST).error, /0 to/);
});

test('validateOptions: a freight option is judged exactly as before, a kind or none on the request', () => {
    for (const request of [REQUEST, { ...REQUEST, kind: 'freight' }]) {
        assert.equal(F.validateOptions([OPTION], request).options[0].loadingDate, '2026-12-05');
        assert.equal(F.validateOptions([{ ...OPTION, transitDays: 0 }], request).field, 'options[0].transitDays', 'a sailing takes at least a day');
        const noLoading = { ...OPTION };
        delete noLoading.loadingDate;
        assert.equal(F.validateOptions([noLoading], request).field, 'options[0].loadingDate');
    }
});

test('the words of a delivery request and of its options: collection and delivery, never sailing or loading', () => {
    const request = { kind: 'delivery', windowStart: '2026-12-01', windowEnd: '2026-12-10', origin: 'Felixstowe', destination: 'Unit 5, Leeds', containerSize: '40HQ', services: ['UK port to warehouse'], note: null };
    const url = 'https://portal.example/freight/abc';
    const asked = F.quoteBlockHtml({ request, url, expiresOn: '2026-12-31', registerUrl: '' });
    assert.match(asked, /UK delivery quote request/);
    assert.match(asked, /Collection between/);
    assert.match(asked, /Collect from/);
    assert.match(asked, /Deliver to/);
    assert.doesNotMatch(asked, /Sailing between|sailing|开船|装柜|航程/);
    const option = { loadingDate: '2026-12-04', sailingDate: '2026-12-04', transitDays: 1, portFrom: 'Felixstowe', portTo: 'Unit 5, Leeds', service: 'UK port to warehouse', containerSize: '40HQ', price: '650.00', currency: 'GBP', carrier: null };
    const booked = F.bookingBlockHtml({ option, url, note: null, kind: 'delivery' });
    assert.match(booked, /Please book this delivery/);
    assert.match(booked, /Collection/);
    assert.match(booked, /Delivered/);
    assert.match(booked, /2026-12-05/, 'the day it is delivered: collection plus the days');
    assert.doesNotMatch(booked, /Loading|Sailing|Transit|sailing|装柜|开船|航程/);
    // A freight request and option read as before, a kind said or not.
    assert.match(F.quoteBlockHtml({ request: { ...request, kind: 'freight' }, url, expiresOn: '2026-12-31', registerUrl: '' }), /Sailing between/);
    for (const kind of [undefined, 'freight']) {
        const sea = F.bookingBlockHtml({ option, url, note: null, kind });
        assert.match(sea, /Please book this sailing/);
        assert.match(sea, /Loading \/ 装柜日期/);
    }
});
