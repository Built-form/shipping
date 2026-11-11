'use strict';

// Unit tests for src/lib/artwork-confirmations.js — the supplier's artwork
// confirmation per PO line (docs/artwork-confirmations.md): the token and link,
// which lines can be confirmed, a request's state, the email block, the
// deposit gate, and what applying confirmations writes — plus
// applyArtworkConfirmations (src/handlers/artwork-confirmed.js) against a fake
// connection. No database.
//   node --test tools/test-artwork-confirmations-lib.js

const test = require('node:test');
const assert = require('node:assert/strict');
const A = require('../src/lib/artwork-confirmations');
const { applyArtworkConfirmations } = require('../src/handlers/artwork-confirmed');
const { DEFAULT_EMAIL_TEMPLATES, ARTWORK_CONFIRMATION_TEMPLATE_KEY } = require('../src/lib/email-template-defaults');

const CLEAR = { clear: true, reason: 'no_deposit' };
const HELD = { clear: false, reason: 'deposit_unpaid' };

test('token and link', () => {
    const t = A.newToken();
    assert.match(t, A.TOKEN_RE);
    assert.notEqual(t, A.newToken());
    assert.equal(A.isToken('abc'), false);
    assert.equal(A.isToken(t.toUpperCase()), false);
    assert.equal(A.artworkUrl('https://p.example', t), `https://p.example/artwork/${t}`);
});

test('the template is known to the in-code fallback', () => {
    const tpl = DEFAULT_EMAIL_TEMPLATES.find(x => x.key === ARTWORK_CONFIRMATION_TEMPLATE_KEY);
    assert.equal(tpl.name, 'Artwork Confirmed');
    assert.equal(tpl.category, 'Artwork');
    assert.match(tpl.subject, /\{poNumber\}/);
});

test('isTickable: production-phase lines the supplier has not confirmed', () => {
    assert.equal(A.isTickable({ status: 'PO_SENT', artwork_confirmed_by_supplier: null }), true);
    assert.equal(A.isTickable({ status: 'READY', artworkConfirmedBySupplier: '' }), true);
    assert.equal(A.isTickable({ status: 'PO_SENT', artwork_confirmed_by_supplier: '2026-11-01' }), false);
    assert.equal(A.isTickable({ status: 'SCHEDULED', artwork_confirmed_by_supplier: null }), false);
    assert.equal(A.isTickable({ status: 'ON_SEA', artwork_confirmed_by_supplier: null }), false);
    assert.equal(A.isTickable(null), false);
    // _FQC sample lines are never asked about, by code or by asin.
    assert.equal(A.isTickable({ status: 'PO_SENT', jf_code: 'JF0787_FQC', artwork_confirmed_by_supplier: null }), false);
    assert.equal(A.isTickable({ status: 'PO_SENT', jf_code: 'JF0787', asin: 'b0abc_fqc', artwork_confirmed_by_supplier: null }), false);
    assert.equal(A.isTickable({ status: 'PO_SENT', jf_code: 'JF0787', artwork_confirmed_by_supplier: null }), true);
});

test('fqcSamplesOf: only the _FQC samples of the lines in the email', () => {
    const all = [
        { id: 1, jf_code: 'JF0787', asin: 'B0AAA' },
        { id: 2, jf_code: 'JF0895', asin: 'B0BBB' },
        { id: 3, jf_code: 'jf0787_fqc', asin: null },      // JF0787's sample, by code, any case
        { id: 4, jf_code: 'JF0895_FQC', asin: null },      // JF0895's sample
        { id: 5, jf_code: 'JF0787', asin: 'B0AAA_FQC' },   // JF0787's sample, by asin
        { id: 6, jf_code: 'JF9999_FQC', asin: null },      // its product is not on the PO
    ];
    const ids = (parents) => A.fqcSamplesOf(parents, all).map(l => l.id);
    assert.deepEqual(ids([all[0]]), [3, 5]);
    assert.deepEqual(ids([all[1]]), [4]);
    assert.deepEqual(ids([all[0], all[1]]), [3, 4, 5]);
    assert.deepEqual(ids([]), []);
    // A sample is never a parent: JF0787_FQC does not pull in anything.
    assert.deepEqual(ids([all[2]]), []);
    // JF078 is not JF0787: no prefix matching.
    assert.deepEqual(ids([{ id: 9, jf_code: 'JF078', asin: null }]), []);
});

test('staff\'s own Artwork Confirmed date is a different field: it does not confirm for the supplier', () => {
    const line = { status: 'IN_PRODUCTION', artwork_confirmed_date: '2026-10-01', artwork_confirmed_by_supplier: null };
    assert.equal(A.isConfirmed(line), false);
    assert.equal(A.isTickable(line), true);
});

test('chooseLines: default is every tickable line; a named line must be tickable', () => {
    const lines = [
        { id: 1, status: 'PO_SENT', artwork_confirmed_by_supplier: null },
        { id: 2, status: 'IN_PRODUCTION', artwork_confirmed_by_supplier: '2026-11-01' },
        { id: 3, status: 'IN_PRODUCTION', artwork_confirmed_by_supplier: null },
    ];
    assert.deepEqual(A.chooseLines(lines), { orderIds: [1, 3] });
    assert.deepEqual(A.chooseLines(lines, [3, '3']), { orderIds: [3] });
    assert.equal(A.chooseLines(lines, [2]).code, 'BAD_FIELD');
    assert.equal(A.chooseLines(lines, [99]).code, 'BAD_FIELD');
    assert.equal(A.chooseLines(lines, []).code, 'BAD_FIELD');
    assert.equal(A.chooseLines([lines[1]]).code, 'NOTHING_TO_CONFIRM');
});

test('requestState: revoked > expired > done > open', () => {
    const open = [{ confirmed: false }, { confirmed: true }];
    const done = [{ confirmed: true }];
    assert.equal(A.requestState({ revoked_at: new Date(), expired: 1 }, done), 'revoked');
    assert.equal(A.requestState({ revoked_at: null, expired: 1 }, done), 'expired');
    assert.equal(A.requestState({ revoked_at: null, expired: 0 }, done), 'done');
    assert.equal(A.requestState({ revoked_at: null, expired: 0 }, open), 'open');
    assert.equal(A.requestState({ revoked_at: null, expired: 0 }, []), 'done');
});

test('parseOrderIds: driver-parsed or text, cleaned', () => {
    assert.deepEqual(A.parseOrderIds('[3, 3, "4", 0, "x"]'), [3, 4]);
    assert.deepEqual(A.parseOrderIds([5]), [5]);
    assert.deepEqual(A.parseOrderIds('not json'), []);
});

test('artworkBlockHtml: items and link escaped', () => {
    const html = A.artworkBlockHtml({
        poNumber: 'PO<1>',
        url: 'https://p.example/artwork/abc?x="y"',
        lines: [{ id: 1, jf_code: 'JF<1>', product_name: 'Mask & strap', quantity: 1200 }],
        expiresOn: '2027-01-10',
    });
    assert.ok(html.includes('PO&lt;1&gt;'));
    assert.ok(html.includes('JF&lt;1&gt; · Mask &amp; strap'));
    assert.ok(!html.includes('1,200'), 'no quantities in the supplier email');
    assert.ok(html.includes('https://p.example/artwork/abc?x=&quot;y&quot;'));
    assert.ok(html.includes('2027-01-10'));
    assert.ok(!html.includes('<1>'));
});

test('confirmerOf', () => {
    assert.equal(A.confirmerOf({ confirmed_by_email: 'a@s.cn' }, ['x@s.cn']), 'a@s.cn');
    assert.equal(A.confirmerOf({ confirmed_by_name: 'Li Wei' }, ['x@s.cn']), 'x@s.cn (Li Wei)');
    assert.equal(A.confirmerOf({}, ['x@s.cn']), 'x@s.cn');
    assert.equal(A.confirmerOf({ confirmed_by_name: 'Li Wei' }, []), 'Li Wei');
    assert.equal(A.confirmerOf({}, undefined), 'email link');
});

test('depositGate: clear only with no deposit required, or the deposit paid', () => {
    assert.deepEqual(A.depositGate({ depositPct: 0, depositPaid: false }), { depositPct: 0, depositPaid: false, clear: true, reason: 'no_deposit' });
    assert.deepEqual(A.depositGate({ depositPct: 30, depositPaid: true }), { depositPct: 30, depositPaid: true, clear: true, reason: 'deposit_paid' });
    assert.deepEqual(A.depositGate({ depositPct: 30, depositPaid: false }), { depositPct: 30, depositPaid: false, clear: false, reason: 'deposit_unpaid' });
    // Nothing says what the terms are: we cannot say no deposit is required.
    assert.deepEqual(A.depositGate({ depositPct: null, depositPaid: false }), { depositPct: null, depositPaid: false, clear: false, reason: 'terms_unknown' });
    assert.equal(A.depositGate({ depositPct: null, depositPaid: true }).clear, true);
    assert.equal(A.depositGate({ depositPct: '0.000', depositPaid: false }).reason, 'no_deposit');
});

const CONFIRMATIONS = [
    { id: 10, order_id: 1, request_id: 7, confirmed_on: '2026-11-11' },
    { id: 11, order_id: 2, request_id: 7, confirmed_on: '2026-11-11', confirmed_by_name: 'Li' },
    { id: 12, order_id: 3, request_id: null, confirmed_on: '2026-11-11', confirmed_by_email: 'u@s.cn' },
    { id: 13, order_id: 4, request_id: null, confirmed_on: '2026-11-11' },
];
const ORDERS = [
    { id: 1, status: 'PO_SENT', artwork_confirmed_by_supplier: null },
    { id: 2, status: 'IN_PRODUCTION', artwork_confirmed_by_supplier: null },
    { id: 3, status: 'PO_SENT', artwork_confirmed_by_supplier: '2026-10-01' },
];

test('planApply, gate clear: confirms the line and moves PO_SENT into production', () => {
    const plan = A.planApply(CONFIRMATIONS, ORDERS, new Map([[7, ['sales@s.cn']]]), CLEAR);
    assert.deepEqual(plan.appliedIds, [10, 11, 12, 13]);
    assert.deepEqual(plan.updates.map(u => [u.orderId, u.confirmedOn, u.status]), [
        [1, '2026-11-11', 'IN_PRODUCTION'],
        [2, '2026-11-11', null],
    ]);
    assert.deepEqual(plan.updates[0].before, { artworkConfirmedBySupplier: null, status: 'PO_SENT' });
    assert.equal(plan.updates[0].after.artworkConfirmedBySupplier, '2026-11-11');
    assert.equal(plan.updates[0].after.status, 'IN_PRODUCTION');
    assert.equal(plan.updates[0].userEmail, 'supplier-portal:sales@s.cn');
    assert.equal(plan.updates[1].userEmail, 'supplier-portal:sales@s.cn (Li)');
    assert.equal('status' in plan.updates[1].after, false);
    // Never the staff field.
    assert.equal(JSON.stringify(plan).includes('artworkConfirmedDate'), false);
});

test('planApply, gate held: the confirmation is recorded, the status stays, the note says why', () => {
    for (const gate of [HELD, { clear: false, reason: 'terms_unknown' }, undefined]) {
        const plan = A.planApply(CONFIRMATIONS, ORDERS, new Map(), gate);
        assert.deepEqual(plan.updates.map(u => [u.orderId, u.confirmedOn, u.status]), [
            [1, '2026-11-11', null],
            [2, '2026-11-11', null],
        ]);
        assert.equal('status' in plan.updates[0].after, false);
        assert.match(plan.updates[0].after.note, /not moved to production/);
        // A line that was never going to move carries no such remark.
        assert.doesNotMatch(plan.updates[1].after.note, /not moved/);
    }
    assert.match(A.planApply(CONFIRMATIONS, ORDERS, new Map(), HELD).updates[0].after.note, /deposit has not been paid/);
});

// A connection that answers by the statement's first words and records writes.
function fakeConn({ confirmations, orders, requests = [] }) {
    const log = [];
    return {
        log,
        beginTransaction: async () => { log.push('BEGIN'); },
        commit: async () => { log.push('COMMIT'); },
        rollback: async () => { log.push('ROLLBACK'); },
        query: async (sql, params) => {
            const s = sql.replace(/\s+/g, ' ').trim();
            if (s.startsWith('SELECT id, order_id')) return [confirmations];
            if (s.startsWith('SELECT id, status')) return [orders];
            if (s.startsWith('SELECT id, sent_to')) return [requests];
            if (s.startsWith('SELECT id, supplier FROM purchase_orders')) return [[{ id: params[0], supplier: 'Acme' }]];
            log.push([s.split(' ').slice(0, 3).join(' '), params, s]);
            return [{ affectedRows: 1 }];
        },
    };
}

const TWO = () => ({
    confirmations: [
        { id: 10, order_id: 1, request_id: 7, confirmed_on: '2026-11-11' },
        { id: 11, order_id: 2, request_id: 7, confirmed_on: '2026-11-11' },
    ],
    orders: [
        { id: 1, status: 'PO_SENT', artwork_confirmed_by_supplier: null },
        { id: 2, status: 'READY', artwork_confirmed_by_supplier: null },
    ],
    requests: [{ id: 7, sent_to: '["sales@s.cn"]' }],
});

test('applyArtworkConfirmations: nothing waiting is a no-op', async () => {
    const conn = fakeConn({ confirmations: [], orders: [] });
    assert.deepEqual(await applyArtworkConfirmations(conn, 5), { outcome: 'noop', updated: 0, moved: 0, applied: 0 });
    assert.deepEqual(conn.log, ['BEGIN', 'ROLLBACK']);
});

test('applyArtworkConfirmations, gate clear: writes the new field and the move, the audit row, and stamps the confirmations', async () => {
    const conn = fakeConn(TWO());
    const out = await applyArtworkConfirmations(conn, 5, { loadGate: async () => CLEAR });
    assert.deepEqual(out, { outcome: 'applied', updated: 2, moved: 1, applied: 2, gate: 'no_deposit' });
    const writes = conn.log.filter(Array.isArray);
    assert.equal(writes[0][2], 'UPDATE orders SET artwork_confirmed_by_supplier = ?, status = ? WHERE id = ?');
    assert.deepEqual(writes[0][1], ['2026-11-11', 'IN_PRODUCTION', 1]);
    assert.equal(writes[1][0], 'INSERT INTO audit_log');
    assert.equal(writes[1][1][5], 'supplier-portal:sales@s.cn');
    assert.equal(writes[2][2], 'UPDATE orders SET artwork_confirmed_by_supplier = ? WHERE id = ?');
    assert.deepEqual(writes[4].slice(0, 2), ['UPDATE artwork_confirmations SET', [10, 11]]);
    assert.equal(conn.log[conn.log.length - 1], 'COMMIT');
    // artwork_confirmed_date, staff's field, is never written.
    assert.ok(writes.every(w => !/artwork_confirmed_date/.test(w[2])));
});

test('applyArtworkConfirmations, deposit unpaid: records the confirmation and leaves the status', async () => {
    const conn = fakeConn(TWO());
    const out = await applyArtworkConfirmations(conn, 5, { loadGate: async () => HELD });
    assert.deepEqual(out, { outcome: 'applied', updated: 2, moved: 0, applied: 2, gate: 'deposit_unpaid' });
    const writes = conn.log.filter(Array.isArray);
    assert.ok(writes.every(w => !/status = \?/.test(w[2])));
    assert.match(JSON.parse(writes[1][1][4]).note, /deposit has not been paid/);
});

test('applyArtworkConfirmations: a gate that cannot be read holds the move, and still records', async () => {
    const conn = fakeConn(TWO());
    const out = await applyArtworkConfirmations(conn, 5, { loadGate: async () => { throw Object.assign(new Error('no table'), { errno: 1146 }); } });
    assert.equal(out.updated, 2);
    assert.equal(out.moved, 0);
    assert.equal(out.gate, 'terms_unknown');
});

test('applyArtworkConfirmations: the gate is not read when no line could move', async () => {
    const data = TWO();
    data.orders[0].status = 'IN_PRODUCTION';
    let asked = 0;
    await applyArtworkConfirmations(fakeConn(data), 5, { loadGate: async () => { asked++; return CLEAR; } });
    assert.equal(asked, 0);
});
