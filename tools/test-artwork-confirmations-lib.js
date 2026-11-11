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
    // The body: both languages, the PO number, and the two things it asks for.
    assert.match(tpl.bodyHtml, /^<div[^>]*><p>Greetings,<\/p>/);
    assert.match(tpl.bodyHtml, /selected items under <strong>\{poNumber\}<\/strong>/);
    assert.match(tpl.bodyHtml, /glue rather than staples/);
    assert.match(tpl.bodyHtml, /tick the checkbox next to each item/);
    assert.match(tpl.bodyHtml, /采购订单 <strong>\{poNumber\}<\/strong>/);
    assert.match(tpl.bodyHtml, /胶水/);
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
function fakeConn({ confirmations, orders, requests = [], samples = [] }) {
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
            if (s.startsWith('SELECT id, jf_code, asin, status')) return [samples];
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
    assert.deepEqual(out, { outcome: 'applied', updated: 2, moved: 1, samplesMoved: 0, applied: 2, gate: 'no_deposit' });
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
    assert.deepEqual(out, { outcome: 'applied', updated: 2, moved: 0, samplesMoved: 0, applied: 2, gate: 'deposit_unpaid' });
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

// ── _FQC sample lines follow their product into production ──

const PARENTS = [
    { id: 1, status: 'PO_SENT', artwork_confirmed_by_supplier: null, jf_code: 'JF0787', asin: 'B0AAA' },
    { id: 2, status: 'READY', artwork_confirmed_by_supplier: null, jf_code: 'JF0900', asin: null },
];
const SAMPLES = [
    { id: 21, status: 'PO_SENT', jf_code: 'jf0787_fqc', asin: null },   // product moved: follows
    { id: 22, status: 'PO_SENT', jf_code: 'JF0900_FQC', asin: null },   // product confirmed but did not move
    { id: 23, status: 'PO_SENT', jf_code: 'JF0555_FQC', asin: null },   // product not ticked
    { id: 24, status: 'READY_FOR_QC', jf_code: null, asin: 'B0AAA_FQC' }, // already further on
];

test('planSampleMoves: a sample moves only with its own product, only from PO_SENT', () => {
    const confirmations = [
        { id: 10, order_id: 1, request_id: 7, confirmed_on: '2026-11-11' },
        { id: 11, order_id: 2, request_id: 7, confirmed_on: '2026-11-11' },
    ];
    const plan = A.planApply(confirmations, PARENTS, new Map([[7, ['sales@s.cn']]]), CLEAR);
    const moves = A.planSampleMoves(plan.updates, PARENTS, SAMPLES);
    assert.deepEqual(moves.map(m => [m.orderId, m.parentOrderId, m.status]), [[21, 1, 'IN_PRODUCTION']]);
    assert.deepEqual(moves[0].before, { status: 'PO_SENT' });
    assert.equal(moves[0].after.status, 'IN_PRODUCTION');
    assert.match(moves[0].after.note, /with JF0787, whose artwork the supplier confirmed/);
    assert.equal(moves[0].userEmail, 'supplier-portal:sales@s.cn');
    // The sample is given no date of either kind.
    assert.equal(/artworkConfirmed/.test(JSON.stringify(moves)), false);
});

test('planSampleMoves: nothing follows a product the deposit gate held', () => {
    const confirmations = [{ id: 10, order_id: 1, request_id: null, confirmed_on: '2026-11-11' }];
    const plan = A.planApply(confirmations, PARENTS, new Map(), HELD);
    assert.deepEqual(A.planSampleMoves(plan.updates, PARENTS, SAMPLES), []);
    assert.deepEqual(A.planSampleMoves([], PARENTS, SAMPLES), []);
});

test('planSampleMoves: a sample matched on asin, and never moved twice', () => {
    const parents = [
        { id: 1, status: 'PO_SENT', artwork_confirmed_by_supplier: null, jf_code: null, asin: 'B0AAA' },
        { id: 3, status: 'PO_SENT', artwork_confirmed_by_supplier: null, jf_code: null, asin: 'b0aaa' },
    ];
    const confirmations = [
        { id: 10, order_id: 1, request_id: null, confirmed_on: '2026-11-11' },
        { id: 11, order_id: 3, request_id: null, confirmed_on: '2026-11-11' },
    ];
    const plan = A.planApply(confirmations, parents, new Map(), CLEAR);
    const moves = A.planSampleMoves(plan.updates, parents, [{ id: 24, status: 'PO_SENT', jf_code: null, asin: 'B0AAA_FQC' }]);
    assert.deepEqual(moves.map(m => [m.orderId, m.parentOrderId]), [[24, 1]]);
    assert.match(moves[0].after.note, /with B0AAA,/);
});

const WITH_SAMPLE = () => ({
    confirmations: [
        { id: 10, order_id: 1, request_id: 7, confirmed_on: '2026-11-11' },
        { id: 11, order_id: 2, request_id: 7, confirmed_on: '2026-11-11' },
    ],
    orders: PARENTS.map(o => ({ ...o })),
    samples: SAMPLES.filter(s => s.status === 'PO_SENT').map(s => ({ ...s })),
    requests: [{ id: 7, sent_to: '["sales@s.cn"]' }],
});

test('applyArtworkConfirmations, gate clear: the moved line takes its _FQC sample into production', async () => {
    const conn = fakeConn(WITH_SAMPLE());
    const out = await applyArtworkConfirmations(conn, 5, { loadGate: async () => CLEAR });
    assert.deepEqual(out, { outcome: 'applied', updated: 2, moved: 1, samplesMoved: 1, applied: 2, gate: 'no_deposit' });
    const writes = conn.log.filter(Array.isArray);
    const sample = writes.filter(w => w[2] === 'UPDATE orders SET status = ? WHERE id = ? AND status = ?');
    assert.deepEqual(sample.map(w => w[1]), [['IN_PRODUCTION', 21, 'PO_SENT']]);
    const audit = writes[writes.indexOf(sample[0]) + 1];
    assert.equal(audit[0], 'INSERT INTO audit_log');
    assert.equal(audit[1][5], 'supplier-portal:sales@s.cn');
    assert.match(JSON.parse(audit[1][4]).note, /with JF0787/);
    // Only the product's own lines get the supplier date.
    assert.deepEqual(writes.filter(w => /artwork_confirmed_by_supplier/.test(w[2])).map(w => w[1][w[1].length - 1]), [1, 2]);
    assert.equal(conn.log[conn.log.length - 1], 'COMMIT');
});

test('applyArtworkConfirmations, deposit unpaid: the sample stays, and is not even read', async () => {
    const data = WITH_SAMPLE();
    const conn = fakeConn(data);
    let read = 0;
    const query = conn.query;
    conn.query = async (sql, params) => {
        if (/SELECT id, jf_code, asin, status/.test(sql.replace(/\s+/g, ' '))) read += 1;
        return query(sql, params);
    };
    const out = await applyArtworkConfirmations(conn, 5, { loadGate: async () => HELD });
    assert.equal(out.moved, 0);
    assert.equal(out.samplesMoved, 0);
    assert.equal(read, 0);
    assert.ok(conn.log.filter(Array.isArray).every(w => !/status = \?/.test(w[2])));
});

test('applyArtworkConfirmations: a sample that changed under us is not counted or audited', async () => {
    const conn = fakeConn(WITH_SAMPLE());
    const query = conn.query;
    conn.query = async (sql, params) => {
        const out = await query(sql, params);
        return /^UPDATE orders SET status = \? WHERE id/.test(sql.trim()) ? [{ affectedRows: 0 }] : out;
    };
    const out = await applyArtworkConfirmations(conn, 5, { loadGate: async () => CLEAR });
    assert.equal(out.samplesMoved, 0);
    const audits = conn.log.filter(Array.isArray).filter(w => w[0] === 'INSERT INTO audit_log');
    assert.equal(audits.length, 2);
});
