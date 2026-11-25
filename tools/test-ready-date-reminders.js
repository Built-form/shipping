'use strict';

// Unit tests for the ready-date reminders: the rules
// (src/lib/ready-date-reminders.js) and the sweep
// (src/services/ready-date-reminders.js). No database and no Front: a fake
// connection answers by the SQL it is asked, and records every statement.
//
//   node --test tools/test-ready-date-reminders.js

const test = require('node:test');
const assert = require('node:assert/strict');

const R = require('../src/lib/ready-date-reminders');
const { runReadyDateReminders } = require('../src/services/ready-date-reminders');

const quiet = { info() {}, warn() {}, error() {} };
const ENV = { SUPPLIER_PORTAL_URL: 'https://portal.example.com/' };
const WEDNESDAY = '2026-11-18';

// A waiting line as the sweep reads it. No estimated date = it needs one
// ('estimated'); a past one = it needs the actual date ('actual').
const line = (id, poId, poNumber, supplier, jf, estimated = null) => ({
    id, purchase_order_id: poId, po_number: poNumber, po_supplier: supplier, jf_code: jf, product_name: `Product ${jf}`,
    status: 'IN_PRODUCTION', estimated_ready_date: estimated,
});

/**
 * A pool of one fake connection. `data`: lines (the waiting rows), pending
 * (portal change requests), sends (PO email recipients by PO id), chases (open
 * ready_date_reminders rows by supplier key), receipts (email_receipts rows by
 * chase id).
 */
function fakePool(data = {}) {
    const queries = [];
    let nextId = 100;
    const conn = {
        release() {},
        async query(sql, params = []) {
            const text = String(sql).replace(/\s+/g, ' ').trim();
            queries.push({ sql: text, params });
            if (/^SELECT o\.id, o\.purchase_order_id/.test(text)) return [data.lines || []];
            if (/po_change_requests/.test(text)) return [data.pending || []];
            if (/FROM email_templates/.test(text)) return [[]];
            if (/FROM purchase_order_document_sends/.test(text)) {
                return [params[0].filter(id => (data.sends || {})[id]).map(id => ({ purchase_order_id: id, sent_to: JSON.stringify(data.sends[id]) }))];
            }
            if (/FROM artwork_confirmation_requests/.test(text)) return [[]];
            if (/^SELECT id, order_ids, send_count/.test(text)) {
                const chase = (data.chases || {})[params[0]];
                return [chase ? [chase] : []];
            }
            if (/^INSERT INTO ready_date_reminders/.test(text)) return [{ insertId: nextId++ }];
            if (/^UPDATE ready_date_reminders SET last_sent_at = UTC_TIMESTAMP\(\)/.test(text)) return [{ affectedRows: data.claimRefused ? 0 : 1 }];
            if (/^SELECT id, reminder_count FROM email_receipts/.test(text)) {
                const receipt = (data.receipts || {})[params[1]];
                return [receipt ? [receipt] : []];
            }
            return [{ affectedRows: 1 }];
        },
    };
    return { queries, getConnection: async () => conn };
}

function runner(pool, extra = {}) {
    const sent = [];
    const opts = {
        today: WEDNESDAY, env: ENV, log: quiet,
        loadDepositGate: async () => ({ clear: true }),
        sendEmail: async (msg) => { sent.push(msg); return { ok: true, messageId: 'msg_1' }; },
        ...extra,
    };
    return { sent, run: () => runReadyDateReminders(pool, opts) };
}

const wrote = (pool, re) => pool.queries.filter(q => re.test(q.sql));

test('isWeekend: Saturday and Sunday only', () => {
    assert.equal(R.isWeekend('2026-11-20'), false); // Friday
    assert.equal(R.isWeekend('2026-11-21'), true);
    assert.equal(R.isWeekend('2026-11-22'), true);
    assert.equal(R.isWeekend('2026-11-23'), false);
});

test('supplierKey / groupBySupplier: two spellings of one company are one email; no name is a group per PO', () => {
    assert.equal(R.supplierKey('Suzhou Sunmed Co., Ltd'), R.supplierKey('Suzhou Sunmed Co.,Ltd.'));
    assert.equal(R.supplierKey('  '), '');
    const groups = R.groupBySupplier([
        line(1, 419, 'PO_00419J', 'Suzhou Sunmed Co.,Ltd.', 'JF0001'),
        line(2, 409, 'PO_00409J', 'Suzhou Sunmed Co., Ltd', 'JF0002'),
        line(3, 409, 'PO_00409J', 'Suzhou Sunmed Co., Ltd', 'JF0003', '2026-11-10'),
        line(4, 500, 'PO_00500J', null, 'JF0004'),
        line(5, 501, 'PO_00501J', '', 'JF0005'),
    ]);
    assert.equal(groups.length, 3);
    assert.deepEqual(groups[0].pos.map(p => [p.poNumber, p.lines.map(l => l.kind)]), [['PO_00409J', ['estimated', 'actual']], ['PO_00419J', ['estimated']]]);
    assert.deepEqual(groups.slice(1).map(g => g.key), ['po:500', 'po:501']);
});

test('waitingSql: board columns 01 to 06, no actual date, no _FQC samples; artwork a week ago or an estimate that has passed', () => {
    const sql = R.waitingSql('o', '?').replace(/\s+/g, ' ');
    assert.match(sql, /o\.actual_ready_date IS NULL/);
    assert.match(sql, /o\.status IN \('SCHEDULED', 'PO_SENT', 'IN_PRODUCTION', 'READY_FOR_QC', 'READY', 'CONSOLIDATED'\)/);
    assert.match(sql, /NOT LIKE '%\\_FQC'/);
    assert.match(sql, /\(\(o\.estimated_ready_date IS NULL AND o\.artwork_confirmed_date IS NOT NULL AND o\.artwork_confirmed_date <= \? - INTERVAL 7 DAY\) OR \(o\.estimated_ready_date IS NOT NULL AND o\.estimated_ready_date < \?\)\)/);
    assert.equal(sql.split('?').length - 1, R.WAITING_TODAYS);
    assert.match(R.waitingSql(), /<= CURDATE\(\) - INTERVAL 7 DAY/);
    assert.equal(R.kindOf({ estimated_ready_date: null }), 'estimated');
    assert.equal(R.kindOf({ estimated_ready_date: '2026-11-10' }), 'actual');
});

test('answersKind: a pending portal request answers only the date that line is asked for', () => {
    assert.equal(R.answersKind({ estimatedReadyDate: '2026-12-01' }, 'estimated'), true);
    assert.equal(R.answersKind('{"estimatedReadyDate":"2026-12-01"}', 'estimated'), true);
    assert.equal(R.answersKind({ estimatedReadyDate: null }, 'estimated'), false);
    assert.equal(R.answersKind({ actualReadyDate: '2026-12-01' }, 'estimated'), false);
    assert.equal(R.answersKind({ actualReadyDate: '2026-12-01' }, 'actual'), true);
    assert.equal(R.answersKind({ estimatedReadyDate: '2026-12-01' }, 'actual'), false);
    assert.equal(R.answersKind('not json', 'actual'), false);
});

test('reminderBlockHtml: a section per kind that has items; one PO links to its page, several to the list', () => {
    const base = { portalBase: 'https://portal.example.com', registerUrl: 'https://portal.example.com/register' };
    const one = R.reminderBlockHtml({
        ...base, pos: [{ id: 7, poNumber: 'PO_7', lines: [{ id: 1, kind: 'estimated', jf_code: 'JF<1>', product_name: 'Gloves' }] }],
    });
    assert.match(one, /href="https:\/\/portal\.example\.com\/orders\/7"/);
    assert.match(one, /JF&lt;1&gt; · Gloves/);
    assert.match(one, /Estimated ready date needed/);
    assert.doesNotMatch(one, /Actual ready date needed/);
    assert.doesNotMatch(one, /Reminder #/);
    const two = R.reminderBlockHtml({
        ...base, reminderNumber: 3,
        pos: [
            { id: 7, poNumber: 'PO_7', lines: [{ id: 1, kind: 'estimated', jf_code: 'JF1' }, { id: 2, kind: 'actual', jf_code: 'JF2', estimated_ready_date: '2026-11-10' }] },
            { id: 8, poNumber: 'PO_8', lines: [{ id: 3, kind: 'actual', jf_code: 'JF3', estimated_ready_date: '2026-11-12' }] },
        ],
    });
    assert.match(two, /Reminder #3 · 第 3 次提醒/);
    assert.match(two, /href="https:\/\/portal\.example\.com\/orders"/);
    // The estimated section first, then the actual one with the date that passed.
    assert.ok(two.indexOf('Estimated ready date needed') < two.indexOf('Actual ready date needed'));
    assert.ok(two.indexOf('JF1') < two.indexOf('Actual ready date needed'));
    assert.ok(two.indexOf('JF2') > two.indexOf('Actual ready date needed'));
    assert.match(two, /JF2 <span[^>]*>· estimated \/ 预计 2026-11-10/);
    assert.match(two, /JF3 <span[^>]*>· estimated \/ 预计 2026-11-12/);
});

test('sweep: nothing at a weekend, and nothing without the portal address', async () => {
    const pool = fakePool({ lines: [line(1, 7, 'PO_7', 'Acme', 'JF1')] });
    const weekend = await runner(pool, { today: '2026-11-21' }).run();
    assert.equal(weekend.aborted, 'weekend');
    const noUrl = await runner(pool, { env: {} }).run();
    assert.equal(noUrl.aborted, 'no_portal_url');
    assert.equal(pool.queries.length, 0);
});

test('sweep: one email per supplier covers all of their POs, and is logged as a new Email Receipts row', async () => {
    const pool = fakePool({
        lines: [
            line(1, 409, 'PO_00409J', 'Suzhou Sunmed Co., Ltd', 'JF0001'),
            line(2, 419, 'PO_00419J', 'Suzhou Sunmed Co.,Ltd.', 'JF0002'),
            line(3, 430, 'PO_00430J', 'Immanuel', 'JF0003'),
        ],
        sends: { 409: ['amy@sunmed.example'], 419: ['Amy@sunmed.example', 'bo@sunmed.example'], 430: ['c@immanuel.example'] },
    });
    const { sent, run } = runner(pool);
    const summary = await run();
    assert.equal(summary.sent, 2);
    assert.equal(summary.suppliers, 2);
    assert.deepEqual(summary.lines, { estimated: 3, actual: 0 });
    assert.equal(sent.length, 2);
    assert.deepEqual(sent[0].to, ['amy@sunmed.example', 'bo@sunmed.example']);
    assert.equal(sent[0].subject, 'Ready dates needed – PO_00409J, PO_00419J / 请填写完成日期');
    assert.match(sent[0].html, /JF0001/);
    assert.match(sent[0].html, /JF0002/);
    assert.doesNotMatch(sent[0].html, /JF0003/);
    // Chases are closed before anyone is picked; the day is bound, not the server's.
    assert.match(pool.queries[0].sql, /^UPDATE ready_date_reminders c SET c\.closed_at/);
    assert.deepEqual(wrote(pool, /^SELECT o\.id, o\.purchase_order_id/)[0].params, [WEDNESDAY, WEDNESDAY]);
    // Each supplier: claimed, recorded, and one receipt with no action_url.
    assert.equal(wrote(pool, /^INSERT INTO ready_date_reminders/).length, 2);
    const receipts = wrote(pool, /^INSERT INTO email_receipts/);
    assert.equal(receipts.length, 2);
    assert.deepEqual(receipts[0].params.slice(1, 4), ['ready_date_reminder', 'ready_date_reminders', 100]);
    assert.deepEqual(receipts[0].params.slice(6), ['portal', null]);
    const recorded = wrote(pool, /^UPDATE ready_date_reminders SET purchase_order_ids/)[0];
    assert.deepEqual(recorded.params.slice(0, 2), ['[409,419]', '[1,2]']);
});

test('sweep: the deposit holds only the estimated date; a date already sent on the portal is left out', async () => {
    const pool = fakePool({
        lines: [
            line(1, 7, 'PO_7', 'Acme', 'JF1'), line(2, 7, 'PO_7', 'Acme', 'JF2'),
            line(3, 8, 'PO_8', 'Acme', 'JF3'), line(4, 8, 'PO_8', 'Acme', 'JF4', '2026-11-10'),
            line(5, 9, 'PO_9', 'Held Ltd', 'JF5'),
            line(6, 10, 'PO_10', 'Late Ltd', 'JF6', '2026-11-01'), line(7, 10, 'PO_10', 'Late Ltd', 'JF7', '2026-11-02'),
        ],
        pending: [
            { order_id: 2, requested_json: { estimatedReadyDate: '2026-12-20' } }, // answered
            { order_id: 1, requested_json: { actualReadyDate: '2026-12-20' } },    // not what line 1 is asked for
            { order_id: 7, requested_json: { actualReadyDate: '2026-11-17' } },    // answered
        ],
        sends: { 7: ['a@acme.example'], 8: ['a@acme.example'], 9: ['h@held.example'], 10: ['l@late.example'] },
    });
    const gated = [];
    const { sent, run } = runner(pool, { loadDepositGate: async (conn, po) => { gated.push(po.id); return { clear: po.id === 7 }; } });
    const summary = await run();
    assert.deepEqual(summary.lines, { estimated: 3, actual: 2 });
    assert.deepEqual(summary.skipped, { answeredOnPortal: 2, depositHeld: 2, noRecipients: 0, alreadySent: 0 });
    // The gate is read only for a PO with a line that needs an estimated date.
    assert.deepEqual(gated, [7, 8, 9]);
    assert.equal(sent.length, 2);
    // Acme: PO_7's line 1 (gate clear), and PO_8's passed estimate (its undated line is held).
    assert.match(sent[0].html, /JF1/);
    assert.match(sent[0].html, /JF4/);
    assert.doesNotMatch(sent[0].html, /JF2|JF3|JF5/);
    assert.equal(sent[0].subject, 'Ready dates needed – PO_7, PO_8 / 请填写完成日期');
    // Late Ltd: only the actual date still owed, no estimated section.
    assert.match(sent[1].html, /JF6/);
    assert.doesNotMatch(sent[1].html, /JF7|Estimated ready date needed/);
});

test('sweep: a later day is a reminder under the same receipt; a supplier already emailed today is skipped', async () => {
    const lines = [line(1, 7, 'PO_7', 'Acme', 'JF1')];
    const chase = { id: 55, order_ids: '[1,9]', send_count: 2, last_sent_at: new Date('2026-11-17T01:00:00Z'), recent: 0 };
    const pool = fakePool({ lines, sends: { 7: ['a@acme.example'] }, chases: { acme: chase }, receipts: { 55: { id: 900, reminder_count: 1 } } });
    const { sent, run } = runner(pool);
    const summary = await run();
    assert.equal(summary.sent, 1);
    assert.match(sent[0].html, /Reminder #2 · 第 2 次提醒/);
    assert.equal(wrote(pool, /^INSERT INTO ready_date_reminders/).length, 0);
    assert.equal(wrote(pool, /^INSERT INTO email_receipts/).length, 0);
    const reminder = wrote(pool, /^INSERT INTO email_receipt_reminders/)[0];
    assert.deepEqual(reminder.params.slice(0, 2), [900, 2]);
    assert.equal(reminder.params[6], null); // automatic
    // Lines chased earlier stay on the chase.
    assert.equal(wrote(pool, /^UPDATE ready_date_reminders SET purchase_order_ids/)[0].params[1], '[1,9]');

    const refused = fakePool({ lines, sends: { 7: ['a@acme.example'] }, chases: { acme: chase }, claimRefused: true });
    const again = runner(refused);
    const second = await again.run();
    assert.equal(again.sent.length, 0);
    assert.equal(second.skipped.alreadySent, 1);
});

test('sweep: a failed send gives the supplier back; nobody to email is counted', async () => {
    const pool = fakePool({ lines: [line(1, 7, 'PO_7', 'Acme', 'JF1'), line(2, 8, 'PO_8', 'Nobody Ltd', 'JF2')], sends: { 7: ['a@acme.example'] } });
    const { run } = runner(pool, { sendEmail: async () => ({ ok: false, reason: 'ses_error', status: 500 }) });
    const summary = await run();
    assert.equal(summary.failed, 1);
    assert.equal(summary.sent, 0);
    assert.equal(summary.skipped.noRecipients, 1);
    assert.equal(wrote(pool, /^DELETE FROM ready_date_reminders WHERE id = \? AND send_count = 0/).length, 1);
    assert.equal(wrote(pool, /^INSERT INTO email_receipts/).length, 0);
});

test('sweep: a dry run reports the emails and writes nothing', async () => {
    const pool = fakePool({ lines: [line(1, 7, 'PO_7', 'Acme', 'JF1'), line(2, 7, 'PO_7', 'Acme', 'JF2', '2026-11-10')], sends: { 7: ['a@acme.example'] } });
    const { sent, run } = runner(pool, { dryRun: true });
    const summary = await run();
    assert.equal(sent.length, 0);
    assert.equal(summary.sent, 0);
    assert.deepEqual(summary.emails, [{
        supplier: 'Acme', to: ['a@acme.example'], subject: 'Ready dates needed – PO_7 / 请填写完成日期', emailNumber: 1,
        purchaseOrders: [{ id: 7, poNumber: 'PO_7', estimated: ['JF1'], actual: ['JF2'] }],
    }]);
    assert.equal(wrote(pool, /^(INSERT|UPDATE|DELETE)/).length, 0);
});
