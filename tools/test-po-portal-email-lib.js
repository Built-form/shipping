'use strict';

// Unit tests for src/lib/po-portal-email.js — the PO send's portal-link email
// (docs/po-portal-send.md): portal links, parsing pasted recipients, the
// outcome per portal user row, the appended portal block (the PO's, and the
// signed PI's, which is sent the same way) — plus preapprovePortalUsers /
// loadPoDocumentViews / loadSignedPiViews from src/services/portal-access.js
// against a fake connection. No database.
//   node --test tools/test-po-portal-email-lib.js

const test = require('node:test');
const assert = require('node:assert/strict');
const P = require('../src/lib/po-portal-email');
const { preapprovePortalUsers, loadPoDocumentViews, loadSignedPiViews } = require('../src/services/portal-access');

test('portal base and links', () => {
    assert.equal(P.portalBaseOf({ SUPPLIER_PORTAL_URL: ' https://portal.example.com// ' }), 'https://portal.example.com');
    assert.equal(P.portalBaseOf({}), '');
    assert.equal(P.poPortalUrl('https://p.example', 42), 'https://p.example/orders/42?tab=pdfs');
    assert.equal(P.registerUrl('https://p.example'), 'https://p.example/register');
});

test('provisionList: every recipient, staff included, normalised and de-duplicated', () => {
    assert.deepEqual(
        P.provisionList([' Sales@Supplier.cn ', 'sales@supplier.cn', 'Cindy@JFAmedical.co.uk', 'lukepbasil@gmail.com', '', null]),
        ['sales@supplier.cn', 'cindy@jfamedical.co.uk', 'lukepbasil@gmail.com']
    );
});

test('outcomeFor', () => {
    assert.equal(P.outcomeFor(null), 'invited');
    assert.equal(P.outcomeFor({ status: 'pending' }), 'approved');
    assert.equal(P.outcomeFor({ status: 'active' }), 'already_active');
    assert.equal(P.outcomeFor({ status: 'rejected' }), 'blocked');
    assert.equal(P.outcomeFor({ status: 'disabled' }), 'blocked');
    assert.equal(P.outcomeFor({ status: 'weird' }), 'blocked');
});

test('portalBlockHtml: link escaped, wording follows pre-approval', () => {
    const base = { poNumber: 'PO<1>', version: 3, poUrl: 'https://p.example/orders/1?tab=pdfs&x="y"', registerUrl: 'https://p.example/register' };
    const yes = P.portalBlockHtml({ ...base, preapproved: true });
    assert.match(yes, /PO&lt;1&gt; v3/);
    assert.match(yes, /href="https:\/\/p\.example\/orders\/1\?tab=pdfs&amp;x=&quot;y&quot;"/);
    assert.match(yes, /already approved/);
    assert.match(yes, /无需等待审核/);
    const no = P.portalBlockHtml({ ...base, preapproved: false });
    assert.doesNotMatch(no, /already approved/);
    assert.match(no, /our team will approve/);
});

test('signedPiPortalBlockHtml: names the PO and the file, links to the order, says nothing is attached', () => {
    const base = { poNumber: 'PO<1>', filename: 'PI_signed <x>.pdf', poUrl: 'https://p.example/orders/1?tab=pdfs', registerUrl: 'https://p.example/register' };
    const yes = P.signedPiPortalBlockHtml({ ...base, preapproved: true });
    assert.match(yes, /signed proforma invoice<\/strong> for purchase order <strong>PO&lt;1&gt;<\/strong> \(PI_signed &lt;x&gt;\.pdf\)/);
    assert.match(yes, /已签署形式发票/);
    assert.match(yes, /href="https:\/\/p\.example\/orders\/1\?tab=pdfs"/);
    assert.match(yes, /No file is attached/);
    assert.match(yes, /already approved/);
    // The PO's own wording stays the PO's.
    assert.doesNotMatch(yes, /download the PDF from the order/);
    const no = P.signedPiPortalBlockHtml({ ...base, filename: null, preapproved: false });
    assert.match(no, /for purchase order <strong>PO&lt;1&gt;<\/strong> is on the supplier portal/);
    assert.match(no, /our team will approve/);
    assert.doesNotMatch(no, /already approved/);
});

// A fake connection: just enough of supplier_users / user_supplier_access /
// suppliers for preapprovePortalUsers.
function fakeConn({ suppliers = [], users = [], missing = false, deadlocks = 0 } = {}) {
    const state = { users: users.map(u => ({ ...u })), access: [], committed: false, rolledBack: false, nextId: 100, isolation: [], deadlocks };
    const conn = {
        state,
        async beginTransaction() {},
        async commit() { state.committed = true; },
        async rollback() { state.rolledBack = true; },
        async query(sql, params = []) {
            if (/^SET TRANSACTION ISOLATION LEVEL/.test(sql)) { state.isolation.push(sql); return [{}]; }
            if (/FROM suppliers s/.test(sql)) return [suppliers.filter(s => params.includes(s.name))];
            if (missing && /jportal/.test(sql)) { const e = new Error('Unknown database'); e.errno = 1049; throw e; }
            if (/SELECT id, status FROM .*supplier_users/.test(sql)) {
                return [state.users.filter(u => u.email === params[0])];
            }
            if (/INSERT INTO .*supplier_users/.test(sql) && state.deadlocks > 0) {
                state.deadlocks--;
                const e = new Error('Deadlock found when trying to get lock'); e.errno = 1213; e.code = 'ER_LOCK_DEADLOCK';
                throw e;
            }
            if (/INSERT INTO .*supplier_users/.test(sql)) {
                const id = state.nextId++;
                state.users.push({ id, email: params[0], status: 'active', approved_by: params[1], decided_note: params[2] });
                return [{ insertId: id, affectedRows: 1 }];
            }
            if (/UPDATE .*supplier_users/.test(sql)) {
                const u = state.users.find(x => x.id === params[2] && x.status === 'pending');
                if (u) Object.assign(u, { status: 'active', approved_by: params[0], decided_note: params[1] });
                return [{ affectedRows: u ? 1 : 0 }];
            }
            if (/INSERT IGNORE INTO .*user_supplier_access/.test(sql)) {
                let added = 0;
                for (let i = 0; i < params.length; i += 4) {
                    const [userId, supplierId, name] = params.slice(i, i + 3);
                    if (!state.access.some(a => a.userId === userId && a.supplierId === supplierId)) {
                        state.access.push({ userId, supplierId, name });
                        added++;
                    }
                }
                return [{ affectedRows: added }];
            }
            if (/po_document_views/.test(sql)) {
                assert.match(sql, /SELECT purchase_order_document_id AS viewed_id/);
                return [[
                    { viewed_id: 5, user_email: 'a@s.cn', view_count: 2, first_viewed_at: '2026-11-03T08:00:00Z', last_viewed_at: '2026-11-03T09:00:00Z' },
                ]];
            }
            if (/signed_pi_views/.test(sql)) {
                assert.match(sql, /SELECT purchase_order_signed_pi_id AS viewed_id/);
                return [[
                    { viewed_id: 12, user_email: 'a@s.cn', view_count: 1, first_viewed_at: '2026-11-12T08:00:00Z', last_viewed_at: '2026-11-12T08:00:00Z' },
                    { viewed_id: 12, user_email: 'b@s.cn', view_count: 3, first_viewed_at: '2026-11-12T09:00:00Z', last_viewed_at: '2026-11-13T09:00:00Z' },
                ]];
            }
            throw new Error(`unexpected SQL: ${sql}`);
        },
    };
    return conn;
}

test('preapprovePortalUsers: invites, approves pending, keeps active, blocks rejected, includes staff', async () => {
    const conn = fakeConn({
        suppliers: [{ id: 7, name: 'Acme Co' }],
        users: [
            { id: 1, email: 'pending@acme.cn', status: 'pending' },
            { id: 2, email: 'active@acme.cn', status: 'active' },
            { id: 3, email: 'rejected@acme.cn', status: 'rejected' },
        ],
    });
    const r = await preapprovePortalUsers(conn, {
        recipients: ['New@Acme.cn', 'pending@acme.cn', 'active@acme.cn', 'rejected@acme.cn', 'colleague@built-form.co.uk'],
        poSupplier: 'Acme Co', actorEmail: 'ops@built-form.co.uk', note: 'n',
    });
    assert.equal(r.status, 'ok');
    assert.deepEqual(r.users.map(u => [u.email, u.outcome, u.suppliersAdded]), [
        ['new@acme.cn', 'invited', 1],
        ['pending@acme.cn', 'approved', 1],
        ['active@acme.cn', 'already_active', 1],
        ['rejected@acme.cn', 'blocked', 0],
        ['colleague@built-form.co.uk', 'invited', 1],
    ]);
    assert.equal(conn.state.users.find(u => u.email === 'pending@acme.cn').status, 'active');
    assert.equal(conn.state.users.find(u => u.email === 'rejected@acme.cn').status, 'rejected');
    assert.ok(conn.state.committed);
});

test('parseRecipients: pasted forms become clean addresses, anything else is invalid', () => {
    assert.deepEqual(P.parseRecipients([
        ' Zoe <Zoe@Roosin.com> ',
        'a@x.com; b@x.com, a@X.com',
        '<c@x.com>',
        'mailto:d@x.com',
        'e@x.com​',
        '‎f@x.com',
        'g＠x．com',
        '',
        null,
    ]), {
        addresses: ['Zoe@Roosin.com', 'a@x.com', 'b@x.com', 'c@x.com', 'd@x.com', 'e@x.com', 'f@x.com', 'g@x.com'],
        invalid: [],
    });
    assert.deepEqual(P.parseRecipients('one@x.com'), { addresses: ['one@x.com'], invalid: [] });
    assert.deepEqual(P.parseRecipients(['zoe@roosin,com', 'two words@x.com', 'a@@x.com', 'nodot@x', 'ok@x.com']), {
        addresses: ['ok@x.com'],
        invalid: ['zoe@roosin', 'com', 'two words@x.com', 'a@@x.com', 'nodot@x'],
    });
});

test('preapprovePortalUsers: links the PO supplier AND its lines\' suppliers', async () => {
    const conn = fakeConn({ suppliers: [{ id: 2, name: 'Sunmed Co.,Ltd.' }, { id: 87, name: 'Sunmed Co., Ltd' }, { id: 9, name: 'Other' }] });
    const r = await preapprovePortalUsers(conn, {
        recipients: ['a@sunmed.cn'], poSupplier: 'Sunmed Co.,Ltd.', lineSuppliers: ['Sunmed Co., Ltd', 'Sunmed Co.,Ltd.', null],
    });
    assert.equal(r.status, 'ok');
    assert.equal(r.poSupplierMatched, true);
    assert.deepEqual(r.suppliers.map(s => s.id), [2, 87]);
    assert.deepEqual(r.users, [{ email: 'a@sunmed.cn', outcome: 'invited', suppliersAdded: 2 }]);

    // Only a line supplier exists: linked to it, but the PO itself will not show.
    const lineOnly = await preapprovePortalUsers(fakeConn({ suppliers: [{ id: 87, name: 'Sunmed Co., Ltd' }] }), {
        recipients: ['a@sunmed.cn'], poSupplier: 'Sunmed (typo)', lineSuppliers: ['Sunmed Co., Ltd'],
    });
    assert.equal(lineOnly.status, 'ok');
    assert.equal(lineOnly.poSupplierMatched, false);
    assert.deepEqual(lineOnly.suppliers.map(s => s.id), [87]);
});

test('preapprovePortalUsers: READ COMMITTED right before BEGIN, a deadlock is retried', async () => {
    const conn = fakeConn({ suppliers: [{ id: 7, name: 'Acme Co' }], deadlocks: 1 });
    const r = await preapprovePortalUsers(conn, { recipients: ['a@acme.cn'], poSupplier: 'Acme Co' });
    assert.equal(r.status, 'ok');
    assert.deepEqual(r.users, [{ email: 'a@acme.cn', outcome: 'invited', suppliersAdded: 1 }]);
    assert.equal(conn.state.isolation.length, 2);   // once per attempt
    assert.match(conn.state.isolation[0], /READ COMMITTED/);
    assert.equal(conn.state.users.filter(u => u.email === 'a@acme.cn').length, 1);

    // Deadlocking every time: gives up after a few attempts and throws (the route then reports 'error').
    const stuck = fakeConn({ suppliers: [{ id: 7, name: 'Acme Co' }], deadlocks: 99 });
    await assert.rejects(preapprovePortalUsers(stuck, { recipients: ['a@acme.cn'], poSupplier: 'Acme Co' }), { errno: 1213 });
    assert.equal(stuck.state.isolation.length, 3);
});

test('preapprovePortalUsers: no exact supplier name, staff only, no recipients, missing portal', async () => {
    const noMatch = await preapprovePortalUsers(fakeConn({ suppliers: [{ id: 7, name: 'Acme Co' }] }), {
        recipients: ['a@acme.cn'], poSupplier: 'ACME CO',
    });
    assert.equal(noMatch.status, 'no_supplier');

    // What happened to PO_00430J on 2026-11-04: colleagues only. Now they are set up too.
    const staffOnly = await preapprovePortalUsers(fakeConn({ suppliers: [{ id: 7, name: 'Acme Co' }] }), {
        recipients: ['cindy@jfamedical.co.uk', 'lukepbasil@gmail.com'], poSupplier: 'Acme Co',
    });
    assert.equal(staffOnly.status, 'ok');
    assert.deepEqual(staffOnly.users.map(u => u.outcome), ['invited', 'invited']);

    assert.equal((await preapprovePortalUsers(fakeConn(), { recipients: ['  '], poSupplier: 'Acme Co' })).status, 'none');

    const conn = fakeConn({ suppliers: [{ id: 7, name: 'Acme Co' }], missing: true });
    const missing = await preapprovePortalUsers(conn, { recipients: ['a@acme.cn'], poSupplier: 'Acme Co' });
    assert.equal(missing.status, 'unavailable');
    assert.ok(conn.state.rolledBack);
});

test('loadPoDocumentViews: grouped by document, empty when the table is missing', async () => {
    const views = await loadPoDocumentViews(fakeConn(), [5, 5, 6]);
    assert.deepEqual(views.get(5), [{ email: 'a@s.cn', firstViewedAt: '2026-11-03T08:00:00Z', lastViewedAt: '2026-11-03T09:00:00Z', count: 2 }]);
    assert.equal(views.has(6), false);
    assert.equal((await loadPoDocumentViews(fakeConn({ missing: true }), [5])).size, 0);
    assert.equal((await loadPoDocumentViews(fakeConn(), [])).size, 0);
});

test('loadSignedPiViews: grouped by signed PI, first viewer first, empty when the table is missing', async () => {
    const views = await loadSignedPiViews(fakeConn(), [12, 12, 13]);
    assert.deepEqual(views.get(12), [
        { email: 'a@s.cn', firstViewedAt: '2026-11-12T08:00:00Z', lastViewedAt: '2026-11-12T08:00:00Z', count: 1 },
        { email: 'b@s.cn', firstViewedAt: '2026-11-12T09:00:00Z', lastViewedAt: '2026-11-13T09:00:00Z', count: 3 },
    ]);
    assert.equal(views.has(13), false);
    assert.equal((await loadSignedPiViews(fakeConn({ missing: true }), [12])).size, 0);
    assert.equal((await loadSignedPiViews(fakeConn(), [])).size, 0);
});
