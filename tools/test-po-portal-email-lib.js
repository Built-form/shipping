'use strict';

// Unit tests for src/lib/po-portal-email.js — the PO send's portal-link email
// (docs/po-portal-send.md): portal links, which recipients are staff, the
// outcome per portal user row, the appended portal block — plus
// preapprovePortalUsers / loadPoDocumentViews from
// src/services/portal-access.js against a fake connection. No database.
//   node --test tools/test-po-portal-email-lib.js

const test = require('node:test');
const assert = require('node:assert/strict');
const P = require('../src/lib/po-portal-email');
const { preapprovePortalUsers, loadPoDocumentViews } = require('../src/services/portal-access');

test('portal base and links', () => {
    assert.equal(P.portalBaseOf({ SUPPLIER_PORTAL_URL: ' https://portal.example.com// ' }), 'https://portal.example.com');
    assert.equal(P.portalBaseOf({}), '');
    assert.equal(P.poPortalUrl('https://p.example', 42), 'https://p.example/orders/42?tab=pdfs');
    assert.equal(P.registerUrl('https://p.example'), 'https://p.example/register');
});

test('splitRecipients: staff addresses and their company domain are skipped, public domains are not', () => {
    const staff = ['Ops@Built-Form.co.uk', 'someone@gmail.com'];
    const { provision, skipped } = P.splitRecipients(
        [' Sales@Supplier.cn ', 'sales@supplier.cn', 'boss@built-form.co.uk', 'someone@gmail.com', 'factory@gmail.com', 'li@qq.com'],
        staff
    );
    assert.deepEqual(provision, ['sales@supplier.cn', 'factory@gmail.com', 'li@qq.com']);
    assert.deepEqual(skipped, ['boss@built-form.co.uk', 'someone@gmail.com']);
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

// A fake connection: just enough of supplier_users / user_supplier_access /
// suppliers / shipping_allowed_emails for preapprovePortalUsers.
function fakeConn({ staff = [], suppliers = [], users = [], missing = false } = {}) {
    const state = { users: users.map(u => ({ ...u })), access: [], committed: false, rolledBack: false, nextId: 100 };
    const conn = {
        state,
        async beginTransaction() {},
        async commit() { state.committed = true; },
        async rollback() { state.rolledBack = true; },
        async query(sql, params = []) {
            if (/FROM shipping_allowed_emails/.test(sql)) return [staff.map(email => ({ email }))];
            if (/FROM suppliers s/.test(sql)) return [suppliers.filter(s => s.name === params[0])];
            if (missing && /jportal/.test(sql)) { const e = new Error('Unknown database'); e.errno = 1049; throw e; }
            if (/SELECT id, status FROM .*supplier_users/.test(sql)) {
                return [state.users.filter(u => u.email === params[0])];
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
                return [[
                    { purchase_order_document_id: 5, user_email: 'a@s.cn', view_count: 2, first_viewed_at: '2026-11-03T08:00:00Z', last_viewed_at: '2026-11-03T09:00:00Z' },
                ]];
            }
            throw new Error(`unexpected SQL: ${sql}`);
        },
    };
    return conn;
}

test('preapprovePortalUsers: invites, approves pending, keeps active, blocks rejected, skips staff', async () => {
    const conn = fakeConn({
        staff: ['ops@built-form.co.uk'],
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
    assert.deepEqual(r.skipped, ['colleague@built-form.co.uk']);
    assert.deepEqual(r.users.map(u => [u.email, u.outcome, u.suppliersAdded]), [
        ['new@acme.cn', 'invited', 1],
        ['pending@acme.cn', 'approved', 1],
        ['active@acme.cn', 'already_active', 1],
        ['rejected@acme.cn', 'blocked', 0],
    ]);
    assert.equal(conn.state.users.find(u => u.email === 'pending@acme.cn').status, 'active');
    assert.equal(conn.state.users.find(u => u.email === 'rejected@acme.cn').status, 'rejected');
    assert.ok(conn.state.committed);
});

test('preapprovePortalUsers: no exact supplier name, staff only, missing portal', async () => {
    const noMatch = await preapprovePortalUsers(fakeConn({ suppliers: [{ id: 7, name: 'Acme Co' }] }), {
        recipients: ['a@acme.cn'], poSupplier: 'ACME CO',
    });
    assert.equal(noMatch.status, 'no_supplier');

    const staffOnly = await preapprovePortalUsers(fakeConn({ staff: ['ops@built-form.co.uk'], suppliers: [{ id: 7, name: 'Acme Co' }] }), {
        recipients: ['ops@built-form.co.uk'], poSupplier: 'Acme Co',
    });
    assert.equal(staffOnly.status, 'none');

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
