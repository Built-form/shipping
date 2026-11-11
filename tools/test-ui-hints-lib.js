'use strict';

// node --test tools/test-ui-hints-lib.js
//
// ShipLine's info bubbles (/api/v1/ui-hints):
//   - the pure rules in src/lib/ui-hints.js (key grammar, body, link, roles,
//     the upsert plan, the JSON shape);
//   - the contract fixture docs/frontend/ui-hints-contract.json, which ShipLine
//     keeps a copy of and pins its client rules against — it must be exactly
//     what tools/gen-ui-hints-contract.js builds from the rules today;
//   - the routes in src/services/ui-hint-routes.js, mounted on a bare Express
//     app over a scripted fake connection (nothing reaches a database).

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const http = require('node:http');
const express = require('express');

const L = require('../src/lib/ui-hints');

// ── Rules ──────────────────────────────────────────────────────────────────

test('keys: the grammar accepts page.tab/section/kind.name shapes', () => {
    for (const k of [
        'a/b',
        'payments.ready/col.supplier',
        'order/field.carton-cbm',
        'draft.plan/candidates/col.mo_ex_sea',
        'stages/stage.in_transit',
        'a.b.c.d.e/w/x/y/z',
        `a/${'x'.repeat(61)}`,
    ]) assert.deepEqual(L.parseKey(k), { key: k }, k);
});

test('keys: the grammar refuses everything else', () => {
    for (const k of [
        '', 'payments', 'Payments/col.x', 'payments/', '/col.x', 'payments//x',
        'a.b.c.d.e.f/x',          // six scope parts
        'a/b/c/d/e/f',            // five slash segments
        'a/-x', 'a/.x', '-a/x',   // a segment starts with a letter or digit
        'a b/c', 'a/c d', 'a/é',
        `a/${'x'.repeat(62)}`,    // segment over 61
    ]) assert.ok(L.parseKey(k).error, JSON.stringify(k));
    assert.ok(L.parseKey(5).error);
    assert.ok(L.parseKey(null).error);
    assert.ok(L.parseKey(undefined).error);
});

test('keys: a key that fits the grammar is still refused over 160 characters', () => {
    const long = `${'a'.repeat(60)}.${'b'.repeat(60)}/${'c'.repeat(50)}`;
    assert.ok(L.HINT_KEY_RE.test(long));
    assert.equal(long.length, 172);
    assert.match(L.parseKey(long).error, /160/);
    const fits = `${'a'.repeat(60)}.${'b'.repeat(60)}/${'c'.repeat(38)}`;
    assert.equal(fits.length, 160);
    assert.deepEqual(L.parseKey(fits), { key: fits });
});

test('keys: the regex is the one JFPRO uses, character for character', () => {
    assert.equal(
        L.HINT_KEY_RE.source,
        '^[a-z0-9][a-z0-9_-]{0,59}(\\.[a-z0-9][a-z0-9_-]{0,59}){0,4}(\\/[a-z0-9][a-z0-9_.-]{0,60}){1,4}$',
    );
});

test('body: line endings normalised, trimmed, 1 to 600 characters, text only', () => {
    assert.deepEqual(L.parseBody('  one\r\ntwo\rthree  '), { body: 'one\ntwo\nthree' });
    assert.deepEqual(L.parseBody('x'.repeat(600)), { body: 'x'.repeat(600) });
    assert.match(L.parseBody('x'.repeat(601)).error, /600/);
    assert.ok(L.parseBody('   ').error);
    assert.ok(L.parseBody('').error);
    assert.ok(L.parseBody(undefined).error);
    assert.ok(L.parseBody(42).error);
});

test('link: both or neither; the path must be a ShipLine page', () => {
    assert.deepEqual(L.parseLink({}), { linkPath: null, linkLabel: null });
    assert.deepEqual(L.parseLink({ linkPath: '', linkLabel: '  ' }), { linkPath: null, linkLabel: null });
    assert.deepEqual(L.parseLink({ linkPath: null, linkLabel: null }), { linkPath: null, linkLabel: null });
    assert.deepEqual(L.parseLink({ linkPath: '/payments?tab=ready', linkLabel: '  Ready to pay ' }), { linkPath: '/payments?tab=ready', linkLabel: 'Ready to pay' });
    assert.deepEqual(L.parseLink({ linkPath: '/info', linkLabel: 'All hints' }), { linkPath: '/info', linkLabel: 'All hints' });
    assert.match(L.parseLink({ linkPath: '/payments' }).error, /linkLabel/);
    assert.match(L.parseLink({ linkLabel: 'Payments' }).error, /linkPath/);
    for (const p of [
        'payments', '//evil.example', 'https://evil.example/payments', '/qc', '/supplier-changes', '/nope',
        '/payments/../users', '/%2e%2e/users', '/payments%zz', '/payments\\x', '/pay ments',
        `/payments?${'x'.repeat(200)}`,
    ]) assert.ok(L.parseLink({ linkPath: p, linkLabel: 'x' }).error, p);
    assert.match(L.parseLink({ linkPath: '/payments', linkLabel: 'x'.repeat(81) }).error, /80/);
    assert.deepEqual(L.parseLink({ linkPath: '/payments', linkLabel: 'x'.repeat(80) }), { linkPath: '/payments', linkLabel: 'x'.repeat(80) });
    assert.ok(L.parseLink({ linkPath: 7, linkLabel: 'x' }).error);
    assert.ok(L.parseLink({ linkPath: '/payments', linkLabel: 7 }).error);
});

test('link roots are exactly the ShipLine view paths, plus info', () => {
    assert.deepEqual([...L.LINK_ROOTS].sort(), [
        'audit-log', 'bridge', 'delivered', 'draft', 'email-receipts', 'info', 'manifest', 'payments',
        'purchase-orders', 'qc-visits', 'shipments', 'supplier-portal', 'users', 'warehouse',
    ]);
});

test('rowVersion: a non-negative integer, numeric strings allowed (query string)', () => {
    assert.deepEqual(L.parseRowVersion(0), { rowVersion: 0 });
    assert.deepEqual(L.parseRowVersion(3), { rowVersion: 3 });
    assert.deepEqual(L.parseRowVersion('2'), { rowVersion: 2 });
    for (const v of [undefined, null, '', -1, 1.5, 'x', 2 ** 31, true]) assert.ok(L.parseRowVersion(v).error, String(v));
});

test('roles: admin and standard write; nobody else', () => {
    assert.deepEqual([...L.HINT_ROLES], ['admin', 'standard']);
    assert.equal(L.canWriteHints('admin'), true);
    assert.equal(L.canWriteHints('standard'), true);
    for (const t of ['accountant', 'warehouse', 'exec', '', undefined, null, 'Admin']) assert.equal(L.canWriteHints(t), false, String(t));
});

test('delete: the author (any case) or an admin', () => {
    const row = { created_by: 'Maria@JFA.test' };
    assert.equal(L.canDeleteHint('admin', 'someone@jfa.test', row), true);
    assert.equal(L.canDeleteHint('standard', 'maria@jfa.test', row), true);
    assert.equal(L.canDeleteHint('standard', 'tom@jfa.test', row), false);
    assert.equal(L.canDeleteHint('accountant', 'maria@jfa.test', row), false);
    assert.equal(L.canDeleteHint('standard', undefined, row), false);
    assert.equal(L.canDeleteHint('standard', 'maria@jfa.test', null), false);
});

test('planUpsert: rowVersion 0 creates; an existing row must be named by id and version', () => {
    const row = { id: 7, row_version: 3 };
    assert.equal(L.planUpsert(null, 0, null), 'insert');
    assert.equal(L.planUpsert(row, 0, null), 'occupied');
    assert.equal(L.planUpsert(null, 3, 7), 'missing');
    assert.equal(L.planUpsert({ id: 8, row_version: 1 }, 3, 7), 'missing'); // deleted and recreated
    assert.equal(L.planUpsert(row, 2, 7), 'stale');
    assert.equal(L.planUpsert(row, 3, 7), 'update');
});

test('parsePutBody: create needs no id; an update needs a positive id', () => {
    assert.deepEqual(L.parsePutBody({ key: 'a/b', rowVersion: 0, body: ' hi ' }), {
        value: { key: 'a/b', id: null, rowVersion: 0, body: 'hi', linkPath: null, linkLabel: null },
    });
    assert.deepEqual(L.parsePutBody({ key: 'a/b', id: 4, rowVersion: 2, body: 'hi', linkPath: '/payments', linkLabel: 'Pay' }), {
        value: { key: 'a/b', id: 4, rowVersion: 2, body: 'hi', linkPath: '/payments', linkLabel: 'Pay' },
    });
    assert.match(L.parsePutBody({ key: 'a/b', rowVersion: 2, body: 'hi' }).error, /id/);
    assert.match(L.parsePutBody({ key: 'a/b', id: 0, rowVersion: 2, body: 'hi' }).error, /id/);
    assert.match(L.parsePutBody({ key: 'A/b', rowVersion: 0, body: 'hi' }).error, /key/);
    assert.ok(L.parsePutBody({ key: 'a/b', rowVersion: 0, body: '' }).error);
    assert.ok(L.parsePutBody(undefined).error);
});

test('parseMoveBody: the current version (1 or more) and a valid destination', () => {
    assert.deepEqual(L.parseMoveBody({ rowVersion: 2, toKey: 'a/c' }), { value: { rowVersion: 2, toKey: 'a/c' } });
    assert.ok(L.parseMoveBody({ rowVersion: 0, toKey: 'a/c' }).error);
    assert.ok(L.parseMoveBody({ rowVersion: 2, toKey: 'nope' }).error);
    assert.ok(L.parseMoveBody({ toKey: 'a/c' }).error);
});

test('rowToJson and hintSnapshot', () => {
    const r = {
        id: '7', hint_key: 'a/b', body: 'text', link_path: null, link_label: undefined,
        created_by: 'maria@jfa.test', updated_by: 'tom@jfa.test', updated_by_name: 'Tom',
        created_at: new Date('2026-11-10T09:00:00Z'), updated_at: new Date('2026-11-10T10:30:00Z'), row_version: '2',
    };
    assert.deepEqual(L.rowToJson(r), {
        id: 7, key: 'a/b', body: 'text', linkPath: null, linkLabel: null,
        createdBy: 'maria@jfa.test', updatedBy: 'tom@jfa.test', updatedByName: 'Tom',
        createdAt: '2026-11-10T09:00:00.000Z', updatedAt: '2026-11-10T10:30:00.000Z', rowVersion: 2,
    });
    assert.equal(L.rowToJson({ ...r, updated_by_name: null }).updatedByName, null);
    assert.deepEqual(L.hintSnapshot(r), { key: 'a/b', body: 'text', linkPath: null, linkLabel: null });
});

// ── Contract fixture ───────────────────────────────────────────────────────

test('the contract fixture on disk is what the rules build today', () => {
    const { buildContract, CONTRACT_FILE } = require('./gen-ui-hints-contract');
    const onDisk = JSON.parse(fs.readFileSync(CONTRACT_FILE, 'utf8'));
    assert.deepEqual(onDisk, buildContract(), 'run: node tools/gen-ui-hints-contract.js --write — then copy the file to ShipLine/src/hints/');
});

test('the contract fixture carries cases that exercise both outcomes', () => {
    const { buildContract } = require('./gen-ui-hints-contract');
    const c = buildContract();
    for (const group of ['keys', 'bodies', 'links', 'deletes']) {
        assert.ok(c.cases[group].some(x => x.ok), `${group}: an accepted case`);
        assert.ok(c.cases[group].some(x => !x.ok), `${group}: a refused case`);
    }
});

// ── Routes, against a fake connection ──────────────────────────────────────

const { registerUiHintRoutes } = require('../src/services/ui-hint-routes');

function harness({ userType = 'standard', userEmail = 'maria@jfa.test', rows = [] } = {}) {
    const db = {
        rows: rows.map(r => ({ ...r })),
        users: [{ email: 'maria@jfa.test', display_name: 'Maria' }, { email: 'tom@jfa.test', display_name: 'Tom' }],
        audits: [],
        tx: [],
        nextId: 100,
    };
    const hooks = { raceNextUpdate: false };
    const dup = () => Object.assign(new Error('Duplicate entry'), { code: 'ER_DUP_ENTRY' });
    const withName = r => ({ ...r, updated_by_name: (db.users.find(u => u.email === r.updated_by) || {}).display_name || null });
    const conn = {
        async beginTransaction() { db.tx.push('begin'); },
        async commit() { db.tx.push('commit'); },
        async rollback() { db.tx.push('rollback'); },
        async query(sql, params = []) {
            const s = sql.replace(/\s+/g, ' ').trim();
            const joined = 'SELECT h.*, u.display_name AS updated_by_name FROM shipline_ui_hints h LEFT JOIN shipping_allowed_emails u ON u.email = h.updated_by';
            if (s === `${joined} ORDER BY h.hint_key`) {
                return [[...db.rows].sort((a, b) => (a.hint_key < b.hint_key ? -1 : 1)).map(withName)];
            }
            if (s === `${joined} WHERE h.id = ?`) return [db.rows.filter(r => r.id === params[0]).map(withName)];
            if (s === `${joined} WHERE h.hint_key = ?`) return [db.rows.filter(r => r.hint_key === params[0]).map(withName)];
            if (s === 'SELECT * FROM shipline_ui_hints WHERE hint_key = ? FOR UPDATE') return [db.rows.filter(r => r.hint_key === params[0]).map(r => ({ ...r }))];
            if (s === 'SELECT * FROM shipline_ui_hints WHERE id = ? FOR UPDATE') return [db.rows.filter(r => r.id === params[0]).map(r => ({ ...r }))];
            if (s === 'INSERT INTO shipline_ui_hints (hint_key, body, link_path, link_label, created_by, updated_by) VALUES (?, ?, ?, ?, ?, ?)') {
                const [hint_key, body, link_path, link_label, created_by, updated_by] = params;
                if (hooks.dupNextInsert) {
                    hooks.dupNextInsert = false;
                    db.rows.push({ id: 900, hint_key, body: 'theirs', link_path: null, link_label: null, created_by: 'tom@jfa.test', updated_by: 'tom@jfa.test', created_at: new Date(), updated_at: new Date(), row_version: 1 });
                    throw dup();
                }
                if (db.rows.some(r => r.hint_key === hint_key)) throw dup();
                const id = db.nextId++;
                db.rows.push({ id, hint_key, body, link_path, link_label, created_by, updated_by, created_at: new Date('2026-11-10T09:00:00Z'), updated_at: new Date('2026-11-10T09:00:00Z'), row_version: 1 });
                return [{ insertId: id, affectedRows: 1 }];
            }
            if (s === 'UPDATE shipline_ui_hints SET body = ?, link_path = ?, link_label = ?, updated_by = ?, row_version = row_version + 1 WHERE id = ? AND hint_key = ? AND row_version = ?') {
                const [body, link_path, link_label, updated_by, id, key, rv] = params;
                if (hooks.raceNextUpdate) { hooks.raceNextUpdate = false; return [{ affectedRows: 0 }]; }
                const r = db.rows.find(x => x.id === id && x.hint_key === key && x.row_version === rv);
                if (!r) return [{ affectedRows: 0 }];
                Object.assign(r, { body, link_path, link_label, updated_by, row_version: r.row_version + 1, updated_at: new Date('2026-11-10T11:00:00Z') });
                return [{ affectedRows: 1 }];
            }
            if (s === 'UPDATE shipline_ui_hints SET hint_key = ?, updated_by = ?, row_version = row_version + 1 WHERE id = ? AND row_version = ?') {
                const [toKey, updated_by, id, rv] = params;
                if (db.rows.some(x => x.hint_key === toKey && x.id !== id)) throw dup();
                const r = db.rows.find(x => x.id === id && x.row_version === rv);
                if (!r) return [{ affectedRows: 0 }];
                Object.assign(r, { hint_key: toKey, updated_by, row_version: r.row_version + 1 });
                return [{ affectedRows: 1 }];
            }
            if (s === 'DELETE FROM shipline_ui_hints WHERE id = ? AND row_version = ?') {
                const [id, rv] = params;
                const before = db.rows.length;
                db.rows = db.rows.filter(x => !(x.id === id && x.row_version === rv));
                return [{ affectedRows: before - db.rows.length }];
            }
            throw new Error(`unexpected statement: ${s.slice(0, 120)}`);
        },
    };

    const app = express();
    app.use(express.json());
    app.use((req, _res, next) => { req.userType = userType; req.userEmail = userEmail; next(); });
    const logs = [];
    registerUiHintRoutes(app, {
        withConnection: fn => fn(conn),
        recordAudit: async (_c, a) => { db.audits.push(a); },
        auditLogSchemaReady: Promise.resolve(),
        log: { error: (...a) => logs.push(['error', ...a]), warn: (...a) => logs.push(['warn', ...a]), info: () => {} },
    });

    const call = (method, urlPath, body) => new Promise((resolve, reject) => {
        const server = app.listen(0, () => {
            const { port } = server.address();
            const req = http.request({ port, method, path: urlPath, headers: { 'content-type': 'application/json' } }, res => {
                let text = '';
                res.on('data', d => { text += d; });
                res.on('end', () => { server.close(); resolve({ status: res.statusCode, body: text ? JSON.parse(text) : null }); });
            });
            req.on('error', e => { server.close(); reject(e); });
            if (body !== undefined) req.write(JSON.stringify(body));
            req.end();
        });
    });

    return { db, hooks, call, logs };
}

const ROW = (over = {}) => ({
    id: 7, hint_key: 'payments.ready/col.supplier', body: 'The supplier on the PO.', link_path: null, link_label: null,
    created_by: 'maria@jfa.test', updated_by: 'maria@jfa.test',
    created_at: new Date('2026-11-01T09:00:00Z'), updated_at: new Date('2026-11-01T09:00:00Z'), row_version: 2, ...over,
});

test('GET: every hint, by key, with the editor\'s name — for any role', async () => {
    const h = harness({ userType: 'accountant', rows: [ROW({ id: 8, hint_key: 'z/x', updated_by: 'tom@jfa.test' }), ROW()] });
    const r = await h.call('GET', '/api/v1/ui-hints');
    assert.equal(r.status, 200);
    assert.deepEqual(r.body.data.map(x => x.key), ['payments.ready/col.supplier', 'z/x']);
    assert.equal(r.body.data[1].updatedByName, 'Tom');
    assert.equal(r.body.data[0].rowVersion, 2);
});

test('PUT: roles other than admin and standard are refused before anything is read', async () => {
    for (const userType of ['accountant', 'warehouse', 'exec']) {
        const h = harness({ userType });
        const r = await h.call('PUT', '/api/v1/ui-hints', { key: 'a/b', rowVersion: 0, body: 'x' });
        assert.equal(r.status, 403, userType);
        assert.equal(r.body.code, 'CANNOT_WRITE_HINTS');
        assert.deepEqual(h.db.tx, []);
        assert.deepEqual(h.db.rows, []);
    }
});

test('PUT: a bad body is a 400 BAD_FIELD', async () => {
    const h = harness();
    const r = await h.call('PUT', '/api/v1/ui-hints', { key: 'Bad Key', rowVersion: 0, body: 'x' });
    assert.equal(r.status, 400);
    assert.equal(r.body.code, 'BAD_FIELD');
    assert.deepEqual(h.db.tx, []);
});

test('PUT create: 201 with the row at version 1, audited, committed', async () => {
    const h = harness();
    const r = await h.call('PUT', '/api/v1/ui-hints', { key: 'a/b', rowVersion: 0, body: '  Explained.  ', linkPath: '/payments', linkLabel: 'Payments' });
    assert.equal(r.status, 201);
    assert.equal(r.body.hint.key, 'a/b');
    assert.equal(r.body.hint.body, 'Explained.');
    assert.equal(r.body.hint.rowVersion, 1);
    assert.equal(r.body.hint.createdBy, 'maria@jfa.test');
    assert.equal(r.body.hint.updatedByName, 'Maria');
    assert.deepEqual(h.db.tx, ['begin', 'commit']);
    assert.equal(h.db.audits.length, 1);
    assert.deepEqual(h.db.audits[0], {
        entityType: 'ui_hint', entityId: 100, action: 'create', before: null,
        after: { key: 'a/b', body: 'Explained.', linkPath: '/payments', linkLabel: 'Payments' }, userEmail: 'maria@jfa.test',
    });
});

test('PUT create on a taken key: 409 OCCUPIED carrying the current hint', async () => {
    const h = harness({ rows: [ROW()] });
    const r = await h.call('PUT', '/api/v1/ui-hints', { key: 'payments.ready/col.supplier', rowVersion: 0, body: 'mine' });
    assert.equal(r.status, 409);
    assert.equal(r.body.code, 'OCCUPIED');
    assert.equal(r.body.current.id, 7);
    assert.equal(r.body.current.body, 'The supplier on the PO.');
    assert.deepEqual(h.db.tx, ['begin', 'rollback']);
    assert.equal(h.db.audits.length, 0);
});

test('PUT create racing another create: the duplicate insert becomes 409 OCCUPIED, rolled back', async () => {
    const h = harness();
    h.hooks.dupNextInsert = true;
    const r = await h.call('PUT', '/api/v1/ui-hints', { key: 'a/b', rowVersion: 0, body: 'mine' });
    assert.equal(r.status, 409);
    assert.equal(r.body.code, 'OCCUPIED');
    assert.equal(r.body.current.body, 'theirs');
    assert.deepEqual(h.db.tx, ['begin', 'rollback']);
    assert.equal(h.db.audits.length, 0);
});

test('PUT update: the version moves on, the audit holds what changed', async () => {
    const h = harness({ userEmail: 'tom@jfa.test', rows: [ROW()] });
    const r = await h.call('PUT', '/api/v1/ui-hints', { key: 'payments.ready/col.supplier', id: 7, rowVersion: 2, body: 'The supplier named on the PO.' });
    assert.equal(r.status, 200);
    assert.equal(r.body.hint.rowVersion, 3);
    assert.equal(r.body.hint.updatedBy, 'tom@jfa.test');
    assert.equal(r.body.hint.createdBy, 'maria@jfa.test');
    assert.deepEqual(h.db.tx, ['begin', 'commit']);
    assert.equal(h.db.audits[0].action, 'update');
    assert.equal(h.db.audits[0].before.body, 'The supplier on the PO.');
    assert.equal(h.db.audits[0].after.body, 'The supplier named on the PO.');
});

test('PUT update with nothing changed: 200 unchanged, no new version, no audit', async () => {
    const h = harness({ rows: [ROW()] });
    const r = await h.call('PUT', '/api/v1/ui-hints', { key: 'payments.ready/col.supplier', id: 7, rowVersion: 2, body: ' The supplier on the PO. ' });
    assert.equal(r.status, 200);
    assert.equal(r.body.unchanged, true);
    assert.equal(r.body.hint.rowVersion, 2);
    assert.equal(h.db.audits.length, 0);
});

test('PUT update against an old version: 409 STALE carrying the current hint, rolled back', async () => {
    const h = harness({ rows: [ROW({ row_version: 3, body: 'Theirs.' })] });
    const r = await h.call('PUT', '/api/v1/ui-hints', { key: 'payments.ready/col.supplier', id: 7, rowVersion: 2, body: 'Mine.' });
    assert.equal(r.status, 409);
    assert.equal(r.body.code, 'STALE');
    assert.equal(r.body.current.rowVersion, 3);
    assert.equal(r.body.current.body, 'Theirs.');
    assert.deepEqual(h.db.tx, ['begin', 'rollback']);
    assert.equal(h.db.rows[0].body, 'Theirs.');
});

test('PUT update when the guarded statement changes no row: 409 STALE', async () => {
    const h = harness({ rows: [ROW()] });
    h.hooks.raceNextUpdate = true;
    const r = await h.call('PUT', '/api/v1/ui-hints', { key: 'payments.ready/col.supplier', id: 7, rowVersion: 2, body: 'Mine.' });
    assert.equal(r.status, 409);
    assert.equal(r.body.code, 'STALE');
    assert.deepEqual(h.db.tx, ['begin', 'rollback']);
});

test('PUT update of a hint deleted (or deleted and recreated) meanwhile: 404 DELETED', async () => {
    const gone = harness();
    const r1 = await gone.call('PUT', '/api/v1/ui-hints', { key: 'payments.ready/col.supplier', id: 7, rowVersion: 2, body: 'Mine.' });
    assert.equal(r1.status, 404);
    assert.equal(r1.body.code, 'DELETED');
    const recreated = harness({ rows: [ROW({ id: 9, row_version: 1 })] });
    const r2 = await recreated.call('PUT', '/api/v1/ui-hints', { key: 'payments.ready/col.supplier', id: 7, rowVersion: 2, body: 'Mine.' });
    assert.equal(r2.status, 404);
    assert.equal(recreated.db.rows[0].body, 'The supplier on the PO.');
});

test('POST move: to an empty place, version moves on, audited', async () => {
    const h = harness({ rows: [ROW()] });
    const r = await h.call('POST', '/api/v1/ui-hints/7/move', { rowVersion: 2, toKey: 'payments.ready/col.vendor' });
    assert.equal(r.status, 200);
    assert.equal(r.body.hint.key, 'payments.ready/col.vendor');
    assert.equal(r.body.hint.rowVersion, 3);
    assert.deepEqual(h.db.tx, ['begin', 'commit']);
    // Full snapshots go in; recordAudit itself keeps only what differs.
    assert.equal(h.db.audits[0].action, 'update');
    assert.equal(h.db.audits[0].before.key, 'payments.ready/col.supplier');
    assert.equal(h.db.audits[0].after.key, 'payments.ready/col.vendor');
    assert.equal(h.db.audits[0].after.body, h.db.audits[0].before.body);
});

test('POST move refusals: taken place, same place, old version, gone, read-only role', async () => {
    const taken = harness({ rows: [ROW(), ROW({ id: 8, hint_key: 'a/b' })] });
    const r1 = await taken.call('POST', '/api/v1/ui-hints/7/move', { rowVersion: 2, toKey: 'a/b' });
    assert.equal(r1.status, 409);
    assert.equal(r1.body.code, 'OCCUPIED_MOVE');
    assert.deepEqual(taken.db.tx, ['begin', 'rollback']);

    const same = harness({ rows: [ROW()] });
    const r2 = await same.call('POST', '/api/v1/ui-hints/7/move', { rowVersion: 2, toKey: 'payments.ready/col.supplier' });
    assert.equal(r2.status, 400);
    assert.equal(r2.body.code, 'BAD_FIELD');

    const stale = harness({ rows: [ROW({ row_version: 4 })] });
    const r3 = await stale.call('POST', '/api/v1/ui-hints/7/move', { rowVersion: 2, toKey: 'a/c' });
    assert.equal(r3.status, 409);
    assert.equal(r3.body.code, 'STALE');
    assert.equal(r3.body.current.rowVersion, 4);

    const gone = harness();
    const r4 = await gone.call('POST', '/api/v1/ui-hints/7/move', { rowVersion: 2, toKey: 'a/c' });
    assert.equal(r4.status, 404);

    const reader = harness({ userType: 'accountant', rows: [ROW()] });
    const r5 = await reader.call('POST', '/api/v1/ui-hints/7/move', { rowVersion: 2, toKey: 'a/c' });
    assert.equal(r5.status, 403);
    assert.equal(r5.body.code, 'CANNOT_WRITE_HINTS');

    const bad = harness({ rows: [ROW()] });
    const r6 = await bad.call('POST', '/api/v1/ui-hints/7/move', { rowVersion: 0, toKey: 'a/c' });
    assert.equal(r6.status, 400);
});

test('DELETE: the author removes their hint; audited', async () => {
    const h = harness({ rows: [ROW()] });
    const r = await h.call('DELETE', '/api/v1/ui-hints/7?rowVersion=2');
    assert.equal(r.status, 200);
    assert.equal(r.body.removed.id, 7);
    assert.deepEqual(h.db.rows, []);
    assert.deepEqual(h.db.tx, ['begin', 'commit']);
    assert.equal(h.db.audits[0].action, 'delete');
    assert.equal(h.db.audits[0].after, null);
    assert.equal(h.db.audits[0].before.body, 'The supplier on the PO.');
});

test('DELETE: another standard user is refused; an admin is not', async () => {
    const other = harness({ userEmail: 'tom@jfa.test', rows: [ROW()] });
    const r1 = await other.call('DELETE', '/api/v1/ui-hints/7?rowVersion=2');
    assert.equal(r1.status, 403);
    assert.equal(r1.body.code, 'NOT_AUTHOR');
    assert.equal(other.db.rows.length, 1);
    assert.deepEqual(other.db.tx, ['begin', 'rollback']);

    const admin = harness({ userType: 'admin', userEmail: 'tom@jfa.test', rows: [ROW()] });
    const r2 = await admin.call('DELETE', '/api/v1/ui-hints/7?rowVersion=2');
    assert.equal(r2.status, 200);
    assert.equal(admin.db.rows.length, 0);
});

test('DELETE refusals: old version, gone, no version, read-only role', async () => {
    const stale = harness({ rows: [ROW({ row_version: 3 })] });
    const r1 = await stale.call('DELETE', '/api/v1/ui-hints/7?rowVersion=2');
    assert.equal(r1.status, 409);
    assert.equal(r1.body.code, 'STALE');
    assert.equal(stale.db.rows.length, 1);

    const gone = harness();
    const r2 = await gone.call('DELETE', '/api/v1/ui-hints/7?rowVersion=2');
    assert.equal(r2.status, 404);
    assert.equal(r2.body.code, 'DELETED');

    const none = harness({ rows: [ROW()] });
    const r3 = await none.call('DELETE', '/api/v1/ui-hints/7');
    assert.equal(r3.status, 400);
    assert.equal(r3.body.code, 'BAD_FIELD');

    const reader = harness({ userType: 'exec', rows: [ROW()] });
    const r4 = await reader.call('DELETE', '/api/v1/ui-hints/7?rowVersion=2');
    assert.equal(r4.status, 403);
    assert.equal(r4.body.code, 'CANNOT_WRITE_HINTS');
    assert.deepEqual(reader.db.tx, []);
});
