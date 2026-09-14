#!/usr/bin/env node
'use strict';

// Smoke test for the shipping_allowed_emails allowlist + /api/v1/users CRUD.
//
// Drives the real Express app in-process against the real DB. Everything it
// creates uses a disposable @example.invalid address and is deleted again, so
// no real user is ever touched.
//
//   node tools/test-allowed-emails.js                    # as an admin
//   $env:LOCAL_USER_TYPE='standard'; node tools/...      # checks the 403 gate
//
// NODE_ENV=development makes the auth middleware short-circuit to local@dev;
// LOCAL_USER_TYPE picks the role that short-circuit hands out.

process.env.NODE_ENV = 'development';
process.env.LOCAL_USER_TYPE = process.env.LOCAL_USER_TYPE || 'admin';
const AS_ADMIN = process.env.LOCAL_USER_TYPE === 'admin';

require('dotenv').config();
const http = require('http');
const { getPool, closePool } = require('../src/db');
const { app } = require('../src/handlers/orders');
const { lookupUserType } = require('../src/lib/allowed-emails');

const TEST_EMAIL = 'allowlist-smoke-test@example.invalid';

let base;
const call = async (method, path, body) => {
    const res = await fetch(`${base}${path}`, {
        method,
        headers: body ? { 'Content-Type': 'application/json' } : {},
        body: body ? JSON.stringify(body) : undefined,
    });
    const text = await res.text();
    let json;
    try { json = JSON.parse(text); } catch { json = text; }
    return { status: res.status, body: json };
};

let failures = 0;
const check = (label, ok, detail) => {
    console.log(`${ok ? 'PASS' : 'FAIL'}  ${label}${ok ? '' : `  -> ${JSON.stringify(detail)}`}`);
    if (!ok) failures++;
};

(async () => {
    const server = http.createServer(app).listen(0);
    await new Promise(r => server.once('listening', r));
    base = `http://127.0.0.1:${server.address().port}`;

    const pool = getPool();

    if (!AS_ADMIN) {
        // ── Admin gate ────────────────────────────────────────────────────
        const list = await call('GET', '/api/v1/users');
        check('non-admin GET /users 403s', list.status === 403, list);
        const post = await call('POST', '/api/v1/users', { email: TEST_EMAIL });
        check('non-admin POST /users 403s', post.status === 403, post);
        const del = await call('DELETE', `/api/v1/users/${TEST_EMAIL}`);
        check('non-admin DELETE /users/:email 403s', del.status === 403, del);
        const me = await call('GET', '/api/v1/users/me');
        check('non-admin GET /users/me is allowed', me.status === 200, me);

        server.close();
        await closePool();
        console.log(failures === 0 ? '\nAll checks passed.' : `\n${failures} check(s) failed.`);
        process.exit(failures === 0 ? 0 : 1);
    }

    // Clean any leftover from a previous aborted run.
    await pool.query('DELETE FROM shipping_allowed_emails WHERE email = ?', [TEST_EMAIL]);

    // ── Seed ──────────────────────────────────────────────────────────────
    const [[{ c: mine }]] = await pool.query('SELECT COUNT(*) AS c FROM shipping_allowed_emails');
    const [[{ c: shared }]] = await pool.query('SELECT COUNT(*) AS c FROM allowed_emails');
    check(`seeded from allowed_emails (${mine} rows here, ${shared} shared)`, mine >= shared, { mine, shared });

    const [diff] = await pool.query(`
        SELECT a.email FROM allowed_emails a
        LEFT JOIN shipping_allowed_emails s ON s.email = LOWER(a.email)
        WHERE s.email IS NULL
    `);
    check('every shared user was copied', diff.length === 0, diff);

    // ── Read ──────────────────────────────────────────────────────────────
    const list = await call('GET', '/api/v1/users');
    check('GET /users returns the list', list.status === 200 && Array.isArray(list.body.data), list);

    const filtered = await call('GET', '/api/v1/users?type=admin');
    check('GET /users?type=admin filters',
        filtered.status === 200 && filtered.body.data.length > 0
        && filtered.body.data.every(u => u.type === 'admin'), filtered);

    const searched = await call('GET', '/api/v1/users?q=built-form');
    check('GET /users?q= searches',
        searched.status === 200 && searched.body.data.every(u => /built-form/i.test(u.email)), searched);

    const unusedFilter = await call('GET', '/api/v1/users?type=wizard');
    check('GET /users?type=<unused role> returns an empty list, not an error',
        unusedFilter.status === 200 && unusedFilter.body.data.length === 0, unusedFilter);

    const badFilter = await call('GET', `/api/v1/users?type=${encodeURIComponent('Not A Role!')}`);
    check('GET /users?type=<malformed> 400s', badFilter.status === 400, badFilter);

    const me = await call('GET', '/api/v1/users/me');
    check('GET /users/me resolves the caller', me.status === 200 && !!me.body.email, me);

    // The role list is DISTINCT over the table, not a hardcoded array — the
    // structural two are always present, and whatever is in use joins them.
    const types = await call('GET', '/api/v1/user-types');
    const [distinct] = await pool.query(
        "SELECT DISTINCT type FROM shipping_allowed_emails WHERE type IS NOT NULL AND type <> ''"
    );
    const expectedTypes = [...new Set(['standard', 'admin', ...distinct.map(r => r.type)])].sort();
    check(`GET /user-types is the live DISTINCT list (${expectedTypes.join(', ')})`,
        types.status === 200 && JSON.stringify(types.body.data) === JSON.stringify(expectedTypes), types);

    // ── Create ────────────────────────────────────────────────────────────
    const created = await call('POST', '/api/v1/users', {
        email: TEST_EMAIL.toUpperCase(), type: 'warehouse', displayName: 'Smoke Test',
    });
    check('POST /users creates (and lower-cases the email)',
        created.status === 201 && created.body.email === TEST_EMAIL && created.body.type === 'warehouse', created);

    const dup = await call('POST', '/api/v1/users', { email: TEST_EMAIL });
    check('POST /users 409s on duplicate', dup.status === 409, dup);

    const badEmail = await call('POST', '/api/v1/users', { email: 'not-an-email' });
    check('POST /users 400s on a malformed address', badEmail.status === 400, badEmail);

    const badType = await call('POST', '/api/v1/users', { email: 'nope@example.invalid', type: 'Not A Role!' });
    check('POST /users 400s on a malformed type', badType.status === 400, badType);

    const fetched = await call('GET', `/api/v1/users/${TEST_EMAIL}`);
    check('GET /users/:email reads it back', fetched.status === 200 && fetched.body.id === created.body.id, fetched);

    // The new row must be visible to the auth middleware's own lookup path.
    check('lookupUserType sees the new user', await lookupUserType(pool, TEST_EMAIL) === 'warehouse');

    // ── Update ────────────────────────────────────────────────────────────
    const patched = await call('PATCH', `/api/v1/users/${TEST_EMAIL}`, { type: 'standard', displayName: 'Renamed' });
    check('PATCH /users/:email updates type + displayName',
        patched.status === 200 && patched.body.type === 'standard' && patched.body.displayName === 'Renamed', patched);

    const putAlias = await call('PUT', `/api/v1/users/${TEST_EMAIL}`, { displayName: null });
    check('PUT alias clears displayName', putAlias.status === 200 && putAlias.body.displayName === null, putAlias);

    // A brand-new role needs no deploy: assign it and it joins the live list.
    const NEW_ROLE = 'smoke-test-role';
    const roled = await call('PATCH', `/api/v1/users/${TEST_EMAIL}`, { type: NEW_ROLE });
    check('PATCH accepts a role no row has used before',
        roled.status === 200 && roled.body.type === NEW_ROLE, roled);
    const typesAfter = await call('GET', '/api/v1/user-types');
    check('the new role appears in /user-types immediately',
        typesAfter.status === 200 && typesAfter.body.data.includes(NEW_ROLE), typesAfter);

    const emptyPatch = await call('PATCH', `/api/v1/users/${TEST_EMAIL}`, {});
    check('PATCH with nothing to change 400s', emptyPatch.status === 400, emptyPatch);

    const rename = await call('PATCH', `/api/v1/users/${TEST_EMAIL}`, { email: 'other@example.invalid' });
    check('PATCH cannot change the email', rename.status === 400, rename);

    const missing = await call('PATCH', '/api/v1/users/nobody@example.invalid', { type: 'admin' });
    check('PATCH on an unknown user 404s', missing.status === 404, missing);

    // ── Audit trail ───────────────────────────────────────────────────────
    const [audits] = await pool.query(
        `SELECT action FROM audit_log WHERE entity_type = 'user' AND entity_id = ? ORDER BY id`,
        [created.body.id]
    );
    check('create + update are audited',
        audits.some(a => a.action === 'create') && audits.some(a => a.action === 'update'), audits);

    // ── Lockout guards ────────────────────────────────────────────────────
    // Read-only checks: the delete/demote guards are asserted against the live
    // admin count rather than by actually removing anyone.
    const [[{ c: admins }]] = await pool.query(
        "SELECT COUNT(*) AS c FROM shipping_allowed_emails WHERE type = 'admin'"
    );
    check(`more than one admin exists, so nobody is locked out (${admins})`, admins > 1, { admins });

    // ── Delete ────────────────────────────────────────────────────────────
    const removed = await call('DELETE', `/api/v1/users/${TEST_EMAIL}`);
    check('DELETE /users/:email revokes access', removed.status === 200, removed);
    check('lookupUserType no longer sees them', await lookupUserType(pool, TEST_EMAIL) === null);

    const removedAgain = await call('DELETE', `/api/v1/users/${TEST_EMAIL}`);
    check('DELETE on an unknown user 404s', removedAgain.status === 404, removedAgain);

    // …and a role with no users left simply stops being offered.
    const typesFinal = await call('GET', '/api/v1/user-types');
    check('a role with no users drops off /user-types',
        !typesFinal.body.data.includes(NEW_ROLE)
        && typesFinal.body.data.includes('admin') && typesFinal.body.data.includes('standard'), typesFinal);

    const [delAudit] = await pool.query(
        `SELECT action FROM audit_log WHERE entity_type = 'user' AND entity_id = ? AND action = 'delete'`,
        [created.body.id]
    );
    check('delete is audited', delAudit.length === 1, delAudit);

    server.close();
    await closePool();
    console.log(failures === 0 ? '\nAll checks passed.' : `\n${failures} check(s) failed.`);
    process.exit(failures === 0 ? 0 : 1);
})().catch(err => { console.error(err); process.exit(1); });
