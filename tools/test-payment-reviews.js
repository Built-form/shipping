#!/usr/bin/env node
'use strict';

// HTTP suite for payment sign-offs (/api/v1/payment-reviews) and assignees
// (/api/v1/payment-assignees). Drives the real Express app in-process against
// the TEST database, like tools/test-allowed-emails.js.
//
//   node tools/test-payment-reviews.js
//
// Locally every request is local@dev; the role is read from LOCAL_USER_TYPE on
// each request, so the suite switches it between calls. A second reviewer is
// a row inserted directly (there is only one local identity). Everything it
// writes uses keys and @example.invalid addresses stamped with this run, and
// is deleted again — audit rows included.

process.env.NODE_ENV = 'development';
process.env.LOCAL_USER_TYPE = 'standard';

require('dotenv').config();
const http = require('http');
const { getPool, closePool } = require('../src/db');
const { app } = require('../src/handlers/orders');

const STAMP = Date.now();
const DEP_KEY = `deposit:9${String(STAMP % 1e9).padStart(9, '0')}`;
const BAL_KEY = `balance:USD:PRTEST-${STAMP}|prtest supplier`;
const ACC = `prtest-acc-${STAMP}@example.invalid`;
const STD = `prtest-std-${STAMP}@example.invalid`;
const ME = 'local@dev';

let base;
let failures = 0;
let passes = 0;
const check = (label, ok, detail) => {
    if (ok) passes++; else failures++;
    console.log(`${ok ? 'PASS' : 'FAIL'}  ${label}${ok ? '' : `  -> ${JSON.stringify(detail)}`}`);
    return ok;
};
const as = role => { process.env.LOCAL_USER_TYPE = role; };
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
const sql = async (q, p = []) => (await getPool().query(q, p))[0];
const keys = [DEP_KEY, BAL_KEY];

async function cleanup() {
    const reviews = await sql(`SELECT id FROM payment_reviews WHERE payment_key IN (?, ?)`, keys);
    const assignees = await sql(`SELECT id FROM payment_assignees WHERE payment_key IN (?, ?)`, keys);
    const ids = rows => rows.map(r => r.id);
    if (reviews.length) await sql(`DELETE FROM audit_log WHERE entity_type = 'payment_review' AND entity_id IN (?)`, [ids(reviews)]);
    // Assignee audit rows outlive their row (unassigning deletes it), so they are found by key.
    await sql(`DELETE FROM audit_log WHERE entity_type = 'payment_assignee' AND (JSON_UNQUOTE(JSON_EXTRACT(after_json, '$.paymentKey')) IN (?, ?) OR JSON_UNQUOTE(JSON_EXTRACT(before_json, '$.paymentKey')) IN (?, ?))`, [...keys, ...keys]);
    await sql(`DELETE FROM payment_reviews WHERE payment_key IN (?, ?)`, keys);
    await sql(`DELETE FROM payment_assignees WHERE payment_key IN (?, ?)`, keys);
    await sql(`DELETE FROM shipping_allowed_emails WHERE email IN (?, ?)`, [ACC, STD]);
}

(async () => {
    const host = process.env.DB_HOST || '';
    const db = process.env.DB_NAME || '';
    if (!/test/i.test(host) && !/test/i.test(db)) {
        console.error(`Refusing to run: ${host}/${db} does not look like the TEST database.`);
        process.exit(2);
    }
    const server = http.createServer(app).listen(0);
    await new Promise(r => server.once('listening', r));
    base = `http://127.0.0.1:${server.address().port}`;
    console.log(`TEST database ${host}/${db} · run ${STAMP}`);

    try {
        await sql(`INSERT INTO shipping_allowed_emails (email, display_name, type) VALUES (?, 'PR Test Accountant', 'accountant'), (?, 'PR Test Reviewer', 'standard')`, [ACC, STD]);

        console.log('\nList');
        as('standard');
        const empty = await call('GET', '/api/v1/payment-reviews');
        check('GET /payment-reviews → 200 with data, assignees and the required count',
            empty.status === 200 && Array.isArray(empty.body.data) && Array.isArray(empty.body.assignees) && empty.body.required === 2, empty);

        console.log('\nSigning off');
        const first = await call('POST', '/api/v1/payment-reviews', {
            paymentKey: DEP_KEY, currency: 'USD', amount: 1000, supplierName: 'PR Test Supplier', poNumbers: ['PRTEST-PO-1'], dueDate: '2026-10-01',
        });
        check('POST → 201, stamped with the caller', first.status === 201 && first.body.review?.reviewedByEmail === ME && first.body.review?.amount === 1000, first);
        check('POST → the key\'s current reviews come back', first.body.reviews?.length === 1 && first.body.reviews[0].paymentKey === DEP_KEY, first.body);
        const again = await call('POST', '/api/v1/payment-reviews', { paymentKey: DEP_KEY, currency: 'USD', amount: 1000 });
        check('same person, same figure → 409 ALREADY_REVIEWED', again.status === 409 && again.body.code === 'ALREADY_REVIEWED', again);

        const changed = await call('POST', '/api/v1/payment-reviews', { paymentKey: DEP_KEY, currency: 'USD', amount: 1200 });
        check('changed figure → 201, one current review at the new figure', changed.status === 201 && changed.body.reviews?.length === 1 && changed.body.reviews[0].amount === 1200, changed);
        const [old] = await sql(`SELECT revoked_at, revoked_by_email, revoked_reason FROM payment_reviews WHERE id = ?`, [first.body.review.id]);
        check('the old review is revoked as superseded', old && old.revoked_at != null && old.revoked_reason === 'superseded' && old.revoked_by_email === ME, old);

        // Somebody else, straight into the table: locally there is one identity.
        const other = await sql(
            `INSERT INTO payment_reviews (payment_key, kind, currency, amount, reviewed_by_email) VALUES (?, 'deposit', 'USD', 1200, ?)`, [DEP_KEY, STD]);
        const listed = await call('GET', '/api/v1/payment-reviews');
        const mine = (listed.body.data || []).filter(r => r.paymentKey === DEP_KEY);
        check('list → two current reviews by two people', mine.length === 2 && new Set(mine.map(r => r.reviewedByEmail)).size === 2, mine);
        check('list → the reviewer\'s name comes from the allowlist', mine.find(r => r.reviewedByEmail === STD)?.reviewedByName === 'PR Test Reviewer', mine);
        const history = await call('GET', `/api/v1/payment-reviews?paymentKey=${encodeURIComponent(DEP_KEY)}`);
        check('?paymentKey= → the whole history, replaced review included',
            history.status === 200 && history.body.data?.length === 3 && history.body.data.filter(r => r.revokedReason === 'superseded').length === 1, history);

        console.log('\nWithdrawing');
        const notMine = await call('DELETE', `/api/v1/payment-reviews/${other.insertId}`);
        check('somebody else\'s review, as standard → 403 NOT_YOURS', notMine.status === 403 && notMine.body.code === 'NOT_YOURS', notMine);
        const withdraw = await call('DELETE', `/api/v1/payment-reviews/${changed.body.review.id}`);
        check('own review → 200, the key\'s current reviews back', withdraw.status === 200 && withdraw.body.reviews?.length === 1 && withdraw.body.reviews[0].reviewedByEmail === STD, withdraw);
        const [gone] = await sql(`SELECT revoked_reason, revoked_by_email FROM payment_reviews WHERE id = ?`, [changed.body.review.id]);
        check('withdrawn row kept, marked withdrawn', gone?.revoked_reason === 'withdrawn' && gone?.revoked_by_email === ME, gone);
        const twice = await call('DELETE', `/api/v1/payment-reviews/${changed.body.review.id}`);
        check('withdrawing it again → 409 NOT_ACTIVE', twice.status === 409 && twice.body.code === 'NOT_ACTIVE', twice);
        as('admin');
        const byAdmin = await call('DELETE', `/api/v1/payment-reviews/${other.insertId}`);
        check('somebody else\'s review, as admin → 200', byAdmin.status === 200 && byAdmin.body.reviews?.length === 0, byAdmin);
        as('standard');
        const unknown = await call('DELETE', '/api/v1/payment-reviews/999999999');
        check('unknown review → 404', unknown.status === 404, unknown);
        const audits = await sql(`SELECT action FROM audit_log WHERE entity_type = 'payment_review' AND entity_id = ?`, [changed.body.review.id]);
        check('create and withdraw are audited', audits.some(a => a.action === 'create') && audits.some(a => a.action === 'update'), audits);

        console.log('\nRefusals');
        const zero = await call('POST', '/api/v1/payment-reviews', { paymentKey: DEP_KEY, currency: 'USD', amount: 0 });
        check('amount 0 → 400', zero.status === 400, zero);
        const badKey = await call('POST', '/api/v1/payment-reviews', { paymentKey: 'refund:1', currency: 'USD', amount: 5 });
        check('unknown key shape → 400', badKey.status === 400, badKey);
        const wrongCur = await call('POST', '/api/v1/payment-reviews', { paymentKey: BAL_KEY, currency: 'EUR', amount: 5 });
        check('balance key in USD reviewed as EUR → 400', wrongCur.status === 400, wrongCur);

        console.log('\nAccountants: assign, never review');
        as('accountant');
        const accReview = await call('POST', '/api/v1/payment-reviews', { paymentKey: BAL_KEY, currency: 'USD', amount: 2296 });
        check('accountant POST review → 403 CANNOT_REVIEW', accReview.status === 403 && accReview.body.code === 'CANNOT_REVIEW', accReview);
        const candidates = await call('GET', '/api/v1/payment-assignees/candidates');
        check('accountant GET candidates → 200, accountants only',
            candidates.status === 200 && candidates.body.data?.some(u => u.email === ACC && u.displayName === 'PR Test Accountant') && !candidates.body.data.some(u => u.email === STD), candidates);
        const assign = await call('PUT', '/api/v1/payment-assignees', { paymentKey: BAL_KEY, assigneeEmail: ACC.toUpperCase() });
        check('accountant PUT assignee → 200', assign.status === 200 && assign.body.assignee?.assigneeEmail === ACC && assign.body.assignee?.assigneeName === 'PR Test Accountant', assign);
        const inList = await call('GET', '/api/v1/payment-reviews');
        check('the assignee is in the list', inList.body.assignees?.some(a => a.paymentKey === BAL_KEY && a.assigneeEmail === ACC), inList.body.assignees);

        console.log('\nAssignment rules');
        as('standard');
        const notAcc = await call('PUT', '/api/v1/payment-assignees', { paymentKey: BAL_KEY, assigneeEmail: STD });
        check('a standard user as assignee → 422 NOT_ACCOUNTANT', notAcc.status === 422 && notAcc.body.code === 'NOT_ACCOUNTANT', notAcc);
        const stranger = await call('PUT', '/api/v1/payment-assignees', { paymentKey: BAL_KEY, assigneeEmail: `nobody-${STAMP}@example.invalid` });
        check('someone not on the allowlist → 422 NOT_ACCOUNTANT', stranger.status === 422 && stranger.body.code === 'NOT_ACCOUNTANT', stranger);
        const clear = await call('PUT', '/api/v1/payment-assignees', { paymentKey: BAL_KEY, assigneeEmail: null });
        const [left] = await sql(`SELECT COUNT(*) AS c FROM payment_assignees WHERE payment_key = ?`, [BAL_KEY]);
        check('null unassigns', clear.status === 200 && clear.body.assignee === null && Number(left.c) === 0, clear);
        const assignAudit = await sql(`SELECT action FROM audit_log WHERE entity_type = 'payment_assignee' AND JSON_UNQUOTE(JSON_EXTRACT(COALESCE(after_json, before_json), '$.paymentKey')) = ?`, [BAL_KEY]);
        check('assign and unassign are audited', assignAudit.some(a => a.action === 'create') && assignAudit.some(a => a.action === 'delete'), assignAudit);
        as('warehouse');
        const wh = await call('PUT', '/api/v1/payment-assignees', { paymentKey: BAL_KEY, assigneeEmail: ACC });
        check('warehouse PUT assignee → 403 CANNOT_ASSIGN', wh.status === 403 && wh.body.code === 'CANNOT_ASSIGN', wh);
        as('standard');

        // The assignee pays; two OTHER people sign off — both ways round.
        await sql(`INSERT INTO payment_assignees (payment_key, assignee_email, assigned_by_email) VALUES (?, ?, 'test')`, [BAL_KEY, ME]);
        const self = await call('POST', '/api/v1/payment-reviews', { paymentKey: BAL_KEY, currency: 'USD', amount: 2296 });
        check('the assignee reviewing → 409 ASSIGNEE_CANNOT_REVIEW', self.status === 409 && self.body.code === 'ASSIGNEE_CANNOT_REVIEW', self);
        await sql(`DELETE FROM payment_assignees WHERE payment_key = ?`, [BAL_KEY]);
        await sql(`INSERT INTO payment_reviews (payment_key, kind, container_ref, currency, amount, reviewed_by_email) VALUES (?, 'balance', ?, 'USD', 2296, ?)`, [BAL_KEY, `PRTEST-${STAMP}`, ACC]);
        const reviewedFirst = await call('PUT', '/api/v1/payment-assignees', { paymentKey: BAL_KEY, assigneeEmail: ACC });
        check('assigning someone who signed it off → 409 ASSIGNEE_HAS_REVIEWED', reviewedFirst.status === 409 && reviewedFirst.body.code === 'ASSIGNEE_HAS_REVIEWED', reviewedFirst);
        const balReview = await call('POST', '/api/v1/payment-reviews', { paymentKey: BAL_KEY, currency: 'USD', amount: 2296, poNumbers: ['PRTEST-PO-2'] });
        check('a balance review keeps its container from the key', balReview.status === 201 && balReview.body.review?.containerRef === `PRTEST-${STAMP}` && balReview.body.review?.kind === 'balance', balReview);
    } catch (e) {
        failures++;
        console.error('Suite crashed:', e);
    } finally {
        await cleanup().catch(e => { failures++; console.error('Cleanup failed:', e.message); });
        server.close();
        await closePool();
    }
    console.log(`\n${passes} passed, ${failures} failed.`);
    process.exit(failures === 0 ? 0 : 1);
})();
