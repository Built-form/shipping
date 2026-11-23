'use strict';

// Unit tests for the email receipt log (src/lib/email-receipt.js) and the
// reminders built on it (src/services/receipt-reminders.js): which emails carry
// the "Confirm receipt" button, which are confirmed through the portal instead,
// and which are chased. No database: a fake connection records the SQL.
//
//   node --test tools/test-email-receipt-lib.js

const test = require('node:test');
const assert = require('node:assert/strict');

const R = require('../src/lib/email-receipt');
const { sendReminderForReceipt, runReceiptReminders } = require('../src/services/receipt-reminders');

/** A connection that records every query and answers from `answer(sql, params)`. */
function fakeConn(answer = () => [[]]) {
    const queries = [];
    return {
        queries,
        release() {},
        async query(sql, params) {
            queries.push({ sql: String(sql).replace(/\s+/g, ' ').trim(), params: params || [] });
            return answer(String(sql), params || []);
        },
    };
}

test('confirmViaOf / canRemind: a row from before the column is an email receipt', () => {
    assert.equal(R.confirmViaOf({}), 'email');
    assert.equal(R.confirmViaOf({ confirm_via: 'portal' }), 'portal');
    assert.equal(R.confirmViaOf({ confirm_via: 'whatever' }), 'email');
    assert.equal(R.canRemind({}), true);
    assert.equal(R.canRemind({ confirm_via: 'email' }), true);
    // A portal email is reminded only with an address to point at (the PO).
    assert.equal(R.canRemind({ confirm_via: 'portal', action_url: 'https://portal.example.com/orders/7?tab=pdfs' }), true);
    assert.equal(R.canRemind({ confirm_via: 'portal', action_url: null }), false);
    assert.equal(R.canRemind({ confirm_via: 'none', action_url: 'https://x.example.com' }), false);
});

test('createEmailReceipt: an email receipt by default; a portal one keeps its send and address', async () => {
    const conn = fakeConn();
    const token = await R.createEmailReceipt(conn, { emailType: 'quality_assurance', sentTo: ['a@x.com', 'b@x.com'], subject: 'QA' });
    assert.match(token, /^[a-f0-9]{48}$/);
    assert.deepEqual(conn.queries[0].params, [token, 'quality_assurance', null, null, '["a@x.com","b@x.com"]', 'QA', 'email', null]);

    const t2 = await R.createEmailReceipt(conn, {
        emailType: 'purchase_order', sentTo: ['a@x.com'], subject: 'PO 1', confirmVia: 'portal',
        actionUrl: 'https://portal.example.com/orders/7?tab=pdfs', sendTable: 'purchase_order_document_sends', sendId: 12,
    });
    assert.deepEqual(conn.queries[1].params, [
        t2, 'purchase_order', 'purchase_order_document_sends', 12, '["a@x.com"]', 'PO 1', 'portal',
        'https://portal.example.com/orders/7?tab=pdfs',
    ]);

    // An unknown confirmVia is not stored as typed.
    await R.createEmailReceipt(conn, { emailType: 'x', sentTo: 'a@x.com', subject: null, confirmVia: 'bogus' });
    assert.equal(conn.queries[2].params[6], 'email');
    assert.equal(conn.queries[2].params[4], 'a@x.com');
});

test('logSentEmail: a notice by default, and a database failure never reaches the caller', async () => {
    const conn = fakeConn();
    const token = await R.logSentEmail(conn, { emailType: 'pi_decision', sentTo: 'a@x.com', subject: 'PI', sendTable: 'pi_submissions', sendId: 3 });
    assert.match(token, /^[a-f0-9]{48}$/);
    assert.equal(conn.queries[0].params[6], 'none');

    const broken = { query: async () => { throw new Error('gone'); } };
    assert.equal(await R.logSentEmail(broken, { emailType: 'file_transfer', sentTo: ['a@x.com'], confirmVia: 'portal' }), null);
});

test('discardEmailReceipt: only a row no send was linked to, and it never throws', async () => {
    const conn = fakeConn();
    await R.discardEmailReceipt(conn, 'abc');
    assert.match(conn.queries[0].sql, /^DELETE FROM email_receipts WHERE token = \? AND send_id IS NULL/);
    await R.discardEmailReceipt(conn, null);
    assert.equal(conn.queries.length, 1);
    await R.discardEmailReceipt({ query: async () => { throw new Error('gone'); } }, 'abc');
});

test('the public confirm link answers email receipts only', async () => {
    const conn = fakeConn();
    assert.equal(await R.markReceiptOpened(conn, 'a'.repeat(48)), null);
    assert.equal(await R.markReceiptConfirmed(conn, 'a'.repeat(48)), null);
    for (const q of conn.queries) assert.match(q.sql, /WHERE token = \? AND confirm_via = 'email'/);
});

test('reminder body: a portal reminder asks to open the portal and never to confirm', () => {
    const email = R.appendReceiptLink(R.renderReminderEmailHtml('PO 1 <b>', 2), 'a'.repeat(48), 'https://api.example.com');
    assert.match(email, /confirm you have received it/);
    assert.match(email, /api\/v1\/confirm-receipt\/a{48}/);
    assert.match(email, /PO 1 &lt;b&gt;/);

    const url = 'https://portal.example.com/orders/7?tab=pdfs';
    const portal = R.appendPortalLink(R.renderReminderEmailHtml('PO 1', 2, { portal: true }), url);
    assert.match(portal, /Reminder #2/);
    assert.match(portal, /waiting for you on our supplier portal/);
    assert.match(portal, /href="https:\/\/portal\.example\.com\/orders\/7\?tab=pdfs"/);
    assert.doesNotMatch(portal, /confirm-receipt/);
    assert.doesNotMatch(portal, /confirm you have received/i);
    assert.doesNotMatch(portal, /确认您已收到/);
    // No address: nothing appended.
    assert.equal(R.appendPortalLink('<p>x</p>', null), '<p>x</p>');
});

test('sendReminderForReceipt: the confirm button for an email receipt, the portal address for a portal one, nothing otherwise', async () => {
    const realFetch = globalThis.fetch;
    const saved = { c: process.env.FRONT_CHANNEL_ID, t: process.env.FRONT_API_TOKEN };
    process.env.FRONT_CHANNEL_ID = 'cha_1';
    process.env.FRONT_API_TOKEN = 'tok';
    const sent = [];
    globalThis.fetch = async (url, init) => {
        sent.push(Object.fromEntries(init.body.entries()));
        return { ok: true, status: 202, text: async () => JSON.stringify({ message_uid: 'm1' }) };
    };
    try {
        let r = await sendReminderForReceipt({ token: 'b'.repeat(48), sent_to: '["a@x.com"]', subject: 'QA sheet', reminder_count: 0 }, 'https://api.example.com');
        assert.equal(r.ok, true);
        assert.equal(r.reminderNumber, 1);
        assert.match(sent[0].body, /confirm-receipt\/b{48}/);

        r = await sendReminderForReceipt({
            token: 'c'.repeat(48), sent_to: '["a@x.com"]', subject: 'PO 1', reminder_count: 1,
            confirm_via: 'portal', action_url: 'https://portal.example.com/orders/7?tab=pdfs',
        }, 'https://api.example.com');
        assert.equal(r.ok, true);
        assert.equal(r.reminderNumber, 2);
        assert.match(sent[1].subject, /^Reminder #2 .*: PO 1$/);
        assert.match(sent[1].body, /portal\.example\.com\/orders\/7/);
        assert.doesNotMatch(sent[1].body, /confirm-receipt/);

        // A magic-link email and a notice have no reminder: nothing goes out.
        r = await sendReminderForReceipt({ token: 'd'.repeat(48), sent_to: '["a@x.com"]', subject: 'Artwork', confirm_via: 'portal', action_url: null }, 'https://api.example.com');
        assert.deepEqual(r, { ok: false, reason: 'no_reminder' });
        r = await sendReminderForReceipt({ token: 'e'.repeat(48), sent_to: '["a@x.com"]', subject: 'PI', confirm_via: 'none' }, 'https://api.example.com');
        assert.deepEqual(r, { ok: false, reason: 'no_reminder' });
        assert.equal(sent.length, 2);
    } finally {
        globalThis.fetch = realFetch;
        if (saved.c === undefined) delete process.env.FRONT_CHANNEL_ID; else process.env.FRONT_CHANNEL_ID = saved.c;
        if (saved.t === undefined) delete process.env.FRONT_API_TOKEN; else process.env.FRONT_API_TOKEN = saved.t;
    }
});

test('portalSyncSteps: every step touches unconfirmed portal receipts only', () => {
    const steps = R.portalSyncSteps('`jportal`');
    assert.deepEqual(steps.map(s => s[0]), [
        'po_viewed', 'signed_pi_viewed', 'request_viewed', 'request_pl_uploaded', 'request_ci_uploaded', 'artwork_ticked', 'file_opened', 'qc_availability_answered', 'qc_schedule_opened', 'quote_answered',
        'payment_viewed', 'forwarder_payment_seen',
        'freight_quote_answered', 'freight_booking_answered', 'freight_movements_priced', 'forwarder_container_answered', 'ready_dates_added',
    ]);
    for (const [name, sql] of steps) {
        assert.match(sql, /^\s*UPDATE email_receipts r/, name);
        assert.match(sql, /r\.confirm_via = 'portal' AND r\.confirmed_at IS NULL/, name);
    }
    const step = Object.fromEntries(steps);
    // What happened before the email went out does not confirm it.
    assert.match(step.po_viewed, /v\.last_viewed_at >= r\.created_at/);
    assert.match(step.qc_availability_answered, /a\.answered_at >= r\.created_at/);
    // A fetched schedule page is an open, not a confirmation.
    assert.match(step.qc_schedule_opened, /SET r\.opened_at/);
    assert.doesNotMatch(step.qc_schedule_opened, /SET r\.confirmed_at/);
    // The signed PI: its own sends and its own portal table, that file or a later one of the PO.
    assert.match(step.signed_pi_viewed, /JOIN purchase_order_signed_pi_sends s ON s\.id = r\.send_id/);
    assert.match(step.signed_pi_viewed, /r\.send_table = 'purchase_order_signed_pi_sends'/);
    assert.match(step.signed_pi_viewed, /`jportal`\.signed_pi_views v/);
    assert.match(step.signed_pi_viewed, /v\.purchase_order_signed_pi_id >= d\.id/);
    assert.match(step.signed_pi_viewed, /v\.last_viewed_at >= r\.created_at/);
    assert.doesNotMatch(step.signed_pi_viewed, /po_document_views/);
    // Every chased portal email has the step that confirms it.
    // (The Request to send's step has its own tests: tools/test-request-to-send-portal-lib.js.)
    assert.deepEqual(R.CHASED_PORTAL_STEPS, {
        purchase_order_document_sends: 'po_viewed',
        purchase_order_signed_pi_sends: 'signed_pi_viewed',
        draft_container_document_sends: 'request_viewed',
    });
    for (const name of Object.values(R.CHASED_PORTAL_STEPS)) assert.ok(step[name], name);
});

// ── The container email to a forwarder (docs/forwarder-containers.md) ────────

test('the container email is confirmed when that forwarder sends something complete for that container after it', () => {
    const s = Object.fromEntries(R.portalSyncSteps('`jportal`')).forwarder_container_answered.replace(/\s+/g, ' ').trim();
    assert.match(s, /^UPDATE email_receipts r JOIN freight_container_notices n ON n\.id = r\.send_id SET r\.confirmed_at = /);
    assert.match(s, /r\.send_table = 'freight_container_notices' AND n\.sent_at IS NOT NULL/);
    // The same container AND the same forwarder, at or after the email actually went.
    const details = '`jportal`.forwarder_container_details d WHERE d.shipment_id = n.shipment_id AND d.forwarder_id = n.forwarder_id AND d.created_at >= n.sent_at';
    const uploads = '`jportal`.forwarder_uploads u WHERE u.shipment_id = n.shipment_id AND u.forwarder_id = n.forwarder_id AND u.sent_at IS NOT NULL AND u.sent_at >= n.sent_at';
    assert.equal(s.split(details).length - 1, 3, 'when it is confirmed (twice: alone and against an upload), and whether it is');
    assert.equal(s.split(uploads).length - 1, 3);
    // An upload counts from the moment its file arrived, never from when its address was asked for.
    assert.doesNotMatch(s, /u\.created_at/);
    // Measured from when the email went, not from when its row was first written.
    assert.doesNotMatch(s, /r\.created_at/);
});

// ── A payment sent to a supplier (docs/payment-proof-send.md) ────────────────

test('a payment email sent as a portal link is confirmed when that payment is opened on the portal after it', () => {
    const step = Object.fromEntries(R.portalSyncSteps('`jportal`')).payment_viewed;
    const s = step.replace(/\s+/g, ' ').trim();
    assert.match(s, /^UPDATE email_receipts r JOIN supplier_payment_sends s ON s\.id = r\.send_id SET r\.confirmed_at = /);
    assert.match(s, /r\.send_table = 'supplier_payment_sends'/);
    // The portal's own record of who opened which payment: that payment, after this email.
    const view = '`jportal`.payment_views v WHERE v.supplier_payment_id = s.supplier_payment_id AND v.last_viewed_at >= r.created_at';
    assert.equal(s.split(view).length - 1, 2, 'the time it is confirmed at, and whether it is');
    assert.match(s, /SELECT MIN\(IF\(v\.first_viewed_at >= r\.created_at, v\.first_viewed_at, v\.last_viewed_at\)\)/);
});

test('a forwarder\'s payment email is confirmed by what that forwarder did on the portal after it, never by a supplier\'s opening', () => {
    const steps = Object.fromEntries(R.portalSyncSteps('`jportal`'));
    // The supplier's step leaves a forwarder's email alone: payment_views says nothing about a forwarder.
    assert.match(steps.payment_viewed.replace(/\s+/g, ' '), /r\.send_table = 'supplier_payment_sends' AND s\.notice_kind <> 'forwarder' AND EXISTS/);
    const s = steps.forwarder_payment_seen.replace(/\s+/g, ' ').trim();
    assert.match(s, /^UPDATE email_receipts r JOIN supplier_payment_sends s ON s\.id = r\.send_id SET r\.confirmed_at = \(SELECT MIN\(e\.created_at\) FROM /);
    assert.match(s, /r\.send_table = 'supplier_payment_sends' AND s\.notice_kind = 'forwarder' AND EXISTS/);
    // The portal's own events (a proof opened, or Acknowledge pressed): that payment, by the
    // forwarder this email went to, after this email.
    const event = '`jportal`.forwarder_payment_events e WHERE e.supplier_payment_id = s.supplier_payment_id AND e.forwarder_id = s.forwarder_id AND e.created_at >= r.created_at';
    assert.equal(s.split(event).length - 1, 2, 'the time it is confirmed at, and whether it is');
    assert.doesNotMatch(s, /payment_views/);
});

test('payment emails are never swept: no automatic reminder, by either delivery', async () => {
    assert.deepEqual(R.NEVER_SWEPT_TYPES, ['payment_proof']);
    const conn = fakeConn(sql => (/COALESCE\(MAX\(send_id\)/.test(sql) ? [[{ last: 0 }]] : [[]]));
    await runReceiptReminders({ getConnection: async () => conn }, { baseUrl: 'https://api.example.com' });
    const pick = conn.queries.find(q => /^\s*SELECT id, token, email_type/.test(q.sql));
    assert.match(pick.sql.replace(/\s+/g, ' '), /AND sent_to IS NOT NULL AND email_type NOT IN \('payment_proof'\) AND reminder_count < \?/);
    // By its type, so a receipt minted before its send row exists is left alone too.
    assert.doesNotMatch(pick.sql, /send_table NOT IN \('supplier_payment_sends'\)/);
    // And it is not one of the chased portal kinds either.
    assert.equal(Object.keys(R.CHASED_PORTAL_STEPS).includes('supplier_payment_sends'), false);
    // A view that cannot be read is reported like any other step.
    const blind = fakeConn((sql) => {
        if (/payment_views/.test(sql)) throw Object.assign(new Error('no such table'), { errno: 1146 });
        return /COALESCE\(MAX\(send_id\)/.test(sql) ? [[{ last: 0 }]] : [{ affectedRows: 0 }];
    });
    assert.ok((await R.syncPortalReceipts(blind, { maxAgeMs: 0 })).failed.includes('payment_viewed'));
    // And when the portal's schema cannot be named at all, it is among the steps that did not run.
    const saved = process.env.JPORTAL_DB_SCHEMA;
    process.env.JPORTAL_DB_SCHEMA = 'not a schema';
    try {
        const none = fakeConn(sql => (/COALESCE\(MAX\(send_id\)/.test(sql) ? [[{ last: 0 }]] : [{ affectedRows: 0 }]));
        assert.ok((await R.syncPortalReceipts(none, { maxAgeMs: 0 })).failed.includes('payment_viewed'));
    } finally {
        if (saved === undefined) delete process.env.JPORTAL_DB_SCHEMA; else process.env.JPORTAL_DB_SCHEMA = saved;
    }
});

test('getReceiptForResend: also says which send the email belongs to, for a guard that needs it', async () => {
    const conn = fakeConn(() => [[{ id: 5, send_table: 'supplier_payment_sends', send_id: 12 }]]);
    const row = await R.getReceiptForResend(conn, 5);
    assert.deepEqual([row.send_table, row.send_id], ['supplier_payment_sends', 12]);
    assert.match(conn.queries[0].sql, /send_table, send_id/);
});

test('syncPortalReceipts: a missing schema or a failing step is reported, not thrown; and it is throttled', async () => {
    const missing = Object.assign(new Error('no such table'), { errno: 1146 });
    const conn = fakeConn((sql) => {
        if (/FROM jfpro\.audit_log|JOIN jfpro\.audit_log/.test(sql)) throw missing;
        if (/po_document_views/.test(sql)) throw new Error('boom');
        if (/COALESCE\(MAX\(send_id\)/.test(sql)) return [[{ last: 41 }]];
        return [{ affectedRows: 0 }];
    });
    const out = await R.syncPortalReceipts(conn, { maxAgeMs: 0 });
    assert.deepEqual(out.failed.sort(), ['jfpro_emails', 'po_viewed', 'quote_answered']);
    // The import reads on from the last audit row it copied.
    const imp = conn.queries.find(q => /^INSERT IGNORE INTO email_receipts/.test(q.sql));
    assert.equal(imp.params[2], 41);
    assert.match(imp.sql, /a\.action LIKE 'approval_email_s%'/);

    // Within the window nothing is run again.
    const before = conn.queries.length;
    const again = await R.syncPortalReceipts(conn);
    assert.equal(again.skipped, true);
    assert.equal(conn.queries.length, before);
});

test('runReceiptReminders: portal emails are chased only when their views could be read', async () => {
    const pick = conn => conn.queries.find(q => /^SELECT id, token, email_type/.test(q.sql));

    const ok = fakeConn(sql => (/COALESCE\(MAX\(send_id\)/.test(sql) ? [[{ last: 0 }]] : [[]]));
    let summary = await runReceiptReminders({ getConnection: async () => ok }, { baseUrl: 'https://api.example.com' });
    assert.deepEqual(summary, { due: 0, sent: 0, failed: 0, skipped: 0 });
    assert.match(pick(ok).sql, /\(confirm_via = 'email' OR \(confirm_via = 'portal' AND action_url IS NOT NULL\)\)/);
    // The portal is read before anyone is picked.
    assert.ok(ok.queries.findIndex(q => /po_document_views/.test(q.sql)) < ok.queries.indexOf(pick(ok)));

    const blind = fakeConn((sql) => {
        if (/po_document_views|signed_pi_views|supplier_quote_views/.test(sql)) throw Object.assign(new Error('denied'), { errno: 1142 });
        return /COALESCE\(MAX\(send_id\)/.test(sql) ? [[{ last: 0 }]] : [[]];
    });
    summary = await runReceiptReminders({ getConnection: async () => blind }, { baseUrl: 'https://api.example.com' });
    assert.equal(summary.due, 0);
    assert.match(pick(blind).sql, /AND confirm_via = 'email' AND sent_to IS NOT NULL/);
    assert.doesNotMatch(pick(blind).sql, /action_url IS NOT NULL/);

    // Only the signed PI's views are missing (shipping deployed before the
    // portal's table exists): PO emails are still chased, signed-PI emails are not.
    const half = fakeConn((sql) => {
        if (/signed_pi_views/.test(sql)) throw Object.assign(new Error('no such table'), { errno: 1146 });
        return /COALESCE\(MAX\(send_id\)/.test(sql) ? [[{ last: 0 }]] : [[]];
    });
    await runReceiptReminders({ getConnection: async () => half }, { baseUrl: 'https://api.example.com' });
    assert.match(
        pick(half).sql,
        /\(confirm_via = 'email' OR \(confirm_via = 'portal' AND action_url IS NOT NULL AND send_table NOT IN \('purchase_order_signed_pi_sends'\)\)\)/
    );

    // The same for a Request to send sent as a portal link: while the portal's
    // supplier_quote_views is not there, those are left alone and the rest chased.
    const noRequestViews = fakeConn((sql) => {
        if (/supplier_quote_views/.test(sql)) throw Object.assign(new Error('no such table'), { errno: 1146 });
        return /COALESCE\(MAX\(send_id\)/.test(sql) ? [[{ last: 0 }]] : [[]];
    });
    await runReceiptReminders({ getConnection: async () => noRequestViews }, { baseUrl: 'https://api.example.com' });
    assert.match(
        pick(noRequestViews).sql,
        /\(confirm_via = 'email' OR \(confirm_via = 'portal' AND action_url IS NOT NULL AND send_table NOT IN \('draft_container_document_sends'\)\)\)/
    );
});

// ── A Request to send whose chase is over (docs/request-to-send-portal.md) ────

const flat = sql => String(sql).replace(/\s+/g, ' ').trim();
const isPick = sql => /^\s*SELECT id, token, email_type/.test(sql);
const DUE = { id: 5, token: 'f'.repeat(48), email_type: 'request_to_send', sent_to: '["a@x.com"]', subject: 'Request to send – 333', reminder_count: 0, confirm_via: 'email', action_url: null };

/** Runs `fn` with Front configured and every call to it recorded. */
async function withFront(fn) {
    const realFetch = globalThis.fetch;
    const saved = { c: process.env.FRONT_CHANNEL_ID, t: process.env.FRONT_API_TOKEN };
    process.env.FRONT_CHANNEL_ID = 'cha_1';
    process.env.FRONT_API_TOKEN = 'tok';
    const sent = [];
    globalThis.fetch = async (url, init) => {
        sent.push(Object.fromEntries(init.body.entries()));
        return { ok: true, status: 202, text: async () => JSON.stringify({ message_uid: 'm1' }) };
    };
    try {
        return await fn(sent);
    } finally {
        globalThis.fetch = realFetch;
        if (saved.c === undefined) delete process.env.FRONT_CHANNEL_ID; else process.env.FRONT_CHANNEL_ID = saved.c;
        if (saved.t === undefined) delete process.env.FRONT_API_TOKEN; else process.env.FRONT_API_TOKEN = saved.t;
    }
}

test('runReceiptReminders: a Request to send that was replaced, has shipped or has its documents is not picked', async () => {
    const conn = fakeConn(sql => (/COALESCE\(MAX\(send_id\)/.test(sql) ? [[{ last: 0 }]] : [[]]));
    await runReceiptReminders({ getConnection: async () => conn }, { baseUrl: 'https://api.example.com', maxReminders: 5 });
    const picks = conn.queries.filter(q => isPick(q.sql));
    assert.equal(picks.length, 1);
    assert.match(picks[0].sql, /FROM email_receipts r WHERE confirmed_at IS NULL AND /);
    // The reason must be NULL: any of the three leaves the email alone.
    assert.ok(picks[0].sql.includes(` AND ${flat(R.requestRemindersStoppedSql())} IS NULL ORDER BY id ASC LIMIT 200`), picks[0].sql);
    assert.deepEqual(picks[0].params, [5]);
});

test('runReceiptReminders: a server that cannot read that rule chases as it did before', async () => {
    for (const errno of [1049, 1054, 1142, 1143, 1146]) {
        const conn = fakeConn((sql) => {
            if (isPick(sql) && /THEN 'superseded'/.test(sql)) throw Object.assign(new Error('not there'), { errno });
            if (isPick(sql)) return [[DUE]];
            return /COALESCE\(MAX\(send_id\)/.test(sql) ? [[{ last: 0 }]] : [[]];
        });
        await withFront(async (sent) => {
            const summary = await runReceiptReminders({ getConnection: async () => conn }, { baseUrl: 'https://api.example.com', maxReminders: 5 });
            assert.deepEqual(summary, { due: 1, sent: 1, failed: 0, skipped: 0 }, `errno ${errno}`);
            assert.equal(sent.length, 1);
            assert.equal(sent[0].subject, 'Reminder #1 / 第 1 次提醒: Request to send – 333');
        });
        const picks = conn.queries.filter(q => isPick(q.sql));
        assert.equal(picks.length, 2, `errno ${errno}`);
        // The second time without the rule, and otherwise the same question.
        assert.doesNotMatch(picks[1].sql, /superseded|shipments|packing_lists/);
        assert.equal(picks[1].sql, picks[0].sql.replace(` AND ${flat(R.requestRemindersStoppedSql())} IS NULL`, ''));
        assert.deepEqual(picks[1].params, [5]);
    }
});

test('runReceiptReminders: any other fault of that question is not swallowed', async () => {
    const conn = fakeConn((sql) => {
        if (isPick(sql)) throw Object.assign(new Error('Deadlock found'), { errno: 1213 });
        return /COALESCE\(MAX\(send_id\)/.test(sql) ? [[{ last: 0 }]] : [[]];
    });
    await assert.rejects(
        runReceiptReminders({ getConnection: async () => conn }, { baseUrl: 'https://api.example.com' }),
        /Deadlock found/
    );
    assert.equal(conn.queries.filter(q => isPick(q.sql)).length, 1);
});

const RECEIPT_COLUMNS = 'id, email_type, send_table, send_id, sent_to, subject, created_at, opened_at, open_count,'
    + ' confirmed_at, confirmed_ip, confirmed_user_agent, reminder_count, last_reminder_at, confirm_via, action_url';

test('selectEmailReceipts: every row carries why its reminders stopped', async () => {
    const conn = fakeConn(() => [[{ id: 7, reminders_stopped: 'shipped' }]]);
    const rows = await R.selectEmailReceipts(conn, 'WHERE id = ?', [7]);
    assert.deepEqual(rows, [{ id: 7, reminders_stopped: 'shipped' }]);
    assert.equal(conn.queries.length, 1);
    assert.equal(
        conn.queries[0].sql,
        `SELECT ${RECEIPT_COLUMNS}, ${flat(R.requestRemindersStoppedSql())} AS reminders_stopped FROM email_receipts r WHERE id = ?`
    );
    assert.deepEqual(conn.queries[0].params, [7]);
});

test('selectEmailReceipts: a server that cannot read the rule still lists them, with no reason', async () => {
    for (const errno of [1049, 1054, 1142, 1143, 1146]) {
        const conn = fakeConn((sql) => {
            if (/THEN 'superseded'/.test(sql)) throw Object.assign(new Error('not there'), { errno });
            return [[{ id: 7, reminders_stopped: null }]];
        });
        const rows = await R.selectEmailReceipts(conn, 'ORDER BY created_at DESC, id DESC LIMIT 100 OFFSET 0');
        assert.deepEqual(rows, [{ id: 7, reminders_stopped: null }], `errno ${errno}`);
        assert.equal(conn.queries.length, 2);
        assert.equal(
            conn.queries[1].sql,
            `SELECT ${RECEIPT_COLUMNS}, NULL AS reminders_stopped FROM email_receipts r ORDER BY created_at DESC, id DESC LIMIT 100 OFFSET 0`
        );
        assert.deepEqual(conn.queries[1].params, []);
    }
    const broken = fakeConn(() => { throw Object.assign(new Error('Deadlock found'), { errno: 1213 }); });
    await assert.rejects(R.selectEmailReceipts(broken, 'WHERE id = ?', [7]), /Deadlock found/);
    assert.equal(broken.queries.length, 1);
});
