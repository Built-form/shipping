'use strict';

// The email log (portal README section 26): src/lib/email-log.js, a copy of
// the portal's src/lib/emailLog.js held to the SAME masking cases, and the row
// src/services/email-send.js records for each send. No database, no network:
// the record step is replaced through _setRecordImplForTests, and nothing here
// can reach the table .env points at (a stubbed send is never logged for real).
//
//   node --test tools/test-email-log.js

const { test, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const L = require('../src/lib/email-log');
const { sendEmail, _setRecordImplForTests, _setSendImplForTests } = require('../src/services/email-send');

afterEach(() => { _setRecordImplForTests(null); _setSendImplForTests(null); });

const MASK = L.MASK;
const hex = 'a3f09c'.repeat(8);

test('masking: the same cases as the portal original', () => {
    assert.equal(L.redactUrl('https://portal.built-form.co.uk/contact/thread#k=abcDEF123_-xyz'), `https://portal.built-form.co.uk/contact/thread#${MASK}`);
    assert.equal(L.redactUrl(`https://portal.built-form.co.uk/artwork/${hex}`), `https://portal.built-form.co.uk/artwork/${MASK}`);
    assert.equal(L.redactUrl('https://p.example/quote-decline/Zk3_q9XwLmNoPqRsTuVwXyZaBcDeFgHiJkLmNoPqRsT'), `https://p.example/quote-decline/${MASK}`);
    assert.equal(L.redactUrl(`https://api.example/api/v1/confirm-receipt?t=${hex}`), `https://api.example/api/v1/confirm-receipt?t=${MASK}`);
    assert.equal(
        L.redactUrl('https://b.s3.eu-north-1.amazonaws.com/files/x.pdf?X-Amz-Credential=AKIA%2F20261125&amp;X-Amz-Signature=ab12&amp;tab=pi'),
        `https://b.s3.eu-north-1.amazonaws.com/files/x.pdf?X-Amz-Credential=${MASK}&amp;X-Amz-Signature=${MASK}&amp;tab=pi`,
    );
    assert.equal(L.redactUrl('https://portal.built-form.co.uk/orders/12345?tab=pi'), 'https://portal.built-form.co.uk/orders/12345?tab=pi');
    assert.equal(L.redactUrl('https://portal.built-form.co.uk/requests-to-send-documents'), 'https://portal.built-form.co.uk/requests-to-send-documents');
    assert.equal(L.redactUrl('https://portal.built-form.co.uk/help#'), 'https://portal.built-form.co.uk/help#');
});

const ENV = { SES_FROM_ARN: 'arn:aws:ses:eu-north-1:123456789012:identity/ops@example.com', EMAIL_REPLY_TO: 'purchasing01@example.com', EMAIL_LOG_STAGE: 'dev' };

test('a send is logged: kind, From and Reply-To as sent, attachments by name and size, the receipt link masked', async () => {
    const rows = [];
    _setRecordImplForTests(async (input) => { rows.push(input); });
    const html = `<p>Your PO</p><a href="https://api.example/api/v1/confirm-receipt?t=${hex}">Confirm</a>`;
    const out = await sendEmail({
        kind: 'po.documents', to: ['s@sup.example', ' '], subject: 'PO 1', html,
        attachments: [{ bytes: Buffer.alloc(300), filename: 'PO-1.pdf', contentType: 'application/pdf' }],
    }, { env: ENV, sendImpl: async () => ({ MessageId: 'm-1' }) });
    assert.deepEqual(out, { ok: true, messageId: 'm-1' });
    assert.equal(rows.length, 1);
    const r = rows[0];
    assert.equal(r.app, 'shipping');
    assert.equal(r.kind, 'po.documents');
    assert.equal(r.stage, 'dev');
    assert.equal(r.from, 'JFA Medical <ops@example.com>');
    assert.deepEqual(r.replyTo, ['purchasing01@example.com']);
    assert.deepEqual(r.to, ['s@sup.example']);
    assert.equal(r.result.ok, true);

    const values = L.emailLogValues(r);
    const row = Object.fromEntries(L.COLUMNS.map((c, i) => [c, values[i]]));
    assert.ok(!row.html_body.includes(hex), 'the receipt token is never stored');
    assert.match(row.html_body, /confirm-receipt\?t=\[redacted\]/);
    assert.deepEqual(JSON.parse(row.attachments), [{ filename: 'PO-1.pdf', contentType: 'application/pdf', size: 300 }]);
    assert.equal(row.message_id, 'm-1');
});

test('a refusal and no answer are logged as failed and unknown; nothing sent, nothing logged', async () => {
    const rows = [];
    _setRecordImplForTests(async (input) => { rows.push(L.statusOf(input.result).status); });
    const refuse = async () => { throw Object.assign(new Error('not verified'), { name: 'MessageRejected', $metadata: { httpStatusCode: 400 } }); };
    const hangUp = async () => { throw Object.assign(new Error('socket hang up'), { code: 'ECONNRESET' }); };
    await sendEmail({ to: 'a@b.com', subject: 's', html: 'x' }, { env: ENV, sendImpl: refuse });
    await sendEmail({ to: 'a@b.com', subject: 's', html: 'x' }, { env: ENV, sendImpl: hangUp });
    await sendEmail({ to: [], subject: 's', html: 'x' }, { env: ENV });
    await sendEmail({ to: 'a@b.com', subject: 's', html: 'x' }, { env: {} });
    assert.deepEqual(rows, ['failed', 'unknown']);
});

test('a log that fails or hangs never changes the send', async () => {
    _setRecordImplForTests(async () => { throw new Error('db down'); });
    assert.deepEqual(await sendEmail({ to: 'a@b.com', subject: 's', html: 'x' }, { env: ENV, sendImpl: async () => ({ MessageId: 'm' }) }), { ok: true, messageId: 'm' });
});

test('without the test seam, a stubbed send is never written to a real database', async () => {
    // env has no DB_HOST: were it tried, this would warn about a refused connection. It must not be tried at all.
    const warn = console.warn;
    let tried = false;
    _setSendImplForTests(async () => ({ MessageId: 'm' }));
    const mysql = require('mysql2/promise');
    const real = mysql.createConnection;
    mysql.createConnection = async () => { tried = true; throw new Error('no'); };
    try {
        await sendEmail({ to: 'a@b.com', subject: 's', html: 'x' }, { env: ENV });
        await sendEmail({ to: 'a@b.com', subject: 's', html: 'x' }, { env: ENV, sendImpl: async () => ({ MessageId: 'm' }) });
    } finally {
        mysql.createConnection = real;
        console.warn = warn;
    }
    assert.equal(tried, false);
});

// ── Delivered, opened, bounced (portal README section 26) ──

test('every email is sent with the configuration set; without one configured, with none', async () => {
    const seen = [];
    const sendImpl = async (region, input) => { seen.push(input); return { MessageId: 'm' }; };
    await sendEmail({ to: 'a@b.com', subject: 's', html: 'x' }, { env: { ...ENV, SES_CONFIGURATION_SET: ' jportal-email-prod ' }, sendImpl });
    await sendEmail({ to: 'a@b.com', subject: 's', html: 'x' }, { env: ENV, sendImpl });
    assert.equal(seen[0].ConfigurationSetName, 'jportal-email-prod');
    assert.equal('ConfigurationSetName' in seen[1], false);
});

test('a configuration set SES refuses never costs the email: it goes again without the set', async () => {
    const env = { ...ENV, SES_CONFIGURATION_SET: 'jportal-email-prod' };
    for (const refusal of [
        Object.assign(new Error('Configuration set <jportal-email-prod> does not exist.'), { name: 'NotFoundException', $metadata: { httpStatusCode: 404 } }),
        Object.assign(new Error("User: arn:aws:sts::1:assumed-role/x is not authorized to perform: ses:SendEmail on resource: arn:aws:ses:eu-north-1:1:configuration-set/jportal-email-prod"), { name: 'AccessDeniedException', $metadata: { httpStatusCode: 403 } }),
    ]) {
        const seen = [];
        const rows = [];
        _setRecordImplForTests(async (input) => { rows.push(input.result); });
        const sendImpl = async (region, input) => {
            seen.push(input);
            if (input.ConfigurationSetName) throw refusal;
            return { MessageId: 'm-2' };
        };
        assert.deepEqual(await sendEmail({ to: 'a@b.com', subject: 's', html: 'x' }, { env, sendImpl }), { ok: true, messageId: 'm-2' });
        assert.equal(seen.length, 2);
        assert.equal('ConfigurationSetName' in seen[1], false);
        assert.equal(seen[1].Destination.ToAddresses[0], 'a@b.com');
        assert.deepEqual(rows, [{ ok: true, messageId: 'm-2' }], 'logged once, as sent');
    }
});

test('any other refusal is not retried', async () => {
    let calls = 0;
    const sendImpl = async () => { calls++; throw Object.assign(new Error('Email address is not verified'), { name: 'MessageRejected', $metadata: { httpStatusCode: 400 } }); };
    const out = await sendEmail({ to: 'a@b.com', subject: 's', html: 'x' }, { env: { ...ENV, SES_CONFIGURATION_SET: 'jportal-email-prod' }, sendImpl });
    assert.equal(out.ok, false);
    assert.equal(out.reason, 'ses_error');
    assert.equal(calls, 1);
});
