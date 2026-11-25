'use strict';

// Sends one HTML email through Amazon SES (v2) and NEVER throws: the caller
// records the outcome. Every supplier / forwarder email this service sends
// goes through here (the document sends in src/handlers/orders.js, the QC
// visit emails, the reminders, the portal notices, the freight quotes, ...).
//
// Replaced src/services/front-send.js on 2026-11-25: the emails used to go out
// through the team's Front inbox (channel purchasing01@jfamedical.co.uk).
// They now come from the SES identity SES_FROM_ARN names, with Reply-To set to
// EMAIL_REPLY_TO (the same inbox), so a supplier's reply still lands with the
// team. Nothing threads in Front any more: front_message_uid columns hold the
// SES message id, front_conversation_id stays NULL for new sends.
//
// Config (process.env, from the shipping/<env> secret via serverless.yml):
//   SES_FROM_ARN      arn:aws:ses:<region>:<account>:identity/<address>. An
//                     EMAIL-ADDRESS identity: the From address and the SES
//                     region come from it. Missing: nothing is sent, reason
//                     'ses_not_configured'.
//   EMAIL_FROM_NAME   the display name on From (default "JFA Medical").
//   EMAIL_REPLY_TO    optional, comma-separated: where a reply goes.
//
// `attachments` is optional: [{ bytes, filename, contentType }]. SES caps a
// message at 40 MB after encoding.
//
// → { ok: true, messageId }
//   { ok: false, reason: 'no_recipients' | 'ses_not_configured' | 'ses_error' | 'network_error', status?, retryable?, detail? }
// 'ses_error' is SES ANSWERING with a refusal: nothing was sent. `retryable`
// is true for a throttle or a server-side fault, false when the message itself
// was refused (a bad address, an unverified sender, a suspended account).
// 'network_error' is no answer at all (a timeout, a dropped connection): the
// email may or may not have gone. An accepted call is SES taking the message
// for sending, not proof of delivery.

const log = require('../lib/logger');

const SEND_TIMEOUT_MS = 15 * 1000;
const DEFAULT_FROM_NAME = 'JFA Medical';

/**
 * The sender from an SES EMAIL-ADDRESS identity ARN: { region, address, arn }.
 * null when the ARN is missing, malformed or a domain identity.
 */
function senderOf(arn) {
    const m = /^arn:aws:ses:([a-z0-9-]+):(\d{12}):identity\/(.+)$/.exec(String(arn || '').trim());
    if (!m) return null;
    const address = m[3].trim();
    if (!/^[^\s@<>()[\]\\,;:"]+@[^\s@<>()[\]\\,;:"]+\.[^\s@<>()[\]\\,;:"]+$/.test(address)) return null;
    return { region: m[1], address, arn: m[0] };
}

/** True when SES_FROM_ARN names a usable sender. The route guards use it. */
function isConfigured(env = process.env) {
    return senderOf(env.SES_FROM_ARN) !== null;
}

const splitAddresses = v => String(v || '').split(',').map(s => s.trim()).filter(Boolean);

// SES error names that mean "try again later", from the SESv2 API reference.
const RETRYABLE_NAMES = new Set([
    'TooManyRequestsException', 'LimitExceededException', 'SendingPausedException',
    'ServiceUnavailable', 'ServiceUnavailableException', 'InternalFailure', 'InternalServiceErrorException',
]);
const NETWORK_NAMES = new Set(['TimeoutError', 'AbortError', 'NetworkingError', 'RequestTimeout', 'RequestTimeoutException']);
const NETWORK_CODES = new Set(['ECONNRESET', 'ECONNREFUSED', 'ETIMEDOUT', 'ENOTFOUND', 'EAI_AGAIN', 'EPIPE', 'ERR_SOCKET_CLOSED']);

const clients = new Map();
async function sesSend(region, input, abortSignal) {
    const { SESv2Client, SendEmailCommand } = require('@aws-sdk/client-sesv2');
    if (!clients.has(region)) clients.set(region, new SESv2Client({ region }));
    return clients.get(region).send(new SendEmailCommand(input), { abortSignal });
}

// Test seam: the SES call every send goes through unless the caller passes its
// own sendImpl. (region, input, abortSignal) => SendEmail output. null restores SES.
let sendOverride = null;
function _setSendImplForTests(f) { sendOverride = f || null; }
const defaultSend = (region, input, abortSignal) => (sendOverride || sesSend)(region, input, abortSignal);

/** The SendEmail input for one message. Exported for tests. */
function buildInput({ to, subject, html, text, attachments, replyTo }, sender, env = process.env) {
    const fromName = String(env.EMAIL_FROM_NAME || '').trim() || DEFAULT_FROM_NAME;
    const reply = replyTo != null ? splitAddresses(Array.isArray(replyTo) ? replyTo.join(',') : replyTo) : splitAddresses(env.EMAIL_REPLY_TO);
    const files = (Array.isArray(attachments) ? attachments : [])
        .filter(a => a && a.bytes && a.filename)
        .map(a => ({
            RawContent: a.bytes instanceof Uint8Array ? a.bytes : Buffer.from(a.bytes),
            FileName: String(a.filename),
            ContentType: a.contentType || a.type || 'application/octet-stream',
            ContentDisposition: 'ATTACHMENT',
        }));
    return {
        FromEmailAddress: `${fromName.replace(/[<>"\r\n]/g, '')} <${sender.address}>`,
        FromEmailAddressIdentityArn: sender.arn,
        Destination: { ToAddresses: to },
        ...(reply.length ? { ReplyToAddresses: reply } : {}),
        Content: {
            Simple: {
                Subject: { Data: String(subject || '').replace(/[\r\n]+/g, ' ').slice(0, 500), Charset: 'UTF-8' },
                Body: {
                    Html: { Data: String(html || ''), Charset: 'UTF-8' },
                    ...(text ? { Text: { Data: String(text), Charset: 'UTF-8' } } : {}),
                },
                ...(files.length ? { Attachments: files } : {}),
            },
        },
    };
}

async function sendEmail({ to, subject, html, text, attachments, replyTo }, { sendImpl = defaultSend, env = process.env } = {}) {
    try {
        const recipients = (Array.isArray(to) ? to : [to])
            .filter(x => typeof x === 'string').map(x => x.trim()).filter(Boolean);
        if (!recipients.length) return { ok: false, reason: 'no_recipients' };
        const sender = senderOf(env.SES_FROM_ARN);
        if (!sender) return { ok: false, reason: 'ses_not_configured' };

        const input = buildInput({ to: recipients, subject, html, text, attachments, replyTo }, sender, env);
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), SEND_TIMEOUT_MS);
        let out;
        try {
            out = await sendImpl(sender.region, input, controller.signal);
        } finally {
            clearTimeout(timer);
        }
        return { ok: true, messageId: out?.MessageId || null };
    } catch (e) {
        const name = String(e?.name || '');
        const code = String(e?.code || '');
        const status = Number(e?.$metadata?.httpStatusCode) || null;
        const detail = String((e && e.message) || e).slice(0, 500);
        // No HTTP status is no answer from SES at all.
        if (NETWORK_NAMES.has(name) || NETWORK_CODES.has(code) || (!status && !name.endsWith('Exception'))) {
            log.warn('[email-send] send failed', { name, code, detail });
            return { ok: false, reason: 'network_error', detail };
        }
        const retryable = RETRYABLE_NAMES.has(name) || status === 429 || (status !== null && status >= 500);
        log.warn('[email-send] SES refused the send', { name, status, detail });
        return { ok: false, reason: 'ses_error', status, retryable, detail: `${name}: ${detail}`.slice(0, 500) };
    }
}

module.exports = { sendEmail, buildInput, senderOf, isConfigured, _setSendImplForTests, SEND_TIMEOUT_MS, DEFAULT_FROM_NAME };
