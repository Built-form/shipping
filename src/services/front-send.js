'use strict';

// Sends one plain HTML email through Front (/channels/{FRONT_CHANNEL_ID}/messages),
// no attachments, and NEVER throws: the caller records the outcome. Used by the
// supplier-portal PI decision email (src/services/pi-submission-routes.js); the
// older document-send handlers in orders.js and receipt-reminders.js build
// their own multipart requests and are left as they are.
//
// → { ok: true, frontMessageUid, frontConversationId }
//   { ok: false, reason: 'no_recipients' | 'front_not_configured' | 'front_error' | 'network_error', status?, detail? }

const log = require('../lib/logger');

const SEND_TIMEOUT_MS = 15 * 1000;

async function sendFrontMessage({ to, subject, html }, { fetchImpl = globalThis.fetch, env = process.env } = {}) {
    try {
        const recipients = (Array.isArray(to) ? to : [to])
            .filter(x => typeof x === 'string').map(x => x.trim()).filter(Boolean);
        if (!recipients.length) return { ok: false, reason: 'no_recipients' };
        if (!env.FRONT_CHANNEL_ID || !env.FRONT_API_TOKEN) return { ok: false, reason: 'front_not_configured' };

        const form = new FormData();
        for (const addr of recipients) form.append('to[]', addr);
        form.append('subject', String(subject || '').replace(/[\r\n]+/g, ' ').slice(0, 500));
        form.append('body', String(html || ''));
        form.append('body_format', 'html');
        form.append('options[archive]', 'false');

        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), SEND_TIMEOUT_MS);
        let resp;
        let respText;
        try {
            resp = await fetchImpl(`https://api2.frontapp.com/channels/${encodeURIComponent(env.FRONT_CHANNEL_ID)}/messages`, {
                method: 'POST',
                headers: { Authorization: `Bearer ${env.FRONT_API_TOKEN}`, Accept: 'application/json' },
                body: form,
                signal: controller.signal,
            });
            respText = await resp.text();
        } finally {
            clearTimeout(timer);
        }
        if (!resp.ok) {
            log.warn('[front-send] Front API error', { status: resp.status, body: String(respText).slice(0, 300) });
            return { ok: false, reason: 'front_error', status: resp.status, detail: String(respText).slice(0, 500) };
        }
        let parsed = null;
        try { parsed = JSON.parse(respText); } catch { /* 202 accepted with an empty body is fine */ }
        const conversationUrl = parsed?._links?.related?.conversation || '';
        return {
            ok: true,
            frontMessageUid: parsed?.message_uid || parsed?.id || null,
            frontConversationId: conversationUrl ? conversationUrl.split('/').pop() : null,
        };
    } catch (e) {
        log.warn('[front-send] send failed', { error: e && e.message });
        return { ok: false, reason: 'network_error', detail: String((e && e.message) || e).slice(0, 500) };
    }
}

module.exports = { sendFrontMessage, SEND_TIMEOUT_MS };
