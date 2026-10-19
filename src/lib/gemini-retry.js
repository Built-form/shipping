'use strict';

// Transient-error retry around a Gemini call, and the classification of a
// Gemini failure into the error codes the supplier-portal PI check stores.
//
// withGeminiRetry moved here from src/services/supplier-email-check.js (which
// re-exports it, so packing-list-check / shipment-payment-extract /
// suggestion-refine keep their import). Without a deadline it behaves exactly
// as before: up to `tries` attempts, exponential backoff, transient errors
// only. With `{ deadline }` (an epoch-ms instant, e.g. a Lambda's
// Date.now() + context.getRemainingTimeInMillis()), it also stops retrying
// once less than DEADLINE_RESERVE_MS would remain after the backoff, so the
// caller still has time to write its result.

const log = require('./logger');

// Retries stop when less than this would be left before the deadline.
const DEADLINE_RESERVE_MS = 90 * 1000;

// ── Transient retry (mirrors qc-report-check.js) ─────────────────────────────
function isTransientGeminiError(e) {
    if (!e) return false;
    const code = e.status ?? e.code ?? e.statusCode;
    if (typeof code === 'number' && (code >= 500 || code === 429)) return true;
    return /50\d|429|unavailable|high demand|overloaded|RESOURCE_EXHAUSTED|ECONNRESET|ETIMEDOUT|socket hang up/i
        .test(String((e && e.message) || ''));
}

async function withGeminiRetry(label, fn, tries = 5, { deadline = null, now = Date.now, sleep = null } = {}) {
    const wait = sleep || (ms => new Promise(r => setTimeout(r, ms)));
    let lastErr;
    for (let i = 0; i < tries; i++) {
        try { return await fn(); }
        catch (e) {
            lastErr = e;
            if (!isTransientGeminiError(e) || i === tries - 1) throw e;
            const backoff = Math.min(20000, 1500 * Math.pow(2, i)) + Math.floor(Math.random() * 750);
            if (deadline != null && deadline - now() - backoff < DEADLINE_RESERVE_MS) {
                log.warn(`[gemini-retry] ${label} transient — no time left to retry before the deadline`,
                    { error: String((e && e.message) || '').slice(0, 160) });
                throw e;
            }
            log.warn(`[gemini-retry] ${label} transient — retry ${i + 1}/${tries - 1} in ${backoff}ms`,
                { error: String((e && e.message) || '').slice(0, 160) });
            await wait(backoff);
        }
    }
    throw lastErr;
}

// ── Classification (supplier-portal PI check) ───────────────────────────────
// → { code, retryable } for jportal.pi_submissions.error_code:
//   DOCUMENT_UNPROCESSABLE  400 / 413 / INVALID_ARGUMENT, a page or payload
//                           limit: the file itself is the problem (portal: `file`)
//   MODEL_UNAVAILABLE       429 / RESOURCE_EXHAUSTED / 5xx / timeout / network;
//                           retryable (portal: `unavailable`)
//   NOT_CONFIGURED          401 / 403, or no key (portal: `unavailable`, logged
//                           at error level by the caller)
//   MODEL_NO_JSON / MODEL_BAD_OUTPUT  no JSON, or JSON that is not the schema
// Errors thrown by po-invoice-check.js carry the SDK error as `cause`; the code
// they already carry (MODEL_NO_JSON, NOT_CONFIGURED) wins.
const PASS_THROUGH = new Set(['NOT_CONFIGURED', 'MODEL_NO_JSON', 'MODEL_BAD_OUTPUT', 'DOCUMENT_UNPROCESSABLE', 'MODEL_UNAVAILABLE']);

function httpStatusOf(e) {
    for (const v of [e && e.status, e && e.statusCode, e && e.code]) {
        if (typeof v === 'number' && v >= 100 && v < 600) return v;
    }
    const m = /\b(400|401|403|404|408|413|429|5\d\d)\b/.exec(String((e && e.message) || ''));
    return m ? Number(m[1]) : null;
}

function classifyGeminiError(err) {
    if (err && typeof err.code === 'string' && PASS_THROUGH.has(err.code)) {
        return { code: err.code, retryable: err.code === 'MODEL_UNAVAILABLE' };
    }
    const e = err && err.cause ? err.cause : err;
    const status = httpStatusOf(e);
    const msg = String((e && e.message) || (err && err.message) || '');
    if (status === 401 || status === 403 || /PERMISSION_DENIED|UNAUTHENTICATED|API key not valid/i.test(msg)) {
        return { code: 'NOT_CONFIGURED', retryable: false };
    }
    if (status === 429 || (status != null && status >= 500)) return { code: 'MODEL_UNAVAILABLE', retryable: true };
    if (status === 400 || status === 413) return { code: 'DOCUMENT_UNPROCESSABLE', retryable: false };
    if (/RESOURCE_EXHAUSTED|UNAVAILABLE|DEADLINE_EXCEEDED|overloaded|high demand/i.test(msg)
        || /timed? ?out|timeout|abort|ECONNRESET|ETIMEDOUT|ECONNREFUSED|EAI_AGAIN|socket hang up|fetch failed/i.test(msg)) {
        return { code: 'MODEL_UNAVAILABLE', retryable: true };
    }
    if (/INVALID_ARGUMENT|page limit|too many pages|payload|request entity too large|exceeds the (maximum|limit)/i.test(msg)) {
        return { code: 'DOCUMENT_UNPROCESSABLE', retryable: false };
    }
    // Anything else: treat as the model being unavailable, so the supplier can
    // check again rather than being told their file is bad.
    return { code: 'MODEL_UNAVAILABLE', retryable: false };
}

module.exports = {
    DEADLINE_RESERVE_MS,
    isTransientGeminiError,
    withGeminiRetry,
    classifyGeminiError,
};
