'use strict';

// Lambda jportal-pl-refresh-<env>: recomputes the supplier-facing result of
// one portal packing-list submission (src/services/pl-results.js
// refreshSubmission) — no S3, no Gemini, so a batch of checks can't hold up a
// page open. Invoked directly, async, by jportal whenever a supplier opens the
// page ({ submissionId }, throttled on the portal's side to once per 60 s per
// row); { submissionId, action: 'refresh' } is accepted too (the direct-invoke
// shape src/lib/pl-submissions.js parseTrigger understands). A bucket event
// is not for this function. The write is guarded on the row as it was read,
// so a late or duplicate refresh is harmless; an error never changes the row.

require('dotenv').config();
const log = require('../lib/logger');
const { getPool } = require('../db');
const { parseTrigger } = require('../lib/pl-submissions');
const { refreshSubmission } = require('../services/pl-results');

async function handler(event) {
    const trigger = parseTrigger(event);
    let out;
    if (trigger.ignore) out = { outcome: 'ignored', reason: trigger.ignore };
    else if (trigger.source !== 'direct') out = { outcome: 'ignored', reason: 'not_a_direct_invoke' };
    else {
        try {
            out = { outcome: 'refreshed', ...(await refreshSubmission(getPool(), trigger.submissionId)) };
        } catch (e) {
            log.error('[pl-refresh] refresh failed; the last result stays', { submissionId: trigger.submissionId, error: e.message });
            out = { outcome: 'refresh_failed', submissionId: trigger.submissionId, reason: e.message };
        }
    }
    log.info('[pl-refresh] done', out);
    return out;
}

exports.handler = handler;
