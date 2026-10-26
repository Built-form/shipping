'use strict';

// Lambda jportal-ci-check-<env>: reads a commercial invoice a supplier
// uploaded through the supplier portal (jportal) and suggests which of their
// booked shipments it belongs to (for staff; the supplier sees none of it).
// Triggered by the portal bucket's EventBridge "Object Created" event for
// ci/* (rule in serverless.yml) and invoked directly, async, by jportal with
// { submissionId }. The work is in src/services/ci-check.js. Async invokes
// are retried once by Lambda, which only helps with a failure before the
// claim: run() throws only when writing the failed row itself fails.

require('dotenv').config();
const log = require('../lib/logger');
const { getPool } = require('../db');
const { createCiChecker } = require('../services/ci-check');

let checker = null;

async function handler(event, context) {
    if (!checker) checker = createCiChecker({ pool: getPool() });
    const remaining = context && typeof context.getRemainingTimeInMillis === 'function'
        ? context.getRemainingTimeInMillis()
        : 300 * 1000;
    const out = await checker.run(event, { deadline: Date.now() + remaining });
    log.info('[ci-check] done', out);
    return out;
}

exports.handler = handler;
