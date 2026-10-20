'use strict';

// Lambda jportal-pl-check-<env>: reads a packing list a supplier uploaded
// through the supplier portal (jportal) and suggests which container it
// belongs to. Triggered by the portal bucket's EventBridge "Object Created"
// event for pl/* (rule in serverless.yml) and invoked directly, async, by
// jportal with { submissionId }. The work is in src/services/pl-check.js.
// Async invokes are retried once by Lambda, which only helps with a failure
// before the claim: run() throws only when writing the failed row itself fails.

require('dotenv').config();
const log = require('../lib/logger');
const { getPool } = require('../db');
const { createPlChecker } = require('../services/pl-check');

let checker = null;

async function handler(event, context) {
    if (!checker) checker = createPlChecker({ pool: getPool() });
    const remaining = context && typeof context.getRemainingTimeInMillis === 'function'
        ? context.getRemainingTimeInMillis()
        : 300 * 1000;
    const out = await checker.run(event, { deadline: Date.now() + remaining });
    log.info('[pl-check] done', out);
    return out;
}

exports.handler = handler;
