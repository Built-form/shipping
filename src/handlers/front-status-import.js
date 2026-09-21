'use strict';

// Scheduled importer: reads recent inbound supplier emails from Front, asks
// Gemini to infer an order-status milestone, and queues PENDING
// 'status_suggestion' alerts for a human to approve/deny in ShipLine. See
// src/services/front-status-import.js. Runs hourly (serverless.yml).
//
// Idempotent — already-processed messages dedup-skip on
// front_status_imports.source_ref, so an empty run is just a Front search + a
// few DB queries. A keyword pre-filter + a per-run Gemini cap keep cost bounded;
// only genuinely new, status-suggestive supplier mail incurs a Gemini call.

require('dotenv').config();
const log = require('../lib/logger');
const { getPool } = require('../db');
const { importStatusUpdatesFromFront } = require('../services/front-status-import');

// By default the importer is INCREMENTAL: it scans only since the last
// fully-drained run (minus a small overlap), tracked in front_status_import_state
// — so the hourly tick doesn't re-walk days of Front history. Pass an explicit
// `{ "sinceDays": N }` to force a fixed window (e.g. a manual backfill). Also
// accepts `{ dryRun, verbose, model, confMin, maxGemini }`.

async function handler(event = {}) {
    for (const k of ['FRONT_API_TOKEN', 'GEMINI_API_KEY']) {
        if (!process.env[k]) throw new Error(`${k} is not configured`);
    }
    // undefined => incremental (the service reads the watermark).
    const sinceDays = Number(event && event.sinceDays) > 0 ? Number(event.sinceDays) : undefined;

    const pool = getPool();
    const conn = await pool.getConnection();
    try {
        const res = await importStatusUpdatesFromFront(conn, {
            sinceDays,
            dryRun: !!(event && event.dryRun),
            verbose: !!(event && event.verbose),
            force: !!(event && event.force),
            model: event && event.model,
            // Pass-2 order-aware refine is ON by default; allow an explicit
            // { "refine": false } to disable it (e.g. a cheap/fast manual run).
            refine: !(event && event.refine === false),
            refineModel: event && event.refineModel,
            ...(Number(event && event.confMin) >= 0 ? { confMin: Number(event.confMin) } : {}),
            ...(Number(event && event.maxGemini) > 0 ? { maxGemini: Number(event.maxGemini) } : {}),
        });
        log.info('[front-status-import] done', {
            window: res.window, scanned: res.scanned, geminiCalls: res.geminiCalls,
            alerted: res.alerted, alertsCreated: res.alertsCreated,
            skipped: res.skipped, failed: res.failed, capped: res.capped,
            watermarkAdvanced: res.watermarkAdvanced,
        });
        return { statusCode: 200, body: JSON.stringify(res) };
    } catch (err) {
        log.error('[front-status-import] fatal', err);
        throw err;
    } finally {
        conn.release();
    }
}

exports.handler = handler;
