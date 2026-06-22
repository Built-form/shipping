'use strict';

// Scheduled importer: pulls new AQI QC inspection reports out of Front into the
// QC pipeline (see src/services/front-qc-import.js) and analyzes each new one.
// Runs hourly (serverless.yml). Idempotent — already-imported reports dedup-skip
// on source_ref, so an empty run is just a Front search + a few DB queries (~3s);
// only genuinely new reports incur a download + Gemini analyze.

require('dotenv').config();
const log = require('../lib/logger');
const { getPool } = require('../db');
const { importQcReportsFromFront, analyzeAndPersist } = require('../services/front-qc-import');

// Look-back window. Small but generous enough that a few missed/failed hourly
// runs still get caught up; the overlap is free because imports dedup on
// source_ref. Override per-invocation with an event `{ "sinceDays": N }`.
const DEFAULT_SINCE_DAYS = 3;

async function handler(event = {}) {
    for (const k of ['FRONT_API_TOKEN', 'PO_DOCS_BUCKET', 'GEMINI_API_KEY']) {
        if (!process.env[k]) throw new Error(`${k} is not configured`);
    }
    const sinceDays = Number(event && event.sinceDays) > 0 ? Number(event.sinceDays) : DEFAULT_SINCE_DAYS;

    const conn = await getPool().getConnection();
    try {
        const res = await importQcReportsFromFront(conn, { sinceDays });
        log.info('[qc-front-import] import done', {
            found: res.found.length, imported: res.imported.length,
            skipped: res.skipped.length, failed: res.failed.length,
        });
        for (const f of res.failed) {
            log.warn('[qc-front-import] fetch/store failed', { sourceKey: f.sourceKey, error: f.error });
        }

        // Analyze every imported-but-unanalyzed report (status 'uploaded'), not
        // just this run's new ones — so an interrupted prior run self-heals. The
        // schedule has the full 900s timeout; reservedConcurrency=1 = no overlap.
        const [pending] = await conn.query(
            `SELECT * FROM qc_reports WHERE status = 'uploaded' AND deleted_at IS NULL ORDER BY id`
        );
        const analyzed = [];
        for (const row of pending) {
            try {
                const out = await analyzeAndPersist(conn, row);
                analyzed.push({ id: row.id, items: out.items.length, matched: out.matchedItemCount });
                log.info('[qc-front-import] analyzed', {
                    id: row.id, items: out.items.length, matched: out.matchedItemCount, unmatched: out.unmatched.length,
                });
            } catch (e) {
                log.error('[qc-front-import] analyze failed', { id: row.id, error: e.message });
            }
        }

        return {
            statusCode: 200,
            body: JSON.stringify({
                sinceDays,
                imported: res.imported.map(i => ({ id: i.id, kind: i.kind, filename: i.filename })),
                analyzed,
                skipped: res.skipped.length,
                failed: res.failed.length,
            }),
        };
    } catch (err) {
        log.error('[qc-front-import] fatal', err);
        throw err;
    } finally {
        conn.release();
    }
}

exports.handler = handler;
