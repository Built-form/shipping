'use strict';

require('dotenv').config();
const crypto = require('crypto');
const log = require('./logger');
const { getPool } = require('./db');
// SP-API phases (downloads, inventory summaries) can run longer than MySQL's
// `wait_timeout` (see db.js), which kills an idle connection mid-call. So we
// release the pool connection before the long HTTP work and reacquire a fresh
// one for the subsequent DB writes.
const {
    REPORT_TYPES,
    getEndpoint, getTokenManager,
    checkReport, downloadReport, parseTsvReport,
    fetchInventorySummaries, getMarketplace,
    writeSnapshots, copyFailedFromPrevious, runBackfill,
} = require('./amazon-stock-shared');

// Collector: polls outstanding report jobs, then processes one (account, country)
// unit per invocation. Designed to run every 2–5 minutes.

const POLL_LIMIT            = 30;                  // max reports to poll per run
const REQUEST_STALE_HOURS   = 6;                   // REQUESTED older than this → FAILED
const CLAIM_TIMEOUT_MINUTES = 15;                  // stale claims can be stolen

// ── Phase 1: poll outstanding REQUESTED reports ───────────────────────────────
async function pollPending(conn) {
    const [rows] = await conn.query(
        `SELECT id, batch_date, account, country, report_type, report_id, requested_at
         FROM amazon_report_jobs
         WHERE status = 'REQUESTED' AND report_id IS NOT NULL
         ORDER BY COALESCE(polled_at, requested_at) ASC
         LIMIT ?`,
        [POLL_LIMIT]
    );

    let done = 0, failed = 0, stillPending = 0;

    for (const row of rows) {
        const label = `${row.account}/${row.country}/${row.report_type}`;

        // Stale-timeout: reports that have been REQUESTED for too long get marked FAILED
        const ageHours = (Date.now() - new Date(row.requested_at).getTime()) / 3600000;
        if (ageHours > REQUEST_STALE_HOURS) {
            await conn.execute(
                `UPDATE amazon_report_jobs
                 SET status='FAILED', error=?, polled_at=NOW()
                 WHERE id = ?`,
                [`Report ${row.report_id} still REQUESTED after ${REQUEST_STALE_HOURS}h`, row.id]
            );
            log.warn(`[${label}] Stale — marked FAILED`);
            failed++;
            continue;
        }

        try {
            const tokenManager = getTokenManager(row.account);
            const { status, documentId } = await checkReport(tokenManager, getEndpoint(), row.report_id);

            if (status === 'DONE') {
                await conn.execute(
                    `UPDATE amazon_report_jobs
                     SET status='DONE', document_id=?, completed_at=NOW(), polled_at=NOW()
                     WHERE id = ?`,
                    [documentId, row.id]
                );
                log.info(`[${label}] DONE (doc=${documentId})`);
                done++;
            } else if (status === 'CANCELLED' || status === 'FATAL') {
                await conn.execute(
                    `UPDATE amazon_report_jobs
                     SET status='FAILED', error=?, polled_at=NOW()
                     WHERE id = ?`,
                    [`SP-API returned ${status}`, row.id]
                );
                log.warn(`[${label}] ${status}`);
                failed++;
            } else {
                await conn.execute(
                    `UPDATE amazon_report_jobs SET polled_at=NOW() WHERE id = ?`,
                    [row.id]
                );
                stillPending++;
            }
        } catch (err) {
            log.error(`[${label}] Poll error: ${err.message}`);
            await conn.execute(
                `UPDATE amazon_report_jobs SET polled_at=NOW() WHERE id = ?`,
                [row.id]
            );
        }
    }

    return { polled: rows.length, done, failed, stillPending };
}

// ── Phase 2: download + cache pan_eu ASIN lists ───────────────────────────────
async function cachePanEuResults(conn) {
    const [rows] = await conn.query(
        `SELECT id, batch_date, account, document_id
         FROM amazon_report_jobs
         WHERE report_type = ?
           AND status = 'DONE'
           AND result_cache IS NULL
           AND document_id IS NOT NULL
         ORDER BY completed_at ASC
         LIMIT 5`,
        [REPORT_TYPES.PAN_EU]
    );

    for (const row of rows) {
        const label = `${row.account}/pan_eu`;
        try {
            const tokenManager = getTokenManager(row.account);
            const tsv = await downloadReport(tokenManager, getEndpoint(), row.document_id);
            const parsed = parseTsvReport(tsv);
            // GET_PAN_EU_OFFER_STATUS lists every ASIN with its Pan-EU state.
            // We filter on `Enrol = 'Y'`, which covers both `Pan-EU status =
            // 'Enrolled'` and `'Enrolment ended'`. "Enrolment ended" ASINs
            // are opted out going forward, but existing pooled inventory sits
            // physically in DE/FR/ES/IT warehouses until it sells through, so
            // Amazon's Health report keeps reporting the same fulfillable
            // number against each marketplace — they must still be deduped.
            const asins = [...new Set(
                parsed.filter(r => (r['Enrol'] || '').trim().toUpperCase() === 'Y')
                      .map(r => (r['ASIN'] || '').trim())
                      .filter(Boolean)
            )];
            await conn.execute(
                `UPDATE amazon_report_jobs SET result_cache=? WHERE id = ?`,
                [JSON.stringify(asins), row.id]
            );
            log.info(`[${label}] Cached ${asins.length} enrolled ASINs`);
        } catch (err) {
            log.error(`[${label}] Pan-EU download/parse failed: ${err.message}`);
            await conn.execute(
                `UPDATE amazon_report_jobs SET status='FAILED', error=? WHERE id = ?`,
                [`Download/parse failed: ${err.message}`, row.id]
            );
        }
    }
}

// ── Phase 3: find & claim one (account, country) unit ready to process ───────
async function claimNextUnit(conn) {
    const claimToken = crypto.randomUUID();

    // A unit is processable when BOTH active_listings and health rows are DONE or FAILED
    // (neither PROCESSED) and not currently claimed by a live run.
    const [candidates] = await conn.query(
        `SELECT al.id  AS active_id,    al.status AS active_status,
                al.document_id AS active_doc,
                al.error AS active_error,
                h.id   AS health_id,    h.status AS health_status,
                h.document_id  AS health_doc,
                h.error  AS health_error,
                al.batch_date, al.account, al.country, al.marketplace_id
         FROM amazon_report_jobs al
         JOIN amazon_report_jobs h
             ON al.batch_date = h.batch_date
            AND al.account   = h.account
            AND al.country   = h.country
            AND h.report_type = ?
         WHERE al.report_type = ?
           AND al.status IN ('DONE','FAILED')
           AND h.status  IN ('DONE','FAILED')
           AND (al.claim_token IS NULL OR al.claimed_at < NOW() - INTERVAL ? MINUTE)
           AND (h.claim_token  IS NULL OR h.claimed_at  < NOW() - INTERVAL ? MINUTE)
         ORDER BY al.batch_date ASC,
                  CASE al.country WHEN 'DE' THEN 0 WHEN 'UK' THEN 1 ELSE 2 END,
                  al.completed_at ASC
         LIMIT 5`,
        [REPORT_TYPES.HEALTH, REPORT_TYPES.ACTIVE_LISTINGS, CLAIM_TIMEOUT_MINUTES, CLAIM_TIMEOUT_MINUTES]
    );

    for (const c of candidates) {
        const [result] = await conn.execute(
            `UPDATE amazon_report_jobs
             SET claim_token=?, claimed_at=NOW()
             WHERE id IN (?, ?)
               AND (claim_token IS NULL OR claimed_at < NOW() - INTERVAL ? MINUTE)`,
            [claimToken, c.active_id, c.health_id, CLAIM_TIMEOUT_MINUTES]
        );
        if (result.affectedRows === 2) {
            return { ...c, claimToken };
        }
        // Race lost — release anything we partially took and move on
        await conn.execute(
            `UPDATE amazon_report_jobs SET claim_token=NULL, claimed_at=NULL
             WHERE claim_token = ?`,
            [claimToken]
        );
    }

    return null;
}

async function releaseClaim(conn, claimToken) {
    await conn.execute(
        `UPDATE amazon_report_jobs SET claim_token=NULL, claimed_at=NULL
         WHERE claim_token = ?`,
        [claimToken]
    );
}

async function markProcessed(conn, ids, error = null) {
    if (ids.length === 0) return;
    const placeholders = ids.map(() => '?').join(',');
    await conn.execute(
        `UPDATE amazon_report_jobs
         SET status='PROCESSED', processed_at=NOW(),
             claim_token=NULL, claimed_at=NULL,
             error = COALESCE(?, error)
         WHERE id IN (${placeholders})`,
        [error, ...ids]
    );
}

// ── Phase 3b: process a claimed unit ──────────────────────────────────────────
async function processUnit(pool, unit) {
    const { account, country, batch_date, active_id, health_id,
            active_status, health_status, active_doc, health_doc,
            active_error, health_error, marketplace_id, claimToken } = unit;
    const label = `${account}/${country}`;
    const dateRan = String(batch_date).slice(0, 10);

    // Phase A: short DB work — preflight checks and fallback paths.
    let panEuAsins;
    {
        const conn = await pool.getConnection();
        try {
            // If either report is FAILED, fall back to copying forward from previous day
            if (active_status === 'FAILED' || health_status === 'FAILED') {
                const err = active_error || health_error || 'Report failed';
                log.warn(`[${label}] Falling back to previous day: ${err}`);
                await copyFailedFromPrevious(conn, country, account);
                await markProcessed(conn, [active_id, health_id], err);
                return { label, fallback: true };
            }

            // Pan-EU is load-bearing for correctness: country_snapshots must only hold
            // DE + UK rows for Pan-EU enrolled ASINs, otherwise FR/ES/IT will each get
            // a duplicate row for the same pool. So if pan_eu is missing or FAILED we
            // must NOT proceed with an empty set — that silently writes duplicates.
            // Instead we fall back to copying the previous day's (correct) data.
            const [peRows] = await conn.query(
                `SELECT status, result_cache FROM amazon_report_jobs
                 WHERE batch_date = ? AND account = ? AND country = 'ALL' AND report_type = ?`,
                [batch_date, account, REPORT_TYPES.PAN_EU]
            );

            if (peRows.length === 0 || peRows[0].status === 'FAILED') {
                const reason = peRows.length === 0 ? 'no pan_eu row' : 'pan_eu FAILED';
                log.warn(`[${label}] ${reason} — falling back to previous day to avoid duplicate Pan-EU rows`);
                await copyFailedFromPrevious(conn, country, account);
                await markProcessed(conn, [active_id, health_id], `pan_eu unavailable: ${reason}`);
                return { label, fallback: true, reason: 'pan_eu_unavailable' };
            }

            if (peRows[0].status !== 'DONE' || !peRows[0].result_cache) {
                log.info(`[${label}] pan_eu not yet cached — deferring unit`);
                await releaseClaim(conn, claimToken);
                return { label, deferred: 'pan_eu_not_ready' };
            }

            // Defer FR/ES/IT until DE is processed — writeSnapshots reads
            // DE's country_snapshot rows to dedupe EFN/Pan-EU pools by FNSKU.
            if (country !== 'DE' && country !== 'UK') {
                const [deJobs] = await conn.query(
                    `SELECT status FROM amazon_report_jobs
                     WHERE batch_date = ? AND account = ? AND country = 'DE'
                       AND report_type IN (?, ?)`,
                    [batch_date, account, REPORT_TYPES.ACTIVE_LISTINGS, REPORT_TYPES.HEALTH]
                );
                const deDone = deJobs.length === 2
                    && deJobs.every(r => r.status === 'PROCESSED' || r.status === 'FAILED');
                if (!deDone) {
                    log.info(`[${label}] DE not yet processed — deferring unit`);
                    await releaseClaim(conn, claimToken);
                    return { label, deferred: 'de_not_ready' };
                }
            }

            try { panEuAsins = new Set(JSON.parse(peRows[0].result_cache) || []); }
            catch (err) {
                log.error(`[${label}] pan_eu cache unreadable — falling back to previous day: ${err.message}`);
                await copyFailedFromPrevious(conn, country, account);
                await markProcessed(conn, [active_id, health_id], `pan_eu cache parse error`);
                return { label, fallback: true, reason: 'pan_eu_cache_unreadable' };
            }
        } finally {
            conn.release();
        }
    }

    // Phase B: long SP-API work with NO DB connection held, so MySQL's
    // wait_timeout can't kill a connection we aren't using.
    const tokenManager = getTokenManager(account);
    const endpoint = getEndpoint();

    log.info(`[${label}] Downloading Active Listings (${active_doc})...`);
    const listingTsv = await downloadReport(tokenManager, endpoint, active_doc);
    const listingRows = parseTsvReport(listingTsv);

    log.info(`[${label}] Downloading Health Report (${health_doc})...`);
    const healthTsv = await downloadReport(tokenManager, endpoint, health_doc);
    const reportRows = parseTsvReport(healthTsv);

    log.info(`[${label}] Fetching API summaries...`);
    const apiSummaries = await fetchInventorySummaries(
        tokenManager, endpoint, marketplace_id, label, account
    );

    // Phase C: fresh connection for writes.
    const conn = await pool.getConnection();
    try {
        const stats = await writeSnapshots(conn, {
            accountName: account, countryCode: country, dateRan,
            listingRows, reportRows, apiSummaries, panEuAsins,
        });

        await markProcessed(conn, [active_id, health_id]);

        log.info(
            `[${label}] Processed — ${stats.upsertCount} ASINs `
            + `(recovered ${stats.recoveredCount}), ${stats.rawCount} raw`
            + (stats.panEuBroadcast ? `, ${stats.panEuBroadcast} pan-EU broadcast` : '')
        );
        return { label, ...stats };
    } finally {
        conn.release();
    }
}

// ── Cleanup: mark pan_eu rows PROCESSED once all dependent country units are done
async function cleanupPanEu(conn) {
    // MySQL disallows referencing the update-target in a subquery, so we
    // select ids first and update in a second statement.
    const [rows] = await conn.query(
        `SELECT pe.id
         FROM amazon_report_jobs pe
         LEFT JOIN amazon_report_jobs sib
             ON sib.batch_date = pe.batch_date
            AND sib.account    = pe.account
            AND sib.report_type IN (?, ?)
            AND sib.status <> 'PROCESSED'
         WHERE pe.report_type = ?
           AND pe.status = 'DONE'
         GROUP BY pe.id
         HAVING COUNT(sib.id) = 0`,
        [REPORT_TYPES.ACTIVE_LISTINGS, REPORT_TYPES.HEALTH, REPORT_TYPES.PAN_EU]
    );
    if (rows.length === 0) return;
    const ids = rows.map(r => r.id);
    const placeholders = ids.map(() => '?').join(',');
    await conn.execute(
        `UPDATE amazon_report_jobs
         SET status = 'PROCESSED', processed_at = NOW()
         WHERE id IN (${placeholders})`,
        ids
    );
}

// ── Handler ───────────────────────────────────────────────────────────────────
// Each phase acquires + releases its own pool connection so long SP-API work
// inside processUnit can run without holding an idle MySQL connection past
// wait_timeout.
async function withConn(pool, fn) {
    const conn = await pool.getConnection();
    try { return await fn(conn); }
    finally { conn.release(); }
}

const handler = async (event = {}) => {
    const pool = getPool();

    const pollStats = await withConn(pool, (conn) => pollPending(conn));
    await withConn(pool, (conn) => cachePanEuResults(conn));

    const unit = await withConn(pool, (conn) => claimNextUnit(conn));
    let processResult = null;
    if (unit) {
        try {
            processResult = await processUnit(pool, unit);
        } catch (err) {
            log.error(`[${unit.account}/${unit.country}] Process failed: ${err.message}`);
            try {
                await withConn(pool, (conn) => releaseClaim(conn, unit.claimToken));
            } catch (releaseErr) {
                log.error(`[${unit.account}/${unit.country}] Release claim failed: ${releaseErr.message}`);
            }
            processResult = { error: err.message };
        }
    }

    await withConn(pool, (conn) => cleanupPanEu(conn));

    // Optional daily backfill pass
    if (event.job === 'backfill') {
        await withConn(pool, (conn) => runBackfill(conn));
    }

    return {
        statusCode: 200,
        body: JSON.stringify({ poll: pollStats, processed: processResult }),
    };
};

exports.handler = handler;

// ── CLI ────────────────────────────────────────────────────────────────────────
// node amazon-stock-snapshot-collector.js
// node amazon-stock-snapshot-collector.js backfill
if (require.main === module) {
    const [,, arg1] = process.argv;
    const event = arg1 === 'backfill' ? { job: 'backfill' } : {};
    handler(event).then(r => {
        log.info('Result:', r.body);
        process.exit(0);
    }).catch(err => {
        log.error('Fatal:', err.message, err.stack);
        process.exit(1);
    });
}
