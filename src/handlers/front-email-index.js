'use strict';

// Scheduled email→order indexer. Walks recent Front conversations (ALL senders),
// matches known order identifiers (PO / container / AWB / SKU / lot) in the
// thread text, and writes link rows into `order_emails` so the UI can answer
// "show every email about this order" (GET /api/v1/orders/:id/emails). A gated +
// capped Gemini Flash fallback handles order-looking mail the rules can't match.
// See src/services/front-email-index.js. Runs hourly (serverless.yml).
//
// Deterministic core is essentially free (a Front search + in-memory matching),
// so empty runs are a few seconds. Idempotent: per-conversation dedup ledger
// (front_email_index) + an incremental watermark mean re-runs only re-walk
// threads with new activity.
//
// Event options (all optional):
//   { sinceDays }   force a fixed look-back window (manual backfill)
//   { dryRun }      match + log but don't write
//   { force }       re-scan conversations already in the ledger
//   { verbose }     per-conversation log lines
//   { useGemini }   set false to run deterministic-only
//   { maxGemini }   per-run Flash call cap (cost guard)
//   { geminiConfMin, geminiModel }

require('dotenv').config();
const log = require('../lib/logger');
const { getPool } = require('../db');
const { indexEmailsFromFront } = require('../services/front-email-index');

async function handler(event = {}) {
    if (!process.env.FRONT_API_TOKEN) throw new Error('FRONT_API_TOKEN is not configured');

    const sinceDays = Number(event && event.sinceDays) > 0 ? Number(event.sinceDays) : undefined;
    const pool = getPool();
    const conn = await pool.getConnection();
    try {
        const res = await indexEmailsFromFront(conn, {
            sinceDays,
            dryRun: !!(event && event.dryRun),
            force: !!(event && event.force),
            verbose: !!(event && event.verbose),
            useGemini: event && event.useGemini === false ? false : true,
            ...(Number(event && event.maxGemini) >= 0 ? { maxGemini: Number(event.maxGemini) } : {}),
            ...(Number(event && event.geminiConfMin) >= 0 ? { geminiConfMin: Number(event.geminiConfMin) } : {}),
            ...(event && event.geminiModel ? { geminiModel: event.geminiModel } : {}),
        });
        log.info('[front-email-index] done', {
            window: res.window, scanned: res.scanned, matched: res.matched,
            links: res.linksUpserted, byTier: res.byTier, bySource: res.bySource,
            geminiCalls: res.geminiCalls, geminiMatched: res.geminiMatched,
            geminiBudgetSkipped: res.geminiBudgetSkipped,
            ambiguous: res.ambiguous, failed: res.failed,
            fetchFailed: res.fetchFailed, processFailed: res.processFailed,
            capped: res.capped, watermarkAdvanced: res.watermarkAdvanced,
        });
        return { statusCode: 200, body: JSON.stringify(res) };
    } catch (err) {
        log.error('[front-email-index] fatal', err);
        throw err;
    } finally {
        conn.release();
    }
}

exports.handler = handler;
