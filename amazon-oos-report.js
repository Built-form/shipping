'use strict';

const axios = require('axios');
const zlib = require('zlib');
require('dotenv').config();
const log = require('./logger');
const { getPool } = require('./db');
const { REGIONS } = require('./amazon-stock-snapshot');

// ── Accounts ────────────────────────────────────────────────────────────────────
const ACCOUNTS = [
    {
        name: 'JFA',
        clientId: process.env.AMAZON_SP_CLIENT_ID,
        clientSecret: process.env.AMAZON_SP_CLIENT_SECRET,
        refreshToken: process.env.AMAZON_SP_REFRESH_TOKEN,
    },
    {
        name: 'Hangerworld',
        clientId: process.env.AMAZON_SP_CLIENT_ID_HW,
        clientSecret: process.env.AMAZON_SP_CLIENT_SECRET_HW,
        refreshToken: process.env.AMAZON_SP_REFRESH_TOKEN_HW,
    },
];

// ── SP-API token manager ────────────────────────────────────────────────────────
class SPTokenManager {
    constructor(clientId, clientSecret, refreshToken) {
        this._clientId = clientId;
        this._clientSecret = clientSecret;
        this._refreshToken = refreshToken;
        this._token = null;
        this._fetchedAt = 0;
    }
    async getToken() {
        if (this._token && (Date.now() - this._fetchedAt) < 45 * 60 * 1000) return this._token;
        log.info('[SP] Refreshing access token...');
        const res = await axios.post('https://api.amazon.com/auth/o2/token', {
            grant_type: 'refresh_token', refresh_token: this._refreshToken,
            client_id: this._clientId, client_secret: this._clientSecret,
        });
        this._token = res.data.access_token;
        this._fetchedAt = Date.now();
        return this._token;
    }
}

// ── Retry helper (with rate limit backoff) ──────────────────────────────────────
async function withRetry(fn, label) {
    for (let attempt = 1; attempt <= 3; attempt++) {
        try { return await fn(); } catch (err) {
            if (attempt === 3) throw err;
            const retryAfter = err.response?.headers?.['retry-after'];
            const delay = err.response?.status === 429
                ? (retryAfter ? parseInt(retryAfter, 10) : 60) * 1000
                : 5000 * attempt;
            log.warn(`[${label}] Attempt ${attempt}/3 failed (${err.response?.status || err.message}) — retrying in ${delay / 1000}s...`);
            await new Promise(r => setTimeout(r, delay));
        }
    }
}

// ── Reports API (with date range + gzip support) ────────────────────────────────

// Check for an existing in-progress or completed report before creating a new one
async function findExistingReport(tokenManager, endpoint, reportType, marketplaceIds) {
    const token = await tokenManager.getToken();
    const params = new URLSearchParams();
    params.append('reportTypes', reportType);
    params.append('processingStatuses', 'IN_QUEUE,IN_PROGRESS,DONE');
    for (const id of marketplaceIds) params.append('marketplaceIds', id);
    params.append('pageSize', '1');
    try {
        const res = await axios.get(`${endpoint}/reports/2021-06-30/reports?${params}`, {
            headers: { 'x-amz-access-token': token },
        });
        const reports = res.data.reports || [];
        if (reports.length > 0) return reports[0];
    } catch (err) {
        log.warn(`[findExistingReport] Could not check existing reports: ${err.message}`);
    }
    return null;
}

async function requestReport(tokenManager, endpoint, reportType, marketplaceIds, opts = {}) {
    const existing = await findExistingReport(tokenManager, endpoint, reportType, marketplaceIds);
    if (existing) {
        const st = existing.processingStatus;
        if (st === 'DONE') {
            // Reuse DONE reports if created in the last 2 hours (same run / recent retry)
            const createdAt = new Date(existing.createdTime).getTime();
            if (Date.now() - createdAt < 2 * 60 * 60 * 1000) {
                log.info(`[requestReport] Reusing recent DONE report ${existing.reportId}`);
                return { reportId: existing.reportId, documentId: existing.reportDocumentId };
            }
        } else {
            log.info(`[requestReport] Reusing in-progress report ${existing.reportId} (${st})`);
            return { reportId: existing.reportId, documentId: null };
        }
    }

    const token = await tokenManager.getToken();
    const body = { reportType, marketplaceIds };
    if (opts.dataStartTime) body.dataStartTime = opts.dataStartTime;
    if (opts.dataEndTime) body.dataEndTime = opts.dataEndTime;
    const res = await withRetry(() => axios.post(`${endpoint}/reports/2021-06-30/reports`, body, {
        headers: { 'x-amz-access-token': token, 'Content-Type': 'application/json' },
    }), `requestReport ${reportType}`);
    return { reportId: res.data.reportId, documentId: null };
}

async function downloadReport(tokenManager, endpoint, reportDocumentId) {
    const token = await tokenManager.getToken();
    const meta = await axios.get(`${endpoint}/reports/2021-06-30/documents/${reportDocumentId}`, {
        headers: { 'x-amz-access-token': token },
    });
    const isGzip = meta.data.compressionAlgorithm === 'GZIP';
    const doc = await axios.get(meta.data.url, {
        responseType: isGzip ? 'arraybuffer' : 'text',
    });
    return isGzip
        ? zlib.gunzipSync(Buffer.from(doc.data)).toString('utf-8')
        : doc.data;
}

function parseTsvReport(tsv) {
    // Strip BOM if present (Amazon reports sometimes include it)
    const clean = tsv.replace(/^\uFEFF/, '');
    const lines = clean.trim().split('\n');
    if (lines.length < 2) return [];
    // Strip surrounding double quotes from headers (ledger reports quote them)
    const headers = lines[0].split('\t').map(h => h.trim().replace(/^"|"$/g, ''));
    log.info(`[TSV] Headers: ${JSON.stringify(headers)}`);
    return lines.slice(1).map(line => {
        const vals = line.split('\t');
        const row = {};
        headers.forEach((h, i) => { row[h] = (vals[i] || '').trim().replace(/^"|"$/g, ''); });
        return row;
    });
}

// ── Parse date from ledger row ────────────────────────────────────────────────
function parseDate(rawDate) {
    const slashDate = rawDate.match(/^(\d{1,2})[\/\-.](\d{1,2})[\/\-.](\d{4})$/);
    if (slashDate) {
        const [, a, b, year] = slashDate;
        let month, day;
        if (parseInt(b, 10) > 12) { month = a; day = b; }
        else if (parseInt(a, 10) > 12) { day = a; month = b; }
        else { month = a; day = b; }
        return new Date(`${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}T00:00:00Z`);
    }
    return new Date(rawDate);
}

// ── Build daily ending balances from ledger detail events ───────────────────────
function buildDailyBalances(rows, code) {
    // "Ending Warehouse Balance" is per-FNSKU, not per-ASIN.
    // Track per FNSKU first, then sum across FNSKUs for the ASIN total.

    // Step 1: collect ending balance per FNSKU per date (last event wins)
    const fnskuDaily = {}; // { "asin:fnsku": { date: endingBalance } }
    const fnskuToAsin = {}; // { "asin:fnsku": asin }

    for (const row of rows) {
        const asin = row['ASIN'] || '';
        const fnsku = row['FNSKU'] || '';
        if (!asin) continue;

        // Skip B-prefix FNSKUs outside DE/UK (Pan-European duplicates)
        if (fnsku.toUpperCase().startsWith('B') && code !== 'DE' && code !== 'UK') continue;

        const rawDate = (row['Date'] || row['date'] || '').trim();
        if (!rawDate) continue;
        const dateObj = parseDate(rawDate);
        if (!dateObj || isNaN(dateObj.getTime())) {
            log.warn(`[Ledger] Unparseable date: "${rawDate}" for ASIN ${asin}`);
            continue;
        }
        const date = dateObj.toISOString().slice(0, 10);

        const disposition = (row['Disposition'] || '').toLowerCase();
        if (disposition && disposition !== 'sellable') continue;

        const endBal = parseInt(row['Ending Warehouse Balance'] || '0', 10);
        const key = `${asin}:${fnsku}`;
        fnskuToAsin[key] = asin;
        if (!fnskuDaily[key]) fnskuDaily[key] = {};
        fnskuDaily[key][date] = endBal; // last event per FNSKU+date wins
    }

    // Step 2: sum across FNSKUs to get per-ASIN daily totals
    const dailyStock = {}; // { asin: { date: totalBalance } }
    for (const [key, dateMap] of Object.entries(fnskuDaily)) {
        const asin = fnskuToAsin[key];
        if (!dailyStock[asin]) dailyStock[asin] = {};
        for (const [date, balance] of Object.entries(dateMap)) {
            dailyStock[asin][date] = (dailyStock[asin][date] || 0) + balance;
        }
    }

    return dailyStock;
}

// ── Shared: build task list with incremental date ranges ──────────────────────
async function buildTasks(pool) {
    const daysBack = parseInt(process.env.OOS_DAYS_BACK || '90', 10);
    const overlapDays = 7;
    const fullStartDate = new Date(Date.now() - daysBack * 86400000).toISOString();

    const [latestRows] = await pool.execute(
        `SELECT country, MAX(date) as latest_date FROM amazon_oos_daily GROUP BY country`
    );
    const latestByCountry = {};
    for (const row of latestRows) {
        latestByCountry[row.country] = row.latest_date;
    }

    const tasks = [];
    for (const account of ACCOUNTS) {
        if (!account.clientId || !account.clientSecret || !account.refreshToken) {
            log.warn(`[${account.name}] SP-API credentials not configured — skipping`);
            continue;
        }
        const tokenManager = new SPTokenManager(account.clientId, account.clientSecret, account.refreshToken);
        for (const [, { endpoint, marketplaces }] of Object.entries(REGIONS)) {
            for (const [code, marketplaceId] of Object.entries(marketplaces)) {
                let startDate = fullStartDate;
                if (latestByCountry[code]) {
                    const latest = new Date(latestByCountry[code]);
                    const incrementalStart = new Date(latest.getTime() - overlapDays * 86400000).toISOString();
                    if (incrementalStart > fullStartDate) startDate = incrementalStart;
                }
                tasks.push({ account, tokenManager, endpoint, code, marketplaceId, startDate });
            }
        }
    }

    // Shuffle so a consistently slow marketplace doesn't always block others on retry
    for (let i = tasks.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [tasks[i], tasks[j]] = [tasks[j], tasks[i]];
    }

    return tasks;
}

// ── Phase 1: Request reports ──────────────────────────────────────────────────
// Fires off report requests for all account/marketplace combos, stores report
// IDs in DB, returns immediately. Designed to run fast (< 30s).
const requestHandler = async () => {
    const pool = getPool();
    const endDate = new Date().toISOString();

    await pool.execute(`
        CREATE TABLE IF NOT EXISTS amazon_oos_daily (
            asin VARCHAR(20) NOT NULL,
            country VARCHAR(5) NOT NULL,
            account VARCHAR(20) NOT NULL DEFAULT '',
            date DATE NOT NULL,
            ending_balance INT NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (asin, country, date),
            INDEX idx_country_oos (country, ending_balance, date),
            INDEX idx_account_country (account, country)
        )
    `);

    await pool.execute(`
        CREATE TABLE IF NOT EXISTS amazon_oos_report_jobs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            account VARCHAR(20) NOT NULL,
            country VARCHAR(5) NOT NULL,
            marketplace_id VARCHAR(20) NOT NULL,
            report_id VARCHAR(50) NOT NULL,
            start_date VARCHAR(30) NOT NULL,
            end_date VARCHAR(30) NOT NULL,
            status ENUM('pending', 'done', 'failed') NOT NULL DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_status (status)
        )
    `);

    const tasks = await buildTasks(pool);
    let requested = 0;

    const results = await Promise.allSettled(tasks.map(async ({ account, tokenManager, endpoint, code, marketplaceId, startDate }) => {
        const label = `${account.name}/${code}`;
        const daysRequested = Math.round((Date.now() - new Date(startDate).getTime()) / 86400000);
        log.info(`[${label}] Requesting report (${startDate.slice(0, 10)} → now, ~${daysRequested}d)...`);

        const { reportId } = await requestReport(
            tokenManager, endpoint,
            'GET_LEDGER_DETAIL_VIEW_DATA',
            [marketplaceId],
            { dataStartTime: startDate, dataEndTime: endDate }
        );

        await pool.execute(
            `INSERT INTO amazon_oos_report_jobs (account, country, marketplace_id, report_id, start_date, end_date)
             VALUES (?, ?, ?, ?, ?, ?)`,
            [account.name, code, marketplaceId, reportId, startDate, endDate]
        );

        log.info(`[${label}] Report ${reportId} requested`);
        return reportId;
    }));

    for (let i = 0; i < results.length; i++) {
        if (results[i].status === 'fulfilled') {
            requested++;
        } else {
            const label = `${tasks[i].account.name}/${tasks[i].code}`;
            log.error(`[${label}] Request failed: ${results[i].reason?.message || results[i].reason}`);
        }
    }

    log.info(`=== Request phase done: ${requested} reports requested ===`);
    return { statusCode: 200, body: JSON.stringify({ requested }) };
};

// ── Phase 2: Collect reports ──────────────────────────────────────────────────
// Checks pending report jobs, downloads any that are DONE, processes + saves.
// Can be called multiple times — only processes pending jobs.
const collectHandler = async () => {
    const pool = getPool();

    const [jobs] = await pool.execute(
        `SELECT * FROM amazon_oos_report_jobs WHERE status = 'pending' ORDER BY created_at`
    );

    if (jobs.length === 0) {
        log.info('No pending report jobs to collect');
        return { statusCode: 200, body: JSON.stringify({ collected: 0, pending: 0 }) };
    }

    log.info(`${jobs.length} pending report jobs to check`);

    // Build token managers by account name
    const tokenManagers = {};
    for (const account of ACCOUNTS) {
        if (account.clientId && account.clientSecret && account.refreshToken) {
            tokenManagers[account.name] = new SPTokenManager(account.clientId, account.clientSecret, account.refreshToken);
        }
    }

    // Map marketplace IDs to their region endpoint
    const marketplaceEndpoints = {};
    for (const [, { endpoint, marketplaces }] of Object.entries(REGIONS)) {
        for (const [, marketplaceId] of Object.entries(marketplaces)) {
            marketplaceEndpoints[marketplaceId] = endpoint;
        }
    }

    const dateRe = /^\d{4}-\d{2}-\d{2}$/;
    let collected = 0;
    let pending = 0;
    let failed = 0;
    let totalRows = 0;
    let totalOosDays = 0;

    const results = await Promise.allSettled(jobs.map(async (job) => {
        const label = `${job.account}/${job.country}`;
        const tokenManager = tokenManagers[job.account];
        const endpoint = marketplaceEndpoints[job.marketplace_id];

        if (!tokenManager || !endpoint) {
            log.error(`[${label}] No credentials or endpoint for job ${job.id} — marking failed`);
            await pool.execute(`UPDATE amazon_oos_report_jobs SET status = 'failed' WHERE id = ?`, [job.id]);
            return { status: 'failed' };
        }

        // Check report status (single check, no long polling)
        const token = await tokenManager.getToken();
        const res = await axios.get(`${endpoint}/reports/2021-06-30/reports/${job.report_id}`, {
            headers: { 'x-amz-access-token': token },
        });
        const reportStatus = res.data.processingStatus;

        if (reportStatus === 'CANCELLED' || reportStatus === 'FATAL') {
            log.error(`[${label}] Report ${job.report_id} ${reportStatus} — marking failed`);
            await pool.execute(`UPDATE amazon_oos_report_jobs SET status = 'failed' WHERE id = ?`, [job.id]);
            return { status: 'failed' };
        }

        if (reportStatus !== 'DONE') {
            log.info(`[${label}] Report ${job.report_id} still ${reportStatus}`);
            return { status: 'pending' };
        }

        // Download and process
        log.info(`[${label}] Report ${job.report_id} DONE — downloading...`);
        const docId = res.data.reportDocumentId;
        const tsv = await downloadReport(tokenManager, endpoint, docId);
        const rows = parseTsvReport(tsv);
        log.info(`[${label}] ${rows.length} ledger events`);

        if (rows.length === 0) {
            await pool.execute(`UPDATE amazon_oos_report_jobs SET status = 'done' WHERE id = ?`, [job.id]);
            return { status: 'done', days: 0, oosDays: 0 };
        }

        const dailyStock = buildDailyBalances(rows, job.country);
        const asinCount = Object.keys(dailyStock).length;
        log.info(`[${label}] ${asinCount} ASINs with daily stock data`);

        // Batch insert
        const BATCH_SIZE = 500;
        const batch = [];
        let saved = 0;
        let oosDays = 0;

        async function flushBatch() {
            if (batch.length === 0) return;
            const placeholders = batch.map(() => '(?, ?, ?, ?, ?)').join(', ');
            const params = batch.flat();
            await pool.execute(
                `INSERT INTO amazon_oos_daily (asin, country, account, date, ending_balance)
                 VALUES ${placeholders}
                 ON DUPLICATE KEY UPDATE ending_balance = VALUES(ending_balance), account = VALUES(account)`,
                params
            );
            saved += batch.length;
            batch.length = 0;
        }

        for (const [asin, dateMap] of Object.entries(dailyStock)) {
            for (const [date, balance] of Object.entries(dateMap)) {
                if (!dateRe.test(date)) continue;
                if (balance <= 0) oosDays++;
                batch.push([asin, job.country, job.account, date, balance]);
                if (batch.length >= BATCH_SIZE) await flushBatch();
            }
        }
        await flushBatch();

        await pool.execute(`UPDATE amazon_oos_report_jobs SET status = 'done' WHERE id = ?`, [job.id]);
        log.info(`[${label}] Saved ${saved} daily rows (${oosDays} OOS days)`);
        return { status: 'done', days: saved, oosDays };
    }));

    for (const r of results) {
        if (r.status === 'rejected') {
            failed++;
            log.error(`Job failed: ${r.reason?.message || r.reason}`);
        } else if (r.value.status === 'done') {
            collected++;
            totalRows += r.value.days || 0;
            totalOosDays += r.value.oosDays || 0;
        } else if (r.value.status === 'pending') {
            pending++;
        } else {
            failed++;
        }
    }

    log.info('');
    log.info(`=== Collect phase: ${collected} done, ${pending} still pending, ${failed} failed — ${totalRows} rows saved, ${totalOosDays} OOS days ===`);
    return { statusCode: 200, body: JSON.stringify({ collected, pending, failed, totalRows, totalOosDays }) };
};

// ── Combined handler (for local dev / single-shot) ────────────────────────────
const handler = async (event) => {
    const phase = event?.phase || process.env.OOS_PHASE || 'both';
    if (phase === 'request') return requestHandler();
    if (phase === 'collect') return collectHandler();
    // 'both' — request then poll+collect in one go (original behaviour for local dev)
    await requestHandler();
    return collectHandler();
};

exports.handler = handler;
exports.requestHandler = requestHandler;
exports.collectHandler = collectHandler;

if (require.main === module) {
    const phase = process.argv[2] || 'both'; // node amazon-oos-report.js [request|collect|both]
    handler({ phase }).then(r => {
        log.info('Result:', JSON.stringify(r, null, 2));
        process.exit(0);
    }).catch(err => {
        log.error('Fatal:', err.message, err.stack);
        process.exit(1);
    });
}
