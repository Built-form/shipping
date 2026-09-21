// ── Email receipt confirmation — PUBLIC API ─────────────────────────────────
// A deliberately tiny, isolated Lambda that serves the single "Confirm receipt"
// link embedded in every outbound supplier/forwarder email (the emails are sent
// from orders.js). It is SEPARATE from the authed orders.js and from the
// supplier-portal Lambda on purpose: the public attack surface is just the two
// routes below, and none of the authed routes are reachable through it.
//
// The token in the path IS the capability (unguessable, minted per email) — no
// login and no access code. Two-step so confirmation is trustworthy: the GET
// (the emailed link) records a weak "open" and shows a Confirm button; the POST
// (the button press) records the strong "confirmed" — a link-scanner fetches
// the GET but won't submit the form. All responses are HTML (browser nav).
//
// Routes (unauthenticated — no JWT authorizer in serverless.yml):
//   GET  /api/v1/confirm-receipt/{token}   → open + render confirm button
//   POST /api/v1/confirm-receipt/{token}   → confirm + redirect/thank-you
const serverless = require('serverless-http');
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { getPool } = require('../db');
const log = require('../lib/logger');
const {
    CONFIRM_PATH,
    markReceiptOpened, markReceiptConfirmed,
    renderConfirmButtonHtml, renderThankYouHtml,
} = require('../lib/email-receipt');

const app = express();
app.use(cors());

const pool = getPool();

// email_receipts is schema applied at deploy time from src/db/migrate/: this
// Lambda runs no DDL. Kept so the routes' awaits stand.
const schemaReady = Promise.resolve();

async function withConnection(fn) {
    const conn = await pool.getConnection();
    try {
        return await fn(conn);
    } finally {
        conn.release();
    }
}

// Optional static confirmation page. When EMAIL_RECEIPT_REDIRECT_URL is set
// (e.g. an S3-hosted "we've recorded it" page), a confirmation 302-redirects
// there instead of rendering the built-in thank-you page. Left blank → render
// the built-in page, so the endpoint works before a static page exists.
const REDIRECT_URL = (process.env.EMAIL_RECEIPT_REDIRECT_URL || '').trim();

// ── GET /api/v1/confirm-receipt/:token ──────────────────────────────────────
// The link target in the email. Records a (weak) OPEN and renders a page with a
// single "Confirm receipt" button that POSTs back here. It deliberately does NOT
// record a confirmation — link-scanners fetch GETs, so a GET must not confirm.
// Unknown/expired tokens get a neutral "link not recognised" page.
app.get('/api/v1/confirm-receipt/:token', async (req, res) => {
    res.set('Content-Type', 'text/html; charset=utf-8');
    res.set('Cache-Control', 'no-store');
    try {
        await schemaReady;
        const token = req.params.token;
        const receipt = await withConnection((conn) => markReceiptOpened(conn, token));
        const actionUrl = `${CONFIRM_PATH}/${encodeURIComponent(token)}`;
        res.status(receipt ? 200 : 404).send(renderConfirmButtonHtml(receipt, actionUrl));
    } catch (error) {
        log.error('[GET /confirm-receipt]', error);
        res.status(500).send(renderThankYouHtml(null));
    }
});

// ── POST /api/v1/confirm-receipt/:token ─────────────────────────────────────
// The button press from the page above — the STRONG confirmation signal.
// Records confirmed_at (idempotent), then redirects to the static confirmation
// page if configured, else renders the built-in thank-you page. Unknown tokens
// get the neutral page and are never redirected (nothing was recorded).
app.post('/api/v1/confirm-receipt/:token', async (req, res) => {
    res.set('Cache-Control', 'no-store');
    try {
        await schemaReady;
        const receipt = await withConnection((conn) => markReceiptConfirmed(conn, req.params.token, {
            userAgent: req.headers['user-agent'],
            ip: req.requestContext?.http?.sourceIp || req.headers['x-forwarded-for'],
        }));
        if (receipt && REDIRECT_URL) {
            return res.redirect(302, REDIRECT_URL);
        }
        res.set('Content-Type', 'text/html; charset=utf-8');
        res.status(receipt ? 200 : 404).send(renderThankYouHtml(receipt));
    } catch (error) {
        log.error('[POST /confirm-receipt]', error);
        res.set('Content-Type', 'text/html; charset=utf-8');
        res.status(500).send(renderThankYouHtml(null));
    }
});

// ── Serverless export ───────────────────────────────────────────────────────
const serverlessApp = serverless(app);

module.exports.handler = async (event, context) => {
    context.callbackWaitsForEmptyEventLoop = false;
    return await serverlessApp(event, context);
};

if (require.main === module) {
    const PORT = process.env.EMAIL_RECEIPT_PORT || 3003;
    app.listen(PORT, () => {
        log.info(`Email receipt API running on http://localhost:${PORT}`);
    });
}
