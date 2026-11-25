'use strict';

// "Send to supplier" on a recorded payment (docs/payment-proof-send.md):
//   GET  /api/v1/supplier-payments/:id/notices
//        what the send dialog shows: whether the payment can go out (as a
//        link to the supplier portal, the only way) and why not, its
//        proofs, and each part of it (deposit / balance) with the wording to
//        start from, its state and its sends. Any signed-in user.
//   POST /api/v1/supplier-payments/:id/notices/:kind/email
//        sends one part: { sendKey, to, subject, body, delivery, previewToken }.
//        Admin, standard and accountant users.
//   POST /api/v1/supplier-payment-sends/:sendId/resolve
//        { outcome: 'sent' | 'not_sent' } for a send whose outcome is unknown
//        (Front did not answer). The same roles.
// Every refusal carries `code` and `allowedNext`; the frontend shows it, never
// swallows it. The rules are in src/lib/payment-notices.js, the send itself in
// src/services/payment-notices.js.
//
// The roles are checked here and in the service: there is no role middleware
// (orders.js only sets req.userType), and ShipLine's own guard is for display.
//
// Registered from orders.js; authed through the greedy /api/v1/{proxy+} route.

const N = require('../lib/payment-notices');
const S = require('./payment-notices');
const { sendEmail: defaultSendEmail } = require('./email-send');
const { apiBaseUrlFromReq } = require('../lib/email-receipt');

const ER_NO_SUCH_TABLE = 1146;
const NOT_MIGRATED = {
    status: 503,
    body: { error: 'Sending a payment to a supplier is not set up on this server yet: its send log has not been migrated.', code: 'NOT_MIGRATED', allowedNext: [] },
};

function registerPaymentNoticeRoutes(app, deps) {
    const { withConnection, ready, lockSupplierPaymentTargets, getEmailTemplate, recordAudit, log } = deps;
    const sendEmail = deps.sendEmail || defaultSendEmail;

    // No file is read here: a payment goes out as a link to the portal, never with its
    // proof attached (the user, 2026-11-17).
    const store = S.createStore({ withConnection, lockSupplierPaymentTargets, getEmailTemplate, recordAudit });

    const handle = (site, fn) => async (req, res) => {
        try {
            await ready();
            const r = await fn(req);
            res.status(r.status).json(r.body);
        } catch (error) {
            if (error && error.errno === ER_NO_SUCH_TABLE) return res.status(NOT_MIGRATED.status).json(NOT_MIGRATED.body);
            log.error(`[${site}]`, error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    };
    const actorOf = req => ({ email: req.userEmail || null, userType: req.userType });

    app.get('/api/v1/supplier-payments/:id(\\d+)/notices', handle('GET /supplier-payments/:id/notices', async req => {
        const id = Number(req.params.id);
        const loaded = await store.forPreview(id);
        if (!loaded) return { status: 404, body: { error: `Supplier payment ${id} not found.`, code: 'NOT_FOUND' } };
        if (!loaded.migrated) return NOT_MIGRATED;
        const preview = S.previewOf(loaded.source, loaded.delivery, loaded.sends);
        // A forwarder's payment: who opened its proof and who acknowledged it
        // come from the portal's own forwarder events, for the forwarder it is
        // linked to now. Never from payment_views: an opening by a supplier's
        // user, from before the payment was a forwarder's, proves nothing here.
        if (preview.audience === N.FORWARDER_KIND) {
            const evidence = await store.forwarderEvidence(id, preview.forwarder ? preview.forwarder.id : null);
            return { status: 200, body: { ...preview, views: [], ...evidence, canSend: N.maySend(req.userType) } };
        }
        const views = await store.views(id);
        return {
            status: 200,
            body: { ...preview, views, canSend: N.maySend(req.userType) },
        };
    }));

    app.post('/api/v1/supplier-payments/:id(\\d+)/notices/:kind/email', handle('POST /supplier-payments/:id/notices/:kind/email', req =>
        S.sendNotice(
            { store, sendEmail, apiBaseUrl: apiBaseUrlFromReq(req) },
            { paymentId: Number(req.params.id), kind: req.params.kind, actor: actorOf(req), body: req.body || {} }
        )));

    app.post('/api/v1/supplier-payment-sends/:sendId(\\d+)/resolve', handle('POST /supplier-payment-sends/:sendId/resolve', req =>
        S.resolveSend({ store }, { sendId: Number(req.params.sendId), outcome: (req.body || {}).outcome, actor: actorOf(req) })));
}

module.exports = { registerPaymentNoticeRoutes };
