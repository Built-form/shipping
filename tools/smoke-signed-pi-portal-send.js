'use strict';

// End-to-end check of the signed-PI send (docs/po-portal-send.md, "Signed
// PIs") against the TEST database, both halves for real, as
// tools/smoke-po-portal-preapproval.js does for the PO send:
//
//   1. shipping: the real POST /api/v1/purchase-order-signed-pis/:id/email
//      route (the Express app in-process, auth bypassed as local@dev): no
//      attachment, the portal block, the recipient pre-approved, the send row,
//      a 'portal' email receipt;
//   2. portal: the real jportal `supplier` Lambda handler in-process
//      (../portal) with fabricated Cognito claims: the recipient signs in,
//      sees the signed PI on the order, opens it; then shipping lists the view
//      and marks the email confirmed.
//
//   node tools/smoke-signed-pi-portal-send.js
//
// NOTHING LEAVES THIS MACHINE: global fetch is replaced, the Front call is
// answered by a stub (recording what would have been sent) and any other
// outbound request throws. Everything it writes on TEST (portal users and
// their access/audit rows, views, send rows, receipts, audit_log rows) is
// tagged and deleted at the end, pass or fail. Refuses to run unless .env.test
// points at explorer-test.
//
// Limitation: both halves use shipping's TEST `admin` login, so a missing
// jportal_app column grant on the portal side would not show here
// (portal/tools/smoke-sent-documents.js runs as that login).

const path = require('path');
const crypto = require('crypto');
const { pathToFileURL } = require('url');

const ROOT = path.join(__dirname, '..');
const PORTAL = process.env.PORTAL_REPO || path.join(ROOT, '..', 'portal');

require('dotenv').config({ path: path.join(ROOT, '.env.test'), override: true });
const host = String(process.env.DB_HOST || '').toLowerCase();
if (!host.includes('explorer-test')) {
    console.error(`[smoke] REFUSED: .env.test DB_HOST is "${host}", not explorer-test.`);
    process.exit(1);
}
process.env.IS_OFFLINE = '1';
process.env.LOCAL_USER_TYPE = 'admin';
process.env.FRONT_API_TOKEN = 'smoke-not-a-token';
process.env.FRONT_CHANNEL_ID = 'cha_smoke';
process.env.SUPPLIER_PORTAL_URL = process.env.SUPPLIER_PORTAL_URL || 'https://jportal-test.built-form.co.uk';
const JP = '`' + (process.env.JPORTAL_DB_SCHEMA || 'jportal') + '`';

// ── network: Front stubbed, everything else external refused ───────────────
const realFetch = global.fetch;
const frontCalls = [];
global.fetch = async (url, init) => {
    const u = String(url);
    if (u.startsWith('http://127.0.0.1:')) return realFetch(url, init);
    if (u.startsWith('https://api2.frontapp.com/')) {
        const f = init.body;
        const n = frontCalls.push({
            to: f.getAll('to[]'), subject: f.get('subject'), body: String(f.get('body')),
            attachments: f.getAll('attachments[]').length,
        });
        return new Response(JSON.stringify({
            message_uid: `smoke_msg_${n}`,
            _links: { related: { conversation: `https://api2.frontapp.com/conversations/cnv_smoke_${n}` } },
        }), { status: 202, headers: { 'content-type': 'application/json' } });
    }
    throw new Error(`[smoke] blocked outbound fetch to ${u}`);
};

const mysql = require('mysql2/promise');
const { getPool, closePool } = require('../src/db');
const { syncPortalReceipts } = require('../src/lib/email-receipt');

let pass = 0, fail = 0;
const failures = [];
function check(label, cond, detail) {
    if (cond) { pass++; console.log(`  PASS  ${label}`); }
    else {
        fail++; failures.push(label);
        console.log(`  FAIL  ${label}${detail !== undefined ? `\n        got: ${typeof detail === 'string' ? detail : JSON.stringify(detail)}` : ''}`);
    }
}

const TAG = `smk${Date.now().toString(36)}`;
const DOM = `${TAG}.invalid`;
const addr = (who) => `${who}-${TAG}@${DOM}`;
const sendIds = [];

async function q(sql, params) { return (await getPool().query(sql, params))[0]; }

let baseUrl;
async function api(method, url, body) {
    const res = await realFetch(`${baseUrl}${url}`, {
        method, headers: { 'content-type': 'application/json' }, body: body ? JSON.stringify(body) : undefined,
    });
    const parsed = await res.json().catch(() => null);
    if (parsed && parsed.sendId) sendIds.push(parsed.sendId);
    return { status: res.status, body: parsed };
}

let portal;
let seq = 0;
async function portalCall(claims, httpMethod, resource, pathParameters = null) {
    const res = await portal.handler({
        httpMethod, resource, pathParameters, queryStringParameters: null,
        headers: { 'user-agent': 'smoke-signed-pi-portal-send', 'x-requested-with': 'XMLHttpRequest' },
        requestContext: { requestId: `${TAG}-${++seq}`, authorizer: { claims } },
        body: null, isBase64Encoded: false,
    }, {});
    let parsed = null;
    try { parsed = JSON.parse(res.body); } catch { /* left null */ }
    return { status: res.statusCode, data: parsed?.data, code: parsed?.code ?? parsed?.error?.code, raw: String(res.body).slice(0, 300) };
}

async function main() {
    console.log(`TEST DB ${process.env.DB_HOST} / ${process.env.DB_NAME} + ${JP}   tag ${TAG}\n`);

    const { app } = require('../src/handlers/orders');
    const server = await new Promise(r => { const s = app.listen(0, '127.0.0.1', () => r(s)); });
    baseUrl = `http://127.0.0.1:${server.address().port}`;

    // The portal, on its own pool into the jportal schema (the env DB_NAME is shipping's).
    const db = await import(pathToFileURL(path.join(PORTAL, 'src', 'db.js')).href);
    const portalPool = mysql.createPool({
        host: process.env.DB_HOST, user: process.env.DB_USER, password: process.env.DB_PASSWORD,
        port: Number(process.env.DB_PORT) || 3306, database: JP.replace(/`/g, ''),
        ssl: { rejectUnauthorized: false }, connectionLimit: 1, dateStrings: ['DATE'], timezone: 'Z', stringifyObjects: true,
    });
    db._setPoolForTests(portalPool);
    portal = await import(pathToFileURL(path.join(PORTAL, 'src', 'handlers', 'supplier.js')).href);

    try {
        // The newest signed PI of a PO whose supplier is named exactly as a live supplier.
        const [spi] = await q(
            `SELECT d.id, d.purchase_order_id, d.filename, po.po_number, po.supplier
               FROM purchase_order_signed_pis d
               JOIN purchase_orders po ON po.id = d.purchase_order_id AND po.deleted_at IS NULL
              WHERE d.deleted_at IS NULL
                AND EXISTS (SELECT 1 FROM suppliers s WHERE s.deleted_at IS NULL
                             AND CONVERT(s.name USING utf8mb4) COLLATE utf8mb4_0900_bin
                               = CONVERT(po.supplier USING utf8mb4) COLLATE utf8mb4_0900_bin)
              ORDER BY d.id DESC LIMIT 1`);
        if (!spi) throw new Error('no signed PI on TEST whose PO supplier is named exactly');
        console.log(`Fixture: signed PI ${spi.id} "${spi.filename}" of PO ${spi.po_number} (id ${spi.purchase_order_id}) "${spi.supplier}"\n`);
        const portalUrl = `${process.env.SUPPLIER_PORTAL_URL.replace(/\/+$/, '')}/orders/${spi.purchase_order_id}?tab=pdfs`;

        // ── 1. the send ──────────────────────────────────────────────────────
        console.log('1. Send the signed PI');
        const bad = await api('POST', `/api/v1/purchase-order-signed-pis/${spi.id}/email`, { to: ['not an address', addr('x')] });
        check('a piece that is not an address: 400, named, nothing sent', bad.status === 400 && Array.isArray(bad.body?.invalid) && frontCalls.length === 0, bad);

        const to = `Li Wei <New-${TAG}@${DOM.toUpperCase()}>; ${addr('second')}`;
        const sent = await api('POST', `/api/v1/purchase-order-signed-pis/${spi.id}/email`, { to: [to] });
        check('200 with the send, the portal address and the portal access', sent.status === 200 && sent.body?.ok === true
            && sent.body?.portalUrl === portalUrl && sent.body?.portalAccess?.status === 'ok', sent);
        check('both recipients invited and linked to the PO\'s supplier',
            (sent.body?.portalAccess?.users || []).length === 2
            && sent.body.portalAccess.users.every(u => u.outcome === 'invited' && u.suppliersAdded >= 1)
            && sent.body.portalAccess.poSupplierMatched === true, sent.body?.portalAccess);
        const mail = frontCalls[0] || { body: '', to: [] };
        check('one email, to both addresses, with NO attachment', frontCalls.length === 1 && mail.to.length === 2 && mail.attachments === 0, { calls: frontCalls.length, to: mail.to, attachments: mail.attachments });
        check('the email links to the order on the portal and says nothing is attached',
            mail.body.includes(portalUrl.replace(/&/g, '&amp;')) && /No file is attached/.test(mail.body) && /already approved/.test(mail.body));
        check('the reworded template: not "attached" as a promise, {poNumber} filled in',
            !/find attached/i.test(mail.body) && !/\{poNumber\}/.test(mail.body) && !/\{poNumber\}/.test(String(mail.subject))
            && mail.body.includes(spi.po_number), { subject: mail.subject });
        check('no "Confirm receipt" button', !/confirm-receipt/.test(mail.body));

        const [receipt] = await q(
            `SELECT id, email_type, confirm_via, action_url, confirmed_at FROM email_receipts
              WHERE send_table = 'purchase_order_signed_pi_sends' AND send_id = ?`, [sent.body?.sendId || 0]);
        check('a portal email receipt with the portal address, not yet confirmed',
            receipt && receipt.email_type === 'purchase_order_signed_pi' && receipt.confirm_via === 'portal'
            && receipt.action_url === portalUrl && receipt.confirmed_at === null, receipt);

        // An edited body keeps its token filled in.
        const edited = await api('POST', `/api/v1/purchase-order-signed-pis/${spi.id}/email`,
            { to: addr('second'), subject: 'Signed {poNumber}', body: '<p>Signed PI for {poNumber}, as discussed.</p>' });
        const mail2 = frontCalls[1] || { body: '', subject: '' };
        check('an edited subject and body go out with {poNumber} filled in, the block still appended',
            edited.status === 200 && mail2.subject === `Signed ${spi.po_number}`
            && mail2.body.includes(`Signed PI for ${spi.po_number}, as discussed.`) && /No file is attached/.test(mail2.body)
            && edited.body?.portalAccess?.users?.[0]?.outcome === 'already_active', { status: edited.status, subject: mail2.subject });

        // ── 2. the recipient on the portal ───────────────────────────────────
        console.log('\n2. The recipient signs in on the portal');
        const claims = { sub: crypto.randomUUID(), email: `new-${TAG}@${DOM}`, email_verified: 'true' };
        const me = await portalCall(claims, 'GET', '/api/supplier/me');
        check('active at the first sign-in, no approval to wait for', me.status === 200 && me.data?.status === 'active', me.raw);
        const list = await portalCall(claims, 'GET', '/api/supplier/purchase-orders/{id}/signed-pis', { id: String(spi.purchase_order_id) });
        const item = (list.data?.items || []).find(i => Number(i.id) === Number(spi.id));
        check('the order\'s page lists the signed PI, with a link to the file',
            list.status === 200 && item && typeof item.url === 'string' && item.url.includes('/signed-pis/') && item.lastSentAt, list.raw);
        const stranger = await portalCall({ sub: crypto.randomUUID(), email: addr('stranger'), email_verified: 'true' },
            'GET', '/api/supplier/purchase-orders/{id}/signed-pis', { id: String(spi.purchase_order_id) });
        check('an address that was not sent it sees nothing (pending: refused)', stranger.status === 403, stranger.raw);
        const viewed = await portalCall(claims, 'POST', '/api/supplier/signed-pis/{id}/viewed', { id: String(spi.id) });
        check('opening it records the view', viewed.status === 200 && viewed.data?.viewed === true, viewed.raw);

        // ── 3. back in shipping ──────────────────────────────────────────────
        console.log('\n3. Shipping sees the view');
        const shown = await api('GET', `/api/v1/purchase-orders/${spi.purchase_order_id}/signed-pis`);
        const row = (shown.body?.data || []).find(r => Number(r.id) === Number(spi.id));
        check('GET signed-pis lists who opened it', row && (row.views || []).some(v => v.email === claims.email && v.count === 1), row && row.views);
        check('and the sends just made', row && sendIds.every(id => (row.sends || []).some(s => Number(s.id) === Number(id))), row && row.sends);
        const conn = await getPool().getConnection();
        let sync;
        try { sync = await syncPortalReceipts(conn, { maxAgeMs: 0 }); } finally { conn.release(); }
        check('the receipts sync ran its signed-PI step', sync && !sync.failed.includes('signed_pi_viewed'), sync);
        const after = await q(
            `SELECT send_id, confirmed_at FROM email_receipts
              WHERE send_table = 'purchase_order_signed_pi_sends' AND send_id IN (?)`, [sendIds]);
        check('both emails are confirmed by the view on the portal', after.length === 2 && after.every(r => r.confirmed_at !== null), after);
    } finally {
        await cleanup();
        server.close();
        await portalPool.end();
        await closePool();
    }
}

async function cleanup() {
    const users = await q(`SELECT id FROM ${JP}.supplier_users WHERE email LIKE ?`, [`%${TAG}%`]);
    const ids = users.map(u => Number(u.id));
    if (ids.length) {
        await q(`DELETE FROM ${JP}.signed_pi_views WHERE user_id IN (?)`, [ids]);
        await q(`DELETE FROM ${JP}.user_supplier_access WHERE user_id IN (?)`, [ids]);
        await q(`DELETE FROM ${JP}.audit_logs WHERE entity_type = 'supplier_user' AND entity_id IN (?)`, [ids]);
        await q(`DELETE FROM ${JP}.supplier_users WHERE id IN (?)`, [ids]);
    }
    if (sendIds.length) {
        await q(`DELETE FROM email_receipts WHERE send_table = 'purchase_order_signed_pi_sends' AND send_id IN (?)`, [sendIds]);
        await q(`DELETE FROM purchase_order_signed_pi_sends WHERE id IN (?)`, [sendIds]);
    }
    const audit = await q(`DELETE FROM audit_log WHERE action = 'signed_pi_emailed' AND user_email = 'local@dev'
                             AND JSON_UNQUOTE(JSON_EXTRACT(after_json, '$.frontMessageUid')) LIKE 'smoke_msg_%'`);
    console.log(`\ncleanup: ${ids.length} portal users, ${sendIds.length} sends, ${audit.affectedRows} audit rows removed`);
}

main()
    .then(() => {
        console.log(`\n${pass} passed, ${fail} failed${fail ? `:\n  - ${failures.join('\n  - ')}` : ''}`);
        process.exit(fail ? 1 : 0);
    })
    .catch(async (e) => {
        console.error('[smoke] crashed:', e);
        process.exit(1);
    });
