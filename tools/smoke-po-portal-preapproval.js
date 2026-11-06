'use strict';

// End-to-end check of PO SEND's supplier-portal pre-approval
// (docs/po-portal-send.md) against the TEST database, both halves for real:
//
//   1. shipping: the real POST /api/v1/purchase-order-documents/:id/email
//      route (the Express app in-process, auth bypassed as local@dev), so
//      preapprovePortalUsers runs its real SQL against jportal on TEST;
//   2. portal: the real jportal `supplier` Lambda handler in-process
//      (../portal, like portal/tools/smoke-*.js) with fabricated Cognito
//      claims, so a recipient "registers and signs in" and we check they are
//      bound, active, linked to the supplier and can see the PO + its PDF.
//
//   node tools/smoke-po-portal-preapproval.js
//
// NOTHING LEAVES THIS MACHINE: global fetch is replaced, the Front call is
// answered by a stub (recording what would have been sent) and any other
// outbound request throws. Everything it writes on TEST (portal users and
// their access/audit rows, send rows, receipts, audit_log rows) is tagged and
// deleted at the end, pass or fail. Refuses to run unless .env.test points at
// explorer-test.
//
// Limitation: both halves use shipping's TEST `admin` login, so a missing
// jportal_app column grant on the portal side would not show here.

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
        const n = frontCalls.push({ to: f.getAll('to[]'), subject: f.get('subject'), body: String(f.get('body')) });
        return new Response(JSON.stringify({
            message_uid: `smoke_msg_${n}`,
            _links: { related: { conversation: `https://api2.frontapp.com/conversations/cnv_smoke_${n}` } },
        }), { status: 202, headers: { 'content-type': 'application/json' } });
    }
    throw new Error(`[smoke] blocked outbound fetch to ${u}`);
};

const mysql = require('mysql2/promise');
const { getPool, closePool } = require('../src/db');
const { preapprovePortalUsers } = require('../src/services/portal-access');

// ── reporting ──────────────────────────────────────────────────────────────
let pass = 0, fail = 0;
const failures = [];
function check(label, cond, detail) {
    if (cond) { pass++; console.log(`  PASS  ${label}`); }
    else {
        fail++; failures.push(label);
        console.log(`  FAIL  ${label}${detail !== undefined ? `\n        got: ${typeof detail === 'string' ? detail : JSON.stringify(detail)}` : ''}`);
    }
}
const note = (s) => console.log(`  ....  ${s}`);

// ── fixtures ───────────────────────────────────────────────────────────────
const TAG = `smk${Date.now().toString(36)}`;
const DOM = `${TAG}.invalid`;
const addr = (who) => `${who}-${TAG}@${DOM}`;
const STAFF_ADDR = `${TAG}@jfamedical.co.uk`;  // a colleague's address (staff domain)
const sendIds = [];

async function q(sql, params) { return (await getPool().query(sql, params))[0]; }

const BIN = (col) => `CONVERT(${col} USING utf8mb4) COLLATE utf8mb4_0900_bin`;
const idsNamed = async (name) => (await q(
    `SELECT id FROM suppliers s WHERE s.deleted_at IS NULL AND ${BIN('s.name')} = ${BIN('?')} ORDER BY id`, [name])).map(r => Number(r.id));

// headerIds: suppliers named as the PO; supplierIds: those plus its live lines' suppliers.
async function pickPo(where, label, { optional = false } = {}) {
    const rows = await q(
        `SELECT po.id, po.po_number, po.supplier, MAX(d.id) AS doc_id
           FROM purchase_orders po
           JOIN purchase_order_documents d ON d.purchase_order_id = po.id AND d.deleted_at IS NULL
          WHERE po.deleted_at IS NULL AND ${where}
          GROUP BY po.id, po.po_number, po.supplier
          ORDER BY po.id DESC LIMIT 1`);
    if (!rows.length) {
        if (optional) { console.log(`  fixture ${label}: none on TEST (its checks are skipped)`); return null; }
        throw new Error(`no TEST PO for fixture "${label}"`);
    }
    const po = rows[0];
    po.headerIds = await idsNamed(po.supplier);
    const lines = (await q(`SELECT DISTINCT supplier FROM orders WHERE purchase_order_id = ? AND deleted_at IS NULL
                              AND supplier IS NOT NULL AND supplier <> ''`, [po.id])).map(r => r.supplier);
    const all = new Set(po.headerIds);
    for (const n of lines) for (const id of await idsNamed(n)) all.add(id);
    po.supplierIds = [...all].sort((a, b) => a - b);
    console.log(`  fixture ${label}: PO ${po.po_number} (id ${po.id}, doc ${po.doc_id}) "${po.supplier}"`
        + ` -> header suppliers [${po.headerIds}], with lines [${po.supplierIds}]`);
    return po;
}
const NAMED = (col) => `EXISTS (SELECT 1 FROM suppliers s WHERE s.deleted_at IS NULL AND ${BIN('s.name')} = ${BIN(col)})`;
const EXACT = NAMED('po.supplier');
const LINE = (cond) => `EXISTS (SELECT 1 FROM orders o WHERE o.purchase_order_id = po.id AND o.deleted_at IS NULL
    AND NOT (o.supplier <=> po.supplier) AND ${cond})`;

const userRow = async (email) => (await q(
    `SELECT id, email, cognito_sub, status, approved_by, decided_note, row_version FROM ${JP}.supplier_users WHERE email = ?`, [email]))[0] || null;
const accessOf = async (userId) => (await q(
    `SELECT supplier_id FROM ${JP}.user_supplier_access WHERE user_id = ? ORDER BY supplier_id`, [userId])).map(r => Number(r.supplier_id));
const hasAll = (have, want) => want.every(id => have.includes(id));

// ── shipping route ─────────────────────────────────────────────────────────
let baseUrl;
async function sendPo(docId, to, extra = {}) {
    const res = await realFetch(`${baseUrl}/api/v1/purchase-order-documents/${docId}/email`, {
        method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ to, ...extra }),
    });
    const body = await res.json().catch(() => null);
    if (body && body.sendId) sendIds.push(body.sendId);
    return { status: res.status, body };
}
const outcomes = (pa) => Object.fromEntries((pa?.users || []).map(u => [u.email, `${u.outcome}/${u.suppliersAdded}`]));

// ── portal handler ─────────────────────────────────────────────────────────
let portal;
let seq = 0;
async function portalCall(claims, resource, query = null) {
    const res = await portal.handler({
        httpMethod: 'GET', resource, pathParameters: null, queryStringParameters: query,
        headers: { 'user-agent': 'smoke-po-portal-preapproval' },
        requestContext: { requestId: `${TAG}-${++seq}`, authorizer: { claims } },
        body: null, isBase64Encoded: false,
    }, {});
    let parsed = null;
    try { parsed = JSON.parse(res.body); } catch { /* left null */ }
    return { status: res.statusCode, data: parsed?.data, code: parsed?.code ?? parsed?.error?.code, raw: String(res.body).slice(0, 300) };
}
const claimsFor = (email) => ({ sub: crypto.randomUUID(), email, email_verified: 'true' });

async function signInAndLook(label, claims, po) {
    const me = await portalCall(claims, '/api/supplier/me');
    const list = await portalCall(claims, '/api/supplier/purchase-orders', { all: '1' });
    const docs = await portalCall(claims, '/api/supplier/po-documents');
    const poIds = (list.data?.items || []).map(p => Number(p.id));
    const group = (docs.data?.items || []).find(g => Number(g.purchaseOrderId) === Number(po.id));
    const docIds = (group?.documents || []).map(d => Number(d.id ?? d.documentId));
    return { me, list, docs, poIds, docIds, label };
}

// ── main ───────────────────────────────────────────────────────────────────
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

    // Two independent connections, as two Lambda containers would have.
    const racePool = mysql.createPool({
        host: process.env.DB_HOST, user: process.env.DB_USER, password: process.env.DB_PASSWORD,
        port: Number(process.env.DB_PORT) || 3306, database: process.env.DB_NAME,
        ssl: { rejectUnauthorized: false }, connectionLimit: 4,
    });

    try {
        console.log('Fixtures');
        const poA = await pickPo(`${EXACT} AND po.supplier = 'Roosin Medical Co., Ltd.'`, 'A');
        const poB = await pickPo(`${EXACT} AND po.supplier <> 'Roosin Medical Co., Ltd.'`, 'B');
        const poL = await pickPo(`${EXACT} AND ${LINE(NAMED('o.supplier'))}`, 'lines add a supplier', { optional: true });
        const poX = await pickPo(`NOT ${EXACT} AND ${LINE(NAMED('o.supplier'))}`, 'only a line supplier matches', { optional: true });
        const poN = await pickPo(`NOT ${EXACT} AND NOT ${LINE(NAMED('o.supplier'))}`, 'nothing matches', { optional: true });

        // An address that registered on the portal BEFORE being sent the PO: the portal's own
        // first request creates its `pending` row.
        const pendingClaims = claimsFor(addr('pending'));
        const reg = await portalCall(pendingClaims, '/api/supplier/me');
        check('setup: a self-registered address is pending on the portal', reg.status === 200 && reg.data?.status === 'pending', reg.raw);
        // Already active and linked to supplier A (unbound, as JFPRO pre-provisions).
        await q(`INSERT INTO ${JP}.supplier_users (email, status, created_by) VALUES (?, 'active', ?)`, [addr('active'), TAG]);
        const activeRow = await userRow(addr('active'));
        await q(`INSERT INTO ${JP}.user_supplier_access (user_id, supplier_id, supplier_name, created_by) VALUES ${poA.supplierIds.map(() => '(?, ?, ?, ?)').join(', ')}`,
            poA.supplierIds.flatMap(id => [activeRow.id, id, poA.supplier, TAG]));
        await q(`INSERT INTO ${JP}.supplier_users (email, status, created_by) VALUES (?, 'disabled', ?), (?, 'rejected', ?)`,
            [addr('disabled'), TAG, addr('rejected'), TAG]);

        // ── 1. a normal send, every kind of recipient at once ────────────────
        console.log('\n1. Send PO A to a mix of recipients');
        const mixed = [
            `  New-${TAG}@${DOM.toUpperCase()} `, addr('new'),   // same address twice, case/space differ
            addr('pending'), addr('active'), addr('disabled'), addr('rejected'), STAFF_ADDR,
        ];
        const s1 = await sendPo(poA.doc_id, mixed);
        check('route answers 200', s1.status === 200, s1.body);
        const pa1 = s1.body?.portalAccess;
        check('portalAccess.status is ok', pa1?.status === 'ok', pa1);
        check('linked suppliers = every row named as the PO or one of its lines',
            JSON.stringify((pa1?.suppliers || []).map(s => s.id).sort((a, b) => a - b)) === JSON.stringify(poA.supplierIds)
            && pa1?.poSupplierMatched === true, pa1?.suppliers);
        const o1 = outcomes(pa1);
        const nA = poA.supplierIds.length;
        check('new address -> invited (once, lower-cased)', o1[addr('new')] === `invited/${nA}` && Object.keys(o1).length === 6, o1);
        check('self-registered pending -> approved', o1[addr('pending')] === `approved/${nA}`, o1);
        check('already active + linked -> already_active, nothing added', o1[addr('active')] === 'already_active/0', o1);
        check('disabled and rejected -> blocked, nothing added',
            o1[addr('disabled')] === 'blocked/0' && o1[addr('rejected')] === 'blocked/0', o1);
        check('colleague (staff domain) -> invited and linked too', o1[STAFF_ADDR] === `invited/${nA}`, o1);

        const newRow = await userRow(addr('new'));
        check('new row: active, unbound, approved_by sender, note names the PO',
            newRow && newRow.status === 'active' && newRow.cognito_sub === null && newRow.approved_by === 'local@dev'
            && String(newRow.decided_note).includes(poA.po_number), newRow);
        check('new row: linked to the supplier', newRow && hasAll(await accessOf(newRow.id), poA.supplierIds));
        const pendRow = await userRow(addr('pending'));
        check('pending row: now active, still bound, row_version bumped, linked',
            pendRow && pendRow.status === 'active' && pendRow.cognito_sub === pendingClaims.sub && pendRow.row_version >= 1
            && hasAll(await accessOf(pendRow.id), poA.supplierIds), pendRow);
        const disRow = await userRow(addr('disabled'));
        check('disabled row: untouched and unlinked', disRow?.status === 'disabled' && (await accessOf(disRow.id)).length === 0, disRow);
        const staffRow = await userRow(STAFF_ADDR);
        check('colleague row: active and linked to the supplier',
            staffRow?.status === 'active' && hasAll(await accessOf(staffRow.id), poA.supplierIds), staffRow);

        const f1 = frontCalls[frontCalls.length - 1];
        check('Front got every recipient once', f1 && f1.to.length === 6
            && [addr('pending'), addr('active'), addr('disabled'), addr('rejected'), STAFF_ADDR].every(m => f1.to.includes(m))
            && f1.to.some(t => t.toLowerCase() === addr('new')), f1?.to);
        check('email body links to the PO on the portal', f1 && f1.body.includes(`/orders/${poA.id}?tab=pdfs`));
        check('email says "our team will approve" (one recipient was blocked)', f1 && /our team will approve/.test(f1.body) && !/already approved/.test(f1.body));
        const sendRow = (await q(`SELECT sent_to FROM purchase_order_document_sends WHERE id = ?`, [s1.body?.sendId]))[0];
        check('send row written (it is what lists the PDF on the portal)', Boolean(sendRow), s1.body?.sendId);

        // ── 2. the recipients sign in on the portal ──────────────────────────
        console.log('\n2. Recipients sign in on the portal');
        const newClaims = claimsFor(addr('new'));
        const vNew = await signInAndLook('new', newClaims, poA);
        check('new: /me is active and lists the supplier',
            vNew.me.status === 200 && vNew.me.data?.status === 'active'
            && hasAll((vNew.me.data?.suppliers || []).map(s => Number(s.id)), poA.supplierIds), vNew.me.raw);
        check('new: first sign-in binds the pre-approved row (keeps it active)',
            (await userRow(addr('new')))?.cognito_sub === newClaims.sub);
        check('new: PO A is in their orders list', vNew.list.status === 200 && vNew.poIds.includes(Number(poA.id)), vNew.list.raw);
        check('new: the emailed PDF is in their Emailed PDFs', vNew.docs.status === 200 && vNew.docIds.includes(Number(poA.doc_id)), vNew.docs.raw);

        const vPend = await signInAndLook('pending', pendingClaims, poA);
        check('registered-earlier: now active and sees PO A + PDF',
            vPend.me.data?.status === 'active' && vPend.poIds.includes(Number(poA.id)) && vPend.docIds.includes(Number(poA.doc_id)),
            { me: vPend.me.raw, list: vPend.list.raw });

        const vDis = await signInAndLook('disabled', claimsFor(addr('disabled')), poA);
        check('disabled: refused (403 USER_DISABLED)', vDis.list.status === 403 && vDis.list.code === 'USER_DISABLED', vDis.list.raw);

        const vStaff = await signInAndLook('staff', claimsFor(STAFF_ADDR), poA);
        check('colleague: signs in active and sees PO A + PDF (was "waiting for approval", the cindy@ case)',
            vStaff.me.data?.status === 'active' && vStaff.poIds.includes(Number(poA.id)) && vStaff.docIds.includes(Number(poA.doc_id)),
            { me: vStaff.me.raw, list: vStaff.list.raw });

        // ── 3. same people, a PO from a second supplier ──────────────────────
        console.log('\n3. Send PO B (another supplier) to an existing portal user');
        const s3 = await sendPo(poB.doc_id, [addr('new')]);
        const o3 = outcomes(s3.body?.portalAccess);
        check('already_active, and supplier B is added', s3.status === 200 && o3[addr('new')] === `already_active/${poB.supplierIds.length}`, s3.body?.portalAccess);
        const v3 = await signInAndLook('new', newClaims, poB);
        check('they now see PO B too, without signing in again', v3.poIds.includes(Number(poB.id)) && v3.docIds.includes(Number(poB.doc_id)), v3.list.raw);
        check('email says "already approved"', /already approved/.test(frontCalls[frontCalls.length - 1].body));

        // ── 4. colleagues only (PO_00430J on prod, 2026-11-04) ───────────────
        console.log('\n4. Send to colleagues only');
        const solo = [`${TAG}.solo@jfamedical.co.uk`, `${TAG}.solo@gmail.com`];
        const s4 = await sendPo(poA.doc_id, solo);
        const o4 = outcomes(s4.body?.portalAccess);
        const soloRows = await Promise.all(solo.map(userRow));
        check('each colleague invited and linked to every PO supplier',
            s4.status === 200 && s4.body?.portalAccess?.status === 'ok' && solo.every(e => o4[e] === `invited/${nA}`)
            && soloRows.every(u => u?.status === 'active'), s4.body?.portalAccess);
        check('email says "already approved"', /already approved/.test(frontCalls[frontCalls.length - 1].body));

        // ── 5. suppliers named on the PO's lines ─────────────────────────────
        console.log('\n5. Suppliers named on the PO\'s lines');
        if (poL) {
            const r = await sendPo(poL.doc_id, [addr('lines')]);
            const row = await userRow(addr('lines'));
            check('recipient linked to the PO supplier AND every line supplier',
                r.status === 200 && r.body?.portalAccess?.poSupplierMatched === true && row
                && JSON.stringify(await accessOf(row.id)) === JSON.stringify(poL.supplierIds)
                && poL.supplierIds.length > poL.headerIds.length, { access: row && await accessOf(row.id), want: poL.supplierIds });
            const v = await signInAndLook('lines', claimsFor(addr('lines')), poL);
            check('they see the PO and its PDF', v.poIds.includes(Number(poL.id)) && v.docIds.includes(Number(poL.doc_id)), v.list.raw);
        }
        if (poX) {
            const r = await sendPo(poX.doc_id, [addr('lineonly')]);
            const pa = r.body?.portalAccess;
            const row = await userRow(addr('lineonly'));
            check('PO supplier unknown but a line supplier is: linked to it, poSupplierMatched false',
                r.status === 200 && pa?.status === 'ok' && pa.poSupplierMatched === false && row
                && JSON.stringify(await accessOf(row.id)) === JSON.stringify(poX.supplierIds), pa);
            check('...and the email does not promise they will see the order', !/already approved/.test(frontCalls[frontCalls.length - 1].body));
        }
        if (poN) {
            const r = await sendPo(poN.doc_id, [addr('nosup')]);
            check('no supplier matches at all: email sent, status no_supplier, nobody created',
                r.status === 200 && r.body?.portalAccess?.status === 'no_supplier' && (await userRow(addr('nosup'))) === null, r.body?.portalAccess);
        }

        // ── 6. addresses the way people paste them ───────────────────────────
        console.log('\n6. Pasted address forms');
        for (const [label, raw, clean] of [
            ['display name', `Zoe <${addr('display')}>`, [addr('display')]],
            ['two in one box (comma)', `${addr('c1')}, ${addr('c2')}`, [addr('c1'), addr('c2')]],
            ['two in one box (semicolon)', `${addr('s1')};${addr('s2')}`, [addr('s1'), addr('s2')]],
            ['full-width @ (Chinese IME)', `fw-${TAG}＠${DOM}`, [addr('fw')]],
            ['angle brackets', `<${addr('angle')}>`, [addr('angle')]],
            ['zero-width space', `${addr('zws')}​`, [addr('zws')]],
            ['mailto: prefix', `mailto:${addr('mailto')}`, [addr('mailto')]],
        ]) {
            const r = await sendPo(poA.doc_id, [raw]);
            const sentTo = frontCalls[frontCalls.length - 1].to;
            const rows = await Promise.all(clean.map(userRow));
            check(`${label}: sent to and pre-approved as the clean address`,
                r.status === 200 && JSON.stringify(sentTo) === JSON.stringify(clean) && rows.every(u => u && u.status === 'active'),
                `HTTP ${r.status}, Front to ${JSON.stringify(sentTo)}, rows ${JSON.stringify(rows.map(u => u && u.email))}`);
        }
        for (const raw of [`zoe@${TAG},invalid`, `two words@${DOM}`, `a@@${DOM}`]) {
            const before = frontCalls.length;
            const r = await sendPo(poA.doc_id, [addr('fine'), raw]);
            check(`not an address ${JSON.stringify(raw)}: 400 naming it, nothing sent, nobody pre-approved`,
                r.status === 400 && Array.isArray(r.body?.invalid) && r.body.invalid.length > 0
                && frontCalls.length === before && (await userRow(addr('fine'))) === null, r.body);
        }

        // ── 7. two sends to the same new address at the same moment ──────────
        console.log('\n7. Two concurrent sends inviting new addresses (two Lambdas)');
        const race = async (label, a, b, n = 5) => {
            const statuses = [];
            for (let i = 0; i < n; i++) {
                const c1 = await racePool.getConnection();
                const c2 = await racePool.getConnection();
                try {
                    const args = (email) => ({ recipients: [email], poSupplier: poA.supplier, actorEmail: 'local@dev', note: TAG });
                    const rs = await Promise.all([[c1, a(i)], [c2, b(i)]].map(([c, e]) =>
                        preapprovePortalUsers(c, args(e)).catch(err => ({ status: `threw ${err.code || err.errno}` }))));
                    statuses.push(rs.map(r => r.status === 'ok' ? `ok:${r.users[0].outcome}` : r.status).join(' + '));
                } finally { c1.release(); c2.release(); }
            }
            note(`${label}: ${statuses.join(' | ')}`);
            return statuses;
        };
        const same = await race('same address', i => addr(`race${i}`), i => addr(`race${i}`));
        const diff = await race('different addresses', i => addr(`rx${i}`), i => addr(`ry${i}`));
        const rows = await q(`SELECT email, status FROM ${JP}.supplier_users WHERE email LIKE ? OR email LIKE ? OR email LIKE ?`,
            [`race%-${TAG}@%`, `rx%-${TAG}@%`, `ry%-${TAG}@%`]);
        check('every round ends with the right active rows (5 + 10)', rows.length === 15 && rows.every(r => r.status === 'active'), rows);
        check('no concurrent send reports a failure', [...same, ...diff].every(s => !/threw|error/.test(s)), [...same, ...diff]);
    } finally {
        await cleanup();
        server.close();
        await racePool.end();
        await portalPool.end();
        await closePool();
    }
}

async function cleanup() {
    const users = await q(`SELECT id FROM ${JP}.supplier_users WHERE email LIKE ? OR email = ?`, [`%${TAG}%`, STAFF_ADDR]);
    const ids = users.map(u => Number(u.id));
    if (ids.length) {
        await q(`DELETE FROM ${JP}.user_supplier_access WHERE user_id IN (?)`, [ids]);
        await q(`DELETE FROM ${JP}.audit_logs WHERE entity_type = 'supplier_user' AND entity_id IN (?)`, [ids]);
        await q(`DELETE FROM ${JP}.supplier_users WHERE id IN (?)`, [ids]);
    }
    if (sendIds.length) {
        await q(`DELETE FROM email_receipts WHERE send_table = 'purchase_order_document_sends' AND send_id IN (?)`, [sendIds]);
        await q(`DELETE FROM purchase_order_document_sends WHERE id IN (?)`, [sendIds]);
    }
    const audit = await q(`DELETE FROM audit_log WHERE action = 'document_emailed' AND user_email = 'local@dev'
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
        process.exit(2);
    });
