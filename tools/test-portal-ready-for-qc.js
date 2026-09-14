'use strict';

// Smoke test for the supplier portal's markReadyForQc flag (IN_PRODUCTION →
// READY_FOR_QC on POST /api/v1/portal/po-ready-date).
//
// Self-contained: it creates a THROWAWAY purchase order + 3 throwaway order
// lines against a real supplier (read-only use of that supplier's portal_code),
// drives the real Lambda handler in-process via a synthetic HTTP API v2 event —
// no server to start, no network — then HARD-DELETES everything it created in a
// finally block. It never touches pre-existing rows.
//
//   node tools/test-portal-ready-for-qc.js

require('dotenv').config();
const { getPool, closePool } = require('../src/db');
const { normalizeCode } = require('../src/lib/portal-code');
const { handler } = require('../src/handlers/supplier-portal');

const STAMP = Date.now();
const PO_NUMBER = `ZZ-PORTAL-TEST-${STAMP}`;

let pass = 0;
let fail = 0;
function check(label, cond, detail = '') {
    if (cond) { pass++; console.log(`  PASS  ${label}`); }
    else { fail++; console.log(`  FAIL  ${label}${detail ? ' — ' + detail : ''}`); }
}

// Minimal API Gateway HTTP API v2 event — what serverless-http unwraps.
async function post(path, body) {
    const res = await handler({
        version: '2.0',
        rawPath: path,
        rawQueryString: '',
        headers: { 'content-type': 'application/json' },
        requestContext: { http: { method: 'POST', path, sourceIp: '127.0.0.1' } },
        body: JSON.stringify(body),
        isBase64Encoded: false,
    }, { callbackWaitsForEmptyEventLoop: true });
    let data = null;
    try { data = JSON.parse(res.body); } catch { data = res.body; }
    return { status: res.statusCode, data };
}

const LOOKUP = '/api/v1/portal/po-lookup';
const READY = '/api/v1/portal/po-ready-date';

async function run() {
    const pool = getPool();
    let poId = null;
    const orderIds = [];

    try {
        // ── Setup ───────────────────────────────────────────────────────────
        const [[supplier]] = await pool.query(
            `SELECT id, name, portal_code FROM suppliers
              WHERE deleted_at IS NULL AND portal_code IS NOT NULL AND portal_code <> ''
              ORDER BY id LIMIT 1`
        );
        if (!supplier) throw new Error('No supplier with a portal_code — cannot test.');
        const CODE = normalizeCode(supplier.portal_code);
        console.log(`Supplier: ${supplier.name} (id ${supplier.id})`);
        console.log(`Fake PO:  ${PO_NUMBER}\n`);

        const [poIns] = await pool.query(
            `INSERT INTO purchase_orders (po_number, supplier, created_at) VALUES (?, ?, NOW())`,
            [PO_NUMBER, supplier.name]
        );
        poId = poIns.insertId;

        // A: IN_PRODUCTION, NO actual ready date  → the gate case
        // B: IN_PRODUCTION, actual ready date already stored → flag alone works
        // C: READY (past the QC step, still date-editable) → wrong start status
        for (const [jf, status, ard] of [
            [`ZZTEST-A-${STAMP}`, 'IN_PRODUCTION', null],
            [`ZZTEST-B-${STAMP}`, 'IN_PRODUCTION', '2026-08-01'],
            [`ZZTEST-C-${STAMP}`, 'READY', '2026-07-01'],
        ]) {
            const [ins] = await pool.query(
                `INSERT INTO orders (jf_code, product_name, quantity, status, purchase_order_id,
                                     actual_ready_date, dates)
                 VALUES (?, ?, ?, ?, ?, ?, ?)`,
                [jf, 'Portal smoke test line', 10, status, poId, ard, JSON.stringify({ ordered: '2026-07-01T00:00:00.000Z' })]
            );
            orderIds.push(ins.insertId);
        }
        const [A, B, C] = orderIds;
        console.log(`Lines: A=${A} (IN_PRODUCTION, no date)  B=${B} (IN_PRODUCTION, dated)  C=${C} (READY)\n`);

        const readOrder = async (id) => {
            const [[r]] = await pool.query(
                'SELECT status, actual_ready_date, estimated_ready_date, dates FROM orders WHERE id = ?', [id]
            );
            r.datesObj = typeof r.dates === 'string' ? JSON.parse(r.dates) : (r.dates || {});
            return r;
        };
        const auditCount = async (id) => {
            const [[r]] = await pool.query(
                `SELECT COUNT(*) AS n FROM audit_log WHERE entity_type = 'order' AND entity_id = ?`, [id]
            );
            return r.n;
        };

        // ── 1. Auth still generic ───────────────────────────────────────────
        console.log('1. po-lookup with a wrong access code');
        let r = await post(LOOKUP, { poNumber: PO_NUMBER, accessCode: 'WRONGCODE' });
        check('401', r.status === 401, `got ${r.status}`);

        // ── 2. Lookup exposes canMarkReadyForQc ─────────────────────────────
        console.log('\n2. po-lookup with the right code');
        r = await post(LOOKUP, { poNumber: PO_NUMBER, accessCode: CODE });
        check('200', r.status === 200, `got ${r.status} ${JSON.stringify(r.data)}`);
        const byId = new Map((r.data?.data?.items || []).map(i => [i.orderId, i]));
        check('3 lines returned', byId.size === 3, `got ${byId.size}`);
        check('A canMarkReadyForQc=true', byId.get(A)?.canMarkReadyForQc === true, JSON.stringify(byId.get(A)));
        check('B canMarkReadyForQc=true', byId.get(B)?.canMarkReadyForQc === true);
        check('C canMarkReadyForQc=false (READY)', byId.get(C)?.canMarkReadyForQc === false);
        check('C still editable for dates', byId.get(C)?.editable === true);

        // ── 3. Flag without an actual ready date → 422 ──────────────────────
        console.log('\n3. markReadyForQc on A (no actual ready date anywhere)');
        r = await post(READY, { poNumber: PO_NUMBER, accessCode: CODE, updates: [{ orderId: A, markReadyForQc: true }] });
        check('422', r.status === 422, `got ${r.status} ${JSON.stringify(r.data)}`);
        check('A untouched', (await readOrder(A)).status === 'IN_PRODUCTION');

        // ── 4. Non-boolean flag → 400 ───────────────────────────────────────
        console.log('\n4. markReadyForQc: "yes"');
        r = await post(READY, { poNumber: PO_NUMBER, accessCode: CODE, updates: [{ orderId: B, markReadyForQc: 'yes' }] });
        check('400', r.status === 400, `got ${r.status} ${JSON.stringify(r.data)}`);

        // ── 5. Wrong starting status → 409 ──────────────────────────────────
        console.log('\n5. markReadyForQc on C (status READY)');
        r = await post(READY, { poNumber: PO_NUMBER, accessCode: CODE, updates: [{ orderId: C, markReadyForQc: true }] });
        check('409', r.status === 409, `got ${r.status} ${JSON.stringify(r.data)}`);
        check('C untouched', (await readOrder(C)).status === 'READY');

        // ── 6. Batch is all-or-nothing ──────────────────────────────────────
        console.log('\n6. batch: valid B move + invalid C move');
        const bAuditBefore = await auditCount(B);
        r = await post(READY, {
            poNumber: PO_NUMBER, accessCode: CODE,
            updates: [{ orderId: B, markReadyForQc: true }, { orderId: C, markReadyForQc: true }],
        });
        check('409', r.status === 409, `got ${r.status}`);
        check('B NOT moved (nothing applied)', (await readOrder(B)).status === 'IN_PRODUCTION');
        check('no audit row written', (await auditCount(B)) === bAuditBefore);

        // ── 7. The real move: date + flag in one update ─────────────────────
        console.log('\n7. A: actualReadyDate + markReadyForQc, alongside a plain date edit on C');
        const beforeMs = Date.now();
        r = await post(READY, {
            poNumber: PO_NUMBER, accessCode: CODE,
            updates: [
                { orderId: A, actualReadyDate: '2026-08-05', markReadyForQc: true },
                { orderId: C, estimatedReadyDate: '2026-08-20' },
            ],
        });
        check('200', r.status === 200, `got ${r.status} ${JSON.stringify(r.data)}`);
        const rowA = await readOrder(A);
        check('A status = READY_FOR_QC', rowA.status === 'READY_FOR_QC', `got ${rowA.status}`);
        check('A actual_ready_date set', String(rowA.actual_ready_date).slice(0, 10) === '2026-08-05', String(rowA.actual_ready_date));
        check('A dates.ready_for_qc stamped', !!rowA.datesObj.ready_for_qc, JSON.stringify(rowA.datesObj));
        check('stamp is fresh', new Date(rowA.datesObj.ready_for_qc).getTime() >= beforeMs - 1000);
        check('A pre-existing dates keys kept', !!rowA.datesObj.ordered);
        check('C moved by nobody, date applied', (await readOrder(C)).status === 'READY');
        check('C estimated_ready_date set', String((await readOrder(C)).estimated_ready_date).slice(0, 10) === '2026-08-20');
        const respA = (r.data?.data?.items || []).find(i => i.orderId === A);
        check('response echoes READY_FOR_QC', respA?.status === 'READY_FOR_QC', JSON.stringify(respA));
        check('response canMarkReadyForQc now false', respA?.canMarkReadyForQc === false);

        // Audit trail
        const [audits] = await pool.query(
            `SELECT before_json, after_json, user_email FROM audit_log
              WHERE entity_type = 'order' AND entity_id = ? ORDER BY id DESC LIMIT 1`, [A]
        );
        const au = audits[0];
        const parse = (v) => (typeof v === 'string' ? JSON.parse(v) : v) || {};
        check('audit row written', !!au);
        check('audit user is the supplier portal', String(au?.user_email || '').startsWith('supplier-portal:'), au?.user_email);
        check('audit before.status = IN_PRODUCTION', parse(au?.before_json).status === 'IN_PRODUCTION', JSON.stringify(au?.before_json));
        check('audit after.status = READY_FOR_QC', parse(au?.after_json).status === 'READY_FOR_QC');
        check('audit carries the date change too', parse(au?.after_json).actualReadyDate === '2026-08-05');

        // ── 8. Double-submit is an idempotent no-op ─────────────────────────
        console.log('\n8. re-send the same update (double-click)');
        const aAuditBefore = await auditCount(A);
        const stampBefore = rowA.datesObj.ready_for_qc;
        r = await post(READY, {
            poNumber: PO_NUMBER, accessCode: CODE,
            updates: [{ orderId: A, actualReadyDate: '2026-08-05', markReadyForQc: true }],
        });
        check('200 (not 409)', r.status === 200, `got ${r.status} ${JSON.stringify(r.data)}`);
        const rowA2 = await readOrder(A);
        check('still READY_FOR_QC', rowA2.status === 'READY_FOR_QC');
        check('timestamp NOT re-stamped', rowA2.datesObj.ready_for_qc === stampBefore);
        check('no duplicate audit row', (await auditCount(A)) === aAuditBefore);

        // ── 9. Locked line still refuses everything ─────────────────────────
        console.log('\n9. line pushed past the factory (ON_SEA) is fully locked');
        await pool.query('UPDATE orders SET status = ? WHERE id = ?', ['ON_SEA', B]);
        r = await post(READY, { poNumber: PO_NUMBER, accessCode: CODE, updates: [{ orderId: B, estimatedReadyDate: '2026-09-01' }] });
        check('409 on a date edit', r.status === 409, `got ${r.status}`);
        r = await post(READY, { poNumber: PO_NUMBER, accessCode: CODE, updates: [{ orderId: B, markReadyForQc: true }] });
        check('409 on markReadyForQc', r.status === 409, `got ${r.status}`);

        // ── 10. Another supplier's code can't reach this PO ─────────────────
        console.log('\n10. a different supplier\'s code against this PO');
        const [[other]] = await pool.query(
            `SELECT portal_code FROM suppliers
              WHERE deleted_at IS NULL AND portal_code IS NOT NULL AND portal_code <> '' AND name <> ?
              ORDER BY id LIMIT 1`, [supplier.name]
        );
        r = await post(READY, {
            poNumber: PO_NUMBER, accessCode: normalizeCode(other.portal_code),
            updates: [{ orderId: A, markReadyForQc: true }],
        });
        check('401', r.status === 401, `got ${r.status}`);
    } finally {
        // ── Teardown — hard-delete only the rows this script created ────────
        console.log('\nCleaning up…');
        if (orderIds.length) {
            await pool.query(
                `DELETE FROM audit_log WHERE entity_type = 'order' AND entity_id IN (?)`, [orderIds]
            );
            await pool.query('DELETE FROM orders WHERE id IN (?)', [orderIds]);
        }
        if (poId) await pool.query('DELETE FROM purchase_orders WHERE id = ?', [poId]);
        const [[left]] = await pool.query('SELECT COUNT(*) AS n FROM purchase_orders WHERE po_number = ?', [PO_NUMBER]);
        console.log(`  removed ${orderIds.length} order line(s) + ${poId ? 1 : 0} PO (residual: ${left.n})`);
        await closePool();
    }

    console.log(`\n${pass} passed, ${fail} failed`);
    process.exit(fail ? 1 : 0);
}

run().catch(err => { console.error(err); process.exit(1); });
