'use strict';
// The portal's views of a payment sent to a forwarder, against a real database
// (2026-11-20_21 portal_forwarder_payments_v / _lines_v / _proofs_v, and the
// supplier's portal_payments_v as that migration tightened it;
// docs/payment-proof-send.md, "A forwarder's payment").
//
// What only a database can show: who each view lists, that a payment sent to
// a forwarder is never a row of the supplier's view, what `revision` covers,
// and that the portal's write (one INSERT ... SELECT, as jportal's
// src/handlers/forwarderPayments.js makes it) waits for an uncommitted edit
// and then writes nothing for the revision that went.
//
// It WRITES: fixture rows of its own (forwarders, contacts, charges, payments,
// lines, sends, proof rows, portal events), every one deleted by id at the
// end, pass or fail. TEST only: it refuses a host that does not look like TEST
// and needs the host said back to it.
//
//   node tools/test-portal-forwarder-views.js --confirm-host <DB_HOST>
//
// Needs the migrations of 2026-11-20 applied here. The two race checks also
// need the portal's forwarder_payment_events table (jportal's migration); where
// it is missing they are reported as SKIPPED and the run does not pass.
require('dotenv').config();
const crypto = require('crypto');
const mysql = require('mysql2/promise');
const { getPool, closePool } = require('../src/db');
const { jportalSchema } = require('../src/lib/jportal');

const host = process.env.DB_HOST || '';
const confirm = process.argv[process.argv.indexOf('--confirm-host') + 1];
const TAG = `ZZ fwd-view-test ${crypto.randomBytes(4).toString('hex')}`;
const wait = ms => new Promise(r => setTimeout(r, ms));

let passed = 0;
let failed = 0;
let skipped = 0;
const check = (name, ok, detail = '') => {
    if (ok) { passed++; console.log(`  ok    ${name}`); } else { failed++; console.log(`  FAIL  ${name}${detail ? `  [${detail}]` : ''}`); }
};
const skip = name => { skipped++; console.log(`  SKIP  ${name}`); };

(async () => {
    if (!/test/i.test(host)) { console.error(`Refusing: ${host || '(DB_HOST unset)'} does not look like the TEST database.`); process.exit(2); }
    if (confirm !== host) { console.error(`Refusing to write fixtures: pass --confirm-host ${host}`); process.exit(2); }
    const pool = getPool();
    const conn = await pool.getConnection();
    // A connection of its own for the other side of a race: the app's pool holds one
    // connection only (src/db/index.js), so a second one asked of it would wait for ever.
    const other = await mysql.createConnection({
        host: process.env.DB_HOST, user: process.env.DB_USER, password: process.env.DB_PASSWORD, database: process.env.DB_NAME,
        port: Number(process.env.DB_PORT || 3306), ssl: { rejectUnauthorized: false },
    });
    const jp = jportalSchema();
    const made = { forwarders: [], forwarder_contacts: [], payment_extras: [], supplier_payments: [], supplier_payment_lines: [], supplier_payment_sends: [], shipment_payment_documents: [] };
    const q = async (sql, params) => (await conn.query(sql, params))[0];
    const ins = async (table, row) => {
        const cols = Object.keys(row);
        const r = await q(`INSERT INTO ${table} (${cols.join(', ')}) VALUES (${cols.map(() => '?').join(', ')})`, cols.map(c => row[c]));
        made[table].push(r.insertId);
        return r.insertId;
    };
    const forwarder = name => ins('forwarders', { name: `${TAG} ${name}`, created_by_email: 'view-test@tools' });
    const contact = (forwarderId, email) => ins('forwarder_contacts', { forwarder_id: forwarderId, name: 'View Test', email });
    const charge = (forwarderId, over = {}) => ins('payment_extras', {
        supplier_name: TAG, supplier_key: TAG.toLowerCase(), currency: 'USD', amount: 100, kind: 'freight', rides_with: 'shipment',
        shipment_reference: '339', description: 'INTERNAL-DESCRIPTION', note: 'INTERNAL-NOTE', forwarder_id: forwarderId, created_by_email: 'view-test@tools', ...over,
    });
    const payment = (over = {}) => ins('supplier_payments', { supplier_name: TAG, supplier_key: TAG.toLowerCase(), amount: 100, currency: 'USD', paid_on: '2026-11-21', created_by_email: 'view-test@tools', ...over });
    const line = (paymentId, targetId, over = {}) => ins('supplier_payment_lines', { payment_id: paymentId, target_kind: 'extra', target_id: targetId, amount: 100, ...over });
    const send = (paymentId, forwarderId, over = {}) => ins('supplier_payment_sends', {
        supplier_payment_id: paymentId, notice_kind: 'forwarder', delivery: 'portal', supplier_name: TAG, forwarder_id: forwarderId,
        send_key: `vt-${crypto.randomBytes(12).toString('hex')}`, request_hash: 'x'.repeat(64), status: 'sent',
        deadline_at: '2026-11-21 10:01:00', sent_to: '[]', sent_at: '2026-11-21 10:00:00', ...over,
    });
    const proof = (paymentId, over = {}) => ins('shipment_payment_documents', {
        shipment_id: 0, shipment_reference: 'view-test', supplier_name: TAG, supplier_key: TAG.toLowerCase(), supplier_payment_id: paymentId,
        doc_kind: 'remittance', filename: 'proof.pdf', s3_key: `shipment-payments/view-test/${crypto.randomBytes(6).toString('hex')}.pdf`,
        public_url: 'https://shipping-purchase-orders-test.s3.eu-north-1.amazonaws.com/shipment-payments/view-test/proof.pdf', ...over,
    });
    // A payment that is one forwarder's, with `n` charges, sent to it, with a proof.
    const sentPayment = async (forwarderId, { charges = 1, sendTo = forwarderId, withProof = true, chargeOver = {} } = {}) => {
        const id = await payment({ amount: 100 * charges });
        const extras = [];
        for (let i = 0; i < charges; i++) { const e = await charge(forwarderId, chargeOver); extras.push(e); await line(id, e); }
        if (sendTo != null) await send(id, sendTo);
        if (withProof) await proof(id);
        return { id, extras };
    };
    const inFwd = async id => (await q('SELECT forwarder_id, revision FROM portal_forwarder_payments_v WHERE payment_id = ?', [id]))[0] || null;
    const inSup = async id => (await q('SELECT payment_id FROM portal_payments_v WHERE payment_id = ?', [id])).length > 0;
    const rev = async id => { const r = await inFwd(id); return r ? r.revision : null; };

    try {
        console.log(`Database: ${process.env.DB_USER || '?'}@${host}/${process.env.DB_NAME || '?'}   fixtures tagged "${TAG}"`);
        const A = await forwarder('A');
        const B = await forwarder('B');
        const C = await forwarder('C removed');
        await contact(A, `${TAG.replace(/\W+/g, '.')}.a@view-test.example`.toLowerCase());
        await q('UPDATE forwarders SET deleted_at = NOW() WHERE id = ?', [C]);

        console.log('\nWho the views list');
        const p1 = await sentPayment(A, { charges: 2 });
        const row1 = await inFwd(p1.id);
        check('a payment that is one forwarder\'s and was sent to it is listed, under that forwarder', row1 && Number(row1.forwarder_id) === A, JSON.stringify(row1));
        check('its revision is a 64-character hash', row1 && /^[a-f0-9]{64}$/.test(row1.revision));
        check('it is NOT a row of the supplier\'s view, though its send carries the payment\'s own name', !(await inSup(p1.id)));
        const lines1 = await q('SELECT * FROM portal_forwarder_payment_lines_v WHERE payment_id = ? ORDER BY line_id', [p1.id]);
        check('its charges are listed, one each', lines1.length === 2);
        check('a charge is told as amount, kind and container only', lines1.length > 0 && Object.keys(lines1[0]).sort().join() === 'amount,extra_kind,line_id,payment_id,shipment_reference', lines1[0] && Object.keys(lines1[0]).join());
        check('nothing typed on a charge is in any column', !JSON.stringify(lines1).includes('INTERNAL'));
        const proofs1 = await q('SELECT * FROM portal_forwarder_payment_proofs_v WHERE payment_id = ?', [p1.id]);
        check('its proof is listed', proofs1.length === 1 && proofs1[0].filename === 'proof.pdf');

        const unsent = await sentPayment(A, { sendTo: null });
        check('never sent: in neither view', !(await inFwd(unsent.id)) && !(await inSup(unsent.id)));
        const failedSend = await sentPayment(A, { sendTo: null });
        await send(failedSend.id, A, { status: 'failed', sent_at: null });
        check('a send that did not go out does not list it', !(await inFwd(failedSend.id)));

        const mixed = await sentPayment(A);
        await line(mixed.id, 2000000000, { target_kind: 'balance' });
        check('mixed with a line of another kind: in neither view', !(await inFwd(mixed.id)) && !(await inSup(mixed.id)));
        const unlinked = await sentPayment(null, { sendTo: A });
        check('a charge with a typed payee (no forwarder): not listed', !(await inFwd(unlinked.id)));
        const two = await sentPayment(A);
        await line(two.id, await charge(B));
        check('charges of two forwarders: not listed for either', !(await inFwd(two.id)));
        const gone = await sentPayment(A);
        await q('UPDATE payment_extras SET deleted_at = NOW() WHERE id = ?', [gone.extras[0]]);
        check('a deleted charge: not listed', !(await inFwd(gone.id)));
        const missing = await payment();
        await line(missing, 2000000001);
        await send(missing, A);
        check('a line whose charge is not there: not listed', !(await inFwd(missing)));
        const empty = await payment();
        await send(empty, A);
        check('no lines at all: not listed', !(await inFwd(empty)));
        const removed = await sentPayment(C);
        check('a forwarder taken off the list: not listed', !(await inFwd(removed.id)));
        const coincide = await payment();
        await line(coincide, p1.extras[0], { target_kind: 'balance' });
        await send(coincide, A);
        check('a line of another kind whose id equals a charge\'s id: not listed', !(await inFwd(coincide)));
        const wrongSend = await sentPayment(A, { sendTo: B });
        check('sent to another forwarder than the one it is linked to: not listed', !(await inFwd(wrongSend.id)));

        console.log('\nA payment that changes hands');
        const turned = await sentPayment(A, { sendTo: null });
        await send(turned.id, null, { notice_kind: 'balance', forwarder_id: null });
        check('sent to a supplier once, then turned into a forwarder\'s: not in the supplier\'s view', !(await inSup(turned.id)));
        check('and not in the forwarder\'s until it is sent to the forwarder', !(await inFwd(turned.id)));
        await send(turned.id, A);
        check('then sent to the forwarder: in the forwarder\'s view only', Boolean(await inFwd(turned.id)) && !(await inSup(turned.id)));
        const supplierOnly = await payment();
        await ins('supplier_payment_lines', { payment_id: supplierOnly, target_kind: 'po_deposit', target_id: 2000000002, amount: 100 });
        await send(supplierOnly, null, { notice_kind: 'deposit', forwarder_id: null });
        check('a supplier\'s payment is still a row of the supplier\'s view', await inSup(supplierOnly));
        await send(supplierOnly, A);
        check('and a forwarder send on it lists it for no forwarder', !(await inFwd(supplierOnly)));
        const forwarderSendOnly = await payment();
        await ins('supplier_payment_lines', { payment_id: forwarderSendOnly, target_kind: 'po_deposit', target_id: 2000000003, amount: 100 });
        await send(forwarderSendOnly, A);
        check('a forwarder send alone never puts a supplier\'s payment in the supplier\'s view', !(await inSup(forwarderSendOnly)));

        const revA = await rev(p1.id);
        await q('UPDATE payment_extras SET forwarder_id = ? WHERE id IN (?)', [B, p1.extras]);
        check('re-pointed at another forwarder: gone from the first one\'s page, and not on the second\'s (not sent to it)', !(await inFwd(p1.id)));
        await send(p1.id, B);
        const asB = await inFwd(p1.id);
        check('sent to the second: listed under the second', asB && Number(asB.forwarder_id) === B);
        check('the same payment under another forwarder has another revision', asB && asB.revision !== revA);
        await q('UPDATE payment_extras SET forwarder_id = ? WHERE id IN (?)', [A, p1.extras]);
        const back = await inFwd(p1.id);
        check('pointed back: shown to the first again, at the revision it had', back && Number(back.forwarder_id) === A && back.revision === revA);

        console.log('\nWhat the revision covers');
        const r = await sentPayment(A, { charges: 2 });
        const r0 = await rev(r.id);
        const changes = async (name, change, undo) => {
            await change();
            const now = await rev(r.id);
            check(`changes with ${name}`, now !== null && now !== r0);
            await undo();
            check(`and is what it was once ${name} is put back`, (await rev(r.id)) === r0);
        };
        await changes('the amount', () => q('UPDATE supplier_payments SET amount = 201 WHERE id = ?', [r.id]), () => q('UPDATE supplier_payments SET amount = 200 WHERE id = ?', [r.id]));
        await changes('the currency', () => q("UPDATE supplier_payments SET currency = 'GBP' WHERE id = ?", [r.id]), () => q("UPDATE supplier_payments SET currency = 'USD' WHERE id = ?", [r.id]));
        await changes('the paid date', () => q("UPDATE supplier_payments SET paid_on = '2026-11-22' WHERE id = ?", [r.id]), () => q("UPDATE supplier_payments SET paid_on = '2026-11-21' WHERE id = ?", [r.id]));
        const [firstLine] = await q('SELECT id FROM supplier_payment_lines WHERE payment_id = ? ORDER BY id LIMIT 1', [r.id]);
        await changes('a charge\'s amount', () => q('UPDATE supplier_payment_lines SET amount = 99 WHERE id = ?', [firstLine.id]), () => q('UPDATE supplier_payment_lines SET amount = 100 WHERE id = ?', [firstLine.id]));
        await changes('a charge\'s container', () => q("UPDATE payment_extras SET shipment_reference = '340' WHERE id = ?", [r.extras[0]]), () => q("UPDATE payment_extras SET shipment_reference = '339' WHERE id = ?", [r.extras[0]]));
        await changes('a charge\'s kind', () => q("UPDATE payment_extras SET kind = 'customs' WHERE id = ?", [r.extras[0]]), () => q("UPDATE payment_extras SET kind = 'freight' WHERE id = ?", [r.extras[0]]));
        const [doc] = await q('SELECT id FROM shipment_payment_documents WHERE supplier_payment_id = ?', [r.id]);
        await changes('the proof\'s file name', () => q("UPDATE shipment_payment_documents SET filename = 'other.pdf' WHERE id = ?", [doc.id]), () => q("UPDATE shipment_payment_documents SET filename = 'proof.pdf' WHERE id = ?", [doc.id]));
        await changes('the proof no longer filed under the payment\'s name', () => q("UPDATE shipment_payment_documents SET supplier_name = 'Somebody Else' WHERE id = ?", [doc.id]), () => q('UPDATE shipment_payment_documents SET supplier_name = ? WHERE id = ?', [TAG, doc.id]));
        check('a proof filed under another name is not listed either', (await q("SELECT COUNT(*) AS n FROM portal_forwarder_payment_proofs_v WHERE payment_id = ?", [r.id]))[0].n === 1);
        // What a save that changes nothing does: the lines are written again, with new ids.
        const before = await q('SELECT target_kind, target_id, amount FROM supplier_payment_lines WHERE payment_id = ? ORDER BY id', [r.id]);
        await q('DELETE FROM supplier_payment_lines WHERE payment_id = ?', [r.id]);
        for (const l of before.reverse()) await line(r.id, l.target_id, { amount: l.amount });
        check('a save that changes nothing (lines written again, in another order) keeps the revision', (await rev(r.id)) === r0);
        // The same charge named by one, two and three lines: no two cancel each other out.
        const d = await sentPayment(A, { charges: 1 });
        const one = await rev(d.id);
        const l2 = await line(d.id, d.extras[0]);
        const twoLines = await rev(d.id);
        const l3 = await line(d.id, d.extras[0]);
        const three = await rev(d.id);
        check('equal lines do not cancel: one, two and three of the same are three revisions', new Set([one, twoLines, three]).size === 3 && ![one, twoLines, three].includes(null));
        await q('DELETE FROM supplier_payment_lines WHERE id IN (?)', [[l2, l3]]);
        // Values that would run into each other if they were only joined.
        const e = await sentPayment(A, { charges: 1 });
        const variants = [];
        for (const [filename, url] of [['1:x', 'y'], ['1', ':xy'], ['1:', 'xy']]) {
            await q('UPDATE shipment_payment_documents SET filename = ?, public_url = ? WHERE supplier_payment_id = ?', [filename, url, e.id]);
            variants.push(await rev(e.id));
        }
        check('values holding the separator each give their own revision', new Set(variants).size === 3);
        const refs = [];
        for (const ref of ['', null, '339', '货柜 339']) {
            await q('UPDATE payment_extras SET shipment_reference = ? WHERE id = ?', [ref, e.extras[0]]);
            refs.push(await rev(e.id));
        }
        check('empty, NULL and a non-ASCII container are each their own revision', new Set(refs).size === 4 && !refs.includes(null));

        console.log('\nWhose proof');
        const sameName = await sentPayment(A);
        await proof(sameName.id, { filename: 'theirs.pdf' });
        await proof(p1.id, { doc_kind: 'balance_invoice', filename: 'invoice.pdf' });
        const mine = await q('SELECT filename FROM portal_forwarder_payment_proofs_v WHERE payment_id = ? ORDER BY document_id', [p1.id]);
        check('a proof of another payment under the same name, and an invoice, are not a payment\'s proofs', mine.map(x => x.filename).join() === 'proof.pdf', mine.map(x => x.filename).join());

        console.log('\nThe portal\'s write against an uncommitted edit');
        let hasEvents = true;
        try { await q(`SELECT 1 FROM ${jp}.forwarder_payment_events LIMIT 1`); } catch (err) { if (err.errno === 1146 || err.errno === 1049) hasEvents = false; else throw err; }
        if (!hasEvents) {
            skip(`${jp}.forwarder_payment_events is not on this database: an acknowledgement against an uncommitted edit`);
            skip('the same, against a contact being removed');
        } else {
            const email = (await q('SELECT email FROM forwarder_contacts WHERE forwarder_id = ? AND deleted_at IS NULL', [A]))[0].email;
            // jportal src/handlers/forwarderPayments.js: one statement, read and write together.
            const WRITE = `INSERT INTO ${jp}.forwarder_payment_events (supplier_payment_id, forwarder_id, user_id, user_email, user_name, event, revision)
                SELECT p.payment_id, p.forwarder_id, 0, ?, 'View Test', ?, p.revision
                  FROM portal_forwarder_payments_v p
                 WHERE p.payment_id = ? AND p.revision = ?
                   AND p.forwarder_id IN (SELECT c.forwarder_id FROM forwarder_contacts c WHERE c.email = ? AND c.deleted_at IS NULL)`;
            const race = async (name, edit) => {
                const t = await sentPayment(A);
                const seen = await rev(t.id);
                await other.beginTransaction();
                let done = false;
                let written = null;
                try {
                    await edit(other, t);
                    const writing = conn.query(WRITE, [email, 'acknowledged', t.id, seen, email]).then(([res]) => { done = true; written = res.affectedRows; });
                    await wait(600);
                    check(`${name}: the write waits while the edit is uncommitted`, done === false);
                    await other.commit();
                    await writing;
                } catch (err) {
                    await other.rollback().catch(() => {});
                    throw err;
                }
                check(`${name}: once it commits, nothing is written for what went`, written === 0, `affectedRows ${written}`);
                check(`${name}: no event row exists`, (await q(`SELECT COUNT(*) AS n FROM ${jp}.forwarder_payment_events WHERE supplier_payment_id = ?`, [t.id]))[0].n === 0);
                return t;
            };
            await race('an edit of the amount', (c, t) => c.query('UPDATE supplier_payments SET amount = 150 WHERE id = ?', [t.id]));
            await race('a contact being removed', c => c.query('UPDATE forwarder_contacts SET deleted_at = NOW() WHERE forwarder_id = ?', [A]));
            await q('UPDATE forwarder_contacts SET deleted_at = NULL WHERE forwarder_id = ?', [A]);
            // And with nothing in the way it writes exactly one row, once per revision.
            const calm = await sentPayment(A);
            const seen = await rev(calm.id);
            const [w1] = await conn.query(WRITE, [email, 'acknowledged', calm.id, seen, email]);
            check('with nothing in the way: one row', w1.affectedRows === 1);
            let dup = null;
            try { await conn.query(WRITE, [email, 'acknowledged', calm.id, seen, email]); } catch (err) { dup = err.errno; }
            check('a second acknowledgement of one revision is a duplicate key, not a second row', dup === 1062, `errno ${dup}`);
            const [w3] = await conn.query(WRITE, ['nobody@view-test.example', 'acknowledged', calm.id, seen, 'nobody@view-test.example']);
            check('an address that is not a contact of that forwarder writes nothing', w3.affectedRows === 0);
            const [w4] = await conn.query(WRITE, [email, 'opened', calm.id, 'f'.repeat(64), email]);
            check('an opening that carries a revision the payment does not have writes nothing', w4.affectedRows === 0);
        }
    } catch (err) {
        failed++;
        console.error('\nFAILED with an error:', err.code || '', err.message);
    } finally {
        // Every row this run made, by id; the lines also by payment (some were written again above).
        try {
            await other.rollback().catch(() => {});
            if (made.supplier_payments.length) {
                try { await q(`DELETE FROM ${jp}.forwarder_payment_events WHERE supplier_payment_id IN (?)`, [made.supplier_payments]); } catch (err) { if (err.errno !== 1146 && err.errno !== 1049) throw err; }
                await q('DELETE FROM supplier_payment_lines WHERE payment_id IN (?)', [made.supplier_payments]);
            }
            for (const table of ['shipment_payment_documents', 'supplier_payment_sends', 'supplier_payment_lines', 'supplier_payments', 'payment_extras', 'forwarder_contacts', 'forwarders']) {
                if (made[table].length) await q(`DELETE FROM ${table} WHERE id IN (?)`, [made[table]]);
            }
            const [left] = await q('SELECT (SELECT COUNT(*) FROM supplier_payments WHERE supplier_name = ?) + (SELECT COUNT(*) FROM payment_extras WHERE supplier_name = ?) + (SELECT COUNT(*) FROM forwarders WHERE name LIKE ?) AS n', [TAG, TAG, `${TAG}%`]);
            console.log(`\nFixtures removed: ${Number(left.n) === 0 ? 'all' : `${left.n} row(s) LEFT, tagged "${TAG}"`}`);
            if (Number(left.n) !== 0) failed++;
        } catch (err) {
            failed++;
            console.error(`Cleanup failed (rows tagged "${TAG}" may remain):`, err.code || '', err.message);
        }
        conn.release();
        await other.end().catch(() => {});
        await closePool();
    }
    console.log(`\n${passed} passed, ${failed} failed${skipped ? `, ${skipped} SKIPPED (not a pass)` : ''}`);
    process.exit(failed || skipped ? 1 : 0);
})();
