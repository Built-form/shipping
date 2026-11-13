'use strict';

// Unit tests for the Request to send's portal delivery
// (docs/request-to-send-portal.md): the pure rules in
// src/lib/request-to-send-portal.js, and the SQL in
// src/services/request-to-send-portal.js against a fake connection. No database.
//   node --test tools/test-request-to-send-portal-lib.js

const test = require('node:test');
const assert = require('node:assert/strict');
const L = require('../src/lib/request-to-send-portal');
const S = require('../src/services/request-to-send-portal');
const { loadSupplierQuoteViews } = require('../src/services/portal-access');
const R = require('../src/lib/email-receipt');
const { DEFAULT_EMAIL_TEMPLATES } = require('../src/lib/email-template-defaults');

test('requestPortalUrl: the Requests to send page, opened on that document', () => {
    assert.equal(L.requestPortalUrl('https://p.example', 412), 'https://p.example/requests-to-send?doc=412');
});

// A draft's name is internal: its stamp, other suppliers' names, a departure date long
// out of date ("DRAFT-SEA-260923-141617 - 333 Sunmed Kingphar Foreu ETD 13 Oct"). A
// supplier is shown the container's number and nothing else (user, 2026-11-13).
// The same table is pinned in the portal (jportal tools/test-supplier-quotes-handler.js).
const REFS = [
    ['DRAFT-SEA-260923-141617 - 333 Sunmed Kingphar Foreu ETD 13 Oct', '333'],
    ['DRAFT-SEA-260922-105426 - 331 INTCO ETD 26 Oct (TBC)', '331'],
    ['DRAFT-SEA-260917-164404 - 327', '327'],
    ['DRAFT-SEA-260814-104313 - 311 ', '311'],
    ['DRAFT-AIR-260928-102710 - 126. Air Freight', '126. Air Freight'],
    ['DRAFT-AIR-260916-154300 - 123. Airfreight urgent', '123. Air Freight'],
    // An air draft numbered without the words is still air freight.
    ['DRAFT-AIR-260928-102710 - 127', '127. Air Freight'],
    // No stamp and no ' - ': not a name this rule reads a number out of.
    ['XX45 pallets', null],
    // Once booked, the document carries the container's reference itself.
    ['328', '328'],
    ['121. Air Freight', '121. Air Freight'],
    // No number in it: nothing of the name is shown.
    ['DRAFT-AIR-260911-181909 - Airfreight', null],
    ['DRAFT-SEA-260903-100903', null],
    ['DRAFT-SEA-260714-121102 - TAM Container 2', null],
    ['DRAFT-AIR-260707-140656- JFA Airfreight Kingphar', null],
    ['DRAFT-SEA-260923-141617 - 333rd attempt', null],
    ['Sunmed Container', null],
    ['', null],
    [null, null],
];

test('supplierRef: the number of a container, never the rest of its name', () => {
    for (const [name, ref] of REFS) assert.equal(L.supplierRef(name), ref, String(name));
});

test('requestNames: what a Request to send prints and is filed as', () => {
    // "Ref:" on the PDF, and the file the supplier downloads: the number, the supplier, the version.
    assert.deepEqual(
        L.requestNames({ name: 'DRAFT-SEA-260923-141617 - 333 Sunmed Kingphar Foreu ETD 13 Oct', supplierName: 'Suzhou Sunmed Co.,Ltd.', version: 2 }),
        { ref: '333', fileBase: 'request-to-send-SuzhouSunmedCoLtd-333-v2' }
    );
    assert.deepEqual(
        L.requestNames({ name: 'DRAFT-AIR-260928-102710 - 126. Air Freight', supplierName: 'Intco Medical', version: 1 }),
        { ref: '126. Air Freight', fileBase: 'request-to-send-IntcoMedical-126._Air_Freight-v1' }
    );
    // No number: no reference printed, and nothing of the name in the file's.
    assert.deepEqual(
        L.requestNames({ name: 'DRAFT-AIR-260707-140656- JFA Airfreight Kingphar', supplierName: 'Roosin', version: 3 }),
        { ref: '', fileBase: 'request-to-send-Roosin-v3' }
    );
    // A supplier name with no Latin letter still gives a file name.
    assert.equal(L.requestNames({ name: '328', supplierName: '苏州', version: 1 }).fileBase, 'request-to-send-supplier-328-v1');
});

test('emailName / hideInternalName: the email says the number, whatever the sender\'s screen filled in', () => {
    const name = 'DRAFT-SEA-260923-141617 - 333 Sunmed Kingphar Foreu ETD 13 Oct';
    assert.equal(L.emailName(name), '333');
    // No number: a plain word, never the name.
    assert.equal(L.emailName('DRAFT-AIR-260707-140656- JFA Airfreight Kingphar'), 'shipment');

    // A subject an older ShipLine filled with the whole name, and a body that repeats it.
    assert.equal(L.hideInternalName(`Request to send – ${name}`, name), 'Request to send – 333');
    assert.equal(L.hideInternalName(`<p>For ${name}: see ${name}.</p>`, name), '<p>For 333: see 333.</p>');
    // As an editor escapes it.
    const amp = 'DRAFT-SEA-260923-141617 - 340 Sunmed & Roosin <urgent>';
    assert.equal(L.hideInternalName('<p>Re DRAFT-SEA-260923-141617 - 340 Sunmed &amp; Roosin &lt;urgent&gt;</p>', amp), '<p>Re 340</p>');
    // In a subject it is plain text, not escaped.
    assert.equal(L.hideInternalName('Re ' + amp, amp), 'Re 340');
    // Nothing to hide: a booked reference is its own number, and other text is left alone.
    assert.equal(L.hideInternalName('Request to send – 328', '328'), 'Request to send – 328');
    assert.equal(L.hideInternalName('Request to send – 333', name), 'Request to send – 333');
    assert.equal(L.hideInternalName('', name), '');
    assert.equal(L.hideInternalName(null, name), null);
});

test('deliveryFor: a link only when the portal can show it, else attached with the reason', () => {
    const ok = { docType: 'supplier-quote', supplier: 'Acme Co', portalBase: 'https://p.example', supplierMatched: true, portalAvailable: true };
    assert.deepEqual(L.deliveryFor(ok), { delivery: 'portal', reason: null });
    // A forwarder quote goes to a forwarder: never the portal.
    assert.deepEqual(L.deliveryFor({ ...ok, docType: 'forwarder-quote' }), { delivery: 'attached', reason: 'not_request_to_send' });
    assert.deepEqual(L.deliveryFor({ ...ok, docType: 'quote' }), { delivery: 'attached', reason: 'not_request_to_send' });
    assert.deepEqual(L.deliveryFor({ ...ok, portalBase: '' }), { delivery: 'attached', reason: 'portal_url_unset' });
    assert.deepEqual(L.deliveryFor({ ...ok, portalAvailable: false }), { delivery: 'attached', reason: 'portal_unavailable' });
    // The supplier's spelling matches no portal supplier: the page would be empty.
    assert.deepEqual(L.deliveryFor({ ...ok, supplierMatched: false }), { delivery: 'attached', reason: 'no_supplier' });
    assert.deepEqual(L.deliveryFor({ ...ok, supplier: '  ' }), { delivery: 'attached', reason: 'no_supplier' });
    assert.deepEqual(L.deliveryFor({ ...ok, supplier: null }), { delivery: 'attached', reason: 'no_supplier' });
});

test('requestedDelivery: absent = an older ShipLine (attached, unchecked); anything else must be one of the two', () => {
    assert.deepEqual(L.requestedDelivery(undefined), { ok: true, requested: null });
    assert.deepEqual(L.requestedDelivery(null), { ok: true, requested: null });
    assert.deepEqual(L.requestedDelivery(''), { ok: true, requested: null });
    assert.deepEqual(L.requestedDelivery('portal'), { ok: true, requested: 'portal' });
    assert.deepEqual(L.requestedDelivery('attached'), { ok: true, requested: 'attached' });
    assert.equal(L.requestedDelivery('PORTAL').ok, false);
    assert.equal(L.requestedDelivery(true).ok, false);
});

test('deliveryConflict: what the sender was shown must still be true', () => {
    assert.equal(L.deliveryConflict(null, { delivery: 'portal', reason: null }), null);
    assert.equal(L.deliveryConflict('portal', { delivery: 'portal', reason: null }), null);
    assert.equal(L.deliveryConflict('attached', { delivery: 'attached', reason: 'no_supplier' }), null);

    const lost = L.deliveryConflict('portal', { delivery: 'attached', reason: 'no_supplier' });
    assert.equal(lost.code, 'DELIVERY_CHANGED');
    assert.equal(lost.delivery, 'attached');
    assert.equal(lost.reason, 'no_supplier');
    assert.deepEqual(lost.allowedNext, ['attached']);
    assert.match(lost.error, /attached/);

    const gained = L.deliveryConflict('attached', { delivery: 'portal', reason: null });
    assert.equal(gained.code, 'DELIVERY_CHANGED');
    assert.equal(gained.delivery, 'portal');
    assert.deepEqual(gained.allowedNext, ['portal']);
    assert.match(gained.error, /portal/);
});

test('parseSentFields: the ETD and port the email carried, or nothing', () => {
    assert.deepEqual(L.parseSentFields(undefined), { ok: true, etd: null, port: null });
    assert.deepEqual(L.parseSentFields(null), { ok: true, etd: null, port: null });
    assert.deepEqual(L.parseSentFields({}), { ok: true, etd: null, port: null });
    assert.deepEqual(L.parseSentFields({ etd: '2026-11-22', port: '  Shanghai ' }), { ok: true, etd: '2026-11-22', port: 'Shanghai' });
    assert.deepEqual(L.parseSentFields({ etd: '', port: '' }), { ok: true, etd: null, port: null });
    assert.deepEqual(L.parseSentFields({ etd: null, port: 'Ningbo   Port' }), { ok: true, etd: null, port: 'Ningbo Port' });
    // Not a calendar day: refused, never stored as something else.
    for (const etd of ['22 November', '2026-11-31', '2026-11-00', '2026-13-01', '2026-00-10', '2026-02-30', '2027-02-29', '0026-11-22', '2026-11-22T00:00:00Z', 20261122]) {
        const r = L.parseSentFields({ etd });
        assert.equal(r.ok, false, String(etd));
        assert.match(r.error, /etd/);
    }
    assert.equal(L.parseSentFields({ etd: '2028-02-29' }).etd, '2028-02-29');
    assert.equal(L.parseSentFields({ port: 'x'.repeat(129) }).ok, false);
    assert.equal(L.parseSentFields({ port: 'x'.repeat(128) }).port.length, 128);
    assert.equal(L.parseSentFields({ port: 7 }).ok, false);
    assert.equal(L.parseSentFields('etd').ok, false);
    assert.equal(L.parseSentFields([]).ok, false);
});

test('formatEtd: the day as text, built from its parts (no time zone)', () => {
    assert.deepEqual(L.formatEtd('2026-11-22'), { en: '22 November 2026', zh: '2026年11月22日' });
    assert.deepEqual(L.formatEtd('2027-01-05'), { en: '5 January 2027', zh: '2027年1月5日' });
    assert.equal(L.formatEtd(null), null);
    assert.equal(L.formatEtd('soon'), null);
});

test('requestPortalBlockHtml: link, what was asked, and wording that follows pre-approval', () => {
    const base = { version: 2, etd: '2026-11-22', port: 'Shang<hai>', url: 'https://p.example/requests-to-send?doc=9&x="y"', registerUrl: 'https://p.example/register' };
    const yes = L.requestPortalBlockHtml({ ...base, preapproved: true });
    assert.match(yes, /href="https:\/\/p\.example\/requests-to-send\?doc=9&amp;x=&quot;y&quot;"/);
    assert.match(yes, /22 November 2026/);
    assert.match(yes, /2026年11月22日/);
    assert.match(yes, /Shang&lt;hai&gt;/);
    assert.doesNotMatch(yes, /Shang<hai>/);
    assert.match(yes, /Requests to send/);
    assert.match(yes, /发货通知/);
    assert.match(yes, /already approved/);
    // Not the PO block's wording: there is no order here.
    assert.doesNotMatch(yes, /this order/);
    assert.doesNotMatch(yes, /此订单/);
    assert.match(yes, /No file is attached/);
    // The page it links to shows the request; it does not take uploads (user, 2026-11-13).
    assert.doesNotMatch(yes, /upload/i);
    assert.doesNotMatch(yes, /上传/);

    const no = L.requestPortalBlockHtml({ ...base, preapproved: false });
    assert.doesNotMatch(no, /already approved/);
    assert.match(no, /our team will approve/);

    // Nothing was said about the date or the port: nothing is invented.
    const bare = L.requestPortalBlockHtml({ ...base, etd: null, port: null, preapproved: true });
    assert.doesNotMatch(bare, /ETD/);
    assert.doesNotMatch(bare, /port of departure/i);
    const etdOnly = L.requestPortalBlockHtml({ ...base, port: null, preapproved: true });
    assert.match(etdOnly, /ETD/);
    assert.doesNotMatch(etdOnly, /port of departure/i);
});

test('emailTypeFor: a Request to send is listed as one on the Email Receipts page, not as a forwarder quote', () => {
    assert.equal(L.emailTypeFor('supplier-quote'), 'request_to_send');
    assert.equal(L.emailTypeFor('forwarder-quote'), 'draft_container_quote');
    assert.equal(L.emailTypeFor('quote'), 'draft_container_quote');
    assert.equal(L.emailTypeFor(null), 'draft_container_quote');
});

test('preapprovalNote / sendFactsOf', () => {
    assert.equal(
        L.preapprovalNote({ ref: '328', version: 3, actor: 'ops@x.com' }),
        'Pre-approved from ShipLine: Request to send 328 v3 emailed by ops@x.com'
    );
    assert.match(L.preapprovalNote({ ref: '328', version: 3 }), /emailed by ShipLine$/);
    assert.deepEqual(L.sendFactsOf({ delivery: 'portal', request_etd: '2026-11-22', request_port: 'Shanghai' }), { delivery: 'portal', etd: '2026-11-22', port: 'Shanghai' });
    // A row from before the columns existed went out attached.
    assert.deepEqual(L.sendFactsOf({}), { delivery: 'attached', etd: null, port: null });
    assert.deepEqual(L.sendFactsOf(undefined), { delivery: 'attached', etd: null, port: null });
    assert.deepEqual(L.sendFactsOf({ delivery: 'weird', request_etd: null, request_port: '' }), { delivery: 'attached', etd: null, port: null });
});

test('the portal template: bound to the Request to send dialog, asks for the ETD and the port, promises no attachment', () => {
    const t = DEFAULT_EMAIL_TEMPLATES.find(x => x.key === L.TEMPLATE_KEY);
    assert.ok(t, 'request_to_send_portal is a default template');
    assert.equal(L.TEMPLATE_KEY, 'request_to_send_portal');
    // ShipLine offers a template in the Request to send dialog by this category.
    assert.equal(t.category, 'Supplier Quote');
    assert.match(t.bodyHtml, /\[etd:date\]/);
    assert.match(t.bodyHtml, /\[port:port\]/);
    assert.doesNotMatch(t.bodyHtml, /attached packing list|see attached|see the attached/i);
    assert.match(t.subject, /\{draftContainerName\}/);
});

// ── The SQL, against a fake connection ──────────────────────────────────────

const missing = errno => Object.assign(new Error('missing'), { errno });

function fakeConn({ suppliers = [], portal = 'ok', sendColumns = true, sends = [], views = [], earlier = [], updateColumn = true } = {}) {
    const queries = [];
    return {
        queries,
        async query(sql, params = []) {
            const flat = String(sql).replace(/\s+/g, ' ').trim();
            queries.push({ sql: flat, params });
            if (/FROM suppliers s/.test(flat)) return [suppliers.filter(s => s.name === params[0])];
            if (/supplier_users/.test(flat)) {
                if (portal === 'missing') throw missing(1049);
                if (portal === 'broken') throw new Error('boom');
                return [[{ ok: 1 }]];
            }
            // The latest earlier version of a request that was emailed.
            if (/FROM draft_container_documents p/.test(flat)) return [earlier.slice(0, 1)];
            if (/^UPDATE draft_container_document_sends SET request_update/.test(flat)) {
                if (!updateColumn) throw missing(1054);
                return [{ affectedRows: 1 }];
            }
            if (/SELECT id, request_update FROM draft_container_document_sends/.test(flat)) {
                if (!updateColumn) throw missing(1054);
                return [sends];
            }
            if (/^INSERT INTO draft_container_document_sends/.test(flat)) {
                if (/delivery/.test(flat) && !sendColumns) throw missing(1054);
                return [{ insertId: 77 }];
            }
            if (/FROM draft_container_document_sends/.test(flat)) {
                if (!sendColumns) throw missing(1054);
                return [sends];
            }
            if (/supplier_quote_views/.test(flat)) {
                if (portal === 'missing') throw missing(1146);
                return [views];
            }
            throw new Error(`unexpected SQL: ${flat}`);
        },
    };
}

const ENV = { SUPPLIER_PORTAL_URL: 'https://p.example/' };
const DOC = { id: 412, type: 'supplier-quote', supplier: 'Acme Co', version: 2, draft_container_name: '328' };

test('resolveDelivery: portal when a live supplier has exactly that name and the portal is there', async () => {
    const conn = fakeConn({ suppliers: [{ id: 7, name: 'Acme Co' }, { id: 8, name: 'ACME CO' }] });
    const r = await S.resolveDelivery(conn, DOC, ENV);
    assert.deepEqual(r, {
        delivery: 'portal', reason: null,
        portalUrl: 'https://p.example/requests-to-send?doc=412',
        registerUrl: 'https://p.example/register',
        suppliers: [{ id: 7, name: 'Acme Co' }],
    });
    // The portal's own comparison: byte for byte.
    const q = conn.queries.find(x => /FROM suppliers s/.test(x.sql));
    assert.match(q.sql, /utf8mb4_0900_bin/);
    assert.match(q.sql, /s\.deleted_at IS NULL/);
    assert.deepEqual(q.params, ['Acme Co']);
});

test('resolveDelivery: attached, with the reason, and no link offered', async () => {
    const noMatch = await S.resolveDelivery(fakeConn({ suppliers: [{ id: 8, name: 'ACME CO' }] }), DOC, ENV);
    assert.deepEqual(noMatch, { delivery: 'attached', reason: 'no_supplier', portalUrl: null, registerUrl: null, suppliers: [] });

    const noPortal = await S.resolveDelivery(fakeConn({ suppliers: [{ id: 7, name: 'Acme Co' }], portal: 'missing' }), DOC, ENV);
    assert.equal(noPortal.delivery, 'attached');
    assert.equal(noPortal.reason, 'portal_unavailable');
    assert.equal(noPortal.portalUrl, null);

    const unset = fakeConn({ suppliers: [{ id: 7, name: 'Acme Co' }] });
    assert.deepEqual(await S.resolveDelivery(unset, DOC, {}), { delivery: 'attached', reason: 'portal_url_unset', portalUrl: null, registerUrl: null, suppliers: [] });
    assert.equal(unset.queries.length, 0);

    // A forwarder quote is never looked up at all.
    const fq = fakeConn({ suppliers: [{ id: 7, name: 'Acme Co' }] });
    assert.equal((await S.resolveDelivery(fq, { ...DOC, type: 'forwarder-quote' }, ENV)).reason, 'not_request_to_send');
    assert.equal(fq.queries.length, 0);

    // A fault that is not "the portal is not here" is a fault.
    await assert.rejects(S.resolveDelivery(fakeConn({ suppliers: [{ id: 7, name: 'Acme Co' }], portal: 'broken' }), DOC, ENV), /boom/);
});

test('preparePortalSend: recipients pre-approved for the document\'s supplier, the block under the body, no attachment promised', async () => {
    const plan = { delivery: 'portal', reason: null, portalUrl: 'https://p.example/requests-to-send?doc=412', registerUrl: 'https://p.example/register', suppliers: [{ id: 7, name: 'Acme Co' }] };
    const input = {
        doc: DOC, plan, recipients: ['a@acme.cn', 'b@acme.cn'], fields: { etd: '2026-11-22', port: 'Shanghai' },
        bodyHtml: '<p>BODY</p>', subject: 'Request to send – 328', actorEmail: 'ops@x.com',
    };
    const calls = [];
    const granted = async (conn, args) => {
        calls.push(args);
        return { status: 'ok', suppliers: plan.suppliers, poSupplierMatched: true, users: [
            { email: 'a@acme.cn', outcome: 'invited', suppliersAdded: 1 },
            { email: 'b@acme.cn', outcome: 'already_active', suppliersAdded: 0 },
        ] };
    };
    const r = await S.preparePortalSend({}, input, { preapprove: granted });
    assert.equal(r.conflict, undefined);
    assert.equal(r.preapproved, true);
    assert.equal(r.portalAccess.status, 'ok');
    // Linked to the document's own supplier name, nothing wider.
    assert.deepEqual(calls[0], {
        recipients: ['a@acme.cn', 'b@acme.cn'], poSupplier: 'Acme Co', actorEmail: 'ops@x.com',
        note: 'Pre-approved from ShipLine: Request to send 328 v2 emailed by ops@x.com',
    });
    assert.match(r.emailHtml, /^<!DOCTYPE html>/);
    assert.ok(r.emailHtml.indexOf('<p>BODY</p>') < r.emailHtml.indexOf('requests-to-send?doc=412'));
    assert.match(r.emailHtml, /22 November 2026/);
    assert.match(r.emailHtml, /Shanghai/);
    assert.match(r.emailHtml, /already approved/);
    // A portal email asks for one thing: no confirm button.
    assert.doesNotMatch(r.emailHtml, /confirm-receipt/);

    // One blocked address (an admin's decision in JFPRO): the email still goes, without the promise.
    const blocked = await S.preparePortalSend({}, input, { preapprove: async () => ({
        status: 'ok', suppliers: plan.suppliers, poSupplierMatched: true,
        users: [{ email: 'a@acme.cn', outcome: 'invited', suppliersAdded: 1 }, { email: 'b@acme.cn', outcome: 'blocked', suppliersAdded: 0 }],
    }) });
    assert.equal(blocked.preapproved, false);
    assert.match(blocked.emailHtml, /our team will approve/);

    // Pre-approval broke: logged, reported, and the email still goes as a link.
    const logged = [];
    const failed = await S.preparePortalSend({}, input, {
        preapprove: async () => { throw Object.assign(new Error('deadlock'), { errno: 1213 }); },
        logError: (msg, meta) => logged.push([msg, meta]),
    });
    assert.deepEqual(failed.portalAccess, { status: 'error', suppliers: [], poSupplierMatched: false, users: [] });
    assert.equal(failed.preapproved, false);
    assert.match(failed.emailHtml, /our team will approve/);
    assert.equal(logged.length, 1);
    assert.equal(logged[0][1].documentId, 412);
});

test('preparePortalSend: the supplier or the portal went away since the check: handed back, nothing composed', async () => {
    const plan = { delivery: 'portal', reason: null, portalUrl: 'https://p.example/requests-to-send?doc=412', registerUrl: 'https://p.example/register', suppliers: [] };
    const input = { doc: DOC, plan, recipients: ['a@acme.cn'], fields: { etd: null, port: null }, bodyHtml: '<p>B</p>', subject: 'S' };
    const empty = status => async () => ({ status, suppliers: [], poSupplierMatched: false, users: [] });

    const gone = await S.preparePortalSend({}, input, { preapprove: empty('no_supplier') });
    assert.equal(gone.emailHtml, undefined);
    assert.equal(gone.conflict.code, 'DELIVERY_CHANGED');
    assert.equal(gone.conflict.delivery, 'attached');
    assert.equal(gone.conflict.reason, 'no_supplier');

    const down = await S.preparePortalSend({}, input, { preapprove: empty('unavailable') });
    assert.equal(down.conflict.reason, 'portal_unavailable');
});

// ── A new version of a request the supplier already has ─────────────────────
// (user, 2026-11-13: "very important to let them know about new changes or V2 of requests")

const snap = lines => JSON.stringify({ v: 1, source: 'generate', lines });
const V1 = { id: 30, version: 1, lines_json: snap([
    { jfCode: 'JF1049', sku: 'Gloves', lot: 'L1', units: 500, supplier: 'Acme Co' },
    { jfCode: 'JF2000', sku: 'Masks', lot: null, units: 1200, supplier: 'Acme Co' },
    { jfCode: 'JF4000', sku: 'Caps', lot: 'C1', units: 80, supplier: 'Acme Co' },
]) };
const V2 = { id: 31, version: 2, lines_json: snap([
    { jfCode: 'JF1049', sku: 'Gloves', lot: 'L1', units: 620, supplier: 'Acme Co' },
    { jfCode: 'JF3000', sku: 'Wipes', lot: 'B7', units: 40, supplier: 'Acme Co' },
    { jfCode: 'JF4000', sku: 'Caps', lot: 'C2', units: 80, supplier: 'Acme Co' },
]) };
const CHANGES = [
    { kind: 'units', item: 'JF1049', lot: 'L1', unitsWas: 500, unitsNow: 620 },
    { kind: 'removed', item: 'JF2000', lot: null, unitsWas: 1200 },
    { kind: 'lot', item: 'JF4000', lotWas: 'C1', lot: 'C2' },
    { kind: 'added', item: 'JF3000', lot: 'B7', unitsNow: 40 },
];

test('requestUpdate: which version it replaces, and what a supplier acts on that changed', () => {
    assert.deepEqual(L.requestUpdate(V1, V2), { replacesVersion: 1, changes: CHANGES });
    // Nothing they act on changed (a carton size, a weight): an update all the same, with an empty list.
    assert.deepEqual(L.requestUpdate(V1, { ...V2, lines_json: V1.lines_json }), { replacesVersion: 1, changes: [] });
    // No record of what either version printed: an update, with what changed unknown.
    assert.deepEqual(L.requestUpdate({ ...V1, lines_json: null }, V2), { replacesVersion: 1, changes: null });
    assert.deepEqual(L.requestUpdate(V1, { ...V2, lines_json: 'not json' }), { replacesVersion: 1, changes: null });
    // The first version anyone was sent is not an update.
    assert.equal(L.requestUpdate(null, V2), null);
    assert.equal(L.requestUpdate(undefined, V2), null);
});

test('changeText: one change as a supplier reads it, in English and Chinese', () => {
    assert.deepEqual(CHANGES.map(L.changeText), [
        { en: 'JF1049 (lot L1): 500 → 620 units', zh: 'JF1049（批号 L1）：数量 500 → 620' },
        { en: 'JF2000: removed (was 1,200 units)', zh: 'JF2000：已取消（原数量 1,200）' },
        { en: 'JF4000: lot C1 → C2', zh: 'JF4000：批号 C1 → C2' },
        { en: 'JF3000 (lot B7): added, 40 units', zh: 'JF3000（批号 B7）：新增，数量 40' },
    ]);
    // A lot that changed together with its quantity, and a lot that was not there before.
    assert.deepEqual(L.changeText({ kind: 'lot', item: 'JF1', lotWas: null, lot: 'A', unitsWas: 10, unitsNow: 12 }),
        { en: 'JF1: lot none → A, 10 → 12 units', zh: 'JF1：批号 无 → A，数量 10 → 12' });
    assert.deepEqual(L.changeText({ kind: 'units', item: 'JF1', lot: null, unitsWas: 10, unitsNow: 12 }),
        { en: 'JF1: 10 → 12 units', zh: 'JF1：数量 10 → 12' });
});

test('updateBlockHtml: says it replaces the earlier version, and lists what changed', () => {
    const html = L.updateBlockHtml({ version: 2, replacesVersion: 1, changes: CHANGES });
    assert.match(html, /This is version 2 of this request/);
    assert.match(html, /replaces version 1/);
    assert.match(html, /这是本通知的第 2 版/);
    assert.match(html, /取代第 1 版/);
    assert.match(html, /JF1049 \(lot L1\): 500 → 620 units/);
    assert.match(html, /JF2000：已取消（原数量 1,200）/);
    assert.equal(html.split('<li').length - 1, 4);
    // An item's name is our own text: escaped.
    assert.match(L.updateBlockHtml({ version: 2, replacesVersion: 1, changes: [{ kind: 'added', item: 'Gloves <XL>', lot: null, unitsNow: 5 }] }), /Gloves &lt;XL&gt;: added, 5 units/);

    // Nothing they act on changed: said, so nobody goes looking for a difference.
    const same = L.updateBlockHtml({ version: 3, replacesVersion: 2, changes: [] });
    assert.match(same, /items, quantities and lots are the same as in version 2/);
    assert.equal(same.split('<li').length - 1, 0);
    // What changed is not known: only that it replaces the earlier one.
    const unknown = L.updateBlockHtml({ version: 2, replacesVersion: 1, changes: null });
    assert.match(unknown, /replaces version 1/);
    assert.doesNotMatch(unknown, /are the same/);
    assert.equal(unknown.split('<li').length - 1, 0);

    // A long list is cut, and says by how much.
    const many = Array.from({ length: 34 }, (_, i) => ({ kind: 'added', item: `JF${i}`, lot: null, unitsNow: 1 }));
    const cut = L.updateBlockHtml({ version: 2, replacesVersion: 1, changes: many });
    assert.equal(cut.split('<li').length - 1, 30);
    assert.match(cut, /and 4 more/);
    assert.match(cut, /另有 4 项/);
});

test('storedUpdate: what a send row keeps of it, and reads back', () => {
    const update = { replacesVersion: 1, changes: CHANGES };
    assert.deepEqual(L.storedUpdate(JSON.stringify(update)), update);
    assert.deepEqual(L.storedUpdate(update), update);
    assert.deepEqual(L.storedUpdate({ replacesVersion: 2, changes: null }), { replacesVersion: 2, changes: null });
    // Anything that is not one reads as "not an update".
    for (const bad of [null, undefined, '', 'x', '{}', { replacesVersion: 0 }, { replacesVersion: 'one' }, []]) assert.equal(L.storedUpdate(bad), null, JSON.stringify(bad));
    // A change of a kind this code does not know is dropped, not shown as something else.
    assert.deepEqual(L.storedUpdate({ replacesVersion: 1, changes: [{ kind: 'colour', item: 'x' }, CHANGES[0]] }), { replacesVersion: 1, changes: [CHANGES[0]] });
});

test('the update template: for the same dialog, says it is an update, promises no attachment', () => {
    const t = DEFAULT_EMAIL_TEMPLATES.find(x => x.key === L.UPDATE_TEMPLATE_KEY);
    assert.equal(L.UPDATE_TEMPLATE_KEY, 'request_to_send_update_portal');
    assert.equal(t.category, 'Supplier Quote');
    assert.match(t.subject, /^UPDATED/);
    assert.match(t.subject, /\{draftContainerName\}/);
    assert.match(t.subject, /\{version\}/);
    assert.match(t.bodyHtml, /\[etd:date\]/);
    assert.match(t.bodyHtml, /\[port:port\]/);
    assert.match(t.bodyHtml, /replaces/);
    assert.doesNotMatch(t.bodyHtml, /attached packing list|see attached|see the attached/i);
});

test('resolveUpdate: the latest EARLIER version of the same request that was emailed', async () => {
    const doc = { ...DOC, id: 31, version: 2, shipment_id: 163, lines_json: V2.lines_json };
    const conn = fakeConn({ earlier: [V1] });
    assert.deepEqual(await S.resolveUpdate(conn, doc), { replacesVersion: 1, changes: CHANGES });
    const q = conn.queries[0];
    assert.match(q.sql, /p\.type = 'supplier-quote' AND p\.deleted_at IS NULL/);
    // The same supplier's request for the same container: by its name, or by its shipment.
    assert.match(q.sql, /p\.supplier <=> \?/);
    assert.match(q.sql, /\(p\.draft_container_name = \? OR \(p\.shipment_id IS NOT NULL AND p\.shipment_id = \?\)\)/);
    // Earlier, and really sent: a version nobody was emailed replaces nothing.
    assert.match(q.sql, /p\.id <> \? AND p\.version < \?/);
    assert.match(q.sql, /EXISTS \(SELECT 1 FROM draft_container_document_sends s WHERE s\.draft_container_document_id = p\.id\)/);
    assert.match(q.sql, /ORDER BY p\.version DESC, p\.id DESC LIMIT 1$/);
    assert.deepEqual(q.params, [31, 2, 'Acme Co', '328', 163]);

    // Nothing earlier was sent: not an update.
    assert.equal(await S.resolveUpdate(fakeConn(), doc), null);
    // A forwarder quote is never one, and is not even looked up.
    const fq = fakeConn({ earlier: [V1] });
    assert.equal(await S.resolveUpdate(fq, { ...doc, type: 'forwarder-quote' }), null);
    assert.equal(fq.queries.length, 0);
});

test('preparePortalSend: an update says so above the portal block', async () => {
    const plan = { delivery: 'portal', reason: null, portalUrl: 'https://p.example/requests-to-send?doc=412', registerUrl: 'https://p.example/register', suppliers: [{ id: 7, name: 'Acme Co' }] };
    const granted = async () => ({ status: 'ok', suppliers: plan.suppliers, poSupplierMatched: true, users: [{ email: 'a@acme.cn', outcome: 'already_active', suppliersAdded: 0 }] });
    const input = { doc: DOC, plan, recipients: ['a@acme.cn'], fields: { etd: null, port: null }, bodyHtml: '<p>BODY</p>', subject: 'S' };
    const r = await S.preparePortalSend({}, { ...input, update: { replacesVersion: 1, changes: CHANGES } }, { preapprove: granted });
    const body = r.emailHtml.indexOf('<p>BODY</p>');
    const note = r.emailHtml.indexOf('replaces version 1');
    const link = r.emailHtml.indexOf('requests-to-send?doc=412');
    assert.ok(body >= 0 && body < note && note < link, `order ${body} ${note} ${link}`);
    // Not an update: nothing is said about versions.
    const plain = await S.preparePortalSend({}, input, { preapprove: granted });
    assert.doesNotMatch(plain.emailHtml, /replaces version/);
});

test('saveSendUpdate / loadSendUpdates: kept on the send row; a table without the column still works', async () => {
    const update = { replacesVersion: 1, changes: CHANGES };
    const conn = fakeConn();
    await S.saveSendUpdate(conn, 77, update);
    assert.match(conn.queries[0].sql, /^UPDATE draft_container_document_sends SET request_update = \? WHERE id = \?$/);
    assert.deepEqual(conn.queries[0].params, [JSON.stringify(update), 77]);
    // Not an update: nothing is written.
    const none = fakeConn();
    await S.saveSendUpdate(none, 77, null);
    assert.equal(none.queries.length, 0);
    // The column is not there yet: the send stands without it.
    await S.saveSendUpdate(fakeConn({ updateColumn: false }), 77, update);
    await assert.rejects(S.saveSendUpdate({ query: async () => { throw Object.assign(new Error('gone away'), { errno: 2006 }); } }, 77, update), /gone away/);

    const read = fakeConn({ sends: [{ id: 5, request_update: JSON.stringify(update) }, { id: 6, request_update: null }] });
    const got = await S.loadSendUpdates(read, [412, 412, null]);
    assert.deepEqual(read.queries[0].params, [412]);
    assert.deepEqual(got.get(5), update);
    assert.equal(got.has(6), false);
    assert.equal((await S.loadSendUpdates(fakeConn({ updateColumn: false }), [412])).size, 0);
    await assert.rejects(S.loadSendUpdates({ query: async () => { throw Object.assign(new Error('gone away'), { errno: 2006 }); } }, [412]), /gone away/);
    const empty = fakeConn();
    assert.equal((await S.loadSendUpdates(empty, [])).size, 0);
    assert.equal(empty.queries.length, 0);
});

test('insertSend: stores how it went out and what the email said; an unmigrated table still records the send', async () => {
    const row = {
        documentId: 412, sentTo: ['a@x.com'], subject: 'S', frontMessageUid: 'm', frontConversationId: 'c', sentByEmail: 'ops@x.com',
        delivery: 'portal', etd: '2026-11-22', port: 'Shanghai',
    };
    const conn = fakeConn();
    assert.equal(await S.insertSend(conn, row), 77);
    assert.equal(conn.queries.length, 1);
    assert.match(conn.queries[0].sql, /\(draft_container_document_id, sent_to, subject, front_message_uid, front_conversation_id, sent_by_email, delivery, request_etd, request_port\)/);
    assert.deepEqual(conn.queries[0].params, [412, '["a@x.com"]', 'S', 'm', 'c', 'ops@x.com', 'portal', '2026-11-22', 'Shanghai']);

    const old = fakeConn({ sendColumns: false });
    assert.equal(await S.insertSend(old, row), 77);
    assert.equal(old.queries.length, 2);
    assert.doesNotMatch(old.queries[1].sql, /delivery/);
    assert.deepEqual(old.queries[1].params, [412, '["a@x.com"]', 'S', 'm', 'c', 'ops@x.com']);

    // Any other failure is not swallowed, and nothing is written in its place: a send
    // stored without what it said would look like one that said nothing.
    const attempts = [];
    const broken = { query: async (sql) => { attempts.push(sql); if (attempts.length === 1) throw Object.assign(new Error('Data too long'), { errno: 1406 }); return [{ insertId: 78 }]; } };
    await assert.rejects(S.insertSend(broken, row), /Data too long/);
    assert.equal(attempts.length, 1);
});

test('loadSendFacts: per send; empty before the migration', async () => {
    const conn = fakeConn({ sends: [
        { id: 5, delivery: 'portal', request_etd: '2026-11-22', request_port: 'Shanghai' },
        { id: 6, delivery: null, request_etd: null, request_port: null },
    ] });
    const facts = await S.loadSendFacts(conn, [412, 412, 413, null]);
    assert.deepEqual(conn.queries[0].params, [412, 413]);
    // The day as text, never a Date (a DATE column read as a Date shifts with the server's zone).
    assert.match(conn.queries[0].sql, /DATE_FORMAT\(request_etd, '%Y-%m-%d'\) AS request_etd/);
    assert.deepEqual(facts.get(5), { delivery: 'portal', etd: '2026-11-22', port: 'Shanghai' });
    assert.deepEqual(facts.get(6), { delivery: 'attached', etd: null, port: null });

    assert.equal((await S.loadSendFacts(fakeConn({ sendColumns: false }), [412])).size, 0);
    // Only "the columns are not there yet" reads as empty: any other fault is a fault.
    await assert.rejects(S.loadSendFacts({ query: async () => { throw Object.assign(new Error('gone away'), { errno: 2006 }); } }, [412]), /gone away/);
    const none = fakeConn();
    assert.equal((await S.loadSendFacts(none, [])).size, 0);
    assert.equal(none.queries.length, 0);
});

test('loadSupplierQuoteViews: who opened a Request to send on the portal', async () => {
    const conn = fakeConn({ views: [
        { viewed_id: 412, user_email: 'a@s.cn', view_count: 2, first_viewed_at: '2026-11-13T08:00:00Z', last_viewed_at: '2026-11-13T09:00:00Z' },
    ] });
    const views = await loadSupplierQuoteViews(conn, [412, 413]);
    assert.match(conn.queries[0].sql, /SELECT draft_container_document_id AS viewed_id/);
    assert.match(conn.queries[0].sql, /\.supplier_quote_views/);
    assert.deepEqual(views.get(412), [{ email: 'a@s.cn', firstViewedAt: '2026-11-13T08:00:00Z', lastViewedAt: '2026-11-13T09:00:00Z', count: 2 }]);
    assert.equal(views.has(413), false);
    assert.equal((await loadSupplierQuoteViews(fakeConn({ portal: 'missing' }), [412])).size, 0);
});

test('the receipt: confirmed by a view of that request or a later version of it, and chased until then', () => {
    const step = Object.fromEntries(R.portalSyncSteps('`jportal`'));
    const sql = step.request_viewed;
    assert.ok(sql, 'request_viewed is a sync step');
    assert.match(sql, /^\s*UPDATE email_receipts r/);
    assert.match(sql, /r\.confirm_via = 'portal' AND r\.confirmed_at IS NULL/);
    assert.match(sql, /JOIN draft_container_document_sends s ON s\.id = r\.send_id/);
    assert.match(sql, /r\.send_table = 'draft_container_document_sends'/);
    assert.match(sql, /`jportal`\.supplier_quote_views v/);
    // What was opened before the email went out does not confirm it.
    assert.match(sql, /v\.last_viewed_at >= r\.created_at/);
    // A later version of the same container and supplier counts; another supplier's never.
    assert.match(sql, /d2\.draft_container_name = d\.draft_container_name/);
    assert.match(sql, /d2\.supplier <=> d\.supplier/);
    assert.match(sql, /d2\.id >= d\.id/);
    assert.match(sql, /d2\.type = 'supplier-quote'/);
    assert.equal(R.CHASED_PORTAL_STEPS.draft_container_document_sends, 'request_viewed');
});

test('the receipt: an upload for that shipment confirms it too (a packing list or an invoice, after the email)', () => {
    const steps = R.portalSyncSteps('`jportal`');
    // After the view step, so a view and an upload on the same day keep the earlier of them.
    const names = steps.map(s => s[0]);
    assert.deepEqual(names.slice(names.indexOf('request_viewed'), names.indexOf('request_viewed') + 3), ['request_viewed', 'request_pl_uploaded', 'request_ci_uploaded']);
    const step = Object.fromEntries(steps);

    for (const name of ['request_pl_uploaded', 'request_ci_uploaded']) {
        const sql = step[name].replace(/\s+/g, ' ');
        assert.match(sql, /^ UPDATE email_receipts r JOIN draft_container_document_sends s ON s\.id = r\.send_id JOIN draft_container_documents d ON d\.id = s\.draft_container_document_id/, name);
        assert.match(sql, /r\.confirm_via = 'portal' AND r\.confirmed_at IS NULL AND r\.send_table = 'draft_container_document_sends'/, name);
        // Only a Request to send: a forwarder quote is never confirmed by a supplier's upload.
        assert.match(sql, /AND d\.type = 'supplier-quote'/, name);
        // Sent to our team (not a draft the supplier never sent, nor one they threw away), after the email.
        assert.match(sql, /u\.status NOT IN \('draft', 'discarded'\)/, name);
        assert.match(sql, /u\.submitted_at >= r\.created_at/, name);
        // By someone who could see this request: their suppliers include one named exactly as it.
        assert.match(sql, /CONVERT\(sup\.name USING utf8mb4\) COLLATE utf8mb4_0900_bin = CONVERT\(d\.supplier USING utf8mb4\) COLLATE utf8mb4_0900_bin/, name);
        assert.match(sql, /u\.supplier_id = sup\.id OR JSON_CONTAINS\(COALESCE\(u\.group_supplier_ids, JSON_ARRAY\(\)\), CAST\(sup\.id AS JSON\)\)/, name);
        assert.match(sql, /SET r\.confirmed_at = \(SELECT MIN\(u\.submitted_at\) FROM/, name);
    }

    // A packing list: the container our team filed it in; until they decide, the
    // one it was matched to, and only a confident match.
    const pl = step.request_pl_uploaded.replace(/\s+/g, ' ');
    assert.match(pl, /`jportal`\.pl_submissions u/);
    assert.match(pl, /u\.confirmed_kind = 'booked' AND u\.confirmed_container_number = \(SELECT sh\.reference FROM shipments sh WHERE sh\.id = d\.shipment_id\)/);
    assert.match(pl, /u\.confirmed_kind IS NULL AND u\.match_confidence = 'confident' AND u\.suggested_kind = 'booked' AND u\.suggested_container_number = \(SELECT sh\.reference FROM shipments sh WHERE sh\.id = d\.shipment_id\)/);
    // A draft has no reference yet: its registry row, found by the shipment or by its name.
    assert.match(pl, /u\.confirmed_kind = 'draft' AND u\.confirmed_draft_id IN \(SELECT dc\.id FROM draft_containers dc WHERE dc\.shipment_id = d\.shipment_id OR dc\.name = d\.draft_container_name\)/);
    assert.match(pl, /u\.confirmed_kind IS NULL AND u\.match_confidence = 'confident' AND u\.suggested_kind = 'draft' AND u\.suggested_draft_id IN \(SELECT dc\.id FROM draft_containers dc/);
    assert.doesNotMatch(pl, /ci_submissions/);

    // An invoice: the shipment it was filed on; until then a confident match.
    const ci = step.request_ci_uploaded.replace(/\s+/g, ' ');
    assert.match(ci, /`jportal`\.ci_submissions u/);
    assert.match(ci, /u\.confirmed_shipment_id = d\.shipment_id/);
    assert.match(ci, /u\.confirmed_shipment_id IS NULL AND u\.match_confidence = 'confident' AND u\.suggested_shipment_id = d\.shipment_id/);
    assert.doesNotMatch(ci, /pl_submissions/);
});

// ── When the chase ends without the supplier doing anything ─────────────────

const flat = sql => String(sql).replace(/\s+/g, ' ').trim();
const LETTERS = column => `REGEXP_REPLACE(LOWER(CONVERT(${column} USING utf8mb4)), '[^[:alnum:]]', '')`;

test('reminders stop: the reasons, in the order they are tried', () => {
    assert.deepEqual(R.REMINDERS_STOPPED, ['superseded', 'shipped', 'documents']);
    const sql = flat(R.requestRemindersStoppedSql());
    const at = reason => sql.indexOf(`THEN '${reason}'`);
    assert.ok(at('superseded') > 0, 'superseded');
    assert.ok(at('superseded') < at('shipped'), 'superseded before shipped');
    assert.ok(at('shipped') < at('documents'), 'shipped before documents');
    // Nothing else is ever answered: three reasons, and no ELSE.
    assert.equal(sql.split(' THEN ').length - 1, 3);
    assert.doesNotMatch(sql, / ELSE /);
});

test('reminders stop: only for the email of a Request to send, read through its own send', () => {
    const sql = flat(R.requestRemindersStoppedSql());
    assert.ok(sql.startsWith('(SELECT CASE WHEN EXISTS (SELECT 1 FROM draft_container_documents n '), sql.slice(0, 80));
    // One row at most (a send's id), and none for any other kind of email or a
    // forwarder's document: the expression is then NULL and the email is chased.
    assert.ok(sql.endsWith(
        " THEN 'documents' END FROM draft_container_document_sends s"
        + ' JOIN draft_container_documents d ON d.id = s.draft_container_document_id'
        + " WHERE r.send_table = 'draft_container_document_sends' AND s.id = r.send_id AND d.type = 'supplier-quote')"
    ), sql.slice(-260));
});

test('reminders stop: a later version of the same request has been emailed (the rule that makes it an update)', () => {
    const sql = flat(R.requestRemindersStoppedSql());
    assert.ok(sql.includes(
        "WHEN EXISTS (SELECT 1 FROM draft_container_documents n WHERE n.type = 'supplier-quote' AND n.deleted_at IS NULL"
        + ' AND n.id <> d.id AND n.version > d.version'
        + ' AND n.supplier <=> d.supplier'
        + ' AND (n.draft_container_name = d.draft_container_name OR (n.shipment_id IS NOT NULL AND n.shipment_id = d.shipment_id))'
        + ' AND EXISTS (SELECT 1 FROM draft_container_document_sends ns WHERE ns.draft_container_document_id = n.id))'
        + " THEN 'superseded'"
    ), sql);
});

test('reminders stop: the shipment has left (the portal\'s own "shipped")', () => {
    const sql = flat(R.requestRemindersStoppedSql());
    assert.ok(sql.includes(
        "WHEN EXISTS (SELECT 1 FROM shipments sh WHERE sh.id = d.shipment_id AND sh.stage IN ('IN_TRANSIT', 'ARRIVED', 'CLOSED'))"
        + ' OR EXISTS (SELECT 1 FROM orders o WHERE o.shipment_id = d.shipment_id AND o.deleted_at IS NULL'
        + " AND o.status IN ('ON_SEA', 'ON_AIR', 'ARRIVED_AT_WAREHOUSE', 'RECEIVED', 'PARTIALLY_RECEIVED'))"
        + " THEN 'shipped'"
    ), sql);
});

test('reminders stop: its packing list AND its invoice are filed for that shipment under that supplier', () => {
    const sql = flat(R.requestRemindersStoppedSql());
    const same = column => `${LETTERS(column)} = ${LETTERS('d.supplier')} AND ${LETTERS('d.supplier')} <> ''`;
    assert.ok(sql.includes(
        "WHEN EXISTS (SELECT 1 FROM packing_lists pl WHERE pl.deleted_at IS NULL AND pl.container_kind = 'booked'"
        + ' AND pl.container_number = (SELECT sh.reference FROM shipments sh WHERE sh.id = d.shipment_id)'
        + ` AND ${same('pl.supplier_name')})`
        + " AND EXISTS (SELECT 1 FROM shipment_payment_documents c WHERE c.deleted_at IS NULL AND c.doc_kind = 'balance_invoice'"
        + ' AND c.shipment_id = d.shipment_id'
        + ` AND ${same('c.supplier_name')})`
        + " THEN 'documents'"
    ), sql);
});

test('remindersStoppedOf: what a receipt row tells ShipLine, and only while there is something to stop', () => {
    const row = (reminders_stopped, more = {}) => ({ confirm_via: 'portal', action_url: 'https://p.example/requests-to-send?doc=1', confirmed_at: null, reminders_stopped, ...more });
    assert.equal(R.remindersStoppedOf(row('superseded')), 'superseded');
    assert.equal(R.remindersStoppedOf(row('shipped')), 'shipped');
    assert.equal(R.remindersStoppedOf(row('documents')), 'documents');
    // The attached kind (the confirm button) is chased too, so it stops too.
    assert.equal(R.remindersStoppedOf({ confirm_via: 'email', confirmed_at: null, reminders_stopped: 'shipped' }), 'shipped');
    // Still chased, or a server that could not say.
    assert.equal(R.remindersStoppedOf(row(null)), null);
    assert.equal(R.remindersStoppedOf(row(undefined)), null);
    // Not one of the reasons: never passed on as typed.
    assert.equal(R.remindersStoppedOf(row('whatever')), null);
    // Confirmed: nothing was being sent, so nothing stopped.
    assert.equal(R.remindersStoppedOf(row('shipped', { confirmed_at: new Date('2026-11-13T10:00:00Z') })), null);
    // Never reminded in the first place (a notice, a link no reminder can repeat).
    assert.equal(R.remindersStoppedOf(row('shipped', { confirm_via: 'none' })), null);
    assert.equal(R.remindersStoppedOf(row('shipped', { action_url: null })), null);
    assert.equal(R.remindersStoppedOf(null), null);
});
