'use strict';

// End-to-end check of freight quotes (docs/freight-quotes.md) against the TEST
// database, both halves for real, as tools/smoke-signed-pi-portal-send.js does
// for the signed-PI send:
//
//   1. shipping: the real /api/v1/forwarders, /freight-quotes and
//      /freight-bookings routes (the Express app in-process, auth bypassed as
//      local@dev);
//   2. portal: the real jportal `availability` (the emailed link) and
//      `supplier` (signed in) Lambda handlers in-process (../portal), on a pool
//      that logs in as the portal's OWN runtime login (jportal_app, from
//      ../portal/.env). So every statement the portal sends runs under its
//      column grants: a missing grant is a failure here.
//
//   node tools/smoke-freight-quotes.js
//
// It walks the whole flow: two forwarders, one send to both (and its retry),
// the forwarder's answer through the link, an answer typed in by staff, a
// booking, the forwarder's confirmation closing the other link as filled, the
// stock movements (each priced on its own through the link, one added after
// the booking with its email and its retry, a price typed in by staff, a
// removal), the signed-in list, a container's contents by supplier and the contents email
// (and its retry), the documents sent as the forwarder's signed-in page lists
// them, the cancel that reopens everything, and removing a forwarder.
//
// NOTHING LEAVES THIS MACHINE: global fetch is replaced, the Front call is
// answered by a stub (recording what would have been sent) and any other
// outbound request throws. The contents email is sent with
// attachDocuments: false, so no PDF is generated and nothing is written to S3.
// Everything it writes on TEST (forwarders, requests, options, bookings,
// receipts, audit rows, the portal user) is tagged and deleted at the end, pass
// or fail. Refuses to run unless both env files point at explorer-test.
//
// Not covered: the PDF attachments of the contents email, Front itself, the
// WAF, and Cognito (the claims are fabricated).

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { pathToFileURL } = require('url');
const dotenv = require('dotenv');

const ROOT = path.join(__dirname, '..');
const PORTAL = process.env.PORTAL_REPO || path.join(ROOT, '..', 'portal');

dotenv.config({ path: path.join(ROOT, '.env.test'), override: true });
const host = String(process.env.DB_HOST || '').toLowerCase();
if (!host.includes('explorer-test')) {
    console.error(`[smoke] REFUSED: .env.test DB_HOST is "${host}", not explorer-test.`);
    process.exit(1);
}
// The portal's own runtime login, so its statements run under its grants.
const portalEnv = dotenv.parse(fs.readFileSync(path.join(PORTAL, '.env')));
if (!String(portalEnv.DB_HOST || '').toLowerCase().includes('explorer-test')) {
    console.error(`[smoke] REFUSED: ${path.join(PORTAL, '.env')} DB_HOST is "${portalEnv.DB_HOST}", not explorer-test.`);
    process.exit(1);
}
process.env.IS_OFFLINE = '1';
process.env.LOCAL_USER_TYPE = 'admin';
process.env.SES_FROM_ARN = 'arn:aws:ses:eu-north-1:000000000000:identity/smoke@example.com';
process.env.SUPPLIER_PORTAL_URL = process.env.SUPPLIER_PORTAL_URL || 'https://jportal-test.built-form.co.uk';
const JP = '`' + (process.env.JPORTAL_DB_SCHEMA || 'jportal') + '`';

// ── network: SES stubbed, everything else external refused ─────────────────
const realFetch = global.fetch;
const frontCalls = [];
require('../src/services/email-send')._setSendImplForTests(async (region, input) => {
    const s = input.Content.Simple;
    const n = frontCalls.push({
        to: input.Destination.ToAddresses, subject: s.Subject.Data, body: String(s.Body.Html.Data),
        attachments: (s.Attachments || []).length,
    });
    return { MessageId: `smoke_msg_${n}` };
});
global.fetch = async (url, init) => {
    const u = String(url);
    if (u.startsWith('http://127.0.0.1:')) return realFetch(url, init);
    throw new Error(`[smoke] blocked outbound fetch to ${u}`);
};

const mysql = require('mysql2/promise');
const { getPool, closePool } = require('../src/db');
const { syncPortalReceipts } = require('../src/lib/email-receipt');
const F = require('../src/lib/freight-quotes');

let pass = 0, fail = 0;
const failures = [];
function check(label, cond, detail) {
    if (cond) { pass++; console.log(`  PASS  ${label}`); }
    else {
        fail++; failures.push(label);
        console.log(`  FAIL  ${label}${detail !== undefined ? `\n        got: ${(typeof detail === 'string' ? detail : JSON.stringify(detail)).slice(0, 600)}` : ''}`);
    }
}

const TAG = `smk${Date.now().toString(36)}`;
const DOM = `${TAG}.invalid`;
const addr = who => `${who}@${DOM}`;
const forwarderIds = [];
// A delivery option in staff's own words, and the goods to be collected.
const DELIVERY = `Deliver to Unit 5, tail-lift ${TAG}`;
// The first still carries the "to" an older ShipLine sends: it is ignored.
const MOVEMENTS = [
    { from: `Acme Medical ${TAG}`, to: 'Ningbo port', cbm: 12.5, weightKg: 2400 },
    { from: 'Best Gloves', cbm: '3.25', weightKg: '610.5' },
];

async function q(sql, params) { return (await getPool().query(sql, params))[0]; }

let baseUrl;
async function api(method, url, body) {
    const res = await realFetch(`${baseUrl}${url}`, {
        method, headers: { 'content-type': 'application/json' }, body: body ? JSON.stringify(body) : undefined,
    });
    return { status: res.status, body: await res.json().catch(() => null) };
}

let portalLink, portalSupplier, seq = 0;
const parsePortal = (res) => {
    let parsed = null;
    try { parsed = JSON.parse(res.body); } catch { /* left null */ }
    return { status: res.statusCode, data: parsed?.data, code: parsed?.code, details: parsed?.details, raw: String(res.body).slice(0, 300) };
};
// The emailed link: no claims, the token is the permission.
// `part`: true or 'booking' for the booking answer, 'stock-movements' for the prices.
async function link(method, token, body, part = false) {
    const suffix = part === true ? '/booking' : (part ? `/${part}` : '');
    return parsePortal(await portalLink.handler({
        httpMethod: method, resource: `/api/freight/{token}${suffix}`,
        pathParameters: { token }, queryStringParameters: null,
        headers: { 'user-agent': 'smoke-freight-quotes', 'X-Requested-With': 'jportal' },
        requestContext: { requestId: `${TAG}-${++seq}` },
        body: body ? JSON.stringify(body) : null, isBase64Encoded: false,
    }, {}));
}
// Signed in: fabricated Cognito claims for one of the forwarder's contacts.
async function signedIn(claims, method, resource, id, body) {
    return parsePortal(await portalSupplier.handler({
        httpMethod: method, resource, pathParameters: id === undefined ? null : { id: String(id) }, queryStringParameters: null,
        headers: { 'user-agent': 'smoke-freight-quotes', 'X-Requested-With': 'jportal' },
        requestContext: { requestId: `${TAG}-${++seq}`, authorizer: { claims } },
        body: body ? JSON.stringify(body) : null, isBase64Encoded: false,
    }, {}));
}

async function main() {
    console.log(`TEST DB ${process.env.DB_HOST} / ${process.env.DB_NAME} as ${process.env.DB_USER}; portal as ${portalEnv.DB_USER} on ${portalEnv.DB_NAME}   tag ${TAG}\n`);

    const { app } = require('../src/handlers/orders');
    const server = await new Promise(r => { const s = app.listen(0, '127.0.0.1', () => r(s)); });
    baseUrl = `http://127.0.0.1:${server.address().port}`;

    const db = await import(pathToFileURL(path.join(PORTAL, 'src', 'db.js')).href);
    const portalPool = mysql.createPool({
        host: portalEnv.DB_HOST, user: portalEnv.DB_USER, password: portalEnv.DB_PASSWORD,
        port: Number(portalEnv.DB_PORT) || 3306, database: portalEnv.DB_NAME,
        ssl: { rejectUnauthorized: false }, connectionLimit: 1, dateStrings: ['DATE'], timezone: 'Z', stringifyObjects: true,
    });
    db._setPoolForTests(portalPool);
    portalLink = await import(pathToFileURL(path.join(PORTAL, 'src', 'handlers', 'availability.js')).href);
    portalSupplier = await import(pathToFileURL(path.join(PORTAL, 'src', 'handlers', 'supplier.js')).href);

    const today = new Date().toISOString().slice(0, 10);
    const windowStart = F.addDays(today, 20);
    const windowEnd = F.addDays(today, 50);
    const option = (over = {}) => ({
        loadingDate: F.addDays(today, 24), sailingDate: F.addDays(today, 28), transitDays: 32, portFrom: 'Ningbo', portTo: 'Felixstowe',
        service: DELIVERY, price: '1850', currency: 'USD', ...over,
    });

    try {
        // ── 0. the delivery option list ──────────────────────────────────────
        console.log('0. Delivery options');
        const listBefore = await api('GET', '/api/v1/freight-delivery-options');
        check('the list is read (the four seeded entries, unless staff changed them)', listBefore.status === 200 && Array.isArray(listBefore.body?.deliveryOptions), listBefore);
        const addedOption = await api('POST', '/api/v1/freight-delivery-options', { label: `  Smoke option ${TAG} ` });
        const myOption = addedOption.body?.deliveryOptions?.find(o => o.label === `Smoke option ${TAG}`);
        check('a delivery option is added, trimmed', addedOption.status === 201 && Boolean(myOption), addedOption);
        check('the same text again is refused', (await api('POST', '/api/v1/freight-delivery-options', { label: `smoke OPTION ${TAG}` })).body?.code === 'DUPLICATE');
        const reworded = await api('PATCH', `/api/v1/freight-delivery-options/${myOption?.id}`, { label: DELIVERY });
        check('it is reworded', reworded.status === 200 && reworded.body.deliveryOptions.some(o => o.id === myOption.id && o.label === DELIVERY), reworded);


        // ── 1. forwarders ────────────────────────────────────────────────────
        console.log('1. Forwarders');
        const f1 = await api('POST', '/api/v1/forwarders', { name: `Smoke One ${TAG}`, contacts: [{ name: 'Amy', email: addr('amy').toUpperCase() }] });
        check('a forwarder is created, its address lower-cased', f1.status === 201 && f1.body?.forwarder?.contacts?.[0]?.email === addr('amy'), f1);
        const f2 = await api('POST', '/api/v1/forwarders', { name: `Smoke Two ${TAG}`, contacts: [{ email: addr('bo') }] });
        check('a second forwarder is created', f2.status === 201, f2);
        const F1 = f1.body?.forwarder?.id; const F2 = f2.body?.forwarder?.id;
        forwarderIds.push(F1, F2);
        const edited = await api('PATCH', `/api/v1/forwarders/${F1}`, { name: `Smoke One ${TAG}`, notes: 'smoke', contacts: [{ name: 'Amy', email: addr('amy') }, { email: addr('ops') }] });
        check('editing adds a contact', edited.status === 200 && edited.body?.forwarder?.contacts?.length === 2, edited);
        const bad = await api('POST', '/api/v1/forwarders', { name: 'x', contacts: [{ email: 'not an address' }] });
        check('a contact that is not an address: 400', bad.status === 400 && bad.body?.field === 'contacts[0].email', bad);
        const listed = await api('GET', '/api/v1/forwarders');
        check('both are listed', listed.status === 200 && [F1, F2].every(id => listed.body.forwarders.some(f => f.id === id)), listed.status);

        // ── 2. the send, and its retry ───────────────────────────────────────
        console.log('\n2. Send a quote request to both');
        const send = { forwarderIds: [F1, F2], batchId: crypto.randomUUID(), windowStart, windowEnd, origin: 'Ningbo', services: ['Door to door', DELIVERY], stockMovements: MOVEMENTS, note: `smoke ${TAG}`,
            subject: `Smoke quote for {forwarder} ${TAG}`, body: '<p>Edited by staff for {forwarder}: {services}.</p>' };
        const sent = await api('POST', '/api/v1/freight-quotes', send);
        check('both forwarders: sent', sent.status === 200 && sent.body?.results?.every(r => r.outcome === 'sent'), sent.body?.results ?? sent);
        check('one email each, to that forwarder\'s contacts', frontCalls.length === 2 && frontCalls[0].to.includes(addr('amy')) && frontCalls[0].to.includes(addr('ops')) && frontCalls[1].to.join() === addr('bo'), frontCalls.map(c => c.to));
        const rows = await q('SELECT id, token, forwarder_id, sent_at FROM freight_quote_requests WHERE batch_id = ? ORDER BY forwarder_id', [send.batchId]);
        const R1 = rows.find(r => r.forwarder_id === F1); const R2 = rows.find(r => r.forwarder_id === F2);
        check('each request has its own token, in its own email', rows.length === 2 && F.isToken(R1?.token) && R1.token !== R2?.token
            && frontCalls[0].body.includes(`/freight/${R1.token}`) && !frontCalls[0].body.includes(R2.token) && rows.every(r => r.sent_at), rows.length);
        check('the email is the one staff edited, with the stock movements under it',
            frontCalls[0].subject === `Smoke quote for Smoke One ${TAG} ${TAG}` && frontCalls[0].body.includes(`Edited by staff for Smoke One ${TAG}: Door to door, ${DELIVERY}.`)
            && frontCalls[0].body.includes(`Acme Medical ${TAG}`) && frontCalls[0].body.includes('15.75'), frontCalls[0].subject);
        const again = await api('POST', '/api/v1/freight-quotes', send);
        check('the same send again emails nobody', again.status === 200 && again.body?.results?.every(r => r.outcome === 'already_sent') && frontCalls.length === 2, again.body?.results ?? again);
        const users = await q(`SELECT email, status, cognito_sub FROM ${JP}.supplier_users WHERE email LIKE ?`, [`%@${DOM}`]);
        check('the contacts are pre-approved on the portal, with no supplier', users.length === 3 && users.every(u => u.status === 'active' && u.cognito_sub === null), users);

        // ── 3. the forwarder answers through the link (as jportal_app) ────────
        console.log('\n3. The forwarder\'s page, by the emailed link');
        const opened = await link('GET', R1.token);
        check('GET: the request, open, nothing answered', opened.status === 200 && opened.data?.state === 'open' && opened.data.options.length === 0 && opened.data.containerSize === '40HQ', opened);
        check('the page is given the delivery options as written, and the stock movements as unpriced rows with no "to" (the table grant)',
            opened.data?.services?.join('|') === `Door to door|${DELIVERY}` && opened.data?.stockMovements?.length === 2
            && opened.data.stockMovements[0].from === `Acme Medical ${TAG}` && opened.data.stockMovements[1].weightKg === 610.5
            && opened.data.stockMovements.every(m => m.price === null && !('to' in m)), opened.data?.stockMovements ?? opened);
        const firstMove = opened.data?.stockMovements?.[0];
        const priced = await link('PUT', R1.token, { prices: [{ id: firstMove?.id, rowVersion: firstMove?.rowVersion, price: '120.5', currency: 'USD' }], name: 'Amy' }, 'stock-movements');
        check('PUT stock-movements: one movement priced on its own; the request itself is untouched',
            priced.status === 200 && priced.data?.stockMovements?.[0]?.price === '120.50' && priced.data.stockMovements[1].price === null
            && priced.data.rowVersion === opened.data?.rowVersion && priced.data.state === 'open', priced);
        const stalePrice = await link('PUT', R1.token, { prices: [{ id: firstMove?.id, rowVersion: firstMove?.rowVersion, price: '99', currency: 'USD' }] }, 'stock-movements');
        check('a stale price is 409 with the current view', stalePrice.status === 409 && stalePrice.details?.stockMovements?.[0]?.price === '120.50', stalePrice);
        check('an unknown token is 404', (await link('GET', 'f'.repeat(48))).status === 404);
        const answered = await link('PUT', R1.token, { rowVersion: opened.data?.rowVersion, declined: false, name: 'Amy', options: [option(), option({ sailingDate: F.addDays(today, 35), loadingDate: F.addDays(today, 31), price: '1790.50', containerSize: '40GP' })] });
        check('PUT: two options saved', answered.status === 200 && answered.data?.state === 'answered' && answered.data.options.length === 2 && answered.data.options[1].price === '1790.50' && answered.data.options[1].containerSize === '40GP', answered);
        const stale = await link('PUT', R1.token, { rowVersion: opened.data?.rowVersion, declined: false, options: [option()] });
        check('a stale save is 409 with the current view', stale.status === 409 && stale.code === 'CONFLICT' && stale.details?.options?.length === 2, stale);
        const badOption = await link('PUT', R1.token, { rowVersion: answered.data?.rowVersion, declined: false, options: [option({ service: 'port_to_port' })] });
        check('a delivery option that was not asked for is 400', badOption.status === 400 && badOption.details?.field === 'options[0].service', badOption);

        // ── 4. staff see it, and type in the other forwarder's answer ─────────
        console.log('\n4. ShipLine reads the board; staff enter an emailed answer');
        let board = await api('GET', '/api/v1/freight-quotes');
        const b1 = () => board.body.requests.find(r => r.id === R1.id); const b2 = () => board.body.requests.find(r => r.id === R2.id);
        check('the board carries the stock movements with the forwarder\'s price, and the option\'s delivery option',
            b1()?.stockMovements?.length === 2 && b1().stockMovements[1].cbm === 3.25 && b1().stockMovements[0].price === 120.5
            && b1().stockMovements[0].priceSource === 'link' && b1().stockMovements[1].price === null && b2()?.stockMovements?.length === 2
            && b1().options[0].service === DELIVERY, b1()?.stockMovements);
        check('the board shows the answer', board.status === 200 && b1()?.state === 'answered' && b1().options.length === 2 && b1().answerSource === 'link' && b1().answeredBy === 'Amy', b1());
        const summary = await api('GET', '/api/v1/freight-quotes/summary');
        check('the badge counts the send', summary.status === 200 && summary.body.answered >= 1, summary);
        const typed = await api('POST', `/api/v1/freight-quotes/${R2.id}/options`, { options: [option({ price: '1700', currency: 'GBP' })], note: 'by email' });
        board = typed;
        check('a staff-entered answer shows as such', typed.status === 200 && b2()?.state === 'answered' && b2().answerSource === 'staff' && b2().options[0].price === 1700, b2() ?? typed);

        // ── 5. booking ───────────────────────────────────────────────────────
        console.log('\n5. Request a booking');
        const optionId = b1().options[0].id;
        const booked = await api('POST', `/api/v1/freight-quotes/options/${optionId}/book`, { note: `smoke ${TAG}` });
        board = booked;
        const bookingId = booked.body?.bookingId;
        check('the booking is requested and emailed', booked.status === 200 && b1()?.state === 'booked' && frontCalls.length === 3 && frontCalls[2].body.includes(`/freight/${R1.token}`) && /1,850\.00/.test(frontCalls[2].body), booked.body?.error ?? booked.status);
        check('a second booking on the request is refused', (await api('POST', `/api/v1/freight-quotes/options/${optionId}/book`, {})).body?.code === 'ALREADY_BOOKED');
        const page = await link('GET', R1.token);
        check('the forwarder\'s page shows the booking to confirm', page.data?.state === 'booked' && page.data.booking?.status === 'requested' && page.data.booking.option?.id === optionId, page);
        const lateAnswer = await link('PUT', R1.token, { rowVersion: page.data?.rowVersion, declined: false, options: [option()] });
        check('the quote can no longer be changed: 409 with the booked view', lateAnswer.status === 409 && lateAnswer.details?.state === 'booked', lateAnswer);
        const confirmed = await link('PUT', R1.token, { rowVersion: page.data?.booking?.rowVersion, decision: 'confirm', bookingRef: `BK-${TAG}`, name: 'Amy' }, true);
        check('the forwarder confirms with a reference', confirmed.status === 200 && confirmed.data?.booking?.status === 'confirmed' && confirmed.data.booking.bookingRef === `BK-${TAG}`, confirmed);

        board = await api('GET', '/api/v1/freight-quotes');
        const booking = () => board.body.bookings.find(b => b.id === bookingId);
        check('ShipLine sees the confirmation', booking()?.status === 'confirmed' && booking().responseSource === 'link' && booking().bookingRef === `BK-${TAG}`, booking());
        check('the other forwarder\'s link is closed as filled', b2()?.state === 'revoked' && b2().revokeReason === 'filled', b2());
        const filled = await link('GET', R2.token);
        check('and its page says so', filled.status === 200 && filled.data?.state === 'revoked' && filled.data.revokeReason === 'filled' && filled.data.options.length === 0, filled);

        // ── 5b. a stock movement added after the booking ─────────────────────
        console.log('\n5b. Add a stock movement to the booked request');
        const mailsBefore = frontCalls.length;
        const addition = { addKey: crypto.randomUUID(), stockMovements: [{ from: `Late Factory ${TAG}`, cbm: 2, weightKg: 300 }] };
        const addedMove = await api('POST', `/api/v1/freight-quotes/${R1.id}/stock-movements`, addition);
        board = addedMove;
        const late = () => b1()?.stockMovements?.find(m => m.from === `Late Factory ${TAG}`);
        check('it is added under the confirmed booking, and the forwarder is emailed the link once',
            addedMove.status === 200 && addedMove.body?.added === 1 && b1()?.state === 'booked' && Boolean(late()) && late().price === null
            && frontCalls.length === mailsBefore + 1 && frontCalls[mailsBefore].body.includes(`/freight/${R1.token}`)
            && frontCalls[mailsBefore].body.includes(`Late Factory ${TAG}`), addedMove.body?.error ?? addedMove.status);
        const addedAgain = await api('POST', `/api/v1/freight-quotes/${R1.id}/stock-movements`, addition);
        check('the same addition again adds and emails nothing', addedAgain.status === 200 && addedAgain.body?.alreadyAdded === true && frontCalls.length === mailsBefore + 1
            && addedAgain.body.requests.find(r => r.id === R1.id).stockMovements.length === 3, addedAgain.body?.error ?? addedAgain.status);
        check('a closed link takes no stock movement', (await api('POST', `/api/v1/freight-quotes/${R2.id}/stock-movements`, { stockMovements: addition.stockMovements })).body?.code === 'REVOKED');
        const bookedPage = await link('GET', R1.token);
        const lateOnPage = bookedPage.data?.stockMovements?.find(m => m.from === `Late Factory ${TAG}`);
        check('the forwarder\'s page lists it under the booking', bookedPage.data?.state === 'booked' && Boolean(lateOnPage), bookedPage.data?.stockMovements ?? bookedPage);
        const latePriced = await link('PUT', R1.token, { prices: [{ id: lateOnPage?.id, rowVersion: lateOnPage?.rowVersion, price: '45', currency: 'CNY' }] }, 'stock-movements');
        check('and prices it there, booked or not', latePriced.status === 200 && latePriced.data?.stockMovements?.find(m => m.id === lateOnPage?.id)?.price === '45.00', latePriced);
        const second2 = b1()?.stockMovements?.find(m => m.from === 'Best Gloves');
        const typedPrice = await api('PATCH', `/api/v1/freight-stock-movements/${second2?.id}`, { price: '80', currency: 'USD', note: 'by phone' });
        board = typedPrice;
        check('staff type in a price given by phone', typedPrice.status === 200 && b1()?.stockMovements?.find(m => m.id === second2?.id)?.priceSource === 'staff'
            && late()?.price === 45 && late().currency === 'CNY', b1()?.stockMovements ?? typedPrice);

        // ── 6. signed in ─────────────────────────────────────────────────────
        console.log('\n6. A contact signs in to the portal');
        const claims = { sub: `smoke-${TAG}`, email: addr('amy'), email_verified: 'true' };
        const me = await signedIn(claims, 'GET', '/api/supplier/me');
        check('me: an active account whose only tab is Freight quotes', me.status === 200 && me.data?.status === 'active' && me.data.presence?.freightQuotes === true && me.data.presence.orders === false && me.data.suppliers.length === 0, me);
        const mine = await signedIn(claims, 'GET', '/api/supplier/freight-quotes');
        check('the list holds their forwarder\'s request, booked', mine.status === 200 && mine.data?.requests?.length === 1 && mine.data.requests[0].id === R1.id && mine.data.requests[0].bookingStatus === 'confirmed' && mine.data.requests[0].optionCount === 2, mine);
        check('it opens by id', (await signedIn(claims, 'GET', '/api/supplier/freight-quotes/{id}', R1.id)).data?.booking?.status === 'confirmed');
        check('another forwarder\'s request is 404', (await signedIn(claims, 'GET', '/api/supplier/freight-quotes/{id}', R2.id)).status === 404);

        // ── 6b. the same quote for another container (2026-11-26) ────────────
        // A copy of the request, booked in its own right. The portal's code is
        // unchanged: these checks are what say it needs no change.
        console.log('\n6b. Book the same quote for another container');
        const copyKey = crypto.randomUUID();
        const mailsCopy = frontCalls.length;
        const bookedAgain = await api('POST', `/api/v1/freight-quotes/options/${optionId}/book-another`, { copyKey, note: `second ${TAG}` });
        board = bookedAgain;
        const copyId = bookedAgain.body?.requestId;
        const copyBookingId = bookedAgain.body?.bookingId;
        const copyReq = () => board.body?.requests?.find(r => r.id === copyId);
        const [copyRow] = copyId ? await q('SELECT token, batch_id, copied_from_request_id FROM freight_quote_requests WHERE id = ?', [copyId]) : [];
        check('a copy of the request is made and booked: one option, no stock movements, its own link in one email',
            bookedAgain.status === 200 && copyReq()?.state === 'booked' && copyReq().copiedFromRequestId === R1.id && copyReq().options.length === 1
            && copyReq().stockMovements.length === 0 && F.isToken(copyRow?.token) && copyRow.token !== R1.token && copyRow.batch_id === copyKey
            && frontCalls.length === mailsCopy + 1 && frontCalls[mailsCopy].body.includes(`/freight/${copyRow.token}`)
            && !frontCalls[mailsCopy].body.includes(R1.token) && /1,850\.00/.test(frontCalls[mailsCopy].body), bookedAgain.body?.error ?? copyReq() ?? bookedAgain.status);
        check('the original keeps its booking and its stock movements', b1()?.state === 'booked' && b1().stockMovements.length === 3 && booking()?.status === 'confirmed', b1());
        const sameKey = await api('POST', `/api/v1/freight-quotes/options/${optionId}/book-another`, { copyKey });
        check('the same key again books and emails nothing', sameKey.status === 200 && sameKey.body?.alreadyBooked === true && sameKey.body.bookingId === copyBookingId && frontCalls.length === mailsCopy + 1, sameKey.body?.error ?? sameKey.status);
        const copyPage = await link('GET', copyRow?.token);
        check('the copy\'s page (as jportal_app) shows the booking to confirm, on the copied option',
            copyPage.status === 200 && copyPage.data?.state === 'booked' && copyPage.data.booking?.status === 'requested'
            && copyPage.data.booking.option?.id === copyReq()?.options[0].id && copyPage.data.booking.option?.price === '1850.00', copyPage);
        const copyConfirmed = await link('PUT', copyRow?.token, { rowVersion: copyPage.data?.booking?.rowVersion, decision: 'confirm', bookingRef: `BK2-${TAG}`, name: 'Amy' }, true);
        check('the forwarder confirms it with its own reference', copyConfirmed.status === 200 && copyConfirmed.data?.booking?.status === 'confirmed' && copyConfirmed.data.booking.bookingRef === `BK2-${TAG}`, copyConfirmed);
        check('the original\'s page still shows its own reference', (await link('GET', R1.token)).data?.booking?.bookingRef === `BK-${TAG}`);
        const both = await signedIn(claims, 'GET', '/api/supplier/freight-quotes');
        check('signed in, the forwarder sees both, each confirmed', both.status === 200 && both.data?.requests?.length === 2
            && [R1.id, copyId].every(id => both.data.requests.find(r => r.id === id)?.bookingStatus === 'confirmed'), both);
        check('and the copy opens by id', (await signedIn(claims, 'GET', '/api/supplier/freight-quotes/{id}', copyId)).data?.booking?.bookingRef === `BK2-${TAG}`);
        board = await api('GET', '/api/v1/freight-quotes');
        const copyBooking = () => board.body.bookings.find(b => b.id === copyBookingId);
        check('ShipLine sees two confirmed bookings, one per request', booking()?.bookingRef === `BK-${TAG}` && copyBooking()?.requestId === copyId
            && copyBooking().status === 'confirmed' && copyBooking().bookingRef === `BK2-${TAG}` && copyBooking().responseSource === 'link', copyBooking());
        const copyCancelled = await api('PATCH', `/api/v1/freight-bookings/${copyBookingId}`, { status: 'cancelled' });
        board = copyCancelled;
        const closedPage = await link('GET', copyRow?.token);
        check('cancelling the copy\'s booking closes the copy: off the board, its page closed, the original untouched',
            copyCancelled.status === 200 && copyBooking()?.status === 'cancelled' && !copyReq() && closedPage.data?.state === 'revoked'
            && b1()?.state === 'booked' && booking()?.status === 'confirmed', closedPage.data?.state ?? copyCancelled);

        // ── 7. a container's contents ────────────────────────────────────────
        console.log('\n7. Attach a container and send its contents');
        const [draft] = await q(
            `SELECT s.id, s.name FROM shipments s
              WHERE s.stage = 'DRAFT' AND s.deleted_at IS NULL AND s.merged_into_id IS NULL
                AND EXISTS (SELECT 1 FROM draft_container_allocations d
                             JOIN orders o ON o.id = d.order_id AND o.deleted_at IS NULL
                            WHERE d.draft_container_name = s.name)
                AND NOT EXISTS (SELECT 1 FROM freight_bookings b WHERE b.shipment_id = s.id AND b.status IN ('requested', 'confirmed'))
              ORDER BY s.id DESC LIMIT 1`);
        if (!draft) {
            console.log('  SKIPPED  no draft container with lines on TEST');
        } else {
            const attached = await api('PATCH', `/api/v1/freight-bookings/${bookingId}`, { shipmentId: draft.id });
            board = attached;
            check(`"${draft.name}" is attached`, attached.status === 200 && booking()?.shipmentId === draft.id && booking().containerName === draft.name, attached.body?.error ?? booking());
            const contents = await api('GET', `/api/v1/freight-bookings/${bookingId}/contents`);
            check('its contents come back by supplier, with totals', contents.status === 200 && contents.body.groups.length >= 1 && contents.body.totals.lines >= 1 && contents.body.changedSinceSent === null, contents.body?.error ?? contents.body?.totals);
            const before = frontCalls.length;
            const mail = await api('POST', `/api/v1/freight-bookings/${bookingId}/contents-email`, { rowVersion: booking().rowVersion, attachDocuments: false });
            const staleVersion = booking().rowVersion;
            board = mail;
            check('the contents email goes out, with a table per supplier', mail.status === 200 && frontCalls.length === before + 1 && frontCalls[before].body.includes('Subtotal') && frontCalls[before].body.includes(`BK-${TAG}`), mail.body?.error ?? mail.status);
            check('the booking records it, unchanged since sent', Boolean(booking()?.contentsSentAt) && booking().contentsChanged === false, booking());
            const twice = await api('POST', `/api/v1/freight-bookings/${bookingId}/contents-email`, { rowVersion: staleVersion, attachDocuments: false });
            check('the same call again is refused, not sent twice', twice.status === 409 && twice.body?.code === 'STALE' && frontCalls.length === before + 1, twice);

            // The forwarder's signed-in page (as jportal_app): when the contents
            // were sent and which documents went with them. This email attached
            // none, so an existing forwarder quote stands in for one.
            const seen = await signedIn(claims, 'GET', '/api/supplier/freight-quotes/{id}', R1.id);
            check('signed in, the page says when the contents were sent, with no documents (the column grant)',
                seen.status === 200 && Boolean(seen.data?.booking?.contentsSentAt) && Array.isArray(seen.data.booking.documents) && seen.data.booking.documents.length === 0, seen.data?.booking ?? seen);
            const [quote] = await q(
                `SELECT id FROM draft_container_documents
                  WHERE type = 'forwarder-quote' AND deleted_at IS NULL AND public_url LIKE 'https://shipping-purchase-orders-%/draft-containers/%'
                  ORDER BY id DESC LIMIT 1`);
            if (!quote) {
                console.log('  SKIPPED  no forwarder quote document on TEST to list');
            } else {
                await q('UPDATE freight_bookings SET contents_document_ids = ? WHERE id = ?', [JSON.stringify([quote.id]), bookingId]);
                const listed = await signedIn(claims, 'GET', '/api/supplier/freight-quotes/{id}', R1.id);
                check('signed in, the page lists the documents the booking names, with their addresses',
                    listed.status === 200 && listed.data?.booking?.documents?.length === 1 && listed.data.booking.documents[0].id === quote.id
                    && String(listed.data.booking.documents[0].pdfUrl).startsWith('https://'), listed.data?.booking ?? listed);
            }
            const byLink = await link('GET', R1.token);
            check('the emailed link\'s page lists none', byLink.status === 200 && byLink.data?.booking?.contentsSentAt === null && byLink.data.booking.documents.length === 0, byLink.data?.booking ?? byLink);
        }

        // ── 8. receipts ──────────────────────────────────────────────────────
        console.log('\n8. Email receipts');
        const conn = await getPool().getConnection();
        let sync;
        try { sync = await syncPortalReceipts(conn, { maxAgeMs: 0 }); } finally { conn.release(); }
        check('the receipts sync ran the three freight steps', sync && !sync.failed.includes('freight_quote_answered') && !sync.failed.includes('freight_booking_answered')
            && !sync.failed.includes('freight_movements_priced'), sync);
        const receipts = await q(
            `SELECT email_type, send_id, confirm_via, confirmed_at FROM email_receipts
              WHERE (send_table = 'freight_quote_requests' AND send_id IN (?)) OR (send_table = 'freight_bookings' AND send_id = ?)`,
            [[R1.id, R2.id], bookingId]);
        const receipt = (type, id) => receipts.find(r => r.email_type === type && Number(r.send_id) === id);
        check('the answered request and the answered booking are confirmed', Boolean(receipt('freight_quote_request', R1.id)?.confirmed_at) && Boolean(receipt('freight_booking_request', bookingId)?.confirmed_at), receipts);
        check('the stock movement price request is confirmed: every movement has a price', Boolean(receipt('freight_movement_request', R1.id)?.confirmed_at), receipts);
        const lateId = board.body?.requests?.find(r => r.id === R1.id)?.stockMovements?.find(m => m.from === `Late Factory ${TAG}`)?.id;
        const removedMove = await api('DELETE', `/api/v1/freight-stock-movements/${lateId}`);
        board = removedMove;
        check('a stock movement is removed, and leaves the forwarder\'s page', removedMove.status === 200 && b1()?.stockMovements?.length === 2
            && (await link('GET', R1.token)).data?.stockMovements?.length === 2, removedMove.body?.error ?? b1()?.stockMovements);

        // ── 9. cancel, and remove ────────────────────────────────────────────
        console.log('\n9. Cancel the booking; remove a forwarder');
        const cancelled = await api('PATCH', `/api/v1/freight-bookings/${bookingId}`, { status: 'cancelled' });
        board = cancelled;
        check('cancelled: both requests are open to book again', cancelled.status === 200 && booking()?.status === 'cancelled' && b1()?.state === 'answered' && b2()?.state === 'answered', [b1()?.state, b2()?.state]);
        const second = await api('POST', `/api/v1/freight-quotes/options/${b2().options[0].id}/book`, {});
        board = second;
        check('the other forwarder can be booked at once', second.status === 200 && b2()?.state === 'booked', second.body?.error ?? second.status);
        check('a forwarder with a booking in progress cannot be removed', (await api('DELETE', `/api/v1/forwarders/${F2}`)).body?.code === 'HAS_BOOKING');
        const marked = await api('PATCH', `/api/v1/freight-bookings/${second.body?.bookingId}`, { status: 'confirmed', bookingRef: `ST-${TAG}` });
        board = marked;
        check('staff mark it confirmed themselves', marked.status === 200 && board.body.bookings.find(b => b.id === second.body.bookingId)?.responseSource === 'staff' && b1()?.revokeReason === 'filled', marked.body?.error ?? b1());
        const dropped = await api('DELETE', `/api/v1/freight-delivery-options/${myOption?.id}`);
        check('the delivery option is removed from the list; the request keeps its text', dropped.status === 200 && !dropped.body.deliveryOptions.some(o => o.id === myOption.id)
            && (await api('GET', '/api/v1/freight-quotes')).body.requests.find(r => r.id === R1.id)?.services.includes(DELIVERY), dropped);
        const removed = await api('DELETE', `/api/v1/forwarders/${F1}`);
        check('removing a forwarder works', removed.status === 200 && removed.body?.ok === true, removed);
        const gone = await signedIn(claims, 'GET', '/api/supplier/freight-quotes');
        check('and its contact no longer sees its requests on the portal', gone.status === 200 && gone.data?.requests?.length === 0, gone);
    } finally {
        await cleanup().catch(e => console.error('[smoke] cleanup failed:', e.message));
        server.close();
        await portalPool.end();
        await closePool();
    }
}

async function cleanup() {
    const ids = forwarderIds.filter(Boolean);
    let requests = [], bookings = [];
    if (ids.length) {
        requests = (await q('SELECT id FROM freight_quote_requests WHERE forwarder_id IN (?)', [ids])).map(r => Number(r.id));
        if (requests.length) {
            bookings = (await q('SELECT id FROM freight_bookings WHERE request_id IN (?)', [requests])).map(r => Number(r.id));
            await q(`DELETE FROM email_receipts WHERE send_table = 'freight_quote_requests' AND send_id IN (?)`, [requests]);
            await q(`DELETE FROM audit_log WHERE entity_type = 'freight_quote_request' AND entity_id IN (?)`, [requests]);
            await q(`DELETE FROM ${JP}.audit_logs WHERE entity_type = 'freight_quote_request' AND entity_id IN (?)`, [requests]);
            await q('DELETE FROM freight_quote_options WHERE request_id IN (?)', [requests]);
            const movements = (await q('SELECT id FROM freight_stock_movements WHERE request_id IN (?)', [requests])).map(m => Number(m.id));
            if (movements.length) {
                await q(`DELETE FROM audit_log WHERE entity_type = 'freight_stock_movement' AND entity_id IN (?)`, [movements]);
                await q('DELETE FROM freight_stock_movements WHERE id IN (?)', [movements]);
            }
        }
        if (bookings.length) {
            await q(`DELETE FROM email_receipts WHERE send_table = 'freight_bookings' AND send_id IN (?)`, [bookings]);
            await q(`DELETE FROM audit_log WHERE entity_type = 'freight_booking' AND entity_id IN (?)`, [bookings]);
            await q(`DELETE FROM ${JP}.audit_logs WHERE entity_type = 'freight_booking' AND entity_id IN (?)`, [bookings]);
            await q('DELETE FROM freight_bookings WHERE id IN (?)', [bookings]);
        }
        if (requests.length) await q('DELETE FROM freight_quote_requests WHERE id IN (?)', [requests]);
        await q(`DELETE FROM audit_log WHERE entity_type = 'forwarder' AND entity_id IN (?)`, [ids]);
        await q('DELETE FROM forwarder_contacts WHERE forwarder_id IN (?)', [ids]);
        await q('DELETE FROM forwarders WHERE id IN (?)', [ids]);
    }
    const options = (await q('SELECT id FROM freight_delivery_options WHERE label LIKE ?', [`%${TAG}%`])).map(o => Number(o.id));
    if (options.length) {
        await q(`DELETE FROM audit_log WHERE entity_type = 'freight_delivery_option' AND entity_id IN (?)`, [options]);
        await q('DELETE FROM freight_delivery_options WHERE id IN (?)', [options]);
    }
    const users = (await q(`SELECT id FROM ${JP}.supplier_users WHERE email LIKE ?`, [`%@${DOM}`])).map(u => Number(u.id));
    if (users.length) {
        await q(`DELETE FROM ${JP}.audit_logs WHERE entity_type = 'supplier_user' AND entity_id IN (?)`, [users]);
        await q(`DELETE FROM ${JP}.supplier_users WHERE id IN (?)`, [users]);
    }
    console.log(`\ncleanup: ${ids.length} forwarders, ${requests.length} requests, ${bookings.length} bookings, ${users.length} portal users removed`);
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
