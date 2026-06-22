'use strict';

// Smoke test for the draft_container_allocations CRUD endpoints.
//
// Prereq: start the orders server locally with auth bypass:
//   $env:IS_OFFLINE=1; node src/handlers/orders.js
//
// Pass two existing order IDs as args (defaults: 629, 630).
//   node tools/test-draft-containers.js 629 630

require('dotenv').config();
const axios = require('axios');

const BASE = process.env.TEST_BASE_URL || 'http://localhost:3001';
const ORDER_A = Number(process.argv[2] || 629);
const ORDER_B = Number(process.argv[3] || 630);
const DRAFT_1 = `DRAFT_SMOKE_${Date.now()}_1`;
const DRAFT_2 = `DRAFT_SMOKE_${Date.now()}_2`;

const api = axios.create({ baseURL: BASE, validateStatus: () => true });

let pass = 0;
let fail = 0;
const created = [];

function check(label, cond, detail = '') {
    if (cond) { pass++; console.log(`  PASS  ${label}`); }
    else      { fail++; console.log(`  FAIL  ${label}${detail ? ' — ' + detail : ''}`); }
}

async function run() {
    console.log(`Base URL: ${BASE}`);
    console.log(`Orders:   A=${ORDER_A}  B=${ORDER_B}`);
    console.log(`Drafts:   ${DRAFT_1}, ${DRAFT_2}\n`);

    // 1. POST — order A into DRAFT_1 qty 50
    console.log('1. POST orderA → DRAFT_1, allocated=50');
    let r = await api.post('/api/v1/draft-containers', {
        orderId: ORDER_A, draftContainerName: DRAFT_1, allocated: 50,
    });
    check('status 201', r.status === 201, `got ${r.status} ${JSON.stringify(r.data)}`);
    check('joined jfCode present', !!r.data?.order?.jfCode, `order=${JSON.stringify(r.data?.order)}`);
    check('allocated echoed', r.data?.allocated === 50);
    if (r.data?.id) created.push(r.data.id);
    const idA1 = r.data?.id;

    // 2. POST — order B into DRAFT_1 qty 10
    console.log('\n2. POST orderB → DRAFT_1, allocated=10');
    r = await api.post('/api/v1/draft-containers', {
        orderId: ORDER_B, draftContainerName: DRAFT_1, allocated: 10,
    });
    check('status 201', r.status === 201, `got ${r.status}`);
    if (r.data?.id) created.push(r.data.id);
    const idB1 = r.data?.id;

    // 3. POST — order A into DRAFT_2 qty 40 (same order, different draft)
    console.log('\n3. POST orderA → DRAFT_2, allocated=40');
    r = await api.post('/api/v1/draft-containers', {
        orderId: ORDER_A, draftContainerName: DRAFT_2, allocated: 40,
    });
    check('status 201 (same order, new draft allowed)', r.status === 201, `got ${r.status}`);
    if (r.data?.id) created.push(r.data.id);

    // 4. GET ?name=DRAFT_1 → expect 2 rows
    console.log(`\n4. GET ?name=${DRAFT_1}`);
    r = await api.get(`/api/v1/draft-containers?name=${encodeURIComponent(DRAFT_1)}`);
    check('status 200', r.status === 200);
    check('2 rows', r.data?.data?.length === 2, `got ${r.data?.data?.length}`);
    const orderIds = (r.data?.data || []).map(x => x.orderId).sort();
    check('returns both orders', JSON.stringify(orderIds) === JSON.stringify([ORDER_A, ORDER_B].sort()),
        `got ${JSON.stringify(orderIds)}`);
    check('rows include joined product_name', (r.data?.data || []).every(x => 'productName' in (x.order || {})));

    // 5. GET ?orderId=A → expect 2 rows (A is in DRAFT_1 and DRAFT_2)
    console.log(`\n5. GET ?orderId=${ORDER_A}`);
    r = await api.get(`/api/v1/draft-containers?orderId=${ORDER_A}`);
    check('status 200', r.status === 200);
    check('2 rows', r.data?.data?.length === 2, `got ${r.data?.data?.length}`);

    // 6. POST duplicate (orderA, DRAFT_1 again) → 409
    console.log('\n6. POST duplicate (orderA, DRAFT_1) — expect 409');
    r = await api.post('/api/v1/draft-containers', {
        orderId: ORDER_A, draftContainerName: DRAFT_1, allocated: 1,
    });
    check('status 409', r.status === 409, `got ${r.status} ${JSON.stringify(r.data)}`);

    // 7. POST nonexistent order → 404
    console.log('\n7. POST orderId=999999 — expect 404');
    r = await api.post('/api/v1/draft-containers', {
        orderId: 999999, draftContainerName: DRAFT_1, allocated: 1,
    });
    check('status 404', r.status === 404, `got ${r.status}`);

    // 8. POST negative qty → 400
    console.log('\n8. POST allocated=-5 — expect 400');
    r = await api.post('/api/v1/draft-containers', {
        orderId: ORDER_A, draftContainerName: 'DRAFT_BAD', allocated: -5,
    });
    check('status 400', r.status === 400, `got ${r.status}`);

    // 9. POST missing name → 400
    console.log('\n9. POST draftContainerName="" — expect 400');
    r = await api.post('/api/v1/draft-containers', {
        orderId: ORDER_A, draftContainerName: '', allocated: 1,
    });
    check('status 400', r.status === 400, `got ${r.status}`);

    // 10. PUT — update allocated on idA1
    console.log(`\n10. PUT id=${idA1} allocated=99`);
    r = await api.put(`/api/v1/draft-containers/${idA1}`, { allocated: 99 });
    check('status 200', r.status === 200);
    check('allocated=99', r.data?.allocated === 99, `got ${r.data?.allocated}`);
    check('joined order still present', !!r.data?.order?.jfCode);

    // 11. PUT — empty body → 400
    console.log(`\n11. PUT id=${idA1} {} — expect 400`);
    r = await api.put(`/api/v1/draft-containers/${idA1}`, {});
    check('status 400', r.status === 400, `got ${r.status}`);

    // 12. PUT — rename to collide with idB1's (orderB, DRAFT_1) — but we're
    //     renaming idA1 which is order A. To trigger 409 we need to rename
    //     idA1's draft to something that already pairs with order A. order A
    //     is also in DRAFT_2 → rename idA1 (currently DRAFT_1) to DRAFT_2.
    console.log(`\n12. PUT id=${idA1} draftContainerName=${DRAFT_2} — expect 409`);
    r = await api.put(`/api/v1/draft-containers/${idA1}`, { draftContainerName: DRAFT_2 });
    check('status 409', r.status === 409, `got ${r.status} ${JSON.stringify(r.data)}`);

    // 13. GET single by id
    console.log(`\n13. GET /draft-containers/${idA1}`);
    r = await api.get(`/api/v1/draft-containers/${idA1}`);
    check('status 200', r.status === 200);
    check('id matches', r.data?.id === idA1);

    // 14. DELETE idB1
    console.log(`\n14. DELETE id=${idB1}`);
    r = await api.delete(`/api/v1/draft-containers/${idB1}`);
    check('status 200', r.status === 200);
    check('ok flag', r.data?.ok === true);
    created.splice(created.indexOf(idB1), 1);

    // 15. DELETE same id again → 404
    console.log(`\n15. DELETE id=${idB1} again — expect 404`);
    r = await api.delete(`/api/v1/draft-containers/${idB1}`);
    check('status 404', r.status === 404, `got ${r.status}`);

    // 16. GET ?name=DRAFT_1 → 1 row remaining
    console.log(`\n16. GET ?name=${DRAFT_1} — expect 1 row`);
    r = await api.get(`/api/v1/draft-containers?name=${encodeURIComponent(DRAFT_1)}`);
    check('1 row', r.data?.data?.length === 1, `got ${r.data?.data?.length}`);
}

(async () => {
    try {
        await run();
    } catch (e) {
        console.error('TEST CRASHED:', e.message);
        fail++;
    } finally {
        // Cleanup — best effort
        console.log(`\nCleanup: deleting ${created.length} test rows...`);
        for (const id of created) {
            try { await api.delete(`/api/v1/draft-containers/${id}`); } catch {}
        }
        console.log(`\n=========================`);
        console.log(`Result: ${pass} passed, ${fail} failed`);
        console.log(`=========================`);
        process.exit(fail ? 1 : 0);
    }
})();
