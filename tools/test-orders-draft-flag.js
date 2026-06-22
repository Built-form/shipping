'use strict';

// Verify that GET /api/v1/orders surfaces a partDraftContainer field
// listing every draft_container_name the order belongs to.

require('dotenv').config();
const axios = require('axios');

const BASE = process.env.TEST_BASE_URL || 'http://localhost:3001';
const ORDER_A = Number(process.argv[2] || 629);
const ORDER_B = Number(process.argv[3] || 630);
const D1 = `DRAFT_FLAG_${Date.now()}_A`;
const D2 = `DRAFT_FLAG_${Date.now()}_B`;

const api = axios.create({ baseURL: BASE, validateStatus: () => true });

(async () => {
    const created = [];
    let pass = 0, fail = 0;
    const ok = (label, cond, extra = '') => {
        if (cond) { pass++; console.log(`  PASS  ${label}`); }
        else { fail++; console.log(`  FAIL  ${label}${extra ? ' — ' + extra : ''}`); }
    };

    try {
        // Seed allocations
        for (const [oid, name] of [[ORDER_A, D1], [ORDER_A, D2], [ORDER_B, D1]]) {
            const r = await api.post('/api/v1/draft-containers', {
                orderId: oid, draftContainerName: name, allocated: 10,
            });
            if (r.status === 201) created.push(r.data.id);
            else throw new Error(`Seed failed: ${r.status} ${JSON.stringify(r.data)}`);
        }

        // Fetch all orders and find ours
        const all = await api.get('/api/v1/orders');
        ok('GET /orders 200', all.status === 200);
        const ordersById = new Map((all.data?.data || []).map(o => [o.id, o]));

        const a = ordersById.get(ORDER_A);
        const b = ordersById.get(ORDER_B);
        ok(`order ${ORDER_A} found`, !!a);
        ok(`order ${ORDER_B} found`, !!b);

        ok(`order ${ORDER_A}.partDraftContainer is array`, Array.isArray(a?.partDraftContainer));
        ok(`order ${ORDER_A} lists both drafts`,
            JSON.stringify((a?.partDraftContainer || []).slice().sort()) === JSON.stringify([D1, D2].sort()),
            `got ${JSON.stringify(a?.partDraftContainer)}`);

        ok(`order ${ORDER_B}.partDraftContainer = [${D1}]`,
            JSON.stringify(b?.partDraftContainer) === JSON.stringify([D1]),
            `got ${JSON.stringify(b?.partDraftContainer)}`);

        // Pick any order that wasn't seeded — should be empty array, not undefined
        const other = [...ordersById.values()].find(o => o.id !== ORDER_A && o.id !== ORDER_B);
        ok('unseeded order has [] (not undefined)',
            Array.isArray(other?.partDraftContainer) && other.partDraftContainer.length === 0,
            `id=${other?.id} got ${JSON.stringify(other?.partDraftContainer)}`);

    } catch (e) {
        fail++;
        console.error('CRASH:', e.message);
    } finally {
        for (const id of created) {
            try { await api.delete(`/api/v1/draft-containers/${id}`); } catch {}
        }
        console.log(`\n${pass} passed, ${fail} failed`);
        process.exit(fail ? 1 : 0);
    }
})();
