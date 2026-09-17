'use strict';

// Smoke test for the draft-container audit trail: registry, per-line events,
// whole-draft rename / close, and the history read-back through /audit-log.
//
// Prereq: start the orders server locally with auth bypass:
//   npm run dev:local            (tools/dev-server.js → http://localhost:3031)
//
// Pass two existing order IDs as args (defaults: 629, 630).
//   node tools/test-draft-container-audit.js 629 630
//
// Leaves nothing behind except the registry row + its history for the test
// draft (that is the point — closed drafts keep their history).

require('dotenv').config();
const axios = require('axios');

const BASE = process.env.TEST_BASE_URL || 'http://localhost:3031';
const ORDER_A = Number(process.argv[2] || 629);
const ORDER_B = Number(process.argv[3] || 630);
const STAMP = Date.now();
const DRAFT = `DRAFT-SEA-AUDIT-${STAMP} - 9001`;
const RENAMED = `${DRAFT} Ningbo`;

const api = axios.create({ baseURL: BASE, validateStatus: () => true });

let pass = 0;
let fail = 0;
function check(label, cond, detail = '') {
    if (cond) { pass++; console.log(`  PASS  ${label}`); }
    else      { fail++; console.log(`  FAIL  ${label}${detail ? ' — ' + detail : ''}`); }
}

async function history(id) {
    const r = await api.get('/api/v1/audit-log', { params: { entityType: 'draft_container', entityId: id, limit: 500 } });
    return r.status === 200 ? r.data.data : [];
}
const actions = rows => rows.map(r => r.action);

async function run() {
    console.log(`Base URL: ${BASE}`);
    console.log(`Orders:   A=${ORDER_A}  B=${ORDER_B}`);
    console.log(`Draft:    ${DRAFT}\n`);

    // 1. First line registers the draft.
    console.log('1. POST first line → draft registered');
    let r = await api.post('/api/v1/draft-containers', { orderId: ORDER_A, draftContainerName: DRAFT, allocated: 50 });
    check('status 201', r.status === 201, `got ${r.status} ${JSON.stringify(r.data)}`);
    const allocA = r.data?.id;

    r = await api.get('/api/v1/draft-container-registry', { params: { name: DRAFT } });
    check('registry lookup 200', r.status === 200, `got ${r.status}`);
    const rec = r.data?.data?.[0];
    check('registry row exists', !!rec, JSON.stringify(r.data));
    check('status open', rec?.status === 'open', `status=${rec?.status}`);
    check('lineCount 1', rec?.lineCount === 1, `lineCount=${rec?.lineCount}`);
    const draftId = rec?.id;

    let h = await history(draftId);
    check('history has create + line_added', actions(h).includes('create') && actions(h).includes('line_added'), actions(h).join(','));
    check('line_added carries draftName + orderId', h.some(e => e.action === 'line_added' && e.after?.draftName === DRAFT && e.after?.orderId === ORDER_A));

    // 2. Second line, qty change, removal.
    console.log('\n2. Second line, qty change, removal');
    r = await api.post('/api/v1/draft-containers', { orderId: ORDER_B, draftContainerName: DRAFT, allocated: 10 });
    check('status 201', r.status === 201, `got ${r.status}`);
    const allocB = r.data?.id;

    r = await api.put(`/api/v1/draft-containers/${allocA}`, { allocated: 75 });
    check('PUT qty 200', r.status === 200, `got ${r.status}`);
    r = await api.put(`/api/v1/draft-containers/${allocA}`, { allocated: 75 });
    check('PUT same qty 200 (no-op)', r.status === 200);

    r = await api.delete(`/api/v1/draft-containers/${allocB}`);
    check('DELETE line 200', r.status === 200, `got ${r.status}`);

    h = await history(draftId);
    const updated = h.filter(e => e.action === 'line_updated');
    check('exactly one line_updated (no-op PUT not recorded)', updated.length === 1, `got ${updated.length}`);
    check('line_updated before/after allocated 50 → 75', updated[0]?.before?.allocated === 50 && updated[0]?.after?.allocated === 75, JSON.stringify(updated[0]));
    check('line_removed for order B', h.some(e => e.action === 'line_removed' && e.before?.orderId === ORDER_B));

    // 3. Whole-draft rename keeps the id.
    console.log('\n3. Rename');
    r = await api.post('/api/v1/draft-containers/rename', { from: DRAFT, to: RENAMED });
    check('rename 200', r.status === 200, `got ${r.status} ${JSON.stringify(r.data)}`);
    check('rename keeps registry id', r.data?.id === draftId, `id=${r.data?.id}`);
    check('rename moved 1 allocation', r.data?.allocations === 1, `allocations=${r.data?.allocations}`);

    r = await api.get('/api/v1/draft-containers', { params: { name: RENAMED } });
    check('allocations now under new name', r.status === 200 && r.data.data.length === 1, `got ${r.status} ${r.data?.data?.length}`);
    r = await api.get('/api/v1/draft-container-registry', { params: { name: DRAFT } });
    check('old name no longer in registry', r.status === 200 && r.data.data.length === 0);

    r = await api.post('/api/v1/draft-containers/rename', { from: DRAFT, to: RENAMED });
    check('rename unknown source 404', r.status === 404, `got ${r.status}`);
    r = await api.post('/api/v1/draft-containers/rename', { from: RENAMED, to: RENAMED });
    check('rename to same name 200 unchanged', r.status === 200 && r.data?.unchanged === true, `got ${r.status} ${JSON.stringify(r.data)}`);

    h = await history(draftId);
    check('renamed event with before/after names', h.some(e => e.action === 'renamed' && e.before?.draftName === DRAFT && e.after?.draftName === RENAMED));

    // 4. Rename conflict with another draft holding the target name.
    console.log('\n4. Rename conflict');
    const OTHER = `DRAFT-SEA-AUDIT-${STAMP} - 9002`;
    r = await api.post('/api/v1/draft-containers', { orderId: ORDER_B, draftContainerName: OTHER, allocated: 5 });
    check('other draft created', r.status === 201, `got ${r.status}`);
    r = await api.post('/api/v1/draft-containers/rename', { from: RENAMED, to: OTHER });
    check('rename onto existing draft 409', r.status === 409, `got ${r.status}`);

    // 5. Close as converted.
    console.log('\n5. Close (converted)');
    r = await api.post('/api/v1/draft-containers/close', { name: RENAMED, reason: 'converted' });
    check('converted without containerNumber 400', r.status === 400, `got ${r.status}`);
    r = await api.post('/api/v1/draft-containers/close', {
        name: RENAMED, reason: 'converted', containerNumber: '9001', externalContainerNumber: 'TEST1234567',
        vesselName: 'SMOKE VESSEL', eta: '2026-12-01', etd: '2026-11-01', freightType: 'SEA', port: 'Ningbo',
        packs: [{ orderId: ORDER_A, qty: 75 }],
    });
    check('close 200', r.status === 200, `got ${r.status} ${JSON.stringify(r.data)}`);
    check('close deleted 1 allocation', r.data?.deleted === 1, `deleted=${r.data?.deleted}`);

    r = await api.get('/api/v1/draft-containers', { params: { name: RENAMED } });
    check('no allocations remain', r.status === 200 && r.data.data.length === 0);
    r = await api.get('/api/v1/draft-container-registry', { params: { name: RENAMED } });
    const closedRec = r.data?.data?.[0];
    check('registry status converted', closedRec?.status === 'converted', `status=${closedRec?.status}`);
    check('registry containerNumber 9001', closedRec?.containerNumber === '9001', `containerNumber=${closedRec?.containerNumber}`);
    check('registry still lists it (history kept)', closedRec?.id === draftId);

    h = await history(draftId);
    const conv = h.find(e => e.action === 'converted');
    check('converted event present', !!conv);
    check('converted snapshot: 1 line, 75 units', conv?.before?.lineCount === 1 && conv?.before?.totalUnits === 75, JSON.stringify(conv?.before));
    check('converted details: container 9001 + vessel', conv?.after?.containerNumber === '9001' && conv?.after?.vesselName === 'SMOKE VESSEL', JSON.stringify(conv?.after));

    r = await api.post('/api/v1/draft-containers/close', { name: RENAMED, reason: 'deleted' });
    check('closing again is a no-op 200', r.status === 200 && r.data?.alreadyClosed === true, `got ${r.status} ${JSON.stringify(r.data)}`);
    const before = h.length;
    h = await history(draftId);
    check('no extra event from the repeat close', h.length === before, `${before} → ${h.length}`);

    // 6. Close as deleted (the other draft).
    console.log('\n6. Close (deleted)');
    r = await api.post('/api/v1/draft-containers/close', { name: OTHER, reason: 'deleted' });
    check('close deleted 200', r.status === 200, `got ${r.status}`);
    r = await api.get('/api/v1/draft-container-registry', { params: { status: 'deleted', q: `AUDIT-${STAMP}` } });
    check('registry filter status=deleted finds it', r.status === 200 && r.data.data.some(d => d.name === OTHER), JSON.stringify(r.data?.data?.map(d => d.name)));

    r = await api.post('/api/v1/draft-containers/close', { name: `NOPE-${STAMP}`, reason: 'deleted' });
    check('close unknown draft 404', r.status === 404, `got ${r.status}`);

    // 7. Global feed carries the draft events too.
    console.log('\n7. Global audit feed');
    r = await api.get('/api/v1/audit-log', { params: { entityType: 'draft_container', action: 'converted', limit: 50 } });
    check('feed filter entityType+action works', r.status === 200 && r.data.data.some(e => e.entityId === draftId), `got ${r.status}`);

    console.log(`\n${pass} passed, ${fail} failed`);
    process.exit(fail ? 1 : 0);
}

run().catch(err => { console.error(err); process.exit(1); });
