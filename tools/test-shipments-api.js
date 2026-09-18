'use strict';

// /api/v1/shipments CRUD: every write lands in the legacy tables too, so the
// old SPA keeps seeing an ordinary draft / planned container.
//
//   npm run dev:local
//   TEST_BASE_URL=http://localhost:3031 node tools/test-shipments-api.js
//
// Needs the shadow armed. Creates its own fixtures and removes them.

const H = require('./shipments-test-helpers');
const { api, check, section } = H;

async function draftEvents(name) {
    const [reg] = await H.sql(`SELECT id FROM draft_containers WHERE name = ?`, [name]);
    if (!reg) return [];
    const rows = await H.sql(`SELECT action FROM audit_log WHERE entity_type = 'draft_container' AND entity_id = ? ORDER BY id`, [reg.id]);
    return rows.map(r => r.action);
}

async function run() {
    await H.guard();
    const A = await H.createOrder();
    const B = await H.createOrder();
    const C = await H.createOrder();
    const D = await H.createOrder();

    section('1. GET /shipments/next-reference');
    let r = await api.get('/api/v1/shipments/next-reference', { params: { mode: 'SEA' } });
    check('SEA: a bare number past the highest carried or reserved one',
        r.status === 200 && /^\d+$/.test(r.data.reference) && r.data.seq > Math.max(r.data.maxBooked, r.data.maxReserved), r.data);
    r = await api.get('/api/v1/shipments/next-reference', { params: { mode: 'AIR' } });
    check('AIR: NN. Air Freight', r.status === 200 && /^\d+\. Air Freight$/.test(r.data.reference), r.data);
    check('ROAD has no sequence: 422', (await api.get('/api/v1/shipments/next-reference', { params: { mode: 'ROAD' } })).status === 422);
    check('bad mode: 400', (await api.get('/api/v1/shipments/next-reference', { params: { mode: 'BOAT' } })).status === 400);

    section('2. POST /shipments (DRAFT) writes an ordinary legacy draft');
    const name = H.draftName('api');
    r = await api.post('/api/v1/shipments', {
        mode: 'SEA', name, originPort: 'Ningbo', notes: 'SHIPTEST notes',
        lines: [{ orderId: A.id, quantity: 40 }, { orderId: B.id, quantity: 150 }],
    });
    check('201 DRAFT with both lines', r.status === 201 && r.data.stage === 'DRAFT' && r.data.lines.length === 2, r.data);
    const s = r.data;
    H.track(s.id);
    check('header kept: mode (user), originPort, notes', s.mode === 'SEA' && s.modeSource === 'user' && s.originPort === 'Ningbo' && s.notes === 'SHIPTEST notes', s);
    check('over-allocation is a warning, not a rejection', Array.isArray(s.warnings) && s.warnings.some(w => w.includes(`Order ${B.id}`)), s.warnings);
    let legacy = await api.get('/api/v1/draft-containers', { params: { name } });
    check('the old SPA sees the draft: GET /draft-containers', legacy.data.data.length === 2);
    let reg = await api.get('/api/v1/draft-container-registry', { params: { name } });
    check('the registry lists it as open', reg.data.data[0] && reg.data.data[0].status === 'open', reg.data);
    let ev = await draftEvents(name);
    check('legacy draft events: create + one line_added per line', ev[0] === 'create' && ev.filter(a => a === 'line_added').length === 2, ev);
    r = await api.post('/api/v1/shipments', { mode: 'SEA', name, lines: [{ orderId: C.id, quantity: 1 }] });
    check('the same open name again: 409', r.status === 409, r.data);
    check('bad mode: 400', (await api.post('/api/v1/shipments', { mode: 'CAMEL' })).status === 400);

    section('3. Reads');
    const list = await H.shipmentsWhere({ stage: 'DRAFT', q: String(H.STAMP), include: 'lines' });
    const listed = list.find(x => x.id === s.id);
    check('GET /shipments?stage=DRAFT&q=…&include=lines', listed && listed.lines.length === 2, listed);
    const d = await H.shipment(s.id);
    check('GET /shipments/:id: lines, no members, no tracking', d.lines.length === 2 && d.orders.length === 0 && d.tracking === null, d);
    check('unknown id: 404', (await api.get('/api/v1/shipments/999999999')).status === 404);

    section('4. PATCH name (DRAFT) renames the legacy draft');
    const renamed = H.draftName('api-renamed');
    r = await api.patch(`/api/v1/shipments/${s.id}`, { name: renamed });
    check('200 with the new name', r.status === 200 && r.data.name === renamed, r.data);
    legacy = await api.get('/api/v1/draft-containers', { params: { name: renamed } });
    check('legacy allocations moved to the new name', legacy.data.data.length === 2);
    check('legacy "renamed" event', (await draftEvents(renamed)).includes('renamed'));
    r = await api.patch(`/api/v1/shipments/${s.id}`, { mode: 'AIR', notes: null });
    check('PATCH mode / clear notes', r.status === 200 && r.data.mode === 'AIR' && r.data.notes === null, r.data);
    await api.patch(`/api/v1/shipments/${s.id}`, { mode: 'SEA' });

    section('5. Lines: PUT / DELETE /shipments/:id/lines/:orderId');
    r = await api.put(`/api/v1/shipments/${s.id}/lines/${C.id}`, { quantity: 20 });
    check('PUT a new line', r.status === 200 && r.data.line && r.data.line.quantity === 20, r.data);
    r = await api.put(`/api/v1/shipments/${s.id}/lines/${A.id}`, { quantity: 45 });
    check('PUT changes a quantity', r.status === 200 && r.data.line.quantity === 45, r.data);
    ev = await draftEvents(renamed);
    check('legacy line_added + line_updated events', ev.includes('line_updated') && ev.filter(a => a === 'line_added').length === 3, ev);
    r = await api.delete(`/api/v1/shipments/${s.id}/lines/${C.id}`);
    legacy = await api.get('/api/v1/draft-containers', { params: { name: renamed } });
    check('DELETE removes the legacy allocation', r.status === 200 && legacy.data.data.length === 2);
    check('deleting a line that is not there: 404', (await api.delete(`/api/v1/shipments/${s.id}/lines/${C.id}`)).status === 404);
    check('unknown order: 404', (await api.put(`/api/v1/shipments/${s.id}/lines/999999999`, { quantity: 1 })).status === 404);

    section('6. PLANNED shipments');
    check('a planned shipment needs lines: 422', (await api.post('/api/v1/shipments', { mode: 'SEA', stage: 'PLANNED', name: H.draftName('pl-empty', { kind: 'PLANNED' }) })).status === 422);
    const pname = H.draftName('api-planned', { kind: 'PLANNED' });
    r = await api.post('/api/v1/shipments', { mode: 'SEA', stage: 'PLANNED', name: pname, lines: [{ orderId: C.id, quantity: 5 }] });
    check('201 PLANNED', r.status === 201 && r.data.stage === 'PLANNED' && r.data.lines.length === 1, r.data);
    const p = r.data;
    let planned = await api.get('/api/v1/planned-containers', { params: { name: pname } });
    check('the old SPA sees the planned container', planned.data.data.length === 1);
    const pname2 = H.draftName('api-planned-renamed', { kind: 'PLANNED' });
    r = await api.patch(`/api/v1/shipments/${p.id}`, { name: pname2 });
    planned = await api.get('/api/v1/planned-containers', { params: { name: pname2 } });
    check('PATCH name renames the planned rows', r.status === 200 && planned.data.data.length === 1, r.data);
    r = await api.delete(`/api/v1/shipments/${p.id}`);
    planned = await api.get('/api/v1/planned-containers', { params: { name: pname2 } });
    const pd = await H.shipment(p.id);
    check('DELETE: legacy rows gone, shipment cancelled + soft-deleted', r.status === 200 && planned.data.data.length === 0 && pd.stage === 'CANCELLED' && pd.deletedAt, pd);

    section('7. Documents + history (DRAFT)');
    r = await api.post(`/api/v1/shipments/${s.id}/documents`, { type: 'quote', comments: 'SHIPTEST — please disregard' });
    check('generate 201, stamped with the shipment', r.status === 201 && r.data.shipmentId === s.id, r.data);
    const docs = await api.get(`/api/v1/shipments/${s.id}/documents`);
    check('listed by shipment id', docs.status === 200 && docs.data.data.some(x => x.id === r.data.documentId), docs.data);
    const hist = await api.get(`/api/v1/shipments/${s.id}/history`);
    const actions = hist.data.data.map(e => `${e.entityType}:${e.action}`);
    check('history unions shipment and draft events',
        actions.includes('shipment:create') && actions.includes('draft_container:renamed') && actions.includes('draft_container:document_generated'), actions);

    section('8. DELETE a DRAFT, then reuse its name');
    r = await api.delete(`/api/v1/shipments/${s.id}`);
    reg = await api.get('/api/v1/draft-container-registry', { params: { name: renamed } });
    check('DELETE closes the legacy draft as deleted', r.status === 200 && reg.data.data[0].status === 'deleted', reg.data);
    check('its allocations are gone', (await api.get('/api/v1/draft-containers', { params: { name: renamed } })).data.data.length === 0);
    r = await api.post('/api/v1/shipments', { mode: 'SEA', name: renamed, lines: [{ orderId: A.id, quantity: 10 }] });
    check('the name is free again: a new shipment', r.status === 201 && r.data.id !== s.id && r.data.stage === 'DRAFT', r.data);
    reg = await api.get('/api/v1/draft-container-registry', { params: { name: renamed } });
    check('the legacy registry row reopened', reg.data.data[0].status === 'open', reg.data);
    const reused = r.data;

    section('9. Delete a booked-but-empty shipment, then reuse its reference');
    const R = H.reference('R');
    r = await api.post('/api/v1/shipments', { mode: 'SEA', name: H.draftName('ref-1'), lines: [{ orderId: D.id, quantity: 100 }] });
    const first = r.data;
    r = await api.post(`/api/v1/shipments/${first.id}/book`, { reference: R });
    check('booked with the explicit reference', r.status === 200 && r.data.reference === R && r.data.stage === 'BOOKED', r.data);
    check('a booked shipment with members cannot be deleted', (await api.delete(`/api/v1/shipments/${first.id}`)).status === 409);
    await api.delete(`/api/v1/orders/${D.id}`);
    r = await api.delete(`/api/v1/shipments/${first.id}`);
    check('memberless now: DELETE 200', r.status === 200, r.data);
    r = await api.post('/api/v1/shipments', { mode: 'SEA', name: H.draftName('ref-2'), lines: [{ orderId: reused.lines[0].orderId, quantity: 10 }] });
    await api.delete(`/api/v1/shipments/${reused.id}`);
    const second = r.data;
    r = await api.post(`/api/v1/shipments/${second.id}/book`, { reference: R });
    check('the released reference books again', r.status === 200 && r.data.reference === R, r.data);
}

run().catch(err => H.fail('suite crashed', err)).finally(() => H.finish());
