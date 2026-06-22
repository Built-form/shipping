'use strict';

// Smoke test for the draft container PDF generation + email flow.
//
// Hits the live S3 bucket for the PDF upload (cheap — small PDFs in dev
// bucket), but DELIBERATELY skips the actual Front API call so the test
// doesn't spam real recipients. Only the email-validation error paths are
// exercised (400, 404).
//
// Prereqs: orders server on localhost:3001 with IS_OFFLINE=1, and a real
// PO_DOCS_BUCKET (so the generate endpoint can upload).
//
//   $env:IS_OFFLINE=1; node src/handlers/orders.js     # in one shell
//   node tools/test-draft-container-pdf.js             # in another

require('dotenv').config();
const axios = require('axios');

const BASE = process.env.TEST_BASE_URL || 'http://localhost:3001';
const ORDER_A = Number(process.argv[2] || 629);
const ORDER_B = Number(process.argv[3] || 630);
const DRAFT = `DRAFT_PDF_${Date.now()}`;
const EMPTY_DRAFT = `EMPTY_${Date.now()}`;

const api = axios.create({ baseURL: BASE, validateStatus: () => true });

(async () => {
    let pass = 0, fail = 0;
    const allocs = [];
    const docIds = [];
    const ok = (label, cond, extra = '') => {
        if (cond) { pass++; console.log(`  PASS  ${label}`); }
        else      { fail++; console.log(`  FAIL  ${label}${extra ? ' — ' + extra : ''}`); }
    };

    try {
        // Seed two allocations
        console.log(`\nSeed: 2 allocations in ${DRAFT}`);
        for (const [oid, qty] of [[ORDER_A, 50], [ORDER_B, 25]]) {
            const r = await api.post('/api/v1/draft-containers', {
                orderId: oid, draftContainerName: DRAFT, allocated: qty,
            });
            if (r.status !== 201) throw new Error(`seed failed: ${r.status} ${JSON.stringify(r.data)}`);
            allocs.push(r.data.id);
        }
        console.log('  seeded');

        // 1. POST generate — version 1
        console.log(`\n1. POST /draft-containers/${DRAFT}/generate`);
        let r = await api.post(`/api/v1/draft-containers/${encodeURIComponent(DRAFT)}/generate`,
            { comments: 'Test quote request — please disregard.' });
        ok('status 201', r.status === 201, `got ${r.status} ${JSON.stringify(r.data).slice(0,300)}`);
        ok('version=1', r.data?.version === 1, `got ${r.data?.version}`);
        ok('url present', !!r.data?.url);
        ok('fileSize > 1000', (r.data?.fileSize || 0) > 1000);
        if (r.data?.documentId) docIds.push(r.data.documentId);
        const docV1 = r.data?.documentId;

        // 2. POST generate again — version 2
        console.log(`\n2. POST generate again — expect version=2`);
        r = await api.post(`/api/v1/draft-containers/${encodeURIComponent(DRAFT)}/generate`, {});
        ok('status 201', r.status === 201);
        ok('version=2', r.data?.version === 2, `got ${r.data?.version}`);
        if (r.data?.documentId) docIds.push(r.data.documentId);

        // 3. GET documents — expect 2 versions, newest-first
        console.log(`\n3. GET /draft-containers/${DRAFT}/documents`);
        r = await api.get(`/api/v1/draft-containers/${encodeURIComponent(DRAFT)}/documents`);
        ok('status 200', r.status === 200);
        ok('2 documents', r.data?.data?.length === 2, `got ${r.data?.data?.length}`);
        ok('newest first (v2 then v1)',
            r.data?.data?.[0]?.version === 2 && r.data?.data?.[1]?.version === 1);
        ok('sends array present (empty)',
            Array.isArray(r.data?.data?.[0]?.sends) && r.data.data[0].sends.length === 0);

        // 4. Generate against empty draft — 400
        console.log(`\n4. POST generate on ${EMPTY_DRAFT} (no allocations) — expect 400`);
        r = await api.post(`/api/v1/draft-containers/${encodeURIComponent(EMPTY_DRAFT)}/generate`, {});
        ok('status 400', r.status === 400, `got ${r.status}`);

        // 5. Email validation — missing `to`
        console.log(`\n5. POST /draft-container-documents/${docV1}/email with no to — expect 400`);
        r = await api.post(`/api/v1/draft-container-documents/${docV1}/email`, {});
        ok('status 400', r.status === 400, `got ${r.status}`);

        // 6. Email validation — invalid email
        console.log(`\n6. POST email with garbage address — expect 400`);
        r = await api.post(`/api/v1/draft-container-documents/${docV1}/email`,
            { to: 'not-an-email' });
        ok('status 400', r.status === 400, `got ${r.status}`);

        // 7. Email — nonexistent document
        console.log(`\n7. POST email for docId=999999 — expect 404`);
        r = await api.post(`/api/v1/draft-container-documents/999999/email`,
            { to: 'test@example.com' });
        ok('status 404', r.status === 404, `got ${r.status}`);

        console.log('\n(Skipping live Front send — would actually email someone.)');

    } catch (e) {
        fail++;
        console.error('CRASH:', e.message);
    } finally {
        // Cleanup allocations. Doc rows live until you manually purge.
        for (const id of allocs) {
            try { await api.delete(`/api/v1/draft-containers/${id}`); } catch {}
        }
        console.log(`\n${pass} passed, ${fail} failed`);
        if (docIds.length) {
            console.log(`Test doc rows left behind (not auto-cleaned): ${docIds.join(', ')}`);
        }
        process.exit(fail ? 1 : 0);
    }
})();
