'use strict';

// QC Report → order matcher (READ-ONLY, no DB writes).
//
// Feeds one or more QC report PDFs to Gemini, asks it to extract every
// (jf_code, lot_number, pass/fail) inspected item, then looks each item up in
// the `orders` table by jf_code + lot_number and prints the order number(s)
// and the QC result. Items whose (jf_code, lot_number) can't be matched are
// printed too, with a near-miss hint when the jf_code exists under a different
// lot.
//
// This is a prototype for a future "upload QC Report" feature — for now it only
// reports; it does NOT update any rows.
//
// Usage:
//   node tools/qc-report-check.js [pdf...] [--model=<id>] [--json]
//   node tools/qc-report-check.js              # the 3 bundled samples
//   node tools/qc-report-check.js tools/sample/1.pdf --model=gemini-3.1-pro-preview

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { GoogleGenAI, FileState, createUserContent, createPartFromUri } = require('@google/genai');
const { getPool, closePool } = require('../src/db');

// ── args ───────────────────────────────────────────────────────────────────
const argv = process.argv.slice(2);
const flags = argv.filter(a => a.startsWith('--'));
const pdfArgs = argv.filter(a => !a.startsWith('--'));
const MODEL = (flags.find(f => f.startsWith('--model=')) || '').split('=')[1] || 'gemini-3-flash-preview';
const JSON_OUT = flags.includes('--json');
// On persistent transient failure (flash overloaded on big scans) fall back to
// Pro, which has separate capacity. Dedup if the caller already picked Pro.
const FALLBACK_MODEL = 'gemini-3.1-pro-preview';
const MODELS = [...new Set([MODEL, FALLBACK_MODEL])];

const DEFAULT_SAMPLES = ['1.pdf', '2.pdf', '3.pdf'].map(f => path.join(__dirname, 'sample', f));
const PDFS = (pdfArgs.length ? pdfArgs : DEFAULT_SAMPLES).map(p => path.resolve(p));

// ── Gemini extraction ────────────────────────────────────────────────────────
const SYSTEM_INSTRUCTION = `You are a quality-control analyst for a UK importer. You receive a supplier Quality Control (QC) / inspection report as a PDF (often a scanned AQL inspection sheet, possibly with photos and tables).

Extract EVERY distinct inspected item the report covers. For each item, return:
- jfCode: the importer's product/style code. It looks like "JF" or "HW" followed by digits, e.g. "JF1372", "HW0168". It may be labelled "JF Code", "JFA", "Item/Style/Article/Model No", "SKU", "Code", or just appear next to the product. Normalise to the bare code (uppercase, no spaces), e.g. "JF1372". If you genuinely cannot find a code for an item, use null.
- lotNumber: the lot / batch number, e.g. "1372002". Labelled "LOT", "LOT No", "Batch", "Batch No". It is usually a 6-8 digit number and frequently begins with the same digits as the jfCode (e.g. jfCode JF1372 → lot 1372002). Return the digits exactly as printed, no spaces. If absent, use null.
- productName: the product description/name if shown, else null.
- quantityInspected: integer quantity inspected/sampled if stated, else null.
- qcResult: the overall verdict for THIS item — one of "pass", "fail", "unknown". Map the report's wording: PASS/ACCEPT/ACCEPTED/CONFORMING/OK/QUALIFIED → "pass"; FAIL/REJECT/REJECTED/NOT ACCEPTED/NON-CONFORMING → "fail". If the report gives no clear verdict for the item, use "unknown".
- resultDetail: a short phrase with the evidence for the verdict (e.g. "AQL pass, 0 critical / 1 minor", "rejected: stitching defects"), else null.

Rules:
- One object per distinct (jfCode, lotNumber) inspected. If the same code+lot appears on several pages for the same inspection, output it once.
- Do NOT invent codes or lots. Copy digits exactly. Better to return null than to guess.
- Many reports cover a single item; that's fine — return an array with one element.`;

const RESPONSE_SCHEMA = {
    type: 'object',
    additionalProperties: false,
    properties: {
        reportTitle: { type: ['string', 'null'] },
        supplier: { type: ['string', 'null'] },
        inspectionDate: { type: ['string', 'null'] },
        items: {
            type: 'array',
            items: {
                type: 'object',
                additionalProperties: false,
                properties: {
                    jfCode: { type: ['string', 'null'] },
                    lotNumber: { type: ['string', 'null'] },
                    productName: { type: ['string', 'null'] },
                    quantityInspected: { type: ['integer', 'null'] },
                    qcResult: { type: 'string', enum: ['pass', 'fail', 'unknown'] },
                    resultDetail: { type: ['string', 'null'] },
                },
                required: ['jfCode', 'lotNumber', 'productName', 'quantityInspected', 'qcResult', 'resultDetail'],
            },
        },
    },
    required: ['reportTitle', 'supplier', 'inspectionDate', 'items'],
};

async function uploadAndWait(ai, pdfPath) {
    const file = await ai.files.upload({
        file: pdfPath,
        config: { mimeType: 'application/pdf', displayName: path.basename(pdfPath) },
    });
    let f = file;
    const started = Date.now();
    while (f.state === FileState.PROCESSING || f.state === 'PROCESSING') {
        if (Date.now() - started > 5 * 60 * 1000) throw new Error('File processing timed out after 5m');
        await new Promise(r => setTimeout(r, 2000));
        f = await ai.files.get({ name: file.name });
    }
    if (f.state === FileState.FAILED || f.state === 'FAILED') {
        throw new Error(`File processing failed: ${f.error?.message || 'unknown'}`);
    }
    return f;
}

// Gemini occasionally returns transient 503 UNAVAILABLE / 429 under load.
// Retry with exponential backoff before giving up.
async function generateWithRetry(ai, params, tries = 4) {
    let lastErr;
    for (let i = 0; i < tries; i++) {
        try {
            return await ai.models.generateContent(params);
        } catch (e) {
            lastErr = e;
            const msg = String(e.message || '');
            const transient = /50\d|429|UNAVAILABLE|high demand|overloaded|RESOURCE_EXHAUSTED/i.test(msg);
            if (!transient || i === tries - 1) throw e;
            const wait = 3000 * Math.pow(2, i);
            console.log(`    transient error (${msg.slice(0, 60)}…), retry ${i + 1}/${tries - 1} in ${wait / 1000}s`);
            await new Promise(r => setTimeout(r, wait));
        }
    }
    throw lastErr;
}

async function extractItems(ai, pdfPath) {
    const uploaded = await uploadAndWait(ai, pdfPath);
    try {
        let lastErr;
        for (const model of MODELS) {
            try {
                const response = await generateWithRetry(ai, {
                    model,
                    contents: createUserContent([
                        createPartFromUri(uploaded.uri, uploaded.mimeType),
                        'Extract the inspected items from this QC report per the schema.',
                    ]),
                    config: {
                        systemInstruction: SYSTEM_INSTRUCTION,
                        responseMimeType: 'application/json',
                        responseSchema: RESPONSE_SCHEMA,
                    },
                });
                const parsed = JSON.parse(response.text);
                return { parsed, usage: response.usageMetadata || null, modelUsed: model };
            } catch (e) {
                lastErr = e;
                if (model !== MODELS[MODELS.length - 1]) {
                    console.log(`    ${model} unavailable, falling back to ${FALLBACK_MODEL}…`);
                }
            }
        }
        throw lastErr;
    } finally {
        ai.files.delete({ name: uploaded.name }).catch(() => {});
    }
}

// ── DB lookup ────────────────────────────────────────────────────────────────
const norm = s => (s == null ? '' : String(s).toUpperCase().replace(/[\s\-_.]/g, '').trim());

async function matchOrders(conn, jfCode, lotNumber) {
    // 1) exact pair (case-insensitive via collation), 2) normalised pair fallback.
    if (jfCode && lotNumber) {
        const [exact] = await conn.query(
            `SELECT id, jf_code, lot_number, po_number, product_name, supplier, status
               FROM orders
              WHERE jf_code = ? AND lot_number = ? AND deleted_at IS NULL
              ORDER BY id`,
            [jfCode, lotNumber]
        );
        if (exact.length) return { how: 'exact', rows: exact };

        const [loose] = await conn.query(
            `SELECT id, jf_code, lot_number, po_number, product_name, supplier, status
               FROM orders
              WHERE UPPER(REPLACE(REPLACE(REPLACE(jf_code,' ',''),'-',''),'_','')) = ?
                AND UPPER(REPLACE(REPLACE(REPLACE(lot_number,' ',''),'-',''),'_','')) = ?
                AND deleted_at IS NULL
              ORDER BY id`,
            [norm(jfCode), norm(lotNumber)]
        );
        if (loose.length) return { how: 'normalised', rows: loose };
    }

    // No pair match — gather near-miss context so the operator can eyeball it.
    const near = {};
    if (jfCode) {
        const [byCode] = await conn.query(
            `SELECT id, jf_code, lot_number, status FROM orders
              WHERE UPPER(REPLACE(jf_code,' ','')) = ? AND deleted_at IS NULL ORDER BY id`,
            [norm(jfCode)]
        );
        near.byJfCode = byCode;
    }
    if (lotNumber) {
        const [byLot] = await conn.query(
            `SELECT id, jf_code, lot_number, status FROM orders
              WHERE UPPER(REPLACE(lot_number,' ','')) = ? AND deleted_at IS NULL ORDER BY id`,
            [norm(lotNumber)]
        );
        near.byLot = byLot;
    }
    return { how: 'none', rows: [], near };
}

// ── output ───────────────────────────────────────────────────────────────────
const RESULT_TAG = { pass: 'PASS', fail: 'FAIL', unknown: '????' };

(async () => {
    if (!process.env.GEMINI_API_KEY) {
        console.error('Missing GEMINI_API_KEY in .env');
        process.exit(1);
    }
    for (const p of PDFS) {
        if (!fs.existsSync(p)) { console.error(`No such file: ${p}`); process.exit(1); }
    }

    const ai = new GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY });
    const conn = await getPool().getConnection();
    const jsonReport = [];

    try {
        for (const pdfPath of PDFS) {
            const sizeMb = (fs.statSync(pdfPath).size / 1e6).toFixed(1);
            console.log('\n' + '='.repeat(72));
            console.log(`REPORT: ${path.basename(pdfPath)}  (${sizeMb} MB)`);
            console.log('='.repeat(72));

            let parsed, usage, modelUsed;
            try {
                ({ parsed, usage, modelUsed } = await extractItems(ai, pdfPath));
            } catch (e) {
                console.error(`  Extraction failed: ${e.message}`);
                jsonReport.push({ file: path.basename(pdfPath), error: e.message });
                continue;
            }

            console.log(`  title:    ${parsed.reportTitle || '—'}`);
            console.log(`  supplier: ${parsed.supplier || '—'}   date: ${parsed.inspectionDate || '—'}`);
            console.log(`  ${parsed.items.length} inspected item(s) extracted via ${modelUsed}` +
                (usage ? `  [${usage.totalTokenCount} tok]` : ''));
            console.log('');

            const fileItems = [];
            for (const it of parsed.items) {
                const tag = RESULT_TAG[it.qcResult] || '????';
                const m = await matchOrders(conn, it.jfCode, it.lotNumber);
                const codeLot = `${it.jfCode || '?'} / lot ${it.lotNumber || '?'}`;

                if (m.rows.length) {
                    const ids = m.rows.map(r => r.id).join(', ');
                    const note = m.how === 'normalised' ? ' (normalised match)' : '';
                    console.log(`  [${tag}]  ${codeLot}`);
                    console.log(`          -> order #${ids}${note}  ${m.rows[0].product_name || ''}`);
                    if (it.resultDetail) console.log(`          ${it.resultDetail}`);
                    fileItems.push({ ...it, matched: true, how: m.how, orderIds: m.rows.map(r => r.id) });
                } else {
                    console.log(`  [${tag}]  ${codeLot}`);
                    console.log(`          -> NOT FOUND (no order with this jf_code + lot_number)`);
                    if (m.near?.byJfCode?.length) {
                        const lots = m.near.byJfCode.map(r => `#${r.id} lot ${r.lot_number || '∅'}`).join(', ');
                        console.log(`          jf_code exists under other lot(s): ${lots}`);
                    }
                    if (m.near?.byLot?.length) {
                        const codes = m.near.byLot.map(r => `#${r.id} ${r.jf_code}`).join(', ');
                        console.log(`          lot exists under other code(s): ${codes}`);
                    }
                    if (it.resultDetail) console.log(`          ${it.resultDetail}`);
                    fileItems.push({ ...it, matched: false, how: 'none', orderIds: [] });
                }
            }

            jsonReport.push({
                file: path.basename(pdfPath),
                reportTitle: parsed.reportTitle, supplier: parsed.supplier,
                inspectionDate: parsed.inspectionDate, items: fileItems,
            });
        }

        // summary
        console.log('\n' + '='.repeat(72));
        console.log('SUMMARY');
        console.log('='.repeat(72));
        const flat = jsonReport.flatMap(r => (r.items || []).map(i => ({ file: r.file, ...i })));
        const matched = flat.filter(i => i.matched);
        console.log(`  ${flat.length} items across ${jsonReport.length} report(s): ` +
            `${matched.length} matched, ${flat.length - matched.length} unmatched`);
        console.table(flat.map(i => ({
            file: i.file,
            jfCode: i.jfCode, lot: i.lotNumber,
            qc: i.qcResult,
            order: i.orderIds && i.orderIds.length ? i.orderIds.join(',') : '—',
            match: i.how,
        })));

        if (JSON_OUT) {
            console.log('\nJSON:\n' + JSON.stringify(jsonReport, null, 2));
        }
    } finally {
        conn.release();
        await closePool();
    }
})().catch(err => { console.error('Fatal:', err); process.exit(1); });
