'use strict';

// Run the packing-list check on a local file, without the API or S3: Gemini
// reads the PDF, then it is compared against the supplier's lines in the
// container (a booked container's orders, or a draft's allocations). Read-only
// against the DB.
//
// Usage:
//   node tools/packing-list-check.js <file.pdf|-> <container> [supplier name|id|key] [options]
//     <container>  booked ref ('324'), carrier box, exact draft name, a draft's
//                  number ('328'), or draft:<draft_containers.id>
//   --json                 print { extracted, comparison } as JSON
//   --model=<gemini id>    override the model
//   --extracted=<file>     skip Gemini: compare a saved extraction ({ extracted }
//                          from --json, or the bare extraction). File arg may be '-'.
//   node tools/packing-list-check.js "BY SEA PL-26_89160.pdf" 324 SUNMED
// With no supplier, lists the suppliers on board and exits.
// Against TEST: set DOTENV_CONFIG_PATH=.env.test and run with -r dotenv/config.

require('dotenv').config();
const fs = require('fs');
const { getPool, closePool } = require('../src/db');
const P = require('../src/services/packing-list-check');

const opt = name => (process.argv.find(a => a.startsWith(`--${name}=`)) || '').slice(name.length + 3) || undefined;

(async () => {
    const modelArg = opt('model');
    const extractedArg = opt('extracted');
    const args = process.argv.slice(2).filter(a => !a.startsWith('--'));
    const asJson = process.argv.includes('--json');
    const [file, containerArg, supplierArg] = args;
    if (!file || !containerArg) {
        console.error('usage: node tools/packing-list-check.js <file.pdf|-> <container|draft:<id>> [supplier] [--json] [--model=] [--extracted=]');
        process.exit(1);
    }
    const conn = await getPool().getConnection();
    try {
        const draftId = /^draft:(\d+)$/.exec(containerArg);
        const r = await P.resolveContainer(conn, draftId ? { draftContainerId: draftId[1] } : { containerNumber: containerArg });
        if (!r) throw new Error(`No booked container or open draft matches ${containerArg}.`);
        if (r.ambiguous) {
            console.table(r.ambiguous.map(c => ({ draftId: c.draftId, name: c.draftName, lines: c.lineCount, units: c.units })));
            throw new Error(`${containerArg} is ambiguous — use draft:<id>.`);
        }
        const container = r.container;
        const suppliers = await P.loadContainerSuppliers(conn, container);
        if (!supplierArg) {
            console.log(P.describeContainer(container));
            console.table(suppliers.map(s => ({ key: s.supplierKey, name: s.supplierName, lines: s.orderCount, units: s.units })));
            return;
        }
        const key = await P.resolveSupplierChoice(conn, {
            supplierKey: supplierArg,
            supplierId: /^\d+$/.test(supplierArg) ? supplierArg : null,
            supplierName: supplierArg,
        });

        let extracted;
        if (extractedArg) {
            const j = JSON.parse(fs.readFileSync(extractedArg, 'utf8').replace(/^﻿/, ''));
            extracted = j.extracted || j;
            console.error(`${container.kind} ${container.label} · supplier ${key} · replaying ${extractedArg} (${extracted.lines.length} rows)`);
        } else {
            console.error(`${container.kind} ${container.label} · supplier ${key} · reading ${file} …`);
            const t0 = Date.now();
            const read = await P.readPackingList({ bytes: fs.readFileSync(file), contentType: 'application/pdf', model: modelArg });
            console.error(`read by ${read.modelUsed} in ${((Date.now() - t0) / 1000).toFixed(1)}s, ${read.parsed.lines.length} rows`);
            extracted = read.parsed;
        }
        const cmp = await P.buildComparison(conn, { container, supplierKey: key, extracted });
        if (asJson) { console.log(JSON.stringify({ extracted, comparison: cmp }, null, 2)); return; }

        console.log('\ncontainer', cmp.container);
        console.log('summary', cmp.summary);
        console.log('extraction check', cmp.extractionCheck);
        if (cmp.skippedLines.length) console.log('skipped (already booked)', cmp.skippedLines);
        for (const l of cmp.lines) {
            const exp = l.expected ? l.expected.quantity : '-';
            const got = l.packed ? l.packed.quantity : '-';
            console.log(`\n[${l.status.toUpperCase()}${l.matchMethod ? ` ${l.matchMethod}` : ''}] ${l.jfCode} ${l.poNumbers.join(',')}  expected ${exp} / packed ${got}`);
            for (const o of (l.expected?.orders || []).filter(o => o.orderQuantity !== o.quantity)) {
                console.log(`   order ${o.orderId}: ${o.quantity} of ${o.orderQuantity} in this container`);
            }
            for (const d of l.differences) console.log(`   ${d.severity.padEnd(7)} ${d.field}: ${d.message}`);
            for (const e of l.elsewhere || []) console.log(`   elsewhere: ${e.kind} ${e.where || ''} · order ${e.orderId} ${e.poNumber || ''} qty ${e.quantity} ${e.status}`);
        }
    } finally {
        conn.release();
        await closePool();
    }
})().catch(e => { console.error(e.message || e); process.exit(1); });
