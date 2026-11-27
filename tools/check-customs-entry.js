'use strict';

// The customs-entry check (src/services/customs-entry-check.js) from the
// command line: an AI report on a forwarder's customs entry or its draft, saying
// whether it matches what we have on file for the container.
//
//   node tools/check-customs-entry.js <entry.pdf>
//       the container is found from the container number the entry prints
//   node tools/check-customs-entry.js <entry.pdf> --container 313
//       or named: our number or the carrier's (FFAU3767679); or --shipment <id>
//   ... --json          the whole report as JSON on stdout (logs go to stderr)
//   ... --out <file>    also write the report as JSON to that file
//
// Reads the database .env names and WRITES NOTHING. The document is sent to
// Gemini, as every document this server reads is.
// Run via the PowerShell tool on this box (Bash has PATH issues for node).

const fs = require('fs');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });

const argVal = (flag) => { const i = process.argv.indexOf(flag); return i >= 0 ? process.argv[i + 1] : null; };
const JSON_OUT = process.argv.includes('--json');
// The services log to stdout: keep it for the report alone when that is JSON.
if (JSON_OUT) console.log = (...args) => console.error(...args);

const { getPool, closePool } = require('../src/db');
const K = require('../src/services/customs-entry-check');
const C = require('../src/lib/customs-entry-compare');

const TYPES = { '.pdf': 'application/pdf', '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg' };

async function main() {
    const file = process.argv[2];
    if (!file || file.startsWith('--')) {
        console.error('usage: node tools/check-customs-entry.js <entry.pdf> [--container <ours or the carrier\'s> | --shipment <id>] [--json] [--out <file>]');
        process.exit(1);
    }
    const contentType = TYPES[path.extname(file).toLowerCase()];
    if (!contentType) throw new Error(`Not a PDF, PNG or JPEG: ${file}`);
    const bytes = fs.readFileSync(file);

    const read = await K.readEntry({ bytes, contentType });
    const entry = read.entry;

    const conn = await getPool().getConnection();
    let shipment = null;
    let basis;
    try {
        const named = argVal('--shipment') ? { shipmentId: Number(argVal('--shipment')) } : argVal('--container') ? { container: argVal('--container') } : null;
        if (named) shipment = await K.findShipment(conn, named);
        else for (const number of entry.containerNumbers) { shipment = await K.findShipment(conn, { container: number }); if (shipment) break; }
        basis = shipment
            ? await K.loadBasis(conn, shipment)
            : { kind: 'none', label: `nothing: no container of ours is ${named ? Object.values(named)[0] : (entry.containerNumbers.join(', ') || 'named on the entry')}`, sheet: null, rows: [], containerCostUsd: null, notes: [] };
    } finally {
        conn.release();
    }

    const report = await K.assess({ entry, basis, container: shipment, readModel: read.modelUsed });
    report.file = path.basename(file);

    const out = argVal('--out');
    if (out) fs.writeFileSync(out, JSON.stringify(report, null, 2));
    if (JSON_OUT) process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
    else {
        console.log(`\n${'='.repeat(100)}\n${path.basename(file)}\n${'='.repeat(100)}`);
        console.log(C.reportText(report));
        console.log(`\n(read by ${report.models.read}; judged by ${report.models.judge || `nobody: ${report.models.judgeError || 'nothing to judge'}`})`);
    }
}

main()
    .catch((e) => { console.error('FAILED:', e.code || '', e.message); process.exitCode = 1; })
    .finally(() => closePool().catch(() => {}));
