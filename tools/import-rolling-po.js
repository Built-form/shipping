'use strict';

// Import rolling-PO CSV (one row per ASIN with up to 4 future PO date/qty
// pairs) into `orders` as one SCHEDULED row per future PO slot where qty > 0.
//
// Each inserted row is tagged in `notes` with `[rolling-po-import <date>]`
// so the whole batch can be deleted again with:
//   DELETE FROM orders WHERE notes LIKE '[rolling-po-import 2026-05-11]%';
//
// Usage:
//   node tools/import-rolling-po.js <csv-path>            # dry run, prints summary
//   node tools/import-rolling-po.js <csv-path> --apply    # actually insert

require('dotenv').config();
const fs = require('fs');
const { parse } = require('csv-parse/sync');
const { getPool, closePool } = require('../src/db');

const TODAY = new Date();
TODAY.setHours(0, 0, 0, 0);
const TAG = `[rolling-po-import ${TODAY.toISOString().slice(0, 10)}]`;

const MONTHS = {
    january: 1, february: 2, march: 3, april: 4, may: 5, june: 6,
    july: 7, august: 8, september: 9, october: 10, november: 11, december: 12,
};

function clean(s) {
    if (s == null) return null;
    // Strip BOM, non-breaking space (and its UTF-8 mojibake "Â"), and trim.
    const out = String(s).replace(/^﻿/, '').replace(/[ Â]+/g, '').trim();
    return out || null;
}

// Parse "March 11, 2026" -> "2026-03-11". Returns null if unparseable.
function parseDate(s) {
    const v = clean(s);
    if (!v) return null;
    const m = v.match(/^([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})$/);
    if (!m) return null;
    const month = MONTHS[m[1].toLowerCase()];
    if (!month) return null;
    const day = parseInt(m[2], 10);
    const year = parseInt(m[3], 10);
    const iso = `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
    const d = new Date(`${iso}T00:00:00Z`);
    if (isNaN(d.getTime())) return null;
    return { iso, date: d };
}

function loadRows(path) {
    const text = fs.readFileSync(path, 'utf8');
    const records = parse(text, {
        columns: (header) => header.map(h => h.replace(/\s+/g, ' ').trim()),
        skip_empty_lines: true,
        relax_column_count: true,
        trim: true,
    });
    return records;
}

function buildInserts(records) {
    const inserts = [];
    const skipReasons = { pastDate: 0, noDate: 0, zeroQty: 0, noQty: 0, blankRow: 0 };

    for (const row of records) {
        const jfCode = clean(row['JF Code']);
        const asinRaw = clean(row['ASIN']);
        const asin = asinRaw && asinRaw.toUpperCase() !== 'N/A' ? asinRaw : null;

        if (!jfCode && !asin) { skipReasons.blankRow++; continue; }

        for (let i = 1; i <= 4; i++) {
            const dateRaw = row[`PO ${i} Date`];
            const qtyRaw = row[`PO ${i} QTY`];
            const qty = parseInt(clean(qtyRaw), 10);
            const parsed = parseDate(dateRaw);

            if (!parsed) {
                if (Number.isFinite(qty) && qty > 0) skipReasons.noDate++;
                continue;
            }
            if (!Number.isFinite(qty)) { skipReasons.noQty++; continue; }
            if (qty <= 0) { skipReasons.zeroQty++; continue; }
            if (parsed.date <= TODAY) { skipReasons.pastDate++; continue; }

            inserts.push({
                jfCode,
                asin,
                quantity: qty,
                scheduledDate: parsed.iso,
                notes: `${TAG} PO ${i} of 4 (${parsed.iso}, qty ${qty})`,
                slot: i,
            });
        }
    }

    return { inserts, skipReasons };
}

async function applyInserts(inserts) {
    const pool = getPool();
    const conn = await pool.getConnection();
    try {
        await conn.beginTransaction();
        const sql = `
            INSERT INTO orders
                (jf_code, asin, quantity, status, scheduled_date, notes, dates)
            VALUES
                (?, ?, ?, 'SCHEDULED', ?, ?, '{}')
        `;
        let inserted = 0;
        for (const r of inserts) {
            await conn.execute(sql, [r.jfCode, r.asin, r.quantity, r.scheduledDate, r.notes]);
            inserted++;
        }
        await conn.commit();
        return inserted;
    } catch (err) {
        await conn.rollback();
        throw err;
    } finally {
        conn.release();
    }
}

(async () => {
    const args = process.argv.slice(2);
    const apply = args.includes('--apply');
    const csvPath = args.find(a => !a.startsWith('--'));
    if (!csvPath) {
        console.error('usage: node tools/import-rolling-po.js <csv-path> [--apply]');
        process.exit(1);
    }

    const records = loadRows(csvPath);
    console.log(`Parsed ${records.length} CSV rows.`);
    console.log(`Today (cutoff): ${TODAY.toISOString().slice(0, 10)}. Tag: ${TAG}`);

    const { inserts, skipReasons } = buildInserts(records);

    console.log(`\nWould insert ${inserts.length} orders.`);
    console.log('Skipped:', skipReasons);

    // Preview first/last 5
    const preview = [...inserts.slice(0, 5), ...(inserts.length > 10 ? [{ jfCode: '...', asin: '...' }] : []), ...inserts.slice(-5)];
    if (inserts.length) {
        console.log('\nPreview:');
        for (const r of preview) {
            if (r.jfCode === '...') { console.log('  ...'); continue; }
            console.log(`  ${r.jfCode}\t${r.asin || '-'}\tqty=${r.quantity}\t${r.scheduledDate}\tslot=${r.slot}`);
        }
    }

    if (!apply) {
        console.log('\n(dry run — rerun with --apply to insert)');
        await closePool();
        return;
    }

    console.log('\nInserting...');
    const inserted = await applyInserts(inserts);
    console.log(`Inserted ${inserted} rows tagged ${TAG}.`);
    console.log(`To undo: DELETE FROM orders WHERE notes LIKE '${TAG}%';`);
    await closePool();
})().catch(err => {
    console.error('ERROR:', err.message);
    process.exit(1);
});
