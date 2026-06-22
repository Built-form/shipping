'use strict';

// Updates purchase_orders.created_at for a list of (po_number, place_date)
// pairs. Place dates are in US M/D/YYYY format. Times are normalised to
// 12:00:00 (noon) on the given date — date-of-day is what matters here, and
// noon avoids timezone-boundary confusion.
//
// Two-pass safety:
//   1. DRY RUN — look up each po_number, show which match, which don't,
//      and what the proposed new date is vs the current created_at.
//   2. APPLY — if all input matched (or --force is passed), run the updates
//      in one transaction. Errors anywhere abort the whole batch.
//
// Usage:
//   node tools/update-po-created-dates.js           ← dry run only
//   node tools/update-po-created-dates.js --apply   ← actually update
//   node tools/update-po-created-dates.js --apply --force  ← apply even if some PO numbers don't match

require('dotenv').config();
const { getPool, closePool } = require('../src/db');

const APPLY = process.argv.includes('--apply');
const FORCE = process.argv.includes('--force');

// Raw input from the user: "PO_NUMBER PLACE_DATE" per row.
const INPUT = `
MANFO-113 10/2/2025
MANFO-96 7/9/2024
PACKRICH-14 11/20/2025
YOHO-98 1/13/2026
YOHO-97 10/2/2025
YOHO-91 5/15/2025
OWENTEK-02 10/2/2025
OWENTEK-01 6/26/2025
TAM-118 5/1/2025
TAM-119 7/2/2025
TAM-117 2/11/2025
TAM-123 10/16/2025
UKHWNBPASS-39 3/11/2026
YOHO-103 4/27/2026
MANFO-119 3/30/2026
YOHO-102 3/30/2026
YOHO-101 3/25/2026
MANFO-118 3/25/2026
LIPU-01 3/11/2026
YOHO-100 3/12/2026
MANFO-117 3/12/2026
TAM-124 3/11/2026
YOHO-99 3/11/2026
MANFO-116 3/11/2026
TAM-114 11/1/2024
TAM-122 10/2/2025
MANFO-95 7/11/2024
MANFO-104 11/1/2024
MANFO-112 8/5/2025
YOHO-93 6/26/2025
MANFO-108 3/18/2025
MANFO-111 6/25/2025
MANFO-90 4/16/2024
MANFO-98 8/7/2024
YOHO-67 4/25/2024
YOHO-90 4/22/2025
MANFO-110 5/15/2025
`.trim();

function parseUSDate(s) {
    const m = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(s.trim());
    if (!m) return null;
    const month = Number(m[1]);
    const day = Number(m[2]);
    const year = Number(m[3]);
    if (month < 1 || month > 12 || day < 1 || day > 31) return null;
    return `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')} 12:00:00`;
}

const parsed = INPUT.split('\n')
    .map(l => l.trim())
    .filter(Boolean)
    .map(line => {
        const lastSpace = line.lastIndexOf(' ');
        if (lastSpace < 0) return { line, error: 'no space' };
        const poNumber = line.slice(0, lastSpace).trim();
        const dateStr = line.slice(lastSpace + 1).trim();
        const newDate = parseUSDate(dateStr);
        return { poNumber, dateStr, newDate, error: newDate ? null : `bad date "${dateStr}"` };
    });

const bad = parsed.filter(p => p.error);
if (bad.length) {
    console.error('Input parse errors:');
    for (const b of bad) console.error(`  ${b.poNumber || b.line}: ${b.error}`);
    process.exit(1);
}

(async () => {
    const pool = getPool();
    const conn = await pool.getConnection();
    try {
        // 1. Look up each PO number. Use case-insensitive match in case the
        //    DB has different casing than the input.
        const lowerNumbers = parsed.map(p => p.poNumber.toLowerCase());
        const placeholders = lowerNumbers.map(() => '?').join(',');
        const [rows] = await conn.query(
            `SELECT id, po_number, created_at FROM purchase_orders
              WHERE LOWER(po_number) IN (${placeholders})
                AND deleted_at IS NULL`,
            lowerNumbers
        );

        const byLowerNumber = new Map();
        for (const r of rows) byLowerNumber.set(r.po_number.toLowerCase(), r);

        const matched = [];
        const unmatched = [];
        for (const p of parsed) {
            const r = byLowerNumber.get(p.poNumber.toLowerCase());
            if (r) {
                matched.push({
                    id: r.id,
                    csvPo: p.poNumber,
                    dbPo: r.po_number,
                    currentCreated: r.created_at,
                    newCreated: p.newDate,
                });
            } else {
                unmatched.push({ csvPo: p.poNumber, newCreated: p.newDate });
            }
        }

        console.log(`Input rows: ${parsed.length}`);
        console.log(`Matched:    ${matched.length}`);
        console.log(`Unmatched:  ${unmatched.length}`);

        if (unmatched.length) {
            console.log('\nUnmatched PO numbers (not in DB or soft-deleted):');
            console.table(unmatched);
        }

        if (matched.length) {
            console.log('\nFirst 10 matched rows (preview):');
            console.table(matched.slice(0, 10).map(m => ({
                id: m.id,
                csvPo: m.csvPo,
                dbPo: m.dbPo,
                from: m.currentCreated?.toISOString?.() ?? m.currentCreated,
                to: m.newCreated,
            })));
        }

        if (!APPLY) {
            console.log('\n(dry run — re-run with --apply to actually update)');
            return;
        }
        if (unmatched.length && !FORCE) {
            console.log('\nRefusing to apply because some PO numbers did not match.');
            console.log('Pass --force to update only the matched rows anyway.');
            return;
        }

        console.log('\nApplying updates…');
        await conn.beginTransaction();
        let updated = 0;
        try {
            for (const m of matched) {
                await conn.execute(
                    `UPDATE purchase_orders SET created_at = ? WHERE id = ?`,
                    [m.newCreated, m.id]
                );
                updated++;
            }
            await conn.commit();
            console.log(`Done — updated ${updated} rows.`);
        } catch (e) {
            await conn.rollback();
            console.error('Update failed — rolled back. Reason:', e.message);
            process.exitCode = 1;
        }

        // Verification readback
        const [readback] = await conn.query(
            `SELECT id, po_number, created_at FROM purchase_orders
              WHERE id IN (${matched.map(() => '?').join(',')})
              ORDER BY created_at DESC`,
            matched.map(m => m.id)
        );
        console.log('\nVerification readback (first 10):');
        console.table(readback.slice(0, 10).map(r => ({
            id: r.id,
            po: r.po_number,
            created_at: r.created_at?.toISOString?.() ?? r.created_at,
        })));
    } catch (err) {
        console.error('Fatal:', err.message);
        process.exitCode = 1;
    } finally {
        conn.release();
        await closePool();
    }
})();
