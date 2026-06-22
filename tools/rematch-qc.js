'use strict';
// Re-runs order matching for existing QC reports using the CURRENT matchOrders
// rules (e.g. leading-zero-insensitive lots) WITHOUT re-calling Gemini — it
// reuses each report's already-extracted items from result_json. So it isolates
// the effect of a matching-logic change from Gemini's (non-deterministic)
// extraction.
//
// Dry-run by default; pass --apply to rewrite order_qc_reports + matched_count.
// Usage: node tools/rematch-qc.js [--apply] [reportId ...]

require('dotenv').config();
const { getPool, closePool } = require('../src/db');
const { matchOrders, countMatchedItems } = require('../src/services/qc-report-check');

const APPLY = process.argv.includes('--apply');
const ids = process.argv.slice(2).filter(a => /^\d+$/.test(a)).map(Number);

function parseJson(v) { if (v == null) return null; if (typeof v === 'object') return v; try { return JSON.parse(v); } catch { return null; } }

(async () => {
    const pool = getPool();
    const conn = await pool.getConnection();
    try {
        const where = ids.length ? `AND id IN (${ids.map(() => '?').join(',')})` : '';
        const [reports] = await conn.query(
            `SELECT id, report_title, result_json, matched_count FROM qc_reports
              WHERE deleted_at IS NULL AND result_json IS NOT NULL ${where} ORDER BY id`, ids
        );
        console.log(`${APPLY ? 'APPLYING' : 'DRY RUN'} — ${reports.length} report(s)\n`);

        let totalGain = 0;
        for (const r of reports) {
            const data = parseJson(r.result_json) || {};
            const items = data.items || [];
            const matched = [], unmatched = [];
            for (const it of items) {
                const m = await matchOrders(conn, it.jfCode, it.lotNumber);
                if (m.rows.length) {
                    for (const row of m.rows) matched.push({ orderId: row.id, jfCode: it.jfCode || null, lotNumber: it.lotNumber || null, qcResult: it.qcResult, resultDetail: it.resultDetail || null, matchMethod: m.how });
                } else {
                    unmatched.push({ jfCode: it.jfCode || null, lotNumber: it.lotNumber || null, qcResult: it.qcResult, resultDetail: it.resultDetail || null, nearMiss: m.near || null });
                }
            }
            const before = Number(r.matched_count ?? 0);
            const matchedItems = countMatchedItems(matched);
            const delta = matchedItems - before;
            if (delta > 0) totalGain += delta;
            console.log(`#${r.id} ${(r.report_title || '').slice(0, 38).padEnd(39)} ${String(before).padStart(2)} → ${String(matchedItems).padStart(2)} matched (${matched.length} link(s))${delta ? `  Δ${delta > 0 ? '+' : ''}${delta}` : ''}`);

            if (APPLY) {
                await conn.query(`DELETE FROM order_qc_reports WHERE qc_report_id = ?`, [r.id]);
                for (const m of matched) {
                    await conn.query(
                        `INSERT INTO order_qc_reports (qc_report_id, order_id, jf_code, lot_number, qc_result, result_detail, match_method)
                         VALUES (?, ?, ?, ?, ?, ?, ?)
                         ON DUPLICATE KEY UPDATE jf_code=VALUES(jf_code), lot_number=VALUES(lot_number),
                            qc_result=VALUES(qc_result), result_detail=VALUES(result_detail), match_method=VALUES(match_method)`,
                        [r.id, m.orderId, m.jfCode, m.lotNumber, m.qcResult, m.resultDetail ? String(m.resultDetail).slice(0, 1000) : null, m.matchMethod]
                    );
                }
                await conn.query(
                    `UPDATE qc_reports SET matched_count = ?, item_count = ?, result_json = ? WHERE id = ?`,
                    [matchedItems, items.length, JSON.stringify({ ...data, matched, unmatched, items }), r.id]
                );
            }
        }
        console.log(`\n${APPLY ? 'Applied.' : 'Dry run — nothing written.'} Additional matches available: +${totalGain}`);
    } finally { conn.release(); await closePool(); }
})().catch(e => { console.error(e.message); process.exit(1); });
