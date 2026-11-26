'use strict';

// One quote, more than one container (2026-11-26, docs/freight-quotes.md "Book
// for another container"). A request holds one live booking; a second container
// on the same quote is booked on a COPY of the request, made by ShipLine with
// the chosen option as its one answer and a link of its own.
//
//   freight_quote_requests.copied_from_request_id   the request this one was
//                                     copied from (the original, also for a copy
//                                     of a copy); NULL for every request a
//                                     forwarder was asked for. Set when the copy
//                                     is made; no statement changes it.
//
// Additive: every existing row reads as it did. The portal never reads the
// column, so it needs no grant. No foreign keys, like the rest of this schema.
// Each step looks at information_schema first, so a re-run changes nothing.

const has = async (conn, sql, params) => Number((await conn.query(sql, params))[0][0].n) > 0;
const hasColumn = (conn, table, column) => has(conn,
    `SELECT COUNT(*) AS n FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?`, [table, column]);
const hasIndex = (conn, table, index) => has(conn,
    `SELECT COUNT(*) AS n FROM information_schema.STATISTICS
      WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = ?`, [table, index]);

const STEPS = [
    ['column', 'freight_quote_requests', 'copied_from_request_id', 'ALTER TABLE freight_quote_requests ADD COLUMN copied_from_request_id INT NULL'],
    ['index', 'freight_quote_requests', 'idx_freight_requests_copied_from', 'ALTER TABLE freight_quote_requests ADD KEY idx_freight_requests_copied_from (copied_from_request_id)'],
];

exports.STEPS = STEPS;

exports.up = async (conn, { log }) => {
    let added = 0;
    for (const [kind, table, name, sql] of STEPS) {
        const present = kind === 'column' ? await hasColumn(conn, table, name) : await hasIndex(conn, table, name);
        if (present) continue;
        await conn.query(sql);
        added++;
    }
    log(`freight request copies: ${added} of ${STEPS.length} schema change(s) applied`);
};
