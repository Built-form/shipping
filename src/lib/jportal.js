'use strict';

// The supplier portal's schema (the `jportal` repo) on the same MySQL server,
// and the one rule shipping and jportal share for "this purchase order belongs
// to this supplier". Used by the change-request routes
// (src/services/po-change-request-routes.js) and the proforma-invoice
// submission routes and checker (src/services/pi-submission-routes.js,
// src/services/pi-check.js). Moved here from src/lib/po-change-requests.js and
// the change-request routes, which re-export / import them unchanged.

// The schema jportal lives in, backtick-quoted for interpolation. No
// placeholder can carry an identifier, so the name is whitelisted.
function jportalSchema(env = process.env) {
    const name = env.JPORTAL_DB_SCHEMA || 'jportal';
    if (!/^[A-Za-z0-9_]+$/.test(name)) throw new Error('JPORTAL_DB_SCHEMA is not a plain identifier');
    return `\`${name}\``;
}

// Supplier `s` is named exactly as purchase order `po`'s supplier, as jportal
// matches them: utf8mb4_0900_bin is NO PAD, so case, accents and trailing
// spaces all count, and CONVERT avoids an "Illegal mix of collations" between
// the suppliers view and purchase_orders.
const SUPPLIER_NAME_MATCH = `CONVERT(s.name USING utf8mb4) COLLATE utf8mb4_0900_bin
                           = CONVERT(po.supplier USING utf8mb4) COLLATE utf8mb4_0900_bin`;

// The same comparison of `po.supplier` against a bound name (one `?`): a
// jportal row's supplier_name snapshot, which jportal took from
// purchase_orders.supplier when its access rule (the match above) admitted
// the PO. So this can only fail when the PO was renamed or reassigned since.
const PO_SUPPLIER_IS = `CONVERT(po.supplier USING utf8mb4) COLLATE utf8mb4_0900_bin
                      = CONVERT(? USING utf8mb4) COLLATE utf8mb4_0900_bin`;

// MySQL errnos meaning "that schema / table is not there for this login":
// 1049 unknown database, 1142 table command denied, 1143 column command
// denied, 1146 no such table. A source another app owns (jportal, jfpro) that
// answers with one of these is absent in this environment, not broken. Moved
// here from src/services/bank-reference.js; also used by the QC availability
// routes (src/services/qc-availability-routes.js).
const SOURCE_MISSING = new Set([1049, 1142, 1143, 1146]);

const isSourceMissing = err => Boolean(err) && SOURCE_MISSING.has(err.errno);

module.exports = { jportalSchema, SUPPLIER_NAME_MATCH, PO_SUPPLIER_IS, SOURCE_MISSING, isSourceMissing };
