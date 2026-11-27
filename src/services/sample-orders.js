'use strict';

// Sample orders: the database side of src/lib/sample-orders.js (the rules, pure).
// Read docs/sample-orders.md first.
//
// Everything a route needs before it writes an orders row:
//
//   guardWrite(conn, { beforeRow, afterRow, via, req })
//       the ONE call every write path makes. For a sample line it runs the
//       validator; for a stock line it only stops the two ways a sample could
//       leak into stock (a stock line on a sample PO, a stock line taking a
//       sample lot). Returns { refusal } or what the caller must write.
//
// and the facts the validator cannot know by itself: whether the schema has
// the sample columns at all, whether sample orders are armed on this stage,
// the kind of a PO and whether it is committed, whether a lot is a sample lot
// of a product in JFPRO's register.
//
// A database without the sample columns (a TEST rebuilt from a production that
// does not have them yet) holds no sample: every helper here then answers
// "stock", and nothing in the stock paths changes.

const S = require('../lib/sample-orders');
const L = require('../lib/local-orders');
const { jportalSchema } = require('../lib/jportal');
const log = require('../lib/logger');

const CACHE_MS = 60_000;

// ── Does this database know about samples? ───────────────────────────────
// A yes is kept for the life of the process (columns do not vanish under a
// running server except by a rebuild, which also restarts nothing: so a yes is
// re-asked every minute too, cheaply). A no is re-asked every minute.
let schemaSeen = { has: false, at: 0 };

async function schemaReady(conn) {
    const now = Date.now();
    if (now - schemaSeen.at < CACHE_MS) return schemaSeen.has;
    const [rows] = await conn.query(
        `SELECT COUNT(*) AS n FROM information_schema.COLUMNS
          WHERE TABLE_SCHEMA = DATABASE()
            AND ((TABLE_NAME = 'orders' AND COLUMN_NAME IN ('order_kind', 'sample_ever_sent', 'sample_status_changed_at', 'sample_received_qty', 'courier_name', 'courier_tracking'))
              OR (TABLE_NAME = 'purchase_orders' AND COLUMN_NAME = 'kind'))`
    );
    schemaSeen = { has: Number(rows[0].n) === 7, at: now };
    return schemaSeen.has;
}

// `AND orders.order_kind = 'stock'` for a reader that must leave samples out,
// or nothing where the column does not exist. `alias` is the orders table's
// name in the caller's statement; it is spliced into SQL, so it is checked.
async function stockOnlySql(conn, alias = 'orders') {
    if (!/^[a-z_]+$/i.test(alias)) throw new Error(`stockOnlySql: bad alias ${alias}`);
    return (await schemaReady(conn)) ? ` AND ${alias}.order_kind = 'stock'` : '';
}

// `AND orders.order_kind <> 'sample'` for a reader that counts STOCK GOODS:
// stock lines and local lines (docs/local-orders.md), never a sample. Cashboard's
// stock figures read through this one. stockOnlySql above is for the readers
// that talk to a supplier about production (reminders, the email suggestions,
// the old ready-date page), where a local order has nothing to say.
async function notSampleSql(conn, alias = 'orders') {
    if (!/^[a-z_]+$/i.test(alias)) throw new Error(`notSampleSql: bad alias ${alias}`);
    return (await schemaReady(conn)) ? ` AND ${alias}.order_kind <> 'sample'` : '';
}

// ── Can this database hold a local order? ────────────────────────────────
// Yes once both kind CHECKs allow the word (migration 2026-11-27_30). A TEST
// rebuilt from a production without that migration says no, and POST
// /purchase-orders refuses a local PO with a 409 instead of a CHECK error.
let localSeen = { has: false, at: 0 };

async function localReady(conn) {
    const now = Date.now();
    if (now - localSeen.at < CACHE_MS) return localSeen.has;
    let has = false;
    if (await schemaReady(conn)) {
        const [rows] = await conn.query(
            `SELECT CONSTRAINT_NAME, CHECK_CLAUSE FROM information_schema.CHECK_CONSTRAINTS
              WHERE CONSTRAINT_SCHEMA = DATABASE()
                AND CONSTRAINT_NAME IN ('chk_purchase_orders_kind', 'chk_orders_order_kind')`
        );
        const allows = name => rows.some(r => r.CONSTRAINT_NAME === name && /'local'/.test(String(r.CHECK_CLAUSE || '')));
        has = allows('chk_purchase_orders_kind') && allows('chk_orders_order_kind');
    }
    localSeen = { has, at: now };
    return has;
}

// ── Armed? ───────────────────────────────────────────────────────────────
// A row in app_migrations named for THIS stage (APP_STAGE: 'dev' is
// production, 'test' is TEST). Read fresh where a sample is about to be
// created, so no warm server creates one from a remembered answer after the
// database under it was rebuilt; cached a minute for display.
const stage = () => process.env.APP_STAGE || '';
let armedSeen = { on: false, at: 0 };

async function armed(conn, { fresh = false } = {}) {
    const name = S.armingRowName(stage());
    if (!name) return false;
    const now = Date.now();
    if (!fresh && now - armedSeen.at < CACHE_MS) return armedSeen.on;
    if (!await schemaReady(conn)) { armedSeen = { on: false, at: now }; return false; }
    const [rows] = await conn.query('SELECT name FROM app_migrations WHERE name = ?', [name]);
    armedSeen = { on: S.isArmed(rows.map(r => r.name), stage()), at: now };
    return armedSeen.on;
}

// For the unit suite.
function resetCaches() {
    schemaSeen = { has: false, at: 0 };
    armedSeen = { on: false, at: 0 };
    localSeen = { has: false, at: 0 };
}

// ── The purchase order ───────────────────────────────────────────────────
// { id, kind, supplier, currency, shippingTotal, deleted } or null. Deleted
// rows are returned (a deleted line still points at its PO).
async function loadPo(conn, poId, { lock = false } = {}) {
    const id = Number(poId);
    if (!Number.isSafeInteger(id) || id <= 0) return null;
    const hasKind = await schemaReady(conn);
    const [rows] = await conn.query(
        `SELECT id, ${hasKind ? 'kind' : "'stock' AS kind"}, supplier, currency, shipping_total, deleted_at
           FROM purchase_orders WHERE id = ?${lock ? ' FOR UPDATE' : ''}`,
        [id]
    );
    if (!rows.length) return null;
    const r = rows[0];
    return {
        id: Number(r.id),
        kind: r.kind === S.KIND_SAMPLE || r.kind === L.KIND_LOCAL ? r.kind : S.KIND_STOCK,
        supplier: r.supplier ?? null,
        currency: r.currency ?? null,
        shippingTotal: Number(r.shipping_total ?? 0),
        deleted: r.deleted_at != null,
    };
}

// Money recorded on the PO: a deposit paid by transfer, a PI marked paid, or a
// transfer applied to one of its PIs (a settlement by credit note is a transfer
// with nothing sent, so it is one of these).
async function poMoneyRecorded(conn, poId) {
    const id = Number(poId);
    const [rows] = await conn.query(
        `SELECT 1 AS hit
           FROM supplier_payment_lines l
           INNER JOIN supplier_payments p ON p.id = l.payment_id AND p.deleted_at IS NULL
          WHERE l.target_kind = 'po_deposit' AND l.target_id = ?
         UNION ALL
         SELECT 1
           FROM purchase_order_invoice_payments ip
           INNER JOIN purchase_order_invoices i ON i.id = ip.purchase_order_invoice_id AND i.deleted_at IS NULL
          WHERE ip.purchase_order_id = ?
            AND (ip.payment_status = 'paid' OR ip.settled_by_payment_id IS NOT NULL)
         UNION ALL
         SELECT 1
           FROM supplier_payment_lines l
           INNER JOIN supplier_payments p ON p.id = l.payment_id AND p.deleted_at IS NULL
           INNER JOIN purchase_order_invoice_payments ip ON ip.id = l.target_id
          WHERE l.target_kind = 'pi' AND ip.purchase_order_id = ?
          LIMIT 1`,
        [id, id, id]
    );
    return rows.length > 0;
}

// Emailed to the supplier: a version of its PDF has a send row.
async function poEmailed(conn, poId) {
    const [rows] = await conn.query(
        `SELECT 1 AS hit
           FROM purchase_order_document_sends s
           INNER JOIN purchase_order_documents d ON d.id = s.purchase_order_document_id
          WHERE d.purchase_order_id = ? LIMIT 1`,
        [Number(poId)]
    );
    return rows.length > 0;
}

// Committed: the supplier has it, or money is on it. From then on what it costs
// changes only by an admin, with a reason.
async function poCommitted(conn, poId) {
    return (await poEmailed(conn, poId)) || (await poMoneyRecorded(conn, poId));
}

// ── JFPRO's sample lots ──────────────────────────────────────────────────
const lotText = v => String(v ?? '').trim();

// Is `lot` a sample lot of the product `jfCode`? `lock` takes a shared lock on
// the lot, its product link and the product (inside the caller's transaction),
// so JFPRO cannot rename, retype, unlink or delete the sample between this
// check and the order row being committed.
async function lotIsSampleOf(conn, lot, jfCode, { lock = false } = {}) {
    const [rows] = await conn.query(
        `SELECT lot_id FROM sample_lots_v
          WHERE UPPER(TRIM(lot_number)) = UPPER(?) AND UPPER(TRIM(jf_code)) = UPPER(?)${lock ? ' FOR SHARE' : ''}`,
        [lotText(lot), lotText(jfCode)]
    );
    return rows.length > 0;
}

// Is `lot` a sample lot of any product?
async function lotIsSample(conn, lot) {
    const text = lotText(lot);
    if (!text) return false;
    const [rows] = await conn.query(
        'SELECT lot_id FROM sample_lots_v WHERE UPPER(TRIM(lot_number)) = UPPER(?) LIMIT 1', [text]
    );
    return rows.length > 0;
}

// ── A supplier's pending "mark as sent" ──────────────────────────────────
// Staff changed the line's status, product, lot or quantity: a request made
// against the line as it was is no longer about this line. Same transaction as
// the change. A portal schema that is not on this server has no requests.
async function supersedePendingMarkSent(conn, orderId, userEmail) {
    try {
        const [done] = await conn.query(
            `UPDATE ${jportalSchema()}.po_change_requests
                SET status = 'superseded', decided_by = ?, decided_at = UTC_TIMESTAMP(),
                    decided_note = 'The line was changed in ShipLine after this was sent.',
                    row_version = row_version + 1
              WHERE pending_order_id = ? AND status = 'pending'`,
            [userEmail || 'unknown', Number(orderId)]
        );
        return done.affectedRows;
    } catch (err) {
        if (err.errno === 1146 || err.errno === 1049 || err.errno === 1142) {
            log.warn('[sample-orders] no portal change requests to supersede here', { orderId, error: err.message });
            return 0;
        }
        throw err;
    }
}

// ── The guard ────────────────────────────────────────────────────────────
const isAdmin = req => Boolean(req) && req.userType === 'admin';
const reasonOf = req => {
    const r = req && req.body ? req.body.reason : null;
    return typeof r === 'string' && r.trim() ? r.trim().slice(0, 500) : null;
};

const key = v => String(v ?? '').trim().toUpperCase();

// What a route sends back for a refusal.
function toBody(refusal) {
    const { ok, http, message, ...rest } = refusal;
    return { error: message, ...rest };
}

const disabled = () => ({
    ok: false, http: 409, code: 'SAMPLES_DISABLED',
    message: 'Sample orders are not switched on here.',
});

/**
 * May this write go ahead?
 *   beforeRow  the stored orders row (snake_case), or null when creating
 *   afterRow   the row as it would stand after the write (snake_case, complete)
 *   via        'create' | 'update' | 'status' | 'sample-received' | 'change-request'
 *   req        the request (who is asking, and their reason)
 *   lockLot    take the shared lock on the JFPRO lot (the caller is in a transaction)
 *
 * Returns { refusal } or { sample, set, statusChanged, supersedePending,
 * billChanged, clearReceivedStamp }. For a sample, `set` holds columns to write
 * on top of the caller's own, order_kind included on a create.
 */
async function guardWrite(conn, { beforeRow = null, afterRow, via, req = null, lockLot = false }) {
    const none = { sample: false, local: false, set: {}, statusChanged: false, supersedePending: false, billChanged: false, clearReceivedStamp: false };
    if (!await schemaReady(conn)) return none;

    const creating = !beforeRow;
    const po = afterRow.purchase_order_id != null ? await loadPo(conn, afterRow.purchase_order_id) : null;
    const sample = creating ? Boolean(po && po.kind === S.KIND_SAMPLE) : S.isSample(beforeRow);

    if (!sample) {
        // A local line (docs/local-orders.md) is decided like a sample: from
        // the PO on a create, from the stored row after that. It is stock
        // goods, so the stock rules below apply to it as well.
        const local = creating ? Boolean(po && po.kind === L.KIND_LOCAL) : L.isLocal(beforeRow);
        // A stock line stays out of a sample PO, and out of a sample's code.
        if (po && po.kind === S.KIND_SAMPLE) {
            return { refusal: { ok: false, http: 409, code: 'SAMPLE_PO_REQUIRED', message: 'A stock line cannot be put on a sample purchase order.' } };
        }
        if (!local && po && po.kind === L.KIND_LOCAL) {
            return { refusal: { ok: false, http: 409, code: 'LOCAL_PO_REQUIRED', message: 'A stock line cannot be put on a local purchase order.' } };
        }
        let localSet = null;
        if (local) {
            const verdict = L.validateLocalWrite(beforeRow, afterRow, { po: po ? { id: po.id, kind: po.kind } : null });
            if (!verdict.ok) return { refusal: verdict };
            localSet = verdict.set;
        }
        const lotChanged = !lotText(afterRow.lot_number)
            ? false
            : creating || key(beforeRow.lot_number) !== key(afterRow.lot_number);
        if (lotChanged) {
            let lotSample = false;
            try {
                lotSample = await lotIsSample(conn, afterRow.lot_number);
            } catch (err) {
                // The register cannot be read: a stock write is not held up by it.
                log.warn('[sample-orders] sample lot lookup failed on a stock write', { error: err.message });
            }
            const refusal = S.stockLotRefusal({ lotIsSample: lotSample, lotChanged });
            if (refusal) return { refusal };
        }
        return local ? { ...none, local: true, set: localSet } : none;
    }

    if (creating && !await armed(conn, { fresh: true })) return { refusal: disabled() };

    const row = { ...afterRow, order_kind: S.KIND_SAMPLE };
    const before = beforeRow ? { ...beforeRow } : null;

    const identityChanged = creating
        || key(before.jf_code) !== key(row.jf_code)
        || lotText(before.lot_number).toUpperCase() !== lotText(row.lot_number).toUpperCase();
    let lotChecked = null;
    if (identityChanged && lotText(row.lot_number) && lotText(row.jf_code)) {
        try {
            lotChecked = await lotIsSampleOf(conn, row.lot_number, row.jf_code, { lock: lockLot });
        } catch (err) {
            log.error('[sample-orders] sample lot lookup failed', { error: err.message });
            lotChecked = null;
        }
    }

    const committed = po ? await poCommitted(conn, po.id) : false;
    const result = S.validateSampleWrite(before, row, {
        via, isAdmin: isAdmin(req), reason: reasonOf(req),
        po: po ? { id: po.id, kind: po.kind, committed } : null,
        lotChecked,
    });
    if (!result.ok) return { refusal: result };

    const set = { ...result.set };
    if (creating) set.order_kind = S.KIND_SAMPLE;
    return { sample: true, ...result, set };
}

// Runs fn inside a transaction when `on` (a sample write: the lot lock, the
// row and a superseded request commit together), otherwise plainly, exactly as
// the stock paths always ran. fn's return is passed through; a throw rolls back.
async function withSampleTx(conn, on, fn) {
    if (!on) return fn();
    await conn.beginTransaction();
    try {
        const out = await fn();
        // A refusal wrote nothing, but say so by rolling back.
        if (out && out.refusal) await conn.rollback(); else await conn.commit();
        return out;
    } catch (err) {
        try { await conn.rollback(); } catch (rollbackError) {
            log.warn('[sample-orders] rollback failed', rollbackError.message);
        }
        throw err;
    }
}

// The unique key on orders.sample_lot_key refused the write: another sample
// line already carries this code.
function lotClash(err) {
    if (!err || err.errno !== 1062 || !/uq_orders_sample_lot_key/.test(err.message || '')) return null;
    return {
        ok: false, http: 409, code: 'SAMPLE_LOT_IN_USE',
        message: 'Another sample order already carries this sample code. A re-order is a new version of the sample, with its own code.',
    };
}

// The SET fragments and values a sample write adds to the caller's UPDATE:
// what the validator returned, and the moment the status changed.
function sampleSetSql(guard) {
    const sets = [];
    const values = [];
    for (const [col, value] of Object.entries(guard.set || {})) {
        if (!/^[a-z_]+$/.test(col)) throw new Error(`sampleSetSql: bad column ${col}`);
        sets.push(`${col} = ?`);
        values.push(value);
    }
    if (guard.statusChanged) sets.push('sample_status_changed_at = UTC_TIMESTAMP()');
    return { sets, values };
}

// "Not for a sample": the routes that pack, split, book, receive into Mintsoft
// or record a shortfall.
function refuseSample(row, what) {
    if (!S.isSample(row)) return null;
    return {
        ok: false, http: 409, code: 'SAMPLE_NOT_STOCK',
        message: `A sample order cannot be ${what}.`,
        allowedNext: S.allowedNext(row.status, {}),
    };
}

module.exports = {
    schemaReady,
    stockOnlySql,
    notSampleSql,
    localReady,
    refuseLocal: L.refuseLocal,
    armed,
    stage,
    resetCaches,
    loadPo,
    poMoneyRecorded,
    poEmailed,
    poCommitted,
    lotIsSampleOf,
    lotIsSample,
    supersedePendingMarkSent,
    guardWrite,
    withSampleTx,
    lotClash,
    sampleSetSql,
    refuseSample,
    toBody,
    isAdmin,
    reasonOf,
};
