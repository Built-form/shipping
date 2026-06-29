'use strict';

// Shared audit-log helpers, lifted out of src/handlers/orders.js so the
// receive/lookup service (and any other handler) can write the same
// before/after audit rows. `recordAudit` diffs the snapshots, skips no-op
// updates, and never throws — a missing audit row must not fail the mutation.

const log = require('./logger');

function deepEqual(a, b) {
    if (a === b) return true;
    if (a === null || b === null || a === undefined || b === undefined) return a === b;
    if (typeof a !== 'object' || typeof b !== 'object') return false;
    if (Array.isArray(a) !== Array.isArray(b)) return false;
    const ak = Object.keys(a);
    const bk = Object.keys(b);
    if (ak.length !== bk.length) return false;
    for (const k of ak) {
        if (!deepEqual(a[k], b[k])) return false;
    }
    return true;
}

function diffSnapshots(before, after) {
    if (!before || !after) return { before, after };
    const beforeOut = {};
    const afterOut = {};
    const keys = new Set([...Object.keys(before), ...Object.keys(after)]);
    for (const k of keys) {
        if (!deepEqual(before[k], after[k])) {
            beforeOut[k] = before[k];
            afterOut[k] = after[k];
        }
    }
    return { before: beforeOut, after: afterOut };
}

async function recordAudit(conn, { entityType, entityId, action, before, after, userEmail }) {
    try {
        const diffed = diffSnapshots(before, after);
        // Skip no-op updates (e.g. PUT with identical values).
        if (action === 'update' && diffed.before && Object.keys(diffed.before).length === 0) {
            return;
        }
        await conn.query(
            `INSERT INTO audit_log (entity_type, entity_id, action, before_json, after_json, user_email)
             VALUES (?, ?, ?, ?, ?, ?)`,
            [
                entityType,
                entityId,
                action,
                diffed.before ? JSON.stringify(diffed.before) : null,
                diffed.after ? JSON.stringify(diffed.after) : null,
                userEmail || null,
            ]
        );
    } catch (err) {
        log.warn('[audit] insert failed', { entityType, entityId, action, error: err.message });
    }
}

module.exports = { deepEqual, diffSnapshots, recordAudit };
