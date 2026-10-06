'use strict';

// Custom due dates — pure rules behind /api/v1/payment-due-dates (routes:
// src/services/payment-due-date-routes.js).
//
// ShipLine's Payments flow page works each payment's due date out from the
// supplier's terms, the company rules and the goods' dates. A person can set
// a date by hand instead (user, 2026-10-06), and clear it again to go back to
// the derived one. Like a sign-off, the date hangs on a key the page builds:
//   deposit:<purchase order id>                     the PO's deposit payment
//   balance:<currency>:<container>|<supplier key>   a supplier's balance in one container
//   item:<row id>                                   ONE row of a payment (one PO's share),
//                                                   which beats the payment-level date
// The server keeps who set it, when, and an optional note; the page decides
// what the date applies to. Admin and standard users set dates (the people who
// sign payments off); accountants read them.

const R = require('./payment-reviews');

const SETTER_ROLES = ['admin', 'standard'];
const KEY_MAX_LEN = 255;
const NOTE_MAX_LEN = 500;
const DATE_RE = /^(\d{4})-(\d{2})-(\d{2})$/;

const canSet = userType => SETTER_ROLES.includes(String(userType));

// A real calendar date, YYYY-MM-DD.
function isYmd(v) {
    const m = typeof v === 'string' ? DATE_RE.exec(v) : null;
    if (!m) return false;
    const d = new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3])));
    return d.getUTCFullYear() === Number(m[1]) && d.getUTCMonth() === Number(m[2]) - 1 && d.getUTCDate() === Number(m[3]);
}

// { key, scope: 'payment' | 'item' } or { error }.
function parseTargetKey(raw) {
    if (typeof raw !== 'string') return { error: 'key is required.' };
    const key = raw.trim();
    if (!key) return { error: 'key is required.' };
    if (key.length > KEY_MAX_LEN) return { error: `key cannot exceed ${KEY_MAX_LEN} characters.` };
    if (key.startsWith('item:')) {
        if (!key.slice(5).trim()) return { error: 'item key must name a row: "item:<row id>".' };
        return { key, scope: 'item' };
    }
    const k = R.parsePaymentKey(key);
    if (k.error) return { error: 'key must be "deposit:<purchase order id>", "balance:<currency>:<container>|<supplier>" or "item:<row id>".' };
    if (k.kind === 'qc') return { error: 'QC units are paid when you choose: they take no due date.' };
    return { key: k.key, scope: 'payment' };
}

// { value: { key, scope, dueDate, note } } or { error }.
function parseBody(body) {
    const b = body || {};
    const k = parseTargetKey(b.key);
    if (k.error) return { error: k.error };
    if (!isYmd(b.dueDate)) return { error: 'dueDate must be a real date, YYYY-MM-DD.' };
    let note = null;
    if (b.note != null && b.note !== '') {
        if (typeof b.note !== 'string') return { error: 'note must be text.' };
        note = b.note.trim();
        if (note.length > NOTE_MAX_LEN) return { error: `note cannot exceed ${NOTE_MAX_LEN} characters.` };
        if (!note) note = null;
    }
    return { value: { key: k.key, scope: k.scope, dueDate: b.dueDate, note } };
}

// The key of a container renumbered `fromRef` -> `toRef`, or null when `key`
// does not name `fromRef`. Balance keys as sign-offs do (upper-cased inside);
// an item key ends in the container number as the orders store it, so the
// match ignores case and the new number is written as given. A row for goods
// in no container ('…:none', '…@open:<draft id>') names no container.
function rekeyTargetKey(key, fromRef, toRef) {
    const k = String(key || '');
    const from = String(fromRef || '').trim();
    const to = String(toRef || '').trim();
    if (!from || !to) return null;
    if (k.startsWith('item:')) {
        // '@' marks a split row of goods in no container: it names no container.
        if (k.includes('@')) return null;
        const at = k.lastIndexOf(':');
        if (at < 5) return null;
        const last = k.slice(at + 1);
        if (!last || last.toUpperCase() !== from.toUpperCase()) return null;
        return `${k.slice(0, at + 1)}${to}`;
    }
    return R.rekeyBalanceKey(k, from, to);
}

const iso = v => (v == null ? null : v.toISOString ? v.toISOString() : String(v));
const ymd = v => (v == null ? null : v instanceof Date ? v.toISOString().slice(0, 10) : String(v).slice(0, 10));

function rowToJson(r) {
    return {
        id: r.id,
        key: r.target_key,
        scope: r.target_key.startsWith('item:') ? 'item' : 'payment',
        dueDate: ymd(r.due_date),
        note: r.note || null,
        setByEmail: r.set_by_email,
        setByName: r.setter_name || null,
        setAt: iso(r.updated_at),
    };
}

module.exports = { SETTER_ROLES, NOTE_MAX_LEN, canSet, isYmd, parseTargetKey, parseBody, rekeyTargetKey, rowToJson };
