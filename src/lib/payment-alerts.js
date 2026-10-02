'use strict';

// Dismissed "Needs attention" lines — pure rules behind
// /api/v1/payment-alert-dismissals (routes: src/services/payment-alert-routes.js).
//
// ShipLine's Payments flow page works its alerts out on the client, so an
// alert has no row. A dismissal is pinned to the KEY the page builds from what
// the alert says; the server keeps who dismissed it, when, and what it said
// then. Whether a dismissal still hides anything is the page's call: an alert
// whose wording or figure has changed has a new key and shows again.
//
// Rule (user, 2026-10-02): admins dismiss and restore; everyone sees the result.

const KEY_MAX_LEN = 255;
const DISMISSER_ROLES = ['admin'];

const canDismiss = userType => DISMISSER_ROLES.includes(String(userType));

const text = (v, max) => (typeof v === 'string' && v.trim() ? v.trim().slice(0, max) : null);

// { value } or { error } — a dismissal as the page sends it.
function parseDismissBody(body) {
    const b = body || {};
    if (typeof b.alertKey !== 'string' || !b.alertKey.trim()) return { error: 'alertKey is required.' };
    const alertKey = b.alertKey.trim();
    if (alertKey.length > KEY_MAX_LEN) return { error: `alertKey cannot exceed ${KEY_MAX_LEN} characters.` };
    const kind = text(b.kind, 40);
    if (!kind) return { error: 'kind is required — the kind of alert being dismissed.' };
    let currency = null;
    if (b.currency != null && b.currency !== '') {
        currency = typeof b.currency === 'string' ? b.currency.trim().toUpperCase() : '';
        if (!/^[A-Z]{3}$/.test(currency)) return { error: 'currency must be a three-letter code.' };
    }
    let amount = null;
    if (b.amount != null && b.amount !== '') {
        amount = Math.round(Number(b.amount) * 100) / 100;
        if (!Number.isFinite(amount)) return { error: 'amount must be a number.' };
    }
    return {
        value: {
            alertKey, kind, currency, amount,
            poNumber: text(b.poNumber, 100),
            shipmentReference: text(b.shipmentReference, 100),
            supplierName: text(b.supplierName, 255),
            detail: text(b.detail, 1000),
            note: text(b.note, 500),
        },
    };
}

const iso = v => (v == null ? null : v.toISOString ? v.toISOString() : String(v));

function dismissalRowToJson(r) {
    return {
        id: r.id,
        alertKey: r.alert_key,
        kind: r.kind,
        currency: r.currency || null,
        poNumber: r.po_number || null,
        shipmentReference: r.shipment_reference || null,
        supplierName: r.supplier_name || null,
        detail: r.detail || null,
        amount: r.amount == null ? null : Number(r.amount),
        note: r.note || null,
        dismissedByEmail: r.dismissed_by_email,
        dismissedByName: r.dismisser_name || null,
        dismissedAt: iso(r.dismissed_at),
        restoredAt: iso(r.restored_at),
        restoredByEmail: r.restored_by_email || null,
    };
}

module.exports = { KEY_MAX_LEN, DISMISSER_ROLES, canDismiss, parseDismissBody, dismissalRowToJson };
