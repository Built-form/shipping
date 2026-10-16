'use strict';

// A balance record's figure, corrected while it is being paid. Records an
// older invoice read made carry the invoice's GOODS TOTAL (a commercial
// invoice states nothing payable), not the balance: paying the balance the
// terms give left such a record pending with the deposit's worth still "owed"
// on it (Kingphar 124. Air Freight: 9,305 on the record, 6,513.50 owed).
// The page knows the terms, so it sends the balance with the line; the record
// comes down to it and its split keeps each purchase order's share.
// Pure: no database.

const EPS = 0.005;
const round2 = v => Math.round(v * 100) / 100;

/** The record's new amount and split — null when there is nothing to change
 *  (no figure, or one not below the record's: a record is never raised here),
 *  or { error, code } when it would fall below what transfers already applied. */
function correctBalance({ amount, applied = 0, balanceAmount, allocations = [] }) {
    if (balanceAmount == null) return null;
    const next = round2(Number(balanceAmount));
    const current = round2(Number(amount));
    if (!Number.isFinite(next) || !(next > 0) || next >= current - EPS) return null;
    if (next < round2(applied) - EPS) {
        return { error: `${round2(applied)} is already applied to it — the balance cannot be ${next}.`, code: 'BALANCE_BELOW_APPLIED' };
    }
    const factor = next / current;
    const scaled = allocations.map(a => ({ ...a, amount: round2(Number(a.amount) * factor) }));
    // Rounding each share can leave a cent over or under: the largest takes it.
    const target = round2(allocations.reduce((s, a) => s + Number(a.amount), 0) * factor);
    const drift = round2(target - scaled.reduce((s, a) => s + a.amount, 0));
    if (Math.abs(drift) >= EPS && scaled.length) {
        const largest = scaled.reduce((m, a) => (a.amount > m.amount ? a : m), scaled[0]);
        largest.amount = round2(largest.amount + drift);
    }
    return { amount: next, allocations: scaled };
}

module.exports = { correctBalance };
