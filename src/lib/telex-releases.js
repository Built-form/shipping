'use strict';

// Telex releases (docs/telex-releases.md): the telex-released bill of lading a
// supplier sends once a container's balance is paid. One per supplier per
// shipment, as a packing list or an invoice is. Filed by our team from the
// payment, or uploaded by the supplier on the portal's Payments page and
// accepted here.
//
// They live in their OWN table (shipment_telex_releases), not as another
// doc_kind of shipment_payment_documents: some thirty readers of that table
// treat "not a proof of payment" as "an invoice" (it would be read by the
// invoice check, offered to pay, shown as a balance's invoice file), and one
// of them rewrites a row to a proof of payment when a document reads like one.
//
// These are the rules, pure: which containers of a transfer are owed one, the
// state of each, what a file must be, and who may file, accept, send back or
// delete. The SQL and the routes are in src/services/telex-release-routes.js.

const PI = require('./pi-submissions');

const MAX_BYTES = 15 * 1024 * 1024;
// Filing, accepting, sending back and deleting. An accountant's writes are
// payments; a telex release is the shipment's.
const FILE_ROLES = ['admin', 'standard'];
const NOTE_MAX = 500;
const NOTE_MIN = 3;

const mayFile = userType => FILE_ROLES.includes(userType);

// Exact, as the portal compares supplier names (utf8mb4_0900_bin): case,
// punctuation and trailing spaces all count.
const sameName = (a, b) => typeof a === 'string' && typeof b === 'string' && a !== '' && a === b;

const refusal = (status, code, error, extra = {}) => ({ status, code, error, ...extra });
const CANNOT_FILE = () => refusal(403, 'CANNOT_FILE', 'Your role cannot file or delete a telex release: admin and standard users do.');

/**
 * The containers of a transfer that are owed a telex release: one per
 * shipment a live `balance` line paid (a top-up is a balance too), each once,
 * in the order met. `transfer` is a supplier payment as the API hands it out
 * (lines with kind, shipmentId, shipmentReference, targetMissing).
 * A PI paid through containers names them by reference only, so it asks for
 * none here: our team file that one from the shipment.
 */
function slotsFor(transfer) {
    const out = [];
    for (const l of (transfer && transfer.lines) || []) {
        if (l.kind !== 'balance' || l.shipmentId == null || l.targetMissing) continue;
        if (!out.some(s => s.shipmentId === l.shipmentId)) out.push({ shipmentId: l.shipmentId, reference: l.shipmentReference ?? null });
    }
    return out;
}

const later = (a, b) => String(b.createdAt).localeCompare(String(a.createdAt)) || (b.id - a.id);

/**
 * The state of one supplier's telex release for one shipment, from its filed
 * releases and the supplier's portal uploads for it:
 *   received  one is filed;
 *   uploaded  the supplier's latest upload is waiting for us;
 *   rejected  we sent their latest upload back;
 *   awaiting  nothing yet (an upload whose file has not arrived is nothing).
 */
function stateOf(releases, submissions) {
    if ((releases || []).length) return 'received';
    const mine = (submissions || []).filter(s => s.sent && (s.status === 'open' || s.status === 'rejected')).sort(later);
    if (!mine.length) return 'awaiting';
    return mine[0].status === 'open' ? 'uploaded' : 'rejected';
}

/**
 * A transfer's containers with the state of each, for the transfer's own
 * supplier (by exact name): what the Payments page shows on a balance.
 * `releases`: live filed rows; `submissions`: the portal's rows.
 */
function summariesFor(transfer, releases, submissions) {
    return slotsFor(transfer).map((slot) => {
        const mine = r => r.shipmentId === slot.shipmentId && sameName(r.supplierName, transfer.supplierName);
        const filed = (releases || []).filter(mine);
        const uploads = (submissions || []).filter(mine);
        const state = stateOf(filed, uploads);
        const latest = uploads.filter(s => s.sent && (s.status === 'open' || s.status === 'rejected')).sort(later)[0] || null;
        return {
            shipmentId: slot.shipmentId,
            reference: slot.reference,
            state,
            releases: filed.map(r => ({ id: r.id, filename: r.filename, url: r.url, createdAt: r.createdAt })),
            submissionId: state === 'uploaded' || state === 'rejected' ? latest.id : null,
            note: state === 'rejected' ? (latest.note ?? null) : null,
        };
    });
}

// ── A file ──────────────────────────────────────────────────────────────────

/** What the bytes are: a PDF, a PNG or a JPEG, else null. Never the name's word for it. */
const typeOf = bytes => PI.sniffType(bytes);

/** null when the file can be filed for that shipment and supplier, else the refusal. */
function uploadRefusal({ filename, bytes, supplierName, shipment }) {
    if (!shipment) return refusal(404, 'NO_SUCH_SHIPMENT', 'That shipment does not exist.');
    if (typeof supplierName !== 'string' || !supplierName.trim()) {
        return refusal(400, 'BAD_FIELD', 'supplierName is required: a telex release is one supplier\'s, for one shipment.');
    }
    if (typeof filename !== 'string' || !filename.trim()) return refusal(400, 'BAD_FIELD', 'filename is required.');
    if (!bytes || !bytes.length) return refusal(400, 'BAD_FIELD', 'The file is empty.');
    if (bytes.length > MAX_BYTES) return refusal(413, 'TOO_LARGE', 'A telex release can be at most 15 MB.');
    if (!typeOf(bytes)) return refusal(422, 'BAD_TYPE', 'A telex release must be a PDF, a PNG or a JPEG.');
    return null;
}

/** Where a filed release goes in the PO bucket. The name is made safe; one with nothing usable left gets a plain one. */
function s3KeyFor(uuid, filename, contentType) {
    const safe = PI.safeFilename(filename, contentType) || PI.safeFilename('telex-release', contentType);
    return `telex-releases/${uuid}/${safe}`;
}

/** null when this user may delete the filed release, else the refusal. */
function deleteRefusal(row, { userType, email }) {
    if (!mayFile(userType)) return CANNOT_FILE();
    if (!row || row.deletedAt) return refusal(404, 'NOT_FOUND', 'Telex release not found.');
    const me = String(email || '').trim().toLowerCase();
    const theirs = String(row.uploadedByEmail || '').trim().toLowerCase();
    if (userType === 'admin' || (me && me === theirs)) return null;
    return refusal(403, 'NOT_YOURS', 'Only an admin, or whoever filed this telex release, can delete it.');
}

/** A note as it is stored and shown to the supplier: one line, at most 500 characters. */
const cleanNote = note => String(note == null ? '' : note).replace(/\s+/g, ' ').trim().slice(0, NOTE_MAX);

const DECIDED_WORDS = { received: 'was already received', rejected: 'was already sent back' };

/**
 * null when a supplier's upload may be accepted or sent back, else the
 * refusal. `submission`: { status, sent, shipmentLive } (sent: its file has
 * arrived and was not withdrawn). Sending back needs a note the supplier can
 * act on; accepting needs the shipment still there to file it under.
 */
function decisionRefusal(submission, { userType, action, note }) {
    if (!mayFile(userType)) return CANNOT_FILE();
    if (!submission) return refusal(404, 'NOT_FOUND', 'Telex upload not found.');
    if (submission.status !== 'open') {
        return refusal(409, 'ALREADY_DECIDED', `This upload ${DECIDED_WORDS[submission.status] || 'is no longer open'}.`, { allowedNext: ['reload'] });
    }
    if (!submission.sent) {
        return refusal(409, 'FILE_NOT_SENT', 'The supplier has not finished sending this file, or has withdrawn it.', { allowedNext: ['reload'] });
    }
    if (action === 'reject') {
        return cleanNote(note).length >= NOTE_MIN ? null
            : refusal(400, 'NOTE_REQUIRED', 'Say what is wrong with it, in at least 3 characters: the supplier reads this.');
    }
    if (!submission.shipmentLive) {
        return refusal(409, 'SHIPMENT_GONE', 'The shipment this telex release is for no longer exists. Send it back, saying so.', { allowedNext: ['reject'] });
    }
    return null;
}

module.exports = {
    MAX_BYTES, FILE_ROLES, NOTE_MAX,
    mayFile, sameName, slotsFor, stateOf, summariesFor,
    typeOf, uploadRefusal, s3KeyFor, deleteRefusal, cleanNote, decisionRefusal,
};
