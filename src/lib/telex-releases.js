'use strict';

// Telex releases (docs/telex-releases.md): the telex-released bill of lading a
// supplier sends once a container's balance is paid. One per supplier per
// shipment, as a packing list or an invoice is. Filed by our team, or uploaded
// by the supplier on the portal's Payments page and accepted here.
//
// They are shown where a shipment's other papers are: a TELEX tab on the
// shipment card and the container card, one block a supplier, as its packing
// lists are (the user, 2026-11-16: there, not on the Payments page).
//
// They live in their OWN table (shipment_telex_releases), not as another
// doc_kind of shipment_payment_documents: some thirty readers of that table
// treat "not a proof of payment" as "an invoice" (it would be read by the
// invoice check, offered to pay, shown as a balance's invoice file), and one
// of them rewrites a row to a proof of payment when a document reads like one.
//
// These are the rules, pure: a shipment's suppliers with where each one's
// release stands, what a file must be, and who may file, accept, send back or
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
 * One shipment's suppliers, each with where its telex release stands: what the
 * shipment card's TELEX tab lists, a block a supplier.
 *   onBoard      the suppliers of the shipment's order lines;
 *   balances     the suppliers its balance records are filed under (the name
 *                the portal knows a paid balance by);
 *   releases     the shipment's live filed rows; submissions: the portal's.
 * A supplier is whoever any of the four names, each once, by EXACT name: two
 * spellings are two blocks, which is how a release filed under the wrong one
 * gets seen. Those on the shipment or with a balance come first, by name.
 * `upload`: the supplier's upload still waiting for us (shown even when a
 * release is already filed), else the one we last sent back.
 */
function suppliersOf({ onBoard = [], balances = [], releases = [], submissions = [] } = {}) {
    const names = [];
    const add = (name) => { if (typeof name === 'string' && name !== '' && !names.includes(name)) names.push(name); };
    onBoard.forEach(add);
    balances.forEach(add);
    releases.forEach(r => add(r.supplierName));
    submissions.forEach(s => add(s.supplierName));
    const rows = names.map((name) => {
        const filed = releases.filter(r => sameName(r.supplierName, name));
        const reached = submissions.filter(s => sameName(s.supplierName, name) && s.sent && (s.status === 'open' || s.status === 'rejected')).sort(later);
        const state = stateOf(filed, reached);
        const waiting = reached.find(s => s.status === 'open') || null;
        return {
            supplierName: name,
            onBoard: onBoard.includes(name),
            balance: balances.includes(name),
            state,
            releases: filed,
            upload: waiting || (state === 'rejected' ? reached[0] : null),
        };
    });
    const here = r => (r.onBoard || r.balance ? 0 : 1);
    return rows.sort((a, b) => here(a) - here(b) || a.supplierName.localeCompare(b.supplierName, 'en'));
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
    mayFile, sameName, stateOf, suppliersOf,
    typeOf, uploadRefusal, s3KeyFor, deleteRefusal, cleanNote, decisionRefusal,
};
