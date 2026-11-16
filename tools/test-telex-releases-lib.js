'use strict';

// Unit tests for the telex release rules (src/lib/telex-releases.js;
// docs/telex-releases.md): which containers of a transfer are owed a telex
// release, the state of each, what a file must be to be filed, and who may
// file, accept, send back or delete one. Pure: no database, no network.
//
//   npm run test:unit

const { test } = require('node:test');
const assert = require('node:assert/strict');
const T = require('../src/lib/telex-releases');

const SUPPLIER = 'Acme Medical Co., Ltd';
const PDF = Buffer.from('%PDF-1.7\n...');
const PNG = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0, 0]);
const JPG = Buffer.from([0xff, 0xd8, 0xff, 0xe0, 0, 0]);
const EXE = Buffer.from('MZ\x90\x00 this is a program');

const line = (kind, over = {}) => ({ kind, amount: 100, shipmentId: null, shipmentReference: null, targetMissing: false, ...over });
const transfer = (lines, over = {}) => ({ id: 45, supplierName: SUPPLIER, lines, ...over });
const release = (over = {}) => ({ id: 9, shipmentId: 61, supplierName: SUPPLIER, filename: 'telex.pdf', url: 'https://f/telex.pdf', createdAt: '2026-11-16T10:00:00Z', ...over });
const submission = (over = {}) => ({ id: 3, shipmentId: 61, supplierName: SUPPLIER, status: 'open', sent: true, note: null, createdAt: '2026-11-16T09:00:00Z', ...over });

test('roles: admin and standard users file, accept and send back; nobody else', () => {
    assert.deepEqual(T.FILE_ROLES, ['admin', 'standard']);
    assert.equal(T.mayFile('admin'), true);
    assert.equal(T.mayFile('standard'), true);
    assert.equal(T.mayFile('accountant'), false);
    assert.equal(T.mayFile('viewer'), false);
    assert.equal(T.mayFile(undefined), false);
});

test('slotsFor: one per container a balance line paid, each once, in the order met', () => {
    assert.deepEqual(T.slotsFor(transfer([
        line('balance', { shipmentId: 61, shipmentReference: '333' }),
        line('qc'),
        line('balance', { shipmentId: 62, shipmentReference: '126. Air Freight' }),
        line('balance', { shipmentId: 61, shipmentReference: '333' }),
    ])), [{ shipmentId: 61, reference: '333' }, { shipmentId: 62, reference: '126. Air Freight' }]);
});

test('slotsFor: nothing but a live balance on a shipment asks for a telex release', () => {
    assert.deepEqual(T.slotsFor(transfer([line('po_deposit'), line('pi'), line('qc'), line('repackaged'), line('extra', { shipmentId: 61, shipmentReference: '333' })])), []);
    assert.deepEqual(T.slotsFor(transfer([line('balance', { shipmentId: null, shipmentReference: '333' })])), [], 'a balance with no shipment');
    assert.deepEqual(T.slotsFor(transfer([line('balance', { shipmentId: 61, shipmentReference: '333', targetMissing: true })])), [], 'a balance deleted since');
    assert.deepEqual(T.slotsFor(transfer([])), []);
    assert.deepEqual(T.slotsFor({ id: 1, supplierName: SUPPLIER }), []);
    assert.deepEqual(T.slotsFor(transfer([line('balance', { shipmentId: 61, shipmentReference: null })])), [{ shipmentId: 61, reference: null }]);
});

test('stateOf: received outranks everything, then the supplier\'s latest upload', () => {
    assert.equal(T.stateOf([], []), 'awaiting');
    assert.equal(T.stateOf([release()], []), 'received');
    assert.equal(T.stateOf([release()], [submission({ status: 'rejected' })]), 'received');
    assert.equal(T.stateOf([], [submission()]), 'uploaded');
    assert.equal(T.stateOf([], [submission({ status: 'rejected', note: 'wrong container' })]), 'rejected');
    assert.equal(T.stateOf([], [submission({ sent: false })]), 'awaiting', 'a file still on its way is not an upload yet');
    assert.equal(T.stateOf([], [submission({ status: 'void' })]), 'awaiting');
    assert.equal(T.stateOf([], [submission({ status: 'received' })]), 'awaiting', 'received is what a filed release says, not a marker without one');
});

test('stateOf: the latest upload decides between sent back and sent again', () => {
    const older = submission({ id: 3, status: 'rejected', createdAt: '2026-11-15T09:00:00Z' });
    const newer = submission({ id: 4, status: 'open', createdAt: '2026-11-16T09:00:00Z' });
    assert.equal(T.stateOf([], [older, newer]), 'uploaded');
    assert.equal(T.stateOf([], [newer, older]), 'uploaded', 'whatever order they are read in');
    assert.equal(T.stateOf([], [{ ...older, status: 'open' }, { ...newer, status: 'rejected' }]), 'rejected');
    // Same second: the higher id is the later one.
    assert.equal(T.stateOf([], [submission({ id: 5, status: 'rejected' }), submission({ id: 6, status: 'open' })]), 'uploaded');
});

test('summariesFor: each container\'s state for this transfer\'s supplier, by exact name', () => {
    const pm = transfer([line('balance', { shipmentId: 61, shipmentReference: '333' }), line('balance', { shipmentId: 62, shipmentReference: '334' }), line('balance', { shipmentId: 63, shipmentReference: '335' })]);
    const releases = [release({ id: 9, shipmentId: 61 }), release({ id: 10, shipmentId: 62, supplierName: 'Acme Medical Co., Ltd.' }), release({ id: 11, shipmentId: 99 })];
    const submissions = [submission({ id: 3, shipmentId: 63 }), submission({ id: 4, shipmentId: 62, supplierName: 'acme medical co., ltd' })];
    assert.deepEqual(T.summariesFor(pm, releases, submissions), [
        { shipmentId: 61, reference: '333', state: 'received', releases: [{ id: 9, filename: 'telex.pdf', url: 'https://f/telex.pdf', createdAt: '2026-11-16T10:00:00Z' }], submissionId: null, note: null },
        { shipmentId: 62, reference: '334', state: 'awaiting', releases: [], submissionId: null, note: null },
        { shipmentId: 63, reference: '335', state: 'uploaded', releases: [], submissionId: 3, note: null },
    ]);
});

test('summariesFor: a release sent back says why; nothing owed is an empty list', () => {
    const pm = transfer([line('balance', { shipmentId: 61, shipmentReference: '333' })]);
    assert.deepEqual(T.summariesFor(pm, [], [submission({ id: 7, status: 'rejected', note: 'This is the draft, not the release' })]), [
        { shipmentId: 61, reference: '333', state: 'rejected', releases: [], submissionId: 7, note: 'This is the draft, not the release' },
    ]);
    assert.deepEqual(T.summariesFor(transfer([line('po_deposit')]), [release()], [submission()]), []);
    assert.deepEqual(T.summariesFor(pm, [], [submission({ id: 8, status: 'open', note: 'left over from an earlier answer' })]), [
        { shipmentId: 61, reference: '333', state: 'uploaded', releases: [], submissionId: 8, note: null },
    ], 'a note is only said for an upload we sent back');
    assert.deepEqual(T.summariesFor(pm, undefined, undefined), [{ shipmentId: 61, reference: '333', state: 'awaiting', releases: [], submissionId: null, note: null }]);
});

test('uploadRefusal: what a file must be to be filed', () => {
    const ok = { filename: 'telex.pdf', bytes: PDF, supplierName: SUPPLIER, shipment: { id: 61 } };
    assert.equal(T.uploadRefusal(ok), null);
    assert.equal(T.uploadRefusal({ ...ok, bytes: PNG }), null);
    assert.equal(T.uploadRefusal({ ...ok, bytes: JPG }), null);
    assert.deepEqual(T.uploadRefusal({ ...ok, shipment: null }), { status: 404, code: 'NO_SUCH_SHIPMENT', error: 'That shipment does not exist.' });
    assert.deepEqual(T.uploadRefusal({ ...ok, supplierName: '  ' }), { status: 400, code: 'BAD_FIELD', error: 'supplierName is required: a telex release is one supplier\'s, for one shipment.' });
    assert.deepEqual(T.uploadRefusal({ ...ok, filename: '' }), { status: 400, code: 'BAD_FIELD', error: 'filename is required.' });
    assert.deepEqual(T.uploadRefusal({ ...ok, bytes: Buffer.alloc(0) }), { status: 400, code: 'BAD_FIELD', error: 'The file is empty.' });
    assert.deepEqual(T.uploadRefusal({ ...ok, bytes: Buffer.alloc(T.MAX_BYTES + 1, 0x25) }), { status: 413, code: 'TOO_LARGE', error: 'A telex release can be at most 15 MB.' });
    assert.deepEqual(T.uploadRefusal({ ...ok, bytes: EXE }), { status: 422, code: 'BAD_TYPE', error: 'A telex release must be a PDF, a PNG or a JPEG.' });
    assert.equal(T.MAX_BYTES, 15 * 1024 * 1024);
});

test('typeOf and s3KeyFor: the type is read from the bytes, the key is the bucket\'s own layout', () => {
    assert.equal(T.typeOf(PDF), 'application/pdf');
    assert.equal(T.typeOf(PNG), 'image/png');
    assert.equal(T.typeOf(JPG), 'image/jpeg');
    assert.equal(T.typeOf(EXE), null);
    assert.equal(T.s3KeyFor('0b0b0b0b-1111-2222-3333-444444444444', 'Telex release 333.PDF', 'application/pdf'), 'telex-releases/0b0b0b0b-1111-2222-3333-444444444444/Telex_release_333.pdf');
    assert.equal(T.s3KeyFor('u', '电放提单.pdf', 'application/pdf'), 'telex-releases/u/telex-release.pdf', 'a name with nothing usable in it');
    assert.equal(T.s3KeyFor('u', 'scan.jpeg', 'image/jpeg'), 'telex-releases/u/scan.jpg');
    assert.equal(T.s3KeyFor('u', '../../etc/passwd', 'image/png'), 'telex-releases/u/etc_passwd.png');
});

test('deleteRefusal: an admin, or whoever filed it; never one already deleted', () => {
    const row = { id: 9, uploadedByEmail: 'ops@us.com', deletedAt: null };
    assert.equal(T.deleteRefusal(row, { userType: 'admin', email: 'boss@us.com' }), null);
    assert.equal(T.deleteRefusal(row, { userType: 'standard', email: 'OPS@us.com ' }), null, 'the uploader, whatever the case');
    assert.deepEqual(T.deleteRefusal(row, { userType: 'standard', email: 'other@us.com' }), { status: 403, code: 'NOT_YOURS', error: 'Only an admin, or whoever filed this telex release, can delete it.' });
    assert.deepEqual(T.deleteRefusal(row, { userType: 'accountant', email: 'ops@us.com' }), { status: 403, code: 'CANNOT_FILE', error: 'Your role cannot file or delete a telex release: admin and standard users do.' });
    assert.deepEqual(T.deleteRefusal(null, { userType: 'admin', email: 'boss@us.com' }), { status: 404, code: 'NOT_FOUND', error: 'Telex release not found.' });
    assert.deepEqual(T.deleteRefusal({ ...row, deletedAt: '2026-11-16T00:00:00Z' }, { userType: 'admin', email: 'boss@us.com' }), { status: 404, code: 'NOT_FOUND', error: 'Telex release not found.' });
    assert.deepEqual(T.deleteRefusal({ ...row, uploadedByEmail: null }, { userType: 'standard', email: '' }), { status: 403, code: 'NOT_YOURS', error: 'Only an admin, or whoever filed this telex release, can delete it.' });
});

test('decisionRefusal: a supplier\'s upload is accepted or sent back once, while it is open and its file has arrived', () => {
    const open = { id: 3, status: 'open', sent: true, shipmentLive: true };
    assert.equal(T.decisionRefusal(open, { userType: 'admin', action: 'accept' }), null);
    assert.equal(T.decisionRefusal(open, { userType: 'standard', action: 'reject', note: 'Wrong container' }), null);
    assert.deepEqual(T.decisionRefusal(open, { userType: 'accountant', action: 'accept' }), { status: 403, code: 'CANNOT_FILE', error: 'Your role cannot file or delete a telex release: admin and standard users do.' });
    assert.deepEqual(T.decisionRefusal(null, { userType: 'admin', action: 'accept' }), { status: 404, code: 'NOT_FOUND', error: 'Telex upload not found.' });
    assert.deepEqual(T.decisionRefusal({ ...open, status: 'received' }, { userType: 'admin', action: 'accept' }),
        { status: 409, code: 'ALREADY_DECIDED', error: 'This upload was already received.', allowedNext: ['reload'] });
    assert.deepEqual(T.decisionRefusal({ ...open, status: 'rejected' }, { userType: 'admin', action: 'reject', note: 'again' }),
        { status: 409, code: 'ALREADY_DECIDED', error: 'This upload was already sent back.', allowedNext: ['reload'] });
    assert.deepEqual(T.decisionRefusal({ ...open, status: 'void' }, { userType: 'admin', action: 'accept' }),
        { status: 409, code: 'ALREADY_DECIDED', error: 'This upload is no longer open.', allowedNext: ['reload'] });
    assert.deepEqual(T.decisionRefusal({ ...open, sent: false }, { userType: 'admin', action: 'accept' }),
        { status: 409, code: 'FILE_NOT_SENT', error: 'The supplier has not finished sending this file, or has withdrawn it.', allowedNext: ['reload'] });
    assert.deepEqual(T.decisionRefusal({ ...open, shipmentLive: false }, { userType: 'admin', action: 'accept' }),
        { status: 409, code: 'SHIPMENT_GONE', error: 'The shipment this telex release is for no longer exists. Send it back, saying so.', allowedNext: ['reject'] });
    assert.equal(T.decisionRefusal({ ...open, shipmentLive: false }, { userType: 'admin', action: 'reject', note: 'No such shipment' }), null, 'sending back needs no shipment');
});

test('decisionRefusal: sending back says why, in words the supplier reads', () => {
    const open = { id: 3, status: 'open', sent: true, shipmentLive: true };
    const need = { status: 400, code: 'NOTE_REQUIRED', error: 'Say what is wrong with it, in at least 3 characters: the supplier reads this.' };
    assert.deepEqual(T.decisionRefusal(open, { userType: 'admin', action: 'reject' }), need);
    assert.deepEqual(T.decisionRefusal(open, { userType: 'admin', action: 'reject', note: '  ok ' }), need);
    assert.equal(T.decisionRefusal(open, { userType: 'admin', action: 'reject', note: ' no ' }) === null, false);
    assert.equal(T.decisionRefusal(open, { userType: 'admin', action: 'reject', note: 'bad' }), null);
    assert.equal(T.cleanNote('  Wrong   container \n number '), 'Wrong container number');
    assert.equal(T.cleanNote('x'.repeat(600)).length, 500);
    assert.equal(T.cleanNote(null), '');
});

test('sameName: exact, as the portal compares names', () => {
    assert.equal(T.sameName('Acme Co', 'Acme Co'), true);
    assert.equal(T.sameName('Acme Co', 'acme co'), false);
    assert.equal(T.sameName('Acme Co', 'Acme Co '), false);
    assert.equal(T.sameName('', ''), false);
    assert.equal(T.sameName(null, null), false);
});
