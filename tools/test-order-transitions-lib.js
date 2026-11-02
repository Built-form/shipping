'use strict';

// Unit tests for src/lib/order-transitions.js — the backend mirror of
// ShipLine's movement rules (orderHelpers.ts canTransitionStatus).
//
//   node --test tools/test-order-transitions-lib.js

const test = require('node:test');
const assert = require('node:assert/strict');
const T = require('../src/lib/order-transitions');

// REPACKAGED (user, 2026-11-02): "Combined / Repackaged" — the goods were
// combined into, or repackaged as, another product at the factory. Any product
// (not only FQC samples), from a factory stage only: never once it is in a
// container. Hidden, like DESTROYED.
test('REPACKAGED: reachable from every factory stage after the PO is sent', () => {
    for (const from of ['PO_SENT', 'IN_PRODUCTION', 'READY_FOR_QC', 'READY']) {
        assert.equal(T.canTransition(from, 'REPACKAGED'), true, from);
        assert.equal(T.canTransition(from, 'REPACKAGED', { isFqc: true }), true, `${from} (FQC)`);
    }
});

test('REPACKAGED: not from SCHEDULED, nor once the goods are in a container or landed', () => {
    for (const from of ['SCHEDULED', 'CONSOLIDATED', 'ON_SEA', 'ON_AIR', 'ARRIVED_AT_WAREHOUSE', 'RECEIVED', 'PARTIALLY_RECEIVED', 'DESTROYED']) {
        assert.equal(T.canTransition(from, 'REPACKAGED'), false, from);
    }
});

test('REPACKAGED: a non-admin can move it nowhere', () => {
    assert.deepEqual(T.allowedNextStatuses('REPACKAGED'), []);
    assert.deepEqual(T.allowedNextStatuses('REPACKAGED', { isFqc: true }), []);
});

test('REPACKAGED: offered among the next steps of a factory stage', () => {
    assert.ok(T.allowedNextStatuses('IN_PRODUCTION').includes('REPACKAGED'));
    assert.ok(!T.allowedNextStatuses('CONSOLIDATED').includes('REPACKAGED'));
});

test('REPACKAGED: never an AI-suggestion candidate, never suggested, no field gate', () => {
    assert.ok(T.CANDIDATE_EXCLUDED_STATUSES.has('REPACKAGED'));
    assert.ok(!T.INFERRABLE_MILESTONES.includes('REPACKAGED'));
    assert.equal(T.suggestionFor('REPACKAGED', 'ON_SEA').target, null);
    assert.deepEqual(T.gateFieldsFor('REPACKAGED', 'READY'), []);
});

test('DESTROYED rules unchanged', () => {
    assert.equal(T.canTransition('READY_FOR_QC', 'DESTROYED'), true);
    assert.equal(T.canTransition('READY', 'DESTROYED'), false);
    assert.equal(T.canTransition('READY', 'DESTROYED', { isFqc: true }), true);
    assert.equal(T.canTransition('IN_PRODUCTION', 'DESTROYED', { isFqc: true }), false);
});
