'use strict';

// Unit tests for src/lib/payment-rules.js — request body parsing and row
// serialisation for /api/v1/payment-rules. No database.
//   node --test tools/test-payment-rules-lib.js

const test = require('node:test');
const assert = require('node:assert/strict');
const { parsePaymentRuleBody, paymentRuleRowToJson } = require('../src/lib/payment-rules');

const ROW = {
    id: 1, scope: 'default', supplier_name: '', supplier_label: null,
    deposit_pct: null, deposit_trigger: null, deposit_grace_days: 0,
    balance_trigger: null, balance_document_type: null, balance_offset_days: null, balance_grace_days: 0,
    deposit_offset_days: null, estimates_json: null, notes: null, updated_by_email: null,
    created_at: '2026-09-18 10:00:00', updated_at: '2026-09-18 10:00:00',
    air_owed_from: null, air_limit_days: null,
};

test('existing fields still parse: supplier rule, lower-cased key, grace defaults to 0', () => {
    const r = parsePaymentRuleBody({ scope: 'supplier', supplierName: 'Foreu Corporation', balanceTrigger: 'bl', depositPct: 30 });
    assert.equal(r.error, undefined);
    assert.equal(r.row.supplier_name, 'foreu corporation');
    assert.equal(r.row.supplier_label, 'Foreu Corporation');
    assert.equal(r.row.balance_trigger, 'bl');
    assert.equal(r.row.deposit_pct, 30);
    assert.equal(r.row.balance_grace_days, 0);
});

test('existing validation still refuses an unknown balance trigger', () => {
    assert.match(parsePaymentRuleBody({ scope: 'default', balanceTrigger: 'whenever' }).error, /balanceTrigger must be one of/);
});

test('default rule: air start date and limit are stored', () => {
    const r = parsePaymentRuleBody({ scope: 'default', airOwedFrom: '2026-08-01', airLimitDays: 60 });
    assert.equal(r.error, undefined);
    assert.equal(r.row.air_owed_from, '2026-08-01');
    assert.equal(r.row.air_limit_days, 60);
});

test('blank air fields store null', () => {
    const r = parsePaymentRuleBody({ scope: 'default', airOwedFrom: '', airLimitDays: '' });
    assert.equal(r.row.air_owed_from, null);
    assert.equal(r.row.air_limit_days, null);
    const omitted = parsePaymentRuleBody({ scope: 'default' });
    assert.equal(omitted.row.air_owed_from, null);
    assert.equal(omitted.row.air_limit_days, null);
});

test('supplier rule: a limit is allowed, a start date is refused (company-wide)', () => {
    const ok = parsePaymentRuleBody({ scope: 'supplier', supplierName: 'Foreu Corporation', airLimitDays: 30 });
    assert.equal(ok.error, undefined);
    assert.equal(ok.row.air_limit_days, 30);
    assert.equal(ok.row.air_owed_from, null);
    assert.match(parsePaymentRuleBody({ scope: 'supplier', supplierName: 'Foreu Corporation', airOwedFrom: '2026-08-01' }).error, /airOwedFrom/);
});

test('bad air values are refused', () => {
    assert.match(parsePaymentRuleBody({ scope: 'default', airOwedFrom: '01/08/2026' }).error, /airOwedFrom/);
    assert.match(parsePaymentRuleBody({ scope: 'default', airOwedFrom: '2026-02-30' }).error, /airOwedFrom/);
    assert.match(parsePaymentRuleBody({ scope: 'default', airLimitDays: 0 }).error, /airLimitDays/);
    assert.match(parsePaymentRuleBody({ scope: 'default', airLimitDays: 366 }).error, /airLimitDays/);
    assert.match(parsePaymentRuleBody({ scope: 'default', airLimitDays: 7.5 }).error, /airLimitDays/);
});

test('row → JSON carries the air fields', () => {
    const j = paymentRuleRowToJson({ ...ROW, air_owed_from: '2026-08-01', air_limit_days: 45 });
    assert.equal(j.airOwedFrom, '2026-08-01');
    assert.equal(j.airLimitDays, 45);
    const blank = paymentRuleRowToJson(ROW);
    assert.equal(blank.airOwedFrom, null);
    assert.equal(blank.airLimitDays, null);
});

test('row → JSON: a supplier row never reports a start date', () => {
    const j = paymentRuleRowToJson({ ...ROW, scope: 'supplier', supplier_name: 'foreu corporation', supplier_label: 'Foreu Corporation', air_owed_from: '2026-08-01', air_limit_days: 30 });
    assert.equal(j.airOwedFrom, null);
    assert.equal(j.airLimitDays, 30);
});

// "Departure": how long after its goods are ready a shipment in no container
// is expected to leave (user, 2026-10-05). Counted from the ready date only.
test('estimates.departure: days after goods ready, kept through body and row', () => {
    const { parsePaymentRuleEstimates } = require('../src/lib/payment-rules');
    assert.deepEqual(parsePaymentRuleEstimates({ departure: { from: 'ready', days: 21 } }).value.departure, { from: 'ready', days: 21 });
    assert.equal(parsePaymentRuleEstimates({}).value.departure, null);
    assert.equal(parsePaymentRuleEstimates({ departure: '' }).value.departure, null);
    assert.equal(parsePaymentRuleEstimates({ departure: { from: 'po', days: 21 } }).error, 'estimates.departure.from must be one of: ready.');
    assert.equal(parsePaymentRuleEstimates({ departure: { from: 'ready', days: 400 } }).error, 'estimates.departure.days must be a whole number of days between 0 and 365.');
    const body = parsePaymentRuleBody({ scope: 'default', estimates: { departure: { from: 'ready', days: 21 } } });
    assert.deepEqual(JSON.parse(body.row.estimates_json).departure, { from: 'ready', days: 21 });
    const row = paymentRuleRowToJson({ ...ROW, estimates_json: body.row.estimates_json });
    assert.deepEqual(row.estimates.departure, { from: 'ready', days: 21 });
    assert.equal(paymentRuleRowToJson(ROW).estimates.departure, null);
});
