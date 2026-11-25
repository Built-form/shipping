'use strict';

// The supplier portal's virus scan as shipping meets it (src/lib/portal-scan.js): the scan
// result event as a check trigger (PI, PL and CI parseTrigger), and the explanation staff get
// when the portal bucket refuses a read.
//
//   npm run test:unit

const { test } = require('node:test');
const assert = require('node:assert/strict');
const PS = require('../src/lib/portal-scan');
const PI = require('../src/lib/pi-submissions');
const PL = require('../src/lib/pl-submissions');
const CI = require('../src/lib/ci-submissions');

const BUCKET = 'jportal-pi-uploads-test';
const UUID = '123e4567-e89b-12d3-a456-426614174000';
const scanEvent = (key, result, bucket = BUCKET) => ({
    source: 'aws.guardduty',
    'detail-type': 'GuardDuty Malware Protection Object Scan Result',
    detail: {
        scanStatus: 'COMPLETED',
        s3ObjectDetails: { bucketName: bucket, objectKey: key, eTag: 'abc' },
        scanResultDetails: { scanResultStatus: result, threats: null },
    },
});

test('a scan result is an event trigger carrying the result, for its own prefix only', () => {
    assert.deepEqual(PI.parseTrigger(scanEvent(`pi/${UUID}.pdf`, 'NO_THREATS_FOUND'), { bucket: BUCKET }),
        { source: 'event', key: `pi/${UUID}.pdf`, scan: 'NO_THREATS_FOUND' });
    assert.deepEqual(PL.parseTrigger(scanEvent(`pl/${UUID}.pdf`, 'THREATS_FOUND'), { bucket: BUCKET }),
        { source: 'event', key: `pl/${UUID}.pdf`, scan: 'THREATS_FOUND' });
    assert.equal(CI.parseTrigger(scanEvent(`ci/${UUID}.pdf`, 'UNSUPPORTED'), { bucket: BUCKET }).scan, 'UNSUPPORTED');
    assert.equal(PI.parseTrigger(scanEvent(`pl/${UUID}.pdf`, 'NO_THREATS_FOUND'), { bucket: BUCKET }).ignore, 'not_a_pi_key');
    assert.equal(PI.parseTrigger(scanEvent(`pi/${UUID}.pdf`, 'NO_THREATS_FOUND', 'other'), { bucket: BUCKET }).ignore, 'other_bucket');
    assert.equal(PI.parseTrigger(scanEvent(`pi/${UUID}.pdf`, 'SOMETHING_NEW'), { bucket: BUCKET }).scan, 'FAILED');
});

test('the old triggers still parse: a direct invoke, and S3 "Object Created"', () => {
    assert.deepEqual(PI.parseTrigger({ submissionId: 5 }), { source: 'direct', submissionId: 5 });
    const created = { source: 'aws.s3', 'detail-type': 'Object Created', detail: { bucket: { name: BUCKET }, object: { key: `pi/${UUID}.pdf` } } };
    assert.deepEqual(PI.parseTrigger(created, { bucket: BUCKET }), { source: 'event', key: `pi/${UUID}.pdf` });
});

test('verdicts from the tag', () => {
    assert.equal(PS.verdictOf([]).verdict, 'pending');
    assert.equal(PS.verdictOf([{ Key: PS.SCAN_TAG, Value: 'NO_THREATS_FOUND' }]).verdict, 'clean');
    assert.deepEqual(PS.verdictOf([{ Key: PS.SCAN_TAG, Value: 'THREATS_FOUND' }]), { verdict: 'blocked', result: 'THREATS_FOUND' });
});

test('a refused read is explained from the tags, and never throws', async () => {
    const s3With = tags => ({ async send() { if (tags instanceof Error) throw tags; return { TagSet: tags }; } });
    assert.equal((await PS.explainRefusal(s3With([]), BUCKET, 'pi/x.pdf')).body.code, 'SCAN_PENDING');
    const blocked = await PS.explainRefusal(s3With([{ Key: PS.SCAN_TAG, Value: 'THREATS_FOUND' }]), BUCKET, 'pi/x.pdf');
    assert.equal(blocked.status, 409);
    assert.equal(blocked.body.code, 'SCAN_BLOCKED');
    assert.match(blocked.body.error, /malware/);
    assert.equal((await PS.explainRefusal(s3With(new Error('AccessDenied')), BUCKET, 'pi/x.pdf')).body.code, 'SCAN_REFUSED');
});

test('only a 403 counts as the scan refusing', () => {
    assert.ok(PS.isAccessDenied({ name: 'AccessDenied' }));
    assert.ok(PS.isAccessDenied({ name: 'Forbidden', $metadata: { httpStatusCode: 403 } }));
    assert.ok(!PS.isAccessDenied({ name: 'NotFound', $metadata: { httpStatusCode: 404 } }));
    assert.ok(!PS.isAccessDenied(null));
    assert.match(PS.scanFailedMessage('THREATS_FOUND'), /did not pass the virus scan \(THREATS_FOUND\)/);
});
