'use strict';

// The supplier portal's virus scan (jportal README section 27), as staff meet it here.
//
// GuardDuty Malware Protection scans every file in the portal's upload bucket
// (jportal-pi-uploads-<env>) and tags it GuardDutyMalwareScanStatus=<result>. With the portal's
// gate on, the bucket policy refuses s3:GetObject (so HeadObject, a signed download and a
// copy's source too) on what a supplier or forwarder sent unless that tag is NO_THREATS_FOUND.
// The portal stack owns all of that; this file only explains a refusal to staff: when a read
// of the portal bucket answers 403, explainRefusal reads the object's tags (s3:GetObjectTagging,
// which the policy never refuses) and says whether the file is still being scanned or was
// blocked, instead of a raw S3 error or a 500.
//
// The PI, PL and CI checks start on the scan's result event (serverless.yml) and use
// parseScanEvent; a direct invoke that arrives before the scan simply cannot read the file yet.

const { GetObjectTaggingCommand } = require('@aws-sdk/client-s3');

const SCAN_TAG = 'GuardDutyMalwareScanStatus';
const SCAN_CLEAN = 'NO_THREATS_FOUND';
const SCAN_RESULTS = ['NO_THREATS_FOUND', 'THREATS_FOUND', 'UNSUPPORTED', 'ACCESS_DENIED', 'FAILED'];
const SCAN_DETAIL_TYPE = 'GuardDuty Malware Protection Object Scan Result';

const isObj = v => v !== null && typeof v === 'object' && !Array.isArray(v);

/** An S3 refusal (403), as opposed to a missing key or a fault. */
const isAccessDenied = e => Boolean(e) && (e.name === 'AccessDenied' || e.Code === 'AccessDenied' || e.name === 'Forbidden'
    || (e.$metadata && e.$metadata.httpStatusCode === 403));

/** 'clean' | 'pending' (no result yet) | 'blocked', from S3's TagSet. */
function verdictOf(tags) {
    const found = Array.isArray(tags) ? tags.find(t => t && t.Key === SCAN_TAG) : null;
    if (!found) return { verdict: 'pending', result: null };
    if (found.Value === SCAN_CLEAN) return { verdict: 'clean', result: SCAN_CLEAN };
    return { verdict: 'blocked', result: String(found.Value) };
}

function describe(result) {
    switch (result) {
        case 'THREATS_FOUND': return 'the virus scan found malware in it';
        case 'UNSUPPORTED': return 'the virus scan could not read it (password-protected, or nested too deeply)';
        case 'ACCESS_DENIED': return 'the virus scanner could not read it';
        default: return 'the virus scan failed on it';
    }
}

/**
 * The answer for staff once a read of `key` in the portal bucket was refused:
 * { status: 409, body: { error, code: 'SCAN_PENDING' | 'SCAN_BLOCKED' | 'SCAN_REFUSED' } }.
 * Never throws: tags that cannot be read give the generic SCAN_REFUSED.
 */
async function explainRefusal(s3, bucket, key) {
    let verdict = null;
    try {
        const res = await s3.send(new GetObjectTaggingCommand({ Bucket: bucket, Key: key }));
        verdict = verdictOf(res && res.TagSet);
    } catch (e) {
        verdict = null;
    }
    if (verdict && verdict.verdict === 'pending') {
        return { status: 409, body: { error: 'This file is still being virus-scanned. Try again in a minute.', code: 'SCAN_PENDING' } };
    }
    if (verdict && verdict.verdict === 'blocked') {
        return { status: 409, body: { error: `This file cannot be opened: ${describe(verdict.result)}. Ask the supplier for a clean copy.`, code: 'SCAN_BLOCKED', scanResult: verdict.result } };
    }
    return { status: 409, body: { error: 'This file cannot be opened until the virus scan has cleared it.', code: 'SCAN_REFUSED' } };
}

/**
 * A GuardDuty "Object Scan Result" event for `bucket`: { key, result }, or null when the event
 * is not one (or names another bucket). Unknown results read as FAILED.
 */
function parseScanEvent(event, { bucket = null } = {}) {
    const e = isObj(event) ? event : {};
    if (e.source !== 'aws.guardduty' || e['detail-type'] !== SCAN_DETAIL_TYPE) return null;
    const d = isObj(e.detail) ? e.detail : {};
    const o = isObj(d.s3ObjectDetails) ? d.s3ObjectDetails : {};
    const r = isObj(d.scanResultDetails) ? d.scanResultDetails : {};
    return {
        bucketName: typeof o.bucketName === 'string' ? o.bucketName : null,
        key: typeof o.objectKey === 'string' ? o.objectKey : null,
        result: SCAN_RESULTS.includes(r.scanResultStatus) ? r.scanResultStatus : 'FAILED',
        otherBucket: Boolean(bucket) && o.bucketName !== bucket,
    };
}

/** The message stored on a check that failed on the scan (error_code FILE_SCAN_FAILED). */
function scanFailedMessage(result) {
    return `The file did not pass the virus scan (${result}): ${describe(result)}.`;
}

module.exports = {
    SCAN_TAG,
    SCAN_CLEAN,
    SCAN_DETAIL_TYPE,
    isAccessDenied,
    verdictOf,
    explainRefusal,
    parseScanEvent,
    scanFailedMessage,
};
