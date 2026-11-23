'use strict';
// A forwarder's files (docs/forwarder-containers.md, slice C): lists, and when
// told to removes, OUR bucket's copies of uploads that can no longer be accepted.
//
//   node tools/sweep-forwarder-copies.js                                   list only
//   node tools/sweep-forwarder-copies.js --delete --confirm-host <DB_HOST>  remove them
//
// Accepting an upload copies the forwarder's file to our bucket at the row's
// `filed_key` BEFORE the transaction that files it. A refusal after that copy, or
// a crash between the two, leaves one object nothing points at: at most one per
// upload, because the key is made once and kept. This removes it only once the
// upload can no longer be accepted (src/lib/forwarder-sweep.js copySweepable):
// sent back more than a week ago, or taken back more than a week ago. An undecided
// upload's copy is never touched, however old; an accepted upload's copy is the
// filed document and is never touched at all.
//
// Before each delete it reads the upload's row again and asks the rule again. It
// needs credentials that may delete in PO_DOCS_BUCKET, and runs on no schedule.
require('dotenv').config();
const { S3Client, HeadObjectCommand, DeleteObjectCommand } = require('@aws-sdk/client-s3');
const { getPool, closePool } = require('../src/db');
const { jportalSchema } = require('../src/lib/jportal');
const { copySweepable } = require('../src/lib/forwarder-sweep');

const argv = process.argv.slice(2);
const arg = (name) => { const i = argv.indexOf(name); return i === -1 ? undefined : argv[i + 1]; };
const doDelete = argv.includes('--delete');
const host = process.env.DB_HOST || '';
const bucket = process.env.PO_DOCS_BUCKET;

const ROW = jp => `SELECT u.id, u.kind, u.filed_key, u.upload_status, u.review_status, u.decided_at, u.withdrawn_at
                     FROM ${jp}.forwarder_uploads u`;

(async () => {
    if (!bucket) { console.error('PO_DOCS_BUCKET is not set.'); process.exit(2); }
    if (doDelete && arg('--confirm-host') !== host) { console.error(`To delete, say which database this is: --confirm-host ${host || '<DB_HOST>'}`); process.exit(2); }
    const s3 = new S3Client({ region: process.env.AWS_REGION || 'eu-north-1' });
    const pool = getPool();
    const jp = jportalSchema();
    console.log(`Bucket ${bucket}, database ${process.env.DB_USER || '?'}@${host}/${process.env.DB_NAME || '?'}; ${doDelete ? 'DELETING' : 'listing only'}`);
    let found = 0;
    let removed = 0;
    try {
        // Only rows that could ever qualify: a copy was made, and the upload is not waiting and not accepted.
        const [rows] = await pool.query(
            `${ROW(jp)} WHERE u.filed_key IS NOT NULL AND u.review_status <> 'received'
               AND (u.upload_status = 'withdrawn' OR u.review_status = 'rejected') ORDER BY u.id`
        );
        for (const row of rows) {
            if (!copySweepable(row, Date.now())) continue;
            let there = true;
            try { await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: row.filed_key })); } catch { there = false; }
            if (!there) continue;
            found += 1;
            const what = `upload ${row.id} (${row.kind}, ${row.upload_status === 'withdrawn' ? 'taken back' : 'sent back'}): ${row.filed_key}`;
            if (!doDelete) { console.log(`  ${what}`); continue; }
            // Read again, ask again: nothing is removed on what was true a moment ago.
            const [[now]] = await pool.query(`${ROW(jp)} WHERE u.id = ?`, [row.id]);
            if (!now || now.filed_key !== row.filed_key || !copySweepable(now, Date.now())) { console.log(`  ${what} is no longer removable; LEFT ALONE`); continue; }
            await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: row.filed_key }));
            removed += 1;
            console.log(`  ${what} REMOVED`);
        }
        console.log(`${found} stray cop${found === 1 ? 'y' : 'ies'}${doDelete ? `, ${removed} removed` : ' (run with --delete --confirm-host to remove)'}.`);
    } catch (e) {
        console.error('STOPPED:', e.message);
        process.exitCode = 1;
    } finally {
        await closePool();
    }
})();
