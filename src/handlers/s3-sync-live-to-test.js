require('dotenv').config();
const log = require('../lib/logger');
const {
    S3Client,
    ListObjectsV2Command,
    CopyObjectCommand,
    DeleteObjectsCommand,
} = require('@aws-sdk/client-s3');

// ── Nightly one-way S3 mirror: live bucket (stage "dev" = prod) → test ───────
// Companion to the nightly DB replication (explorer → explorer-test). The
// replica carries every purchase_order_documents / quality_assurance / signed-PI
// row, and those rows reference their PDF by S3 key (the key-based GetObject
// paths in orders.js, po-invoice-check.js and front-qc-import.js all read
// process.env.PO_DOCS_BUCKET). The test stack has its own, initially EMPTY
// shipping-purchase-orders-test bucket, so without this mirror every one of
// those reads 404s.
//
// Scheduled in the TEST stack only (see s3SyncEnabled in serverless.yml) — the
// test environment pulls its own mirror and the prod stack never runs it. The
// live bucket is only ever READ; nothing here writes to prod.
//
// Deletions mirror too: keys gone from live are removed from test, which also
// clears test-only uploads — the nightly DB overwrite orphans those rows anyway.
//
// Diff is by key + size. Same-size in-place content changes are missed, but
// document keys are immutable (each PDF regeneration writes a new version key
// carrying a fresh uuid), so overwrites don't happen in practice. Copies are
// server-side (CopyObject) — no object bytes pass through this Lambda, and
// same-region copies incur no data-transfer charge.
//
// NOTE: rows created before this test stack existed also store an ABSOLUTE
// public_url pointing at the live bucket, so those links resolve against prod
// directly (public-read) whether or not this mirror has run. The mirror is what
// makes the key-based server-side reads work.

const PAIRS = [
    { src: 'shipping-purchase-orders-dev', dst: 'shipping-purchase-orders-test' },
];

const CONCURRENCY = 10;

const s3 = new S3Client({});

async function listAll(bucket) {
    const objects = new Map();
    let token;
    do {
        const page = await s3.send(
            new ListObjectsV2Command({ Bucket: bucket, ContinuationToken: token })
        );
        for (const o of page.Contents || []) objects.set(o.Key, o.Size);
        token = page.NextContinuationToken;
    } while (token);
    return objects;
}

async function inBatches(items, size, fn) {
    for (let i = 0; i < items.length; i += size) {
        await Promise.all(items.slice(i, i + size).map(fn));
    }
}

exports.handler = async () => {
    try {
        log.info('[s3-sync-live-to-test] starting');
        const summary = [];

        for (const { src, dst } of PAIRS) {
            const [srcObjs, dstObjs] = await Promise.all([listAll(src), listAll(dst)]);

            const toCopy = [...srcObjs].filter(([key, size]) => dstObjs.get(key) !== size);
            const toDelete = [...dstObjs.keys()].filter((key) => !srcObjs.has(key));

            await inBatches(toCopy, CONCURRENCY, ([key]) =>
                s3.send(
                    new CopyObjectCommand({
                        Bucket: dst,
                        Key: key,
                        // CopySource wants "bucket/key" with the key URI-encoded
                        // but the path separators kept literal.
                        CopySource: `${src}/${encodeURIComponent(key).replace(/%2F/g, '/')}`,
                    })
                )
            );

            // DeleteObjects takes at most 1000 keys per call.
            for (let i = 0; i < toDelete.length; i += 1000) {
                await s3.send(
                    new DeleteObjectsCommand({
                        Bucket: dst,
                        Delete: {
                            Objects: toDelete.slice(i, i + 1000).map((Key) => ({ Key })),
                            Quiet: true,
                        },
                    })
                );
            }

            const line = `${src} -> ${dst}: ${srcObjs.size} live objects, copied ${toCopy.length}, deleted ${toDelete.length}`;
            log.info(`[s3-sync-live-to-test] ${line}`);
            summary.push(line);
        }

        return { statusCode: 200, body: JSON.stringify({ message: 'S3 mirror complete', summary }) };
    } catch (err) {
        log.error('[s3-sync-live-to-test] failed', err);
        return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
    }
};

// Local runner: `node src/handlers/s3-sync-live-to-test.js`
if (require.main === module) {
    exports.handler()
        .then((result) => console.log(result.body))
        .catch((err) => { console.error('Fatal:', err); process.exit(1); });
}
