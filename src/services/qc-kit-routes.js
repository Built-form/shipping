'use strict';

// GET /api/v1/qc-kits?jfCodes=A,B — the QC inspection kits of products, read
// from JFPRO. A product's kit is its JFPRO documents with category 'qa' and
// Plan of Action stage "China Agent — factory QC" (tag qa_type 'poa_china',
// or no qa_type tag), attached to the product or to a product group it
// belongs to. Users add and change kit documents in JFPRO; shipping only
// lists them (view qc_kit_documents,
// src/db/migrate/2026-10-28_20_qc_kit_documents_view.sql) and attaches them
// to the inspector email (qc-visit-comms-routes.js). The grouping and
// de-duplication rules are in src/lib/qc-kit-docs.js.
//
// Each document carries presigned GET links on the JFPRO documents bucket
// (JFPRO_DOCS_BUCKET, a read-only grant in serverless.yml), valid 15 minutes:
// `url` opens inline for safe types and downloads anything else,
// `downloadUrl` always downloads.
//
// Read only; open to every signed-in user, like the visit lists. Registered
// from orders.js; authed through the greedy /api/v1/{proxy+} route.

const { GetObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const KD = require('../lib/qc-kit-docs');
const { contentDisposition, viewResponseParams } = require('../lib/qc-file-types');

const VIEW_URL_SECONDS = 15 * 60;

function registerQcKitRoutes(app, deps) {
    const { withConnection, s3, jfproDocsBucket, log } = deps;

    // A kit document as the API returns it: s3Key swapped for presigned links.
    async function withLinks(doc) {
        const { s3Key, ...out } = doc;
        const sign = extra => getSignedUrl(
            s3, new GetObjectCommand({ Bucket: jfproDocsBucket, Key: s3Key, ...extra }), { expiresIn: VIEW_URL_SECONDS }
        );
        const [url, downloadUrl] = await Promise.all([
            sign(viewResponseParams(doc.filename, doc.mimeType)),
            sign({ ResponseContentDisposition: contentDisposition('attachment', doc.filename) }),
        ]);
        return { ...out, url, downloadUrl };
    }

    // GET /api/v1/qc-kits?jfCodes=A,B (required, at most 500 codes after
    // normalising) → { data: QcKit[] }, one per requested code in request
    // order, even a code with no documents:
    //   { jfCode, productId|null, productName|null, documents: QcKitDocument[] }
    //   productId is the JFPRO product id (also for a code with no documents,
    //   from jfpro.products), null when JFPRO has no product with that code.
    //   QcKitDocument = { documentId, versionId, version, name, filename,
    //     mimeType|null, sizeBytes|null, uploadedBy|null, uploadedAt|null,
    //     attachedVia: 'product'|'product_group', url, downloadUrl }
    app.get('/api/v1/qc-kits', async (req, res) => {
        try {
            const p = KD.parseJfCodesParam(req.query.jfCodes);
            if (p.error) return res.status(400).json({ error: p.error, code: 'BAD_FIELD', field: 'jfCodes' });
            if (!jfproDocsBucket) return res.status(500).json({ error: 'JFPRO_DOCS_BUCKET env var not configured.', code: 'NO_BUCKET' });
            const kits = await withConnection(async (conn) => {
                const [r] = await conn.query(KD.KIT_DOCUMENTS_SQL, [p.codes]);
                const found = KD.kitsFromRows(p.codes, r);
                // Codes with no documents still need their JFPRO product id, so
                // ShipLine can attach a first upload to it. Best effort.
                const missing = found.filter(k => k.productId == null).map(k => k.jfCode);
                if (!missing.length) return found;
                try {
                    const [prows] = await conn.query(KD.KIT_PRODUCTS_SQL, [missing]);
                    return KD.fillKitProducts(found, prows);
                } catch (e) {
                    log.warn('[GET /qc-kits] jfpro.products lookup failed', { error: e.message, code: e.code });
                    return found;
                }
            });
            const data = await Promise.all(kits.map(async k => ({ ...k, documents: await Promise.all(k.documents.map(withLinks)) })));
            res.json({ data });
        } catch (error) {
            log.error('[GET /qc-kits]', error);
            if (!res.headersSent) res.status(500).json({ error: 'An internal error occurred.' });
        }
    });
}

module.exports = { registerQcKitRoutes };
