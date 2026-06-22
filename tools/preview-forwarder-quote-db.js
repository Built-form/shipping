'use strict';

// Renders a forwarder-quote PDF for a REAL draft container, pulling live data
// (read-only) the same way the /generate endpoint's loadForwarderQuoteForPdf
// does. Writes to a local file — does not touch S3.
//   node tools/preview-forwarder-quote-db.js "Shanghai Mixed Container 2"

require('dotenv').config();
const fs = require('fs');
const { getPool, closePool } = require('../src/db');
const { buildForwarderQuotePdf } = require('../src/services/draft-container-pdf');

async function loadForwarderQuoteForPdf(conn, draftName) {
    const [rows] = await conn.query(
        `SELECT dca.allocated,
                o.jf_code, o.asin, o.product_name, o.supplier, o.port, o.po_number,
                o.units_per_carton
           FROM draft_container_allocations dca
           INNER JOIN orders o ON o.id = dca.order_id AND o.deleted_at IS NULL
          WHERE dca.draft_container_name = ?
          ORDER BY dca.id ASC`,
        [draftName]
    );
    if (!rows.length) return { lines: [], originPorts: [] };

    const jfCodes = [...new Set(rows.map(r => r.jf_code).filter(Boolean))];

    const cartonByJf = new Map();
    if (jfCodes.length) {
        const [crows] = await conn.query(
            `SELECT jf_code, weight, carton_weight, carton_qty,
                    carton_height, carton_width, carton_depth
               FROM mintsoft_carton_sizes
              WHERE jf_code IN (${jfCodes.map(() => '?').join(',')})`,
            jfCodes
        );
        for (const c of crows) if (!cartonByJf.has(c.jf_code)) cartonByJf.set(c.jf_code, c);
    }

    const num = v => (v != null ? Number(v) : 0);
    const originPorts = [...new Set(rows.map(r => r.port).filter(Boolean))];

    const lines = rows.map(r => {
        const c = cartonByJf.get(r.jf_code) || {};
        const orderedUnits = num(r.allocated);
        const cartonWeight = num(c.carton_weight) || num(c.weight) * num(c.carton_qty);
        const unitsPerCarton = num(c.carton_qty) || num(r.units_per_carton);
        const cartonH = num(c.carton_height);
        const cartonL = num(c.carton_depth);
        const cartonW = num(c.carton_width);
        const cartonVol = cartonH * cartonW * cartonL;
        const cartonCbm = cartonVol > 0 ? cartonVol / 1_000_000 : 0;
        const noOfCartons = unitsPerCarton > 0 ? Math.ceil(orderedUnits / unitsPerCarton) : 0;
        const totalCbm = cartonCbm * noOfCartons;
        return {
            sku: r.product_name || r.jf_code || r.asin || '',
            jfCode: r.jf_code || '',
            orderedUnits,
            cartonWeight,
            unitsPerCarton,
            cartonH, cartonL, cartonW,
            cartonCbm,
            noOfCartons,
            totalCbm,
            supplier: r.supplier || '',
            port: r.port || '',
            poNumber: r.po_number || '',
        };
    });

    return { lines, originPorts };
}

(async () => {
    const name = process.argv[2] || 'Shanghai Mixed Container 2';
    const pool = getPool();
    const conn = await pool.getConnection();
    try {
        const { lines, originPorts } = await loadForwarderQuoteForPdf(conn, name);
        if (!lines.length) { console.error(`No allocations for draft container "${name}".`); process.exit(1); }
        const pdf = await buildForwarderQuotePdf(
            { name, date: new Date(), originPorts, comments: '' },
            lines
        );
        const safe = name.replace(/[^A-Za-z0-9._-]/g, '_');
        const out = `sample-forwarder-quote-${safe}.pdf`;
        fs.writeFileSync(out, pdf);
        console.log(`Wrote ${out} (${pdf.length} bytes, ${lines.length} lines, ports: ${originPorts.join(', ') || '—'})`);
    } finally {
        conn.release();
        await closePool();
    }
})().catch(e => { console.error('FATAL:', e); process.exit(1); });
