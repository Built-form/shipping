'use strict';

// Renders a sample supplier-quote PDF to sample-supplier-quote.pdf so the
// layout can be eyeballed without hitting the DB / S3. Rows are pre-filtered
// to a single supplier (as the /generate handler does upstream).
//   node tools/preview-supplier-quote.js

const fs = require('fs');
const { buildSupplierQuotePdf } = require('../src/services/draft-container-pdf');

// Same fixture as preview-forwarder-quote.js, but only the SUNMED rows.
const sample = [
    { sku: 'TPM-2.5CMX10M-4', jfCode: 'JF1292', orderedUnits: 4000, cartonWeight: 8.2, unitsPerCarton: 100, cartonH: 15.5, cartonL: 47, cartonW: 38, cartonCbm: 0.027683, noOfCartons: 40, totalCbm: 1.10732, supplier: 'SUNMED', supplierCountry: 'China', port: 'Shanghai', poNumber: 'SUNMED-93' },
    { sku: 'CUTI-AWD-8X7.5-50', jfCode: 'JF1306', orderedUnits: 8400, cartonWeight: 6, unitsPerCarton: 40, cartonH: 27, cartonL: 44.5, cartonW: 38, cartonCbm: 0.045657, noOfCartons: 210, totalCbm: 9.58797, supplier: 'SUNMED', supplierCountry: 'China', port: 'Shanghai', poNumber: 'SUNMED-93' },
];

(async () => {
    const pdf = await buildSupplierQuotePdf(
        { name: 'CONTAINER-001', date: new Date(), supplier: 'SUNMED', originPorts: ['Shanghai'], comments: 'Please quote DDP to our UK warehouse.' },
        sample
    );
    const out = 'sample-supplier-quote.pdf';
    fs.writeFileSync(out, pdf);
    console.log(`Wrote ${out} (${pdf.length} bytes, ${sample.length} rows)`);
})().catch(e => { console.error('FATAL:', e); process.exit(1); });
