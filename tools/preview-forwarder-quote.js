'use strict';

// Renders a sample forwarder-quote PDF to sample-forwarder-quote.pdf so the
// layout can be eyeballed without hitting the DB / S3.
//   node tools/preview-forwarder-quote.js

const fs = require('fs');
const { buildForwarderQuotePdf } = require('../src/services/draft-container-pdf');

const sample = [
    { sku: 'TPM-2.5CMX10M-4', jfCode: 'JF1292', orderedUnits: 4000, cartonWeight: 8.2, unitsPerCarton: 100, cartonH: 15.5, cartonL: 47, cartonW: 38, cartonCbm: 0.027683, noOfCartons: 40, totalCbm: 1.10732, supplier: 'SUNMED', supplierCountry: 'China', port: 'Shanghai', poNumber: 'SUNMED-93' },
    { sku: 'CUTI-AWD-8X7.5-50', jfCode: 'JF1306', orderedUnits: 8400, cartonWeight: 6, unitsPerCarton: 40, cartonH: 27, cartonL: 44.5, cartonW: 38, cartonCbm: 0.045657, noOfCartons: 210, totalCbm: 9.58797, supplier: 'SUNMED', supplierCountry: 'China', port: 'Shanghai', poNumber: 'SUNMED-93' },
    { sku: 'THERAPY-BALLS-MIXED-LONGNAME-VARIANT', jfCode: 'JF1098', orderedUnits: 2160, cartonWeight: 12.6, unitsPerCarton: 48, cartonH: 25.5, cartonL: 45.5, cartonW: 29.5, cartonCbm: 0.035351, noOfCartons: 45, totalCbm: 1.6358, supplier: 'LIKEGREEN WITH A VERY LONG SUPPLIER NAME', supplierCountry: 'China', port: 'Ningbo', poNumber: 'LIKEGREEN-17' },
    { sku: 'CUTI-CA-5X5-10', jfCode: 'JF0918', orderedUnits: 750, cartonWeight: 1.85, unitsPerCarton: 40, cartonH: 24, cartonL: 25.5, cartonW: 25.5, cartonCbm: 0.015606, noOfCartons: 19, totalCbm: 0.3081, supplier: 'ROOSIN', supplierCountry: 'China', port: 'Shanghai', poNumber: 'ROOSIN-35' },
];

(async () => {
    const pdf = await buildForwarderQuotePdf(
        { name: 'CONTAINER-001', date: new Date(), originPorts: ['Shanghai', 'Ningbo'], comments: 'Please quote DDP to our UK warehouse.' },
        sample
    );
    const out = 'sample-forwarder-quote.pdf';
    fs.writeFileSync(out, pdf);
    console.log(`Wrote ${out} (${pdf.length} bytes, ${sample.length} rows)`);
})().catch(e => { console.error('FATAL:', e); process.exit(1); });
