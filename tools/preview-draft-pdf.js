'use strict';

// Render a sample draft container PDF to disk for visual inspection.
// Uses the service directly — no DB, no server, no S3.

const fs = require('fs');
const path = require('path');
const { buildDraftContainerPdf } = require('../src/services/draft-container-pdf');

const draft = {
    name: 'DRAFT1',
    date: new Date(),
    originPorts: ['Shanghai, China', 'Ningbo, China'],
    requestedBy: {
        name: 'JFA Medical Ltd',
        addressLines: ['Unit B Prestige House', 'Cornford Road', 'Blackpool', 'Lancashire', 'FY4 4QQ'],
        country: 'UNITED KINGDOM',
    },
    comments: 'Please quote FOB Shanghai → DDP Blackpool, UK. Required delivery by end of June 2026. Goods are non-hazardous medical consumables.',
};

const lines = [
    { sku: 'JF1441', name: 'CUTI-ALC-WIPE500',   quantity: 50,  cbm: 0.85,  cartons: 2 },
    { sku: 'JF1404', name: 'CUTI-ADH-15x15-25',  quantity: 75,  cbm: 1.20,  cartons: 3 },
    { sku: 'JF1502', name: 'CUTI-GAUZE-10x10',   quantity: 200, cbm: 2.40,  cartons: 8 },
    { sku: 'JF1233', name: 'CUTI-TAPE-2.5cm',    quantity: 120, cbm: 0.65,  cartons: 4 },
    { sku: 'JF0987', name: 'CUTI-GLOVE-M-100',   quantity: 300, cbm: 3.10,  cartons: 12 },
];

(async () => {
    const buf = await buildDraftContainerPdf(draft, lines);
    const outPath = path.join(__dirname, '..', 'sample-draft-container.pdf');
    fs.writeFileSync(outPath, buf);
    console.log(`Wrote ${buf.length} bytes → ${outPath}`);
})();
