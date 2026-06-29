'use strict';

// Renders a "Delivery Quote Request" PDF for a draft container. Mirrors the
// layout of buildPoPdf — same header block, same 5-column table, same totals
// + comments structure — so a freight forwarder receiving both formats sees
// a consistent shape. Columns differ: Ref / Item / Qty / CBM / Cartons.

const PDFDocument = require('pdfkit');

const DELIVERY_ADDRESS = [
    'Blackpool',
    'Prestige House',
    'Cornford Road',
    'Blackpool',
    '',
    'FY4 4QQ',
    'UNITED KINGDOM',
];

const PAGE_TOP = 50;
const PAGE_BOTTOM = 792;
const TABLE_LEFT = 50;
const TABLE_WIDTH = 490;
const ROW_H = 28;
const COLS = [
    { header: 'Ref',      x: 50,  w: 55,  align: 'center' },
    { header: 'Item',     x: 105, w: 160, align: 'center' },
    { header: 'Lot',      x: 265, w: 70,  align: 'center' },
    { header: 'Quantity', x: 335, w: 55,  align: 'center' },
    { header: 'CBM',      x: 390, w: 70,  align: 'center' },
    { header: 'Cartons',  x: 460, w: 75,  align: 'center' },
];

function fmtNumber(n, dp = 2) {
    if (n === null || n === undefined || isNaN(n)) return '-';
    return Number(n).toFixed(dp);
}

function fmtInt(n) {
    if (n === null || n === undefined || isNaN(n)) return '-';
    return String(Math.round(Number(n)));
}

function fmtDate(d) {
    if (!d) return '';
    const dt = d instanceof Date ? d : new Date(d);
    if (isNaN(dt.getTime())) return '';
    const dd = String(dt.getDate()).padStart(2, '0');
    const mm = String(dt.getMonth() + 1).padStart(2, '0');
    return `${dd}/${mm}/${dt.getFullYear()}`;
}

function drawHeaderBlock(doc, draft) {
    doc.fontSize(22).font('Helvetica-Bold').text('DELIVERY QUOTE REQUEST', 50, 60);

    doc.fontSize(10).font('Helvetica-Bold');
    doc.text('Ref:', 50, 110, { continued: true })
       .font('Helvetica').text(`   ${draft.name || ''}`);
    doc.font('Helvetica-Bold')
       .text('Date:', 50, 125, { continued: true })
       .font('Helvetica').text(`   ${fmtDate(draft.date)}`);

    const addrTop = 165;

    // Goods From — list of unique origin ports across allocations, or "—".
    doc.font('Helvetica-Bold').text('Goods From:', 50, addrTop);
    const originLines = draft.originPorts && draft.originPorts.length
        ? draft.originPorts
        : ['—'];
    let y = addrTop + 15;
    for (const line of originLines) {
        doc.font('Helvetica').text(line, 50, y, { width: 200 });
        y += 12;
    }

    // Delivery To — hardcoded JFA UK delivery address (matches PO PDF).
    doc.font('Helvetica-Bold').text('Delivery To:', 280, addrTop);
    y = addrTop + 15;
    for (const line of DELIVERY_ADDRESS) {
        doc.font(DELIVERY_ADDRESS.indexOf(line) === 0 ? 'Helvetica-Bold' : 'Helvetica')
           .text(line || ' ', 280, y, { width: 130, align: 'right' });
        y += 12;
    }

    // Requested by — the JFA contact / company issuing the quote request.
    doc.font('Helvetica-Bold').text('Requested By:', 430, addrTop);
    const requestedByLines = [
        draft.requestedBy?.name || 'JFA Medical Ltd',
        ...(draft.requestedBy?.addressLines || []),
        draft.requestedBy?.country || '',
    ];
    y = addrTop + 15;
    for (const line of requestedByLines) {
        doc.font('Helvetica').text(line || ' ', 430, y, { width: 130, align: 'right' });
        y += 12;
    }
}

function drawTableHeader(doc, y) {
    doc.lineWidth(0.5).rect(TABLE_LEFT, y, TABLE_WIDTH, ROW_H).fillAndStroke('#e6e6e6', '#000');
    doc.fillColor('#000').font('Helvetica-Bold').fontSize(10);
    for (const col of COLS) {
        doc.text(col.header, col.x, y + 9, { width: col.w, align: col.align });
    }
    return y + ROW_H;
}

function drawRow(doc, y, line) {
    doc.lineWidth(0.5).rect(TABLE_LEFT, y, TABLE_WIDTH, ROW_H).stroke('#000');
    doc.font('Helvetica').fontSize(10).fillColor('#000');
    doc.text(line.sku || '',                       50,  y + 9, { width: 55,  align: 'center' });
    doc.text(fitText(doc, line.name || '', 160),   105, y + 9, { width: 160, align: 'center' });
    const lotTxt = line.lot == null ? '' : String(line.lot);
    const lotSize = fitFontSize(doc, lotTxt, 70, 10);
    doc.fontSize(lotSize).text(lotTxt, 265, y + 9 + (10 - lotSize) / 2, { width: 70, align: 'center', lineBreak: false });
    doc.fontSize(10);
    doc.text(String(line.quantity || 0),           335, y + 9, { width: 55,  align: 'center' });
    doc.text(fmtNumber(line.cbm, 3),               390, y + 9, { width: 70,  align: 'center' });
    doc.text(fmtInt(line.cartons),                 460, y + 9, { width: 75,  align: 'center' });
    return { yNext: y + ROW_H };
}

function drawTotalsBox(doc, y, totalQty, totalCbm, totalCartons) {
    const totalsX = 320;
    const labelW = 100;
    const valueW = 100;

    const row = (label, value, by, isBold = false) => {
        doc.lineWidth(0.5).rect(totalsX, by, labelW, ROW_H).stroke('#000');
        doc.lineWidth(0.5).rect(totalsX + labelW, by, valueW, ROW_H).stroke('#000');
        doc.font(isBold ? 'Helvetica-Bold' : 'Helvetica').fontSize(10).fillColor('#000');
        doc.text(label, totalsX + 5, by + 9, { width: labelW - 10, align: 'left' });
        doc.text(value, totalsX + labelW + 5, by + 9, { width: valueW - 10, align: 'left' });
    };
    row('Total Quantity', String(totalQty), y);
    row('Total CBM',      fmtNumber(totalCbm, 3), y + ROW_H);
    row('Total Cartons',  fmtInt(totalCartons), y + ROW_H * 2, true);
    return y + ROW_H * 3;
}

function drawCommentsBox(doc, y, comments) {
    doc.lineWidth(0.5).rect(50, y, 490, 90).fillAndStroke('#f2f2f2', '#000');
    doc.fillColor('#000').font('Helvetica-Bold').fontSize(10).text('Comments:', 60, y + 10);
    doc.font('Helvetica').fontSize(10).text(comments || '', 60, y + 30, {
        width: 470, height: 50,
    });
    return y + 90;
}

// draft: { name, date, originPorts[], requestedBy: { name, addressLines[], country }, comments }
// lines: [{ sku, name, quantity, cbm, cartons }]
async function buildDraftContainerPdf(draft, lines) {
    const doc = new PDFDocument({ size: 'A4', margin: 50 });
    const chunks = [];
    doc.on('data', c => chunks.push(c));
    const done = new Promise(resolve => doc.on('end', () => resolve(Buffer.concat(chunks))));

    drawHeaderBlock(doc, draft);
    let y = drawTableHeader(doc, 320);
    let totalQty = 0;
    let totalCbm = 0;
    let totalCartons = 0;

    const startNewPage = () => {
        doc.addPage();
        y = drawTableHeader(doc, PAGE_TOP);
    };

    for (const line of lines) {
        if (y + ROW_H > PAGE_BOTTOM) startNewPage();
        const { yNext } = drawRow(doc, y, line);
        totalQty     += Number(line.quantity || 0);
        totalCbm     += Number(line.cbm || 0);
        totalCartons += Number(line.cartons || 0);
        y = yNext;
    }

    const totalsHeight = ROW_H * 3;
    const commentsHeight = 90;
    const blockGap = 30;
    const requiredSpace = blockGap + totalsHeight + blockGap + commentsHeight;
    if (y + requiredSpace > PAGE_BOTTOM) {
        doc.addPage();
        y = PAGE_TOP;
    } else {
        y += blockGap;
    }

    y = drawTotalsBox(doc, y, totalQty, totalCbm, totalCartons);
    y = drawCommentsBox(doc, y + blockGap, draft.comments);

    doc.end();
    return done;
}

// ── Forwarder quote (detailed, landscape) ────────────────────────────────
// A wider variant aimed at freight forwarders: one row per allocated order
// line with carton-level weight / dimensions / CBM so the forwarder can price
// the shipment. Landscape A4 to fit the 15 columns.

const FQ_TABLE_LEFT = 25;
const FQ_HEADER_H = 34;
const FQ_ROW_H = 16;
const FQ_PAGE_BOTTOM = 560; // landscape A4 height 595 − bottom margin

const FQ_COLS = [
    { key: 'sku',             header: 'SKU',                w: 90,  align: 'left'  },
    { key: 'jfCode',          header: 'HW/JF Code',         w: 50,  align: 'left'  },
    { key: 'lot',             header: 'LOT',                w: 64,  align: 'left'  },
    { key: 'orderedUnits',    header: 'Ordered Units',      w: 46,  align: 'right', fmt: fmtInt },
    { key: 'cartonWeight',    header: 'Carton Weight (kg)', w: 46,  align: 'right', fmt: v => fmtNumber(v, 2) },
    { key: 'unitsPerCarton',  header: 'Units per Carton',   w: 42,  align: 'right', fmt: fmtInt },
    { key: 'cartonH',         header: 'Carton H (cm)',      w: 36,  align: 'right', fmt: v => fmtNumber(v, 1) },
    { key: 'cartonL',         header: 'Carton L (cm)',      w: 36,  align: 'right', fmt: v => fmtNumber(v, 1) },
    { key: 'cartonW',         header: 'Carton W (cm)',      w: 36,  align: 'right', fmt: v => fmtNumber(v, 1) },
    { key: 'cartonCbm',       header: 'Carton CBM',         w: 48,  align: 'right', fmt: v => fmtNumber(v, 4) },
    { key: 'noOfCartons',     header: 'No. of Cartons',     w: 40,  align: 'right', fmt: fmtInt },
    { key: 'totalCbm',        header: 'Total CBM',          w: 48,  align: 'right', fmt: v => fmtNumber(v, 4) },
    { key: 'totalWeight',     header: 'Total Weight (kg)',  w: 44,  align: 'right', fmt: v => fmtNumber(v, 2),
      compute: l => Number(l.cartonWeight || 0) * Number(l.noOfCartons || 0) },
    { key: 'supplier',        header: 'Supplier',           w: 70,  align: 'left'  },
    { key: 'port',            header: 'Port',               w: 50,  align: 'left'  },
    { key: 'poNumber',        header: 'PO Number',          w: 46,  align: 'left'  },
];
const FQ_TABLE_WIDTH = FQ_COLS.reduce((s, c) => s + c.w, 0);

// Resolve each column's absolute x from cumulative widths.
function fqCols() {
    let x = FQ_TABLE_LEFT;
    return FQ_COLS.map(c => { const col = { ...c, x }; x += c.w; return col; });
}

function drawFqHeaderBlock(doc, draft) {
    doc.fillColor('#000').fontSize(18).font('Helvetica-Bold').text('FORWARDER QUOTE REQUEST', 25, 30);

    doc.fontSize(9).font('Helvetica-Bold')
       .text('Ref:', 25, 60, { continued: true }).font('Helvetica').text(`   ${draft.name || ''}`);
    doc.font('Helvetica-Bold')
       .text('Date:', 25, 74, { continued: true }).font('Helvetica').text(`   ${fmtDate(draft.date)}`);

    const origin = (draft.originPorts && draft.originPorts.length) ? draft.originPorts.join(', ') : '—';
    doc.font('Helvetica-Bold')
       .text('Goods From:', 320, 60, { continued: true }).font('Helvetica').text(`   ${origin}`, { width: 300 });
    doc.font('Helvetica-Bold')
       .text('Delivery To:', 320, 74, { continued: true }).font('Helvetica').text('   Prestige House, Cornford Road, Blackpool, FY4 4QQ, United Kingdom');
}

function drawFqTableHeader(doc, cols, y) {
    doc.lineWidth(0.5);
    for (const c of cols) doc.rect(c.x, y, c.w, FQ_HEADER_H).fillAndStroke('#e6e6e6', '#000');
    doc.fillColor('#000').font('Helvetica-Bold').fontSize(6.5);
    for (const c of cols) {
        doc.text(c.header, c.x + 2, y + 3, { width: c.w - 4, align: c.align === 'right' ? 'right' : 'left' });
    }
    return y + FQ_HEADER_H;
}

// Trim a string with a trailing ellipsis so it renders on one line within
// maxWidth at the doc's current font/size. PDFKit's own lineBreak:false +
// ellipsis still wrapped long hyphenated/spaced strings, overflowing the
// fixed row height — so we measure and clip explicitly.
function fitText(doc, txt, maxWidth) {
    if (txt == null) return '';
    txt = String(txt);
    if (doc.widthOfString(txt) <= maxWidth) return txt;
    let s = txt;
    while (s.length > 1 && doc.widthOfString(s + '…') > maxWidth) s = s.slice(0, -1);
    return s + '…';
}

// Choose the largest font size <= baseSize at which `txt` fits maxWidth on one
// line, down to minSize. Used for the Lot column so a long lot number renders
// smaller rather than being truncated or wrapped. Caller restores the font.
function fitFontSize(doc, txt, maxWidth, baseSize, minSize = 3) {
    txt = txt == null ? '' : String(txt);
    let size = baseSize;
    doc.fontSize(size);
    if (!txt) return size;
    while (size > minSize && doc.widthOfString(txt) > maxWidth) {
        size = Math.max(minSize, size - 0.5);
        doc.fontSize(size);
    }
    return size;
}

function drawFqRow(doc, cols, y, line) {
    doc.lineWidth(0.5);
    for (const c of cols) doc.rect(c.x, y, c.w, FQ_ROW_H).stroke('#000');
    doc.font('Helvetica').fontSize(7).fillColor('#000');
    for (const c of cols) {
        const raw = c.compute ? c.compute(line) : line[c.key];
        const txt = c.fmt ? c.fmt(raw) : (raw == null ? '' : String(raw));
        if (c.key === 'lot') {
            // Lot numbers are never truncated — shrink the font to fit instead.
            const size = fitFontSize(doc, txt, c.w - 4, 7);
            doc.fontSize(size).text(txt, c.x + 2, y + 4 + (7 - size) / 2, {
                width: c.w - 4, align: 'left', lineBreak: false,
            });
            doc.fontSize(7);
        } else {
            doc.text(fitText(doc, txt, c.w - 4), c.x + 2, y + 4, {
                width: c.w - 4,
                align: c.align === 'right' ? 'right' : 'left',
                lineBreak: false,
            });
        }
    }
    return y + FQ_ROW_H;
}

function drawFqTotals(doc, cols, y, totals) {
    doc.lineWidth(0.5);
    for (const c of cols) doc.rect(c.x, y, c.w, FQ_ROW_H).fillAndStroke('#f2f2f2', '#000');
    doc.fillColor('#000').font('Helvetica-Bold').fontSize(7);
    const byKey = {
        orderedUnits: fmtInt(totals.orderedUnits),
        noOfCartons: fmtInt(totals.noOfCartons),
        totalCbm: fmtNumber(totals.totalCbm, 4),
        totalWeight: fmtNumber(totals.totalWeight, 2),
    };
    for (const c of cols) {
        let txt = '';
        if (c.key === 'sku') txt = 'TOTAL';
        else if (byKey[c.key] != null) txt = byKey[c.key];
        if (!txt) continue;
        doc.text(txt, c.x + 2, y + 4, { width: c.w - 4, align: c.align === 'right' ? 'right' : 'left' });
    }
    return y + FQ_ROW_H;
}

// draft: { name, date, originPorts[], comments }
// lines: [{ sku, jfCode, orderedUnits, cartonWeight, unitsPerCarton,
//           cartonH, cartonL, cartonW, cartonCbm, noOfCartons, totalCbm,
//           supplier, supplierCountry, port, poNumber }]
// (Total Weight column is derived: cartonWeight × noOfCartons.)
async function buildForwarderQuotePdf(draft, lines) {
    const doc = new PDFDocument({ size: 'A4', layout: 'landscape', margin: 25 });
    const chunks = [];
    doc.on('data', c => chunks.push(c));
    const done = new Promise(resolve => doc.on('end', () => resolve(Buffer.concat(chunks))));

    const cols = fqCols();
    drawFqHeaderBlock(doc, draft);
    let y = drawFqTableHeader(doc, cols, 100);

    const totals = { orderedUnits: 0, noOfCartons: 0, totalCbm: 0, totalWeight: 0 };
    const newPage = () => {
        doc.addPage({ size: 'A4', layout: 'landscape', margin: 25 });
        y = drawFqTableHeader(doc, cols, 25);
    };

    for (const line of lines) {
        if (y + FQ_ROW_H > FQ_PAGE_BOTTOM) newPage();
        y = drawFqRow(doc, cols, y, line);
        totals.orderedUnits += Number(line.orderedUnits || 0);
        totals.noOfCartons  += Number(line.noOfCartons || 0);
        totals.totalCbm     += Number(line.totalCbm || 0);
        totals.totalWeight  += Number(line.cartonWeight || 0) * Number(line.noOfCartons || 0);
    }

    if (y + FQ_ROW_H > FQ_PAGE_BOTTOM) newPage();
    y = drawFqTotals(doc, cols, y, totals);

    if (draft.comments) {
        const cy = y + 15;
        const boxH = 50;
        if (cy + boxH > FQ_PAGE_BOTTOM) { newPage(); }
        const top = cy + boxH > FQ_PAGE_BOTTOM ? 25 : cy;
        doc.lineWidth(0.5).rect(25, top, FQ_TABLE_WIDTH, boxH).fillAndStroke('#f2f2f2', '#000');
        doc.fillColor('#000').font('Helvetica-Bold').fontSize(9).text('Comments:', 30, top + 8);
        doc.font('Helvetica').fontSize(9).text(draft.comments, 30, top + 24, { width: FQ_TABLE_WIDTH - 10, height: 22 });
    }

    doc.end();
    return done;
}

// ── Supplier quote (detailed, landscape) ─────────────────────────────────
// A near-copy of the forwarder quote, filtered upstream to a single supplier.
// Deliberately duplicated rather than sharing the forwarder helpers: the two
// templates are expected to diverge, so each owns its own layout/columns.

const SQ_TABLE_LEFT = 25;
const SQ_HEADER_H = 34;
const SQ_ROW_H = 16;
const SQ_PAGE_BOTTOM = 560; // landscape A4 height 595 − bottom margin

const SQ_COLS = [
    { key: 'sku',             header: 'SKU',                w: 90,  align: 'left'  },
    { key: 'jfCode',          header: 'HW/JF Code',         w: 50,  align: 'left'  },
    { key: 'lot',             header: 'LOT',                w: 64,  align: 'left'  },
    { key: 'orderedUnits',    header: 'Ordered Units',      w: 46,  align: 'right', fmt: fmtInt },
    { key: 'cartonWeight',    header: 'Carton Weight (kg)', w: 46,  align: 'right', fmt: v => fmtNumber(v, 2) },
    { key: 'unitsPerCarton',  header: 'Units per Carton',   w: 42,  align: 'right', fmt: fmtInt },
    { key: 'cartonH',         header: 'Carton H (cm)',      w: 36,  align: 'right', fmt: v => fmtNumber(v, 1) },
    { key: 'cartonL',         header: 'Carton L (cm)',      w: 36,  align: 'right', fmt: v => fmtNumber(v, 1) },
    { key: 'cartonW',         header: 'Carton W (cm)',      w: 36,  align: 'right', fmt: v => fmtNumber(v, 1) },
    { key: 'cartonCbm',       header: 'Carton CBM',         w: 48,  align: 'right', fmt: v => fmtNumber(v, 4) },
    { key: 'noOfCartons',     header: 'No. of Cartons',     w: 40,  align: 'right', fmt: fmtInt },
    { key: 'totalCbm',        header: 'Total CBM',          w: 48,  align: 'right', fmt: v => fmtNumber(v, 4) },
    { key: 'totalWeight',     header: 'Total Weight (kg)',  w: 44,  align: 'right', fmt: v => fmtNumber(v, 2),
      compute: l => Number(l.cartonWeight || 0) * Number(l.noOfCartons || 0) },
    { key: 'supplier',        header: 'Supplier',           w: 70,  align: 'left'  },
    { key: 'port',            header: 'Port',               w: 50,  align: 'left'  },
    { key: 'poNumber',        header: 'PO Number',          w: 46,  align: 'left'  },
];
const SQ_TABLE_WIDTH = SQ_COLS.reduce((s, c) => s + c.w, 0);

function sqCols() {
    let x = SQ_TABLE_LEFT;
    return SQ_COLS.map(c => { const col = { ...c, x }; x += c.w; return col; });
}

function drawSqHeaderBlock(doc, draft) {
    doc.fillColor('#000').fontSize(18).font('Helvetica-Bold').text('SUPPLIER QUOTE REQUEST', 25, 30);

    doc.fontSize(9).font('Helvetica-Bold')
       .text('Ref:', 25, 60, { continued: true }).font('Helvetica').text(`   ${draft.name || ''}`);
    doc.font('Helvetica-Bold')
       .text('Date:', 25, 74, { continued: true }).font('Helvetica').text(`   ${fmtDate(draft.date)}`);

    doc.font('Helvetica-Bold')
       .text('Supplier:', 320, 60, { continued: true }).font('Helvetica').text(`   ${draft.supplier || ''}`, { width: 300 });
    const origin = (draft.originPorts && draft.originPorts.length) ? draft.originPorts.join(', ') : '—';
    doc.font('Helvetica-Bold')
       .text('Goods From:', 320, 74, { continued: true }).font('Helvetica').text(`   ${origin}`, { width: 300 });
}

function drawSqTableHeader(doc, cols, y) {
    doc.lineWidth(0.5);
    for (const c of cols) doc.rect(c.x, y, c.w, SQ_HEADER_H).fillAndStroke('#e6e6e6', '#000');
    doc.fillColor('#000').font('Helvetica-Bold').fontSize(6.5);
    for (const c of cols) {
        doc.text(c.header, c.x + 2, y + 3, { width: c.w - 4, align: c.align === 'right' ? 'right' : 'left' });
    }
    return y + SQ_HEADER_H;
}

function drawSqRow(doc, cols, y, line) {
    doc.lineWidth(0.5);
    for (const c of cols) doc.rect(c.x, y, c.w, SQ_ROW_H).stroke('#000');
    doc.font('Helvetica').fontSize(7).fillColor('#000');
    for (const c of cols) {
        const raw = c.compute ? c.compute(line) : line[c.key];
        const txt = c.fmt ? c.fmt(raw) : (raw == null ? '' : String(raw));
        if (c.key === 'lot') {
            // Lot numbers are never truncated — shrink the font to fit instead.
            const size = fitFontSize(doc, txt, c.w - 4, 7);
            doc.fontSize(size).text(txt, c.x + 2, y + 4 + (7 - size) / 2, {
                width: c.w - 4, align: 'left', lineBreak: false,
            });
            doc.fontSize(7);
        } else {
            doc.text(fitText(doc, txt, c.w - 4), c.x + 2, y + 4, {
                width: c.w - 4,
                align: c.align === 'right' ? 'right' : 'left',
                lineBreak: false,
            });
        }
    }
    return y + SQ_ROW_H;
}

function drawSqTotals(doc, cols, y, totals) {
    doc.lineWidth(0.5);
    for (const c of cols) doc.rect(c.x, y, c.w, SQ_ROW_H).fillAndStroke('#f2f2f2', '#000');
    doc.fillColor('#000').font('Helvetica-Bold').fontSize(7);
    const byKey = {
        orderedUnits: fmtInt(totals.orderedUnits),
        noOfCartons: fmtInt(totals.noOfCartons),
        totalCbm: fmtNumber(totals.totalCbm, 4),
        totalWeight: fmtNumber(totals.totalWeight, 2),
    };
    for (const c of cols) {
        let txt = '';
        if (c.key === 'sku') txt = 'TOTAL';
        else if (byKey[c.key] != null) txt = byKey[c.key];
        if (!txt) continue;
        doc.text(txt, c.x + 2, y + 4, { width: c.w - 4, align: c.align === 'right' ? 'right' : 'left' });
    }
    return y + SQ_ROW_H;
}

// draft: { name, date, supplier, originPorts[], comments }
// lines: same shape as buildForwarderQuotePdf (already filtered to one supplier).
// (Total Weight column is derived: cartonWeight × noOfCartons.)
async function buildSupplierQuotePdf(draft, lines) {
    const doc = new PDFDocument({ size: 'A4', layout: 'landscape', margin: 25 });
    const chunks = [];
    doc.on('data', c => chunks.push(c));
    const done = new Promise(resolve => doc.on('end', () => resolve(Buffer.concat(chunks))));

    const cols = sqCols();
    drawSqHeaderBlock(doc, draft);
    let y = drawSqTableHeader(doc, cols, 100);

    const totals = { orderedUnits: 0, noOfCartons: 0, totalCbm: 0, totalWeight: 0 };
    const newPage = () => {
        doc.addPage({ size: 'A4', layout: 'landscape', margin: 25 });
        y = drawSqTableHeader(doc, cols, 25);
    };

    for (const line of lines) {
        if (y + SQ_ROW_H > SQ_PAGE_BOTTOM) newPage();
        y = drawSqRow(doc, cols, y, line);
        totals.orderedUnits += Number(line.orderedUnits || 0);
        totals.noOfCartons  += Number(line.noOfCartons || 0);
        totals.totalCbm     += Number(line.totalCbm || 0);
        totals.totalWeight  += Number(line.cartonWeight || 0) * Number(line.noOfCartons || 0);
    }

    if (y + SQ_ROW_H > SQ_PAGE_BOTTOM) newPage();
    y = drawSqTotals(doc, cols, y, totals);

    if (draft.comments) {
        const cy = y + 15;
        const boxH = 50;
        if (cy + boxH > SQ_PAGE_BOTTOM) { newPage(); }
        const top = cy + boxH > SQ_PAGE_BOTTOM ? 25 : cy;
        doc.lineWidth(0.5).rect(25, top, SQ_TABLE_WIDTH, boxH).fillAndStroke('#f2f2f2', '#000');
        doc.fillColor('#000').font('Helvetica-Bold').fontSize(9).text('Comments:', 30, top + 8);
        doc.font('Helvetica').fontSize(9).text(draft.comments, 30, top + 24, { width: SQ_TABLE_WIDTH - 10, height: 22 });
    }

    doc.end();
    return done;
}

// ── Quality Assurance (detailed, landscape) ──────────────────────────────
// A QC inspection sheet built from an explicit set of order lines (not a
// draft container). Same carton-level layout as the forwarder quote, plus
// LOT, Supplier Country and Terms columns, and a trailing "QC Units" column
// holding the per-order quantity to inspect. 18 columns, so widths are
// tighter than the forwarder quote and the totals row drops Total Weight.

const QA_TABLE_LEFT = 25;
const QA_HEADER_H = 34;
const QA_ROW_H = 16;
const QA_PAGE_BOTTOM = 560; // landscape A4 height 595 − bottom margin

const QA_COLS = [
    { key: 'sku',             header: 'SKU',                w: 84, align: 'left'  },
    { key: 'jfCode',          header: 'HW/JF Code',         w: 42, align: 'left'  },
    { key: 'orderedUnits',    header: 'Ordered Units',      w: 40, align: 'right', fmt: fmtInt },
    { key: 'lot',             header: 'LOT',                w: 64, align: 'left'  },
    { key: 'cartonWeight',    header: 'Carton Weight (kg)', w: 40, align: 'right', fmt: v => fmtNumber(v, 2) },
    { key: 'unitsPerCarton',  header: 'Units per Carton',   w: 38, align: 'right', fmt: fmtInt },
    { key: 'cartonH',         header: 'Carton H (cm)',      w: 30, align: 'right', fmt: v => fmtNumber(v, 1) },
    { key: 'cartonL',         header: 'Carton L (cm)',      w: 30, align: 'right', fmt: v => fmtNumber(v, 1) },
    { key: 'cartonW',         header: 'Carton W (cm)',      w: 30, align: 'right', fmt: v => fmtNumber(v, 1) },
    { key: 'cartonCbm',       header: 'Carton CBM',         w: 44, align: 'right', fmt: v => fmtNumber(v, 4) },
    { key: 'noOfCartons',     header: 'No. of Cartons',     w: 38, align: 'right', fmt: fmtInt },
    { key: 'totalCbm',        header: 'Total CBM',          w: 44, align: 'right', fmt: v => fmtNumber(v, 4) },
    { key: 'supplier',        header: 'Supplier',           w: 62, align: 'left'  },
    { key: 'supplierCountry', header: 'Supplier Country',   w: 46, align: 'left'  },
    { key: 'port',            header: 'Port',               w: 48, align: 'left'  },
    { key: 'terms',           header: 'Terms',              w: 34, align: 'left'  },
    { key: 'poNumber',        header: 'PO Number',          w: 44, align: 'left'  },
    { key: 'qcUnits',         header: 'QC Units',           w: 34, align: 'right', fmt: fmtInt },
];
const QA_TABLE_WIDTH = QA_COLS.reduce((s, c) => s + c.w, 0);

function qaCols() {
    let x = QA_TABLE_LEFT;
    return QA_COLS.map(c => { const col = { ...c, x }; x += c.w; return col; });
}

function drawQaHeaderBlock(doc, meta) {
    doc.fillColor('#000').fontSize(18).font('Helvetica-Bold').text('QUALITY ASSURANCE', 25, 30);

    doc.fontSize(9).font('Helvetica-Bold')
       .text('Ref:', 25, 60, { continued: true }).font('Helvetica').text(`   ${meta.name || ''}`);
    doc.font('Helvetica-Bold')
       .text('Date:', 25, 74, { continued: true }).font('Helvetica').text(`   ${fmtDate(meta.date)}`);
}

function drawQaTableHeader(doc, cols, y) {
    doc.lineWidth(0.5);
    for (const c of cols) doc.rect(c.x, y, c.w, QA_HEADER_H).fillAndStroke('#e6e6e6', '#000');
    doc.fillColor('#000').font('Helvetica-Bold').fontSize(6.5);
    for (const c of cols) {
        doc.text(c.header, c.x + 2, y + 3, { width: c.w - 4, align: c.align === 'right' ? 'right' : 'left' });
    }
    return y + QA_HEADER_H;
}

function drawQaRow(doc, cols, y, line) {
    doc.lineWidth(0.5);
    for (const c of cols) doc.rect(c.x, y, c.w, QA_ROW_H).stroke('#000');
    doc.font('Helvetica').fontSize(7).fillColor('#000');
    for (const c of cols) {
        const raw = c.compute ? c.compute(line) : line[c.key];
        const txt = c.fmt ? c.fmt(raw) : (raw == null ? '' : String(raw));
        if (c.key === 'lot') {
            // Lot numbers are never truncated — shrink the font to fit instead.
            const size = fitFontSize(doc, txt, c.w - 4, 7);
            doc.fontSize(size).text(txt, c.x + 2, y + 4 + (7 - size) / 2, {
                width: c.w - 4, align: 'left', lineBreak: false,
            });
            doc.fontSize(7);
        } else {
            doc.text(fitText(doc, txt, c.w - 4), c.x + 2, y + 4, {
                width: c.w - 4,
                align: c.align === 'right' ? 'right' : 'left',
                lineBreak: false,
            });
        }
    }
    return y + QA_ROW_H;
}

function drawQaTotals(doc, cols, y, totals) {
    doc.lineWidth(0.5);
    for (const c of cols) doc.rect(c.x, y, c.w, QA_ROW_H).fillAndStroke('#f2f2f2', '#000');
    doc.fillColor('#000').font('Helvetica-Bold').fontSize(7);
    const byKey = {
        orderedUnits: fmtInt(totals.orderedUnits),
        noOfCartons: fmtInt(totals.noOfCartons),
        totalCbm: fmtNumber(totals.totalCbm, 4),
        qcUnits: fmtInt(totals.qcUnits),
    };
    for (const c of cols) {
        let txt = '';
        if (c.key === 'sku') txt = 'TOTAL';
        else if (byKey[c.key] != null) txt = byKey[c.key];
        if (!txt) continue;
        doc.text(txt, c.x + 2, y + 4, { width: c.w - 4, align: c.align === 'right' ? 'right' : 'left' });
    }
    return y + QA_ROW_H;
}

// meta: { name, date, comments }
// lines: [{ sku, jfCode, orderedUnits, lot, cartonWeight, unitsPerCarton,
//           cartonH, cartonL, cartonW, cartonCbm, noOfCartons, totalCbm,
//           supplier, supplierCountry, port, terms, poNumber, qcUnits }]
async function buildQualityAssurancePdf(meta, lines) {
    const doc = new PDFDocument({ size: 'A4', layout: 'landscape', margin: 25 });
    const chunks = [];
    doc.on('data', c => chunks.push(c));
    const done = new Promise(resolve => doc.on('end', () => resolve(Buffer.concat(chunks))));

    const cols = qaCols();
    drawQaHeaderBlock(doc, meta);
    let y = drawQaTableHeader(doc, cols, 100);

    const totals = { orderedUnits: 0, noOfCartons: 0, totalCbm: 0, qcUnits: 0 };
    const newPage = () => {
        doc.addPage({ size: 'A4', layout: 'landscape', margin: 25 });
        y = drawQaTableHeader(doc, cols, 25);
    };

    for (const line of lines) {
        if (y + QA_ROW_H > QA_PAGE_BOTTOM) newPage();
        y = drawQaRow(doc, cols, y, line);
        totals.orderedUnits += Number(line.orderedUnits || 0);
        totals.noOfCartons  += Number(line.noOfCartons || 0);
        totals.totalCbm     += Number(line.totalCbm || 0);
        totals.qcUnits      += Number(line.qcUnits || 0);
    }

    if (y + QA_ROW_H > QA_PAGE_BOTTOM) newPage();
    y = drawQaTotals(doc, cols, y, totals);

    if (meta.comments) {
        const cy = y + 15;
        const boxH = 50;
        if (cy + boxH > QA_PAGE_BOTTOM) { newPage(); }
        const top = cy + boxH > QA_PAGE_BOTTOM ? 25 : cy;
        doc.lineWidth(0.5).rect(25, top, QA_TABLE_WIDTH, boxH).fillAndStroke('#f2f2f2', '#000');
        doc.fillColor('#000').font('Helvetica-Bold').fontSize(9).text('Comments:', 30, top + 8);
        doc.font('Helvetica').fontSize(9).text(meta.comments, 30, top + 24, { width: QA_TABLE_WIDTH - 10, height: 22 });
    }

    doc.end();
    return done;
}

// ── CSV companions ───────────────────────────────────────────────────────
// Each PDF above is emailed with a CSV sibling so recipients (forwarders,
// suppliers, QC inspectors) can parse the figures without re-keying the PDF.
// The CSVs reuse the very same column definitions + formatters as the PDFs,
// so the two stay in lockstep: change a column here and both formats follow.
// No totals row — one header row + one row per line keeps it cleanly parseable.

// Columns for the simple "Delivery Quote Request" (buildDraftContainerPdf),
// which draws from inline `line.*` fields rather than a COLS array.
const QUOTE_CSV_COLS = [
    { key: 'sku',      header: 'Ref' },
    { key: 'name',     header: 'Item' },
    { key: 'lot',      header: 'Lot' },
    { key: 'quantity', header: 'Quantity', fmt: fmtInt },
    { key: 'cbm',      header: 'CBM',      fmt: v => fmtNumber(v, 3) },
    { key: 'cartons',  header: 'Cartons',  fmt: fmtInt },
];

// Quote a single CSV field per RFC 4180: wrap in double quotes (and double any
// embedded quotes) when it contains a comma, quote, CR or LF.
function csvEscape(value) {
    const s = value == null ? '' : String(value);
    return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

// Resolve one column's cell for a line, applying the same compute/fmt the PDF
// uses. The PDF's '-' placeholder for missing numbers becomes an empty cell so
// the CSV parses as a true blank rather than a literal dash.
function csvCell(col, line) {
    const raw = col.compute ? col.compute(line) : line[col.key];
    let txt = col.fmt ? col.fmt(raw) : (raw == null ? '' : String(raw));
    if (txt === '-') txt = '';
    return txt;
}

// U+FEFF byte-order mark, prepended so Excel opens the CSV as UTF-8.
const CSV_BOM = String.fromCharCode(0xFEFF);

// Build a CSV string from a COLS array + lines. Prepends the UTF-8 BOM and uses
// CRLF line endings (Excel-friendly, RFC 4180).
function colsToCsv(cols, lines) {
    const header = cols.map(c => csvEscape(c.header)).join(',');
    const body = (lines || []).map(line => cols.map(c => csvEscape(csvCell(c, line))).join(','));
    return Buffer.from(CSV_BOM + [header, ...body].join('\r\n') + '\r\n', 'utf8');
}

// Same (draft/meta, lines) signatures as the PDF builders, but synchronous —
// a CSV is just string assembly, no PDFKit stream to await.
function buildDraftContainerCsv(draft, lines) { return colsToCsv(QUOTE_CSV_COLS, lines); }
function buildForwarderQuoteCsv(draft, lines) { return colsToCsv(FQ_COLS, lines); }
function buildSupplierQuoteCsv(draft, lines) { return colsToCsv(SQ_COLS, lines); }
function buildQualityAssuranceCsv(meta, lines) { return colsToCsv(QA_COLS, lines); }

module.exports = {
    buildDraftContainerPdf, buildForwarderQuotePdf, buildSupplierQuotePdf, buildQualityAssurancePdf,
    buildDraftContainerCsv, buildForwarderQuoteCsv, buildSupplierQuoteCsv, buildQualityAssuranceCsv,
};
