'use strict';

// Renders a Purchase Order PDF mirroring the layout of the manually-issued
// POs: header block (PO Ref, Order Date), three-column address block, line-
// items table, totals box, comments box.
//
// Pagination: long line-items split across pages. The table header is
// re-drawn on every page; totals + comments always land together on the
// last page, on a new page if they wouldn't fit alongside the last row.

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
// Fallback when a PO has no company_id linked. Real customer comes from the
// joined `companies` row (po.company.name + addressLines + country).
const DEFAULT_CUSTOMER = {
    name: '–',
    addressLines: ['Unit B Prestige House', 'Cornford Road', 'Blackpool', 'Lancashire', 'FY4 4QQ'],
    country: 'UNITED KINGDOM',
};

// A4 portrait, 50pt margin → usable area 50..545 (width 495), 50..792 (height 742)
const PAGE_TOP = 50;
const PAGE_BOTTOM = 792;
const TABLE_LEFT = 50;
const TABLE_WIDTH = 490;
const ROW_H = 28;
const COLS = [
    { header: 'Ref',        x: 50,  w: 70,  align: 'center' },
    { header: 'Item',       x: 120, w: 200, align: 'center' },
    { header: 'Quantity',   x: 320, w: 60,  align: 'center' },
    { header: 'Unit Price', x: 380, w: 70,  align: 'center' },
    { header: 'Line Price', x: 450, w: 70,  align: 'center' },
];

// Product names can run long. We allow up to 75 characters, wrapped across as
// many lines as the Item column needs, and grow the row height to fit so the
// text stays inside its box instead of bleeding into the row below.
const ITEM_COL = COLS[1];
const MAX_NAME_LEN = 75;
const ROW_PAD_Y = 8; // vertical padding above/below the wrapped item text

function truncateName(name) {
    const s = String(name || '');
    if (s.length <= MAX_NAME_LEN) return s;
    return s.slice(0, MAX_NAME_LEN - 1).trimEnd() + '…';
}

// Height a row needs so its (possibly multi-line) item name fits with padding.
// Never shorter than ROW_H, so short names keep the original single-line look.
function rowHeight(doc, line) {
    doc.font('Helvetica').fontSize(10);
    const nameH = doc.heightOfString(truncateName(line.name) || ' ', {
        width: ITEM_COL.w,
        align: ITEM_COL.align,
    });
    return Math.max(ROW_H, Math.ceil(nameH) + ROW_PAD_Y * 2);
}

function fmtMoney(n, dp = 2) {
    if (n === null || n === undefined || isNaN(n)) return '0.00';
    return Number(n).toFixed(dp);
}

function fmtDate(d) {
    if (!d) return '';
    const dt = d instanceof Date ? d : new Date(d);
    if (isNaN(dt.getTime())) return '';
    const dd = String(dt.getDate()).padStart(2, '0');
    const mm = String(dt.getMonth() + 1).padStart(2, '0');
    const yyyy = dt.getFullYear();
    return `${dd}/${mm}/${yyyy}`;
}

function drawHeaderBlock(doc, po) {
    // Title
    doc.fontSize(22).font('Helvetica-Bold').text('PURCHASE ORDER', 50, 60);

    // PO Ref / Order Date
    doc.fontSize(10).font('Helvetica-Bold');
    doc.text('PO Ref:', 50, 110, { continued: true })
       .font('Helvetica').text(`   ${po.poNumber || ''}`);
    doc.font('Helvetica-Bold')
       .text('Order Date:', 50, 125, { continued: true })
       .font('Helvetica').text(`   ${fmtDate(po.orderDate)}`);

    // Three-column address block
    const addrTop = 165;
    doc.fontSize(10).font('Helvetica-Bold').text('Supplier Details:', 50, addrTop);
    doc.text(po.supplier || '-', 50, addrTop + 15);
    let y = addrTop + 30;
    for (const line of (po.supplierAddress || [])) {
        doc.font('Helvetica').text(line, 50, y);
        y += 12;
    }

    doc.font('Helvetica-Bold').text('Delivery Details:', 280, addrTop);
    y = addrTop + 15;
    for (const line of DELIVERY_ADDRESS) {
        doc.font(DELIVERY_ADDRESS.indexOf(line) === 0 ? 'Helvetica-Bold' : 'Helvetica')
           .text(line || ' ', 280, y, { width: 130, align: 'right' });
        y += 12;
    }

    doc.font('Helvetica-Bold').text('Customer Details:', 430, addrTop);
    const customer = po.customer || DEFAULT_CUSTOMER;
    const customerLines = [
        customer.name || '–',
        ...(customer.addressLines || []),
        customer.country || '',
    ];
    y = addrTop + 15;
    for (const line of customerLines) {
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

function drawRow(doc, y, line, rowH) {
    doc.lineWidth(0.5).rect(TABLE_LEFT, y, TABLE_WIDTH, rowH).stroke('#000');
    const qty = Number(line.quantity || 0);
    const unit = Number(line.unitPrice || 0);
    const lineTotal = qty * unit;
    const name = truncateName(line.name);

    doc.font('Helvetica').fontSize(10).fillColor('#000');
    // Vertically centre single-line cells, and the wrapped name block, within
    // the (possibly grown) row height.
    const lineH = doc.heightOfString('X');
    const nameH = doc.heightOfString(name || ' ', { width: ITEM_COL.w, align: ITEM_COL.align });
    const singleY = y + (rowH - lineH) / 2;
    const nameY = y + (rowH - nameH) / 2;

    doc.text(line.sku || '',          50,  singleY, { width: 70,  align: 'center' });
    doc.text(name,                    120, nameY,   { width: 200, align: 'center' });
    doc.text(String(qty),             320, singleY, { width: 60,  align: 'center' });
    doc.text(fmtMoney(unit, 5),       380, singleY, { width: 70,  align: 'center' });
    doc.text(fmtMoney(lineTotal, 2),  450, singleY, { width: 70,  align: 'center' });
    return { yNext: y + rowH, lineTotal };
}

function drawTotalsBox(doc, y, currency, shipping, subtotal) {
    const totalsX = 380;
    const labelW = 80;
    const valueW = 80;
    const grandTotal = subtotal + shipping;

    const drawTotalRow = (label, value, by, isBold = false) => {
        doc.lineWidth(0.5).rect(totalsX, by, labelW, ROW_H).stroke('#000');
        doc.lineWidth(0.5).rect(totalsX + labelW, by, valueW, ROW_H).stroke('#000');
        doc.font(isBold ? 'Helvetica-Bold' : 'Helvetica').fontSize(10).fillColor('#000');
        doc.text(label, totalsX + 5, by + 9, { width: labelW - 10, align: 'left' });
        doc.text(value, totalsX + labelW + 5, by + 9, { width: valueW - 10, align: 'left' });
    };
    drawTotalRow('Currency',       currency || 'USD',          y);
    drawTotalRow('Shipping Total', fmtMoney(shipping, 2),      y + ROW_H);
    drawTotalRow('Total Cost',     fmtMoney(grandTotal, 2),    y + ROW_H * 2, true);
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

// po: { poNumber, orderDate, supplier, supplierAddress[], currency, shippingTotal, comments }
// lines: [{ sku, name, quantity, unitPrice }]
async function buildPoPdf(po, lines) {
    const doc = new PDFDocument({ size: 'A4', margin: 50 });
    const chunks = [];
    doc.on('data', c => chunks.push(c));
    const done = new Promise(resolve => doc.on('end', () => resolve(Buffer.concat(chunks))));

    // Page 1: header block + table starts at y=320.
    drawHeaderBlock(doc, po);
    let y = drawTableHeader(doc, 320);
    let subtotal = 0;

    const startNewPage = () => {
        doc.addPage();
        y = drawTableHeader(doc, PAGE_TOP);
    };

    for (const line of lines) {
        const rowH = rowHeight(doc, line);
        // If this row (which may be taller than ROW_H for a long name) would
        // overflow the page, paginate before drawing it.
        if (y + rowH > PAGE_BOTTOM) startNewPage();
        const { yNext, lineTotal } = drawRow(doc, y, line, rowH);
        subtotal += lineTotal;
        y = yNext;
    }

    // Totals + comments must stay together on the last page. If they don't fit
    // alongside the last row, push them to a new page.
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

    y = drawTotalsBox(doc, y, po.currency, Number(po.shippingTotal || 0), subtotal);
    y = drawCommentsBox(doc, y + blockGap, po.comments);

    doc.end();
    return done;
}

module.exports = { buildPoPdf };
