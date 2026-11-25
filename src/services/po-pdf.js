'use strict';

// Renders a Purchase Order PDF mirroring the layout of the manually-issued
// POs: header block (PO Ref, Order Date), three-column address block, line-
// items table, totals box, comments box.
//
// A SAMPLE order (po.kind 'sample', docs/sample-orders.md) prints the same
// page with three differences: it is headed SAMPLE ORDER; each line names the
// sample code the supplier should write on the parcel; and a sample with no
// price is not priced on the paper at all (the user, 2026-11-25: "exclude the
// price column dont say free"):
//   - no line has a price: the table has no Unit Price and Line Price columns
//     (the item takes their room), and with no shipping charge either there is
//     no totals box. Nothing says "free", and nothing prints a 0;
//   - some lines have a price: the columns stay, and an unpriced line's two
//     cells are left empty.
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
// The table of a sample order none of whose lines has a price: the same width,
// with the two price columns' room given to the item.
const COLS_NO_PRICE = [
    { header: 'Ref',      x: 50,  w: 70,  align: 'center' },
    { header: 'Item',     x: 120, w: 340, align: 'center' },
    { header: 'Quantity', x: 460, w: 80,  align: 'center' },
];
const MAX_NAME_LEN = 75;
const ROW_PAD_Y = 8; // vertical padding above/below the wrapped item text

// Header address block: the supplier column runs from x=50 to the delivery
// column at x=280, less a 10pt gutter.
const SUPPLIER_COL_W = 220;

function truncateName(name) {
    const s = String(name || '');
    if (s.length <= MAX_NAME_LEN) return s;
    return s.slice(0, MAX_NAME_LEN - 1).trimEnd() + '…';
}

// What the Item cell holds: the name, and under it the sample code of a sample line.
function itemText(line) {
    const name = truncateName(line.name);
    return line.sampleCode ? `${name}\nSample code: ${line.sampleCode}` : name;
}

const isSamplePo = po => Boolean(po) && po.kind === 'sample';
const hasPrice = line => Number(line.unitPrice || 0) > 0;
// A line of a sample order with no price: nothing is printed in its price cells.
const isUnpricedLine = (po, line) => isSamplePo(po) && !hasPrice(line);

// The columns this order's table has. Every order has all five, except a
// sample order with no priced line, which has no price columns.
function tableFor(po, lines) {
    const priced = !isSamplePo(po) || lines.some(hasPrice);
    const cols = priced ? COLS : COLS_NO_PRICE;
    return { priced, cols, item: cols[1], qty: cols[2] };
}

// Height a row needs so its (possibly multi-line) item name fits with padding.
// Never shorter than ROW_H, so short names keep the original single-line look.
function rowHeight(doc, line, itemCol = ITEM_COL) {
    doc.font('Helvetica').fontSize(10);
    const nameH = doc.heightOfString(itemText(line) || ' ', {
        width: itemCol.w,
        align: itemCol.align,
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
    doc.fontSize(22).font('Helvetica-Bold').text(isSamplePo(po) ? 'SAMPLE ORDER' : 'PURCHASE ORDER', 50, 60);

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
    // Supplier names can run long. Wrap them inside the supplier column so they
    // never run across the delivery address, and start the address below
    // however many lines the name took (one line keeps the original spacing).
    const supplierName = po.supplier || '-';
    doc.text(supplierName, 50, addrTop + 15, { width: SUPPLIER_COL_W });
    const supplierNameH = doc.heightOfString(supplierName, { width: SUPPLIER_COL_W });
    let y = addrTop + 15 + Math.ceil(supplierNameH) + 3;
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

function drawTableHeader(doc, y, cols = COLS) {
    doc.lineWidth(0.5).rect(TABLE_LEFT, y, TABLE_WIDTH, ROW_H).fillAndStroke('#e6e6e6', '#000');
    doc.fillColor('#000').font('Helvetica-Bold').fontSize(10);
    for (const col of cols) {
        doc.text(col.header, col.x, y + 9, { width: col.w, align: col.align });
    }
    return y + ROW_H;
}

// `table`: tableFor(...). `unpriced`: a sample line with no price, whose price
// cells stay empty where the table has them.
function drawRow(doc, y, line, rowH, table = { priced: true, item: ITEM_COL, qty: COLS[2] }, unpriced = false) {
    doc.lineWidth(0.5).rect(TABLE_LEFT, y, TABLE_WIDTH, rowH).stroke('#000');
    const qty = Number(line.quantity || 0);
    const unit = Number(line.unitPrice || 0);
    const lineTotal = qty * unit;
    const name = itemText(line);

    doc.font('Helvetica').fontSize(10).fillColor('#000');
    // Vertically centre single-line cells, and the wrapped name block, within
    // the (possibly grown) row height.
    const lineH = doc.heightOfString('X');
    const nameH = doc.heightOfString(name || ' ', { width: table.item.w, align: table.item.align });
    const singleY = y + (rowH - lineH) / 2;
    const nameY = y + (rowH - nameH) / 2;

    doc.text(line.sku || '',          50,  singleY, { width: 70,  align: 'center' });
    doc.text(name,                    table.item.x, nameY,   { width: table.item.w, align: 'center' });
    doc.text(String(qty),             table.qty.x,  singleY, { width: table.qty.w,  align: 'center' });
    // No price columns, or a sample line with no price: nothing is printed
    // there. Not a 0, and not a word.
    if (table.priced && !unpriced) {
        doc.text(fmtMoney(unit, 5),       380, singleY, { width: 70,  align: 'center' });
        doc.text(fmtMoney(lineTotal, 2),  450, singleY, { width: 70,  align: 'center' });
    }
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

// po: { poNumber, orderDate, supplier, supplierAddress[], currency, shippingTotal, comments, kind? }
// lines: [{ sku, name, quantity, unitPrice, sampleCode? }]
async function buildPoPdf(po, lines) {
    const doc = new PDFDocument({ size: 'A4', margin: 50 });
    const chunks = [];
    doc.on('data', c => chunks.push(c));
    const done = new Promise(resolve => doc.on('end', () => resolve(Buffer.concat(chunks))));

    // Page 1: header block + table starts at y=320.
    drawHeaderBlock(doc, po);
    const table = tableFor(po, lines);
    let y = drawTableHeader(doc, 320, table.cols);
    let subtotal = 0;

    const startNewPage = () => {
        doc.addPage();
        y = drawTableHeader(doc, PAGE_TOP, table.cols);
    };

    for (const line of lines) {
        const rowH = rowHeight(doc, line, table.item);
        // If this row (which may be taller than ROW_H for a long name) would
        // overflow the page, paginate before drawing it.
        if (y + rowH > PAGE_BOTTOM) startNewPage();
        const { yNext, lineTotal } = drawRow(doc, y, line, rowH, table, isUnpricedLine(po, line));
        subtotal += lineTotal;
        y = yNext;
    }

    // Totals + comments must stay together on the last page. If they don't fit
    // alongside the last row, push them to a new page.
    // A sample order with nothing to pay (no priced line, no shipping charge)
    // has no totals box, and nothing in its place.
    const shipping = Number(po.shippingTotal || 0);
    const nothingToPay = isSamplePo(po) && subtotal + shipping === 0;
    const totalsHeight = nothingToPay ? 0 : ROW_H * 3;
    const commentsHeight = 90;
    const blockGap = 30;
    const requiredSpace = (nothingToPay ? 0 : blockGap + totalsHeight) + blockGap + commentsHeight;
    if (y + requiredSpace > PAGE_BOTTOM) {
        doc.addPage();
        y = PAGE_TOP;
    } else if (!nothingToPay) {
        y += blockGap;
    }

    if (!nothingToPay) y = drawTotalsBox(doc, y, po.currency, shipping, subtotal);
    y = drawCommentsBox(doc, y + blockGap, po.comments);

    doc.end();
    return done;
}

module.exports = { buildPoPdf };
