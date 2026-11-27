'use strict';

// Clearance instructions (docs/clearance-instructions.md): the rows' check and
// the file. The golden case is the sheet our team typed by hand for SEKU6400902.
//   node --test tools/test-clearance-instructions-lib.js

const { test } = require('node:test');
const assert = require('node:assert/strict');
const CI = require('../src/lib/clearance-instructions');

const row = (over = {}) => ({
    supplier: 'Health&Beyond Health Technology(Suzhou)Co.Ltd', invoiceNumber: '26JFAA17002', company: 'JFA',
    description: 'Medical Sterile Lubricating Jelly JF0781', origin: 'China', cartons: 250, unitType: 'Carton', netKg: 1825, grossKg: 2125,
    cbm: 5.83, commodityCode: '3006700000', dutyPercent: 0, valueUsd: 8400, jfCodes: ['JF0781'],
    ...over,
});

// The ten rows of the hand-typed sheet (its stray spaces left out).
const KING = 'HUBEI  QIANJIANG  KINGPHAR  MEDICAL  MATERIAL  CO.  LTD.';
const SHEET = [
    row(),
    row({ supplier: KING, invoiceNumber: '0E8260759', description: 'Gauze Swabs JF1477 JF1475 JF1469', cartons: 836, netKg: 2842.4, grossKg: 3176.8, cbm: 39.607, commodityCode: '3005903100', valueUsd: 11411.4 }),
    row({ supplier: KING, invoiceNumber: '0E8260759', description: 'Microporous Tape JF1497', cartons: 80, netKg: 916, grossKg: 996, cbm: 2.663, commodityCode: '3005100000', valueUsd: 5402.88 }),
    row({ supplier: 'Roosin', invoiceNumber: 'No.00337J  ROOSIN-38', description: 'Burning Dressings JF1010', cartons: 88, netKg: 924, grossKg: 994.4, cbm: 3.232, commodityCode: '3005909900', valueUsd: 8272 }),
    row({ supplier: 'Roosin', invoiceNumber: 'No.00337J  ROOSIN-38', description: 'Paraffin Gauze Dressings JF0920 JF1049', cartons: 121, netKg: 522, grossKg: 677.6, cbm: 2.813, commodityCode: '3005903100', valueUsd: 11008 }),
    row({ supplier: 'Roosin', invoiceNumber: 'No.00337J  ROOSIN-38', description: 'Hydrocolloid Dressings JF1030', cartons: 75, netKg: 360, grossKg: 412.5, cbm: 1.741, commodityCode: '3005100000', valueUsd: 10200 }),
    row({ supplier: 'Roosin', invoiceNumber: 'No.00337J  ROOSIN-38', description: 'Calcium Alginate Wound Dressings JF1026', cartons: 33, netKg: 99, grossKg: 122.1, cbm: 0.937, commodityCode: '3005909900', valueUsd: 4752 }),
    row({ supplier: 'Roosin', invoiceNumber: 'No.00337J  ROOSIN-38', description: 'Foam Dressings JF1051', cartons: 12, netKg: 21.6, grossKg: 27.6, cbm: 0.335, commodityCode: '3005909900', valueUsd: 792 }),
    row({ supplier: 'Roosin', invoiceNumber: 'No.00337J  ROOSIN-38', description: 'Silicone Foam Dressing JF1361', cartons: 17, netKg: 100.3, grossKg: 110.5, cbm: 1.143, commodityCode: '3005100000', valueUsd: 3264 }),
    row({ supplier: 'YERCON DIAGNOSTIC CO.,LTD', invoiceNumber: '26YE5055', description: 'Urinalysis Test Strips JF1376', cartons: 50, netKg: 500, grossKg: 550, cbm: 2.45, commodityCode: '3822190000', valueUsd: 13250 }),
];

test('the file: the sheet as it has always been laid out, to the character', () => {
    const csv = CI.toCsv({ title: 'SEKU6400902', rows: SHEET, containerCostUsd: 3450 });
    assert.deepEqual(csv.split('\r\n'), [
        'SEKU6400902 - Clearance Instructions,,,,,,,,,,,,,,,,',
        // The heading row is the sheet's own, its misspelling and its stray space included: the forwarder knows it by these words.
        'Supplier,Commerical Invoice Number,JFA / HW, Description of goods [6/8],Country of Origin,Unit Count,Unit Type,Total NW (kg),Total GW (kg),Total Carton CBM,Commodity Code,Duty Percentage,Container Cost USD,Total Container CBM,Cost of Shipping,Value of Goods USD,',
        'Health&Beyond Health Technology(Suzhou)Co.Ltd,26JFAA17002,JFA,Medical Sterile Lubricating Jelly JF0781,China,250,Carton,1825,2125,5.830,3006700000,0.00%,3450,60.75,331,8400.00,',
        `${KING},0E8260759,JFA,Gauze Swabs JF1477 JF1475 JF1469,China,836,Carton,2842.4,3176.8,39.607,3005903100,0.00%,3450,60.75,2249,11411.40,`,
        `${KING},0E8260759,JFA,Microporous Tape JF1497,China,80,Carton,916,996,2.663,3005100000,0.00%,3450,60.75,151,5402.88,`,
        'Roosin,No.00337J  ROOSIN-38,JFA,Burning Dressings JF1010,China,88,Carton,924,994.4,3.232,3005909900,0.00%,3450,60.75,184,8272.00,',
        'Roosin,No.00337J  ROOSIN-38,JFA,Paraffin Gauze Dressings JF0920 JF1049,China,121,Carton,522,677.6,2.813,3005903100,0.00%,3450,60.75,160,11008.00,',
        'Roosin,No.00337J  ROOSIN-38,JFA,Hydrocolloid Dressings JF1030,China,75,Carton,360,412.5,1.741,3005100000,0.00%,3450,60.75,99,10200.00,',
        'Roosin,No.00337J  ROOSIN-38,JFA,Calcium Alginate Wound Dressings JF1026,China,33,Carton,99,122.1,0.937,3005909900,0.00%,3450,60.75,53,4752.00,',
        'Roosin,No.00337J  ROOSIN-38,JFA,Foam Dressings JF1051,China,12,Carton,21.6,27.6,0.335,3005909900,0.00%,3450,60.75,19,792.00,',
        'Roosin,No.00337J  ROOSIN-38,JFA,Silicone Foam Dressing JF1361,China,17,Carton,100.3,110.5,1.143,3005100000,0.00%,3450,60.75,65,3264.00,',
        '"YERCON DIAGNOSTIC CO.,LTD",26YE5055,JFA,Urinalysis Test Strips JF1376,China,50,Carton,500,550,2.450,3822190000,0.00%,3450,60.75,139,13250.00,',
        ',,,,,,,,,,,,,TOTAL CBM,60.75,,',
        ',,,,,,,,,,,,,Cost/CBM,56.7901,,',
        ',,,,,,,,,,,,,TOTAL,"USD 3,450.00",,',
        '',
    ]);
});

test('the file: a blank is an empty cell, a quote is doubled, and a line break never splits a row', () => {
    const csv = CI.toCsv({ title: 'ABCU1', rows: [row({ supplier: 'He said "hi"\r\nLtd', invoiceNumber: '', cartons: null, netKg: null, grossKg: null, cbm: null, dutyPercent: null, valueUsd: null, commodityCode: '' })], containerCostUsd: null });
    const lines = csv.split('\r\n');
    assert.equal(lines.length, 7);
    assert.equal(lines[2], '"He said ""hi"" Ltd",,JFA,Medical Sterile Lubricating Jelly JF0781,China,,Carton,,,,,,,0.00,,,');
    assert.deepEqual(lines.slice(3, 6), [',,,,,,,,,,,,,TOTAL CBM,0.00,,', ',,,,,,,,,,,,,Cost/CBM,,,', ',,,,,,,,,,,,,TOTAL,,,']);
    // A duty that is not a whole number, and a cost with pence.
    const odd = CI.toCsv({ title: 'ABCU1', rows: [row({ dutyPercent: 2.5 })], containerCostUsd: 3450.5 }).split('\r\n');
    assert.match(odd[2], /,2\.50%,3450\.50,5\.83,3451,8400\.00,$/);
    assert.equal(odd[5], ',,,,,,,,,,,,,TOTAL,"USD 3,450.50",,');
});

test('the name of the file, and of the container on it', () => {
    assert.equal(CI.filenameOf('SEKU6400902'), 'Clearance Instructions - SEKU6400902.csv');
    assert.equal(CI.filenameOf('121. Air/Freight: 7'), 'Clearance Instructions - 121. Air-Freight- 7.csv');
    // The carrier's number when the container has one, else our own.
    assert.equal(CI.titleOf({ trackingRef: ' seku6400902 ', reference: '339' }), 'SEKU6400902');
    assert.equal(CI.titleOf({ trackingRef: null, reference: '339' }), '339');
    assert.equal(CI.titleOf({ trackingRef: '', reference: null }), 'Container');
});

test('the cost is shared by volume: over the total to two places, at a rate to four, to the dollar', () => {
    const t = CI.totalsOf(SHEET, 3450);
    assert.equal(t.totalCbm, 60.75);
    assert.equal(t.costPerCbm, 56.7901);
    assert.deepEqual(t.shipping, [331, 2249, 151, 184, 160, 99, 53, 19, 65, 139]);
    assert.deepEqual(CI.totalsOf(SHEET, null), { totalCbm: 60.75, costPerCbm: null, shipping: SHEET.map(() => null) });
    assert.deepEqual(CI.totalsOf([row({ cbm: null })], 3450), { totalCbm: 0, costPerCbm: null, shipping: [null] });
});

test('what came in is cleaned: text trimmed, figures as numbers, nothing beyond the fields the file has', () => {
    const { rows, containerCostUsd, problems } = CI.check({
        containerCostUsd: '3450',
        rows: [{ ...row(), supplier: '  Roosin ', cartons: '250', netKg: '1825.004', cbm: 5.8304, dutyPercent: '0', valueUsd: '8400', jfCodes: ['jf0781', 7, ''], notes: ['x'], key: 'k', evil: '<script>' }],
    });
    assert.deepEqual(problems, []);
    assert.equal(containerCostUsd, 3450);
    assert.deepEqual(rows, [{
        supplier: 'Roosin', invoiceNumber: '26JFAA17002', company: 'JFA', description: 'Medical Sterile Lubricating Jelly JF0781', origin: 'China',
        cartons: 250, unitType: 'Carton', netKg: 1825, grossKg: 2125, cbm: 5.83, commodityCode: '3006700000', dutyPercent: 0, valueUsd: 8400, jfCodes: ['JF0781'],
    }]);
});

test('it is sent only complete: every blank and every figure that cannot be right is named, by its row', () => {
    assert.deepEqual(CI.check({ containerCostUsd: 3450, rows: SHEET }).problems, []);
    const bad = { supplier: '', invoiceNumber: ' ', company: 'XX', description: '', origin: '', cartons: 2.5, unitType: '', netKg: 0, grossKg: 'abc', cbm: -1, commodityCode: '30067', dutyPercent: '', valueUsd: null };
    const { problems, rows } = CI.check({ containerCostUsd: 0, rows: [row(), bad] });
    assert.deepEqual(problems.map(p => [p.row, p.field]), [
        [null, 'containerCostUsd'],
        [1, 'supplier'], [1, 'invoiceNumber'], [1, 'company'], [1, 'description'], [1, 'origin'], [1, 'cartons'], [1, 'netKg'], [1, 'grossKg'], [1, 'cbm'],
        [1, 'commodityCode'], [1, 'dutyPercent'], [1, 'valueUsd'],
    ]);
    // A figure that cannot be right is a blank on the file, never a wrong number.
    assert.deepEqual([rows[1].cartons, rows[1].netKg, rows[1].grossKg, rows[1].cbm, rows[1].dutyPercent, rows[1].company, rows[1].unitType], [null, null, null, null, null, '', 'Carton']);
    assert.deepEqual(CI.check({ containerCostUsd: 3450, rows: [row({ netKg: 2200 })] }).problems, [{ row: 0, field: 'netKg', message: 'Net weight is above gross weight.' }]);
    assert.deepEqual(CI.check({ containerCostUsd: 3450, rows: [row({ dutyPercent: 101 })] }).problems.map(p => p.field), ['dutyPercent']);
    assert.deepEqual(CI.check({ containerCostUsd: 3450, rows: [row({ commodityCode: '3006 7000' })] }).problems, []);
    // No rows, not a list, or more rows than a container could have.
    assert.deepEqual(CI.check({ containerCostUsd: 3450, rows: [] }).problems.map(p => p.field), ['rows']);
    assert.deepEqual(CI.check({ containerCostUsd: 3450 }).problems.map(p => p.field), ['rows']);
    assert.deepEqual(CI.check(null).problems.map(p => p.field), ['rows', 'containerCostUsd']);
    assert.deepEqual(CI.check({ containerCostUsd: 3450, rows: Array.from({ length: CI.MAX_ROWS + 1 }, () => row()) }).problems.map(p => p.field), ['rows']);
});

test('the duty of each commodity code, to be offered next time: the last row to say it', () => {
    assert.deepEqual(CI.dutyRatesOf([row(), row({ commodityCode: '3005909900', dutyPercent: 2.5 }), row({ commodityCode: '3005909900', dutyPercent: 4 }), row({ commodityCode: '', dutyPercent: 9 }), row({ commodityCode: '3822190000', dutyPercent: null })]), [
        ['3006700000', 0], ['3005909900', 4],
    ]);
});

// ── Sent as a link to the portal only, never attached (the user, 2026-11-27) ──

const { FREIGHT_CLEARANCE_TEMPLATE } = require('../src/lib/email-template-defaults');
const { clearanceBlockHtml } = require('../src/lib/freight-quotes');
const linkOnly = require('../src/db/migrate/2026-11-27_50_clearance_link_only');

// Wording that claims a file is on the email ("is not attached" does not match).
const SAYS_ATTACHED = /(find|is|are|been) attached|attached \/|附件|附上|随函/i;

test('link only: the template and the block under it say the file is on the portal, never that it is attached', () => {
    for (const text of [FREIGHT_CLEARANCE_TEMPLATE.bodyHtml, FREIGHT_CLEARANCE_TEMPLATE.description]) {
        assert.ok(!SAYS_ATTACHED.test(text), text);
    }
    assert.ok(FREIGHT_CLEARANCE_TEMPLATE.bodyHtml.includes('{containerName}'));
    assert.ok(FREIGHT_CLEARANCE_TEMPLATE.bodyHtml.includes('supplier portal'));
    const html = clearanceBlockHtml({
        containerName: 'SEKU6400902', filename: 'Clearance Instructions - SEKU6400902.csv',
        url: 'https://portal.example/freight-containers/1214', registerUrl: 'https://portal.example/register',
    });
    assert.ok(!SAYS_ATTACHED.test(html), html);
    assert.ok(html.includes('Clearance Instructions - SEKU6400902.csv'));
    assert.ok(html.includes('href="https://portal.example/freight-containers/1214"'));
    assert.ok(html.includes('https://portal.example/register'));
    assert.ok(html.includes('not attached to this email'));
});

// A connection that answers the UPDATE with `affected` and a SELECT with `rows`.
function linkOnlyConn(affected, rows = []) {
    const asked = [];
    return {
        asked,
        query: async (sql, params) => {
            const text = String(sql).replace(/\s+/g, ' ').trim();
            asked.push({ sql: text, params });
            return /^UPDATE/.test(text) ? [{ affectedRows: affected }] : [rows];
        },
    };
}
const runLinkOnly = async (conn) => { const said = []; await linkOnly.up(conn, { log: m => said.push(m) }); return said; };

test('link only: the stored template is reworded only where it still holds what 2026-11-24_23 seeded', async () => {
    // What it looks for is the wording that said the file was attached, word for word.
    assert.equal(linkOnly.KEY, 'freight_clearance_instructions');
    assert.ok(linkOnly.SEEDED.bodyHtml.includes('<p>Please find attached our clearance instructions for container <strong>{containerName}</strong>. The same file is on the container’s page on our supplier portal.</p>'));
    assert.ok(linkOnly.SEEDED.bodyHtml.includes('<p>随函附上集装箱 <strong>{containerName}</strong> 的清关资料。该文件也可在供应商门户的集装箱页面下载。</p>'));
    assert.notEqual(linkOnly.SEEDED.bodyHtml, FREIGHT_CLEARANCE_TEMPLATE.bodyHtml);

    const conn = linkOnlyConn(1);
    assert.deepEqual(await runLinkOnly(conn), ['freight_clearance_instructions reworded']);
    assert.deepEqual(conn.asked, [{
        sql: 'UPDATE email_templates SET description = ?, body_html = ? WHERE template_key = ? AND deleted_at IS NULL AND body_html = ?',
        params: [FREIGHT_CLEARANCE_TEMPLATE.description, FREIGHT_CLEARANCE_TEMPLATE.bodyHtml, 'freight_clearance_instructions', linkOnly.SEEDED.bodyHtml],
    }]);
});

test('link only: an edited template is left alone and named; a missing or already reworded one is said', async () => {
    assert.deepEqual(await runLinkOnly(linkOnlyConn(0, [{ body_html: '<p>Our own words</p>' }])), [
        'freight_clearance_instructions has been edited since it was seeded and was left as it is: check it does not say the file is attached',
    ]);
    assert.deepEqual(await runLinkOnly(linkOnlyConn(0, [])), ['freight_clearance_instructions not found; nothing to reword']);
    assert.deepEqual(await runLinkOnly(linkOnlyConn(0, [{ body_html: FREIGHT_CLEARANCE_TEMPLATE.bodyHtml }])), ['freight_clearance_instructions already holds the new wording']);
    // One write asked for, never a second try at an edited template.
    const edited = linkOnlyConn(0, [{ body_html: '<p>x</p>' }]);
    await runLinkOnly(edited);
    assert.equal(edited.asked.filter(a => /^UPDATE/.test(a.sql)).length, 1);
});
