'use strict';

// Live probe of the ShipsGo air integration for a single AWB.
//   node tools/probe-air.js 235-98027403
// Calls the real ShipsGo API (registers the AWB if not already tracked).

require('dotenv').config();
const { getShipmentByAwb, parseAirShipment } = require('../src/services/shipsgo');

(async () => {
    const awb = process.argv[2] || '235-98027403';
    console.log(`\n=== Probing AWB ${awb} ===`);

    const bundle = await getShipmentByAwb(awb);
    const parsed = parseAirShipment(bundle);

    console.log('\n--- Parsed (table shape) ---');
    const { milestones, transshipments, route_geojson, raw, ...flat } = parsed;
    console.log(JSON.stringify(flat, null, 2));

    console.log(`\n--- Transshipments (${transshipments.length}) ---`);
    console.log(JSON.stringify(transshipments, null, 2));

    console.log(`\n--- Milestones (${milestones.length}) ---`);
    for (const m of milestones) {
        const flag = m.is_actual ? 'ACT' : 'EST';
        const ap = m.airport.iata || '???';
        console.log(`  ${flag}  ${(m.event || '').padEnd(4)} ${ap}  ${m.timestamp || ''}  ${m.flight || ''}`);
    }

    console.log('\n--- Geojson present:', !!route_geojson, route_geojson?.features ? `(${route_geojson.features.length} features)` : '');
})().catch(err => {
    console.error('\nFATAL:', err.message);
    process.exit(1);
});
