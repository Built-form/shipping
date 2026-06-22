'use strict';
// List Mintsoft locations for a warehouse.
// Usage: node tools/list-locations.js <warehouseId>
require('dotenv').config();
const { getLocations } = require('../src/services/mintsoft');

const warehouseId = Number(process.argv[2]);
if (!warehouseId) {
    console.error('Usage: node tools/list-locations.js <warehouseId>');
    process.exit(1);
}

(async () => {
    const locs = await getLocations(warehouseId);
    locs.forEach(l => console.log(`${l.locationId}\t${l.locationName}`));
    console.log(`\n(${locs.length} locations)`);
})().catch(err => { console.error(err.message); process.exit(1); });
