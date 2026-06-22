'use strict';

// Calls getProductStock() directly against Mintsoft and prints the result.
// No DB writes — verifies the new Math.max(0, ...) clamp is in effect for
// the JF0954 row that triggered the bug report (product_id=2728).

require('dotenv').config();
const { getProductStock } = require('../src/services/mintsoft');

const PRODUCT_ID = Number(process.argv[2] || 2728);

(async () => {
    try {
        const rows = await getProductStock(PRODUCT_ID);
        console.log(`getProductStock(${PRODUCT_ID}) → ${rows.length} warehouse rows\n`);
        for (const r of rows) {
            console.log(JSON.stringify(r, null, 2));
            if (r.available < 0) {
                console.log('  !!! still negative — clamp not applied');
            }
        }
        const anyNegative = rows.some(r => r.available < 0);
        console.log(`\n${anyNegative ? 'FAIL — negative available returned' : 'OK — no negative available'}`);
        process.exit(anyNegative ? 1 : 0);
    } catch (e) {
        console.error('Mintsoft call failed:', e.message);
        process.exit(1);
    }
})();
