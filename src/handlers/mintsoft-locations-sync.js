require('dotenv').config();
const { getPool, closePool } = require('../db');
const { getWarehouses, getLocations } = require('../services/mintsoft');
const log = require('../lib/logger');

exports.handler = async () => {
    const pool = getPool();
    const conn = await pool.getConnection();
    try {
        log.info('[mintsoft-locations-sync] starting');

        // 1. Fetch everything from Mintsoft first. If anything fails, we
        // bail before touching the cache table.
        const warehouses = await getWarehouses();
        if (!warehouses.length) {
            log.warn('[mintsoft-locations-sync] no warehouses returned — aborting to preserve cache');
            return { statusCode: 200, body: JSON.stringify({ warehouses: 0, locations: 0 }) };
        }

        const allRows = [];
        const perWarehouse = {};
        for (const w of warehouses) {
            const locations = await getLocations(w.warehouseId);
            for (const l of locations) {
                allRows.push([
                    l.locationId,
                    l.warehouseId || w.warehouseId,
                    w.name || null,
                    w.code || null,
                    w.active ? 1 : 0,
                    l.name || null,
                    l.locationName || null,
                    l.pickSequence,
                    l.locationTypeId,
                ]);
            }
            perWarehouse[w.warehouseId] = locations.length;
        }

        // 2. Replace the table contents.
        await conn.execute('TRUNCATE TABLE mintsoft_locations');

        const BATCH_SIZE = 500;
        for (let i = 0; i < allRows.length; i += BATCH_SIZE) {
            const batch = allRows.slice(i, i + BATCH_SIZE);
            const placeholders = batch.map(() => '(?, ?, ?, ?, ?, ?, ?, ?, ?)').join(',');
            const values = batch.flat();
            await conn.execute(
                `INSERT INTO mintsoft_locations (
                    location_id, warehouse_id, warehouse_name, warehouse_code, warehouse_active,
                    name, location_name, pick_sequence, location_type_id
                 ) VALUES ${placeholders}`,
                values
            );
        }

        log.info('[mintsoft-locations-sync] done', { warehouses: warehouses.length, locations: allRows.length });
        return {
            statusCode: 200,
            body: JSON.stringify({ warehouses: warehouses.length, locations: allRows.length, perWarehouse }),
        };
    } catch (err) {
        log.error('[mintsoft-locations-sync] failed', err?.response?.data || err);
        return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
    } finally {
        conn.release();
    }
};

if (require.main === module) {
    exports.handler({}).then(r => {
        console.log(r.body);
    }).catch(err => {
        console.error('Fatal:', err);
        process.exit(1);
    }).finally(() => closePool());
}
