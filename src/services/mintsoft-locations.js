'use strict';

// Reads the local `mintsoft_locations` cache (synced nightly by the
// mintsoftLocationsSync job). Lifted out of src/handlers/orders.js so both the
// authed admin API and the public carton-scan Lambda return the SAME warehouse
// / location lists for their pickers. Caller supplies the connection.

// Every warehouse with its locations nested.
async function listWarehousesWithLocations(conn) {
    const [rows] = await conn.query(
        `SELECT warehouse_id, warehouse_name, warehouse_code, warehouse_active,
                location_id, name, location_name, pick_sequence, location_type_id, last_synced
         FROM mintsoft_locations
         ORDER BY warehouse_name, warehouse_id, pick_sequence, name`
    );
    const byWh = new Map();
    for (const r of rows) {
        let wh = byWh.get(r.warehouse_id);
        if (!wh) {
            wh = {
                warehouseId: r.warehouse_id,
                name: r.warehouse_name || '',
                code: r.warehouse_code || '',
                active: !!r.warehouse_active,
                lastSynced: r.last_synced?.toISOString?.() ?? r.last_synced,
                locations: [],
            };
            byWh.set(r.warehouse_id, wh);
        }
        wh.locations.push({
            locationId: r.location_id,
            name: r.name || '',
            locationName: r.location_name || r.name || '',
            pickSequence: r.pick_sequence,
            locationTypeId: r.location_type_id,
        });
    }
    return Array.from(byWh.values());
}

// The locations belonging to one warehouse.
async function listLocationsForWarehouse(conn, warehouseId) {
    const [rows] = await conn.query(
        `SELECT location_id, warehouse_id, name, location_name,
                pick_sequence, location_type_id, last_synced
         FROM mintsoft_locations
         WHERE warehouse_id = ?
         ORDER BY pick_sequence, name`,
        [warehouseId]
    );
    return rows.map(r => ({
        locationId: r.location_id,
        warehouseId: r.warehouse_id,
        name: r.name || '',
        locationName: r.location_name || r.name || '',
        pickSequence: r.pick_sequence,
        locationTypeId: r.location_type_id,
        lastSynced: r.last_synced?.toISOString?.() ?? r.last_synced,
    }));
}

module.exports = { listWarehousesWithLocations, listLocationsForWarehouse };
