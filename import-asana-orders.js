require('dotenv').config();
const { getPool, closePool } = require('./utils/db');
const { fetchAsanaProject } = require('./services/asanaService');
const { transformAsanaToCSV } = require('./transformers/asanaTransformer');
const { mapGoodsOnSeaStatus, mapOrdersStatus } = require('./utils/statusMappers');

// Note: Database table creation should ideally be handled by a migration script
// and not within the Lambda handler for production environments.
// For this refactoring, the CREATE TABLE statement is removed from the handler.

// ── Import ────────────────────────────────────────────────────────────────
async function importGoodsOnSea(conn, rows) {
    let inserted = 0, skipped = 0;
    const BATCH_SIZE = 100;
    const batches = [];

    for (const row of rows) {
        let id = row['Name'];
        if (!id) {
            id = row['PO Number'] || row['ASIN'] || `sea-${Date.now()}-${Math.random().toString(36).substring(2, 11)}`;
        }

        const containerNumber = row['Section/Column'];
        const status = mapGoodsOnSeaStatus(row['Container Status']);
        const etaRaw = row['ETA to Port'];
        const eta = etaRaw ? etaRaw.slice(0, 10) : null;
        const sailingRaw = row['Sailing Date'];

        const dates = {};
        if (sailingRaw) dates.shipped = new Date(sailingRaw).toISOString();
        if (eta) dates.eta = new Date(eta).toISOString();

        batches.push([
            id.trim(),
            row['ASIN'] ? row['ASIN'].trim() : null,
            row['SKU'] ? row['SKU'].trim() : null,
            parseInt(row['Units'], 10) || 0,
            status,
            row['PO Number'] ? row['PO Number'].trim() : null,
            row['Supplier Name'] ? row['Supplier Name'].trim() : null,
            containerNumber ? containerNumber.trim() : null,
            eta,
            parseFloat(row['CBM/Line']) || null,
            JSON.stringify(dates),
        ]);
    }

    // Execute batch inserts
    for (let i = 0; i < batches.length; i += BATCH_SIZE) {
        const batch = batches.slice(i, i + BATCH_SIZE);
        const placeholders = batch.map(() => '(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)').join(',');
        const values = batch.flat();

        try {
            await conn.execute(
                `INSERT INTO orders
                    (id, asin, product_name, quantity, status, po_number, supplier,
                     container_number, eta, cbm_per_unit, dates)
                 VALUES ${placeholders}`,
                values
            );
            inserted += batch.length;
        } catch (err) {
            console.warn(`  Batch error: ${err.message}`);
            skipped += batch.length;
        }
    }

    return { inserted, skipped };
}

async function importOrders(conn, rows) {
    let inserted = 0, skipped = 0;
    const BATCH_SIZE = 100;
    const batches = [];

    for (const row of rows) {
        let id = row['Name'];
        if (!id) {
            id = row['PO Number'] || row['ASIN'] || `order-${Date.now()}-${Math.random().toString(36).substring(2, 11)}`;
        }

        const status = mapOrdersStatus(row['Goods Status']);
        const poDateRaw = row['PO Placed'];
        const dates = {};
        if (poDateRaw) dates.ordered = new Date(poDateRaw).toISOString();

        const totalCbm = parseFloat(row['Total CBM.']);
        const units = parseInt(row['Units Ordered'], 10);
        const cbmPerUnit = totalCbm && units ? totalCbm / units : null;

        const section = row['Section/Column'];
        const containerNumber = section && section !== 'Untitled section' ? section : null;

        batches.push([
            id.trim(),
            row['ASIN'] ? row['ASIN'].trim() : null,
            row['SKU'] ? row['SKU'].trim() : null,
            units || 0,
            status,
            row['PO Number'] ? row['PO Number'].trim() : null,
            row['Supplier Name'] ? row['Supplier Name'].trim() : null,
            containerNumber ? containerNumber.trim() : null,
            cbmPerUnit,
            JSON.stringify(dates),
        ]);
    }

    // Execute batch inserts
    for (let i = 0; i < batches.length; i += BATCH_SIZE) {
        const batch = batches.slice(i, i + BATCH_SIZE);
        const placeholders = batch.map(() => '(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)').join(',');
        const values = batch.flat();

        try {
            await conn.execute(
                `INSERT INTO orders
                    (id, asin, product_name, quantity, status, po_number, supplier,
                     container_number, cbm_per_unit, dates)
                 VALUES ${placeholders}`,
                values
            );
            inserted += batch.length;
        } catch (err) {
            console.error(`  Batch error: ${err.message}`);
            skipped += batch.length;
        }
    }

    return { inserted, skipped };
}

// ── Lambda Handler ─────────────────────────────────────────────────────────
exports.handler = async (event) => {
    try {
        const pool = getPool();
        const conn = await pool.getConnection();

        // Fetch from Asana BEFORE truncating
        const asanaPAT = process.env.ASANA_PAT;
        const seaProjectId = '1210568539171010';
        const ordersProjectId = '1210599256348524';

        console.log('Fetching Goods_on_sea from Asana...');
        const seaTasks = await fetchAsanaProject(seaProjectId, asanaPAT);
        const seaRows = transformAsanaToCSV(seaTasks);
        console.log(`  Fetched ${seaRows.length} rows`);
        console.log('Fetching Orders from Asana...');
        const orderTasks = await fetchAsanaProject(ordersProjectId, asanaPAT);
        const orderRows = transformAsanaToCSV(orderTasks);
        console.log(`  Fetched ${orderRows.length} rows`);

        // Validate before truncating
        if (!seaRows.length && !orderRows.length) {
            throw new Error('No data fetched from Asana projects — aborting to preserve existing data');
        }

        // Only truncate after successful fetch
        await conn.execute('TRUNCATE TABLE orders');
        console.log('Truncated orders table.');

        // Import data
        console.log('Importing Goods_on_sea...');
        const seaResult = await importGoodsOnSea(conn, seaRows);
        console.log(`  Inserted: ${seaResult.inserted}, Skipped: ${seaResult.skipped}`);

        console.log('Importing Orders...');
        const orderResult = await importOrders(conn, orderRows);
        console.log(`  Inserted: ${orderResult.inserted}, Skipped: ${orderResult.skipped}`);

        return {
            statusCode: 200,
            body: JSON.stringify({
                sea: seaResult,
                orders: orderResult,
            }),
        };
    } catch (err) {
        console.error('Fatal:', err.message);
        return {
            statusCode: 500,
            body: JSON.stringify({ error: err.message }),
        };
    } finally {
        conn.release(); // Release connection back to the pool
    }
};

// ── CLI Runner ─────────────────────────────────────────────────────────────
if (require.main === module) {
    exports.handler({}).then(result => {
        console.log(JSON.stringify(result, null, 2));
    }).catch(err => {
        console.error('Fatal:', err);
    }).finally(() => {
        closePool(); // Close the pool when the CLI runner finishes
    });
}
