require('dotenv').config();
const { getPool, closePool } = require('../db');
const { fetchAsanaProject } = require('../services/asana-service');
const { transformAsanaToCSV } = require('../services/asana-transformer');
const { mapGoodsOnSeaStatus, mapGoodsOnAirStatus, mapOrdersStatus } = require('../domain/status-mappers');

// Note: Database table creation should ideally be handled by a migration script
// and not within the Lambda handler for production environments.
// For this refactoring, the CREATE TABLE statement is removed from the handler.

// Mirror every importer-attached order line as a PO-level audit event.
// Batched 200 rows per INSERT to keep the round-trip count low.
async function auditImporterAttachments(conn) {
    const [linkedRows] = await conn.query(`
        SELECT id, purchase_order_id, jf_code, asin, product_name,
               quantity, po_number, supplier
          FROM orders
         WHERE purchase_order_id IS NOT NULL AND deleted_at IS NULL
    `);
    if (!linkedRows.length) return 0;

    const BATCH = 200;
    let total = 0;
    for (let i = 0; i < linkedRows.length; i += BATCH) {
        const batch = linkedRows.slice(i, i + BATCH);
        const placeholders = batch.map(() => '(?, ?, ?, ?, ?, ?)').join(',');
        const values = [];
        for (const r of batch) {
            const payload = {
                orderId: r.id,
                jfCode: r.jf_code || null,
                asin: r.asin || null,
                productName: r.product_name || null,
                quantity: r.quantity ?? null,
                poNumber: r.po_number || null,
                supplier: r.supplier || null,
                source: 'asana-importer',
            };
            values.push(
                'purchase_order',
                r.purchase_order_id,
                'order_attached',
                null,
                JSON.stringify(payload),
                null,
            );
        }
        try {
            await conn.execute(
                `INSERT INTO audit_log (entity_type, entity_id, action, before_json, after_json, user_email)
                 VALUES ${placeholders}`,
                values
            );
            total += batch.length;
        } catch (err) {
            console.warn(`  Audit batch error: ${err.message}`);
        }
    }
    return total;
}

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
        // Populated by update-imported-dates.js (synced from GIGO Container No.)
        const externalContainerNumber = row['External Container Number']
            ? row['External Container Number'].trim()
            : null;

        const containerNumber = row['Section/Column'];
        const status = mapGoodsOnSeaStatus(row['Container Status']);
        const etaRaw = row['ETA to Port'];
        const eta = etaRaw ? etaRaw.slice(0, 10) : null;
        const sailingRaw = row['Sailing Date'];
        const deliveryDateRaw = row['Delivery Date'];
        const deliveryDate = deliveryDateRaw ? deliveryDateRaw.slice(0, 10) : null;
        const mfgDateRaw = row['MFG DATE'];
        const mfgDate = mfgDateRaw ? mfgDateRaw.slice(0, 10) : null;
        const expDateRaw = row['EXP DATE'];
        const expDate = expDateRaw ? expDateRaw.slice(0, 10) : null;
        const arrivedRaw = row['Arrived'];
        const arrivedDate = arrivedRaw ? arrivedRaw.slice(0, 10) : null;

        const artworkDateRaw = row['Artwork Confirmed Date'];
        const sailingDate = sailingRaw ? sailingRaw.slice(0, 10) : null;
        const artworkDate = artworkDateRaw ? artworkDateRaw.slice(0, 10) : null;
        // dates JSON is now state-transitions only (manufacturing, ready,
        // consolidated, etc.). Scalar Asana dates write to flat columns.
        const dates = {};

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
            deliveryDate,
            row['LOT number'] ? row['LOT number'].trim() : null,
            mfgDate,
            expDate,
            row['Delivery Time'] ? row['Delivery Time'].trim() : null,
            row['Container Status'] ? row['Container Status'].trim() : null,
            row['Booking Status'] ? row['Booking Status'].trim() : null,
            arrivedDate,
            externalContainerNumber,
            sailingDate,
            artworkDate,
        ]);
    }

    // Execute batch inserts
    for (let i = 0; i < batches.length; i += BATCH_SIZE) {
        const batch = batches.slice(i, i + BATCH_SIZE);
        const placeholders = batch.map(() => '(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)').join(',');
        const values = batch.flat();

        try {
            await conn.execute(
                `INSERT INTO orders
                    (jf_code, asin, product_name, quantity, status, po_number, supplier,
                     container_number, eta, order_cbm, dates, delivery_date,
                     lot_number, mfg_date, exp_date, delivery_time,
                     container_status, booking_status, arrived_date, external_container_number,
                     shipped_date, artwork_confirmed_date)
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

async function importGoodsOnAir(conn, rows) {
    let inserted = 0, skipped = 0;
    const BATCH_SIZE = 100;
    const batches = [];

    for (const row of rows) {
        let id = row['Name'];
        if (!id) {
            id = row['PO Number'] || row['ASIN'] || `air-${Date.now()}-${Math.random().toString(36).substring(2, 11)}`;
        }

        const status = mapGoodsOnAirStatus();
        const etaRaw = row['ETA to Port'];
        const eta = etaRaw ? etaRaw.slice(0, 10) : null;
        const departureRaw = row['Departure Date'];
        const departureDate = departureRaw ? departureRaw.slice(0, 10) : null;
        const dates = {};

        batches.push([
            id.trim(),
            row['ASIN'] ? row['ASIN'].trim() : null,
            row['SKU'] ? row['SKU'].trim() : null,
            parseInt(row['Units'], 10) || 0,
            status,
            row['PO Number'] ? row['PO Number'].trim() : null,
            row['Supplier Name'] ? row['Supplier Name'].trim() : null,
            row['Section/Column'] ? row['Section/Column'].trim() : null,
            eta,
            parseFloat(row['CBM/Line']) || null,
            JSON.stringify(dates),
            departureDate,
        ]);
    }

    for (let i = 0; i < batches.length; i += BATCH_SIZE) {
        const batch = batches.slice(i, i + BATCH_SIZE);
        const placeholders = batch.map(() => '(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)').join(',');
        const values = batch.flat();

        try {
            await conn.execute(
                `INSERT INTO orders
                    (jf_code, asin, product_name, quantity, status, po_number, supplier,
                     container_number, eta, order_cbm, dates, shipped_date)
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

        const status = mapOrdersStatus(row['Goods Status'], row['Artwork Confirmed Date']);
        const poDateRaw = row['PO Placed'];
        const estimatedReadyRaw = row['Estimated Ready Date'];
        const artworkDateRaw = row['Artwork Confirmed Date'];
        const orderedDate = poDateRaw ? poDateRaw.slice(0, 10) : null;
        const estimatedReadyDate = estimatedReadyRaw ? estimatedReadyRaw.slice(0, 10) : null;
        const artworkConfirmedDate = artworkDateRaw ? artworkDateRaw.slice(0, 10) : null;
        const dates = {};

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
            row['Port'] ? row['Port'].trim() : null,
            orderedDate,
            estimatedReadyDate,
            artworkConfirmedDate,
        ]);
    }

    // Execute batch inserts
    for (let i = 0; i < batches.length; i += BATCH_SIZE) {
        const batch = batches.slice(i, i + BATCH_SIZE);
        const placeholders = batch.map(() => '(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)').join(',');
        const values = batch.flat();

        try {
            await conn.execute(
                `INSERT INTO orders
                    (jf_code, asin, product_name, quantity, status, po_number, supplier,
                     container_number, cbm_per_unit, dates, port,
                     ordered_date, estimated_ready_date, artwork_confirmed_date)
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
    const pool = getPool();
    const conn = await pool.getConnection();
    try {

        // Fetch from Asana BEFORE truncating
        const asanaPAT = process.env.ASANA_PAT;
        const seaProjectId = '1210568539171010';
        const airProjectId = '1210880888784849';
        const ordersProjectId = '1210599256348524';

        console.log('Fetching Goods_on_sea from Asana...');
        const seaTasks = await fetchAsanaProject(seaProjectId, asanaPAT);
        const seaRows = transformAsanaToCSV(seaTasks);
        console.log(`  Fetched ${seaRows.length} rows`);
        console.log('Fetching Goods_on_air from Asana...');
        const airTasks = await fetchAsanaProject(airProjectId, asanaPAT);
        const airRows = transformAsanaToCSV(airTasks);
        console.log(`  Fetched ${airRows.length} rows`);
        console.log('Fetching Orders from Asana...');
        const orderTasks = await fetchAsanaProject(ordersProjectId, asanaPAT);
        const orderRows = transformAsanaToCSV(orderTasks);
        console.log(`  Fetched ${orderRows.length} rows`);

        // Validate before truncating
        if (!seaRows.length && !airRows.length && !orderRows.length) {
            throw new Error('No data fetched from Asana projects — aborting to preserve existing data');
        }

        // Only truncate after successful fetch
        await conn.execute('TRUNCATE TABLE orders');
        console.log('Truncated orders table.');

        // Import data
        console.log('Importing Goods_on_sea...');
        const seaResult = await importGoodsOnSea(conn, seaRows);
        console.log(`  Inserted: ${seaResult.inserted}, Skipped: ${seaResult.skipped}`);

        console.log('Importing Goods_on_air...');
        const airResult = await importGoodsOnAir(conn, airRows);
        console.log(`  Inserted: ${airResult.inserted}, Skipped: ${airResult.skipped}`);

        console.log('Importing Orders...');
        const orderResult = await importOrders(conn, orderRows);
        console.log(`  Inserted: ${orderResult.inserted}, Skipped: ${orderResult.skipped}`);

        // Auto-create purchase_orders for any po_number we don't have yet,
        // then re-resolve orders.purchase_order_id by joining on po_number.
        // The TRUNCATE above wipes purchase_order_id every cycle — without
        // this, the link would only exist briefly between runs.
        console.log('Linking orders → purchase_orders by po_number...');
        const [poInsert] = await conn.execute(`
            INSERT INTO purchase_orders (po_number, supplier)
            SELECT po_number, MAX(supplier) AS supplier
              FROM orders
             WHERE po_number IS NOT NULL AND po_number <> ''
               AND po_number NOT IN (SELECT po_number FROM purchase_orders)
             GROUP BY po_number
        `);
        const [linkResult] = await conn.execute(`
            UPDATE orders o
              JOIN purchase_orders po ON po.po_number = o.po_number
               SET o.purchase_order_id = po.id
             WHERE o.po_number IS NOT NULL AND o.po_number <> ''
        `);
        console.log(`  Created ${poInsert.affectedRows} new POs, linked ${linkResult.affectedRows} orders.`);

        // Mirror each newly-linked row as a PO-level order_attached audit
        // event so the PO's audit trail captures the importer's re-link
        // sweep. Historically the importer was excluded from audit because
        // it TRUNCATEd orders every 10 min (per src/handlers/orders.js:46),
        // but the schedule is now disabled and re-links typically happen
        // manually — so the audit growth is bounded by manual invocations.
        // If the schedule is re-enabled, revisit (e.g. dedupe by (po_id,
        // order_id) or downsample to changes only).
        const attachedAudited = await auditImporterAttachments(conn);
        console.log(`  Audited ${attachedAudited} order_attached events.`);

        return {
            statusCode: 200,
            body: JSON.stringify({
                sea: seaResult,
                air: airResult,
                orders: orderResult,
                purchaseOrders: {
                    created: poInsert.affectedRows,
                    ordersLinked: linkResult.affectedRows,
                    auditEvents: attachedAudited,
                },
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
