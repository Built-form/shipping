const serverless = require('serverless-http');
require('dotenv').config();
const express = require('express');
const mysql = require('mysql2/promise');
const cors = require('cors');
const compression = require('compression');
const { v4: uuidv4 } = require('uuid');

const app = express();

// 1. Middleware
app.use(compression());
app.use(cors());
app.use(express.json());

// ── Ensure allowed_emails table exists (runs lazily on first auth check) ──
let _ensureAllowedEmailsPromise;
function ensureAllowedEmailsTable() {
    if (!_ensureAllowedEmailsPromise) {
        _ensureAllowedEmailsPromise = (async () => {
            const conn = await pool.getConnection();
            try {
                await conn.execute(`
                    CREATE TABLE IF NOT EXISTS allowed_emails (
                        email VARCHAR(255) NOT NULL PRIMARY KEY,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                `);
            } finally {
                conn.release();
            }
        })();
    }
    return _ensureAllowedEmailsPromise;
}

// ── Email whitelist middleware ─────────────────────────────────────────────
const IS_LOCAL = process.env.IS_OFFLINE || process.env.NODE_ENV === 'development';

app.use(async (req, res, next) => {
    if (IS_LOCAL) {
        req.userEmail = 'local@dev';
        return next();
    }

    try {
        const email = req.requestContext?.authorizer?.jwt?.claims?.email;
        if (!email) {
            return res.status(401).json({ error: 'Unauthorized: no email in token.' });
        }

        await ensureAllowedEmailsTable();
        const connection = await pool.getConnection();
        try {
            const [rows] = await connection.execute(
                'SELECT 1 FROM allowed_emails WHERE email = ?',
                [email.toLowerCase()]
            );
            if (rows.length === 0) {
                return res.status(401).json({ error: 'Unauthorized: email not in allowlist.' });
            }
        } finally {
            connection.release();
        }

        req.userEmail = email.toLowerCase();
        next();
    } catch (err) {
        console.error('[auth-middleware]', err);
        res.status(500).json({ error: 'Internal authentication error.' });
    }
});

// Cache-Control
app.use((req, res, next) => {
    if (req.method === 'GET') {
        res.set('Cache-Control', 'private, max-age=60');
    } else {
        res.set('Cache-Control', 'no-store');
    }
    next();
});

// 2. Database Configuration
const pool = mysql.createPool({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    port: process.env.DB_PORT || 3306,
    ssl: { rejectUnauthorized: false },
    waitForConnections: true,
    connectionLimit: 5,
    queueLimit: 0,
});

// ── Status → dates key mapping ────────────────────────────────────────────
const STATUS_DATE_KEY = {
    PLANNING: 'planned',
    PO_RAISED: 'ordered',
    MANUFACTURING: 'manufacturing',
    READY: 'ready',
    CONTAINERIZED: 'containerized',
    IN_TRANSIT: 'shipped',
    DELIVERED: 'delivered',
    COMPLETED: 'completed',
};

// ── Ensure orders table exists ────────────────────────────────────────────
let _ensureOrdersTablePromise;
function ensureOrdersTable() {
    if (!_ensureOrdersTablePromise) {
        _ensureOrdersTablePromise = (async () => {
            const conn = await pool.getConnection();
            try {
                await conn.execute(`
                    CREATE TABLE IF NOT EXISTS orders (
                        id VARCHAR(50) NOT NULL PRIMARY KEY,
                        asin VARCHAR(20),
                        product_name VARCHAR(255),
                        quantity INT NOT NULL DEFAULT 0,
                        status VARCHAR(30) NOT NULL DEFAULT 'PLANNING',
                        po_number VARCHAR(100),
                        supplier VARCHAR(255),
                        container_number VARCHAR(100),
                        vessel_name VARCHAR(255),
                        eta DATE,
                        cbm_per_unit DECIMAL(10,6),
                        pack_size INT,
                        cbm_per_pack DECIMAL(10,6),
                        notes TEXT,
                        dates JSON,
                        location VARCHAR(255),
                        containerized_location VARCHAR(255),
                        expected_shipping_date DATE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        INDEX idx_status (status),
                        INDEX idx_container (container_number)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                `);
            } finally {
                conn.release();
            }
        })();
    }
    return _ensureOrdersTablePromise;
}

// ── Helpers ───────────────────────────────────────────────────────────────

function generateOrderId() {
    return `ORD-${uuidv4().slice(0, 8).toUpperCase()}`;
}

function rowToOrder(row) {
    let dates = row.dates;
    if (typeof dates === 'string') dates = JSON.parse(dates);
    return {
        id: row.id,
        asin: row.asin || null,
        productName: row.product_name || null,
        quantity: row.quantity,
        status: row.status,
        poNumber: row.po_number || null,
        supplier: row.supplier || null,
        containerNumber: row.container_number || null,
        vesselName: row.vessel_name || null,
        eta: row.eta ? row.eta.toISOString?.().slice(0, 10) ?? row.eta : null,
        cbmPerUnit: row.cbm_per_unit != null ? Number(row.cbm_per_unit) : null,
        packSize: row.pack_size != null ? Number(row.pack_size) : null,
        cbmPerPack: row.cbm_per_pack != null ? Number(row.cbm_per_pack) : null,
        notes: row.notes || null,
        dates: dates || {},
        location: row.location || null,
        containerizedLocation: row.containerized_location || null,
        expectedShippingDate: row.expected_shipping_date ? (row.expected_shipping_date.toISOString?.().slice(0, 10) ?? row.expected_shipping_date) : null,
    };
}

function setDateKey(dates, status) {
    const key = STATUS_DATE_KEY[status];
    if (key) dates[key] = new Date().toISOString();
    return dates;
}

// ── 1. GET /api/v1/orders — Get all orders ────────────────────────────────
app.get('/api/v1/orders', async (req, res) => {
    let connection;
    try {
        await ensureOrdersTable();
        connection = await pool.getConnection();
        const [rows] = await connection.query('SELECT * FROM orders ORDER BY created_at DESC');
        res.json({ data: rows.map(rowToOrder) });
    } catch (error) {
        console.error('[GET /orders]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    } finally {
        if (connection) connection.release();
    }
});

// ── 2. POST /api/v1/orders — Create a new order ──────────────────────────
app.post('/api/v1/orders', async (req, res) => {
    let connection;
    try {
        await ensureOrdersTable();
        const b = req.body || {};
        const id = generateOrderId();
        const status = 'PLANNING';
        const dates = setDateKey({}, status);

        connection = await pool.getConnection();
        await connection.execute(
            `INSERT INTO orders (id, asin, product_name, quantity, status, po_number, supplier,
                container_number, vessel_name, eta, cbm_per_unit, pack_size, cbm_per_pack, notes, dates)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            [
                id,
                b.asin || null,
                b.productName || null,
                b.quantity || 0,
                status,
                b.poNumber || null,
                b.supplier || null,
                b.containerNumber || null,
                b.vesselName || null,
                b.eta || null,
                b.cbmPerUnit ?? null,
                b.packSize ?? null,
                b.cbmPerPack ?? null,
                b.notes || null,
                JSON.stringify(dates),
            ]
        );

        const [rows] = await connection.query('SELECT * FROM orders WHERE id = ?', [id]);
        res.status(201).json(rowToOrder(rows[0]));
    } catch (error) {
        console.error('[POST /orders]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    } finally {
        if (connection) connection.release();
    }
});

// ── 3. PUT /api/v1/orders/:id — Update an order ──────────────────────────
app.put('/api/v1/orders/:id', async (req, res) => {
    let connection;
    try {
        await ensureOrdersTable();
        const { id } = req.params;
        const b = req.body || {};

        connection = await pool.getConnection();

        const [existing] = await connection.query('SELECT * FROM orders WHERE id = ?', [id]);
        if (!existing.length) {
            return res.status(404).json({ error: `Order ${id} not found.` });
        }

        const fields = [];
        const values = [];

        const updatable = {
            asin: 'asin', productName: 'product_name', quantity: 'quantity',
            poNumber: 'po_number', supplier: 'supplier', containerNumber: 'container_number',
            vesselName: 'vessel_name', eta: 'eta', cbmPerUnit: 'cbm_per_unit',
            packSize: 'pack_size', cbmPerPack: 'cbm_per_pack', notes: 'notes',
            location: 'location', containerizedLocation: 'containerized_location', expectedShippingDate: 'expected_shipping_date',
        };

        for (const [key, col] of Object.entries(updatable)) {
            if (b[key] !== undefined) {
                fields.push(`${col} = ?`);
                values.push(b[key]);
            }
        }

        if (b.dates !== undefined) {
            fields.push('dates = ?');
            values.push(JSON.stringify(b.dates));
        }

        if (fields.length === 0) {
            return res.status(400).json({ error: 'No fields to update.' });
        }

        values.push(id);
        await connection.execute(`UPDATE orders SET ${fields.join(', ')} WHERE id = ?`, values);

        const [rows] = await connection.query('SELECT * FROM orders WHERE id = ?', [id]);
        res.json(rowToOrder(rows[0]));
    } catch (error) {
        console.error('[PUT /orders/:id]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    } finally {
        if (connection) connection.release();
    }
});

// ── 4. PATCH /api/v1/orders/:id/status — Move order to new status ────────
app.patch('/api/v1/orders/:id/status', async (req, res) => {
    let connection;
    try {
        await ensureOrdersTable();
        const { id } = req.params;
        const { status } = req.body || {};

        if (!status) {
            return res.status(400).json({ error: 'status is required.' });
        }

        connection = await pool.getConnection();

        const [existing] = await connection.query('SELECT * FROM orders WHERE id = ?', [id]);
        if (!existing.length) {
            return res.status(404).json({ error: `Order ${id} not found.` });
        }

        let dates = existing[0].dates;
        if (typeof dates === 'string') dates = JSON.parse(dates);
        dates = dates || {};
        setDateKey(dates, status);

        await connection.execute(
            'UPDATE orders SET status = ?, dates = ? WHERE id = ?',
            [status, JSON.stringify(dates), id]
        );

        const [rows] = await connection.query('SELECT * FROM orders WHERE id = ?', [id]);
        res.json(rowToOrder(rows[0]));
    } catch (error) {
        console.error('[PATCH /orders/:id/status]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    } finally {
        if (connection) connection.release();
    }
});

// ── 5. POST /api/v1/orders/:id/split — Split order into container ────────
app.post('/api/v1/orders/:id/split', async (req, res) => {
    let connection;
    try {
        await ensureOrdersTable();
        const { id } = req.params;
        const { splitQuantity, containerNumber } = req.body || {};

        if (!splitQuantity || splitQuantity <= 0) {
            return res.status(400).json({ error: 'splitQuantity must be a positive number.' });
        }
        if (!containerNumber) {
            return res.status(400).json({ error: 'containerNumber is required.' });
        }

        connection = await pool.getConnection();
        await connection.beginTransaction();

        try {
            const [existing] = await connection.query('SELECT * FROM orders WHERE id = ?', [id]);
            if (!existing.length) {
                await connection.rollback();
                return res.status(404).json({ error: `Order ${id} not found.` });
            }

            const original = existing[0];
            if (splitQuantity >= original.quantity) {
                await connection.rollback();
                return res.status(400).json({ error: 'splitQuantity must be less than the original order quantity.' });
            }

            const newQty = original.quantity - splitQuantity;
            await connection.execute('UPDATE orders SET quantity = ? WHERE id = ?', [newQty, id]);

            const newId = generateOrderId();
            let originalDates = original.dates;
            if (typeof originalDates === 'string') originalDates = JSON.parse(originalDates);
            const newDates = { ...(originalDates || {}) };
            setDateKey(newDates, 'CONTAINERIZED');

            await connection.execute(
                `INSERT INTO orders (id, asin, product_name, quantity, status, po_number, supplier,
                    container_number, vessel_name, eta, cbm_per_unit, pack_size, cbm_per_pack, notes, dates)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                [
                    newId,
                    original.asin,
                    original.product_name,
                    splitQuantity,
                    'CONTAINERIZED',
                    original.po_number,
                    original.supplier,
                    containerNumber,
                    original.vessel_name,
                    original.eta,
                    original.cbm_per_unit,
                    original.pack_size,
                    original.cbm_per_pack,
                    original.notes,
                    JSON.stringify(newDates),
                ]
            );

            await connection.commit();

            const [updatedOrig] = await connection.query('SELECT * FROM orders WHERE id = ?', [id]);
            const [newOrder] = await connection.query('SELECT * FROM orders WHERE id = ?', [newId]);

            res.json({
                originalOrder: rowToOrder(updatedOrig[0]),
                newOrder: rowToOrder(newOrder[0]),
            });
        } catch (err) {
            await connection.rollback();
            throw err;
        }
    } catch (error) {
        console.error('[POST /orders/:id/split]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    } finally {
        if (connection) connection.release();
    }
});

// ── 6. POST /api/v1/containers/pack — Bulk pack orders into container ────
app.post('/api/v1/containers/pack', async (req, res) => {
    let connection;
    try {
        await ensureOrdersTable();
        const { containerNumber, vesselName, eta, packs } = req.body || {};

        if (!containerNumber) {
            return res.status(400).json({ error: 'containerNumber is required.' });
        }
        if (!packs || !Array.isArray(packs) || packs.length === 0) {
            return res.status(400).json({ error: 'packs array is required and must not be empty.' });
        }

        connection = await pool.getConnection();
        await connection.beginTransaction();

        try {
            const affected = [];

            for (const pack of packs) {
                const { orderId, qty } = pack;
                if (!orderId || !qty || qty <= 0) {
                    await connection.rollback();
                    return res.status(400).json({ error: `Invalid pack entry: orderId and qty > 0 required.` });
                }

                const [existing] = await connection.query('SELECT * FROM orders WHERE id = ?', [orderId]);
                if (!existing.length) {
                    await connection.rollback();
                    return res.status(404).json({ error: `Order ${orderId} not found.` });
                }

                const order = existing[0];

                if (qty === order.quantity) {
                    // Full pack — update in place
                    let dates = order.dates;
                    if (typeof dates === 'string') dates = JSON.parse(dates);
                    dates = dates || {};
                    setDateKey(dates, 'CONTAINERIZED');

                    await connection.execute(
                        `UPDATE orders SET status = 'CONTAINERIZED', container_number = ?, vessel_name = ?, eta = ?, dates = ? WHERE id = ?`,
                        [containerNumber, vesselName || null, eta || null, JSON.stringify(dates), orderId]
                    );

                    affected.push(orderId);
                } else if (qty < order.quantity) {
                    // Partial pack — split
                    const newQty = order.quantity - qty;
                    await connection.execute('UPDATE orders SET quantity = ? WHERE id = ?', [newQty, orderId]);
                    affected.push(orderId);

                    const newId = generateOrderId();
                    let originalDates = order.dates;
                    if (typeof originalDates === 'string') originalDates = JSON.parse(originalDates);
                    const newDates = { ...(originalDates || {}) };
                    setDateKey(newDates, 'CONTAINERIZED');

                    await connection.execute(
                        `INSERT INTO orders (id, asin, product_name, quantity, status, po_number, supplier,
                            container_number, vessel_name, eta, cbm_per_unit, pack_size, cbm_per_pack, notes, dates)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                        [
                            newId,
                            order.asin,
                            order.product_name,
                            qty,
                            'CONTAINERIZED',
                            order.po_number,
                            order.supplier,
                            containerNumber,
                            vesselName || null,
                            eta || null,
                            order.cbm_per_unit,
                            order.pack_size,
                            order.cbm_per_pack,
                            order.notes,
                            JSON.stringify(newDates),
                        ]
                    );

                    affected.push(newId);
                } else {
                    await connection.rollback();
                    return res.status(400).json({ error: `qty (${qty}) exceeds order ${orderId} quantity (${order.quantity}).` });
                }
            }

            await connection.commit();

            // Fetch all affected orders
            const ph = affected.map(() => '?').join(',');
            const [rows] = await connection.query(`SELECT * FROM orders WHERE id IN (${ph})`, affected);
            res.json({ data: rows.map(rowToOrder) });
        } catch (err) {
            await connection.rollback();
            throw err;
        }
    } catch (error) {
        console.error('[POST /containers/pack]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    } finally {
        if (connection) connection.release();
    }
});

// ── 7. PATCH /api/v1/containers/:containerNumber/status — Move container ──
app.patch('/api/v1/containers/:containerNumber/status', async (req, res) => {
    let connection;
    try {
        await ensureOrdersTable();
        const { containerNumber } = req.params;
        const { status } = req.body || {};

        if (!status) {
            return res.status(400).json({ error: 'status is required.' });
        }

        connection = await pool.getConnection();

        const [orders] = await connection.query(
            'SELECT * FROM orders WHERE container_number = ?', [containerNumber]
        );

        if (!orders.length) {
            return res.status(404).json({ error: `No orders found for container ${containerNumber}.` });
        }

        for (const order of orders) {
            let dates = order.dates;
            if (typeof dates === 'string') dates = JSON.parse(dates);
            dates = dates || {};
            setDateKey(dates, status);

            await connection.execute(
                'UPDATE orders SET status = ?, dates = ? WHERE id = ?',
                [status, JSON.stringify(dates), order.id]
            );
        }

        const [updated] = await connection.query(
            'SELECT * FROM orders WHERE container_number = ?', [containerNumber]
        );
        res.json({ data: updated.map(rowToOrder) });
    } catch (error) {
        console.error('[PATCH /containers/:containerNumber/status]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    } finally {
        if (connection) connection.release();
    }
});


// ─── Constants ────────────────────────────────────────────────────────────────

const ASIN_PATTERN = /^[A-Z0-9]{10}$/;
const QUERY_TIMEOUT_MS = 10_000;

/**
 * Wraps a promise with a timeout. Rejects if not settled within ms.
 */
function withTimeout(promise, ms, label = 'Query') {
    let timer;
    const timeout = new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
    });
    return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

/**
 * Parse and validate an ASIN from raw input. Returns the clean ASIN or null.
 */
function parseAsin(raw) {
    if (!raw) return null;
    const cleaned = String(raw).trim().toUpperCase();
    return ASIN_PATTERN.test(cleaned) ? cleaned : null;
}

// ─── Route handler ────────────────────────────────────────────────────────────

app.get('/api/v1/stock-snapshots/sum', async (req, res) => {
    res.set('Cache-Control', 'private, max-age=60');

    try {
        const cleanAsin = parseAsin(req.query.asin);

        if (!cleanAsin) {
            return res.status(400).json({
                error: 'A valid 10-character alphanumeric ASIN is required.',
            });
        }

        // Get latest snapshot dates (fallback to last available from any source)
        const [
            [msTodayRows],
            [amzTodayRows],
            [msFallbackRows],
            [amzFallbackRows],
        ] = await withTimeout(
            Promise.all([
                pool.query(`SELECT MAX(date_ran) as latest FROM stock_snapshots WHERE asin = ? AND date_ran = CURDATE()`, [cleanAsin]),
                pool.query(`SELECT MAX(date_ran) as latest FROM amazon_stock_country_snapshots WHERE asin = ? AND date_ran = CURDATE()`, [cleanAsin]),
                pool.query(`SELECT MAX(date_ran) as latest FROM stock_snapshots WHERE asin = ?`, [cleanAsin]),
                pool.query(`SELECT MAX(date_ran) as latest FROM amazon_stock_country_snapshots WHERE asin = ?`, [cleanAsin]),
            ]),
            QUERY_TIMEOUT_MS,
            'Date lookup'
        );

        let msDate = msTodayRows[0]?.latest ?? msFallbackRows[0]?.latest ?? null;
        let amzDate = amzTodayRows[0]?.latest ?? amzFallbackRows[0]?.latest ?? null;

        // Parallel data fetch
        const days = parseInt(req.query.days, 10) || 30;

        const [msRows, amzRows, orderRows, onSeaRows, msHistoryRows, amzHistoryRows] = await withTimeout(
            Promise.all([
                msDate
                    ? pool.query(
                        `SELECT
                            CAST(COALESCE(SUM(stock_level), 0) AS UNSIGNED)  as total_stock_level,
                            CAST(COALESCE(SUM(available), 0) AS UNSIGNED)    as total_available,
                            CAST(COALESCE(SUM(allocated), 0) AS UNSIGNED)    as total_allocated,
                            CAST(COALESCE(SUM(quarantine), 0) AS UNSIGNED)   as total_quarantine
                         FROM stock_snapshots
                         WHERE date_ran = ? AND asin = ?`,
                        [msDate, cleanAsin]
                    ).then(([rows]) => rows)
                    : Promise.resolve([{
                        total_stock_level: 0, total_available: 0,
                        total_allocated: 0, total_quarantine: 0,
                    }]),

                amzDate
                    ? pool.query(
                        `SELECT country,
                            CAST(COALESCE(SUM(fulfillable), 0) AS UNSIGNED)       as amazon_fulfillable,
                            CAST(COALESCE(SUM(inbound_working), 0) AS UNSIGNED)   as amazon_inbound_working,
                            CAST(COALESCE(SUM(inbound_shipped), 0) AS UNSIGNED)   as amazon_inbound_shipped,
                            CAST(COALESCE(SUM(inbound_receiving), 0) AS UNSIGNED) as amazon_inbound_receiving,
                            CAST(COALESCE(SUM(reserved), 0) AS UNSIGNED)          as amazon_reserved
                         FROM amazon_stock_country_snapshots
                         WHERE date_ran = ? AND asin = ?
                         GROUP BY country`,
                        [amzDate, cleanAsin]
                    ).then(([rows]) => rows)
                    : Promise.resolve([]),

                pool.query(
                    `SELECT status,
                            CAST(COALESCE(SUM(quantity), 0) AS UNSIGNED) as total_quantity
                     FROM orders
                     WHERE asin = ?
                     GROUP BY status`,
                    [cleanAsin]
                ).then(([rows]) => rows),

                pool.query(
                    `SELECT id, product_name, quantity, eta, container_number, supplier, po_number
                     FROM orders
                     WHERE asin = ? AND status = 'ON_SEA'
                     ORDER BY eta ASC`,
                    [cleanAsin]
                ).then(([rows]) => rows),

                pool.query(
                    `SELECT date_ran,
                        CAST(COALESCE(SUM(stock_level), 0) AS UNSIGNED)  as stock_level,
                        CAST(COALESCE(SUM(available), 0) AS UNSIGNED)    as available,
                        CAST(COALESCE(SUM(allocated), 0) AS UNSIGNED)    as allocated,
                        CAST(COALESCE(SUM(quarantine), 0) AS UNSIGNED)   as quarantine
                     FROM stock_snapshots
                     WHERE asin = ? AND date_ran >= DATE_SUB(CURDATE(), INTERVAL ? DAY)
                     GROUP BY date_ran
                     ORDER BY date_ran`,
                    [cleanAsin, days]
                ).then(([rows]) => rows),

                pool.query(
                    `SELECT date_ran,
                        CAST(COALESCE(SUM(fulfillable), 0) AS UNSIGNED)       as fulfillable,
                        CAST(COALESCE(SUM(inbound_working), 0) AS UNSIGNED)   as inbound_working,
                        CAST(COALESCE(SUM(inbound_shipped), 0) AS UNSIGNED)   as inbound_shipped,
                        CAST(COALESCE(SUM(inbound_receiving), 0) AS UNSIGNED) as inbound_receiving,
                        CAST(COALESCE(SUM(reserved), 0) AS UNSIGNED)          as reserved
                     FROM amazon_stock_country_snapshots
                     WHERE asin = ? AND date_ran >= DATE_SUB(CURDATE(), INTERVAL ? DAY)
                     GROUP BY date_ran
                     ORDER BY date_ran`,
                    [cleanAsin, days]
                ).then(([rows]) => rows),
            ]),
            QUERY_TIMEOUT_MS,
            'Data fetch'
        );

        const amazon_stock_by_country = {};
        const totals = { amazon_fulfillable: 0, amazon_inbound_working: 0, amazon_inbound_shipped: 0, amazon_inbound_receiving: 0, amazon_reserved: 0 };
        for (const row of amzRows) {
            const country = row.country?.trim().toUpperCase();
            if (!country) continue;
            const entry = {
                fulfillable: Number(row.amazon_fulfillable || 0),
                inbound_working: Number(row.amazon_inbound_working || 0),
                inbound_shipped: Number(row.amazon_inbound_shipped || 0),
                inbound_receiving: Number(row.amazon_inbound_receiving || 0),
                reserved: Number(row.amazon_reserved || 0),
            };
            amazon_stock_by_country[country] = entry;
            totals.amazon_fulfillable       += entry.fulfillable;
            totals.amazon_inbound_working   += entry.inbound_working;
            totals.amazon_inbound_shipped   += entry.inbound_shipped;
            totals.amazon_inbound_receiving += entry.inbound_receiving;
            totals.amazon_reserved          += entry.reserved;
        }
        totals.amazon_total = totals.amazon_fulfillable + totals.amazon_inbound_working + totals.amazon_inbound_shipped + totals.amazon_inbound_receiving + totals.amazon_reserved;

        const ALL_STATUSES = [
            'SCHEDULED', 'PO_SENT', 'UNDER_PRODUCTION', 'READY_AT_FACTORY',
            'CONSOLIDATED', 'ON_SEA', 'IN_WAREHOUSE', 'MINTSOFT',
        ];
        const orders = Object.fromEntries(ALL_STATUSES.map(s => [s, 0]));
        for (const row of orderRows) {
            orders[row.status] = Number(row.total_quantity || 0);
        }

        const msStats = msRows[0] || {};

        res.json({
            data: {
                mintsoft_date: msDate,
                amazon_date: amzDate,
                mintsoft_stock_level: Number(msStats.total_stock_level || 0),
                mintsoft_available: Number(msStats.total_available || 0),
                mintsoft_allocated: Number(msStats.total_allocated || 0),
                mintsoft_quarantine: Number(msStats.total_quarantine || 0),
                ...totals,
                amazon_stock_by_country,
                orders,
                goods_on_sea: onSeaRows.map(r => ({
                    id: r.id,
                    productName: r.product_name,
                    quantity: Number(r.quantity),
                    eta: r.eta ? (r.eta.toISOString?.().slice(0, 10) ?? r.eta) : null,
                    containerNumber: r.container_number || null,
                    supplier: r.supplier || null,
                    poNumber: r.po_number || null,
                })),
                history: {
                    mintsoft: msHistoryRows.map(r => ({
                        date: r.date_ran,
                        stock_level: Number(r.stock_level || 0),
                        available: Number(r.available || 0),
                        allocated: Number(r.allocated || 0),
                        quarantine: Number(r.quarantine || 0),
                    })),
                    amazon: amzHistoryRows.map(r => ({
                        date: r.date_ran,
                        fulfillable: Number(r.fulfillable || 0),
                        inbound_working: Number(r.inbound_working || 0),
                        inbound_shipped: Number(r.inbound_shipped || 0),
                        inbound_receiving: Number(r.inbound_receiving || 0),
                        reserved: Number(r.reserved || 0),
                    })),
                },
            },
        });
    } catch (error) {
        console.error('[GET /stock-snapshots/sum]', error);

        const status = error.message?.includes('timed out') ? 504 : 500;
        res.status(status).json({ error: 'An internal error occurred.' });
    }
});

// ── 8. GET /api/v1/stock-snapshots/sum/all-asins — Full summary for each ASIN ──
app.get('/api/v1/stock-snapshots/sum/all-asins', async (_req, res) => {
    res.set('Cache-Control', 'private, max-age=60');

    try {
        // Get all distinct ASINs from all three tables
        const [
            [asinsFromOrders],
            [asinsFromMintsoft],
            [asinsFromAmazon],
        ] = await withTimeout(
            Promise.all([
                pool.query(`SELECT DISTINCT asin FROM orders WHERE asin IS NOT NULL AND asin != ''`),
                pool.query(`SELECT DISTINCT asin FROM stock_snapshots WHERE asin IS NOT NULL AND asin != ''`),
                pool.query(`SELECT DISTINCT asin FROM amazon_stock_country_snapshots WHERE asin IS NOT NULL AND asin != ''`),
            ]),
            QUERY_TIMEOUT_MS,
            'ASIN list fetch'
        );

        const allAsins = new Set([
            ...asinsFromOrders.map(r => r.asin),
            ...asinsFromMintsoft.map(r => r.asin),
            ...asinsFromAmazon.map(r => r.asin),
        ]);

        // Batch fetch all data for all ASINs
        const [
            [msDates],
            [amzDates],
            [orderStats],
            [onSeaOrders],
            [msLatestRows],
            [amzLatestRows],
        ] = await withTimeout(
            Promise.all([
                // Latest mintsoft dates by ASIN
                pool.query(`
                    SELECT asin, MAX(date_ran) as latest
                    FROM stock_snapshots WHERE asin IS NOT NULL AND asin != ''
                    GROUP BY asin ORDER BY asin
                `),
                // Latest amazon dates by ASIN
                pool.query(`
                    SELECT asin, MAX(date_ran) as latest
                    FROM amazon_stock_country_snapshots WHERE asin IS NOT NULL AND asin != ''
                    GROUP BY asin ORDER BY asin
                `),
                // Orders by status and ASIN
                pool.query(`
                    SELECT asin, status,
                           CAST(COALESCE(SUM(quantity), 0) AS UNSIGNED) as total_quantity
                    FROM orders WHERE asin IS NOT NULL
                    GROUP BY asin, status
                `),
                // All goods on sea
                pool.query(`
                    SELECT asin, id, product_name, quantity, eta, container_number, supplier, po_number
                    FROM orders
                    WHERE status = 'ON_SEA' AND asin IS NOT NULL
                    ORDER BY asin, eta ASC
                `),
                // Latest mintsoft snapshots
                pool.query(`
                    WITH LatestDates AS (
                        SELECT asin, MAX(date_ran) as latest FROM stock_snapshots
                        WHERE asin IS NOT NULL AND asin != '' GROUP BY asin
                    )
                    SELECT s.asin, s.date_ran,
                           CAST(COALESCE(SUM(s.stock_level), 0) AS UNSIGNED) as total_stock_level,
                           CAST(COALESCE(SUM(s.available), 0) AS UNSIGNED) as total_available,
                           CAST(COALESCE(SUM(s.allocated), 0) AS UNSIGNED) as total_allocated,
                           CAST(COALESCE(SUM(s.quarantine), 0) AS UNSIGNED) as total_quarantine
                    FROM stock_snapshots s
                    JOIN LatestDates ld ON s.asin = ld.asin AND s.date_ran = ld.latest
                    GROUP BY s.asin, s.date_ran
                `),
                // Latest amazon snapshots by country
                pool.query(`
                    WITH LatestDates AS (
                        SELECT asin, MAX(date_ran) as latest FROM amazon_stock_country_snapshots
                        WHERE asin IS NOT NULL AND asin != '' GROUP BY asin
                    )
                    SELECT a.asin, a.country, a.date_ran,
                           CAST(COALESCE(SUM(a.fulfillable), 0) AS UNSIGNED) as amazon_fulfillable,
                           CAST(COALESCE(SUM(a.inbound_working), 0) AS UNSIGNED) as amazon_inbound_working,
                           CAST(COALESCE(SUM(a.inbound_shipped), 0) AS UNSIGNED) as amazon_inbound_shipped,
                           CAST(COALESCE(SUM(a.inbound_receiving), 0) AS UNSIGNED) as amazon_inbound_receiving,
                           CAST(COALESCE(SUM(a.reserved), 0) AS UNSIGNED) as amazon_reserved
                    FROM amazon_stock_country_snapshots a
                    JOIN LatestDates ld ON a.asin = ld.asin AND a.date_ran = ld.latest
                    GROUP BY a.asin, a.country, a.date_ran
                `),
            ]),
            QUERY_TIMEOUT_MS,
            'Batch data fetch'
        );

        // Aggregate in-memory
        const msDatesMap = Object.fromEntries(msDates.map(r => [r.asin, r.latest]));
        const amzDatesMap = Object.fromEntries(amzDates.map(r => [r.asin, r.latest]));

        const results = {};
        const ALL_STATUSES = [
            'SCHEDULED', 'PO_SENT', 'UNDER_PRODUCTION', 'READY_AT_FACTORY',
            'CONSOLIDATED', 'ON_SEA', 'IN_WAREHOUSE', 'MINTSOFT',
        ];

        for (const asin of allAsins) {
            if (!asin || !parseAsin(asin)) continue;

            const msStats = msLatestRows.find(r => r.asin === asin) || {};
            const amzByCountry = amzLatestRows.filter(r => r.asin === asin);
            const ordersByStatus = orderStats.filter(r => r.asin === asin);
            const seaOrders = onSeaOrders.filter(r => r.asin === asin);

            // Aggregate amazon by country
            const amazon_stock_by_country = {};
            const totals = { amazon_fulfillable: 0, amazon_inbound_working: 0, amazon_inbound_shipped: 0, amazon_inbound_receiving: 0, amazon_reserved: 0 };
            for (const row of amzByCountry) {
                const country = row.country?.trim().toUpperCase();
                if (!country) continue;
                const entry = {
                    fulfillable: Number(row.amazon_fulfillable || 0),
                    inbound_working: Number(row.amazon_inbound_working || 0),
                    inbound_shipped: Number(row.amazon_inbound_shipped || 0),
                    inbound_receiving: Number(row.amazon_inbound_receiving || 0),
                    reserved: Number(row.amazon_reserved || 0),
                };
                amazon_stock_by_country[country] = entry;
                totals.amazon_fulfillable       += entry.fulfillable;
                totals.amazon_inbound_working   += entry.inbound_working;
                totals.amazon_inbound_shipped   += entry.inbound_shipped;
                totals.amazon_inbound_receiving += entry.inbound_receiving;
                totals.amazon_reserved          += entry.reserved;
            }
            totals.amazon_total = totals.amazon_fulfillable + totals.amazon_inbound_working + totals.amazon_inbound_shipped + totals.amazon_inbound_receiving + totals.amazon_reserved;

            // Build orders by status
            const orders = Object.fromEntries(ALL_STATUSES.map(s => [s, 0]));
            for (const row of ordersByStatus) {
                orders[row.status] = Number(row.total_quantity || 0);
            }

            results[asin] = {
                mintsoft_date: msDatesMap[asin] || null,
                amazon_date: amzDatesMap[asin] || null,
                mintsoft_stock_level: Number(msStats.total_stock_level || 0),
                mintsoft_available: Number(msStats.total_available || 0),
                mintsoft_allocated: Number(msStats.total_allocated || 0),
                mintsoft_quarantine: Number(msStats.total_quarantine || 0),
                ...totals,
                amazon_stock_by_country,
                orders,
                goods_on_sea: seaOrders.map(r => ({
                    id: r.id,
                    productName: r.product_name,
                    quantity: Number(r.quantity),
                    eta: r.eta ? (r.eta.toISOString?.().slice(0, 10) ?? r.eta) : null,
                    containerNumber: r.container_number || null,
                    supplier: r.supplier || null,
                    poNumber: r.po_number || null,
                })),
            };
        }

        res.json({ data: results });
    } catch (error) {
        console.error('[GET /stock-snapshots/sum/all-asins]', error);
        const status = error.message?.includes('timed out') ? 504 : 500;
        res.status(status).json({ error: 'An internal error occurred.' });
    }
});

// ── Serverless export ─────────────────────────────────────────────────────
const serverlessApp = serverless(app, {
    binary: ['application/json', 'image/*', 'application/javascript']
});

module.exports.handler = async (event, context) => {
    context.callbackWaitsForEmptyEventLoop = false;
    return await serverlessApp(event, context);
};

if (require.main === module) {
    const PORT = process.env.ORDERS_PORT || 3001;
    app.listen(PORT, () => {
        console.log(`Orders API running on http://localhost:${PORT}`);
    });
}
