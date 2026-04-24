const serverless = require('serverless-http');
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const compression = require('compression');
const axios = require('axios');
const { v4: uuidv4 } = require('uuid');
const { getPool } = require('../db');
const log = require('../lib/logger');

const app = express();

// ── Middleware ────────────────────────────────────────────────────────────
app.use(compression());
app.use(cors());
app.use(express.json());

// Database pool — declared before middleware that depends on it
const pool = getPool();

// ── Email whitelist middleware ────────────────────────────────────────────
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
        log.error('[auth-middleware]', err);
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

// ── Constants ────────────────────────────────────────────────────────────

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

const ALL_STATUSES = [
    'SCHEDULED', 'PO_SENT', 'CONSOLIDATED', 'ON_SEA', 'ON_AIR', 'IN_WAREHOUSE', 'MINTSOFT',
];

const ASIN_PATTERN = /^[A-Z0-9]{10}$/;
const QUERY_TIMEOUT_MS = 10_000;

const UPDATABLE_FIELDS = {
    asin: 'asin', productName: 'product_name', quantity: 'quantity',
    poNumber: 'po_number', supplier: 'supplier', containerNumber: 'container_number',
    vesselName: 'vessel_name', eta: 'eta', cbmPerUnit: 'cbm_per_unit',
    packSize: 'pack_size', cbmPerPack: 'cbm_per_pack', notes: 'notes',
    location: 'location', containerizedLocation: 'containerized_location',
    expectedShippingDate: 'expected_shipping_date',
    deliveryDate: 'delivery_date',
};

const ORDER_INSERT_COLS = `(id, asin, product_name, quantity, status, po_number, supplier,
    container_number, vessel_name, eta, cbm_per_unit, pack_size, cbm_per_pack, notes, dates)`;
const ORDER_INSERT_PLACEHOLDERS = '(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)';

// ── Helpers ──────────────────────────────────────────────────────────────

function generateOrderId() {
    return `ORD-${uuidv4().slice(0, 8).toUpperCase()}`;
}

function parseDates(raw) {
    if (typeof raw === 'string') return JSON.parse(raw) || {};
    return raw || {};
}

function setDateKey(dates, status) {
    const key = STATUS_DATE_KEY[status];
    if (key) dates[key] = new Date().toISOString();
    return dates;
}

function formatDate(val) {
    if (!val) return null;
    return val.toISOString?.().slice(0, 10) ?? val;
}

function rowToOrder(row) {
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
        eta: formatDate(row.eta),
        cbmPerUnit: row.cbm_per_unit != null ? Number(row.cbm_per_unit) : null,
        packSize: row.pack_size != null ? Number(row.pack_size) : null,
        cbmPerPack: row.cbm_per_pack != null ? Number(row.cbm_per_pack) : null,
        notes: row.notes || null,
        dates: parseDates(row.dates),
        location: row.location || null,
        containerizedLocation: row.containerized_location || null,
        expectedShippingDate: formatDate(row.expected_shipping_date),
        deliveryDate: formatDate(row.delivery_date),
    };
}

function orderInsertValues(order, id, status, dates, quantity) {
    return [
        id,
        order.asin || null,
        order.product_name || null,
        quantity ?? order.quantity ?? 0,
        status,
        order.po_number || null,
        order.supplier || null,
        order.container_number || null,
        order.vessel_name || null,
        order.eta || null,
        order.cbm_per_unit ?? null,
        order.pack_size ?? null,
        order.cbm_per_pack ?? null,
        order.notes || null,
        JSON.stringify(dates),
    ];
}

async function withConnection(fn) {
    const connection = await pool.getConnection();
    try {
        return await fn(connection);
    } finally {
        connection.release();
    }
}

function withTimeout(promise, ms, label = 'Query') {
    let timer;
    const timeout = new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
    });
    return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

function parseAsin(raw) {
    if (!raw) return null;
    const cleaned = String(raw).trim().toUpperCase();
    return ASIN_PATTERN.test(cleaned) ? cleaned : null;
}

function aggregateAmazonStock(rows) {
    const byCountry = {};
    const totals = {
        amazon_fulfillable: 0, amazon_inbound_working: 0,
        amazon_inbound_shipped: 0, amazon_inbound_receiving: 0, amazon_reserved: 0,
    };
    for (const row of rows) {
        const country = row.country?.trim().toUpperCase();
        if (!country) continue;
        const entry = {
            fulfillable: Number(row.amazon_fulfillable || 0),
            inbound_working: Number(row.amazon_inbound_working || 0),
            inbound_shipped: Number(row.amazon_inbound_shipped || 0),
            inbound_receiving: Number(row.amazon_inbound_receiving || 0),
            reserved: Number(row.amazon_reserved || 0),
        };
        byCountry[country] = entry;
        totals.amazon_fulfillable       += entry.fulfillable;
        totals.amazon_inbound_working   += entry.inbound_working;
        totals.amazon_inbound_shipped   += entry.inbound_shipped;
        totals.amazon_inbound_receiving += entry.inbound_receiving;
        totals.amazon_reserved          += entry.reserved;
    }
    totals.amazon_total = totals.amazon_fulfillable + totals.amazon_inbound_working
        + totals.amazon_inbound_shipped + totals.amazon_inbound_receiving + totals.amazon_reserved;
    return { byCountry, totals };
}

function mapGoodsOnSeaRow(r) {
    const dates = parseDates(r.dates);
    if (r.delivery_date) dates.delivery = formatDate(r.delivery_date);
    return {
        id: r.id,
        productName: r.product_name,
        quantity: Number(r.quantity),
        eta: formatDate(r.eta),
        containerNumber: r.container_number || null,
        supplier: r.supplier || null,
        poNumber: r.po_number || null,
        dates,
    };
}

function mapGoodsOnAirRow(r) {
    return {
        id: r.id,
        productName: r.product_name,
        quantity: Number(r.quantity),
        eta: formatDate(r.eta),
        containerNumber: r.container_number || null,
        supplier: r.supplier || null,
        poNumber: r.po_number || null,
        dates: parseDates(r.dates),
    };
}

function mapOrderBreakdownRow(r) {
    return {
        id: r.id,
        productName: r.product_name,
        quantity: Number(r.quantity),
        status: r.status,
        poNumber: r.po_number || null,
        supplier: r.supplier || null,
        dates: parseDates(r.dates),
    };
}

function buildOrderStatusMap(rows) {
    const orders = Object.fromEntries(ALL_STATUSES.map(s => [s, 0]));
    for (const row of rows) {
        orders[row.status] = Number(row.total_quantity || 0);
    }
    return orders;
}

// ── 1. GET /api/v1/orders ────────────────────────────────────────────────
app.get('/api/v1/orders', async (req, res) => {
    try {
        const rows = await withConnection(async (conn) => {
            const [rows] = await conn.query('SELECT * FROM orders ORDER BY created_at DESC');
            return rows;
        });
        res.status(200).json({ data: rows.map(rowToOrder) });
    } catch (error) {
        log.error('[GET /orders]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── 2. POST /api/v1/orders ───────────────────────────────────────────────
app.post('/api/v1/orders', async (req, res) => {
    try {
        const order = await withConnection(async (conn) => {
            const b = req.body || {};
            const id = generateOrderId();
            const status = 'PLANNING';
            const dates = setDateKey({}, status);

            await conn.execute(
                `INSERT INTO orders ${ORDER_INSERT_COLS} VALUES ${ORDER_INSERT_PLACEHOLDERS}`,
                [
                    id, b.asin || null, b.productName || null, b.quantity || 0, status,
                    b.poNumber || null, b.supplier || null, b.containerNumber || null,
                    b.vesselName || null, b.eta || null, b.cbmPerUnit ?? null,
                    b.packSize ?? null, b.cbmPerPack ?? null, b.notes || null,
                    JSON.stringify(dates),
                ]
            );

            const [rows] = await conn.query('SELECT * FROM orders WHERE id = ?', [id]);
            return rowToOrder(rows[0]);
        });
        res.status(201).json(order);
    } catch (error) {
        log.error('[POST /orders]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── 3. PUT /api/v1/orders/:id ────────────────────────────────────────────
app.put('/api/v1/orders/:id', async (req, res) => {
    try {
        const result = await withConnection(async (conn) => {
            const { id } = req.params;
            const b = req.body || {};

            const [existing] = await conn.query('SELECT * FROM orders WHERE id = ?', [id]);
            if (!existing.length) return { notFound: id };

            const fields = [];
            const values = [];

            for (const [key, col] of Object.entries(UPDATABLE_FIELDS)) {
                if (b[key] !== undefined) {
                    fields.push(`${col} = ?`);
                    values.push(b[key]);
                }
            }

            if (b.dates !== undefined) {
                fields.push('dates = ?');
                values.push(JSON.stringify(b.dates));
            }

            if (fields.length === 0) return { noFields: true };

            values.push(id);
            await conn.execute(`UPDATE orders SET ${fields.join(', ')} WHERE id = ?`, values);

            const [rows] = await conn.query('SELECT * FROM orders WHERE id = ?', [id]);
            return { order: rowToOrder(rows[0]) };
        });

        if (result.notFound) return res.status(404).json({ error: `Order ${result.notFound} not found.` });
        if (result.noFields) return res.status(400).json({ error: 'No fields to update.' });
        res.json(result.order);
    } catch (error) {
        log.error('[PUT /orders/:id]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── 4. PATCH /api/v1/orders/:id/status ───────────────────────────────────
app.patch('/api/v1/orders/:id/status', async (req, res) => {
    try {
        const { status } = req.body || {};
        if (!status) return res.status(400).json({ error: 'status is required.' });

        const result = await withConnection(async (conn) => {
            const { id } = req.params;

            const [existing] = await conn.query('SELECT * FROM orders WHERE id = ?', [id]);
            if (!existing.length) return { notFound: id };

            const dates = parseDates(existing[0].dates);
            setDateKey(dates, status);

            await conn.execute(
                'UPDATE orders SET status = ?, dates = ? WHERE id = ?',
                [status, JSON.stringify(dates), id]
            );

            const [rows] = await conn.query('SELECT * FROM orders WHERE id = ?', [id]);
            return { order: rowToOrder(rows[0]) };
        });

        if (result.notFound) return res.status(404).json({ error: `Order ${result.notFound} not found.` });
        res.json(result.order);
    } catch (error) {
        log.error('[PATCH /orders/:id/status]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── 5. POST /api/v1/orders/:id/split ─────────────────────────────────────
app.post('/api/v1/orders/:id/split', async (req, res) => {
    try {
        const { splitQuantity, containerNumber } = req.body || {};

        if (!splitQuantity || splitQuantity <= 0) {
            return res.status(400).json({ error: 'splitQuantity must be a positive number.' });
        }
        if (!containerNumber) {
            return res.status(400).json({ error: 'containerNumber is required.' });
        }

        const result = await withConnection(async (conn) => {
            await conn.beginTransaction();
            try {
                const [existing] = await conn.query('SELECT * FROM orders WHERE id = ?', [req.params.id]);
                if (!existing.length) {
                    await conn.rollback();
                    return { notFound: req.params.id };
                }

                const original = existing[0];
                if (splitQuantity >= original.quantity) {
                    await conn.rollback();
                    return { badQuantity: true };
                }

                await conn.execute('UPDATE orders SET quantity = ? WHERE id = ?',
                    [original.quantity - splitQuantity, req.params.id]);

                const newId = generateOrderId();
                const newDates = setDateKey({ ...parseDates(original.dates) }, 'CONTAINERIZED');

                await conn.execute(
                    `INSERT INTO orders ${ORDER_INSERT_COLS} VALUES ${ORDER_INSERT_PLACEHOLDERS}`,
                    orderInsertValues({ ...original, container_number: containerNumber }, newId, 'CONTAINERIZED', newDates, splitQuantity)
                );

                await conn.commit();

                const [updatedOrig] = await conn.query('SELECT * FROM orders WHERE id = ?', [req.params.id]);
                const [newOrder] = await conn.query('SELECT * FROM orders WHERE id = ?', [newId]);
                return { originalOrder: rowToOrder(updatedOrig[0]), newOrder: rowToOrder(newOrder[0]) };
            } catch (err) {
                await conn.rollback();
                throw err;
            }
        });

        if (result.notFound) return res.status(404).json({ error: `Order ${result.notFound} not found.` });
        if (result.badQuantity) return res.status(400).json({ error: 'splitQuantity must be less than the original order quantity.' });
        res.status(201).json(result);
    } catch (error) {
        log.error('[POST /orders/:id/split]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── 6. POST /api/v1/containers/pack ──────────────────────────────────────
app.post('/api/v1/containers/pack', async (req, res) => {
    try {
        const { containerNumber, vesselName, eta, packs } = req.body || {};

        if (!containerNumber) {
            return res.status(400).json({ error: 'containerNumber is required.' });
        }
        if (!packs || !Array.isArray(packs) || packs.length === 0) {
            return res.status(400).json({ error: 'packs array is required and must not be empty.' });
        }

        const result = await withConnection(async (conn) => {
            await conn.beginTransaction();
            try {
                const affected = [];

                for (const pack of packs) {
                    const { orderId, qty } = pack;
                    if (!orderId || !qty || qty <= 0) {
                        await conn.rollback();
                        return { badPack: true };
                    }

                    const [existing] = await conn.query('SELECT * FROM orders WHERE id = ?', [orderId]);
                    if (!existing.length) {
                        await conn.rollback();
                        return { notFound: orderId };
                    }

                    const order = existing[0];

                    if (qty === order.quantity) {
                        // Full pack — update in place
                        const dates = setDateKey(parseDates(order.dates), 'CONTAINERIZED');
                        await conn.execute(
                            `UPDATE orders SET status = 'CONTAINERIZED', container_number = ?, vessel_name = ?, eta = ?, dates = ? WHERE id = ?`,
                            [containerNumber, vesselName || null, eta || null, JSON.stringify(dates), orderId]
                        );
                        affected.push(orderId);
                    } else if (qty < order.quantity) {
                        // Partial pack — split
                        await conn.execute('UPDATE orders SET quantity = ? WHERE id = ?',
                            [order.quantity - qty, orderId]);
                        affected.push(orderId);

                        const newId = generateOrderId();
                        const newDates = setDateKey({ ...parseDates(order.dates) }, 'CONTAINERIZED');

                        await conn.execute(
                            `INSERT INTO orders ${ORDER_INSERT_COLS} VALUES ${ORDER_INSERT_PLACEHOLDERS}`,
                            orderInsertValues(
                                { ...order, container_number: containerNumber, vessel_name: vesselName || null, eta: eta || null },
                                newId, 'CONTAINERIZED', newDates, qty
                            )
                        );
                        affected.push(newId);
                    } else {
                        await conn.rollback();
                        return { qtyExceeds: { qty, orderId, orderQty: order.quantity } };
                    }
                }

                await conn.commit();

                const ph = affected.map(() => '?').join(',');
                const [rows] = await conn.query(
                    `SELECT * FROM orders WHERE id IN (${ph}) ORDER BY created_at DESC`, affected
                );
                return { data: rows.map(rowToOrder) };
            } catch (err) {
                await conn.rollback();
                throw err;
            }
        });

        if (result.badPack) return res.status(400).json({ error: 'Invalid pack entry: orderId and qty > 0 required.' });
        if (result.notFound) return res.status(404).json({ error: `Order ${result.notFound} not found.` });
        if (result.qtyExceeds) {
            const { qty, orderId, orderQty } = result.qtyExceeds;
            return res.status(400).json({ error: `qty (${qty}) exceeds order ${orderId} quantity (${orderQty}).` });
        }
        res.json(result);
    } catch (error) {
        log.error('[POST /containers/pack]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── 7. PATCH /api/v1/containers/:containerNumber/status ──────────────────
app.patch('/api/v1/containers/:containerNumber/status', async (req, res) => {
    try {
        const { status } = req.body || {};
        if (!status) return res.status(400).json({ error: 'status is required.' });

        const result = await withConnection(async (conn) => {
            const { containerNumber } = req.params;
            const [orders] = await conn.query(
                'SELECT * FROM orders WHERE container_number = ?', [containerNumber]
            );
            if (!orders.length) return { notFound: containerNumber };

            for (const order of orders) {
                const dates = setDateKey(parseDates(order.dates), status);
                await conn.execute(
                    'UPDATE orders SET status = ?, dates = ? WHERE id = ?',
                    [status, JSON.stringify(dates), order.id]
                );
            }

            const [updated] = await conn.query(
                'SELECT * FROM orders WHERE container_number = ?', [containerNumber]
            );
            return { data: updated.map(rowToOrder) };
        });

        if (result.notFound) return res.status(404).json({ error: `No orders found for container ${result.notFound}.` });
        res.status(200).json(result);
    } catch (error) {
        log.error('[PATCH /containers/:containerNumber/status]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── 8. GET /api/v1/stock-snapshots/sum ───────────────────────────────────
app.get('/api/v1/stock-snapshots/sum', async (req, res) => {
    try {
        const cleanAsin = parseAsin(req.query.asin);
        if (!cleanAsin) {
            return res.status(400).json({ error: 'A valid 10-character alphanumeric ASIN is required.' });
        }
        const company = (req.query.company || '').trim();
        if (!company) {
            return res.status(400).json({ error: 'company query parameter is required.' });
        }

        const [
            [msTodayRows], [amzTodayRows], [msFallbackRows], [amzFallbackRows],
        ] = await withTimeout(
            Promise.all([
                pool.query(`SELECT MAX(date_ran) as latest FROM stock_snapshots WHERE asin = ? AND date_ran = CURDATE()`, [cleanAsin]),
                pool.query(`SELECT MAX(date_ran) as latest FROM amazon_stock_country_snapshots WHERE asin = ? AND company = ? AND date_ran = CURDATE()`, [cleanAsin, company]),
                pool.query(`SELECT MAX(date_ran) as latest FROM stock_snapshots WHERE asin = ?`, [cleanAsin]),
                pool.query(`SELECT MAX(date_ran) as latest FROM amazon_stock_country_snapshots WHERE asin = ? AND company = ?`, [cleanAsin, company]),
            ]),
            QUERY_TIMEOUT_MS, 'Date lookup'
        );

        const msDate = msTodayRows[0]?.latest ?? msFallbackRows[0]?.latest ?? null;
        const amzDate = amzTodayRows[0]?.latest ?? amzFallbackRows[0]?.latest ?? null;
        const days = parseInt(req.query.days, 10) || 30;

        const [msRows, amzRows, orderRows, onSeaRows, onAirRows, ordersBreakdownRows, msHistoryRows, amzHistoryRows, tagRows] = await withTimeout(
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
                        `SELECT a.country,
                            CAST(COALESCE(SUM(a.fulfillable), 0) AS UNSIGNED)       as amazon_fulfillable,
                            CAST(COALESCE(SUM(a.inbound_working), 0) AS UNSIGNED)   as amazon_inbound_working,
                            CAST(COALESCE(SUM(a.inbound_shipped), 0) AS UNSIGNED)   as amazon_inbound_shipped,
                            CAST(COALESCE(SUM(a.inbound_receiving), 0) AS UNSIGNED) as amazon_inbound_receiving,
                            CAST(COALESCE(SUM(a.reserved), 0) AS UNSIGNED)          as amazon_reserved
                         FROM amazon_stock_country_snapshots a
                         JOIN (
                             SELECT country, MAX(date_ran) AS latest
                             FROM amazon_stock_country_snapshots
                             WHERE asin = ? AND company = ?
                             GROUP BY country
                         ) ld ON ld.country = a.country AND a.date_ran = ld.latest
                         WHERE a.asin = ? AND a.company = ?
                         GROUP BY a.country`,
                        [cleanAsin, company, cleanAsin, company]
                    ).then(([rows]) => rows)
                    : Promise.resolve([]),

                pool.query(
                    `SELECT status, CAST(COALESCE(SUM(quantity), 0) AS UNSIGNED) as total_quantity
                     FROM orders WHERE asin = ? GROUP BY status`,
                    [cleanAsin]
                ).then(([rows]) => rows),

                pool.query(
                    `SELECT id, product_name, quantity, eta, container_number, supplier, po_number, dates, delivery_date
                     FROM orders WHERE asin = ? AND status = 'ON_SEA' ORDER BY eta ASC`,
                    [cleanAsin]
                ).then(([rows]) => rows),

                pool.query(
                    `SELECT id, product_name, quantity, eta, container_number, supplier, po_number, dates
                     FROM orders WHERE asin = ? AND status = 'ON_AIR' ORDER BY eta ASC`,
                    [cleanAsin]
                ).then(([rows]) => rows),

                pool.query(
                    `SELECT id, product_name, quantity, status, po_number, supplier, dates
                     FROM orders WHERE asin = ? AND status NOT IN ('ON_SEA', 'ON_AIR') ORDER BY created_at DESC`,
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
                     GROUP BY date_ran ORDER BY date_ran`,
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
                     WHERE asin = ? AND company = ? AND date_ran >= DATE_SUB(CURDATE(), INTERVAL ? DAY)
                     GROUP BY date_ran ORDER BY date_ran`,
                    [cleanAsin, company, days]
                ).then(([rows]) => rows),

                pool.query(
                    `SELECT carton_qty AS units_per_ctn,
                            (height * width * depth) / 1000000 AS carton_cbm
                     FROM mintsoft_carton_sizes
                     WHERE asin = ?
                     LIMIT 1`,
                    [cleanAsin]
                ).then(([rows]) => rows),
            ]),
            QUERY_TIMEOUT_MS, 'Data fetch'
        );

        const { byCountry: amazon_stock_by_country, totals } = aggregateAmazonStock(amzRows);
        const msStats = msRows[0] || {};
        const tagData = tagRows[0] || {};

        res.json({
            data: {
                mintsoft_date: msDate,
                amazon_date: amzDate,
                carton_cbm: tagData.carton_cbm != null ? Number(tagData.carton_cbm) : null,
                units_per_ctn: tagData.units_per_ctn != null ? Number(tagData.units_per_ctn) : null,
                mintsoft_stock_level: Number(msStats.total_stock_level || 0),
                mintsoft_available: Number(msStats.total_available || 0),
                mintsoft_allocated: Number(msStats.total_allocated || 0),
                mintsoft_quarantine: Number(msStats.total_quarantine || 0),
                ...totals,
                amazon_stock_by_country,
                orders: buildOrderStatusMap(orderRows),
                goods_on_sea: onSeaRows.map(mapGoodsOnSeaRow),
                goods_on_air: onAirRows.map(mapGoodsOnAirRow),
                orders_breakdown: ordersBreakdownRows.map(mapOrderBreakdownRow),
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
        log.error('[GET /stock-snapshots/sum]', error);
        const status = error.message?.includes('timed out') ? 504 : 500;
        res.status(status).json({ error: 'An internal error occurred.' });
    }
});

// ── 9. GET /api/v1/stock-snapshots/sum/all-asins ─────────────────────────
app.get('/api/v1/stock-snapshots/sum/all-asins', async (req, res) => {
    try {
        const company = (req.query.company || '').trim();
        if (!company) {
            return res.status(400).json({ error: 'company query parameter is required.' });
        }

        const [
            [asinsFromOrders], [asinsFromMintsoft], [asinsFromAmazon],
        ] = await withTimeout(
            Promise.all([
                pool.query(`SELECT DISTINCT asin FROM orders WHERE asin IS NOT NULL AND asin != ''`),
                pool.query(`SELECT DISTINCT asin FROM stock_snapshots WHERE asin IS NOT NULL AND asin != ''`),
                pool.query(`SELECT DISTINCT asin FROM amazon_stock_country_snapshots WHERE asin IS NOT NULL AND asin != '' AND company = ?`, [company]),
            ]),
            QUERY_TIMEOUT_MS, 'ASIN list fetch'
        );

        const allAsins = new Set([
            ...asinsFromOrders.map(r => r.asin),
            ...asinsFromMintsoft.map(r => r.asin),
            ...asinsFromAmazon.map(r => r.asin),
        ]);

        const [
            [msDates], [amzDates], [orderStats], [onSeaOrders], [onAirOrders], [allOrdersRows], [msLatestRows], [amzLatestRows], [tagRows],
        ] = await withTimeout(
            Promise.all([
                pool.query(`
                    SELECT asin, MAX(date_ran) as latest
                    FROM stock_snapshots WHERE asin IS NOT NULL AND asin != ''
                    GROUP BY asin ORDER BY asin
                `),
                pool.query(`
                    SELECT asin, MAX(date_ran) as latest
                    FROM amazon_stock_country_snapshots WHERE asin IS NOT NULL AND asin != '' AND company = ?
                    GROUP BY asin ORDER BY asin
                `, [company]),
                pool.query(`
                    SELECT asin, status,
                           CAST(COALESCE(SUM(quantity), 0) AS UNSIGNED) as total_quantity
                    FROM orders WHERE asin IS NOT NULL
                    GROUP BY asin, status
                `),
                pool.query(`
                    SELECT asin, id, product_name, quantity, eta, container_number, supplier, po_number, dates, delivery_date
                    FROM orders
                    WHERE status = 'ON_SEA' AND asin IS NOT NULL
                    ORDER BY asin, eta ASC
                `),
                pool.query(`
                    SELECT asin, id, product_name, quantity, eta, container_number, supplier, po_number, dates
                    FROM orders
                    WHERE status = 'ON_AIR' AND asin IS NOT NULL
                    ORDER BY asin, eta ASC
                `),
                pool.query(`
                    SELECT asin, id, product_name, quantity, status, po_number, supplier, dates
                    FROM orders
                    WHERE status NOT IN ('ON_SEA', 'ON_AIR') AND asin IS NOT NULL
                    ORDER BY asin, created_at DESC
                `),
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
                pool.query(`
                    WITH LatestDates AS (
                        SELECT asin, country, MAX(date_ran) as latest FROM amazon_stock_country_snapshots
                        WHERE asin IS NOT NULL AND asin != '' AND company = ?
                        GROUP BY asin, country
                    )
                    SELECT a.asin, a.country, a.date_ran,
                           CAST(COALESCE(SUM(a.fulfillable), 0) AS UNSIGNED) as amazon_fulfillable,
                           CAST(COALESCE(SUM(a.inbound_working), 0) AS UNSIGNED) as amazon_inbound_working,
                           CAST(COALESCE(SUM(a.inbound_shipped), 0) AS UNSIGNED) as amazon_inbound_shipped,
                           CAST(COALESCE(SUM(a.inbound_receiving), 0) AS UNSIGNED) as amazon_inbound_receiving,
                           CAST(COALESCE(SUM(a.reserved), 0) AS UNSIGNED) as amazon_reserved
                    FROM amazon_stock_country_snapshots a
                    JOIN LatestDates ld ON a.asin = ld.asin AND a.country = ld.country AND a.date_ran = ld.latest
                    WHERE a.company = ?
                    GROUP BY a.asin, a.country, a.date_ran
                `, [company, company]),
                pool.query(`
                    SELECT asin,
                           carton_qty AS units_per_ctn,
                           (height * width * depth) / 1000000 AS carton_cbm
                    FROM mintsoft_carton_sizes
                    WHERE asin IS NOT NULL AND asin != ''
                `),
            ]),
            QUERY_TIMEOUT_MS, 'Batch data fetch'
        );

        const msDatesMap = Object.fromEntries(msDates.map(r => [r.asin, r.latest]));
        const amzDatesMap = Object.fromEntries(amzDates.map(r => [r.asin, r.latest]));
        const tagMap = Object.fromEntries(tagRows.map(r => [r.asin, r]));

        const results = {};

        for (const asin of allAsins) {
            if (!asin || !parseAsin(asin)) continue;

            const msStats = msLatestRows.find(r => r.asin === asin) || {};
            const amzByCountry = amzLatestRows.filter(r => r.asin === asin);
            const ordersByStatus = orderStats.filter(r => r.asin === asin);
            const seaOrders = onSeaOrders.filter(r => r.asin === asin);
            const airOrders = onAirOrders.filter(r => r.asin === asin);
            const ordersBreakdown = allOrdersRows.filter(r => r.asin === asin);

            const { byCountry: amazon_stock_by_country, totals } = aggregateAmazonStock(amzByCountry);

            const tagData = tagMap[asin] || {};

            results[asin] = {
                mintsoft_date: msDatesMap[asin] || null,
                amazon_date: amzDatesMap[asin] || null,
                carton_cbm: tagData.carton_cbm != null ? Number(tagData.carton_cbm) : null,
                units_per_ctn: tagData.units_per_ctn != null ? Number(tagData.units_per_ctn) : null,
                mintsoft_stock_level: Number(msStats.total_stock_level || 0),
                mintsoft_available: Number(msStats.total_available || 0),
                mintsoft_allocated: Number(msStats.total_allocated || 0),
                mintsoft_quarantine: Number(msStats.total_quarantine || 0),
                ...totals,
                amazon_stock_by_country,
                orders: buildOrderStatusMap(ordersByStatus),
                goods_on_sea: seaOrders.map(mapGoodsOnSeaRow),
                goods_on_air: airOrders.map(mapGoodsOnAirRow),
                orders_breakdown: ordersBreakdown.map(mapOrderBreakdownRow),
            };
        }

        res.json({ data: results });
    } catch (error) {
        log.error('[GET /stock-snapshots/sum/all-asins]', error);
        const status = error.message?.includes('timed out') ? 504 : 500;
        res.status(status).json({ error: 'An internal error occurred.' });
    }
});

// ── 10. GET /api/v1/stock-snapshots/fnskus ──────────────────────────────
app.get('/api/v1/stock-snapshots/fnskus', async (req, res) => {
    try {
        const cleanAsin = parseAsin(req.query.asin);
        if (!cleanAsin) {
            return res.status(400).json({ error: 'A valid 10-character alphanumeric ASIN is required.' });
        }
        const company = (req.query.company || '').trim();
        if (!company) {
            return res.status(400).json({ error: 'company query parameter is required.' });
        }

        // Authoritative pairs: amazon_active_listings has one row per listed
        // SKU with its FNSKU, no Pan-EU dedup — every marketplace SKU survives.
        // first_seen uses MIN(created_at) (timestamp) not MIN(date_ran) (date)
        // so SKUs in a same-date snapshot still order by insertion time.
        const activeListingsQuery = withTimeout(
            pool.query(
                `SELECT al.country, al.sku, al.fnsku, fs.first_seen
                 FROM amazon_active_listings al
                 JOIN (
                     SELECT sku, MIN(created_at) AS first_seen
                     FROM amazon_active_listings
                     WHERE asin = ? AND company = ?
                     GROUP BY sku
                 ) fs ON fs.sku = al.sku
                 WHERE al.asin = ? AND al.company = ?
                   AND al.date_ran = (SELECT MAX(date_ran) FROM amazon_active_listings
                                      WHERE asin = ? AND company = ?)`,
                [cleanAsin, company, cleanAsin, company, cleanAsin, company]
            ),
            QUERY_TIMEOUT_MS, 'FNSKU lookup (active_listings)'
        );

        // Orphan FNSKUs: physical inventory pools (re-stickered/return units)
        // that exist without an active listing. CSV-packed per country row.
        // Latest row is picked per country so a mid-run refresh (some countries
        // on today, others still on yesterday) doesn't drop the stragglers.
        const countrySnapshotsQuery = withTimeout(
            pool.query(
                `SELECT a.country, a.sku, a.fnsku
                 FROM amazon_stock_country_snapshots a
                 JOIN (
                     SELECT country, MAX(date_ran) AS latest
                     FROM amazon_stock_country_snapshots
                     WHERE asin = ? AND company = ?
                     GROUP BY country
                 ) ld ON ld.country = a.country AND a.date_ran = ld.latest
                 WHERE a.asin = ? AND a.company = ?`,
                [cleanAsin, company, cleanAsin, company]
            ),
            QUERY_TIMEOUT_MS, 'FNSKU lookup (country_snapshots)'
        );

        const [[alRows], [csRows]] = await Promise.all([activeListingsQuery, countrySnapshotsQuery]);

        const splitCsv = v => (v || '').split(',').map(s => s.trim()).filter(Boolean);
        const toMs = d => (d ? new Date(d).getTime() : 0);

        // Per-country fallback FNSKU set from country_snapshots + any non-empty
        // fnskus in active_listings. Used when a listing row has no fnsku of
        // its own (common: all SKUs for an ASIN share one FNSKU pool).
        const fnskusByCountry = new Map();
        const csPositional = new Map(); // `${country}|${sku}` → fnsku (same index in CSV)

        for (const row of csRows) {
            const country = row.country?.trim().toUpperCase();
            if (!country) continue;
            if (!fnskusByCountry.has(country)) fnskusByCountry.set(country, new Set());
            const skus = splitCsv(row.sku);
            const fnskus = splitCsv(row.fnsku);
            for (const f of fnskus) fnskusByCountry.get(country).add(f);
            const pairCount = Math.min(skus.length, fnskus.length);
            for (let i = 0; i < pairCount; i++) {
                csPositional.set(`${country}|${skus[i]}`, fnskus[i]);
            }
        }
        for (const row of alRows) {
            const country = row.country?.trim().toUpperCase();
            const fnsku = (row.fnsku || '').trim();
            if (country && fnsku) {
                if (!fnskusByCountry.has(country)) fnskusByCountry.set(country, new Set());
                fnskusByCountry.get(country).add(fnsku);
            }
        }

        const byCountry = {};
        const seenPairs = new Set();
        const pairKey = (c, s, f) => `${c}|${s || ''}|${f || ''}`;

        const sortedAl = [...alRows].sort((a, b) => {
            const diff = toMs(b.first_seen) - toMs(a.first_seen);
            if (diff) return diff;
            const ca = (a.country || '').toUpperCase();
            const cb = (b.country || '').toUpperCase();
            if (ca !== cb) return ca.localeCompare(cb);
            return (a.sku || '').localeCompare(b.sku || '');
        });

        for (const row of sortedAl) {
            const country = row.country?.trim().toUpperCase();
            if (!country) continue;
            const sku = (row.sku || '').trim() || null;
            const rowFnsku = (row.fnsku || '').trim();
            if (!byCountry[country]) byCountry[country] = [];

            // Resolve fnsku(s) for this SKU: listing's own → positional from CS
            // → country's unique FNSKU → all country FNSKUs (cartesian) → null.
            const countryFnskus = fnskusByCountry.get(country);
            const resolved = [];
            if (rowFnsku) {
                resolved.push(rowFnsku);
            } else if (csPositional.has(`${country}|${sku}`)) {
                resolved.push(csPositional.get(`${country}|${sku}`));
            } else if (countryFnskus && countryFnskus.size === 1) {
                resolved.push([...countryFnskus][0]);
            } else if (countryFnskus && countryFnskus.size > 1) {
                resolved.push(...countryFnskus);
            } else {
                resolved.push(null);
            }

            for (const fnsku of resolved) {
                const key = pairKey(country, sku, fnsku);
                if (seenPairs.has(key)) continue;
                seenPairs.add(key);
                byCountry[country].push({ sku, fnsku });
            }
        }

        // Orphan FNSKUs with no matching SKU in active_listings — surface them
        // so callers can still see the stock pool.
        for (const [country, fnskus] of fnskusByCountry) {
            for (const fnsku of fnskus) {
                const alreadyPaired = [...seenPairs].some(k => k.startsWith(`${country}|`) && k.endsWith(`|${fnsku}`));
                if (alreadyPaired) continue;
                if (!byCountry[country]) byCountry[country] = [];
                byCountry[country].push({ sku: null, fnsku });
                seenPairs.add(pairKey(country, null, fnsku));
            }
        }

        res.json({ asin: cleanAsin, fnskus_by_country: byCountry });
    } catch (error) {
        log.error('[GET /stock-snapshots/fnskus]', error);
        const status = error.message?.includes('timed out') ? 504 : 500;
        res.status(status).json({ error: 'An internal error occurred.' });
    }
});

// ── 11. GET /api/v1/stock-snapshots/asana-tasks ─────────────────────────
app.get('/api/v1/stock-snapshots/asana-tasks', async (req, res) => {
    try {
        const cleanAsin = parseAsin(req.query.asin);
        if (!cleanAsin) {
            return res.status(400).json({ error: 'A valid 10-character alphanumeric ASIN is required.' });
        }

        const pat = process.env.ASANA_PAT;
        if (!pat) return res.status(500).json({ error: 'ASANA_PAT not configured.' });

        const headers = { Authorization: `Bearer ${pat}` };

        const opt_fields = 'name,completed,memberships.section.name,custom_fields.name,custom_fields.display_value';

        const matchesAsin = t =>
            (t.custom_fields || []).some(cf => cf.name === 'ASIN' && cf.display_value === cleanAsin);

        const sources = [
            { projectId: ASANA_REPLENISHMENT_PROJECT_ID },
            ...ASANA_ADDITIONAL_SOURCES,
        ];

        const fetched = await Promise.all(
            sources.map(async ({ projectId }) => {
                const [{ data: { data: meta } }, { data: { data: srcTasks } }] = await Promise.all([
                    axios.get(`${ASANA_BASE}/projects/${projectId}?opt_fields=name`, { headers }),
                    axios.get(`${ASANA_BASE}/projects/${projectId}/tasks`, { headers, params: { opt_fields } }),
                ]);
                return { listName: meta.name, tasks: srcTasks };
            })
        );

        const tasksByCountry = {};
        for (const { listName, tasks: srcTasks } of fetched) {
            for (const t of srcTasks.filter(matchesAsin)) {
                const section = t.memberships?.[0]?.section?.name || null;
                const country = resolveSectionCountry(section);
                if (!country) continue;

                if (!tasksByCountry[country]) tasksByCountry[country] = [];
                tasksByCountry[country].push({
                    gid: t.gid,
                    name: `${listName} — ${t.name}`,
                    completed: t.completed,
                    section,
                    list: listName,
                    custom_fields: Object.fromEntries(
                        (t.custom_fields || [])
                            .filter(cf => cf.display_value)
                            .map(cf => [cf.name, cf.display_value])
                    ),
                });
            }
        }

        res.json({ asin: cleanAsin, tasks_by_country: tasksByCountry });
    } catch (error) {
        log.error('[GET /stock-snapshots/asana-tasks]', error);
        const msg = error.response?.data?.errors?.[0]?.message || error.message;
        res.status(error.response?.status || 500).json({ error: msg });
    }
});

// ── 12. GET /api/v1/stock-snapshots/oos-dates ───────────────────────────
app.get('/api/v1/stock-snapshots/oos-dates', async (req, res) => {
    try {
        const { asin } = req.query;
        if (!asin) return res.status(400).json({ error: 'asin query parameter is required.' });
        const company = (req.query.company || '').trim();
        if (!company) return res.status(400).json({ error: 'company query parameter is required.' });

        const [rows] = await pool.execute(
            `SELECT country, date_ran AS date
             FROM amazon_stock_raw_snapshots
             WHERE asin = ? AND company = ? AND fulfillable = 0
             ORDER BY country, date_ran`,
            [asin, company]
        );

        // Group by territory
        const byTerritory = {};
        for (const row of rows) {
            const country = row.country;
            if (!byTerritory[country]) byTerritory[country] = [];
            byTerritory[country].push(formatDate(row.date));
        }

        res.json({ asin, oos_dates: byTerritory });
    } catch (error) {
        log.error('[GET /stock-snapshots/oos-dates]', error);
        res.status(500).json({ error: error.message });
    }
});

// ── 13. GET /api/v1/stock-snapshots/active-listings ────────────────────
app.get('/api/v1/stock-snapshots/active-listings', async (req, res) => {
    try {
        const cleanAsin = parseAsin(req.query.asin);
        if (!cleanAsin) {
            return res.status(400).json({ error: 'A valid 10-character alphanumeric ASIN is required.' });
        }
        const company = (req.query.company || '').trim();
        if (!company) {
            return res.status(400).json({ error: 'company query parameter is required.' });
        }

        const [[latestRows], [historyRows]] = await withTimeout(
            Promise.all([
                pool.query(
                    `SELECT a.country, a.sku, a.fnsku, a.product_name, a.status, a.price, a.date_ran
                     FROM amazon_active_listings a
                     JOIN (
                         SELECT country, MAX(date_ran) AS latest
                         FROM amazon_active_listings
                         WHERE asin = ? AND company = ?
                         GROUP BY country
                     ) ld ON ld.country = a.country AND a.date_ran = ld.latest
                     WHERE a.asin = ? AND a.company = ?
                     ORDER BY a.country, a.sku`,
                    [cleanAsin, company, cleanAsin, company]
                ),
                pool.query(
                    `SELECT date_ran, country, sku, fnsku, product_name, status, price
                     FROM amazon_active_listings
                     WHERE asin = ? AND company = ?
                     ORDER BY date_ran DESC, country, sku`,
                    [cleanAsin, company]
                ),
            ]),
            QUERY_TIMEOUT_MS, 'Active listings lookup'
        );

        const currentByCountry = {};
        for (const row of latestRows) {
            const country = row.country?.trim().toUpperCase();
            if (!country) continue;
            if (!currentByCountry[country]) currentByCountry[country] = [];
            currentByCountry[country].push({
                sku: row.sku,
                fnsku: row.fnsku,
                product_name: row.product_name,
                status: row.status,
                price: Number(row.price),
            });
        }

        const history = historyRows.map(row => ({
            date: formatDate(row.date_ran),
            country: row.country,
            sku: row.sku,
            fnsku: row.fnsku,
            product_name: row.product_name,
            status: row.status,
            price: Number(row.price),
        }));

        const latestDateRaw = latestRows.reduce((max, r) => {
            const t = r.date_ran ? new Date(r.date_ran).getTime() : 0;
            return t > max ? t : max;
        }, 0);

        res.json({
            asin: cleanAsin,
            company,
            latest_date: latestDateRaw ? formatDate(new Date(latestDateRaw)) : null,
            current: currentByCountry,
            history,
        });
    } catch (error) {
        log.error('[GET /stock-snapshots/active-listings]', error);
        const status = error.message?.includes('timed out') ? 504 : 500;
        res.status(status).json({ error: 'An internal error occurred.' });
    }
});

// ── Asana: Add Dispatch Task ─────────────────────────────────────────────
const ASANA_BASE = 'https://app.asana.com/api/1.0';

const ASANA_REPLENISHMENT_PROJECT_ID = '1214003962428764';
const ASANA_REPLENISHMENT_REGIONS = ['UK', 'DE', 'FR', 'IT', 'ES'];

const COUNTRY_NAME_TO_CODE = {
    UK: 'UK', GB: 'UK', BRITAIN: 'UK', 'UNITED KINGDOM': 'UK', ENGLAND: 'UK',
    DE: 'DE', GERMANY: 'DE', DEUTSCHLAND: 'DE',
    FR: 'FR', FRANCE: 'FR',
    IT: 'IT', ITALY: 'IT', ITALIA: 'IT',
    ES: 'ES', SPAIN: 'ES', ESPANA: 'ES',
};

function resolveSectionCountry(section) {
    if (!section) return null;
    const upper = section.toUpperCase();
    for (const [name, code] of Object.entries(COUNTRY_NAME_TO_CODE)) {
        const re = new RegExp(`(^|[^A-Z])${name}([^A-Z]|$)`);
        if (re.test(upper)) return code;
    }
    return null;
}

// Additional Asana projects to surface tasks from, grouped by project name.
const ASANA_ADDITIONAL_SOURCES = [
    { projectId: '1204907541333306' },
    { projectId: '1208741186098190' },
];

// Body key → Asana custom field display name
const FIELD_KEY_TO_NAME = {
    asin:                  'ASIN',
    publish_date:          'Publish Date',
    jf_hw_code:            'JF / HW Code',
    fnsku:                 'FNSKU',
    qty_required:          'Qty Required',
    order_number:          'Order Number',
    allocations:           'Allocations',
    expiry_date:           'Expiry Date',
    ctns_to_send:          'Ctns. to Send',
    ctn_width:             'Ctn. Width',
    ctn_length:            'Ctn. Length',
    ctn_height:            'Ctn. Height',
    ctn_weight:            'Ctn. Weight',
    units_per_ctn:         'Units/Ctn.',
    drop_area:             'Drop Area',
    packing_dispatch:      'Packing / Dispatch',
    final_dispatch_checks: 'Final Dispatch Checks',
    mintsoft_updated:       'Mintsoft Updated',
    goods_status:          'Goods Status',
};

async function fetchProjectCustomFields(projectId, pat) {
    const { data: { data: settings } } = await axios.get(
        `${ASANA_BASE}/projects/${projectId}/custom_field_settings?opt_fields=custom_field.name,custom_field.gid,custom_field.type,custom_field.enum_options.name,custom_field.enum_options.gid`,
        { headers: { Authorization: `Bearer ${pat}` } }
    );
    // Build a name → field lookup
    const byName = {};
    for (const s of settings) {
        const cf = s.custom_field;
        byName[cf.name] = cf;
    }
    return byName;
}

function buildCustomFields(body, fieldsByName) {
    const custom_fields = {};
    for (const [key, asanaName] of Object.entries(FIELD_KEY_TO_NAME)) {
        const val = body[key];
        if (val === undefined || val === null || val === '') continue;

        const cf = fieldsByName[asanaName];
        if (!cf) continue; // field not on this project, skip

        if (cf.type === 'text') {
            custom_fields[cf.gid] = String(val);
        } else if (cf.type === 'number') {
            custom_fields[cf.gid] = Number(val);
        } else if (cf.type === 'date') {
            custom_fields[cf.gid] = val;
        } else if (cf.type === 'enum') {
            const opt = (cf.enum_options || []).find(o => o.name === val);
            if (!opt) {
                const valid = (cf.enum_options || []).map(o => o.name).join(', ');
                throw new Error(`Invalid value "${val}" for ${key}. Valid: ${valid}`);
            }
            custom_fields[cf.gid] = opt.gid;
        }
    }
    return custom_fields;
}

async function findOrCreateSection(projectId, carrier, pat) {
    const { data: { data: sections } } = await axios.get(
        `${ASANA_BASE}/projects/${projectId}/sections`,
        { headers: { Authorization: `Bearer ${pat}` } }
    );

    const existing = sections.find(s => s.name.toLowerCase() === carrier.toLowerCase());
    if (existing) return existing.gid;

    const { data: { data: newSection } } = await axios.post(
        `${ASANA_BASE}/projects/${projectId}/sections`,
        { data: { name: carrier } },
        { headers: { Authorization: `Bearer ${pat}` } }
    );
    return newSection.gid;
}

app.get('/api/v1/orders/asana-tasks', async (req, res) => {
    try {
        const pat = process.env.ASANA_PAT;
        if (!pat) return res.status(500).json({ error: 'ASANA_PAT not configured.' });

        const headers = { Authorization: `Bearer ${pat}` };
        const queryProjectId = (req.query.projectId || '').trim();
        const projectIds = queryProjectId
            ? [queryProjectId]
            : [ASANA_REPLENISHMENT_PROJECT_ID, ...ASANA_ADDITIONAL_SOURCES.map(s => s.projectId)];

        const tasks = [];
        for (const pid of projectIds) {
            let offset = null;
            do {
                const params = {
                    limit: 100,
                    opt_fields: 'name,completed,created_at,modified_at,memberships.section.name,memberships.section.gid,custom_fields.name,custom_fields.display_value,custom_fields.text_value,custom_fields.number_value,custom_fields.enum_value.name',
                };
                if (offset) params.offset = offset;
                const { data } = await axios.get(
                    `${ASANA_BASE}/projects/${pid}/tasks`,
                    { headers, params }
                );
                for (const t of (data.data || [])) tasks.push({ ...t, _projectId: pid });
                offset = data.next_page?.offset || null;
            } while (offset);
        }

        const flat = tasks.map(t => {
            const m = (t.memberships || []).find(x => x.section) || {};
            const cfByName = {};
            for (const cf of (t.custom_fields || [])) {
                cfByName[cf.name] = cf.display_value
                    ?? cf.text_value
                    ?? cf.number_value
                    ?? cf.enum_value?.name
                    ?? null;
            }
            return {
                gid: t.gid,
                project_id: t._projectId,
                name: t.name,
                completed: !!t.completed,
                created_at: t.created_at,
                modified_at: t.modified_at,
                section_gid: m.section?.gid || null,
                section_name: m.section?.name || null,
                asin: cfByName['ASIN'] || null,
                fnsku: cfByName['FNSKU'] || null,
                custom_fields: cfByName,
            };
        });

        const bySection = {};
        for (const t of flat) {
            const key = t.section_name || '(no section)';
            if (!bySection[key]) bySection[key] = [];
            bySection[key].push(t);
        }

        res.json({ projectIds, total: flat.length, tasks: flat, by_section: bySection });
    } catch (error) {
        log.error('[GET /api/v1/orders/asana-tasks]', error);
        const msg = error.response?.data?.errors?.[0]?.message || error.message;
        res.status(error.response?.status || 500).json({ error: msg });
    }
});

app.post('/api/v1/orders/asana-task', async (req, res) => {
    try {
        const pat = process.env.ASANA_PAT;
        if (!pat) return res.status(500).json({ error: 'ASANA_PAT not configured.' });

        const { sku, asin, carrier, region, ...fields } = req.body;
        if (!sku) return res.status(400).json({ error: 'sku is required.' });

        const regionKey = (region || 'UK').toUpperCase();
        if (!ASANA_REPLENISHMENT_REGIONS.includes(regionKey)) {
            return res.status(400).json({
                error: `Invalid region "${regionKey}". Valid: ${ASANA_REPLENISHMENT_REGIONS.join(', ')}`,
            });
        }

        const projectId = ASANA_REPLENISHMENT_PROJECT_ID;
        const carrierTrimmed = (carrier || '').trim();
        const sectionName = carrierTrimmed ? `${regionKey} - ${carrierTrimmed}` : regionKey;

        const fieldsByName = await fetchProjectCustomFields(projectId, pat);
        const custom_fields = buildCustomFields({ asin, ...fields }, fieldsByName);

        const sectionGid = await findOrCreateSection(projectId, sectionName, pat);

        // Create the task
        const { data: { data: task } } = await axios.post(
            `${ASANA_BASE}/tasks`,
            {
                data: {
                    name: sku,
                    projects: [projectId],
                    memberships: [{ project: projectId, section: sectionGid }],
                    custom_fields,
                },
            },
            { headers: { Authorization: `Bearer ${pat}` } }
        );

        res.status(201).json({ task_gid: task.gid, sku: task.name, asin: asin || null, region: regionKey, section: sectionName });
    } catch (error) {
        log.error('[POST /api/v1/orders/asana-task]', error);
        const msg = error.response?.data?.errors?.[0]?.message || error.message;
        res.status(error.response?.status || 500).json({ error: msg });
    }
});

// ── Serverless export ────────────────────────────────────────────────────
const serverlessApp = serverless(app, {
    binary: ['application/json', 'image/*', 'application/javascript'],
});

module.exports.handler = async (event, context) => {
    context.callbackWaitsForEmptyEventLoop = false;
    return await serverlessApp(event, context);
};

if (require.main === module) {
    const PORT = process.env.ORDERS_PORT || 3001;
    app.listen(PORT, () => {
        log.info(`Orders API running on http://localhost:${PORT}`);
    });
}
