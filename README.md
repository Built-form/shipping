# Shipping & Orders Pipeline

A serverless backend that tracks the full lifecycle of product shipments -- from purchase order through manufacturing, containerisation, ocean transit, and warehouse arrival. It aggregates stock data from **Mintsoft** (3PL warehouse), **Amazon SP-API** (FBA inventory), and **Asana** (order management) into a single MySQL database exposed via a REST API.

Deployed on **AWS Lambda** (Node.js 18) behind **API Gateway HTTP API** with Google JWT authentication, managed by the **Serverless Framework v3**.

---

## Architecture

```
                        Scheduled Lambdas
                   +--------------------------+
                   |                          |
    Every 1 min    |    Every 1 hr            |   Every 6 hrs
  +-------------+  |  +----------------+      |  +---------------------+
  | importAsana |  |  | stockSnapshot  |      |  | amazonStockSnapshot |
  | Orders      |  |  |                |      |  |                     |
  +------+------+  |  +-------+--------+      |  +---------+-----------+
         |         |          |               |            |
         v         |          v               |            v
    Asana REST     |    Mintsoft REST          |     Amazon SP-API
    (Projects)     |    (Product/Stock)        |     (Reports API)
         |         |          |               |            |
         |         |          |               |            |
         +-------->+<---------+---------------+<-----------+
                   |
                   v
        +--------------------+
        |    MySQL (RDS)     |
        |--------------------|
        | orders             |
        | stock_snapshots    |
        | amazon_stock_      |
        |  country_snapshots |
        | allowed_emails     |
        | landed_costs       |
        +--------+-----------+
                 |
                 |  Reads / Writes
                 v
        +--------------------+
        |   ordersApi        |
        |   (Express app     |
        |    on Lambda)      |
        +---------+----------+
                  |
                  v
        +--------------------+
        | API Gateway        |
        | (HTTP API + JWT)   |
        +---------+----------+
                  |
                  v
        +--------------------+
        |  Frontend Client   |
        |  (Google OAuth)    |
        +--------------------+
```

### Data Flow

```
  Asana                Mintsoft              Amazon SP-API
  (source of truth     (3PL warehouse       (FBA inventory
   for POs &            stock levels)        health reports)
   shipments)
    |                      |                       |
    | every 1 min          | every 1 hr            | every 6 hrs
    v                      v                       v
+-----------------------------------------------------------+
|                       MySQL                               |
|                                                           |
|  orders               stock_snapshots    amazon_stock_    |
|  - id, asin           - jf_code, asin    country_snapshots|
|  - status             - sku, warehouse   - country, asin  |
|  - quantity           - stock levels     - fulfillable    |
|  - container_number   - available        - inbound_*      |
|  - dates (JSON)       - allocated        - reserved       |
|  - eta, supplier      - quarantine                        |
+----------------------------+------------------------------+
                             |
                             v
                     Orders REST API
                    /api/v1/orders
                    /api/v1/containers
                    /api/v1/stock-snapshots/sum
```

### Order Status Lifecycle

```
PO_SENT --> IN_PRODUCTION --> READY --> ON_SEA  --> (delivered)
                                    \-> ON_AIR  --> (delivered)
```

Statuses are set during Asana import via `statusMappers.js`:

| Asana field | Result |
|---|---|
| Goods Status = `"ready"` | `READY` |
| Artwork Confirmed Date set | `IN_PRODUCTION` |
| Otherwise | `PO_SENT` |

Goods on Sea records are always `ON_SEA`. Goods on Air records are always `ON_AIR`.

---

## Project Structure

```
shipping/
  orders.js                 Express REST API (main entry point, Lambda handler)
  stock-snapshot.js         Scheduled Lambda: Mintsoft stock snapshots
  amazon-stock-snapshot.js  Scheduled Lambda: Amazon FBA inventory snapshots
  import-asana-orders.js    Scheduled Lambda: Asana order import
  db.js                     MySQL connection pool (singleton)
  logger.js                 Structured logging (timestamp + level)
  mintsoft.js               Mintsoft API client (product search & stock levels)
  asanaService.js           Asana API client (fetch project tasks)
  asanaTransformer.js       Transforms Asana tasks to flat row objects
  statusMappers.js          Maps Asana status strings to internal status enums
  serverless.yml            Serverless Framework deployment config
  .env                      Environment variables (not committed)
  .env.example              Template for required env vars
```

---

## Lambda Functions

| Function | File | Trigger | Purpose |
|---|---|---|---|
| `ordersApi` | `orders.js` | HTTP API (all `/api/v1/*` routes) | REST API for orders, containers, and stock summaries |
| `stockSnapshot` | `stock-snapshot.js` | `rate(1 hour)` | Snapshots Mintsoft warehouse stock by JF code |
| `amazonStockSnapshot` | `amazon-stock-snapshot.js` | `rate(6 hours)` | Snapshots Amazon FBA inventory per marketplace/country |
| `importAsanaOrders` | `import-asana-orders.js` | `rate(1 minute)` | Syncs orders from three Asana projects (Goods on Sea, Goods on Air, Orders) into the `orders` table |

---

## REST API Endpoints

All endpoints require a Google JWT `Authorization` header (bypassed in local dev).

### Orders

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/v1/orders` | List all orders (newest first) |
| `POST` | `/api/v1/orders` | Create a new order (status: `PLANNING`) |
| `PUT` | `/api/v1/orders/:id` | Update order fields |
| `PATCH` | `/api/v1/orders/:id/status` | Move order to a new status (timestamps the transition) |
| `POST` | `/api/v1/orders/:id/split` | Split quantity off into a new `CONTAINERIZED` order |

### Containers

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/containers/pack` | Bulk-pack orders into a container (full or partial split) |
| `PATCH` | `/api/v1/containers/:containerNumber/status` | Update status for all orders in a container |

### Stock Snapshots

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/v1/stock-snapshots/sum?asin=...&days=30` | Aggregated stock summary for a single ASIN (Mintsoft + Amazon + orders + history) |
| `GET` | `/api/v1/stock-snapshots/sum/all-asins` | Full stock summary for every known ASIN |

#### Stock snapshot response fields

| Field | Description |
|---|---|
| `orders` | Map of `{ status: totalQuantity }` across all statuses |
| `goods_on_sea` | Array of individual `ON_SEA` order rows (with `dates`) |
| `goods_on_air` | Array of individual `ON_AIR` order rows (with `dates`) |
| `orders_breakdown` | Array of all other order rows (`PO_SENT`, `IN_PRODUCTION`, `READY`) with `dates` |

---

## Database Tables

### `orders`
Main table for tracking shipments through their lifecycle.

| Column | Type | Notes |
|---|---|---|
| `id` | VARCHAR | `ORD-XXXXXXXX` (UUID-based) or Asana task name |
| `asin` | VARCHAR | Amazon ASIN |
| `product_name` | VARCHAR | SKU or product description |
| `quantity` | INT | Units in this order line |
| `status` | VARCHAR | Current lifecycle status |
| `po_number` | VARCHAR | Purchase order reference |
| `supplier` | VARCHAR | Supplier name |
| `container_number` | VARCHAR | Shipping container ID |
| `vessel_name` | VARCHAR | Ship name |
| `eta` | DATE | Estimated arrival |
| `cbm_per_unit` | DECIMAL | Cubic metres per unit |
| `pack_size` | DECIMAL | Units per pack |
| `cbm_per_pack` | DECIMAL | Cubic metres per pack |
| `notes` | TEXT | Free-text notes |
| `dates` | JSON | Timestamped status transitions — keys include `ordered`, `estimated_ready`, `shipped`, `eta` depending on source |
| `location` | VARCHAR | Current physical location |
| `containerized_location` | VARCHAR | Location when containerized |
| `expected_shipping_date` | DATE | Planned shipping date |

### `stock_snapshots`
Daily Mintsoft warehouse stock levels, keyed by SKU + warehouse + date.

| Column | Type | Notes |
|---|---|---|
| `jf_code` | VARCHAR | Internal JF product code |
| `asin` | VARCHAR | Amazon ASIN |
| `sku` | VARCHAR | Mintsoft SKU (may include suffixes like `_QC`, `_READY`) |
| `product_id` | INT | Mintsoft product ID |
| `warehouse_id` | INT | Mintsoft warehouse ID |
| `date_ran` | DATE | Snapshot date |
| `stock_level` | INT | Total stock |
| `available` | INT | Available for sale |
| `allocated` | INT | Reserved/allocated |
| `quarantine` | INT | In quarantine |

### `amazon_stock_country_snapshots`
Amazon FBA inventory by country, from SP-API inventory health reports.

| Column | Type | Notes |
|---|---|---|
| `date_ran` | DATE | Snapshot date |
| `country` | VARCHAR | Marketplace code (`UK`, `DE`, `FR`, `ES`, `IT`) |
| `asin` | VARCHAR | Amazon ASIN |
| `fnsku` | VARCHAR | Fulfillment network SKU |
| `sku` | VARCHAR | Merchant SKU |
| `fulfillable` | INT | Available at FBA |
| `inbound_working` | INT | Inbound shipment being prepared |
| `inbound_shipped` | INT | In transit to FBA |
| `inbound_receiving` | INT | Being received at FBA |
| `reserved` | INT | Reserved (pending orders, transfers) |

### `allowed_emails`
Email allowlist for API access control.

### `landed_costs`
Reference table mapping JF codes to ASINs (used by `stock-snapshot.js` to know which products to snapshot).

---

## External Integrations

### Mintsoft (3PL Warehouse)
- **Base URL:** `https://api.mintsoft.co.uk/api`
- **Auth:** API key via query parameter
- **Used by:** `stock-snapshot.js` via `mintsoft.js`
- **Endpoints:** `/Product/Search` (find products by JF code), `/Product/StockLevels` (per-warehouse breakdown)
- **Rate limiting:** Batches of 10 concurrent requests with retry (3 attempts, exponential backoff)

### Amazon SP-API (Selling Partner)
- **Endpoint:** `https://sellingpartnerapi-eu.amazon.com`
- **Auth:** OAuth2 refresh token flow (45-min token cache)
- **Used by:** `amazon-stock-snapshot.js`
- **Report type:** `GET_FBA_INVENTORY_PLANNING_DATA` (inventory health, TSV format)
- **Marketplaces:** UK, DE, FR, ES, IT
- **Accounts:** JFA, Hangerworld (separate credentials)
- **Fallback:** If all marketplaces fail, copies the last successful snapshot to today's date

### Asana (Project Management)
- **Base URL:** `https://app.asana.com/api/1.0`
- **Auth:** Personal Access Token (PAT)
- **Used by:** `import-asana-orders.js` via `asanaService.js`
- **Projects:**
  - `1210568539171010` — Goods on Sea → status `ON_SEA`
  - `1210880888784849` — Goods on Air → status `ON_AIR`
  - `1210599256348524` — Orders → status derived from `Goods Status` + `Artwork Confirmed Date`
- **Sync behavior:** Full replace (truncates `orders` table, re-imports all three projects). Only truncates after validating at least one project returned data.

---

## Environment Variables

```bash
# Database
DB_HOST=
DB_USER=
DB_PASSWORD=
DB_NAME=
DB_PORT=3306

# Mintsoft
MINTSOFT_API_KEY=

# Asana
ASANA_PAT=

# Amazon SP-API (primary account - JFA)
AMAZON_SP_CLIENT_ID=
AMAZON_SP_CLIENT_SECRET=
AMAZON_SP_REFRESH_TOKEN=

# Amazon SP-API (secondary account - Hangerworld)
AMAZON_SP_CLIENT_ID_HW=
AMAZON_SP_CLIENT_SECRET_HW=
AMAZON_SP_REFRESH_TOKEN_HW=
```

---

## Getting Started

### Prerequisites
- Node.js 18+
- MySQL database (or RDS)
- Credentials for Mintsoft, Asana, and Amazon SP-API

### Local Development

```bash
# Install dependencies
npm install

# Copy and fill in environment variables
cp .env.example .env

# Run the API server locally
npm run dev
# -> http://localhost:3001

# Or use serverless-offline (uncomment plugin in serverless.yml)
npm run offline
```

### Run Snapshot Jobs Manually

```bash
# Mintsoft stock snapshot
node stock-snapshot.js

# Amazon FBA inventory snapshot
node amazon-stock-snapshot.js

# Asana order import
node import-asana-orders.js
```

### Deploy

```bash
npm run deploy
# Deploys all functions to AWS Lambda (eu-north-1)
```

---

## Authentication

- **Production:** Google JWT via API Gateway HTTP API authorizer. The email claim from the JWT is checked against the `allowed_emails` table in MySQL.
- **Local dev:** Bypassed when `IS_OFFLINE` or `NODE_ENV=development` is set. Requests are attributed to `local@dev`.

---

## Key Design Decisions

- **Asana as source of truth:** The `import-asana-orders` function runs every minute and does a full table replace. The REST API's CRUD operations on orders exist for manual overrides and container packing workflows, but will be overwritten on the next Asana sync. **Note:** Asana is planned to be replaced with a dedicated UI for managing orders and shipments, eliminating the sync overhead and enabling real-time updates.
- **Snapshot-based stock tracking:** Stock levels are recorded as point-in-time snapshots rather than event streams, enabling historical trend queries over configurable date ranges.
- **Per-warehouse granularity:** Mintsoft stock is stored per-warehouse, Amazon stock per-country, allowing breakdown views in the frontend.
- **Graceful degradation:** Amazon snapshot has a fallback that copies the last available data if all marketplace API calls fail, so dashboards never show empty data.
- **Connection pooling:** A singleton MySQL pool (`db.js`, limit 5) is shared across Lambda invocations via container reuse.
