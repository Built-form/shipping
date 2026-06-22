# `orders` table — column sources

How each column in the `orders` table is populated, and what writes to it.

## Schema

```sql
CREATE TABLE `orders` (
  `id` int NOT NULL AUTO_INCREMENT,
  `jf_code` varchar(50) DEFAULT NULL,
  `asin` varchar(20) DEFAULT NULL,
  `product_name` varchar(255) DEFAULT NULL,
  `quantity` int NOT NULL DEFAULT '0',
  `status` varchar(50) NOT NULL DEFAULT 'SCHEDULED',
  `po_number` varchar(100) DEFAULT NULL,
  `supplier` varchar(255) DEFAULT NULL,
  `container_number` varchar(100) DEFAULT NULL,
  `vessel_name` varchar(255) DEFAULT NULL,
  `eta` date DEFAULT NULL,
  `cbm_per_unit` decimal(10,6) DEFAULT NULL,
  `notes` text,
  `dates` json DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `last_updated` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `delivery_date` datetime DEFAULT NULL,
  `lot_number` varchar(255) DEFAULT NULL,
  `mfg_date` date DEFAULT NULL,
  `exp_date` date DEFAULT NULL,
  `delivery_time` varchar(100) DEFAULT NULL,
  `container_status` varchar(100) DEFAULT NULL,
  `booking_status` varchar(100) DEFAULT NULL,
  `arrived_date` date DEFAULT NULL,
  `external_container_number` varchar(255) DEFAULT NULL,
  `order_cbm` decimal(10,3) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_status` (`status`),
  KEY `idx_container` (`container_number`),
  KEY `idx_id` (`jf_code`)
);
```

## Asana source projects

Three Asana projects feed `orders`. The Lambda `importAsanaOrders` (every 10 min) does **`TRUNCATE TABLE orders`** then re-inserts everything — meaning Asana is the source of truth for any column it owns. Frontend-only edits (e.g. `notes`, `vessel_name`) survive only until the next import unless the same edit lands in Asana.

| Asana Project | Project ID | Status it produces |
|---|---|---|
| Goods_on_sea | `1210568539171010` | `ON_SEA`, `IN_WAREHOUSE`, `MINTSOFT` (per `mapGoodsOnSeaStatus(Container Status)`) |
| Goods_on_air | `1210880888784849` | `ON_AIR` |
| Orders | `1210599256348524` | `SCHEDULED` / `PO_SENT` / `READY` per `mapOrdersStatus(Goods Status, Artwork Confirmed Date)` |

## Field-by-field map

| DB column | Source | Notes |
|---|---|---|
| `id` | DB | Auto-increment, assigned on INSERT |
| `jf_code` | Asana `Name` (task title) | Sea/Air/Orders. Fallback chain if blank: `PO Number` → `ASIN` → generated `{sea\|air\|order}-{ts}-{rand}` |
| `asin` | Asana custom field `ASIN` | Sea/Air/Orders |
| `product_name` | Asana custom field `SKU` | Sea/Air/Orders. (Column is `product_name` but the value comes from the Asana SKU field.) |
| `quantity` | Asana custom field `Units` (Sea/Air) or `Units Ordered` (Orders) | parseInt, defaults `0` |
| `status` | Derived in code | Sea: `mapGoodsOnSeaStatus(Container Status)`. Air: `mapGoodsOnAirStatus()`. Orders: `mapOrdersStatus(Goods Status, Artwork Confirmed Date)`. Frontend can change via `PATCH /api/v1/orders/:id/status` (until next import overwrites). |
| `po_number` | Asana custom field `PO Number` | Sea/Air/Orders |
| `supplier` | Asana custom field `Supplier Name` | Sea/Air/Orders |
| `container_number` | Asana **section name** (`Section/Column`) | Sea/Air/Orders. Read from `task.memberships[0].section.name`, not a custom field. For Orders, "Untitled section" → `null` |
| `vessel_name` | **Frontend only** (`PUT /api/v1/orders/:id`) | Not imported from Asana |
| `eta` | Asana custom field `ETA to Port` | Sea/Air. Sliced to YYYY-MM-DD. Orders import doesn't set this. Indirectly fed by ShipsGo (see below). |
| `cbm_per_unit` | Computed (Orders only) | `Total CBM. / Units Ordered`. Sea/Air imports don't set this. |
| `notes` | **Frontend only** (`PUT /api/v1/orders/:id`) | Not imported from Asana |
| `dates` (json) | Composed in code | See "`dates` JSON keys" below. Frontend can also overwrite via `PUT /:id { dates }`. |
| `created_at` | DB | `DEFAULT CURRENT_TIMESTAMP` |
| `last_updated` | DB | `ON UPDATE CURRENT_TIMESTAMP` |
| `delivery_date` | Asana custom field `Delivery Date` (Sea only) | DATETIME (ISO; date-only values land at midnight) |
| `lot_number` | Asana custom field `LOT number` (Sea only) | |
| `mfg_date` | Asana custom field `MFG DATE` (Sea only) | |
| `exp_date` | Asana custom field `EXP DATE` (Sea only) | |
| `delivery_time` | Asana custom field `Delivery Time` (Sea only) | |
| `container_status` | Asana custom field `Container Status` (Sea only) | Also drives the `status` mapping |
| `booking_status` | Asana custom field `Booking Status` (Sea only) | |
| `arrived_date` | Asana custom field `Arrived` (Sea only) | YYYY-MM-DD |
| `external_container_number` | Asana custom field `External Container Number` (Sea only) | Populated upstream by `update-imported-dates.js` syncing from `GIGO Container No.` field |
| `order_cbm` | Asana custom field `CBM/Line` (Sea/Air only) | parseFloat |

## `dates` JSON keys

After the May 2026 sort, `dates` holds **only status-transition timestamps** —
when an order moved into a given workflow state. Scalar dates (the date itself,
not when it was learned) live in flat columns. See "Scalar date columns" below.

| Key | Set by | Source |
|---|---|---|
| `planned` | `POST /api/v1/orders` | Set on creation when status = `PLANNING` |
| `manufacturing` | `PATCH /:id/status` to `MANUFACTURING` | now() |
| `ready` | `PATCH /:id/status` to `READY` | now() |
| `ready_for_qc` | `PATCH /:id/status` to `READY_FOR_QC` | now() |
| `consolidated` | `PATCH /:id/status` to `CONSOLIDATED`, `POST /:id/split`, `POST /containers/pack`, `PATCH /containers/:cn/status` | now() |
| `delivered` | `PATCH /:id/status` to `DELIVERED` | now() |
| `completed` | `PATCH /:id/status` to `COMPLETED` | now() |
| `received_by_warehouse` | `POST /api/v1/orders/:id/receive` | now(), set before Mintsoft call |
| `received` | `POST /api/v1/orders/:id/receive` | now(), set after Mintsoft success |
| `asn_id` | `POST /api/v1/orders/:id/receive` | Mintsoft ASN id (integer, not a date — historical key) |

## Scalar date columns (flat fields)

These are dedicated `DATE` columns. They hold the date *value* (not a stamp of
when it was learned). Use these in the API response, not `dates.*`.

| Column | Source | Notes |
|---|---|---|
| `eta` | Asana `ETA to Port` (Sea/Air) or ShipsGo via `orders-from-shipsgo` | ETA at port |
| `arrived_date` | Asana `Arrived` (Sea) or ShipsGo `arrival_date` when actual | Vessel arrived |
| `delivery_date` | Asana `Delivery Date` (Sea) | Delivery to warehouse |
| `mfg_date` / `exp_date` | Asana (Sea) | Manufacture / expiry |
| `scheduled_date` / `po_date` / `qc_date` | Frontend / Asana | |
| `shipped_date` | Asana `Sailing Date` (Sea) / `Departure Date` (Air) / ShipsGo when actual | Replaces former `dates.shipped` |
| `ordered_date` | Asana `PO Placed` (Orders) | Replaces former `dates.ordered` |
| `estimated_ready_date` | Asana `Estimated Ready Date` (Orders) | Replaces former `dates.estimated_ready` |
| `actual_ready_date` | Frontend mutation only | Set when goods are physically ready |
| `estimated_departure_date` | Frontend mutation only | Planned departure |
| `artwork_confirmed_date` | Asana `Artwork Confirmed Date` (Sea/Orders) | Replaces former `dates.artwork_confirmed` |

## Sources outside Asana

- **ShipsGo → Asana → orders (indirect)**
  - `shipsgo-containers.js` (cron every 6h) hits the ShipsGo API and fills the `shipsgo_containers` table (BL, vessel, voyage, ETA, arrived, etc.).
  - `update-imported-dates.js` (cron 09:00 / 12:00 / 15:00 / 18:00 daily) reads `shipsgo_containers` and writes **back into Asana** the `ETA to Port`, `Delivery Date`, and `External Container Number` custom fields. It does **not** write to the `orders` table directly.
  - Those updated Asana values land in `orders` on the next 10-min Asana import.

- **Mintsoft `/receive` endpoint**
  - `POST /api/v1/orders/:id/receive` writes one row to `order_receipts` per call. The order's `status` is left unchanged on partial receipts (so e.g. `IN_WAREHOUSE` set by the Sea import survives until the order is fully received) and only flips to `RECEIVED` when `sum(order_receipts.quantity) >= orders.quantity`. The `dates` JSON gets `received_by_warehouse` (stamped on the first receipt) and `received` (stamped on the final receipt).
  - Optional `idempotencyKey` in the request body deduplicates retries: a second call with the same `(orderId, idempotencyKey)` returns the prior result without re-hitting Mintsoft.

- **Frontend mutations**
  - `POST /api/v1/orders` — full create payload (camelCase, see `UPDATABLE_FIELDS`)
  - `PUT /api/v1/orders/:id` — any subset of UPDATABLE_FIELDS plus `dates`
  - `PATCH /api/v1/orders/:id/status` — `status` + auto-stamped `dates` key
  - `POST /api/v1/orders/:id/split` — splits a row into two; touches `quantity`, `status`, `container_number`, `dates`
  - `POST /api/v1/containers/pack` — bulk version of split/consolidate
  - `PATCH /api/v1/containers/:cn/status` — bulk status change for everything on a container

  > **Caveat**: any frontend edit to a column that the Asana import owns is wiped on the next 10-min import. The persistent frontend-only columns are `vessel_name`, `notes`, and the `dates` keys not listed in the Asana import (`manufacturing`, `ready`, `delivered`, `completed`, `received_by_warehouse`, `received`, `asn_id`).

## Asana custom-field reference (A→Z)

| Asana field | DB column(s) it feeds |
|---|---|
| `Arrived` | `arrived_date` |
| `Artwork Confirmed Date` | `artwork_confirmed_date` |
| `ASIN` | `asin`, fallback for `jf_code` |
| `Booking Status` | `booking_status` |
| `CBM/Line` | `order_cbm` |
| `Container Status` | `container_status`, drives `status` mapping (Sea) |
| `Delivery Date` | `delivery_date` |
| `Delivery Time` | `delivery_time` |
| `Departure Date` | `shipped_date` (Air) |
| `Estimated Ready Date` | `estimated_ready_date` |
| `ETA to Port` | `eta` |
| `EXP DATE` | `exp_date` |
| `External Container Number` | `external_container_number` |
| `Goods Status` | drives `status` mapping (Orders) |
| `LOT number` | `lot_number` |
| `MFG DATE` | `mfg_date` |
| `Name` (task title) | `jf_code` |
| `PO Number` | `po_number`, fallback for `jf_code` |
| `PO Placed` | `ordered_date` |
| `Sailing Date` | `shipped_date` (Sea) |
| `Section/Column` *(section name, not a custom field)* | `container_number` |
| `SKU` | `product_name` |
| `Supplier Name` | `supplier` |
| `Total CBM.` | `cbm_per_unit` (÷ `Units Ordered`) |
| `Units` / `Units Ordered` | `quantity` |
