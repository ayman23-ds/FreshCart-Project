-- =============================================================================
-- FreshCart source DWH : Bronze tables definitions
-- This file runs automatically the first time the warehouse container starts.
-- =============================================================================

-- 1. Stores
CREATE TABLE IF NOT EXISTS bronze_app.stores (
    bronze_id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    store_id      INTEGER NOT NULL,
    store_name    TEXT NOT NULL,
    city          TEXT NOT NULL,
    country       TEXT NOT NULL,
    currency      CHAR(3) NOT NULL,
    opened_at     DATE NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL,
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_stores_version UNIQUE (store_id, updated_at)
);

-- 2. Customers
CREATE TABLE IF NOT EXISTS bronze_app.customers (
    bronze_id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id   INTEGER NOT NULL,
    first_name    TEXT NOT NULL,
    last_name     TEXT NOT NULL,
    email         TEXT NOT NULL,
    phone         TEXT,
    city          TEXT NOT NULL,
    signup_date   DATE NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL,
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_customers_version UNIQUE (customer_id, updated_at)
);

-- 3. Products
CREATE TABLE IF NOT EXISTS bronze_app.products (
    bronze_id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id      INTEGER NOT NULL,
    product_name    TEXT NOT NULL,
    category        TEXT NOT NULL,
    unit_price_usd  NUMERIC(10,2) NOT NULL,
    is_active       BOOLEAN NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_products_version UNIQUE (product_id, updated_at)
);

-- 4. Orders
CREATE TABLE IF NOT EXISTS bronze_app.orders (
    bronze_id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id      INTEGER NOT NULL,
    customer_id   INTEGER NOT NULL,
    store_id      INTEGER NOT NULL,
    order_status  TEXT NOT NULL,
    placed_at     TIMESTAMPTZ NOT NULL,
    delivered_at  TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL,
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_orders_version UNIQUE (order_id, updated_at)
);

-- 5. Order Items
CREATE TABLE IF NOT EXISTS bronze_app.order_items (
    bronze_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_item_id  INTEGER NOT NULL,
    order_id       INTEGER NOT NULL,
    product_id     INTEGER NOT NULL,
    quantity       INTEGER NOT NULL,
    unit_price     NUMERIC(10,2) NOT NULL,
    line_amount    NUMERIC(12,2) NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL,
    ingested_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_order_items_version UNIQUE (order_item_id, updated_at)
);

-- 6. Payments
CREATE TABLE IF NOT EXISTS bronze_app.payments (
    bronze_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    payment_id     INTEGER NOT NULL,
    order_id       INTEGER NOT NULL,
    payment_method TEXT NOT NULL,
    payment_status TEXT NOT NULL,
    amount         NUMERIC(12,2) NOT NULL,
    paid_at        TIMESTAMPTZ NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL,
    ingested_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_payments_version UNIQUE (payment_id, updated_at)
);

-- 7. Product Prices
CREATE TABLE IF NOT EXISTS bronze_app.product_prices (
    bronze_id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id      INTEGER NOT NULL,
    unit_price_usd  NUMERIC NOT NULL,
    valid_from      TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_product_prices_version
        UNIQUE (product_id, valid_from, updated_at)
);


-- 8. Fresh Stock
CREATE TABLE IF NOT EXISTS bronze_suppliers.fresh_stock (
    bronze_id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    supplier          TEXT,
    product_id        INTEGER,
    store_id          INTEGER,
    stock_date        DATE,
    quantity_on_hand  INTEGER,
    reorder_level     INTEGER,
    source_file       TEXT NOT NULL,
    source_row_number INTEGER NOT NULL,
    ingested_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_fresh_stock_file_row
        UNIQUE (source_file, source_row_number)
);

-- 9. General Stock
CREATE TABLE IF NOT EXISTS bronze_suppliers.general_stock (
    bronze_id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    "Supplier"        TEXT,
    "Product"         TEXT,
    "Store"           TEXT,
    "Date"            TEXT,
    "Qty"             TEXT,
    "MinQty"          TEXT,
    source_file       TEXT NOT NULL,
    source_row_number INTEGER NOT NULL,
    ingested_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_general_stock_file_row
        UNIQUE (source_file, source_row_number)
);

-- ============================================================
-- 11. Supplier File Ingestion Tracking
-- Tracks successfully loaded CSV files
-- Prevents processing the same file twice
-- ============================================================

CREATE TABLE IF NOT EXISTS bronze_suppliers.ingestion_files (
    supplier_type TEXT NOT NULL,
    source_file   TEXT NOT NULL,
    loaded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    row_count     INTEGER NOT NULL,

    CONSTRAINT pk_ingestion_files
        PRIMARY KEY (supplier_type, source_file)
);

-- ============================================================
-- 10. FX Rates - Raw API Responses
-- ============================================================

CREATE TABLE IF NOT EXISTS bronze_fx.fx_rates (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    response_data JSONB NOT NULL,

    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_fx_response UNIQUE (response_data)
);

