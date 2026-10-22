CREATE SCHEMA IF NOT EXISTS silver;

-- Pola semua tabel Silver:
--   * kolom bertipe benar (timestamptz = UTC, numeric untuk uang, integer untuk qty)
--   * PRIMARY KEY = grain → menjamin dedup tidak bisa bocor
--   * lineage: raw_record_id (baris Bronze asal) + pipeline_run_id (run yang membangun)
--   * diisi ulang penuh tiap run oleh build_silver.py (TRUNCATE + INSERT … SELECT)

-- =========================================================================
-- Record yang ditolak validasi (bukan duplikat — duplikat cukup dihitung)
-- =========================================================================
CREATE TABLE IF NOT EXISTS silver.rejected_records (
    rejected_id      BIGSERIAL PRIMARY KEY,
    source_name      TEXT NOT NULL,                  -- 'order_items', ...
    raw_record_id    BIGINT NOT NULL UNIQUE          -- D3: 1 record = 1 baris rejected (alasan pertama yang gagal)
                     REFERENCES bronze.raw_records (raw_record_id),
    reason           TEXT NOT NULL,                  -- 'NON_POSITIVE_QUANTITY', 'UNKNOWN_PRODUCT', ...
    payload          JSONB NOT NULL,                 -- salinan record asli, untuk investigasi
    pipeline_run_id  TEXT NOT NULL REFERENCES ops.pipeline_runs (pipeline_run_id),
    rejected_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- =========================================================================
-- Master
-- =========================================================================
CREATE TABLE IF NOT EXISTS silver.customers (
    customer_id      TEXT PRIMARY KEY,
    city_id          TEXT,
    customer_segment TEXT,
    created_at_utc   TIMESTAMPTZ NOT NULL,
    raw_record_id    BIGINT NOT NULL REFERENCES bronze.raw_records (raw_record_id),
    pipeline_run_id  TEXT NOT NULL REFERENCES ops.pipeline_runs (pipeline_run_id)
);

CREATE TABLE IF NOT EXISTS silver.products (
    product_id       TEXT PRIMARY KEY,
    product_name     TEXT,
    sku              TEXT,
    unit_price       NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
    raw_record_id    BIGINT NOT NULL REFERENCES bronze.raw_records (raw_record_id),
    pipeline_run_id  TEXT NOT NULL REFERENCES ops.pipeline_runs (pipeline_run_id)
);

-- Berversi: 1 product bisa punya >1 kategori sepanjang waktu (P7)
CREATE TABLE IF NOT EXISTS silver.product_categories (
    product_id       TEXT NOT NULL,
    valid_from_utc   TIMESTAMPTZ NOT NULL,
    valid_to_utc     TIMESTAMPTZ,                    -- NULL = masih berlaku
    category_id      TEXT NOT NULL,
    category_name    TEXT,
    raw_record_id    BIGINT NOT NULL REFERENCES bronze.raw_records (raw_record_id),
    pipeline_run_id  TEXT NOT NULL REFERENCES ops.pipeline_runs (pipeline_run_id),
    PRIMARY KEY (product_id, valid_from_utc),
    CHECK (valid_to_utc IS NULL OR valid_to_utc > valid_from_utc)
);

-- =========================================================================
-- Transaksi
-- =========================================================================
CREATE TABLE IF NOT EXISTS silver.orders (
    order_id         TEXT PRIMARY KEY,               -- dedup P1
    customer_id      TEXT NOT NULL,
    sales_channel    TEXT NOT NULL CHECK (sales_channel IN ('WEB', 'MOBILE_APP', 'MARKETPLACE', 'STORE')),
    store_id         TEXT,                           -- NULL untuk order online
    status           TEXT NOT NULL CHECK (status IN ('PLACED', 'CONFIRMED', 'FULFILLED', 'CANCELLED', 'RETURNED')),
    shipping_revenue NUMERIC(12, 2) NOT NULL CHECK (shipping_revenue >= 0),
    ordered_at_utc   TIMESTAMPTZ NOT NULL,
    updated_at_utc   TIMESTAMPTZ NOT NULL,
    raw_record_id    BIGINT NOT NULL REFERENCES bronze.raw_records (raw_record_id),
    pipeline_run_id  TEXT NOT NULL REFERENCES ops.pipeline_runs (pipeline_run_id)
);

CREATE TABLE IF NOT EXISTS silver.order_items (
    order_item_id        TEXT PRIMARY KEY,
    order_id             TEXT NOT NULL,
    product_id           TEXT NOT NULL,
    quantity             INTEGER NOT NULL CHECK (quantity > 0),   -- P5: qty <= 0 → rejected
    unit_price           NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
    item_discount_amount NUMERIC(12, 2) NOT NULL CHECK (item_discount_amount >= 0),
    raw_record_id        BIGINT NOT NULL REFERENCES bronze.raw_records (raw_record_id),
    pipeline_run_id      TEXT NOT NULL REFERENCES ops.pipeline_runs (pipeline_run_id)
);

CREATE TABLE IF NOT EXISTS silver.order_promotions (
    order_id         TEXT NOT NULL,
    promotion_id     TEXT NOT NULL,
    discount_amount  NUMERIC(12, 2) NOT NULL CHECK (discount_amount >= 0),
    raw_record_id    BIGINT NOT NULL REFERENCES bronze.raw_records (raw_record_id),
    pipeline_run_id  TEXT NOT NULL REFERENCES ops.pipeline_runs (pipeline_run_id),
    PRIMARY KEY (order_id, promotion_id)
);

-- =========================================================================
-- Event: 1 tabel per jenis, grain = 1 event (dedup P2, UTC P3)
-- occurred_at_utc = waktu bisnis · ingested_at_utc = waktu tiba (P9)
-- =========================================================================
CREATE TABLE IF NOT EXISTS silver.payment_events (
    event_id         TEXT PRIMARY KEY,
    event_type       TEXT NOT NULL CHECK (event_type IN ('PAYMENT_AUTHORIZED', 'PAYMENT_CAPTURED', 'PAYMENT_FAILED')),
    order_id         TEXT NOT NULL,
    payment_id       TEXT,
    amount           NUMERIC(12, 2),
    occurred_at_utc  TIMESTAMPTZ NOT NULL,
    ingested_at_utc  TIMESTAMPTZ NOT NULL,
    raw_record_id    BIGINT NOT NULL REFERENCES bronze.raw_records (raw_record_id),
    pipeline_run_id  TEXT NOT NULL REFERENCES ops.pipeline_runs (pipeline_run_id)
);

CREATE TABLE IF NOT EXISTS silver.refund_events (
    event_id         TEXT PRIMARY KEY,
    event_type       TEXT NOT NULL CHECK (event_type IN ('REFUND_ISSUED', 'REFUND_COMPLETED')),
    order_id         TEXT NOT NULL,
    refund_id        TEXT,
    amount           NUMERIC(12, 2),
    occurred_at_utc  TIMESTAMPTZ NOT NULL,
    ingested_at_utc  TIMESTAMPTZ NOT NULL,
    raw_record_id    BIGINT NOT NULL REFERENCES bronze.raw_records (raw_record_id),
    pipeline_run_id  TEXT NOT NULL REFERENCES ops.pipeline_runs (pipeline_run_id)
);

CREATE TABLE IF NOT EXISTS silver.return_events (
    event_id         TEXT PRIMARY KEY,
    event_type       TEXT NOT NULL CHECK (event_type IN ('RETURN_REQUESTED', 'RETURN_RECEIVED', 'RETURN_CLOSED')),
    order_id         TEXT NOT NULL,
    return_id        TEXT,
    occurred_at_utc  TIMESTAMPTZ NOT NULL,
    ingested_at_utc  TIMESTAMPTZ NOT NULL,
    raw_record_id    BIGINT NOT NULL REFERENCES bronze.raw_records (raw_record_id),
    pipeline_run_id  TEXT NOT NULL REFERENCES ops.pipeline_runs (pipeline_run_id)
);

CREATE TABLE IF NOT EXISTS silver.support_events (
    event_id         TEXT PRIMARY KEY,
    event_type       TEXT NOT NULL CHECK (event_type IN ('SUPPORT_TICKET_CREATED', 'SUPPORT_TICKET_CLOSED')),
    order_id         TEXT,
    customer_id      TEXT,
    ticket_id        TEXT,
    reason           TEXT,                           -- hanya terisi saat CREATED
    occurred_at_utc  TIMESTAMPTZ NOT NULL,
    ingested_at_utc  TIMESTAMPTZ NOT NULL,
    raw_record_id    BIGINT NOT NULL REFERENCES bronze.raw_records (raw_record_id),
    pipeline_run_id  TEXT NOT NULL REFERENCES ops.pipeline_runs (pipeline_run_id)
);

CREATE TABLE IF NOT EXISTS silver.web_events (
    event_id         TEXT PRIMARY KEY,
    event_type       TEXT NOT NULL CHECK (event_type IN ('PAGE_VIEW', 'ADD_TO_CART', 'PURCHASE')),
    session_id       TEXT,
    customer_id      TEXT,
    order_id         TEXT,
    channel          TEXT CHECK (channel IN ('WEB', 'MOBILE_APP', 'MARKETPLACE', 'STORE')),
    campaign_id      TEXT,                           -- NULL = tanpa campaign
    occurred_at_utc  TIMESTAMPTZ NOT NULL,
    ingested_at_utc  TIMESTAMPTZ NOT NULL,
    raw_record_id    BIGINT NOT NULL REFERENCES bronze.raw_records (raw_record_id),
    pipeline_run_id  TEXT NOT NULL REFERENCES ops.pipeline_runs (pipeline_run_id)
);

-- =========================================================================
-- Snapshot & referensi dari CSV (semua teks di Bronze → di-cast, P10)
-- =========================================================================
CREATE TABLE IF NOT EXISTS silver.inventory_snapshots (
    product_id         TEXT NOT NULL,
    location_id        TEXT NOT NULL,
    snapshot_date      DATE NOT NULL,
    available_quantity INTEGER NOT NULL CHECK (available_quantity >= 0),
    reserved_quantity  INTEGER NOT NULL CHECK (reserved_quantity >= 0),
    unit_cost          NUMERIC(12, 2),
    raw_record_id      BIGINT NOT NULL REFERENCES bronze.raw_records (raw_record_id),
    pipeline_run_id    TEXT NOT NULL REFERENCES ops.pipeline_runs (pipeline_run_id),
    PRIMARY KEY (product_id, location_id, snapshot_date)
);

CREATE TABLE IF NOT EXISTS silver.campaign_spend (
    spend_date       DATE NOT NULL,
    campaign_id      TEXT NOT NULL,
    channel          TEXT NOT NULL CHECK (channel IN ('WEB', 'MOBILE_APP', 'MARKETPLACE', 'STORE')),
    spend_amount     NUMERIC(12, 2) NOT NULL CHECK (spend_amount >= 0),
    raw_record_id    BIGINT NOT NULL REFERENCES bronze.raw_records (raw_record_id),
    pipeline_run_id  TEXT NOT NULL REFERENCES ops.pipeline_runs (pipeline_run_id),
    -- D10: channel = atribut campaign (data: 1 campaign = 1 channel), bukan bagian identitas.
    -- Key minimal → biaya dobel dengan channel berbeda ditolak (loud), tidak lolos diam-diam.
    PRIMARY KEY (spend_date, campaign_id)
);
