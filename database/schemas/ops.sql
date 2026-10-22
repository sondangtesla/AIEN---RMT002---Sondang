CREATE SCHEMA IF NOT EXISTS ops;

-- 1 baris = 1 kali pipeline dijalankan (1 DAG run)
CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    pipeline_run_id TEXT PRIMARY KEY,                 -- dikirim ke semua layer
    status          TEXT NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,                      -- NULL selama masih berjalan
    error_message   TEXT                              -- diisi kalau FAILED
);

-- 1 baris = 1 task di dalam 1 run (validate, load_bronze, build_silver, ...)
CREATE TABLE IF NOT EXISTS ops.task_runs (
    task_run_id     BIGSERIAL PRIMARY KEY,
    pipeline_run_id TEXT NOT NULL REFERENCES ops.pipeline_runs (pipeline_run_id),
    task_name       TEXT NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    rows_affected   BIGINT,                           -- berapa baris dimuat/dibangun
    error_message   TEXT
);

-- 1 baris = 1 hasil quality check di dalam 1 run
CREATE TABLE IF NOT EXISTS ops.quality_checks (
    check_id        BIGSERIAL PRIMARY KEY,
    pipeline_run_id TEXT NOT NULL REFERENCES ops.pipeline_runs (pipeline_run_id),
    layer           TEXT NOT NULL CHECK (layer IN ('bronze', 'silver', 'gold')),
    check_name      TEXT NOT NULL,                    -- mis. 'silver_orders_unique'
    status          TEXT NOT NULL CHECK (status IN ('PASS', 'FAIL')),
    observed_value  NUMERIC,                          -- angka yang ditemukan (mis. 3 duplikat)
    expected_value  TEXT,                             -- aturan (mis. '= 0')
    details         JSONB,                            -- info tambahan bebas
    checked_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Q5: status = HASIL (lolos aturan?), severity = KONSEKUENSI kalau gagal.
-- Gate hanya berhenti pada FAIL + ERROR. DEFAULT 'ERROR' = fail-safe (lupa diisi → memblokir).
-- ALTER (bukan edit CREATE TABLE): tabel lama di-skip oleh IF NOT EXISTS; ALTER benar untuk DB lama & baru.
ALTER TABLE ops.quality_checks
    ADD COLUMN IF NOT EXISTS severity TEXT NOT NULL DEFAULT 'ERROR'
        CHECK (severity IN ('ERROR', 'WARN'));