CREATE SCHEMA IF NOT EXISTS bronze;

-- 1 baris = 1 VERSI file yang pernah dimuat
CREATE TABLE IF NOT EXISTS bronze.source_files (
    source_file_id   BIGSERIAL PRIMARY KEY,
    source_name      TEXT NOT NULL,          -- 'orders', 'payment_events', ...
    source_path      TEXT NOT NULL,          -- 'operational/orders.json' (relatif ke data/raw)
    file_format      TEXT NOT NULL CHECK (file_format IN ('json', 'csv')),
    file_sha256      TEXT NOT NULL,          -- sidik jari isi file
    file_size_bytes  BIGINT NOT NULL,
    record_count     INTEGER NOT NULL,       -- jumlah record di file
    pipeline_run_id  TEXT NOT NULL REFERENCES ops.pipeline_runs (pipeline_run_id),
    loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source_path, file_sha256)        -- ← kunci idempotency
);

-- 1 baris = 1 record dari file, apa adanya
CREATE TABLE IF NOT EXISTS bronze.raw_records (
    raw_record_id    BIGSERIAL PRIMARY KEY,
    source_file_id   BIGINT NOT NULL REFERENCES bronze.source_files (source_file_id),
    source_name      TEXT NOT NULL,          -- disalin dari source_files supaya mudah difilter
    record_number    INTEGER NOT NULL,       -- posisi record di file (mulai dari 1)
    source_record_id TEXT,                   -- event_id / source_row_id; NULL kalau sumber tidak punya
    payload          JSONB NOT NULL,         -- record asli, TIDAK diubah
    record_sha256    TEXT NOT NULL,          -- sidik jari isi record
    pipeline_run_id  TEXT NOT NULL REFERENCES ops.pipeline_runs (pipeline_run_id),
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source_file_id, record_number)   -- satu posisi hanya boleh masuk sekali
);

CREATE INDEX IF NOT EXISTS raw_records_source_name_idx
    ON bronze.raw_records (source_name);

-- B2: hanya record dari VERSI FILE TERBARU per source_path.
-- Dibaca oleh Silver DAN rekonsiliasi → definisi "baris Bronze" selalu sama.
-- View = query bernama, tidak menyimpan data; selalu mengikuti isi Bronze terkini.
CREATE OR REPLACE VIEW bronze.latest_raw_records AS
SELECT r.*
FROM bronze.raw_records r
JOIN (
    SELECT DISTINCT ON (source_path) source_file_id
    FROM bronze.source_files
    ORDER BY source_path, loaded_at DESC, source_file_id DESC   -- terbaru dulu; seri → id terbesar
) latest USING (source_file_id);