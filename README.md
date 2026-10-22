# P1M1 Omnichannel Retail: Pipeline Raw → Bronze → Silver → Gold

Pipeline batch lokal yang memuat 19 file raw retail omnichannel ke PostgreSQL
dalam tiga layer (Bronze, Silver, Gold), dijaga quality gate, dan diorkestrasi
Airflow. Gold mengikuti kontrak `database/schemas/gold.sql` dan menjadi
satu-satunya sumber data untuk NL-to-SQL engine.

```
data/raw (19 file)
   │  Airflow DAG: omnichannel_retail_pipeline (1 run = 1 pipeline_run_id)
   ▼
validate_raw_files → init_schemas → load_bronze → build_silver
   → quality_gate → build_gold → validate_gold_and_record
   │
   ▼
Bronze (evidence) → Silver (typed, UTC, dedup) → [gate] → Gold (5 tabel)
                                                           │ hanya gold.*
                         UI → engine → SQL read-only ──────┘
```

## Isi repo

| Path | Isi |
|---|---|
| `dags/dag.py` | DAG 7 task, logging ke `ops.task_runs`, status run ke `ops.pipeline_runs` |
| `database/schemas/ops.sql` | Metadata pipeline: run, task, quality check |
| `database/schemas/bronze.sql` | `source_files` + `raw_records` (payload JSONB + lineage) |
| `database/schemas/silver.sql` | 13 tabel typed + `rejected_records` |
| `pipelines/bronze/build_bronze.py` | Muat raw ke Bronze |
| `pipelines/silver/build_silver.py` | Bronze → Silver |
| `pipelines/quality/quality_gate.py` | 27 cek setelah Silver, fail-closed |
| `pipelines/gold/build_gold.py` | Silver → 5 tabel Gold |
| `pipelines/gold/validate_gold.py` | 23 cek validasi Gold |
| `shared/ops.py` | Helper pencatatan ke schema `ops` |
| `docker-compose.yml` | Template + service `airflow` |
| `evidence/airflow_run.png` | Riwayat run DAG (termasuk uji gate gagal) |
| `evidence/sample_engine_queries.md` | Jawaban engine + cek silang + temuan |

`shared/ops.py`, `quality_gate.py`, `validate_gold.py`, dan `docker-compose.yml`
tidak ada di daftar wajib README §10, tetapi di-import atau dibutuhkan
`dags/dag.py`, sehingga ikut disertakan.

## Cara menjalankan

Repo ini berisi file implementasi saja. Letakkan isinya di atas repo template
milestone (yang sudah punya `database/schemas/gold.sql`, `shared/db.py`,
`engine/`, `ui/`, dan `data/raw/` berisi 19 file).

```powershell
docker compose up -d postgres airflow
```

1. Buka `http://localhost:8080` (user `admin`; password:
   `docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt`).
2. Aktifkan DAG `omnichannel_retail_pipeline` (dibuat dalam keadaan paused).
3. Klik **Trigger DAG**. Satu run selesai sekitar 25 detik.

Engine + UI membaca Gold lokal:

```powershell
docker compose up -d nl2sql-api analytics-ui
```

Buka `http://localhost:8501`. Di dalam container, host database adalah
`postgres` (bukan `localhost`), karena itu `LOCAL_DATABASE_URL` di-set di
`docker-compose.yml` untuk service `airflow` dan `nl2sql-api`.

## Desain per layer

**Bronze**: payload asli disimpan apa adanya (JSONB), tanpa dedup. Setiap
record membawa file, nomor baris, source record id, sha256, dan
`pipeline_run_id`. `UNIQUE (source_path, file_sha256)` membuat rerun file yang
sama menghasilkan 0 record baru. Satu file dimuat dengan satu
`INSERT … SELECT FROM jsonb_array_elements(...)` dalam satu transaksi.

**Silver**: rebuild penuh (`TRUNCATE` + `INSERT … SELECT`) dalam satu
transaksi dari versi file terbaru di Bronze.

| Masalah di raw | Penanganan |
|---|---|
| Order & event duplikat (2.007) | Dedup deterministik per key (`ROW_NUMBER`) |
| ~10% event ber-offset `+07:00` | Cast ke `timestamptz` (UTC) |
| Qty ≤ 0 (1), product tidak dikenal (3) | `silver.rejected_records` + alasan |
| Split payment (1.915 order) | Disimpan per event, dijumlah di Gold |
| Kategori product berversi | `valid_from_utc` / `valid_to_utc` |
| Event telat (hingga 2 hari 8 jam) | Waktu bisnis = `occurred_at_utc` |

PK = grain di setiap tabel, sehingga kesalahan dedup gagal saat INSERT, tidak
lolos diam-diam.

**Quality gate**: 27 cek (uniqueness, revenue mismatch, refund mismatch,
nilai). Hasil dicatat ke `ops.quality_checks` lebih dulu; `ERROR` menghentikan
DAG, `WARN` tercatat dan lanjut.

**Gold**: setiap tabel anak (item, promo, payment, refund, return) diringkas ke
grain target sebelum di-join untuk mencegah fact multiplication.
`order_360` menjadi sumber tabel harian dan executive.

| Keputusan | Pilihan | Dasar |
|---|---|---|
| Tanggal bisnis | UTC | Inventory & spend persis 22 Jun–19 Sep UTC; WIB menggeser 29% order |
| Refund | Hanya `REFUND_COMPLETED` | ISSUED dan COMPLETED bernilai sama |
| Diskon | Diskon item + diskon promo | Capture = subtotal − kedua diskon + ongkir di 8.785 order |
| `return_status` | Dari event return | Status order tidak sinkron dengan event |
| Stockout | Stok tersedia product (semua lokasi) = 0 | Definisi engine; grain kontrak tanpa lokasi |
| Atribusi campaign | Campaign dari web session order | 1 order = 1 session = 1 campaign |

## Hasil run

| Layer | Hasil |
|---|---|
| Bronze | 19 file, 158.770 record; rerun = 0 record baru |
| Silver | 13 tabel, 151.453 baris; 4 rejected |
| Quality gate | 26 PASS + 1 WARN (254 refund pada order CANCELLED tanpa capture) |
| Gold | 31.210 baris di 5 tabel; validasi 23/23 PASS |
| DAG | Rerun tanpa duplikasi; uji gate gagal → Gold `upstream_failed`, run FAILED |

Rekonsiliasi utama: net revenue `order_360` = executive = 13.429.824,83 ·
units 3 jalur = 62.379 · spend Gold = Silver = 231.441,30.

## Temuan pada engine

Benchmark engine 25/25 PASS hanya berarti query berjalan. Review 25 SQL
benchmark terhadap definisi kontrak menemukan 4 jawaban yang menyimpang
(refund rate per channel, repeat rate harian, refund per kategori, fully
refunded orders). Detail dan buktinya ada di `evidence/sample_engine_queries.md`.

## Keterbatasan

- Gold tidak bisa menjawab revenue per kota, margin, stockout per toko, atau
  return per product; datanya ada di raw tetapi tidak ada di kontrak.
- Bronze membaca satu file utuh ke memori; untuk file besar perlu chunking
  atau `COPY`.
- Silver dan Gold dibangun ulang penuh setiap run; untuk data besar perlu
  incremental load.
- `customer_profiles`, `customer_addresses`, `stores`, `sales_channels`,
  `promotions`, dan `city_reference` belum dimodelkan di Silver.
