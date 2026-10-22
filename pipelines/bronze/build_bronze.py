"""Load raw files from data/raw into Bronze without changing payloads."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from shared.db import PROJECT_ROOT, connect
from shared.ops import ensure_pipeline_run

DEFAULT_RAW_DIR = PROJECT_ROOT / "data" / "raw"

# Daftar file yang WAJIB ada (README bagian 6). Ini pengganti manifest.
EXPECTED_FILES = [
    "operational/customers.json",
    "operational/customer_profiles.json",
    "operational/customer_addresses.json",
    "operational/products.json",
    "operational/product_categories.json",
    "operational/stores.json",
    "operational/sales_channels.json",
    "operational/promotions.json",
    "operational/orders.json",
    "operational/order_items.json",
    "operational/order_promotions.json",
    "events/payment_events.json",
    "events/refund_events.json",
    "events/return_events.json",
    "events/support_events.json",
    "events/web_events.json",
    "inventory/inventory_snapshots.csv",
    "reference/campaign_spend.csv",
    "reference/city_reference.json",
]


# ---------- 1. Validasi & baca file ----------

def validate_raw_files(raw_dir: Path) -> list[Path]:
    """Pastikan ke-19 file ada sebelum memuat apa pun."""
    missing = [name for name in EXPECTED_FILES if not (raw_dir / name).is_file()]
    if missing:
        raise FileNotFoundError(f"Raw file tidak ditemukan: {missing}")
    return [raw_dir / name for name in EXPECTED_FILES]


def file_sha256(path: Path) -> str:
    """Sidik jari isi file; berubah 1 byte saja -> hash berbeda."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_records(path: Path) -> list[dict]:
    """JSON -> list of dict apa adanya. CSV -> list of dict, semua nilai tetap TEKS."""
    if path.suffix == ".json":
        records = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(records, list):
            raise ValueError(f"{path.name}: isi JSON harus berupa array")
        return records
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


# ---------- 2. Tulis ke database ----------

def load_file(conn, raw_dir: Path, path: Path, pipeline_run_id: str) -> int:
    """Muat 1 file. Return jumlah record yang masuk (0 = sudah pernah dimuat)."""
    source_path = path.relative_to(raw_dir).as_posix()
    records = read_records(path)

    row = conn.execute(
        """
        INSERT INTO bronze.source_files
            (source_name, source_path, file_format, file_sha256,
             file_size_bytes, record_count, pipeline_run_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (source_path, file_sha256) DO NOTHING
        RETURNING source_file_id
        """,
        [path.stem, source_path, path.suffix.lstrip("."), file_sha256(path),
         path.stat().st_size, len(records), pipeline_run_id],
    ).fetchone()

    if row is None:  # konflik -> versi file ini sudah ada -> idempotent skip
        return 0

    conn.execute(
        """
        INSERT INTO bronze.raw_records
            (source_file_id, source_name, record_number, source_record_id,
             payload, record_sha256, pipeline_run_id)
        SELECT %s, %s, t.ord,
               COALESCE(t.elem ->> 'event_id', t.elem ->> 'source_row_id'),
               t.elem,
               encode(sha256(convert_to(t.elem::text, 'UTF8')), 'hex'),
               %s
        FROM jsonb_array_elements(%s::jsonb) WITH ORDINALITY AS t(elem, ord)
        """,
        [row[0], path.stem, pipeline_run_id, json.dumps(records)],
    )
    return len(records)


# ---------- 3. Orkestrasi ----------

def build_bronze(raw_dir: Path, pipeline_run_id: str) -> dict[str, int]:
    files = validate_raw_files(raw_dir)
    summary: dict[str, int] = {}
    conn = connect("local")
    try:
        ensure_pipeline_run(conn, pipeline_run_id)
        conn.commit()
        for path in files:
            try:
                summary[path.stem] = load_file(conn, raw_dir, path, pipeline_run_id)
                conn.commit()  # 1 file = 1 transaksi: masuk semua atau tidak sama sekali
            except Exception as exc:
                conn.rollback()
                raise RuntimeError(f"Gagal memuat {path.name}") from exc
    finally:
        conn.close()
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Load raw files into Bronze.")
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument(
        "--pipeline-run-id",
        default=f"manual__{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}",
    )
    args = parser.parse_args()

    summary = build_bronze(args.raw_dir, args.pipeline_run_id)
    for source_name, count in summary.items():
        print(f"{source_name:<22} {count:>7} record baru")
    print(f"TOTAL {sum(summary.values())} record | run {args.pipeline_run_id}")


if __name__ == "__main__":
    main()