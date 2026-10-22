"""DAG utama P1M1: raw -> Bronze -> Silver -> quality gate -> Gold -> validasi (README §8).

Desain: gold_explained.md bagian "DAG (A1-A8)".
- 1 pipeline_run_id untuk seluruh run = run_id Airflow (A5).
- Task memanggil fungsi Python pipeline langsung (A6).
- Tiap task dicatat di ops.task_runs; gagal -> pipeline_runs FAILED; task terakhir -> SUCCESS (A7).
- Gagal = raise -> task berikutnya upstream_failed (A8, trigger_rule bawaan all_success).
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:  # supaya `shared` & `pipelines` bisa di-import
    sys.path.insert(0, str(PROJECT_ROOT))


# ---------- Pembungkus pencatatan ops (A7) ----------

def _ops_ready(conn) -> bool:
    """Di DB baru, schema ops baru ada setelah init_schemas -> task pertama tidak bisa dicatat."""
    ready = conn.execute("SELECT to_regclass('ops.task_runs') IS NOT NULL").fetchone()[0]
    conn.commit()  # jangan biarkan koneksi pencatat menggantung di dalam transaksi
    return ready


def _run_logged(task_name: str, work) -> int:
    """Jalankan `work(run_id)` sambil mencatat RUNNING -> SUCCESS/FAILED ke ops."""
    from shared.db import connect
    from shared.ops import ensure_pipeline_run, finish_pipeline_run, finish_task_run, start_task_run

    run_id = get_current_context()["run_id"]  # A5: 1 id untuk semua task dalam run ini
    conn = connect("local")
    task_run_id = None
    try:
        if _ops_ready(conn):
            ensure_pipeline_run(conn, run_id)
            task_run_id = start_task_run(conn, run_id, task_name)
            conn.commit()

        rows = work(run_id)  # fungsi pipeline membuka koneksinya sendiri

        if task_run_id is None and _ops_ready(conn):  # init_schemas baru saja membuat ops
            ensure_pipeline_run(conn, run_id)
            task_run_id = start_task_run(conn, run_id, task_name)
        if task_run_id is not None:
            finish_task_run(conn, task_run_id, "SUCCESS", rows_affected=rows)
            conn.commit()
        return rows
    except Exception as exc:
        conn.rollback()
        if _ops_ready(conn):  # catat kegagalan di transaksi terpisah -> tidak ikut hilang
            ensure_pipeline_run(conn, run_id)
            if task_run_id is None:
                task_run_id = start_task_run(conn, run_id, task_name)
            finish_task_run(conn, task_run_id, "FAILED", error=str(exc)[:2000])
            finish_pipeline_run(conn, run_id, "FAILED", error=f"{task_name}: {exc}"[:2000])
            conn.commit()
        raise  # A8: task FAILED -> task berikutnya upstream_failed
    finally:
        conn.close()


# ---------- Pekerjaan tiap task (memanggil fungsi pipeline yang sudah teruji) ----------

def _validate_raw_files(run_id: str) -> int:
    from pipelines.bronze.build_bronze import DEFAULT_RAW_DIR, validate_raw_files

    return len(validate_raw_files(DEFAULT_RAW_DIR))  # 19 file wajib ada


def _init_schemas(run_id: str) -> int:
    from shared.db import PROJECT_ROOT as ROOT
    from shared.db import apply_sql_file, connect

    conn = connect("local")
    try:
        for name in ["ops", "bronze", "silver", "gold"]:  # urutan FK: ops dulu
            apply_sql_file(conn, ROOT / "database" / "schemas" / f"{name}.sql")
        conn.commit()  # semua DDL idempotent (IF NOT EXISTS / OR REPLACE / ADD COLUMN IF NOT EXISTS)
    finally:
        conn.close()
    return 4


def _load_bronze(run_id: str) -> int:
    from pipelines.bronze.build_bronze import DEFAULT_RAW_DIR, build_bronze

    return sum(build_bronze(DEFAULT_RAW_DIR, run_id).values())  # rerun -> 0 (idempotent)


def _build_silver(run_id: str) -> int:
    from pipelines.silver.build_silver import build_silver

    return sum(c["silver"] for c in build_silver(run_id).values())


def _quality_gate(run_id: str) -> int:
    from pipelines.quality.quality_gate import enforce_gate, run_checks
    from shared.db import connect

    conn = connect("local")
    try:
        results = run_checks(conn, run_id)  # ukur + COMMIT
        enforce_gate(conn, run_id)          # nilai: raise -> build_gold tidak jalan
    finally:
        conn.close()
    return len(results)


def _build_gold(run_id: str) -> int:
    from pipelines.gold.build_gold import build_gold

    return sum(build_gold(run_id).values())


def _validate_gold_and_record(run_id: str) -> int:
    from pipelines.gold.validate_gold import validate_gold
    from shared.db import connect
    from shared.ops import finish_pipeline_run

    conn = connect("local")
    try:
        results = validate_gold(conn, run_id)        # raise kalau ada FAIL
        finish_pipeline_run(conn, run_id, "SUCCESS")  # record pipeline metadata
        conn.commit()
    finally:
        conn.close()
    return len(results)


# ---------- DAG ----------

@dag(
    dag_id="omnichannel_retail_pipeline",
    description="P1M1: raw -> Bronze -> Silver -> quality gate -> Gold -> validasi",
    schedule=None,                      # dijalankan manual (Trigger DAG)
    start_date=datetime(2026, 10, 1),
    catchup=False,
    default_args={"retries": 0},        # A8: kegagalan data tidak diulang percuma
    tags=["p1m1", "retail"],
)
def omnichannel_retail_pipeline():
    @task
    def validate_raw_files() -> int:
        return _run_logged("validate_raw_files", _validate_raw_files)

    @task
    def init_schemas() -> int:
        return _run_logged("init_schemas", _init_schemas)

    @task
    def load_bronze() -> int:
        return _run_logged("load_bronze", _load_bronze)

    @task
    def build_silver() -> int:
        return _run_logged("build_silver", _build_silver)

    @task
    def quality_gate() -> int:
        return _run_logged("quality_gate", _quality_gate)

    @task
    def build_gold() -> int:
        return _run_logged("build_gold", _build_gold)

    @task
    def validate_gold_and_record() -> int:
        return _run_logged("validate_gold_and_record", _validate_gold_and_record)

    (validate_raw_files() >> init_schemas() >> load_bronze() >> build_silver()
     >> quality_gate() >> build_gold() >> validate_gold_and_record())


omnichannel_retail_pipeline()
