"""Helper metadata pipeline (schema ops), dipakai bersama oleh semua layer.

Bronze dan Silver tidak saling import: keduanya bergantung pada modul ini (B9).
"""

from __future__ import annotations

import json


def ensure_pipeline_run(conn, pipeline_run_id: str) -> None:
    """Pastikan run tercatat di ops.pipeline_runs (semua layer punya FK ke sini).

    Aman dipanggil berulang. Pemanggil wajib COMMIT sebelum transaksi data dimulai,
    supaya catatan run tidak ikut hilang kalau transaksi data di-rollback (B7).
    """
    conn.execute(
        """
        INSERT INTO ops.pipeline_runs (pipeline_run_id, status)
        VALUES (%s, 'RUNNING')
        ON CONFLICT (pipeline_run_id) DO NOTHING
        """,
        [pipeline_run_id],
    )


def finish_pipeline_run(conn, pipeline_run_id: str, status: str, error: str | None = None) -> None:
    """Tutup run: SUCCESS (task terakhir) atau FAILED (task mana pun gagal) + waktu selesai (A7).

    Menutup gap sejak Bronze: status tidak lagi berhenti di RUNNING selamanya.
    """
    conn.execute(
        """
        UPDATE ops.pipeline_runs
        SET status = %s, finished_at = now(), error_message = %s
        WHERE pipeline_run_id = %s
        """,
        [status, error, pipeline_run_id],
    )


def start_task_run(conn, pipeline_run_id: str, task_name: str) -> int:
    """Catat 1 task mulai (RUNNING). Return task_run_id untuk ditutup nanti."""
    return conn.execute(
        """
        INSERT INTO ops.task_runs (pipeline_run_id, task_name, status)
        VALUES (%s, %s, 'RUNNING')
        RETURNING task_run_id
        """,
        [pipeline_run_id, task_name],
    ).fetchone()[0]


def finish_task_run(
    conn, task_run_id: int, status: str, rows_affected: int | None = None, error: str | None = None
) -> None:
    """Tutup 1 task: SUCCESS + jumlah baris, atau FAILED + pesan error."""
    conn.execute(
        """
        UPDATE ops.task_runs
        SET status = %s, finished_at = now(), rows_affected = %s, error_message = %s
        WHERE task_run_id = %s
        """,
        [status, rows_affected, error, task_run_id],
    )


def record_quality_check(
    conn,
    pipeline_run_id: str,
    layer: str,
    check_name: str,
    passed: bool,
    observed_value: float,
    expected_value: str,
    details: dict,
    severity: str = "ERROR",
) -> None:
    """Catat 1 hasil quality check. Hanya MENCATAT; yang memutuskan berhenti = quality gate (B8).

    severity: 'ERROR' (gagal -> gate berhenti) atau 'WARN' (gagal -> tercatat saja). Default ERROR (Q5).
    """
    conn.execute(
        """
        INSERT INTO ops.quality_checks
            (pipeline_run_id, layer, check_name, status, observed_value, expected_value,
             details, severity)
        VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s)
        """,
        [pipeline_run_id, layer, check_name, "PASS" if passed else "FAIL",
         observed_value, expected_value, json.dumps(details), severity],
    )
