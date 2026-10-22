"""Quality gate Silver: ukur semua cek -> catat ke ops -> nilai (berhenti kalau ada ERROR).

Desain lengkap: silver_quality_explained.md (Q1-Q8).
Posisi di DAG: build Silver -> quality gate -> build Gold.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass

from shared.db import connect
from shared.ops import record_quality_check


@dataclass(frozen=True)
class Check:
    """1 cek = 1 query yang mengembalikan (observed, details). Lolos kalau observed = 0."""

    name: str
    sql: str               # SELECT <angka> AS observed, <jsonb atau NULL> AS details
    severity: str = "ERROR"


# ---------- 1. Definisi cek ----------

# Q2 - duplikasi: key = grain tiap tabel Silver (sama dengan PK di silver.sql, D1)
UNIQUE_KEYS = {
    "customers": "customer_id",
    "products": "product_id",
    "product_categories": "(product_id, valid_from_utc)",
    "orders": "order_id",
    "order_items": "order_item_id",
    "order_promotions": "(order_id, promotion_id)",
    "payment_events": "event_id",
    "refund_events": "event_id",
    "return_events": "event_id",
    "support_events": "event_id",
    "web_events": "event_id",
    "inventory_snapshots": "(product_id, location_id, snapshot_date)",
    "campaign_spend": "(spend_date, campaign_id)",
}

UNIQUE_CHECKS = [
    Check(
        name=f"silver_unique_{table}",
        sql=f"SELECT count(*) - count(DISTINCT {key}) AS observed, NULL::jsonb AS details "
            f"FROM silver.{table}",
    )
    for table, key in UNIQUE_KEYS.items()
]

# CTE bersama untuk Q3 & Q4: tiap tabel anak diringkas ke grain order DULU (anti fan-out)
_CAPS = """
    caps AS (SELECT order_id, SUM(amount) AS captured
             FROM silver.payment_events WHERE event_type = 'PAYMENT_CAPTURED'
             GROUP BY order_id)"""
_REFUNDS = """
    refunds AS (SELECT order_id, SUM(amount) AS refunded       -- COMPLETED saja (T2)
                FROM silver.refund_events WHERE event_type = 'REFUND_COMPLETED'
                GROUP BY order_id)"""

# Q3 - revenue mismatch: capture = GREATEST(item - diskon item - diskon promo + ongkir, 0) (T1)
REVENUE_CHECK = Check(
    name="silver_revenue_mismatch",
    sql=f"""
        WITH items AS (SELECT order_id, SUM(quantity * unit_price - item_discount_amount) AS net_items
                       FROM silver.order_items GROUP BY order_id),
             promos AS (SELECT order_id, SUM(discount_amount) AS promo_disc
                        FROM silver.order_promotions GROUP BY order_id),
             excluded AS (SELECT DISTINCT payload->>'order_id' AS order_id     -- item ter-reject
                          FROM silver.rejected_records WHERE source_name = 'order_items'),
             {_CAPS},
             compared AS (
                SELECT c.order_id,
                       abs(c.captured - GREATEST(COALESCE(i.net_items, 0)
                                                 - COALESCE(p.promo_disc, 0)
                                                 + o.shipping_revenue, 0)) > 0.01 AS is_mismatch
                FROM caps c
                JOIN silver.orders o USING (order_id)
                LEFT JOIN items  i USING (order_id)
                LEFT JOIN promos p USING (order_id)
                WHERE c.order_id NOT IN (SELECT order_id FROM excluded)
             )
        SELECT count(*) FILTER (WHERE is_mismatch) AS observed,
               jsonb_build_object(
                   'orders_compared', count(*),
                   'excluded_orders_with_rejected_items', (SELECT count(*) FROM excluded)
               ) AS details
        FROM compared
    """,
)

# Q4 - refund mismatch: 4 cek
REFUND_CHECKS = [
    Check(  # 4a: refund melebihi uang yang diterima (order yang punya capture)
        name="silver_refund_over_captured",
        sql=f"""
            WITH {_REFUNDS}, {_CAPS}
            SELECT count(*) AS observed, NULL::jsonb AS details
            FROM refunds r JOIN caps c USING (order_id)
            WHERE r.refunded > c.captured + 0.01
        """,
    ),
    Check(  # 4b: tiap refund_id hanya boleh COMPLETED sekali (jaga T2 untuk Gold)
        name="silver_refund_completed_once",
        sql="""
            SELECT count(*) AS observed, NULL::jsonb AS details
            FROM (SELECT refund_id FROM silver.refund_events
                  WHERE event_type = 'REFUND_COMPLETED'
                  GROUP BY refund_id HAVING count(*) > 1) x
        """,
    ),
    Check(  # 4c: refund tanpa capture pada order CANCELLED = pola dikenal (T3) -> WARN
        name="silver_refund_without_capture_cancelled",
        severity="WARN",
        sql=f"""
            WITH {_REFUNDS}, {_CAPS}
            SELECT count(*) AS observed, NULL::jsonb AS details
            FROM refunds r
            JOIN silver.orders o USING (order_id)
            LEFT JOIN caps c USING (order_id)
            WHERE c.order_id IS NULL AND o.status = 'CANCELLED'
        """,
    ),
    Check(  # 4d: refund tanpa capture pada status lain = pola baru -> ERROR
        name="silver_refund_without_capture_other",
        sql=f"""
            WITH {_REFUNDS}, {_CAPS}
            SELECT COALESCE(SUM(n), 0) AS observed,                        -- jumlah ORDER, bukan jumlah status
                   COALESCE(jsonb_object_agg(status, n), '{{}}'::jsonb) AS details  -- rincian per status
            FROM (SELECT o.status, count(*) AS n
                  FROM refunds r
                  JOIN silver.orders o USING (order_id)
                  LEFT JOIN caps c USING (order_id)
                  WHERE c.order_id IS NULL AND o.status <> 'CANCELLED'
                  GROUP BY o.status) x
        """,
    ),
]

# Q6 - rekonsiliasi nilai: Silver vs payload Bronze asalnya, dipasangkan lewat raw_record_id (D4)
VALUE_COLUMNS = [
    # (tabel silver, kolom silver, ekspresi nilai yang sama dari payload Bronze)
    ("orders", "shipping_revenue", "b.payload->>'shipping_revenue'"),
    ("order_items", "quantity", "b.payload->>'quantity'"),
    ("order_items", "unit_price", "b.payload->>'unit_price'"),
    ("order_items", "item_discount_amount", "b.payload->>'item_discount_amount'"),
    ("order_promotions", "discount_amount", "b.payload->>'discount_amount'"),
    ("payment_events", "amount", "b.payload->'payload'->>'amount'"),
    ("refund_events", "amount", "b.payload->'payload'->>'amount'"),
    ("inventory_snapshots", "available_quantity", "b.payload->>'available_quantity'"),
    ("campaign_spend", "spend_amount", "b.payload->>'spend_amount'"),
]

VALUE_CHECKS = [
    Check(
        name=f"silver_value_{table}_{column}",
        sql=f"""
            SELECT abs(COALESCE(SUM(s.{column}), 0) - COALESCE(SUM(({bronze_expr})::numeric), 0))
                       AS observed,
                   jsonb_build_object('silver_sum', SUM(s.{column}),
                                      'bronze_sum', SUM(({bronze_expr})::numeric)) AS details
            FROM silver.{table} s
            JOIN bronze.raw_records b USING (raw_record_id)
        """,
    )
    for table, column, bronze_expr in VALUE_COLUMNS
]

ALL_CHECKS = UNIQUE_CHECKS + [REVENUE_CHECK] + REFUND_CHECKS + VALUE_CHECKS


# ---------- 2. Mengukur (Q1) ----------

def run_checks(conn, pipeline_run_id: str) -> list[tuple[str, str, float, bool]]:
    """Jalankan SEMUA cek, catat semuanya ke ops dalam 1 transaksi. Tidak pernah raise karena cek gagal."""
    results = []
    try:
        for check in ALL_CHECKS:
            observed, details = conn.execute(check.sql).fetchone()
            passed = observed == 0
            record_quality_check(
                conn, pipeline_run_id, "silver", check.name,
                passed=passed, observed_value=observed, expected_value="= 0",
                details=details or {}, severity=check.severity,
            )
            results.append((check.name, check.severity, observed, passed))
        conn.commit()  # semua atau tidak sama sekali -> dasar fail-closed Q7
    except Exception:
        conn.rollback()  # cek gagal DIJALANKAN (SQL error) -> tidak ada catatan -> gate fail-closed
        raise
    return results


# ---------- 3. Menilai (Q1 + Q7) ----------

def enforce_gate(conn, pipeline_run_id: str) -> list[str]:
    """Baca SEMUA catatan cek run ini. Tanpa bukti -> raise (Q7). Ada FAIL + ERROR -> raise.

    Return daftar FAIL + WARN (tercatat, tidak menghentikan pipeline).
    """
    recon, others = conn.execute(
        """
        SELECT count(*) FILTER (WHERE starts_with(check_name, 'silver_reconciliation_')),
               count(*) FILTER (WHERE NOT starts_with(check_name, 'silver_reconciliation_'))
        FROM ops.quality_checks
        WHERE pipeline_run_id = %s AND layer = 'silver'
        """,
        [pipeline_run_id],
    ).fetchone()
    # Q7 fail-closed: B7 & run_checks atomic -> "minimal 1 ada" = "semua ada"
    if recon == 0 or others == 0:
        raise RuntimeError(
            f"Quality gate FAILED: tidak ada bukti quality untuk run {pipeline_run_id} "
            f"(rekonsiliasi build_silver={recon}, cek run_checks={others})"
        )

    # Q9: nilai HASIL TERBARU per cek. Task Airflow bisa diulang dengan run id yang sama
    # ("Clear") -> FAIL lama tidak boleh memblokir selamanya. Riwayat tetap utuh di ops.
    # check_id, bukan checked_at: now() = waktu MULAI transaksi -> 27 cek 1 run punya waktu sama.
    rows = conn.execute(
        """
        SELECT check_name, severity
        FROM (
            SELECT DISTINCT ON (check_name) check_name, status, severity
            FROM ops.quality_checks
            WHERE pipeline_run_id = %s AND layer = 'silver'
            ORDER BY check_name, check_id DESC
        ) latest
        WHERE status = 'FAIL'
        ORDER BY check_name
        """,
        [pipeline_run_id],
    ).fetchall()
    errors = [name for name, severity in rows if severity == "ERROR"]
    warnings = [name for name, severity in rows if severity == "WARN"]
    if errors:
        raise RuntimeError(f"Quality gate FAILED untuk run {pipeline_run_id}: {errors}")
    return warnings


# ---------- 4. Orkestrasi: 1 task DAG = ukur lalu nilai ----------

def main() -> None:
    parser = argparse.ArgumentParser(description="Silver quality gate.")
    parser.add_argument("--pipeline-run-id", required=True)  # wajib: harus run yang sama dengan build_silver
    args = parser.parse_args()

    conn = connect("local")
    try:
        results = run_checks(conn, args.pipeline_run_id)      # ukur + COMMIT dulu (bukti aman)
        for name, severity, observed, passed in results:
            print(f"{'PASS' if passed else 'FAIL':<5} {severity:<6} {name:<45} observed={observed}")
        print(f"{len(results)} cek | run {args.pipeline_run_id}")

        warnings = enforce_gate(conn, args.pipeline_run_id)   # nilai: raise -> task Airflow FAILED
        print(f"GATE PASS | WARN tercatat: {warnings or 'tidak ada'}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
