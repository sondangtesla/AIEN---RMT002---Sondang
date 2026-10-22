"""Validasi Gold (G6): grain, kelengkapan, rekonsiliasi antar tabel, aturan bisnis.

Pola sama dengan quality gate Silver: ukur semua -> catat ke ops (layer 'gold') -> COMMIT
-> raise kalau ada FAIL + ERROR (hasil terbaru per cek). Desain: gold_explained.md (GV1-GV6).
Posisi di DAG: build Gold -> validate Gold (dan record pipeline metadata).
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from decimal import Decimal

from shared.db import connect
from shared.ops import record_quality_check


@dataclass(frozen=True)
class Check:
    """1 cek = query (observed, details). Lolos kalau observed <= tolerance (GV6)."""

    name: str
    sql: str
    tolerance: Decimal = Decimal("0")


# ---------- 1. Definisi cek ----------

GOLD_KEYS = {                                                       # grain kontrak gold.sql
    "order_360": "order_id",
    "customer_daily": "(customer_id, metric_date)",
    "product_daily": "(product_id, metric_date)",
    "channel_campaign_daily": "(metric_date, sales_channel, campaign_id)",
    "executive_kpis_daily": "metric_date",
}

# GV2 - grain unik
UNIQUE_CHECKS = [
    Check(f"gold_unique_{t}",
          f"SELECT count(*) - count(DISTINCT {k}) AS observed, NULL::jsonb AS details FROM gold.{t}")
    for t, k in GOLD_KEYS.items()
]

# GV3 - tidak kosong (fail-closed versi Gold) + order_360 lengkap
NOT_EMPTY_CHECKS = [
    Check(f"gold_not_empty_{t}",
          f"SELECT CASE WHEN count(*) = 0 THEN 1 ELSE 0 END AS observed, "
          f"jsonb_build_object('rows', count(*)) AS details FROM gold.{t}")
    for t in GOLD_KEYS
]


def _recon(name: str, gold_expr: str, expected_expr: str, tolerance: str = "0") -> Check:
    """Rekonsiliasi: |angka Gold - angka pembanding|, kedua sisi dicatat di details."""
    return Check(
        name=name,
        sql=f"""
            SELECT abs(({gold_expr}) - ({expected_expr})) AS observed,
                   jsonb_build_object('gold', ({gold_expr}), 'expected', ({expected_expr})) AS details
        """,
        tolerance=Decimal(tolerance),
    )


O360_NET = "SELECT SUM(net_revenue) FROM gold.order_360"
O360_ORDERS = "SELECT count(*) FROM gold.order_360"

# GV3 + GV4 - rekonsiliasi (bukti tidak ada order hilang & tidak ada fact multiplication)
RECON_CHECKS = [
    _recon("gold_order_360_complete", O360_ORDERS, "SELECT count(*) FROM silver.orders"),
    _recon("gold_recon_net_customer_daily", "SELECT SUM(net_revenue) FROM gold.customer_daily", O360_NET),
    _recon("gold_recon_net_executive", "SELECT SUM(net_revenue) FROM gold.executive_kpis_daily", O360_NET),
    _recon("gold_recon_orders_customer_daily", "SELECT SUM(order_count) FROM gold.customer_daily", O360_ORDERS),
    _recon("gold_recon_orders_executive", "SELECT SUM(total_orders) FROM gold.executive_kpis_daily", O360_ORDERS),
    _recon("gold_recon_units_product_daily", "SELECT SUM(units_sold) FROM gold.product_daily",
           "SELECT SUM(unit_quantity) FROM gold.order_360"),
    _recon("gold_recon_gross_product_daily", "SELECT SUM(gross_revenue) FROM gold.product_daily",
           "SELECT SUM(gross_merchandise_value) FROM gold.order_360"),
    _recon("gold_recon_net_product_daily",                          # PD4: net order tanpa ongkir, order ber-item
           "SELECT SUM(net_revenue) FROM gold.product_daily",
           "SELECT SUM(net_revenue - shipping_revenue) FROM gold.order_360 WHERE item_count > 0",
           tolerance="1.00"),                                       # pembulatan alokasi per product-hari
    _recon("gold_recon_spend", "SELECT SUM(campaign_spend) FROM gold.channel_campaign_daily",
           "SELECT SUM(spend_amount) FROM silver.campaign_spend"),  # #15: spend tidak tergandakan
    _recon("gold_recon_attributed_orders", "SELECT SUM(attributed_orders) FROM gold.channel_campaign_daily",
           "SELECT count(DISTINCT order_id) FROM silver.web_events WHERE campaign_id IS NOT NULL"),
]

# GV5 - aturan bisnis kontrak
RULE_CHECKS = [
    Check("gold_rule_net_formula", """
        SELECT count(*) AS observed, NULL::jsonb AS details
        FROM gold.order_360
        WHERE net_revenue <> gross_merchandise_value - discount_amount + shipping_revenue - refunded_amount
    """),
    Check("gold_rule_first_order_once", """
        SELECT count(*) AS observed, NULL::jsonb AS details
        FROM (SELECT customer_id FROM gold.order_360
              GROUP BY customer_id HAVING SUM(first_order_flag::int) <> 1) x
    """),
    Check("gold_rule_rates_between_0_1", """
        SELECT (SELECT count(*) FROM gold.executive_kpis_daily
                WHERE refund_rate NOT BETWEEN 0 AND 1 OR return_rate NOT BETWEEN 0 AND 1
                   OR repeat_customer_rate NOT BETWEEN 0 AND 1 OR stockout_rate NOT BETWEEN 0 AND 1)
             + (SELECT count(*) FROM gold.channel_campaign_daily
                WHERE conversion_rate NOT BETWEEN 0 AND 1) AS observed,   -- NULL tidak dihitung
               NULL::jsonb AS details
    """),
]

ALL_CHECKS = UNIQUE_CHECKS + NOT_EMPTY_CHECKS + RECON_CHECKS + RULE_CHECKS


# ---------- 2. Ukur lalu nilai ----------

def validate_gold(conn, pipeline_run_id: str) -> list[tuple[str, object, bool]]:
    """Jalankan semua cek (1 transaksi, dicatat ke ops), lalu raise kalau ada FAIL."""
    results = []
    try:
        for check in ALL_CHECKS:
            observed, details = conn.execute(check.sql).fetchone()
            passed = observed is not None and observed <= check.tolerance
            record_quality_check(
                conn, pipeline_run_id, "gold", check.name,
                passed=passed, observed_value=observed,
                expected_value=f"<= {check.tolerance}", details=details or {},
            )
            results.append((check.name, observed, passed))
        conn.commit()  # bukti tersimpan dulu, baru dinilai
    except Exception:
        conn.rollback()
        raise

    failed = [r[0] for r in conn.execute(                           # Q9: hasil terbaru per cek
        """
        SELECT check_name FROM (
            SELECT DISTINCT ON (check_name) check_name, status, severity
            FROM ops.quality_checks
            WHERE pipeline_run_id = %s AND layer = 'gold'
            ORDER BY check_name, check_id DESC
        ) latest
        WHERE status = 'FAIL' AND severity = 'ERROR'
        ORDER BY check_name
        """,
        [pipeline_run_id],
    ).fetchall()]
    for name, observed, passed in results:
        print(f"{'PASS' if passed else 'FAIL':<5} {name:<40} observed={observed}")
    print(f"{len(results)} cek | run {pipeline_run_id}")
    if failed:
        raise RuntimeError(f"Validasi Gold FAILED untuk run {pipeline_run_id}: {failed}")
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate Gold.")
    parser.add_argument("--pipeline-run-id", required=True)  # run yang sama dengan build_gold
    args = parser.parse_args()

    conn = connect("local")
    try:
        validate_gold(conn, args.pipeline_run_id)
        print("VALIDASI GOLD PASS")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
