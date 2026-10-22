"""Bangun Gold dari Silver: tiap fakta diringkas ke grain target DULU, baru join (anti fan-out).

Kontrak tetap: database/schemas/gold.sql. Desain: gold_explained.md (G0-1..G0-10).
Pola sama dengan Silver: rebuild penuh dalam 1 transaksi (B7).
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

from shared.db import connect
from shared.ops import ensure_pipeline_run

# Semua tabel Gold, dikosongkan bersama di awal run.
ALL_GOLD_TABLES = [
    "order_360", "customer_daily", "product_daily", "channel_campaign_daily", "executive_kpis_daily",
]

# ---------- 1. Definisi tabel ----------

# order_360: 1 baris per order (G0-1). Tiap tabel anak diringkas per order di CTE sendiri.
ORDER_360_SQL = """
    WITH items AS (
        SELECT order_id,
               SUM(quantity * unit_price)  AS gmv,                 -- item valid saja (G0-9)
               SUM(item_discount_amount)   AS item_discount,
               count(*)                    AS item_count,
               SUM(quantity)               AS unit_quantity
        FROM silver.order_items
        GROUP BY order_id
    ),
    promos AS (
        SELECT order_id,
               SUM(discount_amount) AS promo_discount,
               count(*)             AS promotion_count
        FROM silver.order_promotions
        GROUP BY order_id
    ),
    payments AS (                                                   -- split payment dijumlah (P6a)
        SELECT order_id,
               SUM(amount) FILTER (WHERE event_type = 'PAYMENT_CAPTURED') AS captured,
               CASE WHEN bool_or(event_type = 'PAYMENT_CAPTURED')   THEN 'CAPTURED'   -- G0-6: prioritas,
                    WHEN bool_or(event_type = 'PAYMENT_FAILED')     THEN 'FAILED'     -- bukan event terakhir
                    WHEN bool_or(event_type = 'PAYMENT_AUTHORIZED') THEN 'AUTHORIZED'
               END AS payment_status
        FROM silver.payment_events
        GROUP BY order_id
    ),
    refunds AS (
        SELECT order_id, SUM(amount) AS refunded                    -- G0-4: COMPLETED saja (T2)
        FROM silver.refund_events
        WHERE event_type = 'REFUND_COMPLETED'
        GROUP BY order_id
    ),
    returns AS (                                                    -- G0-7: tahap tertinggi dari EVENT
        SELECT order_id,
               CASE max(CASE event_type WHEN 'RETURN_CLOSED'    THEN 3
                                        WHEN 'RETURN_RECEIVED'  THEN 2
                                        WHEN 'RETURN_REQUESTED' THEN 1 END)
                    WHEN 3 THEN 'CLOSED' WHEN 2 THEN 'RECEIVED' WHEN 1 THEN 'REQUESTED'
               END AS return_status
        FROM silver.return_events
        GROUP BY order_id
    ),
    firsts AS (                                                     -- G0-10: termasuk CANCELLED
        SELECT order_id,
               ROW_NUMBER() OVER (PARTITION BY customer_id
                                  ORDER BY ordered_at_utc, order_id) = 1 AS first_order_flag
        FROM silver.orders
    ),
    joined AS (                                                     -- semua sudah 1 baris/order -> join 1:1
        SELECT o.*,
               COALESCE(i.gmv, 0)                                   AS gmv,
               COALESCE(i.item_discount, 0) + COALESCE(p.promo_discount, 0) AS discount,  -- G0-5
               COALESCE(pay.captured, 0)                            AS captured,
               COALESCE(r.refunded, 0)                              AS refunded,
               COALESCE(i.item_count, 0)                            AS item_count,
               COALESCE(i.unit_quantity, 0)                         AS unit_quantity,
               COALESCE(pay.payment_status, 'NONE')                 AS payment_status,
               COALESCE(ret.return_status, 'NONE')                  AS return_status,
               COALESCE(p.promotion_count, 0)                       AS promotion_count,
               f.first_order_flag
        FROM silver.orders o
        LEFT JOIN items    i   USING (order_id)
        LEFT JOIN promos   p   USING (order_id)
        LEFT JOIN payments pay USING (order_id)
        LEFT JOIN refunds  r   USING (order_id)
        LEFT JOIN returns  ret USING (order_id)
        JOIN firsts        f   USING (order_id)
    )
    INSERT INTO gold.order_360 (
        order_id, customer_id, order_date, sales_channel, store_id,
        gross_merchandise_value, discount_amount, shipping_revenue,
        captured_payment_amount, refunded_amount, net_revenue,
        item_count, unit_quantity, order_status, payment_status, return_status,
        promotion_count, first_order_flag, pipeline_run_id
    )
    SELECT order_id, customer_id,
           (ordered_at_utc AT TIME ZONE 'UTC')::date,              -- G0-2: UTC, tidak bergantung TimeZone sesi
           sales_channel, store_id,
           gmv, discount, shipping_revenue,
           captured, refunded,
           gmv - discount + shipping_revenue - refunded,           -- rumus kontrak README §4 (G0-8)
           item_count, unit_quantity, status, payment_status, return_status,
           promotion_count, first_order_flag, %s
    FROM joined
"""

# customer_daily: 1 baris per (customer, tanggal) yang punya order ATAU kontak support (CD1).
# Angka order diringkas dari gold.order_360 (CD2) -> rumus revenue/refund tidak ditulis ulang.
CUSTOMER_DAILY_SQL = """
    WITH orders_day AS (                                            -- CD3, CD4, CD7
        SELECT customer_id, order_date AS metric_date,
               count(*)                                        AS order_count,
               SUM(unit_quantity)                              AS unit_quantity,
               SUM(gross_merchandise_value)                    AS gross_revenue,
               SUM(net_revenue)                                AS net_revenue,
               SUM(refunded_amount)                            AS refund_amount,
               count(*) FILTER (WHERE return_status <> 'NONE') AS return_count,
               bool_or(first_order_flag)                       AS new_customer_flag
        FROM gold.order_360
        GROUP BY customer_id, order_date
    ),
    last_channel AS (                                               -- CD6: channel order terakhir hari itu
        SELECT DISTINCT ON (g.customer_id, g.order_date)
               g.customer_id, g.order_date AS metric_date, g.sales_channel AS active_channel
        FROM gold.order_360 g
        JOIN silver.orders s USING (order_id)                       -- waktu order ada di Silver
        ORDER BY g.customer_id, g.order_date, s.ordered_at_utc DESC, g.order_id DESC
    ),
    first_date AS (                                                 -- CD8: tanggal order pertama
        SELECT customer_id, order_date AS first_order_date
        FROM gold.order_360
        WHERE first_order_flag
    ),
    support_day AS (                                                -- CD5: tiket baru, tanggal event UTC
        SELECT customer_id, (occurred_at_utc AT TIME ZONE 'UTC')::date AS metric_date,
               count(*) AS support_contacts
        FROM silver.support_events
        WHERE event_type = 'SUPPORT_TICKET_CREATED' AND customer_id IS NOT NULL
        GROUP BY customer_id, (occurred_at_utc AT TIME ZONE 'UTC')::date
    ),
    keys AS (                                                       -- CD1: union aktivitas (UNION = tanpa dobel)
        SELECT customer_id, metric_date FROM orders_day
        UNION
        SELECT customer_id, metric_date FROM support_day
    )
    INSERT INTO gold.customer_daily (
        customer_id, metric_date, order_count, unit_quantity, gross_revenue, net_revenue,
        refund_amount, return_count, support_contacts, active_channel,
        new_customer_flag, repeat_customer_flag, pipeline_run_id
    )
    SELECT k.customer_id, k.metric_date,
           COALESCE(od.order_count, 0), COALESCE(od.unit_quantity, 0),
           COALESCE(od.gross_revenue, 0), COALESCE(od.net_revenue, 0),
           COALESCE(od.refund_amount, 0), COALESCE(od.return_count, 0),
           COALESCE(sd.support_contacts, 0),
           lc.active_channel,                                       -- NULL untuk baris support saja
           COALESCE(od.new_customer_flag, false),
           COALESCE(od.order_count > 0 AND fd.first_order_date < k.metric_date, false),
           %s
    FROM keys k
    LEFT JOIN orders_day   od USING (customer_id, metric_date)
    LEFT JOIN support_day  sd USING (customer_id, metric_date)
    LEFT JOIN last_channel lc USING (customer_id, metric_date)
    LEFT JOIN first_date   fd USING (customer_id)
"""

# product_daily: 1 baris per (product, tanggal) (PD1). Diskon promo & refund level order
# dialokasikan ke item secara proporsional terhadap GMV item (PD4, PD5).
PRODUCT_DAILY_SQL = """
    WITH promos AS (
        SELECT order_id, SUM(discount_amount) AS promo_discount
        FROM silver.order_promotions
        GROUP BY order_id
    ),
    item_alloc AS (                                                 -- 1 baris per item, angka order ikut
        SELECT i.product_id, g.order_date AS metric_date, i.order_id, i.quantity,
               i.quantity * i.unit_price                                        AS item_gmv,
               i.item_discount_amount,
               (i.quantity * i.unit_price) / NULLIF(g.gross_merchandise_value, 0) AS gmv_share,
               COALESCE(p.promo_discount, 0)                                    AS order_promo,
               g.refunded_amount                                                AS order_refund,
               GREATEST(g.gross_merchandise_value - g.discount_amount
                        + g.shipping_revenue, 0)                                AS order_value
        FROM silver.order_items i
        JOIN gold.order_360 g USING (order_id)
        LEFT JOIN promos p USING (order_id)
    ),
    sales AS (                                                      -- PD3, PD4, PD5
        SELECT product_id, metric_date,
               SUM(quantity)                AS units_sold,
               count(DISTINCT order_id)     AS order_count,
               SUM(item_gmv)                AS gross_revenue,
               round(SUM(item_gmv - item_discount_amount
                         - COALESCE(gmv_share, 0) * order_promo                -- porsi diskon promo
                         - COALESCE(gmv_share, 0) * order_refund), 2)          -- porsi refund
                                            AS net_revenue,
               SUM(round(quantity * LEAST(order_refund / NULLIF(order_value, 0), 1)))::int
                                            AS refunded_units                  -- refund penuh -> semua unit
        FROM item_alloc
        GROUP BY product_id, metric_date
    ),
    promo_count AS (                                                -- PD8: CTE sendiri (anti fan-out)
        SELECT i.product_id, g.order_date AS metric_date,
               count(DISTINCT op.promotion_id) AS promotion_count
        FROM silver.order_items i
        JOIN gold.order_360 g             USING (order_id)
        JOIN silver.order_promotions op   USING (order_id)
        GROUP BY i.product_id, g.order_date
    ),
    inventory AS (                                                  -- PD6: Σ lokasi yang melapor
        SELECT product_id, snapshot_date AS metric_date,
               SUM(available_quantity) AS available_inventory,
               SUM(reserved_quantity)  AS reserved_inventory
        FROM silver.inventory_snapshots
        GROUP BY product_id, snapshot_date
    ),
    keys AS (                                                       -- PD1: kunci dulu, angka belakangan
        SELECT product_id, metric_date FROM inventory
        UNION
        SELECT product_id, metric_date FROM sales
    )
    INSERT INTO gold.product_daily (
        product_id, metric_date, active_category, units_sold, order_count,
        gross_revenue, net_revenue, refunded_units, available_inventory,
        reserved_inventory, stockout_flag, promotion_count, pipeline_run_id
    )
    SELECT k.product_id, k.metric_date,
           cat.category_name,                                       -- PD2
           COALESCE(s.units_sold, 0), COALESCE(s.order_count, 0),
           COALESCE(s.gross_revenue, 0), COALESCE(s.net_revenue, 0),
           COALESCE(s.refunded_units, 0),
           inv.available_inventory, inv.reserved_inventory,         -- tanpa snapshot -> NULL, bukan 0
           inv.available_inventory = 0,                             -- PD7 (NULL kalau inventory NULL)
           COALESCE(pc.promotion_count, 0),
           %s
    FROM keys k
    LEFT JOIN sales       s   USING (product_id, metric_date)
    LEFT JOIN inventory   inv USING (product_id, metric_date)
    LEFT JOIN promo_count pc  USING (product_id, metric_date)
    LEFT JOIN LATERAL (                                             -- PD2: versi berlaku di awal hari UTC
        SELECT c.category_name
        FROM silver.product_categories c
        WHERE c.product_id = k.product_id
          AND c.valid_from_utc <= (k.metric_date::timestamp AT TIME ZONE 'UTC')
          AND (c.valid_to_utc IS NULL OR (k.metric_date::timestamp AT TIME ZONE 'UTC') < c.valid_to_utc)
        ORDER BY c.valid_from_utc DESC
        LIMIT 1                                                     -- maks 1 versi -> tidak fan-out
    ) cat ON true
"""

# channel_campaign_daily: 1 baris per (tanggal, sales_channel, campaign) (CC2).
# Spend di-join pada grain-nya sendiri, TIDAK pernah ke level order (CC3, PARKIR #15).
CHANNEL_CAMPAIGN_DAILY_SQL = """
    WITH attribution AS (                                           -- CC1: campaign dari session order
        SELECT DISTINCT ON (order_id) order_id, campaign_id
        FROM silver.web_events
        WHERE campaign_id IS NOT NULL
        ORDER BY order_id, occurred_at_utc DESC, event_id           -- F10: 1 campaign/order; cadangan = last touch
    ),
    orders_attr AS (                                                -- CC4: dari order_360, order teratribusi saja
        SELECT g.order_date AS metric_date, g.sales_channel, a.campaign_id,
               count(*)                       AS attributed_orders,
               count(DISTINCT g.customer_id)  AS attributed_customers,
               SUM(g.gross_merchandise_value) AS gross_revenue,
               SUM(g.net_revenue)             AS net_revenue,
               SUM(g.refunded_amount)         AS refunds
        FROM gold.order_360 g
        JOIN attribution a USING (order_id)
        GROUP BY g.order_date, g.sales_channel, a.campaign_id
    ),
    sessions AS (                                                   -- CC6: session & purchase per baris
        SELECT g.order_date AS metric_date, g.sales_channel, a.campaign_id,
               count(DISTINCT w.session_id)                                          AS sessions,
               count(DISTINCT w.session_id) FILTER (WHERE w.event_type = 'PURCHASE') AS purchases
        FROM silver.web_events w
        JOIN attribution a  USING (order_id)
        JOIN gold.order_360 g USING (order_id)
        GROUP BY g.order_date, g.sales_channel, a.campaign_id
    ),
    spend AS (                                                      -- CC3: grain spend sendiri
        SELECT spend_date AS metric_date, channel AS sales_channel, campaign_id,
               SUM(spend_amount) AS campaign_spend
        FROM silver.campaign_spend
        GROUP BY spend_date, channel, campaign_id
    ),
    keys AS (                                                       -- CC2: order teratribusi ∪ spend
        SELECT metric_date, sales_channel, campaign_id FROM orders_attr
        UNION
        SELECT metric_date, sales_channel, campaign_id FROM spend
    )
    INSERT INTO gold.channel_campaign_daily (
        metric_date, sales_channel, campaign_id, campaign_spend, attributed_orders,
        attributed_customers, gross_revenue, net_revenue, refunds, roas, conversion_rate,
        pipeline_run_id
    )
    SELECT k.metric_date, k.sales_channel, k.campaign_id,
           COALESCE(sp.campaign_spend, 0),                          -- channel lain -> 0 (CC3)
           COALESCE(oa.attributed_orders, 0), COALESCE(oa.attributed_customers, 0),
           COALESCE(oa.gross_revenue, 0), COALESCE(oa.net_revenue, 0), COALESCE(oa.refunds, 0),
           round(COALESCE(oa.net_revenue, 0) / NULLIF(sp.campaign_spend, 0), 4),  -- CC5: tanpa spend -> NULL
           round(se.purchases::numeric / NULLIF(se.sessions, 0), 4),            -- CC6: tanpa session -> NULL
           %s
    FROM keys k
    LEFT JOIN orders_attr oa USING (metric_date, sales_channel, campaign_id)
    LEFT JOIN spend       sp USING (metric_date, sales_channel, campaign_id)
    LEFT JOIN sessions    se USING (metric_date, sales_channel, campaign_id)
"""

# executive_kpis_daily: 1 baris per tanggal bisnis (EK1), dirangkum dari tabel Gold lain
# -> angka eksekutif selalu konsisten dengan tabel detailnya.
EXECUTIVE_KPIS_DAILY_SQL = """
    WITH dates AS (                                                 -- EK1: periode bisnis (90 hari)
        SELECT order_date AS metric_date FROM gold.order_360
        UNION
        SELECT metric_date FROM gold.product_daily
    ),
    orders_day AS (                                                 -- EK2-EK5
        SELECT order_date AS metric_date,
               count(*)                                                      AS total_orders,
               SUM(gross_merchandise_value)                                  AS gross_revenue,
               SUM(net_revenue)                                              AS net_revenue,
               count(*) FILTER (WHERE payment_status = 'CAPTURED')           AS captured_orders,
               count(*) FILTER (WHERE payment_status = 'CAPTURED'
                                  AND refunded_amount > 0)                   AS refunded_captured,
               count(*) FILTER (WHERE return_status <> 'NONE')               AS returned_orders
        FROM gold.order_360
        GROUP BY order_date
    ),
    customers_day AS (                                              -- EK6, EK8, EK9
        SELECT metric_date,
               count(*) FILTER (WHERE order_count > 0)                       AS active_customers,
               AVG(repeat_customer_flag::int) FILTER (WHERE order_count > 0) AS repeat_rate,
               SUM(support_contacts)                                         AS support_contacts
        FROM gold.customer_daily
        GROUP BY metric_date
    ),
    stock_day AS (                                                  -- EK7: sama dengan docs/demo-queries.sql
        SELECT metric_date,
               AVG(stockout_flag::int) FILTER (WHERE stockout_flag IS NOT NULL) AS stockout_rate
        FROM gold.product_daily
        GROUP BY metric_date
    ),
    freshness AS (                                                  -- EK10: data paling baru yang masuk
        SELECT GREATEST(
                   (SELECT max(ingested_at_utc) FROM silver.payment_events),
                   (SELECT max(ingested_at_utc) FROM silver.refund_events),
                   (SELECT max(ingested_at_utc) FROM silver.return_events),
                   (SELECT max(ingested_at_utc) FROM silver.support_events),
                   (SELECT max(ingested_at_utc) FROM silver.web_events)
               ) AS data_freshness_utc
    )
    INSERT INTO gold.executive_kpis_daily (
        metric_date, total_orders, gross_revenue, net_revenue, average_order_value,
        refund_rate, return_rate, repeat_customer_rate, stockout_rate, active_customers,
        support_contact_rate, data_freshness_utc, pipeline_run_id
    )
    SELECT d.metric_date,
           COALESCE(o.total_orders, 0),
           COALESCE(o.gross_revenue, 0),
           COALESCE(o.net_revenue, 0),
           COALESCE(round(o.net_revenue / NULLIF(o.total_orders, 0), 2), 0),                    -- EK3
           COALESCE(round(o.refunded_captured::numeric / NULLIF(o.captured_orders, 0), 4), 0),  -- EK4
           COALESCE(round(o.returned_orders::numeric / NULLIF(o.total_orders, 0), 4), 0),       -- EK5
           COALESCE(round(c.repeat_rate, 4), 0),                                                -- EK6
           COALESCE(round(s.stockout_rate, 4), 0),                                              -- EK7
           COALESCE(c.active_customers, 0),                                                     -- EK8
           COALESCE(round(c.support_contacts::numeric / NULLIF(o.total_orders, 0), 4), 0),      -- EK9
           f.data_freshness_utc,
           %s
    FROM dates d
    LEFT JOIN orders_day    o USING (metric_date)
    LEFT JOIN customers_day c USING (metric_date)
    LEFT JOIN stock_day     s USING (metric_date)
    CROSS JOIN freshness    f                                       -- 1 baris -> nilai sama untuk semua tanggal
"""

# Urutan build: order_360 dulu -> tabel _daily yang diturunkan darinya (mirip B4 di Silver).
# executive_kpis_daily terakhir: merangkum 3 tabel Gold sebelumnya.
GOLD_STEPS = [
    ("order_360", ORDER_360_SQL),
    ("customer_daily", CUSTOMER_DAILY_SQL),
    ("product_daily", PRODUCT_DAILY_SQL),
    ("channel_campaign_daily", CHANNEL_CAMPAIGN_DAILY_SQL),
    ("executive_kpis_daily", EXECUTIVE_KPIS_DAILY_SQL),
]


# ---------- 2. Orkestrasi ----------

def build_gold(pipeline_run_id: str) -> dict[str, int]:
    summary: dict[str, int] = {}
    conn = connect("local")
    try:
        ensure_pipeline_run(conn, pipeline_run_id)
        conn.commit()  # catatan run disimpan duluan, di luar transaksi data (B7)

        try:
            conn.execute("TRUNCATE " + ", ".join(f"gold.{t}" for t in ALL_GOLD_TABLES))
            for name, sql in GOLD_STEPS:
                conn.execute(sql, [pipeline_run_id])
                summary[name] = conn.execute(f"SELECT count(*) FROM gold.{name}").fetchone()[0]
            conn.commit()
        except Exception as exc:
            conn.rollback()  # termasuk TRUNCATE -> Gold lama utuh
            raise RuntimeError("Gagal membangun Gold; Gold lama dipertahankan") from exc
    finally:
        conn.close()
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Gold from Silver.")
    parser.add_argument(
        "--pipeline-run-id",
        default=f"manual__{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}",
    )
    args = parser.parse_args()

    summary = build_gold(args.pipeline_run_id)
    for name, count in summary.items():
        print(f"{name:<24} {count:>7} baris")
    print(f"{len(summary)} tabel | run {args.pipeline_run_id}")


if __name__ == "__main__":
    main()
