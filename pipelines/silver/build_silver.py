"""Bangun Silver dari Bronze: cast -> dedup -> validasi, rebuild penuh dalam 1 transaksi.

Desain lengkap: silver_explained.md (B1-B10). Python = mandor, transformasi = SQL (B1).
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone

from shared.db import connect
from shared.ops import ensure_pipeline_run, record_quality_check

# Semua tabel Silver, dikosongkan bersama di awal run (B7 / D6).
ALL_SILVER_TABLES = [
    "rejected_records",
    "customers", "products", "product_categories",
    "orders", "order_items", "order_promotions",
    "payment_events", "refund_events", "return_events", "support_events", "web_events",
    "inventory_snapshots", "campaign_spend",
]


@dataclass(frozen=True)
class SilverTable:
    """1 tabel Silver = nama + kolom bisnis + SQL staging (cast -> dedup -> reject_reason)."""

    name: str              # nama tabel silver = source_name di Bronze
    columns: list[str]     # kolom bisnis (tanpa raw_record_id & pipeline_run_id)
    stage_sql: str         # CREATE TEMP TABLE stg_<name> ... dengan kolom rn + reject_reason


# ---------- 1. Definisi tabel, urutan = B4 ----------
# Pola stage_sql (B3): typed (CAST) -> ranked (DEDUP, rn) -> SELECT + reject_reason (VALIDASI).
# Nama kolom staging = reject_reason (bukan reason) supaya tidak bentrok dengan
# kolom bisnis support_events.reason (alasan tiket).

# --- Master: tanpa aturan reject (B5), reject_reason selalu NULL ---

CUSTOMERS = SilverTable(
    name="customers",
    columns=["customer_id", "city_id", "customer_segment", "created_at_utc"],
    stage_sql="""
        CREATE TEMP TABLE stg_customers ON COMMIT DROP AS
        WITH typed AS (
            SELECT raw_record_id,
                   payload->>'customer_id'                   AS customer_id,
                   payload->>'city_id'                       AS city_id,
                   payload->>'customer_segment'              AS customer_segment,
                   (payload->>'created_at_utc')::timestamptz AS created_at_utc
            FROM bronze.latest_raw_records
            WHERE source_name = 'customers'
        ), ranked AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id
                                         ORDER BY raw_record_id) AS rn
            FROM typed
        )
        SELECT *, NULL::text AS reject_reason
        FROM ranked
    """,
)

PRODUCTS = SilverTable(
    name="products",
    columns=["product_id", "product_name", "sku", "unit_price"],
    stage_sql="""
        CREATE TEMP TABLE stg_products ON COMMIT DROP AS
        WITH typed AS (
            SELECT raw_record_id,
                   payload->>'product_id'                  AS product_id,
                   payload->>'product_name'                AS product_name,
                   payload->>'sku'                         AS sku,
                   (payload->>'unit_price')::numeric(12,2) AS unit_price
            FROM bronze.latest_raw_records
            WHERE source_name = 'products'
        ), ranked AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id
                                         ORDER BY raw_record_id) AS rn
            FROM typed
        )
        SELECT *, NULL::text AS reject_reason
        FROM ranked
    """,
)

# Berversi (P7): 1 product bisa punya beberapa kategori sepanjang waktu -> key (product_id, valid_from)
PRODUCT_CATEGORIES = SilverTable(
    name="product_categories",
    columns=["product_id", "valid_from_utc", "valid_to_utc", "category_id", "category_name"],
    stage_sql="""
        CREATE TEMP TABLE stg_product_categories ON COMMIT DROP AS
        WITH typed AS (
            SELECT raw_record_id,
                   payload->>'product_id'                    AS product_id,
                   (payload->>'valid_from_utc')::timestamptz AS valid_from_utc,
                   (payload->>'valid_to_utc')::timestamptz   AS valid_to_utc,
                   payload->>'category_id'                   AS category_id,
                   payload->>'category_name'                 AS category_name
            FROM bronze.latest_raw_records
            WHERE source_name = 'product_categories'
        ), ranked AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, valid_from_utc
                                         ORDER BY raw_record_id) AS rn
            FROM typed
        )
        SELECT r.*,
               CASE WHEN p.product_id IS NULL                 THEN 'UNKNOWN_PRODUCT'
                    WHEN r.valid_to_utc <= r.valid_from_utc   THEN 'INVALID_VALIDITY_RANGE'
               END AS reject_reason
        FROM ranked r
        LEFT JOIN silver.products p ON p.product_id = r.product_id
    """,
)

# --- Transaksi ---
# Kolom kategori ber-CHECK -> upper(trim()) dulu (N1): benahi bentuk tulisan, bukan arti.
# Nilai yang artinya baru tetap ditolak CHECK -> loud (B5).

ORDERS = SilverTable(
    name="orders",
    columns=["order_id", "customer_id", "sales_channel", "store_id", "status",
             "shipping_revenue", "ordered_at_utc", "updated_at_utc"],
    stage_sql="""
        CREATE TEMP TABLE stg_orders ON COMMIT DROP AS
        WITH typed AS (
            SELECT raw_record_id,
                   payload->>'order_id'                         AS order_id,
                   payload->>'customer_id'                      AS customer_id,
                   upper(trim(payload->>'sales_channel'))       AS sales_channel,
                   payload->>'store_id'                         AS store_id,
                   upper(trim(payload->>'status'))              AS status,
                   (payload->>'shipping_revenue')::numeric(12,2) AS shipping_revenue,
                   (payload->>'ordered_at_utc')::timestamptz    AS ordered_at_utc,
                   (payload->>'updated_at_utc')::timestamptz    AS updated_at_utc
            FROM bronze.latest_raw_records
            WHERE source_name = 'orders'
        ), ranked AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id
                                         ORDER BY updated_at_utc DESC, raw_record_id) AS rn
            FROM typed
        )
        SELECT r.*,
               CASE WHEN c.customer_id IS NULL THEN 'UNKNOWN_CUSTOMER'
               END AS reject_reason
        FROM ranked r
        LEFT JOIN silver.customers c ON c.customer_id = r.customer_id
    """,
)

ORDER_ITEMS = SilverTable(
    name="order_items",
    columns=["order_item_id", "order_id", "product_id", "quantity", "unit_price",
             "item_discount_amount"],
    stage_sql="""
        CREATE TEMP TABLE stg_order_items ON COMMIT DROP AS
        WITH typed AS (
            SELECT raw_record_id,
                   payload->>'order_item_id'                         AS order_item_id,
                   payload->>'order_id'                              AS order_id,
                   payload->>'product_id'                            AS product_id,
                   (payload->>'quantity')::integer                   AS quantity,
                   (payload->>'unit_price')::numeric(12,2)           AS unit_price,
                   (payload->>'item_discount_amount')::numeric(12,2) AS item_discount_amount
            FROM bronze.latest_raw_records
            WHERE source_name = 'order_items'
        ), ranked AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY order_item_id
                                         ORDER BY raw_record_id) AS rn
            FROM typed
        )
        SELECT r.*,
               CASE WHEN o.order_id   IS NULL THEN 'UNKNOWN_ORDER'          -- referensi dulu (B5)
                    WHEN p.product_id IS NULL THEN 'UNKNOWN_PRODUCT'
                    WHEN r.quantity <= 0      THEN 'NON_POSITIVE_QUANTITY'  -- baru nilai
               END AS reject_reason
        FROM ranked r
        LEFT JOIN silver.orders   o ON o.order_id   = r.order_id
        LEFT JOIN silver.products p ON p.product_id = r.product_id
    """,
)

ORDER_PROMOTIONS = SilverTable(
    name="order_promotions",
    columns=["order_id", "promotion_id", "discount_amount"],
    stage_sql="""
        CREATE TEMP TABLE stg_order_promotions ON COMMIT DROP AS
        WITH typed AS (
            SELECT raw_record_id,
                   payload->>'order_id'                         AS order_id,
                   payload->>'promotion_id'                     AS promotion_id,
                   (payload->>'discount_amount')::numeric(12,2) AS discount_amount
            FROM bronze.latest_raw_records
            WHERE source_name = 'order_promotions'
        ), ranked AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, promotion_id
                                         ORDER BY raw_record_id) AS rn
            FROM typed
        )
        SELECT r.*,
               CASE WHEN o.order_id IS NULL THEN 'UNKNOWN_ORDER'   -- promotion_id belum dicek (B4)
               END AS reject_reason
        FROM ranked r
        LEFT JOIN silver.orders o ON o.order_id = r.order_id
    """,
)

# --- Event: grain 1 event, dedup per event_id (P2), waktu -> UTC (P3) ---
# Field bisnis ada di payload->'payload'. Dedup: ingested_at terbaru, seri -> raw_record_id.

PAYMENT_EVENTS = SilverTable(
    name="payment_events",
    columns=["event_id", "event_type", "order_id", "payment_id", "amount",
             "occurred_at_utc", "ingested_at_utc"],
    stage_sql="""
        CREATE TEMP TABLE stg_payment_events ON COMMIT DROP AS
        WITH typed AS (
            SELECT raw_record_id,
                   payload->>'event_id'                            AS event_id,
                   upper(trim(payload->>'event_type'))             AS event_type,
                   payload->'payload'->>'order_id'                 AS order_id,
                   payload->'payload'->>'payment_id'               AS payment_id,
                   (payload->'payload'->>'amount')::numeric(12,2)  AS amount,
                   (payload->>'occurred_at_utc')::timestamptz      AS occurred_at_utc,
                   (payload->>'ingested_at_utc')::timestamptz      AS ingested_at_utc
            FROM bronze.latest_raw_records
            WHERE source_name = 'payment_events'
        ), ranked AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id
                                         ORDER BY ingested_at_utc DESC, raw_record_id) AS rn
            FROM typed
        )
        SELECT r.*,
               CASE WHEN o.order_id IS NULL THEN 'UNKNOWN_ORDER'
               END AS reject_reason
        FROM ranked r
        LEFT JOIN silver.orders o ON o.order_id = r.order_id
    """,
)

REFUND_EVENTS = SilverTable(
    name="refund_events",
    columns=["event_id", "event_type", "order_id", "refund_id", "amount",
             "occurred_at_utc", "ingested_at_utc"],
    stage_sql="""
        CREATE TEMP TABLE stg_refund_events ON COMMIT DROP AS
        WITH typed AS (
            SELECT raw_record_id,
                   payload->>'event_id'                            AS event_id,
                   upper(trim(payload->>'event_type'))             AS event_type,
                   payload->'payload'->>'order_id'                 AS order_id,
                   payload->'payload'->>'refund_id'                AS refund_id,
                   (payload->'payload'->>'amount')::numeric(12,2)  AS amount,
                   (payload->>'occurred_at_utc')::timestamptz      AS occurred_at_utc,
                   (payload->>'ingested_at_utc')::timestamptz      AS ingested_at_utc
            FROM bronze.latest_raw_records
            WHERE source_name = 'refund_events'
        ), ranked AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id
                                         ORDER BY ingested_at_utc DESC, raw_record_id) AS rn
            FROM typed
        )
        SELECT r.*,
               CASE WHEN o.order_id IS NULL THEN 'UNKNOWN_ORDER'
               END AS reject_reason
        FROM ranked r
        LEFT JOIN silver.orders o ON o.order_id = r.order_id
    """,
)

RETURN_EVENTS = SilverTable(
    name="return_events",
    columns=["event_id", "event_type", "order_id", "return_id",
             "occurred_at_utc", "ingested_at_utc"],
    stage_sql="""
        CREATE TEMP TABLE stg_return_events ON COMMIT DROP AS
        WITH typed AS (
            SELECT raw_record_id,
                   payload->>'event_id'                        AS event_id,
                   upper(trim(payload->>'event_type'))         AS event_type,
                   payload->'payload'->>'order_id'             AS order_id,
                   payload->'payload'->>'return_id'            AS return_id,
                   (payload->>'occurred_at_utc')::timestamptz  AS occurred_at_utc,
                   (payload->>'ingested_at_utc')::timestamptz  AS ingested_at_utc
            FROM bronze.latest_raw_records
            WHERE source_name = 'return_events'
        ), ranked AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id
                                         ORDER BY ingested_at_utc DESC, raw_record_id) AS rn
            FROM typed
        )
        SELECT r.*,
               CASE WHEN o.order_id IS NULL THEN 'UNKNOWN_ORDER'
               END AS reject_reason
        FROM ranked r
        LEFT JOIN silver.orders o ON o.order_id = r.order_id
    """,
)

# support & web: order_id boleh kosong -> dicek hanya kalau terisi (B4). Kosong ≠ rusak.
SUPPORT_EVENTS = SilverTable(
    name="support_events",
    columns=["event_id", "event_type", "order_id", "customer_id", "ticket_id", "reason",
             "occurred_at_utc", "ingested_at_utc"],
    stage_sql="""
        CREATE TEMP TABLE stg_support_events ON COMMIT DROP AS
        WITH typed AS (
            SELECT raw_record_id,
                   payload->>'event_id'                        AS event_id,
                   upper(trim(payload->>'event_type'))         AS event_type,
                   payload->'payload'->>'order_id'             AS order_id,
                   payload->'payload'->>'customer_id'          AS customer_id,
                   payload->'payload'->>'ticket_id'            AS ticket_id,
                   payload->'payload'->>'reason'               AS reason,   -- alasan tiket (bisnis)
                   (payload->>'occurred_at_utc')::timestamptz  AS occurred_at_utc,
                   (payload->>'ingested_at_utc')::timestamptz  AS ingested_at_utc
            FROM bronze.latest_raw_records
            WHERE source_name = 'support_events'
        ), ranked AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id
                                         ORDER BY ingested_at_utc DESC, raw_record_id) AS rn
            FROM typed
        )
        SELECT r.*,
               CASE WHEN r.order_id IS NOT NULL AND o.order_id IS NULL THEN 'UNKNOWN_ORDER'
               END AS reject_reason
        FROM ranked r
        LEFT JOIN silver.orders o ON o.order_id = r.order_id
    """,
)

WEB_EVENTS = SilverTable(
    name="web_events",
    columns=["event_id", "event_type", "session_id", "customer_id", "order_id", "channel",
             "campaign_id", "occurred_at_utc", "ingested_at_utc"],
    stage_sql="""
        CREATE TEMP TABLE stg_web_events ON COMMIT DROP AS
        WITH typed AS (
            SELECT raw_record_id,
                   payload->>'event_id'                         AS event_id,
                   upper(trim(payload->>'event_type'))          AS event_type,
                   payload->'payload'->>'session_id'            AS session_id,
                   payload->'payload'->>'customer_id'           AS customer_id,
                   payload->'payload'->>'order_id'              AS order_id,
                   upper(trim(payload->'payload'->>'channel'))  AS channel,
                   payload->'payload'->>'campaign_id'           AS campaign_id,  -- NULL = tanpa campaign
                   (payload->>'occurred_at_utc')::timestamptz   AS occurred_at_utc,
                   (payload->>'ingested_at_utc')::timestamptz   AS ingested_at_utc
            FROM bronze.latest_raw_records
            WHERE source_name = 'web_events'
        ), ranked AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id
                                         ORDER BY ingested_at_utc DESC, raw_record_id) AS rn
            FROM typed
        )
        SELECT r.*,
               CASE WHEN r.order_id IS NOT NULL AND o.order_id IS NULL THEN 'UNKNOWN_ORDER'
               END AS reject_reason
        FROM ranked r
        LEFT JOIN silver.orders o ON o.order_id = r.order_id
    """,
)

# --- Snapshot & referensi dari CSV: semua nilai teks di Bronze -> di-cast (P10) ---

INVENTORY_SNAPSHOTS = SilverTable(
    name="inventory_snapshots",
    columns=["product_id", "location_id", "snapshot_date", "available_quantity",
             "reserved_quantity", "unit_cost"],
    stage_sql="""
        CREATE TEMP TABLE stg_inventory_snapshots ON COMMIT DROP AS
        WITH typed AS (
            SELECT raw_record_id,
                   payload->>'product_id'                    AS product_id,
                   payload->>'location_id'                   AS location_id,   -- vs stores: Prioritas 2
                   (payload->>'snapshot_date')::date         AS snapshot_date,
                   (payload->>'available_quantity')::integer AS available_quantity,
                   (payload->>'reserved_quantity')::integer  AS reserved_quantity,
                   (payload->>'unit_cost')::numeric(12,2)    AS unit_cost
            FROM bronze.latest_raw_records
            WHERE source_name = 'inventory_snapshots'
        ), ranked AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, location_id, snapshot_date
                                         ORDER BY raw_record_id) AS rn
            FROM typed
        )
        SELECT r.*,
               CASE WHEN p.product_id IS NULL THEN 'UNKNOWN_PRODUCT'
               END AS reject_reason
        FROM ranked r
        LEFT JOIN silver.products p ON p.product_id = r.product_id
    """,
)

CAMPAIGN_SPEND = SilverTable(
    name="campaign_spend",
    columns=["spend_date", "campaign_id", "channel", "spend_amount"],
    stage_sql="""
        CREATE TEMP TABLE stg_campaign_spend ON COMMIT DROP AS
        WITH typed AS (
            SELECT raw_record_id,
                   (payload->>'spend_date')::date             AS spend_date,
                   payload->>'campaign_id'                    AS campaign_id,
                   upper(trim(payload->>'channel'))           AS channel,
                   (payload->>'spend_amount')::numeric(12,2)  AS spend_amount
            FROM bronze.latest_raw_records
            WHERE source_name = 'campaign_spend'
        ), ranked AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY spend_date, campaign_id   -- D10
                                         ORDER BY raw_record_id) AS rn
            FROM typed
        )
        SELECT *, NULL::text AS reject_reason   -- tidak ada master campaign (B5)
        FROM ranked
    """,
)

TABLES: list[SilverTable] = [
    CUSTOMERS, PRODUCTS, PRODUCT_CATEGORIES,                        # 1. master
    ORDERS,                                                         # 2. cek customer
    ORDER_ITEMS, ORDER_PROMOTIONS,                                  # 3. anak orders
    PAYMENT_EVENTS, REFUND_EVENTS, RETURN_EVENTS, SUPPORT_EVENTS, WEB_EVENTS,
    INVENTORY_SNAPSHOTS, CAMPAIGN_SPEND,
]


# ---------- 2. Mesin generik: berlaku sama untuk semua tabel ----------

def build_table(conn, table: SilverTable, pipeline_run_id: str) -> dict[str, int]:
    """Staging -> Silver (lolos) / rejected (gagal) -> rekonsiliasi. Return 4 angka B8."""
    stg = f"stg_{table.name}"  # nama dari konstanta di kode, bukan input pengguna
    cols = ", ".join(table.columns)

    conn.execute(table.stage_sql)                                   # B3: cast -> dedup -> reject_reason

    conn.execute(                                                   # rn = 1 & lolos -> Silver
        f"""
        INSERT INTO silver.{table.name} ({cols}, raw_record_id, pipeline_run_id)
        SELECT {cols}, raw_record_id, %s
        FROM {stg}
        WHERE rn = 1 AND reject_reason IS NULL
        """,
        [pipeline_run_id],
    )
    conn.execute(                                                   # rn = 1 & gagal -> rejected
        f"""
        INSERT INTO silver.rejected_records
            (source_name, raw_record_id, reason, payload, pipeline_run_id)
        SELECT %s, s.raw_record_id, s.reject_reason, b.payload, %s
        FROM {stg} s
        JOIN bronze.raw_records b USING (raw_record_id)
        WHERE s.rn = 1 AND s.reject_reason IS NOT NULL
        """,
        [table.name, pipeline_run_id],
    )
    return reconcile(conn, table, pipeline_run_id)                  # rn > 1 = dedup, cukup dihitung


def reconcile(conn, table: SilverTable, pipeline_run_id: str) -> dict[str, int]:
    """B8: 4 angka dihitung TERPISAH, lalu dicatat ke ops.quality_checks."""
    counts = {
        "bronze": conn.execute(
            "SELECT count(*) FROM bronze.latest_raw_records WHERE source_name = %s",
            [table.name],
        ).fetchone()[0],
        "silver": conn.execute(f"SELECT count(*) FROM silver.{table.name}").fetchone()[0],
        "dedup": conn.execute(f"SELECT count(*) FROM stg_{table.name} WHERE rn > 1").fetchone()[0],
        "rejected": conn.execute(
            "SELECT count(*) FROM silver.rejected_records WHERE source_name = %s",
            [table.name],
        ).fetchone()[0],
    }
    diff = counts["bronze"] - (counts["silver"] + counts["dedup"] + counts["rejected"])
    record_quality_check(
        conn, pipeline_run_id, "silver", f"silver_reconciliation_{table.name}",
        passed=(diff == 0), observed_value=diff, expected_value="= 0", details=counts,
    )
    return counts


# ---------- 3. Orkestrasi ----------

def build_silver(pipeline_run_id: str) -> dict[str, dict[str, int]]:
    summary: dict[str, dict[str, int]] = {}
    conn = connect("local")
    try:
        ensure_pipeline_run(conn, pipeline_run_id)
        conn.commit()  # catatan run disimpan DULUAN, di luar transaksi data (B7)

        try:
            # --- mulai 1 transaksi data: semua atau tidak sama sekali (B7) ---
            conn.execute("TRUNCATE " + ", ".join(f"silver.{t}" for t in ALL_SILVER_TABLES))
            for table in TABLES:  # urutan B4: master -> orders -> anak
                summary[table.name] = build_table(conn, table, pipeline_run_id)
            conn.commit()  # baru di sini Silver baru terlihat; temp table staging ikut terhapus
        except Exception as exc:
            conn.rollback()  # termasuk TRUNCATE -> Silver lama utuh
            raise RuntimeError("Gagal membangun Silver; Silver lama dipertahankan") from exc
    finally:
        conn.close()
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Silver from Bronze.")
    parser.add_argument(
        "--pipeline-run-id",
        default=f"manual__{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}",
    )
    args = parser.parse_args()

    summary = build_silver(args.pipeline_run_id)
    print(f"{'tabel':<22} {'bronze':>7} {'silver':>7} {'dedup':>6} {'rejected':>8}  cek")
    for name, c in summary.items():
        ok = c["bronze"] == c["silver"] + c["dedup"] + c["rejected"]
        print(f"{name:<22} {c['bronze']:>7} {c['silver']:>7} {c['dedup']:>6} {c['rejected']:>8}  "
              f"{'PASS' if ok else 'FAIL'}")
    print(f"{len(summary)} tabel | run {args.pipeline_run_id}")


if __name__ == "__main__":
    main()
