# Sample Engine Queries — Gold lokal

Pertanyaan dijalankan lewat UI Streamlit (`ui/app.py`) → engine (`engine.api.main`)
→ PostgreSQL lokal hasil DAG `omnichannel_retail_pipeline`.

| Setting | Nilai |
|---|---|
| `ANALYTICS_DB_TARGET` | `local` (Gold hasil pipeline ini, bukan sample Neon) |
| `NL2SQL_PROVIDER` | `mock` (SQL benchmark bawaan engine) |
| Benchmark | `uv run python -m engine.benchmarks.run_benchmark` → **25/25 PASS** |

Setiap jawaban engine dicek silang ke angka lain di Gold/Silver. Tujuannya
membuktikan angka yang dibaca engine konsisten antar tabel, bukan sekadar
"query-nya jalan".

---

## Q1 — What is total net revenue by sales channel?

**Tabel:** `gold.order_360`

```sql
SELECT sales_channel, SUM(net_revenue) AS net_revenue
FROM gold.order_360 GROUP BY sales_channel ORDER BY net_revenue DESC
```

| sales_channel | net_revenue |
|---|---:|
| WEB | 3,440,808.22 |
| STORE | 3,353,874.46 |
| MOBILE_APP | 3,340,813.39 |
| MARKETPLACE | 3,294,328.76 |

**Cek silang:** Σ `order_360.net_revenue` = **13,429,824.83** = Σ
`executive_kpis_daily.net_revenue` = **13,429,824.83**. Grain order dan grain
harian memberi total yang sama → tidak ada order hilang/dobel di agregasi harian.

---

## Q2 — Which campaigns had the best ROAS?

**Tabel:** `gold.channel_campaign_daily`

```sql
SELECT campaign_id, SUM(net_revenue) / NULLIF(SUM(campaign_spend), 0) AS roas
FROM gold.channel_campaign_daily GROUP BY campaign_id
HAVING SUM(campaign_spend) > 0 ORDER BY roas DESC
```

| campaign_id | roas |
|---|---:|
| CMP-04 | 46.54 |
| CMP-05 | 45.89 |
| CMP-02 | 45.37 |
| … | … |
| CMP-03 | 40.01 |
| CMP-10 | 39.27 |

10 campaign, semua punya spend.

**Cek silang:** Σ `campaign_spend` di Gold = **231,441.30** = Σ
`silver.campaign_spend.spend_amount` = **231,441.30**. Spend tidak tergandakan
walaupun satu tanggal-campaign punya banyak order (spend diringkas ke grain
tanggal × campaign lebih dulu, baru di-join — keputusan CC3). Penyebut ROAS
benar → ranking bisa dipercaya.

---

## Q3 — How many customers were active each day?

**Tabel:** `gold.customer_daily`

```sql
SELECT metric_date, COUNT(DISTINCT customer_id) AS active_customers
FROM gold.customer_daily WHERE order_count > 0
GROUP BY metric_date ORDER BY metric_date
```

90 baris (22 Jun – 19 Sep 2026). Contoh: 2026-06-22 = 121, 2026-06-23 = 101,
2026-06-24 = 108.

**Cek silang:** dibandingkan per tanggal dengan
`executive_kpis_daily.active_customers` → **0 dari 90 hari berbeda**.
`customer_daily` juga berisi baris tanpa order (customer yang hari itu hanya
menghubungi support; refund/return ikut tanggal order, jadi tidak menambah
baris), tapi filter `order_count > 0` membuatnya tidak ikut terhitung aktif
(keputusan CD1).

---

## Q4 — What are the top products by units sold?

**Tabel:** `gold.product_daily`

```sql
SELECT product_id, SUM(units_sold) AS units_sold
FROM gold.product_daily GROUP BY product_id ORDER BY units_sold DESC
```

| product_id | units_sold |
|---|---:|
| PROD-00056 | 906 |
| PROD-00014 | 905 |
| PROD-00024 | 877 |
| PROD-00008 | 865 |
| PROD-00001 | 860 |

80 product.

**Cek silang:** Σ `product_daily.units_sold` = Σ `order_360.unit_quantity` =
Σ `silver.order_items.quantity` = **62,379**. Tiga jalur, angka sama. Item
ditolak (qty negatif, product tidak dikenal) sudah keluar di Silver, jadi tidak
ikut di mana pun.

---

## Q5 — What was the daily refund rate?

**Tabel:** `gold.executive_kpis_daily`

```sql
SELECT metric_date, refund_rate FROM gold.executive_kpis_daily ORDER BY metric_date
```

90 baris, rentang **0.0991 – 0.2874**, rata-rata harian 0.1975.

**Cek silang (definisi kontrak):** order `CAPTURED` dengan refund selesai /
order `CAPTURED` = 1,737 / 8,785 = **0.1977** untuk seluruh periode. Catatan:
rata-rata rate harian (0.1975) ≠ rate keseluruhan (0.1977) — rata-rata dari
rasio bukan rasio dari total. Untuk rate keseluruhan, hitung ulang dari
pembilang/penyebut, jangan `AVG(refund_rate)`.

---

## Q6 — What was the daily stockout rate?

**Tabel:** `gold.executive_kpis_daily`

```sql
SELECT metric_date, stockout_rate FROM gold.executive_kpis_daily ORDER BY metric_date
```

90 baris, **semua 0.0000**.

**Cek silang & penjelasan:** `product_daily.stockout_flag` = TRUE di 0 dari
7,200 product-hari, dan tidak ada inventory NULL. Stockout didefinisikan
sebagai **total stok tersedia product di semua lokasi = 0** (keputusan PD7).
Di level lokasi sebenarnya ada **424 baris** stok 0 (**416 product-hari**), tapi
selalu ada lokasi lain yang masih punya stok. Ini **limitation definisi**, bukan
bug: kalau bisnis ingin stockout per toko, grain `product_daily` (tanpa lokasi)
tidak bisa menampungnya.

---

## Q7 — Which channels had the highest refund rate? (temuan)

**Tabel:** `gold.order_360`

```sql
SELECT sales_channel, AVG(CASE WHEN refunded_amount > 0 THEN 1.0 ELSE 0.0 END) AS refund_rate
FROM gold.order_360 GROUP BY sales_channel ORDER BY refund_rate DESC
```

SQL ini menghitung refund rate atas **semua order**. Kontrak (README §4)
mendefinisikan refund rate sebagai order dengan refund selesai dibanding order
yang **payment-nya captured**. Hasil keduanya:

| sales_channel | Jawaban engine (semua order) | Definisi kontrak (CAPTURED) |
|---|---:|---:|
| WEB | **0.2042** (#1) | 0.2010 (#2) |
| MOBILE_APP | 0.2005 (#2) | **0.2033** (#1) |
| MARKETPLACE | 0.1970 | 0.1958 |
| STORE | 0.1947 | 0.1909 |
| **Total** | 0.1991 | 0.1977 (= Q5) |

**Ranking teratas tertukar.** Penyebabnya order `CANCELLED` (payment `FAILED`,
tidak pernah di-capture): **1,215 order** ikut di penyebut versi engine, dan
**254** di antaranya tetap punya refund `COMPLETED` sehingga ikut di
pembilang. Definisi kontrak mengecualikan keduanya (1,991 / 10,000 vs
1,737 / 8,785). Anomali ini sudah ditandai quality gate sebagai WARN
(`silver_refund_*`, 254 baris) dan perlakuannya didokumentasikan (keputusan
G0-8).

**Pelajaran:** Gold yang benar belum menjamin jawaban yang benar. SQL engine
yang tampak masuk akal bisa memakai definisi metric yang berbeda dari kontrak.
Untuk refund rate, sumber yang konsisten dengan kontrak adalah
`executive_kpis_daily.refund_rate`.

---

## Temuan tambahan — review 25 SQL benchmark terhadap kontrak

`run_benchmark.py` menandai PASS kalau query **jalan tanpa error**, bukan kalau
jawabannya benar. Karena itu ke-25 SQL mock (`engine/llm/providers.py`)
dibaca satu per satu terhadap definisi metric di README §4 dan
`engine/llm/catalog.py`, lalu setiap kecurigaan dibuktikan dengan query ke Gold
lokal. Penomoran di bawah memakai ID benchmark (`questions.json`); Q7 di atas =
benchmark **Q02**.

| Benchmark | Status |
|---|---|
| Q02 refund rate per channel | Menyimpang (lihat Q7 di atas) |
| Q08, Q15, Q18 | Menyimpang (di bawah) |
| 21 lainnya | Sesuai definisi |

### Q08 — What is the repeat customer rate by day?

```sql
SELECT metric_date, AVG(CASE WHEN repeat_customer_flag THEN 1.0 ELSE 0.0 END) AS repeat_customer_rate
FROM gold.customer_daily GROUP BY metric_date ORDER BY metric_date
```

Penjelasan engine sendiri: *"calculated over active customer-day rows"*, tetapi
SQL-nya tidak memfilter `order_count > 0`. `customer_daily` juga berisi baris
customer yang hari itu **hanya menghubungi support** (keputusan CD1); flag
repeat-nya pasti `false`, jadi baris ini menarik rate ke bawah.

| Cek | Hasil |
|---|---:|
| Baris `customer_daily` tanpa order | **1,550** |
| Hari yang dikembalikan engine | 102 |
| … di luar periode order (tiket support setelah 19 Sep, rate 0) | **12** |
| … dalam periode, angkanya ≠ `executive_kpis_daily.repeat_customer_rate` | **88 dari 90** |

Dengan filter `order_count > 0`, hasilnya sama dengan
`executive_kpis_daily.repeat_customer_rate` di **ke-90 hari** (0 hari berbeda)
→ selisihnya murni berasal dari filter yang hilang. Katalog engine juga menyebut metric harian sebaiknya diambil dari
`executive_kpis_daily` bila tersedia.

### Q18 — Which categories had the highest refund amount?

```sql
SELECT active_category, SUM(gross_revenue - net_revenue) AS refund_amount
FROM gold.product_daily GROUP BY active_category ORDER BY refund_amount DESC
```

Menurut alokasi `product_daily` (keputusan PD4), `gross − net` = diskon item +
porsi diskon promo + porsi refund. Jadi kolom bernama `refund_amount` sebenarnya
**refund + diskon**:

| Komponen | Nilai |
|---|---:|
| Σ `gross − net` (`product_daily`, versi engine) | 1,715,240.24 |
| Σ `refunded_amount` (`order_360`, refund sebenarnya) | 1,549,240.21 |
| Σ `discount_amount` (`order_360`) | 166,015.00 |
| refund + diskon − versi engine | **14.97** |

Selisih 14.97 = promo `ORD-000011` (15.00, order tanpa item valid sehingga tidak
teralokasi ke product) − 0.03 pembulatan, angka yang sama dengan rekonsiliasi
`product_daily` di validasi Gold. "Refund" versi engine **10.7% lebih tinggi**
dari refund sebenarnya.

Akar masalahnya ada di kontrak: `product_daily` hanya menyimpan
`refunded_units`, tidak menyimpan nilai refund, dan `order_360` tidak punya
kategori. Nilai refund per kategori **tidak bisa dijawab tepat dari Gold**;
yang tersedia adalah `SUM(refunded_units)` per kategori (satuan unit).
Apakah ranking kategorinya ikut berubah **belum dibuktikan** (butuh nilai
refund per kategori dari Silver).

### Q15 — Which orders were fully refunded?

```sql
SELECT order_id, refunded_amount, net_revenue FROM gold.order_360
WHERE refunded_amount > 0 AND net_revenue <= 0 ORDER BY order_id
```

Logika `net_revenue <= 0` setara dengan "refund ≥ pendapatan sebelum refund",
dan untuk order yang sudah dibayar hasilnya **benar**. Masalahnya, order yang
tidak pernah dibayar tidak dikecualikan:

| Cek | Jumlah |
|---|---:|
| Jawaban engine | 527 |
| … payment `CAPTURED` (refund penuh yang sah) | **458** |
| … payment bukan `CAPTURED` (order `CANCELLED`, uang masuk 0) | **69** |

Contoh: `ORD-000336` — `CANCELLED`, payment `FAILED`, captured 0.00, refund
2,146.66, net 0.00. 13% jawaban engine adalah anomali refund-tanpa-capture
(WARN di quality gate, keputusan G0-8) yang tampil seolah refund normal.
Selain itu executor membungkus query dengan `LIMIT 100`, sehingga UI hanya
menampilkan 100 dari 527 order tanpa peringatan.

Versi yang sesuai kontrak:

```sql
SELECT order_id, captured_payment_amount, refunded_amount FROM gold.order_360
WHERE payment_status = 'CAPTURED' AND refunded_amount > 0
  AND refunded_amount >= captured_payment_amount
ORDER BY order_id
```

(Syarat `refunded_amount > 0` wajib: ada 9 order `CAPTURED` dengan uang ditarik
0 dan tanpa refund yang lolos `0 >= 0` kalau syarat itu tidak ada.)

**Kesimpulan:** Gold lolos 23/23 validasi, tetapi 4 dari 25 SQL benchmark
menghasilkan jawaban yang tampak masuk akal namun menyimpang dari definisi
kontrak — persis risiko yang disebut README §1. Verifikasi tidak berhenti di
Gold; jawaban di sisi consumer juga harus dicek silang.