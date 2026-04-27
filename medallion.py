# ============================================================
# Konfigurasi awal: buat database DuckDB baru dan sambungkan
# ============================================================
import duckdb
import os

if os.path.exists("superstore.duckdb"):
    os.remove("superstore.duckdb")

con = duckdb.connect("superstore.duckdb")

print("=" * 60)
print("  MEDALLION ARCHITECTURE — SUPERSTORE DATASET")
print("  Bronze → Silver → Gold (Dim + Fact)")
print("=" * 60)

# ============================================================
# Helper: fungsi SQL untuk membersihkan angka format EU
# Contoh: '9.575.775' → 9575.775
# ============================================================
CLEAN_NUM = """
    CASE
        WHEN LENGTH({col}) - LENGTH(REPLACE({col}, '.', '')) > 1
        THEN CAST(
            REPLACE(
                LEFT({col}, LENGTH({col}) - LENGTH(SPLIT_PART({col}, '.', -1)) - 1),
                '.', ''
            ) || '.' || SPLIT_PART({col}, '.', -1)
            AS DOUBLE
        )
        WHEN {col} IS NOT NULL AND {col} != ''
        THEN CAST({col} AS DOUBLE)
        ELSE NULL
    END
"""

def cnum(col_name):
    return CLEAN_NUM.format(col='"' + col_name + '"')


# ============================================================
# BRONZE LAYER — Raw Data (data mentah, tidak diubah)
# EXTRACT — Membaca data mentah dari sumber (CSV)
# ============================================================
print("\n[1/3] BRONZE LAYER — Memuat data mentah dari CSV")

raw_df = con.execute("""
    SELECT *
    FROM read_csv_auto(
        'Superstore.csv',
        delim   = ';',
        header  = true
    )
""").fetchdf()

print(f"   ✓ Data berhasil diekstrak ({len(raw_df)} baris)")


# ============================================================
# VALIDATE — Memastikan data tidak kosong sebelum dilanjutkan
# ============================================================
assert len(raw_df) > 0, "❌ Data kosong! Periksa file CSV sumber."

print("   ✓ Validasi passed: data tidak kosong")

print("\n   Contoh 3 baris pertama:")
print(raw_df[["Order ID", "Order Date", "Customer Name",
              "Category", "Sales", "Profit"]].head(3).to_string())


# ============================================================
# LOAD — Menyimpan data mentah ke tabel bronze.raw_orders
# ============================================================
con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")

con.execute("""
    CREATE OR REPLACE TABLE bronze.raw_orders AS
    SELECT * FROM raw_df
""")

row_count = con.execute("SELECT COUNT(*) FROM bronze.raw_orders").fetchone()[0]
print(f"\n   ✓ Tabel bronze.raw_orders berhasil dibuat ({row_count} baris)")



# ============================================================
# SILVER LAYER — Cleaned & Standardized
# ============================================================
print("\n\n[2/3] SILVER LAYER — Membersihkan dan standarisasi data")

# ============================================================
# Ekspor ke Silver Storage — Membuat schema silver
# ============================================================
con.execute("CREATE SCHEMA IF NOT EXISTS silver;")

# ============================================================
# Load Data dari Bronze Layer
# ============================================================
load_cols = """
    "Row ID"        AS row_id,
    "Order ID"      AS order_id,
    "Order Date"    AS order_date,
    "Ship Date"     AS ship_date,
    "Postal Code"   AS postal_code,
    "Product ID"    AS product_id,
    "Customer ID"   AS customer_id
"""

# ============================================================
# Data Cleaning — TRIM spasi pada kolom teks
# ============================================================
clean_cols = """
    TRIM("Ship Mode")       AS ship_mode,
    TRIM("Customer Name")   AS customer_name,
    TRIM("Segment")         AS segment,
    TRIM("Country")         AS country,
    TRIM("City")            AS city,
    TRIM("State")           AS state,
    TRIM("Region")          AS region,
    TRIM("Category")        AS category,
    TRIM("Sub-Category")    AS sub_category,
    TRIM("Product Name")    AS product_name
"""

# ============================================================
# Transformasi Tipe Data — Konversi angka & tipe kolom
# ============================================================
cast_cols = f"""
    {cnum('Sales')}                 AS sales,
    CAST("Quantity" AS INTEGER)     AS quantity,
    CAST("Discount" AS DOUBLE)      AS discount,
    {cnum('Profit')}                AS profit
"""

# ============================================================
# Feature Engineering — Kolom baru dari kalkulasi
# ============================================================
feature_cols = """
    DATEDIFF('day', "Order Date", "Ship Date") AS shipping_days
"""

# ============================================================
# Gabungkan semua bagian & jalankan query Silver
# ============================================================
silver_sql = f"""
    CREATE TABLE silver.clean_orders AS
    SELECT
        {load_cols},
        {clean_cols},
        {cast_cols},
        {feature_cols}
    FROM bronze.raw_orders
    WHERE "Order ID" IS NOT NULL
      AND "Sales"    IS NOT NULL
      AND "Profit"   IS NOT NULL;
"""

con.execute(silver_sql)

# ============================================================
# Logging & Observasi — Ringkasan hasil load Silver
# ============================================================
silver_count = con.execute("SELECT COUNT(*) FROM silver.clean_orders").fetchone()[0]
null_removed = row_count - silver_count
print(f"   ✓ Tabel silver.clean_orders berhasil dibuat ({silver_count} baris)")
print(f"   ✓ Baris NULL yang dihapus: {null_removed}")

# ============================================================
# Validasi Kualitas Data — Cek duplikat, NULL, dan profit negatif
# ============================================================
print("\n   Data Quality Check:")
dup_count = con.execute("""
    SELECT COUNT(*) - COUNT(DISTINCT row_id) FROM silver.clean_orders
""").fetchone()[0]
print(f"   - Duplikat row_id     : {dup_count}")

null_sales = con.execute("""
    SELECT COUNT(*) FROM silver.clean_orders WHERE sales IS NULL
""").fetchone()[0]
print(f"   - NULL di kolom sales : {null_sales}")

neg_profit = con.execute("""
    SELECT COUNT(*) FROM silver.clean_orders WHERE profit < 0
""").fetchone()[0]
print(f"   - Transaksi rugi      : {neg_profit} dari {silver_count} ({round(neg_profit/silver_count*100,1)}%)")

# ============================================================
# Logging & Observasi — Preview 5 baris Silver
# ============================================================
print("\n   Contoh 5 baris Silver:")
print(con.execute("""
    SELECT order_id, order_date, customer_name, category,
           sales, profit, discount, shipping_days
    FROM silver.clean_orders LIMIT 5
""").fetchdf().to_string())



# ============================================================
# GOLD LAYER — Dimension Tables + Fact Table
# Ini adalah inti dari C4 pattern:
#   dim_customer, dim_product, dim_location, dim_date, dim_shipment
#   fact_orders  (tabel fakta utama, FK ke semua dimensi)
# ============================================================
print("\n\n[3/3] GOLD LAYER — Membuat Dimension Tables & Fact Table")

con.execute("CREATE SCHEMA IF NOT EXISTS gold;")

# ---------------------------------------------------------
# DIMENSION 1: dim_customer
# Atribut unik per customer
# ---------------------------------------------------------
con.execute("""
    CREATE TABLE gold.dim_customer AS
    SELECT DISTINCT
        customer_id,
        customer_name,
        segment
    FROM silver.clean_orders
    ORDER BY customer_id;
""")
dim_customer_cnt = con.execute("SELECT COUNT(*) FROM gold.dim_customer").fetchone()[0]
print(f"   ✓ gold.dim_customer      ({dim_customer_cnt} baris) — customer_id, customer_name, segment")

# ---------------------------------------------------------
# DIMENSION 2: dim_product
# Atribut unik per produk
# ---------------------------------------------------------
con.execute("""
    CREATE TABLE gold.dim_product AS
    SELECT DISTINCT
        product_id,
        product_name,
        category,
        sub_category
    FROM silver.clean_orders
    ORDER BY product_id;
""")
dim_product_cnt = con.execute("SELECT COUNT(*) FROM gold.dim_product").fetchone()[0]
print(f"   ✓ gold.dim_product       ({dim_product_cnt} baris) — product_id, product_name, category, sub_category")

# ---------------------------------------------------------
# DIMENSION 3: dim_location
# Hierarki geografis: region → state → city
# ---------------------------------------------------------
con.execute("""
    CREATE TABLE gold.dim_location AS
    SELECT DISTINCT
        region,
        state,
        city,
        postal_code,
        country
    FROM silver.clean_orders
    ORDER BY region, state, city;
""")
dim_location_cnt = con.execute("SELECT COUNT(*) FROM gold.dim_location").fetchone()[0]
print(f"   ✓ gold.dim_location      ({dim_location_cnt} baris) — country, region, state, city, postal_code")

# ---------------------------------------------------------
# DIMENSION 4: dim_date
# Kalender / time intelligence
# ---------------------------------------------------------
con.execute("""
    CREATE TABLE gold.dim_date AS
    SELECT DISTINCT
        order_date                                      AS date_key,
        EXTRACT(YEAR    FROM order_date)::INTEGER       AS year,
        EXTRACT(QUARTER FROM order_date)::INTEGER       AS quarter,
        EXTRACT(MONTH   FROM order_date)::INTEGER       AS month,
        MONTHNAME(order_date)                           AS month_name,
        EXTRACT(WEEK    FROM order_date)::INTEGER       AS week_of_year,
        EXTRACT(DAY     FROM order_date)::INTEGER       AS day_of_month,
        DAYNAME(order_date)                             AS day_name,
        CONCAT(EXTRACT(YEAR FROM order_date)::VARCHAR, '-Q',
               EXTRACT(QUARTER FROM order_date)::VARCHAR) AS year_quarter,
        CONCAT(EXTRACT(YEAR FROM order_date)::VARCHAR, '-',
               LPAD(EXTRACT(MONTH FROM order_date)::VARCHAR, 2, '0')) AS year_month
    FROM silver.clean_orders
    ORDER BY date_key;
""")
dim_date_cnt = con.execute("SELECT COUNT(*) FROM gold.dim_date").fetchone()[0]
print(f"   ✓ gold.dim_date          ({dim_date_cnt} baris) — date_key, year, quarter, month, week, day")

# ---------------------------------------------------------
# DIMENSION 5: dim_shipment
# Atribut pengiriman
# ---------------------------------------------------------
con.execute("""
    CREATE TABLE gold.dim_shipment AS
    SELECT DISTINCT
        ship_mode,
        CASE ship_mode
            WHEN 'Same Day'    THEN 1
            WHEN 'First Class' THEN 2
            WHEN 'Second Class'THEN 3
            WHEN 'Standard Class' THEN 4
            ELSE 99
        END AS ship_mode_rank,
        CASE ship_mode
            WHEN 'Same Day'    THEN 'Express'
            WHEN 'First Class' THEN 'Express'
            ELSE 'Regular'
        END AS ship_mode_type
    FROM silver.clean_orders
    ORDER BY ship_mode_rank;
""")
dim_shipment_cnt = con.execute("SELECT COUNT(*) FROM gold.dim_shipment").fetchone()[0]
print(f"   ✓ gold.dim_shipment      ({dim_shipment_cnt} baris) — ship_mode, ship_mode_rank, ship_mode_type")

# ---------------------------------------------------------
# FACT TABLE: fact_orders
# Tabel pusat yang menyimpan semua metrik transaksi
# FK ke semua tabel dimensi di atas
# ---------------------------------------------------------
con.execute("""
    CREATE TABLE gold.fact_orders AS
    SELECT
        -- Surrogate / Natural Keys
        o.row_id,
        o.order_id,

        -- Foreign Keys (ke Dimensi)
        o.order_date                    AS date_key,        -- FK → dim_date
        o.customer_id,                                      -- FK → dim_customer
        o.product_id,                                       -- FK → dim_product
        o.city,                                             -- FK → dim_location (bersama state)
        o.state,                                            -- FK → dim_location
        o.region,                                           -- FK → dim_location
        o.ship_mode,                                        -- FK → dim_shipment

        -- Degenerate Dimensions (atribut order, bukan FK ke dim)
        o.ship_date,
        o.shipping_days,
        o.discount,
        o.quantity,

        -- Measures / Metrik
        o.sales,
        o.profit,
        ROUND(o.profit / NULLIF(o.sales, 0) * 100, 4)  AS profit_margin_pct,
        o.sales - o.profit                               AS cost

    FROM silver.clean_orders o
    ORDER BY o.order_date, o.order_id;
""")
fact_cnt = con.execute("SELECT COUNT(*) FROM gold.fact_orders").fetchone()[0]
print(f"   ✓ gold.fact_orders       ({fact_cnt} baris) — Fact table utama (sales, profit, qty, discount)")

print("\n   Preview fact_orders (5 baris):")
print(con.execute("""
    SELECT row_id, order_id, date_key, customer_id, product_id,
           region, ship_mode, quantity, sales, profit, profit_margin_pct
    FROM gold.fact_orders LIMIT 5
""").fetchdf().to_string())


# ============================================================
# PREVIEW GOLD TABLES
# ============================================================
print("\n" + "=" * 60)
print("  PREVIEW GOLD — DIMENSION & FACT TABLES")
print("=" * 60)

print("\n🗂 dim_customer (5 baris):")
print(con.execute("SELECT * FROM gold.dim_customer LIMIT 5").fetchdf().to_string(index=False))

print("\n🗂 dim_product (5 baris):")
print(con.execute("SELECT * FROM gold.dim_product LIMIT 5").fetchdf().to_string(index=False))

print("\n🗂 dim_location (5 baris):")
print(con.execute("SELECT * FROM gold.dim_location LIMIT 5").fetchdf().to_string(index=False))

print("\n🗂 dim_date (5 baris):")
print(con.execute("SELECT * FROM gold.dim_date LIMIT 5").fetchdf().to_string(index=False))

print("\n🗂 dim_shipment:")
print(con.execute("SELECT * FROM gold.dim_shipment").fetchdf().to_string(index=False))

print("\n📦 fact_orders (5 baris):")
print(con.execute("""
    SELECT row_id, order_id, date_key, customer_id, product_id,
           region, ship_mode, quantity, sales, profit, profit_margin_pct, cost
    FROM gold.fact_orders LIMIT 5
""").fetchdf().to_string(index=False))

con.close()
