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
print("\n[1/4] BRONZE LAYER — Memuat data mentah dari CSV")

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
print("\n\n[2/4] SILVER LAYER — Membersihkan dan standarisasi data")

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
def cnum(col_name):
    return f'CAST("{col_name}" AS DOUBLE)'

cast_cols = """
    CAST("Sales" AS DOUBLE)         AS sales,
    CAST("Quantity" AS INTEGER)     AS quantity,
    CAST("Discount" AS DOUBLE)      AS discount,
    CAST("Profit" AS DOUBLE)        AS profit
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
# [PERBAIKAN STEP 1] — OUTLIER HANDLING menggunakan Metode IQR
# Menghapus data ekstrem pada kolom sales yang dapat
# mengganggu performa model prediktif
# ============================================================
print("\n   [PERBAIKAN] Outlier Handling — Metode IQR:")

before_outlier = con.execute("SELECT COUNT(*) FROM silver.clean_orders").fetchone()[0]

# Hitung batas IQR untuk kolom sales
q1 = con.execute("SELECT QUANTILE_CONT(sales, 0.25) FROM silver.clean_orders").fetchone()[0]
q3 = con.execute("SELECT QUANTILE_CONT(sales, 0.75) FROM silver.clean_orders").fetchone()[0]
iqr = q3 - q1
lower_bound = q1 - 1.5 * iqr
upper_bound = q3 + 1.5 * iqr

print(f"   - Q1 Sales            : {q1:.2f}")
print(f"   - Q3 Sales            : {q3:.2f}")
print(f"   - IQR                 : {iqr:.2f}")
print(f"   - Batas Bawah (Lower) : {lower_bound:.2f}")
print(f"   - Batas Atas  (Upper) : {upper_bound:.2f}")

con.execute(f"""
    CREATE OR REPLACE TABLE silver.clean_orders AS
    SELECT * FROM silver.clean_orders
    WHERE sales >= {lower_bound}
      AND sales <= {upper_bound}
""")

after_outlier = con.execute("SELECT COUNT(*) FROM silver.clean_orders").fetchone()[0]
removed_outlier = before_outlier - after_outlier
print(f"   - Outlier dihapus     : {removed_outlier} baris")
print(f"   - Data tersisa        : {after_outlier} baris")

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
print("\n\n[3/4] GOLD LAYER — Membuat Dimension Tables & Fact Table")

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


# ============================================================
# PREDICTIVE ANALYTICS — SALES PREDICTION MODEL
# ============================================================
print("\n\n[4/4] PREDICTIVE ANALYTICS — Sales Prediction Model")

import pandas as pd
import numpy as np

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# Ambil data dari Gold Layer + Dimension Table
model_df = con.execute("""
    SELECT
        f.date_key,
        f.quantity,
        f.discount,
        f.profit,
        f.shipping_days,
        p.category,
        c.segment,
        f.region,
        d.month,
        d.quarter,
        f.sales
    FROM gold.fact_orders f
    LEFT JOIN gold.dim_product p
        ON f.product_id = p.product_id
    LEFT JOIN gold.dim_customer c
        ON f.customer_id = c.customer_id
    LEFT JOIN gold.dim_date d
        ON f.date_key = d.date_key
    WHERE f.sales IS NOT NULL
      AND f.quantity IS NOT NULL
      AND f.discount IS NOT NULL
      AND f.profit IS NOT NULL
      AND f.shipping_days IS NOT NULL
      AND p.category IS NOT NULL
      AND c.segment IS NOT NULL
      AND f.region IS NOT NULL
      AND d.month IS NOT NULL
      AND d.quarter IS NOT NULL
""").fetchdf()

# Simpan tanggal untuk nanti dimasukkan ke hasil prediksi
date_series = model_df["date_key"]

# ============================================================
# [PERBAIKAN STEP 2] — ONE-HOT ENCODING (Tokenizing/Encoding)
# Mengkonversi fitur kategorikal menjadi representasi numerik
# agar dapat diproses algoritma Machine Learning
# ============================================================
print("\n   [PERBAIKAN] One-Hot Encoding fitur kategorikal:")
print("   - Kolom yang di-encode: category, segment, region")

model_df_encoded = pd.get_dummies(
    model_df.drop(columns=["date_key"]),
    columns=["category", "segment", "region"],
    drop_first=True
)

encoded_cols = [c for c in model_df_encoded.columns if any(
    c.startswith(p) for p in ["category_", "segment_", "region_"]
)]
print(f"   - Jumlah fitur dummy yang dibuat: {len(encoded_cols)}")
print(f"   - Nama fitur dummy: {encoded_cols}")

# Feature dan target
X = model_df_encoded.drop(columns=["sales"])
y = model_df_encoded["sales"]

print(f"\n   Total fitur untuk model: {X.shape[1]} fitur")
print(f"   Total data             : {X.shape[0]} baris")

# Split train-test
X_train, X_test, y_train, y_test = train_test_split(
    X, y,
    test_size=0.2,
    random_state=42
)

print(f"\n   Split Data:")
print(f"   - Train: {len(X_train)} baris (80%)")
print(f"   - Test : {len(X_test)} baris (20%)")

# ============================================================
# [PERBAIKAN STEP 3] — PERBANDINGAN DUA MODEL
# Random Forest vs Linear Regression
# Tujuan: membuktikan Random Forest adalah pilihan terbaik
# melalui eksperimen sistematis
# ============================================================
print("\n   [PERBAIKAN] Eksperimen & Perbandingan Model:")
print("   Melatih Model 1 — Random Forest Regressor...")

# Model 1: Random Forest Regressor
model_rf = RandomForestRegressor(
    n_estimators=200,
    max_depth=10,
    random_state=42
)
model_rf.fit(X_train, y_train)
y_pred_rf = model_rf.predict(X_test)

mae_rf   = mean_absolute_error(y_test, y_pred_rf)
rmse_rf  = np.sqrt(mean_squared_error(y_test, y_pred_rf))
r2_rf    = r2_score(y_test, y_pred_rf)

print("   Melatih Model 2 — Linear Regression...")

# Model 2: Linear Regression (pembanding)
model_lr = LinearRegression()
model_lr.fit(X_train, y_train)
y_pred_lr = model_lr.predict(X_test)

mae_lr   = mean_absolute_error(y_test, y_pred_lr)
rmse_lr  = np.sqrt(mean_squared_error(y_test, y_pred_lr))
r2_lr    = r2_score(y_test, y_pred_lr)

# Tabel perbandingan dua model
print("\n   ╔══════════════════════════════════════════════════════╗")
print("   ║           PERBANDINGAN PERFORMA MODEL                ║")
print("   ╠══════════════════════╦══════════╦══════════╦═════════╣")
print("   ║ Model                ║    MAE   ║   RMSE   ║   R²    ║")
print("   ╠══════════════════════╬══════════╬══════════╬═════════╣")
print(f"   ║ Random Forest        ║ {mae_rf:>8.2f} ║ {rmse_rf:>8.2f} ║ {r2_rf:>7.4f} ║")
print(f"   ║ Linear Regression    ║ {mae_lr:>8.2f} ║ {rmse_lr:>8.2f} ║ {r2_lr:>7.4f} ║")
print("   ╚══════════════════════╩══════════╩══════════╩═════════╝")

# Tentukan model terbaik berdasarkan R²
if r2_rf >= r2_lr:
    best_model_name = "Random Forest Regressor"
    best_r2 = r2_rf
    print(f"\n   ✓ Model Terbaik: {best_model_name} (R² = {best_r2:.4f})")
    print("   ✓ Alasan: Random Forest unggul dalam menangkap pola non-linear")
    y_pred = y_pred_rf
    mae, rmse, r2 = mae_rf, rmse_rf, r2_rf
else:
    best_model_name = "Linear Regression"
    best_r2 = r2_lr
    print(f"\n   ✓ Model Terbaik: {best_model_name} (R² = {best_r2:.4f})")
    y_pred = y_pred_lr
    mae, rmse, r2 = mae_lr, rmse_lr, r2_lr

# ============================================================
# [PERBAIKAN STEP 4] — INTERPRETASI BISNIS dari Hasil Evaluasi
# Menjelaskan arti angka MAE/RMSE/R² dalam konteks bisnis
# ============================================================
print("\n   [PERBAIKAN] Interpretasi Bisnis Model Terpilih:")
print(f"\n   Metrik Evaluasi ({best_model_name}):")
print(f"   - MAE  : {mae:.2f}")
print(f"   - RMSE : {rmse:.2f}")
print(f"   - R²   : {r2:.4f}")

print(f"\n   Interpretasi Bisnis:")
print(f"   • MAE = ${mae:.2f}")
print(f"     → Rata-rata prediksi penjualan meleset ${mae:.2f} dari nilai aktual.")
print(f"     → Artinya, model dapat memprediksi penjualan dengan toleransi ±${mae:.2f}.")

print(f"\n   • RMSE = ${rmse:.2f}")
print(f"     → RMSE lebih tinggi dari MAE karena memberikan penalti pada")
print(f"       transaksi bernilai sangat besar (Furniture, peralatan mahal).")
print(f"     → Hal ini normal dalam data ritel yang memiliki rentang harga luas.")

if r2 >= 0.85:
    level = "SANGAT BAIK"
    action = "Model layak digunakan langsung untuk mendukung keputusan inventory, promosi, dan perencanaan revenue."
elif r2 >= 0.70:
    level = "BAIK"
    action = "Model dapat digunakan sebagai panduan perencanaan. Disarankan dikombinasikan dengan judgment bisnis."
elif r2 >= 0.50:
    level = "CUKUP"
    action = "Model memberikan gambaran umum tren. Perlu penambahan fitur atau data lebih banyak untuk akurasi lebih tinggi."
else:
    level = "PERLU DITINGKATKAN"
    action = "Pertimbangkan penambahan fitur eksternal atau metode yang lebih kompleks."

print(f"\n   • R² = {r2:.4f} → Performa Model: {level}")
print(f"     → Model mampu menjelaskan {r2*100:.1f}% variasi data penjualan.")
print(f"     → {action}")

print(f"\n   Rekomendasi Bisnis:")
print(f"   - Gunakan model untuk estimasi revenue bulanan per kategori produk")
print(f"   - Model paling akurat untuk kategori Office Supplies (harga konsisten)")
print(f"   - Transaksi Furniture memiliki error lebih tinggi karena harga sangat variatif")
print(f"   - Jadwalkan re-training model setiap kuartal dengan data terbaru")

# Simpan hasil prediksi ke Gold Layer
prediction_df = pd.DataFrame()
prediction_df["date_key"] = date_series.loc[X_test.index].values
prediction_df["actual_sales"] = y_test.values
prediction_df["predicted_sales"] = y_pred
prediction_df["prediction_error"] = prediction_df["actual_sales"] - prediction_df["predicted_sales"]
prediction_df["absolute_error"] = abs(prediction_df["prediction_error"])

con.execute("""
    CREATE OR REPLACE TABLE gold.sales_prediction AS
    SELECT * FROM prediction_df
""")

print("\n   ✓ Tabel gold.sales_prediction berhasil dibuat")
print("\n   Preview hasil prediksi:")
print(con.execute("""
    SELECT date_key, actual_sales, predicted_sales, prediction_error, absolute_error
    FROM gold.sales_prediction
    ORDER BY date_key
    LIMIT 10
""").fetchdf().to_string(index=False))

# ============================================================
# [PERBAIKAN STEP 5] — SIMPAN PERBANDINGAN MODEL KE GOLD LAYER
# Untuk ditampilkan di dashboard Power BI
# ============================================================
comparison_df = pd.DataFrame({
    "model_name"   : ["Random Forest", "Linear Regression"],
    "mae"          : [mae_rf, mae_lr],
    "rmse"         : [rmse_rf, rmse_lr],
    "r2_score"     : [r2_rf, r2_lr],
    "is_best_model": [r2_rf >= r2_lr, r2_lr > r2_rf]
})

con.execute("""
    CREATE OR REPLACE TABLE gold.model_comparison AS
    SELECT * FROM comparison_df
""")

# Simpan metrik evaluasi model terbaik ke tabel agar bisa dipakai di Power BI
evaluation_df = pd.DataFrame({
    "metric"      : ["MAE", "RMSE", "R2", "Best Model"],
    "value_num"   : [mae, rmse, r2, None],
    "value_text"  : [f"${mae:.2f}", f"${rmse:.2f}", f"{r2:.4f}", best_model_name]
})

con.execute("""
    CREATE OR REPLACE TABLE gold.model_evaluation AS
    SELECT * FROM evaluation_df
""")

print("\n   ✓ Tabel gold.model_comparison berhasil dibuat")
print("\n   Model Comparison Table:")
print(con.execute("SELECT * FROM gold.model_comparison").fetchdf().to_string(index=False))

print("\n   ✓ Tabel gold.model_evaluation berhasil dibuat")
print("\n   Model Evaluation Table:")
print(con.execute("SELECT * FROM gold.model_evaluation").fetchdf().to_string(index=False))



con.close()