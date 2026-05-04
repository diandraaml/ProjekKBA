import duckdb, os

con = duckdb.connect("superstore.duckdb")
os.makedirs("exports", exist_ok=True)

tables = [
    "gold.fact_orders",
    "gold.dim_customer",
    "gold.dim_product",
    "gold.dim_location",
    "gold.dim_date",
    "gold.dim_shipment"
]

for tbl in tables:
    name = tbl.split(".")[1]
    con.execute(f"""
        COPY {tbl} TO 'exports/{name}.csv'
        (HEADER, DELIMITER ',')
    """)
    print(f"✓ exports/{name}.csv")

con.close()