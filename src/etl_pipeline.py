import pandas as pd
import sqlite3
import glob
from pathlib import Path

# === Paths ===
ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
PROCESSED = ROOT / "data" / "processed"
WAREHOUSE = ROOT / "data" / "warehouse"
DB_PATH = WAREHOUSE / "retail.db"

# === Step 1: Extract ===
def extract() -> pd.DataFrame:
    files = sorted(glob.glob(str(RAW / "*.csv")))
    if not files:
        raise FileNotFoundError("No CSV files found in data/raw/")
    dfs = [pd.read_csv(f) for f in files]
    df = pd.concat(dfs, ignore_index=True)
    return df

# === Step 2: Transform ===
def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df = df.dropna(subset=["order_date", "price", "quantity", "discount"])

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["discount"] = pd.to_numeric(df["discount"], errors="coerce")

    df["gross"] = df["price"] * df["quantity"]
    df["net_revenue"] = df["gross"] * (1 - df["discount"])

    PROCESSED.mkdir(parents=True, exist_ok=True)
    df.to_csv(PROCESSED / "sales_clean.csv", index=False)

    return df

# === Step 3: Load to SQLite Star Schema ===
def load_star_schema(df: pd.DataFrame):
    WAREHOUSE.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Drop and create tables
    cur.executescript("""
    PRAGMA foreign_keys = ON;

    DROP TABLE IF EXISTS fact_sales;
    DROP TABLE IF EXISTS dim_date;
    DROP TABLE IF EXISTS dim_customer;
    DROP TABLE IF EXISTS dim_product;
    DROP TABLE IF EXISTS dim_region;

    CREATE TABLE dim_date (
        date_id INTEGER PRIMARY KEY,
        date TEXT,
        year INTEGER, month INTEGER, day INTEGER
    );

    CREATE TABLE dim_customer (
        customer_id INTEGER PRIMARY KEY
    );

    CREATE TABLE dim_product (
        product_id INTEGER PRIMARY KEY,
        category TEXT,
        subcategory TEXT
        
    );

    CREATE TABLE dim_region (
        region_id INTEGER PRIMARY KEY,
        region TEXT UNIQUE
    );

    CREATE TABLE fact_sales (
        order_id TEXT PRIMARY KEY,
        date_id INTEGER,
        customer_id INTEGER,
        product_id INTEGER,
        region_id INTEGER,
        price REAL,
        quantity INTEGER,
        discount REAL,
        gross REAL,
        net_revenue REAL,
        FOREIGN KEY(date_id) REFERENCES dim_date(date_id),
        FOREIGN KEY(customer_id) REFERENCES dim_customer(customer_id),
        FOREIGN KEY(product_id) REFERENCES dim_product(product_id),
        FOREIGN KEY(region_id) REFERENCES dim_region(region_id)
    );
    """)

    # === Load dimensions ===
    dim_date = (
        df[["order_date"]]
        .drop_duplicates()
        .assign(
            date=lambda x: x["order_date"].dt.strftime("%Y-%m-%d"),
            year=lambda x: x["order_date"].dt.year,
            month=lambda x: x["order_date"].dt.month,
            day=lambda x: x["order_date"].dt.day,
        )
        .drop(columns=["order_date"])  # ✅ drop this column before writing to DB
        .reset_index(drop=True)
    )

    
    dim_date["date_id"] = range(1, len(dim_date) + 1)
    dim_date.to_sql("dim_date", conn, if_exists="append", index=False)

    df[["customer_id"]].drop_duplicates().to_sql("dim_customer", conn, if_exists="append", index=False)
    df[["product_id", "category", "subcategory"]].drop_duplicates(subset=["product_id"]).to_sql(
    "dim_product", conn, if_exists="append", index=False
    )


    dim_region = pd.DataFrame(sorted(df["region"].unique()), columns=["region"])
    dim_region["region_id"] = range(1, len(dim_region) + 1)
    dim_region.to_sql("dim_region", conn, if_exists="append", index=False)

    # === Lookups ===
    date_lookup = dict(zip(dim_date["date"], dim_date["date_id"]))
    region_lookup = dict(zip(dim_region["region"], dim_region["region_id"]))

    fact = df.copy()
    fact["date"] = fact["order_date"].dt.strftime("%Y-%m-%d")
    fact["date_id"] = fact["date"].map(date_lookup)
    fact["region_id"] = fact["region"].map(region_lookup)

    fact_sales_cols = [
        "order_id", "date_id", "customer_id", "product_id", "region_id",
        "price", "quantity", "discount", "gross", "net_revenue"
    ]
    fact[fact_sales_cols].drop_duplicates().to_sql("fact_sales", conn, if_exists="append", index=False)

    conn.commit()
    conn.close()

# === Run all ===
def run():
    print("🔍 Extracting...")
    df = extract()
    print(f"   {len(df)} rows loaded from raw CSVs")

    print("🧹 Transforming...")
    df = transform(df)
    print(f"   {len(df)} rows cleaned and saved")

    print("🏗️ Loading to SQLite star schema...")
    load_star_schema(df)

    print("✅ Done! See: data/warehouse/retail.db")

if __name__ == "__main__":
    run()
