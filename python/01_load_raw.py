import os
import sys
import time
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# 1. Project root aur .env configuration load karna
PROJECT_ROOT = Path(__file__).resolve().parent.parent
dotenv_path = PROJECT_ROOT / ".env"

if not dotenv_path.exists():
    print(f"[ERROR] .env file nahi mili: {dotenv_path}")
    sys.exit(1)

load_dotenv(dotenv_path=dotenv_path)

db_host = os.getenv("DB_HOST", "localhost")
db_port = int(os.getenv("DB_PORT", 3306))
db_user = os.getenv("DB_USER", "root")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME", "recon_db")

if not db_password:
    print("[ERROR] .env file mein DB_PASSWORD set nahi hai. Kripya check karein.")
    sys.exit(1)

print("=" * 65)
print("  FINANCIAL RECONCILIATION & LEAKAGE DETECTOR - PHASE 1")
print("  Task: Raw CSV Data Ingestion into MySQL")
print("=" * 65)
print(f"[*] Target Database : {db_name}")
print(f"[*] Host             : {db_host}:{db_port}")
print(f"[*] User             : {db_user}")

# 2. Database create karna agar pehle se exist nahi karta
print("\n[Step 1/4] Checking MySQL connection & ensuring database exists...")
try:
    server_url = URL.create(
        drivername="mysql+pymysql",
        username=db_user,
        password=db_password,
        host=db_host,
        port=db_port,
    )
    server_engine = create_engine(server_url)
    with server_engine.connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{db_name}`;"))
        conn.commit()
    server_engine.dispose()
    print(f"  [+] Database '{db_name}' verified/ready.")
except Exception as e:
    print(f"\n[ERROR] MySQL connection fail hua: {e}")
    sys.exit(1)

# 3. Target database ke liye engine banana
db_url = URL.create(
    drivername="mysql+pymysql",
    username=db_user,
    password=db_password,
    host=db_host,
    port=db_port,
    database=db_name,
)
engine = create_engine(db_url, pool_recycle=3600)

# 4. CSV files list jo load karni hain
datasets = [
    {
        "table_name": "raw_orders",
        "csv_path": PROJECT_ROOT / "data" / "raw" / "olist_orders_dataset.csv",
    },
    {
        "table_name": "raw_order_items",
        "csv_path": PROJECT_ROOT / "data" / "raw" / "olist_order_items_dataset.csv",
    },
    {
        "table_name": "raw_order_payments",
        "csv_path": PROJECT_ROOT / "data" / "raw" / "olist_order_payments_dataset.csv",
    },
]

# 5. Har CSV ko read karke MySQL me insert karna
print("\n[Step 2/4] Loading CSV files into MySQL tables...")
load_summary = []

for item in datasets:
    t_name = item["table_name"]
    c_path = item["csv_path"]

    if not c_path.exists():
        print(f"  [-] CSV file missing: {c_path}")
        continue

    print(f"\n  -> Reading '{c_path.name}'...")
    start_time = time.time()
    df = pd.read_csv(c_path)
    csv_rows = len(df)
    print(f"     Read {csv_rows:,} rows. Inserting into table '{t_name}' in batches...")

    # Batch loading using chunksize
    df.to_sql(
        name=t_name,
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=10000,
    )
    elapsed = time.time() - start_time
    print(f"     [+] Successfully inserted into '{t_name}' in {elapsed:.2f} seconds.")

    load_summary.append({
        "table": t_name,
        "csv_rows": csv_rows,
    })

# 6. Database se row count verify karna
print("\n[Step 3/4] Verifying table row counts directly from MySQL...")
results = []
with engine.connect() as conn:
    for entry in load_summary:
        t_name = entry["table"]
        query = text(f"SELECT COUNT(*) FROM `{t_name}`;")
        db_count = conn.execute(query).scalar()
        status = "MATCH (OK)" if entry["csv_rows"] == db_count else "MISMATCH (ERROR)"
        results.append({
            "table": t_name,
            "csv_rows": entry["csv_rows"],
            "db_rows": db_count,
            "status": status,
        })

# 7. Final Verification Report display karna
print("\n[Step 4/4] Ingestion Summary Report")
print("=" * 65)
print(f"{'TABLE NAME':<22} | {'CSV ROWS':>10} | {'DB ROWS':>10} | {'STATUS':<12}")
print("-" * 65)
for res in results:
    print(f"{res['table']:<22} | {res['csv_rows']:>10,} | {res['db_rows']:>10,} | {res['status']:<12}")
print("=" * 65)
print("\n[SUCCESS] Phase 1: Raw data ingestion complete!")
