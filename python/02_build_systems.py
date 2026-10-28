import os
import sys
import time
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text, types
from sqlalchemy.engine import URL

# ==============================================================================
# 1. ENVIRONMENT AUR CONFIGURATION SETUP
# ==============================================================================
# Project root directory aur .env file dhoondna
PROJECT_ROOT = Path(__file__).resolve().parent.parent
dotenv_path = PROJECT_ROOT / ".env"

if not dotenv_path.exists():
    print(f"[ERROR] .env file nahi mili: {dotenv_path}")
    sys.exit(1)

# .env se credentials load karna (no hardcoded password)
load_dotenv(dotenv_path=dotenv_path)

db_host = os.getenv("DB_HOST", "localhost")
db_port = int(os.getenv("DB_PORT", 3306))
db_user = os.getenv("DB_USER", "root")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME", "recon_db")

if not db_password:
    print("[ERROR] .env file mein DB_PASSWORD nahi mila.")
    sys.exit(1)

print("=" * 78)
print("  FINANCIAL RECONCILIATION & LEAKAGE DETECTOR - PHASE 2")
print("  Task: Create 3 Enterprise Source Systems with Indexes & DATE Types")
print("=" * 78)

# SQLAlchemy database engine create karna
db_url = URL.create(
    drivername="mysql+pymysql",
    username=db_user,
    password=db_password,
    host=db_host,
    port=db_port,
    database=db_name,
)
engine = create_engine(db_url, pool_recycle=3600)

# ==============================================================================
# 2. PURANI TABLES DROP KARNA (IDEMPOTENCY / CLEAN SLATE)
# ==============================================================================
# Script dobara chalane par duplicate data ya conflict na ho, isliye pehle purani tables drop karte hain
print("\n[Step 1/6] Purani system tables drop karna (Clean Slate)...")
with engine.connect() as conn:
    conn.execute(text("DROP TABLE IF EXISTS `sys_bank_settlements`;"))
    conn.execute(text("DROP TABLE IF EXISTS `sys_gateway_payments`;"))
    conn.execute(text("DROP TABLE IF EXISTS `sys_invoices`;"))
    conn.commit()
print("  [+] Tables drop ho chuki hain. Fresh environment taiyar hai.")

# ==============================================================================
# 3. SYSTEM 1: sys_invoices (ERP / Billing System) BANANA
# ==============================================================================
print("\n[Step 2/6] Building 'sys_invoices' (ERP / Billing System)...")
t0 = time.time()

# raw_orders aur raw_order_items ko MySQL se load karna
df_orders = pd.read_sql(
    "SELECT order_id, order_status, order_purchase_timestamp, order_approved_at FROM raw_orders",
    con=engine
)
df_items = pd.read_sql(
    "SELECT order_id, price, freight_value FROM raw_order_items",
    con=engine
)

# Har item ka total price calculate karna: price + freight_value
df_items["item_total"] = df_items["price"] + df_items["freight_value"]

# Order level par group karke sum karna (2 decimal places)
df_order_amounts = (
    df_items.groupby("order_id", as_index=False)["item_total"]
    .sum()
    .rename(columns={"item_total": "invoice_amount"})
)
df_order_amounts["invoice_amount"] = df_order_amounts["invoice_amount"].round(2)

# Orders ke saath INNER JOIN: Sirf wahi orders aayenge jinke items hain
df_invoices = pd.merge(df_order_amounts, df_orders, on="order_id", how="inner")

# Naye columns format karna
df_invoices["invoice_id"] = "INV-" + df_invoices["order_id"].astype(str)
df_invoices["order_ref"] = df_invoices["order_id"].astype(str)
# invoice_date: DATE format mein convert karna
df_invoices["invoice_date"] = pd.to_datetime(df_invoices["order_purchase_timestamp"]).dt.date

# Columns select aur order karna
df_invoices = df_invoices[[
    "invoice_id",
    "order_ref",
    "invoice_date",
    "invoice_amount",
    "order_status"
]]

# MySQL schema types specify karna (DATE type & VARCHAR taaki index fast aur valid bane)
invoices_dtypes = {
    "invoice_id": types.VARCHAR(64),
    "order_ref": types.VARCHAR(64),
    "invoice_date": types.Date(),
    "invoice_amount": types.DECIMAL(12, 2),
    "order_status": types.VARCHAR(32),
}

df_invoices.to_sql(
    name="sys_invoices",
    con=engine,
    if_exists="replace",
    index=False,
    chunksize=10000,
    dtype=invoices_dtypes,
)
print(f"  [+] 'sys_invoices' successfully created with {len(df_invoices):,} rows in {time.time() - t0:.2f}s.")

# ==============================================================================
# 4. SYSTEM 2: sys_gateway_payments (Payment Gateway System) BANANA
# ==============================================================================
print("\n[Step 3/6] Building 'sys_gateway_payments' (Payment Gateway System)...")
t0 = time.time()

# raw_order_payments load karna
df_payments = pd.read_sql("SELECT order_id, payment_type, payment_value FROM raw_order_payments", con=engine)

# Timestamps merge karna (order_approved_at & order_purchase_timestamp)
df_gw = pd.merge(
    df_payments,
    df_orders[["order_id", "order_approved_at", "order_purchase_timestamp"]],
    on="order_id",
    how="left"
)

# Har payment ke liye unique TXN- running number banana (e.g. TXN-000001)
num_payments = len(df_gw)
running_numbers = np.arange(1, num_payments + 1)
df_gw["txn_id"] = [f"TXN-{n:06d}" for n in running_numbers]
df_gw["order_ref"] = df_gw["order_id"].astype(str)

# txn_time logic: order_approved_at use karo; agar missing ya blank ho toh order_purchase_timestamp fallback
df_gw["txn_time"] = df_gw["order_approved_at"].fillna(df_gw["order_purchase_timestamp"])
blank_mask = df_gw["txn_time"].isna() | (df_gw["txn_time"].astype(str).str.strip() == "")
df_gw.loc[blank_mask, "txn_time"] = df_gw.loc[blank_mask, "order_purchase_timestamp"]

df_gw["pay_method"] = df_gw["payment_type"]
df_gw["gross_amount"] = df_gw["payment_value"].round(2)
df_gw["txn_status"] = "SUCCESS"

# Final columns select aur order karna
df_gateway_payments = df_gw[[
    "txn_id",
    "order_ref",
    "txn_time",
    "pay_method",
    "gross_amount",
    "txn_status"
]]

# MySQL schema types specify karna
gateway_dtypes = {
    "txn_id": types.VARCHAR(64),
    "order_ref": types.VARCHAR(64),
    "txn_time": types.VARCHAR(32),
    "pay_method": types.VARCHAR(32),
    "gross_amount": types.DECIMAL(12, 2),
    "txn_status": types.VARCHAR(16),
}

df_gateway_payments.to_sql(
    name="sys_gateway_payments",
    con=engine,
    if_exists="replace",
    index=False,
    chunksize=10000,
    dtype=gateway_dtypes,
)
print(f"  [+] 'sys_gateway_payments' successfully created with {len(df_gateway_payments):,} rows in {time.time() - t0:.2f}s.")

# ==============================================================================
# 5. SYSTEM 3: sys_bank_settlements (Bank System) BANANA
# ==============================================================================
print("\n[Step 4/6] Building 'sys_bank_settlements' (Bank System)...")
t0 = time.time()

# Random seed = 42 fix karna taaki reproducible aur consistent result mile
np.random.seed(42)

df_bank = pd.DataFrame()
# STL- running number banana (e.g. STL-000001)
df_bank["settlement_id"] = [f"STL-{n:06d}" for n in running_numbers]
df_bank["txn_ref"] = df_gateway_payments["txn_id"]

# Random 1 se 3 din ka delay add karna
random_days = np.random.randint(1, 4, size=num_payments)
# settled_date: DATE format mein convert karna
df_bank["settled_date"] = (
    pd.to_datetime(df_gateway_payments["txn_time"]) + pd.to_timedelta(random_days, unit="D")
).dt.date

# Fee amount: gross_amount ka 2% (2 decimal places)
fee = (df_gateway_payments["gross_amount"] * 0.02).round(2)
df_bank["fee_amount"] = fee

# Net amount: gross_amount - fee_amount
df_bank["net_amount"] = (df_gateway_payments["gross_amount"] - fee).round(2)

# MySQL schema types specify karna (settled_date as Date type)
bank_dtypes = {
    "settlement_id": types.VARCHAR(64),
    "txn_ref": types.VARCHAR(64),
    "settled_date": types.Date(),
    "fee_amount": types.DECIMAL(12, 2),
    "net_amount": types.DECIMAL(12, 2),
}

df_bank.to_sql(
    name="sys_bank_settlements",
    con=engine,
    if_exists="replace",
    index=False,
    chunksize=10000,
    dtype=bank_dtypes,
)
print(f"  [+] 'sys_bank_settlements' successfully created with {len(df_bank):,} rows in {time.time() - t0:.2f}s.")

# ==============================================================================
# 6. INDEXES CREATE KARNA (FOR FAST RECONCILIATION JOINS)
# ==============================================================================
print("\n[Step 5/6] Creating database indexes for performance...")
t0 = time.time()

with engine.connect() as conn:
    # 1. sys_invoices: index on order_ref
    conn.execute(text("CREATE INDEX idx_invoices_order_ref ON sys_invoices(order_ref);"))
    # 2. sys_gateway_payments: index on order_ref aur txn_id
    conn.execute(text("CREATE INDEX idx_gateway_order_ref ON sys_gateway_payments(order_ref);"))
    conn.execute(text("CREATE INDEX idx_gateway_txn_id ON sys_gateway_payments(txn_id);"))
    # 3. sys_bank_settlements: index on txn_ref
    conn.execute(text("CREATE INDEX idx_bank_txn_ref ON sys_bank_settlements(txn_ref);"))
    conn.commit()

print(f"  [+] Saare indexes successfully create ho gaye in {time.time() - t0:.2f}s:")
print("      - sys_invoices(order_ref)")
print("      - sys_gateway_payments(order_ref, txn_id)")
print("      - sys_bank_settlements(txn_ref)")

# ==============================================================================
# 7. DIRECT MYSQL VERIFICATION AUR FINANCIAL TOTALS (R$ BRL)
# ==============================================================================
print("\n[Step 6/6] Querying MySQL for row counts & financial totals...")

with engine.connect() as conn:
    # 1. sys_invoices summary
    res_inv = conn.execute(text("SELECT COUNT(*), SUM(invoice_amount) FROM sys_invoices;")).fetchone()
    count_inv, sum_inv = res_inv[0], float(res_inv[1] or 0.0)

    # 2. sys_gateway_payments summary
    res_gw = conn.execute(text("SELECT COUNT(*), SUM(gross_amount) FROM sys_gateway_payments;")).fetchone()
    count_gw, sum_gw = res_gw[0], float(res_gw[1] or 0.0)

    # 3. sys_bank_settlements summary
    res_bnk = conn.execute(text("SELECT COUNT(*), SUM(fee_amount), SUM(net_amount) FROM sys_bank_settlements;")).fetchone()
    count_bnk, sum_fee, sum_net = res_bnk[0], float(res_bnk[1] or 0.0), float(res_bnk[2] or 0.0)

# Final summary table print karna with R$ (BRL) currency
print("\n" + "=" * 80)
print(f"{'SYSTEM TABLE':<24} | {'ROW COUNT':>12} | {'FINANCIAL TOTAL [R$ (BRL)]':<36}")
print("-" * 80)
print(f"{'sys_invoices':<24} | {count_inv:>12,} | Total Invoice:  R$ {sum_inv:>14,.2f} (BRL)")
print(f"{'sys_gateway_payments':<24} | {count_gw:>12,} | Total Gross:    R$ {sum_gw:>14,.2f} (BRL)")
print(f"{'sys_bank_settlements':<24} | {count_bnk:>12,} | Total Net:      R$ {sum_net:>14,.2f} (BRL)")
print(f"{'':<24} | {'':>12} | Total Bank Fee: R$ {sum_fee:>14,.2f} (BRL)")
print("=" * 80)

print("\n[SUCCESS] Phase 2: All 3 source system tables created, indexed, and verified!")
