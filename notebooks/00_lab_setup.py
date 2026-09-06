# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Lab setup
# MAGIC
# MAGIC Creates the Unity Catalog schema, a landing volume, and sample retail files.
# MAGIC
# MAGIC **Before you run**
# MAGIC 1. Attach **Serverless** or a 15.4+ cluster.
# MAGIC 2. Set the `catalog` widget. On Free Edition this is usually `workspace`.
# MAGIC 3. Leave `schema` as `dea_lab` unless that name is already taken.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQN}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {FQN}.{VOLUME}")

print("Schema and volume ready.")
print(f"Open Catalog Explorer → {CATALOG} → {SCHEMA} → Volumes → {VOLUME}")

# COMMAND ----------

from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import *

# --- customers (CSV landing) ---
customers = [
    (101, "Ana Nguyen", "ana@fuvekon.com", "west", "123-45-6789"),
    (102, "Ben Cole", "ben@fuvekon.com", "east", "987-65-4321"),
    (103, "Cara Diaz", None, "west", "555-12-3456"),
    (104, "Dan Park", "dan@fuvekon.com", "east", "111-22-3333"),
    (105, "Eva Chen", "eva@fuvekon.com", "west", "222-33-4444"),
    (105, "Eva Chen", "eva@fuvekon.com", "west", "222-33-4444"),  # duplicate
]
cust_df = spark.createDataFrame(
    customers, ["customer_id", "name", "email", "region", "ssn"]
)
(cust_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", True)
    .csv(CUSTOMERS_CSV))

# --- orders batch 1 (CSV) ---
base = datetime(2026, 9, 1, 10, 0, 0)
orders_b1 = [
    (1, 101, "SKU-TEA", "12.50", (base).isoformat(), "usd"),
    (2, 102, "SKU-MUG", "9.00", (base + timedelta(hours=1)).isoformat(), "usd"),
    (3, 103, "SKU-TEA", None, (base + timedelta(hours=2)).isoformat(), "usd"),
    (4, 999, "SKU-BAG", "25.00", (base + timedelta(hours=3)).isoformat(), "usd"),  # orphan
    (5, 104, "SKU-MUG", "9.00", (base + timedelta(hours=4)).isoformat(), "usd"),
]
ord_schema = "order_id INT, customer_id INT, sku STRING, amount STRING, order_ts STRING, currency STRING"
(spark.createDataFrame(orders_b1, ord_schema)
    .coalesce(1)
    .write.mode("overwrite")
    .option("header", True)
    .csv(f"{ORDERS_CSV}/batch=1"))

# --- nested JSON (semi-structured) ---
json_rows = [
    {
        "order_id": 10,
        "customer_id": 101,
        "items": [{"sku": "SKU-TEA", "qty": 2}, {"sku": "SKU-MUG", "qty": 1}],
        "ship_to": {"city": "Hanoi", "country": "VN"},
        "amount": 34.0,
    },
    {
        "order_id": 11,
        "customer_id": 102,
        "items": [{"sku": "SKU-BAG", "qty": 1}],
        "ship_to": {"city": "Austin", "country": "US"},
        "amount": 25.0,
    },
]
(spark.createDataFrame(json_rows)
    .coalesce(1)
    .write.mode("overwrite")
    .json(f"{ORDERS_JSON}/batch=1"))

print("Landing files written.")
display(dbutils.fs.ls(VOL_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check in the UI
# MAGIC
# MAGIC 1. Left sidebar → **Catalog**.
# MAGIC 2. Open `{catalog}` → `{schema}` → **Volumes** → `landing`.
# MAGIC 3. You should see `customers_csv`, `orders_csv`, `orders_json`.
# MAGIC
# MAGIC That volume is this lab's stand-in for ADLS / S3 / GCS. `COPY INTO` and Auto Loader work the same way against a volume path as they do against `s3://`, `abfss://`, or `gs://`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (run only when you are finished with the whole lab)

# COMMAND ----------

# spark.sql(f"DROP SCHEMA IF EXISTS {FQN} CASCADE")
print("Cleanup is commented out. Uncomment the line above to delete the lab schema.")
