# Databricks notebook source
# MAGIC %md
# MAGIC # Lab config
# MAGIC
# MAGIC Shared catalog / schema / volume names. Every other notebook runs this with `%run ./_config`.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Unity Catalog name")
dbutils.widgets.text("schema", "dea_lab", "Schema name")

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
FQN = f"{CATALOG}.{SCHEMA}"

VOLUME = "landing"
VOL_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
ORDERS_CSV = f"{VOL_PATH}/orders_csv"
ORDERS_JSON = f"{VOL_PATH}/orders_json"
CUSTOMERS_CSV = f"{VOL_PATH}/customers_csv"
CHECKPOINT = f"{VOL_PATH}/_checkpoints"
SCHEMA_LOC = f"{VOL_PATH}/_schemas"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"catalog.schema = {FQN}")
print(f"volume path    = {VOL_PATH}")
