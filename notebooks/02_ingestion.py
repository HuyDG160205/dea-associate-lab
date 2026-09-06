# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Data ingestion and loading (Domain 2, 21%)
# MAGIC
# MAGIC You will:
# MAGIC - Incrementally load CSV with **COPY INTO**
# MAGIC - Land JSON with **Auto Loader** (schema enforcement + evolution, `availableNow` batch)
# MAGIC - Pull JSON from a **REST** API into a UC table
# MAGIC - See the **Lakeflow Connect** UI
# MAGIC - Choose between COPY INTO, Auto Loader, Lakeflow Connect, JDBC, and partners
# MAGIC
# MAGIC Run `00_lab_setup` first.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Ingestion patterns
# MAGIC
# MAGIC | Pattern | When | In this lab |
# MAGIC |---|---|---|
# MAGIC | **Batch** | Full or large slice, on a schedule | First `COPY INTO` |
# MAGIC | **Incremental** | Only new files / new rows | `COPY INTO` (skips already-loaded files) and Auto Loader checkpoints |
# MAGIC | **Streaming** | Continuous or micro-batch | Auto Loader `readStream` |
# MAGIC | **Triggered batch** | Stream engine, but drain all new files then stop | Auto Loader `.trigger(availableNow=True)` |
# MAGIC
# MAGIC Sources the exam names:
# MAGIC - Local / workspace files and **Volumes**
# MAGIC - Cloud object storage (ADLS / S3 / GCS)
# MAGIC - **Lakeflow Connect** standard connectors (files, cloud storage)
# MAGIC - **Lakeflow Connect** managed connectors (Salesforce, SQL Server, Workday, …)
# MAGIC - JDBC / ODBC / REST from a notebook, usually scheduled by a Lakeflow Job

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 COPY INTO — incremental files into a UC table
# MAGIC
# MAGIC `COPY INTO` is idempotent: a file that was already loaded is skipped even if someone edits it later.
# MAGIC
# MAGIC On the exam, the source is `s3://`, `abfss://`, or `gs://`. Here we use a **Volume** path so you do not need cloud credentials. Same SQL.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQN}.bronze_orders_copy (
  order_id INT,
  customer_id INT,
  sku STRING,
  amount STRING,
  order_ts STRING,
  currency STRING
)
""")

# Validate first — exam likes VALIDATE
display(spark.sql(f"""
COPY INTO {FQN}.bronze_orders_copy
FROM '{ORDERS_CSV}'
FILEFORMAT = CSV
VALIDATE 20 ROWS
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
"""))

# COMMAND ----------

display(spark.sql(f"""
COPY INTO {FQN}.bronze_orders_copy
FROM '{ORDERS_CSV}'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true')
"""))

print("Rows after first load:")
display(spark.sql(f"SELECT * FROM {FQN}.bronze_orders_copy ORDER BY order_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC Add a **second batch** and run `COPY INTO` again. Only the new file is ingested.

# COMMAND ----------

from datetime import datetime, timedelta

base = datetime(2026, 9, 2, 9, 0, 0)
batch2 = [
    (6, 101, "SKU-TEA", "12.50", base.isoformat(), "usd"),
    (7, 105, "SKU-MUG", "9.00", (base + timedelta(hours=1)).isoformat(), "usd"),
]
(spark.createDataFrame(batch2, "order_id INT, customer_id INT, sku STRING, amount STRING, order_ts STRING, currency STRING")
    .coalesce(1)
    .write.mode("overwrite")
    .option("header", True)
    .csv(f"{ORDERS_CSV}/batch=2"))

display(spark.sql(f"""
COPY INTO {FQN}.bronze_orders_copy
FROM '{ORDERS_CSV}'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
"""))

print("Should now include order_id 6 and 7. Re-run the same COPY INTO — row count must stay the same.")
display(spark.sql(f"SELECT COUNT(*) AS n FROM {FQN}.bronze_orders_copy"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### COPY INTO options to memorize
# MAGIC
# MAGIC | Option | Meaning |
# MAGIC |---|---|
# MAGIC | `FILEFORMAT` | CSV, JSON, PARQUET, AVRO, ORC, TEXT, BINARYFILE |
# MAGIC | `PATTERN` | Glob, e.g. `files*.csv` |
# MAGIC | `FILES` | Explicit list, max 1000 |
# MAGIC | `FORMAT_OPTIONS` | Reader options (`header`, `inferSchema`, `multiline`) |
# MAGIC | `COPY_OPTIONS ('mergeSchema' = 'true')` | Evolve table schema |
# MAGIC | `COPY_OPTIONS ('force' = 'true')` | Reload files already ingested (breaks idempotency) |
# MAGIC | `VALIDATE [n ROWS]` | Preview / schema check, **no write** |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Auto Loader — schema enforcement and evolution
# MAGIC
# MAGIC API: `spark.readStream.format("cloudFiles")`.
# MAGIC
# MAGIC Two discovery modes:
# MAGIC
# MAGIC | Mode | How files are found | Use when |
# MAGIC |---|---|---|
# MAGIC | **Directory listing** (default) | Lists the prefix | Simple setup, moderate file counts |
# MAGIC | **File notification** | Queue of create events (SQS / Event Grid / Pub/Sub), or UC **file events** | High volume, many files, lower listing cost |
# MAGIC
# MAGIC Newer option: `cloudFiles.useManagedFileEvents` = `true` when file events are enabled on the external location / volume.
# MAGIC
# MAGIC **Batch-style Auto Loader** (what the exam calls batch mode):
# MAGIC
# MAGIC ```python
# MAGIC .trigger(availableNow=True)   # process everything new, then stop
# MAGIC ```
# MAGIC
# MAGIC `once=True` is the older equivalent. Prefer `availableNow`.
# MAGIC
# MAGIC ### Schema modes (`cloudFiles.schemaEvolutionMode`)
# MAGIC
# MAGIC | Mode | New column appears |
# MAGIC |---|---|
# MAGIC | `addNewColumns` | Default if you did **not** provide a schema. Stream fails with `UnknownFieldException` after updating the schema location. **Restart** (Lakeflow Job retry) and it continues |
# MAGIC | `addNewColumnsWithTypeWidening` | Same, plus widens int→long etc. |
# MAGIC | `rescue` | No fail. Extra fields go to `_rescued_data` |
# MAGIC | `failOnNewColumns` | Fail and do **not** update schema. You must fix files or schema |
# MAGIC | `none` | Ignore new columns. Default if you **did** provide a schema |
# MAGIC
# MAGIC `_rescued_data` holds type mismatches, case mismatches, and unexpected columns (JSON blob + file path).
# MAGIC
# MAGIC JSON/CSV infer as **strings** unless `cloudFiles.inferColumnTypes` = `true`. Use `cloudFiles.schemaHints` when you know types.

# COMMAND ----------

from pyspark.sql.functions import col

bronze_al = f"{FQN}.bronze_orders_json"
schema_dir = f"{SCHEMA_LOC}/orders_json"
ckpt = f"{CHECKPOINT}/orders_json"

spark.sql(f"DROP TABLE IF EXISTS {bronze_al}")

def run_autoloader(evolution_mode: str):
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_dir)
        .option("cloudFiles.schemaEvolutionMode", evolution_mode)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints", "order_id INT, customer_id INT, amount DOUBLE")
        .load(ORDERS_JSON)
        .writeStream
        .option("checkpointLocation", ckpt)
        .trigger(availableNow=True)
        .toTable(bronze_al)
    )

q = run_autoloader("addNewColumns")
q.awaitTermination()
print("Auto Loader batch 1 done.")
display(spark.table(bronze_al))

# COMMAND ----------

# MAGIC %md
# MAGIC Add a file with a **new column** `promo_code`. With `addNewColumns`, the first restart updates the schema and the next run lands the data.

# COMMAND ----------

from pyspark.sql.types import *

evolved = [{
    "order_id": 12,
    "customer_id": 104,
    "items": [{"sku": "SKU-TEA", "qty": 1}],
    "ship_to": {"city": "Denver", "country": "US"},
    "amount": 12.5,
    "promo_code": "FALL26",
}]
(spark.createDataFrame(evolved)
    .coalesce(1)
    .write.mode("overwrite")
    .json(f"{ORDERS_JSON}/batch=2"))

# First availableNow after a new field often fails once — that is expected.
try:
    q = run_autoloader("addNewColumns")
    q.awaitTermination()
except Exception as e:
    print("Expected schema-evolution stop. Restarting once.\n", type(e).__name__, str(e)[:400])
    q = run_autoloader("addNewColumns")
    q.awaitTermination()

display(spark.table(bronze_al).select("order_id", "amount", "promo_code", "_rescued_data"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enforcement demo
# MAGIC
# MAGIC `failOnNewColumns` is how you **enforce** a closed schema: unexpected fields fail the stream until you change the schema or remove the file. `rescue` keeps the pipeline up and parks surprises in `_rescued_data`.
# MAGIC
# MAGIC Exam rule of thumb:
# MAGIC - Landing / Bronze, unknown producers → `addNewColumns` or `rescue`
# MAGIC - Silver contract you own → provide a schema + `failOnNewColumns` or `none`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 Lakeflow Connect — UI lab (10 minutes)
# MAGIC
# MAGIC You often cannot finish a managed connector without a Salesforce / SQL Server account. Still open the gallery so the names are familiar.
# MAGIC
# MAGIC 1. Left sidebar → **Data Ingestion** or **Data** → **Lakeflow Connect** (label varies: **Ingestion**, **Add data**).
# MAGIC 2. Open **Add data** from the workspace search box if you do not see it.
# MAGIC 3. Find two groups:
# MAGIC    - **Standard / file connectors** — S3, ADLS, GCS, volume upload, Auto Loader UI
# MAGIC    - **Managed connectors** — Salesforce, ServiceNow, SQL Server, Workday, Google Analytics, …
# MAGIC 4. Click one **managed** connector. Read: source auth, destination catalog/schema, full vs incremental, schedule.
# MAGIC 5. Do **not** save a connection unless you have a real source.
# MAGIC
# MAGIC **Managed connector** = Databricks runs extraction, incremental sync, retries, and writes UC Delta tables.
# MAGIC **Standard connector** = you point Databricks at files / cloud storage; you still own layout and cadence.
# MAGIC
# MAGIC Reliability knobs the exam cares about: incremental cursor/CDC, destination in Unity Catalog, job/pipeline schedule, notifications on failure.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.5 JDBC / ODBC / REST from a notebook
# MAGIC
# MAGIC Production pattern: notebook (or Python task) scheduled by a **Lakeflow Job**.
# MAGIC
# MAGIC JDBC shape (do not run unless you have a database):
# MAGIC
# MAGIC ```python
# MAGIC df = (spark.read.format("jdbc")
# MAGIC       .option("url", "jdbc:postgresql://host:5432/db")
# MAGIC       .option("dbtable", "public.orders")
# MAGIC       .option("user", dbutils.secrets.get("scope", "user"))
# MAGIC       .option("password", dbutils.secrets.get("scope", "password"))
# MAGIC       .option("fetchsize", "10000")
# MAGIC       .load())
# MAGIC df.write.mode("append").saveAsTable(f"{FQN}.bronze_jdbc_orders")
# MAGIC ```
# MAGIC
# MAGIC ODBC is the same idea from BI tools and some partners. In notebooks, prefer the Spark JDBC reader.
# MAGIC
# MAGIC REST — this cell hits a public API and writes a UC table.

# COMMAND ----------

import requests

url = "https://jsonplaceholder.typicode.com/users"
resp = requests.get(url, timeout=30)
resp.raise_for_status()
users = spark.createDataFrame(resp.json())
(users.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{FQN}.bronze_rest_users"))
display(spark.table(f"{FQN}.bronze_rest_users").select("id", "name", "email", "address"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.6 Which ingestion method? (high-value exam table)
# MAGIC
# MAGIC | Situation | Choose |
# MAGIC |---|---|
# MAGIC | Few/medium files in S3/ADLS, SQL-only, incremental by file | **COPY INTO** |
# MAGIC | Many files, growing prefix, schema drift, scale | **Auto Loader** |
# MAGIC | SaaS / enterprise app (Salesforce, SQL Server) | **Lakeflow Connect managed** |
# MAGIC | Files, but you want a UI wizard | Lakeflow Connect **standard** / Add data |
# MAGIC | Database you already can reach, custom query | **JDBC** in a job notebook |
# MAGIC | Public or internal HTTP API | **REST** in a job notebook |
# MAGIC | Complex SaaS + transforms owned by a vendor | **Partner** (Fivetran, Informatica, …) |
# MAGIC | Need UC governance on the destination | All of the above can land in UC — **require** it |
# MAGIC
# MAGIC Volume / frequency cheat sheet:
# MAGIC - Ad-hoc or daily small dumps → COPY INTO
# MAGIC - Continuous or bursty object storage → Auto Loader + file arrival trigger
# MAGIC - Operational systems with APIs/CDC → managed connector
# MAGIC - You must transform during pull → notebook + JDBC/REST + Job

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.7 Semi-structured JSON — explode into a Delta table
# MAGIC
# MAGIC Auto Loader already landed nested `items` and `ship_to`. Flatten for Silver later; here we show the read pattern.

# COMMAND ----------

from pyspark.sql.functions import explode_outer, col

nested = spark.table(f"{FQN}.bronze_orders_json")
flat = (
    nested
    .withColumn("item", explode_outer("items"))
    .select(
        "order_id",
        "customer_id",
        "amount",
        col("ship_to.city").alias("city"),
        col("ship_to.country").alias("country"),
        col("item.sku").alias("sku"),
        col("item.qty").alias("qty"),
    )
)
(flat.write.mode("overwrite")
    .saveAsTable(f"{FQN}.bronze_order_items"))
display(spark.table(f"{FQN}.bronze_order_items"))

# COMMAND ----------

# MAGIC %md
# MAGIC Unstructured files (PDF, images, binary) use Auto Loader `cloudFiles.format` = `binaryFile` or `text`, then store the path / bytes in a Delta table. The exam expects: **land in UC Delta**, do not leave raw files ungoverned.
# MAGIC
# MAGIC ## Self-check
# MAGIC
# MAGIC 1. You re-run `COPY INTO` on the same folder. Do edited old files reload? (**No**, unless `force`)
# MAGIC 2. Auto Loader default discovery mode? (**Directory listing**)
# MAGIC 3. New JSON field, mode `addNewColumns`. What happens first? (**Stream fails after schema update; restart**)
# MAGIC 4. Salesforce → UC Delta, Databricks-operated incremental sync? (**Lakeflow Connect managed**)
# MAGIC 5. 10 million small files arriving all day? (**Auto Loader + file notifications / file events**, not COPY INTO)
