# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Data transformation and modeling (Domain 3, 22%)
# MAGIC
# MAGIC Bronze → Silver → Gold for Fuvekon Mart.
# MAGIC
# MAGIC Run `00_lab_setup` and `02_ingestion` first (`bronze_orders_copy`, `bronze_order_items` should exist).

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Clean Bronze → Silver
# MAGIC
# MAGIC Typical Silver rules: drop/fix nulls, cast types, trim strings, drop bad keys.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

customers_raw = spark.read.option("header", True).csv(CUSTOMERS_CSV)

silver_customers = (
    customers_raw
    .dropDuplicates(["customer_id"])
    .withColumn("customer_id", F.col("customer_id").cast("int"))
    .withColumn("email", F.lower(F.trim("email")))
    .withColumn("region", F.lower("region"))
    .withColumn("email", F.when(F.col("email").isNull(), "unknown@fuvekon.com").otherwise(F.col("email")))
    .filter(F.col("customer_id").isNotNull())
)
(silver_customers.write.mode("overwrite").saveAsTable(f"{FQN}.silver_customers"))
display(spark.table(f"{FQN}.silver_customers"))

# COMMAND ----------

silver_orders = (
    spark.table(f"{FQN}.bronze_orders_copy")
    .withColumn("amount", F.col("amount").cast("double"))
    .withColumn("order_ts", F.to_timestamp("order_ts"))
    .withColumn("sku", F.upper(F.trim("sku")))
    .filter(F.col("amount").isNotNull() & (F.col("amount") > 0))
    .filter(F.col("customer_id").isNotNull())
)
(silver_orders.write.mode("overwrite").saveAsTable(f"{FQN}.silver_orders"))
display(spark.table(f"{FQN}.silver_orders"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Combine DataFrames
# MAGIC
# MAGIC Practice every join type the exam names. `broadcast` is a **hint**: send the small side to every executor and skip a shuffle.

# COMMAND ----------

orders = spark.table(f"{FQN}.silver_orders")
customers = spark.table(f"{FQN}.silver_customers")

print("Inner — only matching keys")
display(orders.join(customers, "customer_id", "inner"))

print("Left — keep all orders, orphan customer_id 999 has null customer fields")
display(orders.join(customers, "customer_id", "left"))

print("Broadcast join (small customers dimension)")
display(orders.join(F.broadcast(customers), "customer_id", "left"))

print("Multiple keys (demo: id + region)")
o2 = orders.join(customers, "customer_id").select("order_id", "customer_id", "region", "amount")
c2 = customers.select("customer_id", "region", F.col("name").alias("cust_name"))
display(o2.join(c2, ["customer_id", "region"], "inner"))

print("Cross join — Cartesian. Only for tiny sets.")
display(customers.limit(2).crossJoin(orders.select("sku").distinct()))

print("union = distinct rows; unionAll / unionByName(allowMissingColumns) keeps duplicates")
a = orders.select("order_id", "sku")
b = orders.select("order_id", "sku")
print("union count", a.union(b).distinct().count(), "unionAll count", a.unionAll(b).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 Columns, rows, structure
# MAGIC
# MAGIC add / drop / rename / split / filter / explode

# COMMAND ----------

from pyspark.sql.functions import explode_outer, split, col

shaped = (
    spark.table(f"{FQN}.silver_customers")
    .withColumn("first_name", split("name", " ").getItem(0))
    .withColumn("last_name", split("name", " ").getItem(1))
    .withColumnRenamed("ssn", "tax_id")
    .drop("name")
    .filter(col("region").isin("west", "east"))
)
display(shaped)

print("Explode nested items from JSON bronze")
display(
    spark.table(f"{FQN}.bronze_orders_json")
    .select("order_id", explode_outer("items").alias("item"))
    .select("order_id", col("item.sku"), col("item.qty"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4 Dedup and aggregates

# COMMAND ----------

# Window dedup: keep latest name per customer_id (already done with dropDuplicates)
w = Window.partitionBy("customer_id").orderBy(F.col("name").desc())
deduped = (
    spark.read.option("header", True).csv(CUSTOMERS_CSV)
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)
display(deduped)

gold_agg = (
    spark.table(f"{FQN}.silver_orders")
    .groupBy("sku")
    .agg(
        F.count("*").alias("order_count"),
        F.countDistinct("customer_id").alias("exact_customers"),
        F.approx_count_distinct("customer_id").alias("approx_customers"),
        F.mean("amount").alias("avg_amount"),
        F.sum("amount").alias("revenue"),
    )
)
(gold_agg.write.mode("overwrite").saveAsTable(f"{FQN}.gold_sku_summary"))
display(spark.table(f"{FQN}.gold_sku_summary"))
display(spark.table(f"{FQN}.silver_orders").select("amount").summary())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.5 Basic Spark tuning (then re-measure)
# MAGIC
# MAGIC | Setting | What it does | Starting point |
# MAGIC |---|---|---|
# MAGIC | `spark.sql.shuffle.partitions` | Partitions after a shuffle (joins, groupBy) | 200 default — too many for small data; try `2 × cores` |
# MAGIC | `spark.default.parallelism` | Default RDD partitions | Often `cores × 2` |
# MAGIC | `spark.executor.memory` / `spark.driver.memory` | Heap. Too low → OOM or disk spill | Raise if spill; don't over-allocate |
# MAGIC | `spark.sql.autoBroadcastJoinThreshold` | Max size to auto-broadcast (bytes). `-1` disables | Default 10MB. Raise for a modest dimension table |
# MAGIC
# MAGIC You can set shuffle partitions and broadcast threshold in a notebook. Executor/driver memory is a **cluster** setting (Compute UI), not a runtime `spark.conf` on serverless.
# MAGIC
# MAGIC Run the same aggregation twice and compare times.

# COMMAND ----------

import time

def time_agg(label):
    t0 = time.time()
    (spark.table(f"{FQN}.silver_orders")
        .join(F.broadcast(spark.table(f"{FQN}.silver_customers")), "customer_id")
        .groupBy("region", "sku")
        .count()
        .collect())
    print(f"{label}: {time.time() - t0:.2f}s  shuffle.partitions={spark.conf.get('spark.sql.shuffle.partitions')}")

print("autoBroadcastJoinThreshold =", spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))
time_agg("baseline")

spark.conf.set("spark.sql.shuffle.partitions", "8")
time_agg("shuffle.partitions=8")

spark.conf.set("spark.sql.shuffle.partitions", "200")
time_agg("shuffle.partitions=200 (default-like)")

# COMMAND ----------

# MAGIC %md
# MAGIC After you run this, open **Spark UI** (notebook menu → **View** → **Spark UI**, or cluster → Spark UI) → **SQL / DataFrame** or **Stages**. Note duration, shuffle read, and whether a broadcast join was used. You will do more of this in notebook 06.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.6 Gold layer objects
# MAGIC
# MAGIC | Object | Stored data? | Refresh | Best for |
# MAGIC |---|---|---|---|
# MAGIC | **Table** (Delta) | Yes | You `INSERT`/`MERGE`/overwrite | Batch Gold facts you control |
# MAGIC | **View** | No — stored query | Always current, recomputed each read | Light logic, row/column security wrappers |
# MAGIC | **Materialized view** | Yes, precomputed | Pipeline refresh (schedule or `TRIGGER ON UPDATE`) | BI aggregates, repeated expensive SQL |
# MAGIC | **Streaming table** | Yes | Incremental from a stream / Auto Loader | Always-on or triggered Bronze/Silver ingestion |
# MAGIC
# MAGIC MVs and streaming tables are **Lakeflow Spark Declarative Pipelines** under the hood. Creating an MV in a SQL warehouse / pipeline is required — a classic all-purpose cluster may reject `CREATE MATERIALIZED VIEW`.
# MAGIC
# MAGIC Also see `pipelines/retail_dlt.py` for the Python pipeline form.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {FQN}.gold_west_orders AS
SELECT o.order_id, o.sku, o.amount, c.name, c.region
FROM {FQN}.silver_orders o
JOIN {FQN}.silver_customers c ON o.customer_id = c.customer_id
WHERE c.region = 'west'
""")

print("View (no extra storage):")
display(spark.sql(f"SELECT * FROM {FQN}.gold_west_orders"))

print("Gold table (materialized by you):")
display(spark.table(f"{FQN}.gold_sku_summary"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optional — materialized view (SQL warehouse or serverless pipeline)
# MAGIC
# MAGIC If this fails on your compute, run it in **SQL Editor** attached to a **Serverless SQL warehouse**.

# COMMAND ----------

try:
    spark.sql(f"""
    CREATE OR REPLACE MATERIALIZED VIEW {FQN}.gold_daily_revenue
    COMMENT 'Exam Gold: precomputed revenue'
    AS
    SELECT CAST(order_ts AS DATE) AS order_date,
           SUM(amount) AS revenue,
           COUNT(*) AS orders
    FROM {FQN}.silver_orders
    GROUP BY CAST(order_ts AS DATE)
    """)
    print("Materialized view created. REFRESH MATERIALIZED VIEW when sources change.")
    display(spark.table(f"{FQN}.gold_daily_revenue"))
except Exception as e:
    print("MV not available on this compute. Use a SQL warehouse.\n", str(e)[:500])

# COMMAND ----------

# MAGIC %md
# MAGIC Streaming table shape (run in a pipeline or SQL warehouse):
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE bronze_orders_st AS
# MAGIC SELECT * FROM STREAM read_files(
# MAGIC   '/Volumes/workspace/dea_lab/landing/orders_csv',
# MAGIC   format => 'csv',
# MAGIC   header => true
# MAGIC );
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.7 Data quality on Silver / Gold
# MAGIC
# MAGIC Three layers you should be able to name:
# MAGIC 1. **Filters** in the notebook (what we did: amount > 0).
# MAGIC 2. **Constraints** on Delta (`CHECK`, `NOT NULL`) — fail or reject the write.
# MAGIC 3. **Declarative pipeline expectations** (`CONSTRAINT ... EXPECT ... ON VIOLATION DROP ROW | FAIL UPDATE`).
# MAGIC
# MAGIC Also: count quarantined rows into a `_dq` table so Gold is trustworthy.

# COMMAND ----------

spark.sql(f"""
ALTER TABLE {FQN}.silver_orders
  SET TBLPROPERTIES ('quality.layer' = 'silver');
""")

# Delta CHECK — reject bad writes
try:
    spark.sql(f"""
    ALTER TABLE {FQN}.silver_orders ADD CONSTRAINT amount_pos CHECK (amount > 0)
    """)
except Exception as e:
    print("Constraint may already exist:", str(e)[:200])

bad = spark.createDataFrame([(99, 101, "SKU-X", -5.0)], "order_id INT, customer_id INT, sku STRING, amount DOUBLE")
try:
    bad.write.mode("append").saveAsTable(f"{FQN}.silver_orders")
    print("ERROR: bad row was accepted")
except Exception as e:
    print("Good — constraint blocked the bad append.\n", str(e)[:350])

# Quarantine pattern
src = spark.table(f"{FQN}.bronze_orders_copy").withColumn("amount_num", F.col("amount").cast("double"))
ok = src.filter(F.col("amount_num").isNotNull() & (F.col("amount_num") > 0))
bad_rows = src.subtract(ok)
(bad_rows.write.mode("overwrite").saveAsTable(f"{FQN}.silver_orders_quarantine"))
print("Quarantine rows:", spark.table(f"{FQN}.silver_orders_quarantine").count())

# COMMAND ----------

# MAGIC %md
# MAGIC Pipeline expectation syntax (exam):
# MAGIC
# MAGIC ```sql
# MAGIC CONSTRAINT valid_amount EXPECT (amount > 0) ON VIOLATION DROP ROW
# MAGIC CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE
# MAGIC ```
# MAGIC
# MAGIC Default (no `ON VIOLATION`) = **warn**: keep the row, record metrics in the pipeline event log.
# MAGIC
# MAGIC ## Self-check
# MAGIC
# MAGIC 1. `union` vs `unionAll`? (distinct vs keep dupes)
# MAGIC 2. When broadcast join? (one side smaller than `autoBroadcastJoinThreshold`)
# MAGIC 3. View vs materialized view vs table? (query / precomputed pipeline / you-managed Delta)
# MAGIC 4. `approx_count_distinct` vs `countDistinct`? (faster sketch vs exact)
# MAGIC 5. Shuffle partitions of 200 on a 10 MB dataset — problem? (tiny files, extra scheduling overhead)
