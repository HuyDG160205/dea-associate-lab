# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Troubleshooting, monitoring, optimization (Domain 6, 10%)
# MAGIC
# MAGIC Half of this domain is **UI**. Keep Jobs & Pipelines and Spark UI open.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.1 Job run history — find a trend
# MAGIC
# MAGIC 1. **Jobs & Pipelines** → open `dea-fuvekon-etl` (create it in notebook 04 if missing).
# MAGIC 2. Open **Runs** (run history).
# MAGIC 3. Run the job **twice** if you only have one run (`Run now`).
# MAGIC 4. Compare **duration** of run 1 vs run 2. First run is often slower (cluster start, cold cache).
# MAGIC 5. Sort / scan for: longer than usual, failed, skipped tasks.
# MAGIC
# MAGIC Exam language: *compare current execution time against historical baselines*. A sudden jump after a data-size change is a smell (skew, more shuffle, more files).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.2 Pipeline / job health in the DAG
# MAGIC
# MAGIC On a run page:
# MAGIC
# MAGIC | UI signal | Meaning |
# MAGIC |---|---|
# MAGIC | Green task | Succeeded |
# MAGIC | Red task | Failed — open logs |
# MAGIC | Gray / skipped | Condition false or upstream failed and `Run if` blocked it |
# MAGIC | Arrow into a red node | **Upstream blocker** — fix the parent first |
# MAGIC | Duration on each box | Which task dominates runtime |
# MAGIC
# MAGIC Failure rate: Runs list → how many of the last N are red. That is health, not just the latest run.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.3 Spark UI — skew, shuffle, spill
# MAGIC
# MAGIC Run the cell below, then:
# MAGIC
# MAGIC 1. Notebook → **View** → **Spark UI** (or Compute → cluster → Spark UI).
# MAGIC 2. **Jobs** → latest job → a **stage** with a long bar.
# MAGIC 3. Look at:
# MAGIC    - **Shuffle Read / Write** — data moved across the network after `groupBy` / `join` / `repartition`
# MAGIC    - **Spill (Memory/Disk)** — executor RAM filled; data written to local disk
# MAGIC    - Task time **min vs max** — if one task is 10× others, you have **skew**
# MAGIC
# MAGIC | Symptom | Cause | Fix you should name |
# MAGIC |---|---|---|
# MAGIC | One task huge, others tiny | Data skew (hot key) | Salt the key, AQE skew join, filter bad keys, liquid cluster on the join key |
# MAGIC | Huge shuffle read | Wide transform, wrong join type | Broadcast the small side, pre-filter, raise `autoBroadcastJoinThreshold` |
# MAGIC | Disk spill | Executor memory too small or partitions too big | More partitions (`spark.sql.shuffle.partitions`), more executor memory, smaller files |
# MAGIC
# MAGIC **AQE** (adaptive query execution) is on by default on modern DBR: it coalesces shuffle partitions and can convert sort-merge to broadcast.

# COMMAND ----------

from pyspark.sql import functions as F

# Tiny skew demo: one customer_id dominates
skewed = (
    spark.range(0, 20000)
    .withColumn("customer_id", F.when(F.col("id") < 18000, F.lit(101)).otherwise((F.col("id") % 20).cast("int")))
    .withColumn("amount", F.lit(1.0))
)
t0 = __import__("time").time()
display(skewed.groupBy("customer_id").count().orderBy(F.desc("count")))
print(f"skew agg seconds={__import__('time').time() - t0:.2f}")
print("Open Spark UI → this job's stage → compare task duration for customer_id 101 vs others.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.4 Liquid Clustering and Predictive Optimization
# MAGIC
# MAGIC ### Liquid Clustering
# MAGIC
# MAGIC Replaces partitioning + `ZORDER` for most new tables.
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE t (id INT, sku STRING, ts TIMESTAMP) CLUSTER BY (sku);
# MAGIC CREATE TABLE t CLUSTER BY AUTO AS SELECT * FROM src;   -- platform picks keys
# MAGIC ALTER TABLE t CLUSTER BY (sku, customer_id);
# MAGIC OPTIMIZE t;            -- incremental cluster
# MAGIC OPTIMIZE t FULL;       -- rewrite everything
# MAGIC ```
# MAGIC
# MAGIC Why the exam likes it:
# MAGIC - Change cluster keys **without** rewriting historical data immediately
# MAGIC - Better for high-cardinality keys and skew than Hive-style partitions
# MAGIC - Not compatible with `PARTITIONED BY` / `ZORDER` on the same table
# MAGIC - Needs `OPTIMIZE` (or Predictive Optimization) to actually cluster files
# MAGIC
# MAGIC ### Predictive Optimization
# MAGIC
# MAGIC Databricks automatically runs `OPTIMIZE` / `VACUUM` / clustering analysis on **UC managed** tables.
# MAGIC
# MAGIC ```sql
# MAGIC ALTER TABLE workspace.dea_lab.silver_orders ENABLE PREDICTIVE OPTIMIZATION;
# MAGIC -- also: ALTER SCHEMA ... ENABLE PREDICTIVE OPTIMIZATION;
# MAGIC --        ALTER CATALOG ... ENABLE PREDICTIVE OPTIMIZATION;
# MAGIC ```
# MAGIC
# MAGIC You do **not** schedule a nightly OPTIMIZE job if PO is on for that managed table. External tables get little or no PO.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {FQN}.gold_orders_clustered
CLUSTER BY (sku)
AS SELECT * FROM {FQN}.silver_orders
""")

try:
    spark.sql(f"ALTER TABLE {FQN}.gold_orders_clustered ENABLE PREDICTIVE OPTIMIZATION")
    print("Predictive Optimization enabled.")
except Exception as e:
    print("PO may be inherited from catalog/schema or not entitled:\n", str(e)[:300])

display(spark.sql(f"DESCRIBE DETAIL {FQN}.gold_orders_clustered"))
spark.sql(f"OPTIMIZE {FQN}.gold_orders_clustered")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.5 Startup failures, library conflicts, OOM
# MAGIC
# MAGIC ### Cluster / compute will not start
# MAGIC
# MAGIC | Log / UI clue | Likely cause |
# MAGIC |---|---|
# MAGIC | Cloud provider quota / `Insufficient instance capacity` | Cloud limit or AZ outage — pick another node type / zone |
# MAGIC | `Invalid instance profile` / credentials | Instance profile, access connector, or UC storage credential |
# MAGIC | Image / init script exit code ≠ 0 | Bad init script — open **Event log** / init script logs |
# MAGIC | Unity Catalog assignment | Cluster not UC-enabled when the table is UC |
# MAGIC | Serverless "waiting for compute" then fail | Entitlement, region, or budget policy |
# MAGIC
# MAGIC ### Library conflicts
# MAGIC
# MAGIC - Same package, two versions (cluster library + notebook `%pip` + init script)
# MAGIC - Wheel compiled for the wrong Python
# MAGIC - Installing a library that **ships with DBR** and breaks Spark
# MAGIC
# MAGIC Fix: one source of truth; prefer **notebook-scoped** `%pip` for experiments; pin versions on job clusters; read the driver log for `pkg_resources` / `import` errors.
# MAGIC
# MAGIC ### Out of memory
# MAGIC
# MAGIC | Where | Symptom | Fix |
# MAGIC |---|---|---|
# MAGIC | Driver | Notebook dies collecting a big DataFrame | Stop `collect()` / `toPandas()` on large data; `limit`; raise `spark.driver.memory` |
# MAGIC | Executor | Task OOM, or disk spill then fail | More partitions, more executor memory, broadcast smaller side, fix skew |
# MAGIC | SQL warehouse | Query cancelled / memory limit | Smaller result, warehouse size up, avoid huge cross joins |
# MAGIC
# MAGIC Driver OOM classic: `df.toPandas()` on a 20 GB table.

# COMMAND ----------

# Driver-OOM anti-pattern (safe: we limit)
pdf = spark.table(f"{FQN}.silver_orders").limit(20).toPandas()
print("OK — limited toPandas. Never do this on a full fact table.")
print(pdf.head())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Self-check
# MAGIC
# MAGIC 1. One Spark task 30 minutes, 199 tasks 20 seconds — name the issue. (**Skew**)
# MAGIC 2. Stage shows Shuffle spill to disk — first two knobs? (**Memory, partition count / size**)
# MAGIC 3. Liquid clustering vs partition — can you change keys without a full rewrite requirement? (**Yes, keys are flexible; OPTIMIZE incrementally**)
# MAGIC 4. Who runs OPTIMIZE if Predictive Optimization is on? (**Databricks, on managed UC tables**)
# MAGIC 5. Job DAG: task C red, A and B green, arrow A→C — where do you look first? (**C logs, but check A's output contract; C is the failure**)
