# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Databricks Intelligence Platform (Domain 1, 6%)
# MAGIC
# MAGIC **Exam asks you to:** name the core pieces, pick compute for a workload, and know cost / limits.
# MAGIC
# MAGIC Run `00_lab_setup` first.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Architecture you must be able to draw
# MAGIC
# MAGIC ```
# MAGIC Control plane (Databricks account / workspace)
# MAGIC   • Workspace UI, Unity Catalog metastore, Jobs, Git Folders
# MAGIC   • You do not store table data here
# MAGIC
# MAGIC Data plane (your cloud account, or Databricks-managed for serverless)
# MAGIC   • Compute: clusters, warehouses, serverless
# MAGIC   • Data: cloud object storage (S3 / ADLS / GCS)
# MAGIC
# MAGIC Delta Lake = the open table format on that storage
# MAGIC Unity Catalog = the governance layer over catalogs → schemas → tables/views/volumes
# MAGIC ```
# MAGIC
# MAGIC ### Delta Lake — memorize these properties
# MAGIC
# MAGIC | Property | What it means on the exam |
# MAGIC |---|---|
# MAGIC | ACID transactions | Concurrent writes do not corrupt the table |
# MAGIC | Time travel | `VERSION AS OF` / `TIMESTAMP AS OF` |
# MAGIC | Schema enforcement | Write fails if it does not match the table schema |
# MAGIC | Schema evolution | `mergeSchema` / Auto Loader `addNewColumns` can add columns |
# MAGIC | Upserts | `MERGE INTO` |
# MAGIC | `_delta_log` | Transaction log of JSON (and checkpoints) next to the data files |
# MAGIC
# MAGIC ### Unity Catalog three-level namespace
# MAGIC
# MAGIC `catalog.schema.table`  (example: `workspace.dea_lab.bronze_orders`)
# MAGIC
# MAGIC Hierarchy of securables: **metastore → catalog → schema → table/view/volume/function/model**.
# MAGIC Privileges inherit downward unless denied. You always need `USE CATALOG` + `USE SCHEMA` to see objects inside.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delta time travel + transaction log (exam favorite)
# MAGIC CREATE OR REPLACE TABLE demo_delta (id INT, note STRING);
# MAGIC INSERT INTO demo_delta VALUES (1, 'first write');
# MAGIC INSERT INTO demo_delta VALUES (2, 'second write');
# MAGIC
# MAGIC DESCRIBE HISTORY demo_delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo_delta VERSION AS OF 1;
# MAGIC -- Compare with: SELECT * FROM demo_delta;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Compute services — pick the right one
# MAGIC
# MAGIC Do this **on the website** while you read (5 minutes):
# MAGIC
# MAGIC 1. Left sidebar → **Compute**.
# MAGIC 2. Note the tabs: **All-purpose**, **Jobs**, **SQL warehouses**, and whether **Serverless** is on.
# MAGIC 3. Click **Create** on each type you have access to, read the form, then cancel. Do not leave extra clusters running.
# MAGIC
# MAGIC | Compute | Interactive notebooks? | Best for | Cost model | Limits to remember |
# MAGIC |---|---|---|---|---|
# MAGIC | **All-purpose cluster** | Yes | Exploration, ad-hoc, shared team cluster | Highest DBU rate × hours × nodes. Stays on until you terminate it | Do **not** use for scheduled production jobs |
# MAGIC | **Jobs compute** (job cluster) | No | Scheduled Lakeflow Jobs / pipelines | Lower DBU rate. Created for the run, then terminated | Cannot attach a notebook to it by hand |
# MAGIC | **Serverless compute** | Yes | Notebooks + jobs without cluster ops | Pay for actual use (DBU-seconds). Cold start possible | Some init scripts / custom images limited |
# MAGIC | **SQL warehouse** (Classic / Pro / Serverless) | SQL Editor, dashboards, BI | SQL / Gold serving / materialized views | Warehouse size (T-shirt) × hours, or serverless usage | Not for arbitrary PySpark / Scala jobs |
# MAGIC | **Lakeflow pipeline compute** | No | Spark Declarative Pipelines (streaming tables, MVs) | Serverless pipelines billed by processing | Create/refresh of MVs and streaming tables run here |
# MAGIC
# MAGIC ### Exam-style choices
# MAGIC
# MAGIC | Workload | Pick |
# MAGIC |---|---|
# MAGIC | Analyst refreshes a Gold dashboard all day | Serverless SQL warehouse |
# MAGIC | Nightly 2-hour ETL notebook, no one watching | Jobs compute (or serverless job) |
# MAGIC | You are debugging Auto Loader in a notebook | All-purpose **or** serverless notebook compute |
# MAGIC | 20 analysts share one always-on cluster | All-purpose, autoscaling, auto-termination 15–30 min |
# MAGIC | CREATE MATERIALIZED VIEW in SQL Editor | Pro or Serverless SQL warehouse |
# MAGIC | Continuous streaming with schema evolution | Jobs compute / serverless job, not an idle all-purpose cluster |
# MAGIC
# MAGIC **Photon** is the vectorized engine on Databricks Runtime. It speeds SQL and many DataFrame ops. Enable it on clusters/warehouses unless you have a reason not to.
# MAGIC
# MAGIC **Auto-termination** stops all-purpose clusters after idle minutes. Always set it. Jobs compute already dies when the job ends.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What you just used
# MAGIC
# MAGIC This notebook is attached to **interactive** compute (serverless or all-purpose). That is correct for learning.
# MAGIC
# MAGIC When you promote the same notebook in Domain 4, you will run it on **Jobs compute** so you stop paying when the DAG finishes.

# COMMAND ----------

print("Current catalog.schema:", spark.sql("SELECT current_catalog(), current_schema()").first())
print("Spark version:", spark.version)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Self-check (say the answer before you look)
# MAGIC
# MAGIC 1. Dropping a **managed** Unity Catalog table deletes the data files. Dropping an **external** table does not. True or false?
# MAGIC 2. You need to run the same notebook every night. All-purpose or jobs compute?
# MAGIC 3. Where does Delta Lake store the transaction log?
# MAGIC 4. Name the three levels of the UC namespace.
# MAGIC
# MAGIC Answers: (1) True. (2) Jobs compute / serverless job. (3) `_delta_log` next to the data. (4) catalog.schema.table.
