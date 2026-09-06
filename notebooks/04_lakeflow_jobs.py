# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Lakeflow Jobs (Domain 4, 16%)
# MAGIC
# MAGIC This notebook is a **job task** you attach in the UI. The markdown below is the click path. Build the job on the website — that is what the exam tests.
# MAGIC
# MAGIC Run `00`–`03` at least once so tables exist.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Control flow you must be able to configure
# MAGIC
# MAGIC | Feature | Where in the Job UI | Exam meaning |
# MAGIC |---|---|---|
# MAGIC | **Retries** | Task → Retries, and/or Job → Retry | Re-run a failed task N times with optional delay. Also used after Auto Loader `addNewColumns` fails once |
# MAGIC | **If / else** (condition task) | Add task → **If/else condition** | Branch on a boolean or on `{{tasks.x.values.y}}` |
# MAGIC | **For each** (loop) | Add task → **For each** | Run a nested task per item in an array (date list, region list, file list) |
# MAGIC | **Depends on** | Task graph edges | DAG: B waits for A; C waits for A and B |
# MAGIC | **Run if** | Task → Run if | `All succeeded` / `At least one succeeded` / `None failed` / `All done` |
# MAGIC
# MAGIC ### Build this DAG
# MAGIC
# MAGIC ```
# MAGIC ingest_copy (notebook 02 or this ingest cell)
# MAGIC        │
# MAGIC        ▼
# MAGIC transform_silver (notebook 03)
# MAGIC        │
# MAGIC        ▼
# MAGIC quality_check ──if failed_rows == 0──► publish_gold
# MAGIC        │
# MAGIC        └──else──► notify_quarantine
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Create the job in the UI (do this now)
# MAGIC
# MAGIC 1. Left sidebar → **Jobs & Pipelines** (sometimes **Workflows**).
# MAGIC 2. **Create** → **Job**.
# MAGIC 3. Job name: `dea-fuvekon-etl`.
# MAGIC 4. First task:
# MAGIC    - **Type:** Notebook
# MAGIC    - **Source:** Workspace (path to this folder) or Git
# MAGIC    - **Path:** `dea-associate-lab/notebooks/02_ingestion`
# MAGIC    - **Compute:** Serverless or a **Job cluster** (not all-purpose)
# MAGIC    - Task name: `ingest`
# MAGIC    - Parameters: `catalog` = your catalog, `schema` = `dea_lab`
# MAGIC    - **Retries:** 2
# MAGIC 5. **Add task** → Notebook → `03_transform_and_model` → name `transform` → **Depends on** `ingest`.
# MAGIC 6. **Add task** → Notebook → this notebook (`04_lakeflow_jobs`) → name `quality_check` → Depends on `transform`.
# MAGIC 7. **Add task** → **If/else condition**
# MAGIC    - Condition: `{{tasks.quality_check.values.failed_rows}}` `==` `0`
# MAGIC    - True path: **SQL** task `SELECT * FROM workspace.dea_lab.gold_sku_summary` or notebook cell that just prints "publish ok"
# MAGIC    - False path: notebook / SQL that selects from `silver_orders_quarantine`
# MAGIC 8. Optional: **Add task** → **For each**
# MAGIC    - Input: `["west", "east"]`
# MAGIC    - Nested notebook that filters `silver_customers` by `{{input}}`
# MAGIC 9. Optional extra task types to click (you do not have to run them):
# MAGIC    - **SQL** query
# MAGIC    - **Dashboard** refresh
# MAGIC    - **Pipeline** (Lakeflow Spark Declarative Pipeline)
# MAGIC
# MAGIC Save. You now have a DAG. Click **Run now**.
# MAGIC
# MAGIC Watch: task boxes turn green/red; click a task for logs. That graph is what Domain 6 asks you to read.

# COMMAND ----------

# This cell is the quality_check task. It sets a job task value the If/else can read.

failed = spark.table(f"{FQN}.silver_orders_quarantine").count()
dbutils.jobs.taskValues.set(key="failed_rows", value=str(failed))
print(f"failed_rows={failed}")
if failed:
    print("Quality gate failed — inspect silver_orders_quarantine")
else:
    print("Quality gate passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 Trigger types
# MAGIC
# MAGIC Job details pane → **Schedules & Triggers** → **Add trigger**.
# MAGIC
# MAGIC | Trigger | Fires when | Use |
# MAGIC |---|---|---|
# MAGIC | **Scheduled** (time-based) | Cron / quartz schedule | Nightly batch, predictable SLA |
# MAGIC | **File arrival** | New files in a UC **volume** or **external location** | Irregular dumps; pair with Auto Loader |
# MAGIC | **Table update** | A UC table / MV / streaming table changes | Downstream Gold after Silver lands |
# MAGIC | **Continuous** | When the previous run ends | Always-on streaming |
# MAGIC | **None / manual** | Run now / API | Dev |
# MAGIC
# MAGIC File arrival extra knobs:
# MAGIC - **Minimum time between triggers** — do not stampede on every object
# MAGIC - **Wait after last change** — debounce a burst of files into one run
# MAGIC
# MAGIC Table update: monitor one or more tables; fire when **any** or **all** have updated.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4 Time-based vs data-driven
# MAGIC
# MAGIC | Question | Pick |
# MAGIC |---|---|
# MAGIC | Vendor drops a file any time between 01:00 and 06:00 | **File arrival** (not a 02:00 cron that might miss it) |
# MAGIC | Finance needs a report on the desk at 07:00 whether or not files moved | **Scheduled**, plus a quality task that fails if data is stale |
# MAGIC | Gold must run only after `silver_orders` MERGE finishes | **Table update** on `silver_orders` |
# MAGIC | Two upstream tables, job needs both | Table update → wait for **all** |
# MAGIC | Auto Loader already running continuously | You may not need a second trigger; or use **continuous** job |
# MAGIC
# MAGIC Practice in the UI (you can delete the trigger after):
# MAGIC 1. Add a **Scheduled** trigger: every day 07:00 in your timezone. Save. Remove it.
# MAGIC 2. Add **File arrival** on `/Volumes/<catalog>/dea_lab/landing/orders_csv`. **Test connection**. Remove it.
# MAGIC 3. Add **Table update** on `silver_orders`. Remove it.
# MAGIC
# MAGIC Leaving a file-arrival trigger on will start real runs when you write more landing files.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Self-check
# MAGIC
# MAGIC 1. Auto Loader `addNewColumns` fails once on purpose. What job setting recovers it? (**Task retries**)
# MAGIC 2. Job should not start until files exist. Scheduled or file arrival? (**File arrival**)
# MAGIC 3. What visual shows that `publish_gold` is blocked by `quality_check`? (**The DAG / task graph**)
# MAGIC 4. Name three task types. (**Notebook, SQL, pipeline, dashboard, if/else, for each, Python, dbt, …**)
