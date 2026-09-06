# Databricks website — click path

Do these in order the first time. After that, jump to whatever domain you are weak on.

Workspace URL looks like `https://dbc-xxxx.cloud.databricks.com`, `https://adb-xxxx.azuredatabricks.net`, or `https://xxxx.gcp.databricks.com`.

## 0. Land in the workspace

1. Open [databricks.com](https://www.databricks.com) → **Sign in** → pick your workspace.
2. Confirm the left sidebar shows **Workspace**, **Catalog**, **Jobs & Pipelines** (or **Workflows**), and **Compute**.
3. Top bar: connect a notebook to **Serverless** if asked.

## 1. Import the lab (Domain 5.1 practice)

1. **Workspace** → users → your email.
2. **Create** → **Git Folder** → paste the Git URL → **Create Git Folder**.
3. Or **Import** each `notebooks/*.py` file into one folder named `notebooks`.

## 2. Compute comparison (Domain 1.2)

1. **Compute** → look at **All-purpose**, **Jobs**, **SQL warehouses**.
2. **Create compute** → read node type, Photon, autoscaling, auto-termination → **Cancel**.
3. **SQL warehouses** → **Create** → Classic vs Pro vs Serverless → **Cancel**.
4. Exam pick: nightly ETL = **Jobs / serverless job**; BI = **SQL warehouse**; debugging = **all-purpose or serverless notebook**.

## 3. Catalog + volume (Domains 1.1, 2.x)

1. Run `notebooks/00_lab_setup`.
2. **Catalog** → your catalog → `dea_lab` → **Volumes** → `landing`.
3. Browse `orders_csv` and `orders_json`. This is the exam's "cloud object storage" stand-in.

## 4. Lakeflow Connect gallery (Domain 2.4)

1. Search (Ctrl/Cmd+P or the search box) for **Add data** or **Lakeflow Connect**.
2. Open a **managed** connector (Salesforce / SQL Server). Read auth, destination catalog, schedule.
3. Open a **file / cloud storage** (standard) connector.
4. Leave without saving unless you have a real source.

## 5. Build the job DAG (Domains 4 and 6)

1. **Jobs & Pipelines** → **Create** → **Job** → name `dea-fuvekon-etl`.
2. Add notebook tasks: `02_ingestion` → `03_transform_and_model` → `04_lakeflow_jobs`.
3. Set **Depends on** so they form a line.
4. Task **Retries** = 2 on `ingest`.
5. Add **If/else** on `{{tasks.quality_check.values.failed_rows}}` == `0`.
6. **Run now**. Click red/green boxes. Open a failed task's log.
7. **Runs** tab: compare two run durations (Domain 6.1).
8. **Schedules & Triggers**: add Scheduled, then File arrival on the volume, then Table update on `silver_orders`. Test connection. Remove triggers when done.

## 6. Declarative pipeline (Domains 3.6, 3.7)

1. **Jobs & Pipelines** → **Create** → **ETL pipeline** / **Pipeline**.
2. Source file: `pipelines/retail_dlt.py`.
3. Destination: same catalog/schema. **Serverless**.
4. **Start**. Open the graph: bronze → silver → gold. Click an expectation.

## 7. Git Folder workflow (Domain 5.1)

1. Open the Git Folder → branch control.
2. Create `practice/dea-lab`, edit a cell, **Commit**, **Push**, **Create pull request**.
3. Switch back to `main`.

## 8. Spark UI (Domain 6.3)

1. Run the skew cell in `06_troubleshoot_optimize`.
2. **View** → **Spark UI** → **Stages**.
3. Find shuffle bytes, spill, and uneven task times.

## 9. Catalog permissions (Domain 7)

1. Catalog Explorer → `gold_sku_summary` → **Permissions** → Grant `SELECT` to yourself.
2. Same table → **Lineage**.
3. `DESCRIBE EXTENDED` on `demo_managed` vs `demo_external` (notebook 07).

## 10. SQL Editor for materialized views

1. **SQL Editor** → attach a **Serverless SQL warehouse**.
2. Run `CREATE OR REPLACE MATERIALIZED VIEW ...` from notebook 03 if the notebook compute rejected it.
