# Databricks Data Engineer Associate — Hands-On Lab

Practice lab for the **May 2026** Databricks Certified Data Engineer Associate exam. You run it on [databricks.com](https://www.databricks.com) (Free Edition or a paid workspace), not in this repo.

The notebooks generate sample retail data, land it in Unity Catalog volumes, and walk you through the same skills the exam tests: ingestion, medallion transforms, Lakeflow Jobs, Git Folders, Declarative Automation Bundles, Spark UI, and Unity Catalog security.

## What you need

| Requirement | Why |
|---|---|
| A Databricks workspace with **Unity Catalog** | Almost every objective uses UC tables, volumes, and grants |
| **Serverless** compute **or** a cluster on DBR 15.4+ | Auto Loader, liquid clustering, row filters |
| Permission to create a **schema** and **volume** | Lab writes files under `/Volumes/...` |
| Optional: Databricks CLI + GitHub | Domain 5 (bundles and Git Folders) |

**Use [Databricks Free Edition](https://www.databricks.com/learn/free-edition)** if you do not have a company workspace. Classic Community Edition is not enough (no Unity Catalog).

Some features (Lakeflow Connect managed SaaS connectors, ABAC policies, materialized views) need extra entitlements. Those cells are marked **optional** and still teach the exam facts.

## Import this lab (10 minutes)

### Option A — Git Folder (best; also practices Domain 5.1)

1. Open your workspace at `https://<your-workspace>.cloud.databricks.com` (or `*.azuredatabricks.net` / `*.gcp.databricks.com`).
2. Left sidebar → **Workspace** → your user folder → **Create** → **Git Folder**.
3. Repo URL: this project's Git remote, or upload the `dea-associate-lab` folder.
4. After it clones, you will see `notebooks/`, `resources/`, and `databricks.yml`.

If this folder is only on your laptop:

1. Push `dea-associate-lab` to a GitHub repo, **or**
2. In the workspace: **Workspace** → **Import** → upload each `notebooks/*.py` file (Databricks source format).

### Option B — Import files one by one

Workspace → your folder → **Import** → choose each file under `notebooks/`. Keep the file names so `%run ./_config` still works.

## Attach compute

1. Open `notebooks/00_lab_setup`.
2. Connect to **Serverless** (recommended) or an all-purpose cluster (15.4 LTS+).
3. Set the widgets:
   - `catalog` — usually `workspace` on Free Edition, or `main`
   - `schema` — `dea_lab` (created for you)
4. Run all cells in `00_lab_setup`. You should see a volume path and a few landing files.

Then run the numbered notebooks in order.

## Lab map (exam weights)

| Order | Notebook / UI guide | Exam domain | Weight | Time |
|---|---|---|---|---|
| 00 | `00_lab_setup` | Shared catalog, volume, sample files | — | 10 min |
| 01 | `01_platform_and_compute` | 1. Platform | 6% | 25 min |
| 02 | `02_ingestion` | 2. Ingestion | 21% | 60 min |
| 03 | `03_transform_and_model` | 3. Transform & model | 22% | 50 min |
| 04 | `04_lakeflow_jobs` + UI | 4. Lakeflow Jobs | 16% | 40 min |
| 05 | `05_cicd` + `databricks.yml` | 5. CI/CD | 10% | 35 min |
| 06 | `06_troubleshoot_optimize` | 6. Monitor & optimize | 10% | 35 min |
| 07 | `07_governance` | 7. Governance | 15% | 40 min |

Full pass: about **4–5 hours**. A first pass of 00 → 02 → 03 → 04 → 07 covers ~74% of the exam.

## Story of the demo

You are the data engineer for a small retailer, **Fuvekon Mart**.

```
landing volume (CSV + JSON files)
        │
        ▼
   bronze tables   ← COPY INTO, Auto Loader, REST
        │
        ▼
   silver tables   ← clean, join, explode, dedupe
        │
        ▼
   gold objects    ← table, view, materialized view, streaming table
        │
        ▼
 Lakeflow Job DAG  ← retries, if/else, schedule vs file/table trigger
        │
        ▼
 Unity Catalog     ← grants, managed vs external, mask, row filter, ABAC
```

## UI-only practice (cannot be fully scripted)

Do these in the Databricks website after the matching notebook:

1. **Compute** — Compare Serverless, Jobs compute, all-purpose, and a SQL warehouse (notebook 01).
2. **Lakeflow Connect** — Open the connector gallery even if you do not finish a Salesforce/SQL Server connection (notebook 02).
3. **Lakeflow Jobs** — Build the DAG, retries, if/else, and three trigger types (notebook 04).
4. **Git Folder** — Branch, commit, push, open a PR (notebook 05).
5. **Spark UI** — Open a job stage and find shuffle / spill (notebook 06).
6. **Catalog Explorer** — Grants UI, table type, lineage (notebook 07).

Click-by-click steps: [`docs/UI_WALKTHROUGH.md`](docs/UI_WALKTHROUGH.md).

Exam decision rules: [`docs/EXAM_CHEATSHEET.md`](docs/EXAM_CHEATSHEET.md).

## Clean up

Run the last cell in `00_lab_setup` (`DROP SCHEMA ... CASCADE`) when you are done, or keep the schema for more practice.
