# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — CI/CD (Domain 5, 10%)
# MAGIC
# MAGIC Work in the **website** for Git Folders. Use a terminal for the Databricks CLI + bundle commands.
# MAGIC
# MAGIC This notebook is the study script. The deployable project is the `databricks.yml` next to these notebooks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 Git Folders (formerly Databricks Repos)
# MAGIC
# MAGIC Do this in the workspace (15 minutes):
# MAGIC
# MAGIC 1. **Workspace** → your user home → **Create** → **Git Folder**.
# MAGIC 2. Connect GitHub / Azure DevOps / GitLab. Paste the repo URL that contains `dea-associate-lab`.
# MAGIC 3. After it clones, click the repo folder → the Git icon (branch name in the header).
# MAGIC 4. **Create branch** `practice/dea-lab` from `main`.
# MAGIC 5. Open any notebook, add a markdown cell `# my notes`, save.
# MAGIC 6. Git panel → **Commit** → message `docs: practice git folder commit` → **Push**.
# MAGIC 7. **Create pull request** — Databricks opens the Git host. Open the PR, then you can close it without merging if this is only practice.
# MAGIC 8. **Switch branch** back to `main`.
# MAGIC
# MAGIC Exam verbs: create branch, switch branch, commit, push, create PR. All of that is Git Folders, not the old "Repos" name (same feature).
# MAGIC
# MAGIC Job tasks can run **from Git** (a branch or tag) instead of a workspace path. That is how prod jobs stay pinned to `main` or a release tag.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 Environment-specific config with Declarative Automation Bundles
# MAGIC
# MAGIC **Declarative Automation Bundles** are the new name for **Databricks Asset Bundles**. YAML still lives in `databricks.yml`.
# MAGIC
# MAGIC Pattern: **one codebase**, many **targets**.
# MAGIC
# MAGIC | Idea | How it shows up |
# MAGIC |---|---|
# MAGIC | `variables` | Catalog, schema, warehouse id, job schedule |
# MAGIC | `targets.dev` | Default, often `mode: development` (prefix resources, pause schedules) |
# MAGIC | `targets.test` / `prod` | Overrides: different workspace `host`, catalog, `run_as` service principal, unpause schedule |
# MAGIC
# MAGIC Open `databricks.yml` in this project. Notice:
# MAGIC
# MAGIC - `variables.catalog` default `workspace`
# MAGIC - `targets.dev` keeps the job paused / development mode
# MAGIC - `targets.prod` overrides catalog to `prod` and can set a schedule
# MAGIC
# MAGIC You never copy notebooks per environment. You deploy the same bundle with `-t dev` or `-t prod`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.3 What a bundle packages
# MAGIC
# MAGIC A bundle can include:
# MAGIC - Lakeflow **Jobs**
# MAGIC - Lakeflow Spark **Declarative Pipelines**
# MAGIC - Notebooks and Python files (synced to the workspace)
# MAGIC - Other workspace assets (dashboards, schemas, …)
# MAGIC
# MAGIC This lab's bundle deploys `dea-fuvekon-etl` and optionally the `retail` pipeline in `pipelines/retail_dlt.py`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.4 Databricks CLI — memorize the lifecycle
# MAGIC
# MAGIC Install once on your laptop: [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html)
# MAGIC
# MAGIC ```bash
# MAGIC databricks auth login --host https://<workspace-url>
# MAGIC cd dea-associate-lab
# MAGIC
# MAGIC databricks bundle validate
# MAGIC databricks bundle validate -t prod
# MAGIC
# MAGIC databricks bundle deploy          # default target (dev)
# MAGIC databricks bundle deploy -t prod
# MAGIC
# MAGIC databricks bundle run dea_fuvekon_etl
# MAGIC databricks bundle run -t prod dea_fuvekon_etl
# MAGIC
# MAGIC databricks bundle destroy -t dev  # cleanup
# MAGIC ```
# MAGIC
# MAGIC | Command | Purpose |
# MAGIC |---|---|
# MAGIC | `bundle validate` | Parse YAML, interpolate variables, catch errors **before** deploy |
# MAGIC | `bundle deploy` | Sync files + create/update jobs and pipelines in that target's workspace |
# MAGIC | `bundle run` | Trigger a job or pipeline defined in the bundle |
# MAGIC | `bundle destroy` | Remove deployed resources |
# MAGIC | `bundle summary` | Show what would exist / IDs after deploy |
# MAGIC
# MAGIC CI/CD mapping:
# MAGIC - PR pipeline: `databricks bundle validate -t test`
# MAGIC - Merge to main: `databricks bundle deploy -t prod` as a **service principal**
# MAGIC - Do not deploy from your laptop to prod as yourself if the exam asks for a production pattern — use a SP + `run_as`
# MAGIC
# MAGIC If you cannot install the CLI today, still **read** `databricks.yml` and `resources/dea_job.yml` so you recognize the keys on a screenshot question.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Self-check
# MAGIC
# MAGIC 1. Old name of Git Folders? (**Databricks Repos**)
# MAGIC 2. Old name of Declarative Automation Bundles? (**Databricks Asset Bundles / DABs**)
# MAGIC 3. Same job, different catalog in prod — copy the YAML or use a target variable override? (**Target / variable override**)
# MAGIC 4. Command that does not change the workspace? (`databricks bundle validate`)
# MAGIC 5. Who should `run_as` in prod? (**Service principal**)
