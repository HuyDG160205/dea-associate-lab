# Databricks notebook source
# MAGIC %md
# MAGIC # Fuvekon Mart — Data Engineer Associate lab
# MAGIC
# MAGIC Open this folder on **databricks.com**, attach **Serverless**, then run in order.
# MAGIC
# MAGIC | # | Notebook | Domain |
# MAGIC |---|---|---|
# MAGIC | 1 | [`00_lab_setup`]($./00_lab_setup) | Create schema, volume, sample files |
# MAGIC | 2 | [`01_platform_and_compute`]($./01_platform_and_compute) | Architecture, Delta, Unity Catalog, compute |
# MAGIC | 3 | [`02_ingestion`]($./02_ingestion) | COPY INTO, Auto Loader, REST, Lakeflow Connect |
# MAGIC | 4 | [`03_transform_and_model`]($./03_transform_and_model) | Silver/Gold, joins, DQ, tuning |
# MAGIC | 5 | [`04_lakeflow_jobs`]($./04_lakeflow_jobs) | DAG, retries, if/else, triggers |
# MAGIC | 6 | [`05_cicd`]($./05_cicd) | Git Folders + Automation Bundles |
# MAGIC | 7 | [`06_troubleshoot_optimize`]($./06_troubleshoot_optimize) | Job history, Spark UI, Liquid Clustering |
# MAGIC | 8 | [`07_governance`]($./07_governance) | Managed/external, grants, mask, ABAC |
# MAGIC
# MAGIC Widgets on every notebook: set `catalog` (`workspace` on Free Edition) and `schema` (`dea_lab`).
# MAGIC
# MAGIC Website click path: `docs/UI_WALKTHROUGH.md`  
# MAGIC Exam tables: `docs/EXAM_CHEATSHEET.md`
# MAGIC
# MAGIC Set the widgets below, then run `00_lab_setup`.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

print(f"Ready. Next: open and Run all on 00_lab_setup  (catalog={CATALOG}, schema={SCHEMA})")
