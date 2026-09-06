# Data Engineer Associate — decision cheatsheet (May 2026)

Memorize these tables. They map 1:1 to exam "which option" items.

## Domain 1 — Platform (6%)

**Draw:** control plane (UI, UC, jobs) vs data plane (compute + cloud storage). Delta Lake is the format; Unity Catalog is the governance layer.

**Namespace:** `catalog.schema.table`. Privileges need `USE CATALOG` + `USE SCHEMA` + object privilege.

**Delta:** ACID, `_delta_log`, time travel (`VERSION AS OF`), MERGE, schema enforcement, optional evolution.

| Workload | Compute |
|---|---|
| Nightly unattended ETL | Jobs compute or serverless job |
| Shared interactive notebooks | All-purpose, auto-termination on |
| BI / SQL / CREATE MATERIALIZED VIEW | Pro or Serverless SQL warehouse |
| Debug a stream | Serverless or all-purpose notebook |
| Don't leave on overnight | Auto-termination; never use all-purpose for prod schedules |

Cost: all-purpose DBU rate > jobs compute. Serverless = usage. SQL warehouse = T-shirt size × time (or serverless usage).

## Domain 2 — Ingestion (21%)

| Need | Tool |
|---|---|
| Incremental files, SQL, moderate volume | `COPY INTO` |
| Lots of files, drift, scale | Auto Loader (`cloudFiles`) |
| SaaS / SQL Server / Workday | Lakeflow Connect **managed** |
| Wizard onto cloud storage | Lakeflow Connect **standard** |
| Custom SQL on a database | JDBC in a Job notebook |
| HTTP API | REST + Job |
| Vendor-owned pipelines | Partner connector |

**COPY INTO:** idempotent (skip already-loaded files). `force=true` reloads. `mergeSchema` evolves. `VALIDATE` does not write. Source can be `s3://`, `abfss://`, `gs://`, or a Volume.

**Auto Loader modes**

| Discovery | When |
|---|---|
| Directory listing (default) | Simple, fewer files |
| File notification / managed file events | High file count |

Batch drain: `.trigger(availableNow=True)`.

| `schemaEvolutionMode` | New column |
|---|---|
| `addNewColumns` | Update schema, **fail once**, restart (Job retry) |
| `rescue` | `_rescued_data`, no fail |
| `failOnNewColumns` | Fail, schema unchanged |
| `none` | Drop/ignore new fields (default if you supplied a schema) |

JSON/CSV inferred as **strings** unless `inferColumnTypes`. Use `schemaHints`.

## Domain 3 — Transform (22%)

**Medallion:** Bronze = raw land. Silver = cleaned, typed, deduped, conformed. Gold = consumer-ready.

| Gold object | Stored? | Refresh |
|---|---|---|
| Table | Yes | You write it |
| View | No | Every query |
| Materialized view | Yes | Pipeline schedule or `TRIGGER ON UPDATE` |
| Streaming table | Yes | Incremental stream |

**Joins:** inner / left / cross / multi-key. `broadcast(df)` skips shuffle of the small side. `union` distinct; `unionAll` keeps dupes.

**Dedup:** `dropDuplicates`, `row_number` over a window keep `rn = 1`.

**Aggs:** `count`, `countDistinct`, `approx_count_distinct`, `mean`, `summary`.

**Tuning**

| Knob | Effect |
|---|---|
| `spark.sql.shuffle.partitions` | Post-shuffle partition count (default 200) |
| `spark.default.parallelism` | Default parallelism |
| `spark.executor.memory` / `spark.driver.memory` | Cluster form, not a casual serverless conf |
| `spark.sql.autoBroadcastJoinThreshold` | Auto-broadcast size; `-1` off |

**Quality:** filter; Delta `CHECK`; pipeline `EXPECT` / `ON VIOLATION DROP ROW | FAIL UPDATE`; default = warn + metrics.

## Domain 4 — Lakeflow Jobs (16%)

Job = DAG of tasks + trigger + notifications.

**Tasks:** notebook, SQL, dashboard, pipeline, Python, if/else, for each.

**Control:** retries; depends on; run-if; if/else; for-each loop.

| Trigger | Use |
|---|---|
| Scheduled | Fixed SLA clock |
| File arrival | Irregular files in a UC volume / external location |
| Table update | Downstream after a UC table/MV/ST changes (any vs all) |
| Continuous | Always-on stream |
| Manual | Dev |

Debounce file/table triggers with min interval and wait-after-last-change.

## Domain 5 — CI/CD (10%)

| Old name | New name |
|---|---|
| Databricks Repos | Git Folders |
| Databricks Asset Bundles | Declarative Automation Bundles |
| Jobs / Workflows | Lakeflow Jobs |
| Delta Live Tables | Lakeflow Spark Declarative Pipelines |

Git Folder: branch, switch, commit, push, PR.

Bundle: one repo, `targets` + `variables` for dev/test/prod.

```text
databricks bundle validate
databricks bundle deploy -t prod
databricks bundle run dea_fuvekon_etl
```

Prod: service principal `run_as`. Validate in CI; deploy on merge.

## Domain 6 — Monitor / optimize (10%)

**Runs list:** duration vs history, failure rate.

**DAG:** upstream red box blocks children; skipped = condition or failed parent.

**Spark UI stage:** uneven task times = **skew**; large shuffle = wide transform; spill = memory / fat partitions.

**Liquid clustering:** `CLUSTER BY (cols)` or `CLUSTER BY AUTO`. Replaces partition + ZORDER. Change keys without a full rewrite mandate. Cluster with `OPTIMIZE` / `OPTIMIZE FULL`.

**Predictive Optimization:** auto OPTIMIZE/VACUUM on **UC managed** tables. Enable at table, schema, or catalog.

**Won't start:** cloud quota, credentials, init script, UC settings.

**Libraries:** version clash cluster vs `%pip` vs init.

**OOM:** driver (`collect` / `toPandas`); executor (spill then death).

## Domain 7 — Governance (15%)

| | Managed | External |
|---|---|---|
| Path | UC picks | You pick `LOCATION` |
| DROP | Data + metadata | Metadata only |
| PO / auto-optimize | Yes | Limited |

`ALTER TABLE t SET MANAGED` (external Delta). Foreign: `SET MANAGED MOVE|COPY`. Revert: `UNSET MANAGED`.

`GRANT` / `REVOKE` / `DENY` on users, groups, service principals. `DENY` wins. Hierarchy: catalog → schema → table.

Masks and row filters **restrict**; they do not grant. Use `is_account_group_member`.

**ABAC:** `CREATE POLICY` on catalog/schema/table, match **governed tags**. Broader than per-table masks; owners cannot bypass catalog policy. Still need base `GRANT SELECT`.
