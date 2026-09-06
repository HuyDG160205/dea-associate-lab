# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Governance and security (Domain 7, 15%)
# MAGIC
# MAGIC Unity Catalog: table types, GRANT/REVOKE/DENY, column masks, row filters, ABAC.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.1 Managed vs external tables
# MAGIC
# MAGIC | | **Managed** | **External** |
# MAGIC |---|---|---|
# MAGIC | Who picks the storage path | Unity Catalog (schema/catalog/metastore root) | You (`LOCATION` on a volume or external location) |
# MAGIC | `DROP TABLE` | Deletes **metadata and data files** | Deletes **metadata only** — files remain |
# MAGIC | Predictive Optimization / auto optimize | Full support | Limited |
# MAGIC | Formats | Delta (and Iceberg managed) | Delta, CSV, JSON, Parquet, … |
# MAGIC | Exam default | Prefer managed for new production tables | Legacy data, sharing a path with another tool |
# MAGIC
# MAGIC Convert:
# MAGIC
# MAGIC ```sql
# MAGIC ALTER TABLE ext_t SET MANAGED;     -- external Delta → managed (keeps history)
# MAGIC ALTER TABLE t UNSET MANAGED;       -- managed → external, typically within 14 days
# MAGIC ```
# MAGIC
# MAGIC Foreign (federated) tables use `SET MANAGED MOVE` or `SET MANAGED COPY` — different command. Don't mix them up.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {FQN}.demo_managed
AS SELECT * FROM {FQN}.silver_customers
""")

ext_path = f"{VOL_PATH}/external/demo_external"
spark.sql(f"DROP TABLE IF EXISTS {FQN}.demo_external")
spark.sql(f"""
CREATE TABLE {FQN}.demo_external
USING DELTA
LOCATION '{ext_path}'
AS SELECT * FROM {FQN}.silver_customers
""")

print("Look at Type = MANAGED vs EXTERNAL")
display(spark.sql(f"DESCRIBE EXTENDED {FQN}.demo_managed"))
display(spark.sql(f"DESCRIBE EXTENDED {FQN}.demo_external"))

# COMMAND ----------

# Convert external → managed (needs privileges; skip if your edition blocks it)
try:
    spark.sql(f"ALTER TABLE {FQN}.demo_external SET MANAGED")
    print("Converted to managed. DESCRIBE EXTENDED again and check Type / Location.")
    display(spark.sql(f"SHOW TBLPROPERTIES {FQN}.demo_external"))
except Exception as e:
    print("SET MANAGED not available here (still know the syntax).\n", str(e)[:400])

# COMMAND ----------

# MAGIC %md
# MAGIC ### DROP behavior demo
# MAGIC
# MAGIC We will **not** drop your Silver tables. A scratch external table:
# MAGIC
# MAGIC 1. `DROP TABLE demo_external` — files under the volume folder can still be listed.
# MAGIC 2. `DROP TABLE demo_managed` — UC deletes the managed files.
# MAGIC
# MAGIC Recreate them after you experiment.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.2 Access control — UI and SQL
# MAGIC
# MAGIC Hierarchy: **metastore → catalog → schema → table/volume/function**.
# MAGIC
# MAGIC You need `USE CATALOG` and `USE SCHEMA` **plus** an object privilege (`SELECT`, `MODIFY`, `CREATE TABLE`, …).
# MAGIC
# MAGIC Principals: users, **groups**, **service principals**.
# MAGIC
# MAGIC ### SQL
# MAGIC
# MAGIC ```sql
# MAGIC GRANT USE CATALOG ON CATALOG workspace TO `account users`;
# MAGIC GRANT USE SCHEMA ON SCHEMA workspace.dea_lab TO `account users`;
# MAGIC GRANT SELECT ON TABLE workspace.dea_lab.gold_sku_summary TO `account users`;
# MAGIC GRANT MODIFY ON TABLE workspace.dea_lab.silver_orders TO `data-engineers`;
# MAGIC
# MAGIC REVOKE MODIFY ON TABLE workspace.dea_lab.silver_orders FROM `data-engineers`;
# MAGIC DENY SELECT ON TABLE workspace.dea_lab.silver_customers TO `externals`;
# MAGIC ```
# MAGIC
# MAGIC `DENY` beats an inherited `GRANT`. Exam trick: a user in a group that has SELECT can still be denied personally.
# MAGIC
# MAGIC ### UI
# MAGIC
# MAGIC 1. **Catalog** → table `gold_sku_summary` → **Permissions**.
# MAGIC 2. **Grant** → pick a group → privilege `SELECT`.
# MAGIC 3. Same screen can revoke.
# MAGIC
# MAGIC Practice GRANT to **yourself** so you do not lock out a teammate.

# COMMAND ----------

me = spark.sql("SELECT current_user() AS u").first().u
print("You are:", me)
spark.sql(f"GRANT SELECT ON TABLE {FQN}.gold_sku_summary TO `{me}`")
display(spark.sql(f"SHOW GRANTS ON TABLE {FQN}.gold_sku_summary"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.3 Column masking and row-level security
# MAGIC
# MAGIC Both are **extra restrictions** on top of `SELECT`. They do not grant access.
# MAGIC
# MAGIC ### Table-level (owner attaches to one table)
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REPLACE FUNCTION email_mask(email STRING)
# MAGIC RETURN CASE
# MAGIC   WHEN is_account_group_member('pii_allowed') THEN email
# MAGIC   ELSE CONCAT(LEFT(email, 2), '***@****')
# MAGIC END;
# MAGIC
# MAGIC ALTER TABLE silver_customers ALTER COLUMN email SET MASK email_mask;
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION west_only(region STRING)
# MAGIC RETURN is_account_group_member('admins') OR region = 'west';
# MAGIC
# MAGIC ALTER TABLE silver_customers SET ROW FILTER west_only ON (region);
# MAGIC ```
# MAGIC
# MAGIC Remove with `ALTER TABLE ... ALTER COLUMN email DROP MASK` and `ALTER TABLE ... DROP ROW FILTER`.
# MAGIC
# MAGIC Functions use `is_account_group_member` / `current_user` so two people querying the same table see different rows/values.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {FQN}.email_mask(email STRING)
RETURNS STRING
RETURN CASE
  WHEN is_account_group_member('admins') THEN email
  ELSE concat(left(coalesce(email, ''), 2), '***@masked')
END
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {FQN}.west_only(region STRING)
RETURNS BOOLEAN
RETURN is_account_group_member('admins') OR lower(region) = 'west'
""")

# Apply to a copy so we do not lock you out of silver_customers
spark.sql(f"CREATE OR REPLACE TABLE {FQN}.silver_customers_secured AS SELECT * FROM {FQN}.silver_customers")

try:
    spark.sql(f"ALTER TABLE {FQN}.silver_customers_secured ALTER COLUMN email SET MASK {FQN}.email_mask")
    spark.sql(f"ALTER TABLE {FQN}.silver_customers_secured SET ROW FILTER {FQN}.west_only ON (region)")
    print("Mask + row filter applied. If you are not in 'admins', emails are masked and east rows disappear.")
    display(spark.table(f"{FQN}.silver_customers_secured"))
except Exception as e:
    print("Need table ownership / APPLY MASK privileges.\n", str(e)[:450])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.4 Unity Catalog ABAC policies
# MAGIC
# MAGIC **ABAC** = policies on a **catalog, schema, or table** that match objects by **governed tags**. One policy can cover every new table that gets the tag. Table owners **cannot** peel off a catalog-level ABAC policy.
# MAGIC
# MAGIC Table-level masks/filters: good for a handful of tables. ABAC: org-wide PII rules.
# MAGIC
# MAGIC Needs DBR **16.4+** or serverless, plus governed tags. Syntax to recognize:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE POLICY mask_email
# MAGIC ON SCHEMA workspace.dea_lab
# MAGIC COLUMN MASK workspace.dea_lab.email_mask
# MAGIC TO `account users`
# MAGIC EXCEPT `admins`
# MAGIC FOR TABLES
# MAGIC WHEN has_tag('pii')
# MAGIC MATCH COLUMNS has_tag_value('pii', 'email') AS em
# MAGIC ON COLUMN em;
# MAGIC
# MAGIC CREATE POLICY hide_east
# MAGIC ON SCHEMA workspace.dea_lab
# MAGIC ROW FILTER workspace.dea_lab.west_only
# MAGIC TO analysts
# MAGIC FOR TABLES
# MAGIC MATCH COLUMNS has_tag('geo') AS region
# MAGIC USING COLUMNS (region);
# MAGIC ```
# MAGIC
# MAGIC ABAC also has **GRANT policies** (beta): dynamically grant privileges from tags. Still: ABAC never replaces `GRANT SELECT` — it only adds filters/masks or extra grants on top.

# COMMAND ----------

# Optional: try ABAC if your workspace has it
try:
    spark.sql(f"""
    CREATE OR REPLACE POLICY dea_mask_email
    ON TABLE {FQN}.silver_customers_secured
    COLUMN MASK {FQN}.email_mask
    TO `account users`
    FOR TABLES
    MATCH COLUMNS has_tag('pii') AS em
    ON COLUMN em
    """)
    print("ABAC policy created. Tag columns in Catalog Explorer with a governed tag named pii for it to match.")
except Exception as e:
    print("ABAC CREATE POLICY skipped (preview / privilege / no tags).\n", str(e)[:400])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Catalog Explorer UI (do this)
# MAGIC
# MAGIC 1. Open `silver_customers_secured`.
# MAGIC 2. **Details** — Type, Location, owner.
# MAGIC 3. **Permissions** — compare with `SHOW GRANTS`.
# MAGIC 4. **Lineage** if you ran notebooks 02–03 — you should see Bronze → Silver → Gold.
# MAGIC 5. Column → tag / mask (if the UI offers it).
# MAGIC
# MAGIC ## Self-check
# MAGIC
# MAGIC 1. DROP managed vs DROP external — what is deleted? (**data+metadata vs metadata only**)
# MAGIC 2. `SET MANAGED` vs `SET MANAGED COPY` — which is for a normal external Delta table? (**SET MANAGED**; COPY/MOVE are foreign tables)
# MAGIC 3. User has inherited SELECT, plus a DENY. Can they read? (**No**)
# MAGIC 4. Mask without SELECT — can they see masked data? (**No. Mask does not grant access**)
# MAGIC 5. ABAC vs table mask — who can remove a catalog-level ABAC policy? (**Not the table owner; catalog/policy admin**)
