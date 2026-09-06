# Lakeflow Spark Declarative Pipeline (formerly Delta Live Tables)
# Deploy with the bundle or: Jobs & Pipelines → Create → ETL pipeline
# Attach this file, set catalog/schema, choose serverless.

import dlt
from pyspark.sql import functions as F


def _vol() -> str:
    catalog = spark.conf.get("dea.catalog", "workspace")
    schema = spark.conf.get("dea.schema", "dea_lab")
    volume = spark.conf.get("dea.volume", "landing")
    return f"/Volumes/{catalog}/{schema}/{volume}"


@dlt.table(comment="Bronze orders from landing CSV via Auto Loader")
def dlt_bronze_orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{_vol()}/orders_csv")
    )


@dlt.table(comment="Silver orders with quality expectations")
@dlt.expect_or_drop("positive_amount", "CAST(amount AS DOUBLE) > 0")
@dlt.expect_or_fail("has_customer", "customer_id IS NOT NULL")
def dlt_silver_orders():
    return (
        dlt.read_stream("dlt_bronze_orders")
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("order_ts", F.to_timestamp("order_ts"))
        .withColumn("sku", F.upper(F.trim("sku")))
    )


@dlt.table(comment="Gold SKU summary for BI")
def dlt_gold_sku_summary():
    return (
        dlt.read("dlt_silver_orders")
        .groupBy("sku")
        .agg(
            F.count("*").alias("order_count"),
            F.sum("amount").alias("revenue"),
            F.mean("amount").alias("avg_amount"),
        )
    )
