# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Sensor Data Cleaning & Enrichment
# MAGIC
# MAGIC Reads from Bronze, applies:
# MAGIC - Schema enforcement and type casting
# MAGIC - Null handling with business-rule-aware defaults
# MAGIC - Machine metadata join (enrichment)
# MAGIC - Great Expectations data quality checkpoint
# MAGIC - Deduplication using event_id
# MAGIC
# MAGIC Writes to Silver as Delta with Z-ordering on machine_id + event_timestamp.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, coalesce, lit, trim, upper,
    when, isnan, isnull, regexp_replace, sha2, concat_ws,
    current_timestamp
)
from pyspark.sql.types import DoubleType, TimestampType
from delta.tables import DeltaTable
import great_expectations as gx
import logging

logger = logging.getLogger(__name__)

# COMMAND ----------

dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("container_name", "iot-lakehouse")
dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("watermark_start", "")
dbutils.widgets.text("watermark_end", "")

STORAGE_ACCOUNT  = dbutils.widgets.get("storage_account")
CONTAINER        = dbutils.widgets.get("container_name")
PIPELINE_RUN_ID  = dbutils.widgets.get("pipeline_run_id")
WATERMARK_START  = dbutils.widgets.get("watermark_start")
WATERMARK_END    = dbutils.widgets.get("watermark_end")

BASE = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"
BRONZE_PATH  = f"{BASE}/bronze/sensor_events"
SILVER_PATH  = f"{BASE}/silver/sensor_events"
MACHINE_PATH = f"{BASE}/reference/machine_metadata"

# COMMAND ----------

# DBTITLE 1,Read Bronze — only records within watermark window (incremental)
df_bronze = (
    spark.read.format("delta").load(BRONZE_PATH)
    .filter(
        (col("_load_date") >= WATERMARK_START) &
        (col("_load_date") <  WATERMARK_END)
    )
)

logger.info(f"Bronze records in window [{WATERMARK_START}, {WATERMARK_END}): {df_bronze.count():,}")

# COMMAND ----------

# DBTITLE 1,Type casting & schema enforcement
df_typed = (
    df_bronze
    # Timestamp: parse multiple possible formats from IoT devices
    .withColumn("event_timestamp",
        coalesce(
            to_timestamp(col("event_timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
            to_timestamp(col("event_timestamp"), "yyyy-MM-dd HH:mm:ss"),
            to_timestamp(col("event_timestamp"), "dd/MM/yyyy HH:mm:ss"),
        )
    )
    # Numeric: cast and null out unparseable values
    .withColumn("sensor_value", col("sensor_value").cast(DoubleType()))
    # String normalisation
    .withColumn("sensor_type", trim(upper(col("sensor_type"))))
    .withColumn("machine_id",  trim(col("machine_id")))
    .withColumn("line_id",     trim(upper(col("line_id"))))
)

# COMMAND ----------

# DBTITLE 1,Business-rule null handling
# Sensor value nulls: flag as anomaly rather than dropping
# Timestamp nulls: these records are ungroupable — quarantine them
df_nulls_handled = (
    df_typed
    .withColumn("sensor_value",
        when(isnull(col("sensor_value")) | isnan(col("sensor_value")),
             lit(None).cast(DoubleType()))
        .otherwise(col("sensor_value"))
    )
    .withColumn("is_anomaly_flag",
        when(
            isnull(col("sensor_value")) |
            isnan(col("sensor_value")) |
            col("is_anomaly_flag"),
            lit(True)
        ).otherwise(lit(False))
    )
    # Drop records with no parseable timestamp — can't place them in time
    .filter(col("event_timestamp").isNotNull())
)

# COMMAND ----------

# DBTITLE 1,Deduplication — idempotent on event_id
# Use event_id as natural dedup key; fallback surrogate for devices that don't send one
df_deduped = (
    df_nulls_handled
    .withColumn("event_id",
        when(col("event_id").isNull(),
             sha2(concat_ws("|", col("machine_id"), col("event_timestamp"), col("sensor_type")), 256))
        .otherwise(col("event_id"))
    )
    .dropDuplicates(["event_id"])
)

logger.info(f"After dedup: {df_deduped.count():,} records")

# COMMAND ----------

# DBTITLE 1,Enrich with machine metadata dimension
df_machines = spark.read.format("delta").load(MACHINE_PATH)

df_enriched = (
    df_deduped
    .join(
        df_machines.select("machine_id", "machine_name", "machine_type", "plant_id", "capacity_units_per_hour"),
        on="machine_id",
        how="left"
    )
    .withColumn("_silver_processed_at", current_timestamp())
    .withColumn("_pipeline_run_id", lit(PIPELINE_RUN_ID))
)

# COMMAND ----------

# DBTITLE 1,Great Expectations — quality gate before Silver write
# If this checkpoint fails, the notebook exits with error and ADF retries
context = gx.get_context(mode="ephemeral")

datasource = context.sources.add_spark(name="silver_validation", spark_session=spark)
data_asset = datasource.add_dataframe_asset(name="sensor_events_silver")
batch_request = data_asset.build_batch_request(dataframe=df_enriched)

suite = context.add_expectation_suite("sensor_events_silver_suite")

# Completeness
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="event_id"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="machine_id"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="event_timestamp"))

# Value ranges — sensor physics constraints
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
    column="sensor_value", min_value=-500, max_value=10000
))

# Referential integrity — every machine_id should have matched a metadata record
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
    column="machine_name",
    mostly=0.98    # Allow 2% tolerance for newly commissioned machines not yet in metadata
))

# Uniqueness
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="event_id"))

checkpoint = context.add_checkpoint(
    name="silver_checkpoint",
    validations=[{
        "batch_request": batch_request,
        "expectation_suite_name": "sensor_events_silver_suite"
    }]
)

result = checkpoint.run()

if not result["success"]:
    failed = [r for r in result["run_results"].values() if not r["validation_result"]["success"]]
    logger.error(f"GE checkpoint FAILED. {len(failed)} expectation(s) failed.")
    raise Exception(f"Data quality gate failed — Silver write blocked. Pipeline run: {PIPELINE_RUN_ID}")

logger.info("Great Expectations checkpoint PASSED. Proceeding to Silver write.")

# COMMAND ----------

# DBTITLE 1,Write to Silver — merge (upsert) on event_id for idempotency
if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    silver_table = DeltaTable.forPath(spark, SILVER_PATH)

    (silver_table.alias("target")
     .merge(
         df_enriched.alias("source"),
         "target.event_id = source.event_id"
     )
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute()
    )
else:
    # First run — create the table
    (df_enriched.write
     .format("delta")
     .mode("overwrite")
     .partitionBy("line_id")
     .save(SILVER_PATH)
    )

# COMMAND ----------

# DBTITLE 1,Optimise Silver table — Z-order on most common query filters
spark.sql(f"""
    OPTIMIZE delta.`{SILVER_PATH}`
    ZORDER BY (machine_id, event_timestamp)
""")

spark.sql(f"VACUUM delta.`{SILVER_PATH}` RETAIN 168 HOURS")  # 7-day time travel window

# COMMAND ----------

# DBTITLE 1,Register in Unity Catalog
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS manufacturing.silver.sensor_events
    USING DELTA
    LOCATION '{SILVER_PATH}'
    COMMENT 'Cleansed, deduplicated, enriched sensor events — quality-validated before write'
""")

count = spark.read.format("delta").load(SILVER_PATH).count()
logger.info(f"Silver sensor_events total: {count:,}")

dbutils.notebook.exit(f"SUCCESS | Silver records: {count}")
