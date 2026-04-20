# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — IoT Sensor Event Ingestion
# MAGIC
# MAGIC Lands raw sensor events from Azure Event Hubs into the Bronze Delta table.
# MAGIC No transformations applied — raw data preserved exactly as received.
# MAGIC Schema-on-read; schema enforcement happens at Silver promotion.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, input_file_name,
    to_timestamp, year, month, dayofmonth
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, TimestampType, BooleanType
)
from delta.tables import DeltaTable
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# DBTITLE 1,Configuration — loaded from ADF pipeline parameters
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("container_name", "iot-lakehouse")
dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("load_date", "")

STORAGE_ACCOUNT = dbutils.widgets.get("storage_account")
CONTAINER       = dbutils.widgets.get("container_name")
PIPELINE_RUN_ID = dbutils.widgets.get("pipeline_run_id")
LOAD_DATE       = dbutils.widgets.get("load_date")

BRONZE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/bronze/sensor_events"
CHECKPOINT  = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/_checkpoints/sensor_events_bronze"

logger.info(f"Pipeline run: {PIPELINE_RUN_ID} | Load date: {LOAD_DATE}")

# COMMAND ----------

# DBTITLE 1,Define raw event schema (permissive — capture all fields)
raw_schema = StructType([
    StructField("event_id",         StringType(),    nullable=True),
    StructField("machine_id",       StringType(),    nullable=True),
    StructField("sensor_type",      StringType(),    nullable=True),
    StructField("sensor_value",     DoubleType(),    nullable=True),
    StructField("unit",             StringType(),    nullable=True),
    StructField("event_timestamp",  StringType(),    nullable=True),   # raw string — parsed at Silver
    StructField("line_id",          StringType(),    nullable=True),
    StructField("shift_id",         StringType(),    nullable=True),
    StructField("is_anomaly_flag",  BooleanType(),   nullable=True),
    StructField("firmware_version", StringType(),    nullable=True),
])

# COMMAND ----------

# DBTITLE 1,Read from Event Hubs (streaming) — or ADF-staged files (batch)
# In production: Event Hubs Structured Streaming with autoloader
# In batch backfill mode: ADF stages JSON files to ADLS, autoloader picks up

df_raw = (
    spark.readStream
    .format("cloudFiles")                          # Auto Loader — handles both new files and schema evolution
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", CHECKPOINT + "/schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.maxFilesPerTrigger", 1000)
    .load(f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/landing/sensor_events/")
)

# COMMAND ----------

# DBTITLE 1,Add metadata columns — audit trail for every record
df_with_metadata = (
    df_raw
    .withColumn("_ingested_at",     current_timestamp())
    .withColumn("_pipeline_run_id", lit(PIPELINE_RUN_ID))
    .withColumn("_source_file",     input_file_name())
    .withColumn("_load_date",       lit(LOAD_DATE))
    # Partition columns — derived from event_timestamp if available, else load_date
    .withColumn("_year",  year(current_timestamp()))
    .withColumn("_month", month(current_timestamp()))
    .withColumn("_day",   dayofmonth(current_timestamp()))
)

# COMMAND ----------

# DBTITLE 1,Write to Bronze Delta table — append only, partitioned by date
query = (
    df_with_metadata
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .option("mergeSchema", "true")           # Schema evolution allowed at Bronze
    .partitionBy("_year", "_month", "_day")
    .trigger(availableNow=True)              # Process all available data then stop (batch-style)
    .start(BRONZE_PATH)
)

query.awaitTermination()

logger.info(f"Bronze ingestion complete. Records landed to: {BRONZE_PATH}")

# COMMAND ----------

# DBTITLE 1,Register table in Unity Catalog
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS manufacturing.bronze.sensor_events
    USING DELTA
    LOCATION '{BRONZE_PATH}'
    COMMENT 'Raw IoT sensor events — append only, no transformations applied'
""")

spark.sql(f"""
    ALTER TABLE manufacturing.bronze.sensor_events
    SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true',
        'pipeline_run_id'                  = '{PIPELINE_RUN_ID}'
    )
""")

# COMMAND ----------

# DBTITLE 1,Log record count for monitoring
count = spark.read.format("delta").load(BRONZE_PATH).count()
logger.info(f"Total records in Bronze sensor_events: {count:,}")

dbutils.notebook.exit(f"SUCCESS | Bronze records: {count}")
