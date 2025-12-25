# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Live Tables Pipeline: Medallion Architecture
# MAGIC
# MAGIC **Databricks Delta Live Tables (DLT) Pipeline**
# MAGIC
# MAGIC This pipeline uses Databricks' native DLT framework to implement the medallion architecture:
# MAGIC - **Bronze Layer**: Streaming ingestion from landing zone
# MAGIC - **Silver Layer**: Cleaned and normalized data with quality expectations
# MAGIC - **Gold Layer**: Analytics-ready aggregations and technical indicators
# MAGIC
# MAGIC **DLT Benefits**:
# MAGIC - Automatic dependency management
# MAGIC - Built-in data quality expectations
# MAGIC - Automatic retries and error handling
# MAGIC - Built-in monitoring and observability
# MAGIC - Declarative pipeline definitions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import DLT and Dependencies

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
import sys
from pathlib import Path

# Add project root to path
def add_project_root_to_path():
    """Add project root to Python path for imports."""
    project_root = None
    current = Path.cwd()
    max_depth = 15
    depth = 0
    while depth < max_depth:
        if (current / "src").exists() and (current / "notebooks").exists():
            project_root = current
            break
        if current.parent == current:
            break
        current = current.parent
        depth += 1
    
    if project_root:
        project_root_str = str(project_root.resolve())
        if project_root_str not in sys.path:
            sys.path.insert(0, project_root_str)
        return project_root_str
    return None

project_root = add_project_root_to_path()

from src.config import DATABRICKS_PATHS
from src.schemas import BRONZE_BARS_SCHEMA, SILVER_BARS_SCHEMA, GOLD_DAILY_OHLCV_SCHEMA, GOLD_ANALYTICS_SCHEMA

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer: Streaming Ingestion

# COMMAND ----------

@dlt.table(
    name="bronze_bars",
    comment="Bronze layer: Raw market data with minimal transformation",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "symbol,timestamp"
    }
)
@dlt.expect_or_drop("valid_symbol", "symbol IS NOT NULL AND symbol != ''")
@dlt.expect_or_drop("valid_timestamp", "timestamp IS NOT NULL")
def bronze_bars():
    """
    Bronze table: Streams raw JSON files from landing zone.
    Uses Auto Loader for automatic file detection and schema evolution.
    """
    raw_bars_path = DATABRICKS_PATHS["raw_bars_path"]
    checkpoint_path = f"{raw_bars_path}/_checkpoints/bronze_dlt"
    
    # Stream from landing zone using Auto Loader
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("pathGlobFilter", "bars_*.json")
        .load(raw_bars_path)
        .withColumn(
            "batch_id",
            F.regexp_extract(F.input_file_name(), r"bars_(\d{8})\.json", 1)
        )
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .select(
            F.col("symbol").cast("string"),
            F.to_timestamp(F.col("timestamp")).alias("timestamp"),
            F.col("open").cast("double"),
            F.col("high").cast("double"),
            F.col("low").cast("double"),
            F.col("close").cast("double"),
            F.col("volume").cast("bigint"),
            F.col("ingestion_timestamp").cast("timestamp"),
            F.col("batch_id").cast("string")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer: Cleaned and Normalized

# COMMAND ----------

@dlt.table(
    name="silver_bars",
    comment="Silver layer: Cleaned, deduplicated, and quality-scored data",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "symbol,timestamp"
    }
)
@dlt.expect("valid_ohlcv", "open IS NOT NULL AND high IS NOT NULL AND low IS NOT NULL AND close IS NOT NULL AND volume IS NOT NULL")
@dlt.expect("valid_prices", "open > 0 AND high > 0 AND low > 0 AND close > 0 AND volume >= 0")
@dlt.expect("price_consistency", "high >= low AND high >= close AND high >= open AND low <= close AND low <= open")
def silver_bars():
    """
    Silver table: Transforms bronze data with cleaning, deduplication, and quality scoring.
    Uses DLT expectations for data quality validation.
    """
    from src.transforms import clean_bronze_to_silver
    
    # Read from bronze DLT table
    bronze_df = dlt.read("bronze_bars")
    
    # Apply transformation
    silver_df = clean_bronze_to_silver(bronze_df)
    
    return silver_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer: Daily OHLCV Aggregation

# COMMAND ----------

@dlt.table(
    name="gold_daily_ohlcv",
    comment="Gold layer: Daily OHLCV aggregations from intraday bars",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "symbol,trade_date"
    }
)
@dlt.expect("valid_daily_data", "open > 0 AND high > 0 AND low > 0 AND close > 0 AND volume >= 0")
@dlt.expect("daily_price_consistency", "high >= low AND high >= close AND high >= open AND low <= close AND low <= open")
def gold_daily_ohlcv():
    """
    Gold table: Aggregates intraday bars to daily OHLCV per symbol.
    """
    from src.transforms import aggregate_to_daily_ohlcv
    
    # Read from silver DLT table (only valid records)
    silver_df = dlt.read("silver_bars").filter(F.col("is_valid") == 1)
    
    # Aggregate to daily
    daily_df = aggregate_to_daily_ohlcv(silver_df)
    
    return daily_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer: Analytics and Technical Indicators

# COMMAND ----------

@dlt.table(
    name="gold_analytics",
    comment="Gold layer: Technical indicators and analytics metrics",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "symbol,trade_date"
    }
)
def gold_analytics():
    """
    Gold table: Calculates technical indicators from daily OHLCV data.
    """
    from src.transforms import calculate_technical_indicators
    
    # Read from gold daily OHLCV DLT table
    daily_df = dlt.read("gold_daily_ohlcv")
    
    # Calculate technical indicators
    analytics_df = calculate_technical_indicators(daily_df)
    
    return analytics_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Complete
# MAGIC
# MAGIC This DLT pipeline automatically:
# MAGIC - Manages dependencies between tables
# MAGIC - Validates data quality using expectations
# MAGIC - Handles errors and retries
# MAGIC - Provides monitoring and observability
# MAGIC - Optimizes table performance with auto-optimization

