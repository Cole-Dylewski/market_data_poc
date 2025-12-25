# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: Analytics and Aggregations
# MAGIC
# MAGIC **Production Pipeline - Step 4: Gold Analytics**
# MAGIC
# MAGIC This notebook creates two Gold layer tables:
# MAGIC 1. **Daily OHLCV**: Aggregates intraday 5-minute bars to daily OHLCV per symbol
# MAGIC 2. **Analytics**: Calculates technical indicators (SMAs, volatility) from daily data
# MAGIC
# MAGIC **Part 1**: Daily OHLCV Aggregation
# MAGIC - Reads from Silver (only valid records)
# MAGIC - Groups by symbol and date
# MAGIC - Aggregates OHLCV (first open, max high, min low, last close, sum volume)
# MAGIC - Calculates metrics (daily_return, price_range, avg_price)
# MAGIC
# MAGIC **Part 2**: Technical Indicators
# MAGIC - Reads from gold.daily_ohlcv
# MAGIC - Calculates SMAs (5-day, 20-day, 50-day)
# MAGIC - Calculates volatility (stddev of returns over 20-day window)

# COMMAND ----------

import sys
import os
from pathlib import Path
from typing import Optional
from datetime import datetime

# Add project root to Python path for imports
def add_project_root_to_path() -> str:
    """Add project root directory to sys.path for imports.
    
    Works in Databricks Repos, Workspace files, and local environments.
    """
    project_root: Optional[Path] = None
    
    # Method 1: Try using __file__ (works when running as a Python script)
    try:
        if '__file__' in globals():
            notebook_dir = Path(__file__).parent
            if (notebook_dir / '04_gold_analytics.py').exists():
                project_root = notebook_dir.parent
    except (NameError, AttributeError):
        pass
    
    # Method 2: Try using dbutils (Databricks-specific)
    if project_root is None:
        try:
            import dbutils
            try:
                workspace_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
            except:
                try:
                    workspace_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
                except:
                    workspace_path = None
            
            if workspace_path:
                if workspace_path.startswith('/'):
                    parts = workspace_path.lstrip('/').split('/')
                    if len(parts) >= 2:
                        if parts[0] == 'Workspace' and parts[1] == 'Repos':
                            if len(parts) >= 4:
                                repo_path = Path('/Workspace/Repos').joinpath(*parts[2:-1])
                                if (repo_path / "src").exists():
                                    project_root = repo_path
                        elif parts[0] == 'Workspace' and parts[1] == 'Users':
                            if len(parts) >= 4:
                                user_path = Path('/Workspace/Users').joinpath(*parts[2:-1])
                                if (user_path / "src").exists():
                                    project_root = user_path
        except Exception as e:
            print(f"Note: Could not use dbutils path detection: {e}")
            pass
    
    # Method 3: Try current working directory and walk up
    if project_root is None:
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
    
    # Method 4: Try common Databricks paths
    if project_root is None:
        common_paths = [
            Path("/Workspace/Repos"),
            Path("/Workspace/Users"),
            Path.cwd(),
        ]
        for base_path in common_paths:
            if base_path.exists():
                for item in base_path.iterdir():
                    if item.is_dir() and (item / "src").exists() and (item / "notebooks").exists():
                        project_root = item
                        break
                if project_root:
                    break
    
    if project_root is None:
        raise RuntimeError(
            "Could not determine project root directory. "
            "Please ensure you're running from within the project directory, "
            "or set the project root manually."
        )
    
    project_root_str = str(project_root.resolve())
    if project_root_str not in sys.path:
        sys.path.insert(0, project_root_str)
    
    print(f"Project root detected: {project_root_str}")
    return project_root_str

project_root = add_project_root_to_path()
print(f"Added project root to path: {project_root}")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_trunc, lit, max as spark_max
from src.config import DATABRICKS_PATHS
from src.transforms import (
    aggregate_to_daily_ohlcv,
    calculate_technical_indicators,
    get_new_dates_for_gold
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Initialize Spark Session

# COMMAND ----------

spark = SparkSession.builder \
    .appName("Gold Analytics") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
    .config("spark.databricks.delta.autoCompact.enabled", "true") \
    .config("spark.databricks.io.cache.enabled", "true") \
    .getOrCreate()

print("Spark session initialized with Delta Lake and Databricks optimizations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Daily OHLCV Aggregation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1.1: Read Silver Data

# COMMAND ----------

silver_table = DATABRICKS_PATHS["silver_bars_table"]
daily_ohlcv_table = DATABRICKS_PATHS["gold_daily_ohlcv_table"]

# Check if silver table exists
try:
    silver_df = spark.table(silver_table)
    silver_count = silver_df.count()
    valid_count = silver_df.filter(col("is_valid") == 1).count()
    print(f"[OK] Silver table exists: {silver_table}")
    print(f"     Total rows: {silver_count}")
    print(f"     Valid rows: {valid_count}")
except Exception as e:
    raise ValueError(f"Silver table {silver_table} does not exist or cannot be accessed: {e}")

if valid_count == 0:
    print("\n=== No valid data in Silver ===")
    print("Gold transformation cannot proceed without valid Silver data")
    dbutils.notebook.exit("ERROR: No valid data in Silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1.2: Determine Incremental Processing

# COMMAND ----------

# Get new dates to process (dates in Silver but not in Gold)
try:
    new_dates_df = get_new_dates_for_gold(spark, daily_ohlcv_table, silver_table)
    new_dates_count = new_dates_df.count()
    
    if new_dates_count > 0:
        print(f"[INFO] Found {new_dates_count} new symbol-date combinations to process")
        # Filter silver to only new dates
        new_dates_df.createOrReplaceTempView("new_dates")
        silver_df = silver_df.join(
            new_dates_df.alias("nd"),
            (col("symbol") == col("nd.symbol")) &
            (date_trunc("day", col("timestamp")) == col("nd.trade_date")),
            how="inner"
        ).select(silver_df["*"])
    else:
        print(f"[INFO] No new dates to process")
        # Check if table exists - if not, process all
        if spark.catalog.tableExists(daily_ohlcv_table):
            print("Gold daily_ohlcv table is up to date")
            dbutils.notebook.exit("SUCCESS: No new dates to process")
        else:
            print("Gold daily_ohlcv table does not exist. Processing all dates.")
except Exception as e:
    print(f"[WARN] Could not determine incremental strategy: {e}")
    print("       Processing all valid Silver data")

silver_count_to_process = silver_df.filter(col("is_valid") == 1).count()
print(f"     Rows to process: {silver_count_to_process}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1.3: Aggregate to Daily OHLCV

# COMMAND ----------

print("\n=== Aggregating to Daily OHLCV ===")
print(f"Input rows: {silver_count_to_process}")

# Aggregate to daily
daily_df = aggregate_to_daily_ohlcv(silver_df)

daily_count = daily_df.count()
print(f"Output rows: {daily_count}")

# Show sample
print("\n=== Sample Daily OHLCV ===")
daily_df.show(5, truncate=False)
daily_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1.4: Create Catalog and Schema

# COMMAND ----------

gold_catalog = DATABRICKS_PATHS["gold_catalog"]
gold_schema = DATABRICKS_PATHS["gold_schema"]

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {gold_catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_catalog}.{gold_schema}")
    print(f"[OK] Created catalog/schema: {gold_catalog}.{gold_schema}")
except Exception as e:
    print(f"Note: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1.5: Write to Gold Daily OHLCV Table

# COMMAND ----------

try:
    table_exists = spark.catalog.tableExists(daily_ohlcv_table)
    
    if table_exists:
        # Use MERGE to upsert data (idempotent)
        print(f"Table {daily_ohlcv_table} exists. Using MERGE for idempotent upsert...")
        
        daily_df.createOrReplaceTempView("new_daily_data")
        
        merge_sql = f"""
        MERGE INTO {daily_ohlcv_table} AS target
        USING new_daily_data AS source
        ON target.symbol = source.symbol 
           AND target.trade_date = source.trade_date
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
        """
        
        spark.sql(merge_sql)
        print(f"[OK] Merged data into {daily_ohlcv_table}")
        
    else:
        # First time: create table
        print(f"Table {daily_ohlcv_table} does not exist. Creating new table...")
        daily_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(daily_ohlcv_table)
        print(f"[OK] Created and wrote to {daily_ohlcv_table}")
    
    # Verify write
    final_count = spark.table(daily_ohlcv_table).count()
    print(f"\n=== Daily OHLCV Complete ===")
    print(f"Total rows in {daily_ohlcv_table}: {final_count}")
    
except Exception as e:
    print(f"[ERROR] Failed to write to Delta table: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Technical Indicators

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2.1: Read Daily OHLCV Data

# COMMAND ----------

analytics_table = DATABRICKS_PATHS["gold_analytics_table"]

# Read from daily_ohlcv table
try:
    daily_df = spark.table(daily_ohlcv_table)
    daily_count = daily_df.count()
    print(f"[OK] Daily OHLCV table exists: {daily_ohlcv_table}")
    print(f"     Total rows: {daily_count}")
except Exception as e:
    raise ValueError(f"Daily OHLCV table {daily_ohlcv_table} does not exist or cannot be accessed: {e}")

if daily_count == 0:
    print("\n=== No daily data available ===")
    print("Analytics cannot be calculated without daily OHLCV data")
    dbutils.notebook.exit("ERROR: No daily OHLCV data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2.2: Determine Incremental Processing for Analytics

# COMMAND ----------

# For analytics, we need to recalculate last N days when new data arrives
# because window functions need full history. Let's recalculate last 50 days
# to ensure SMAs are accurate.

try:
    analytics_exists = spark.catalog.tableExists(analytics_table)
    if analytics_exists:
        # Get last updated date from analytics
        last_updated = spark.table(analytics_table).agg(
            spark_max("updated_timestamp").alias("max_updated")
        ).collect()[0]["max_updated"]
        
        if last_updated:
            # Recalculate last 50 days to ensure window functions are accurate
            from pyspark.sql.functions import date_sub, to_date
            cutoff_date = spark.sql(f"SELECT date_sub('{last_updated}', 50)").collect()[0][0]
            print(f"[INFO] Recalculating analytics for dates >= {cutoff_date}")
            daily_df = daily_df.filter(col("trade_date") >= lit(cutoff_date))
        else:
            print(f"[INFO] Analytics table exists but empty. Processing all daily data.")
    else:
        print(f"[INFO] Analytics table does not exist. Processing all daily data.")
except Exception as e:
    print(f"[WARN] Could not determine incremental strategy: {e}")
    print("       Processing all daily data")

analytics_count_to_process = daily_df.count()
print(f"     Rows to process: {analytics_count_to_process}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2.3: Calculate Technical Indicators

# COMMAND ----------

print("\n=== Calculating Technical Indicators ===")
print(f"Input rows: {analytics_count_to_process}")

# Calculate indicators
analytics_df = calculate_technical_indicators(daily_df)

analytics_count = analytics_df.count()
print(f"Output rows: {analytics_count}")

# Show sample
print("\n=== Sample Analytics ===")
analytics_df.show(10, truncate=False)
analytics_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2.4: Write to Gold Analytics Table

# COMMAND ----------

try:
    table_exists = spark.catalog.tableExists(analytics_table)
    
    if table_exists:
        # Use MERGE to upsert data (idempotent)
        print(f"Table {analytics_table} exists. Using MERGE for idempotent upsert...")
        
        analytics_df.createOrReplaceTempView("new_analytics_data")
        
        merge_sql = f"""
        MERGE INTO {analytics_table} AS target
        USING new_analytics_data AS source
        ON target.symbol = source.symbol 
           AND target.trade_date = source.trade_date
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
        """
        
        spark.sql(merge_sql)
        print(f"[OK] Merged data into {analytics_table}")
        
    else:
        # First time: create table
        print(f"Table {analytics_table} does not exist. Creating new table...")
        analytics_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(analytics_table)
        print(f"[OK] Created and wrote to {analytics_table}")
    
    # Verify write
    final_count = spark.table(analytics_table).count()
    final_with_sma50 = spark.table(analytics_table).filter(col("sma_50").isNotNull()).count()
    
    print(f"\n=== Analytics Complete ===")
    print(f"Total rows in {analytics_table}: {final_count}")
    print(f"Rows with SMA_50 (sufficient history): {final_with_sma50}")
    
except Exception as e:
    print(f"[ERROR] Failed to write to Delta table: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer Complete
# MAGIC
# MAGIC Both Gold layer tables have been successfully created:
# MAGIC - **Daily OHLCV**: Daily aggregated market data
# MAGIC - **Analytics**: Technical indicators and metrics
# MAGIC
# MAGIC The next pipeline step (`05_data_quality_checks.py`) will validate
# MAGIC data quality across all layers.
