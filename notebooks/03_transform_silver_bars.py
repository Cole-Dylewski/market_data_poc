# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Cleaned and Normalized Data
# MAGIC
# MAGIC **Production Pipeline - Step 3: Silver Transformation**
# MAGIC
# MAGIC This notebook:
# MAGIC - Reads from bronze Delta tables
# MAGIC - Applies schema enforcement and type casting
# MAGIC - Deduplicates records (symbol + timestamp)
# MAGIC - Validates data quality (price relationships, volume, nulls)
# MAGIC - Calculates quality scores and flags valid/invalid records
# MAGIC - Uses Delta MERGE for idempotent processing
# MAGIC - Writes to silver Delta tables

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
            if (notebook_dir / '03_transform_silver_bars.py').exists():
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
from pyspark.sql.functions import col, max as spark_max, lit
from src.config import DATABRICKS_PATHS
from src.transforms import clean_bronze_to_silver, get_incremental_bronze_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Initialize Spark Session

# COMMAND ----------

spark = SparkSession.builder \
    .appName("Silver Transformation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
    .config("spark.databricks.delta.autoCompact.enabled", "true") \
    .config("spark.databricks.io.cache.enabled", "true") \
    .getOrCreate()

print("Spark session initialized with Delta Lake and Databricks optimizations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Read Bronze Data (Incremental)

# COMMAND ----------

bronze_table = DATABRICKS_PATHS["bronze_bars_table"]
silver_table = DATABRICKS_PATHS["silver_bars_table"]

# Check if bronze table exists
try:
    bronze_df = spark.table(bronze_table)
    bronze_count = bronze_df.count()
    print(f"[OK] Bronze table exists: {bronze_table}")
    print(f"     Total rows in bronze: {bronze_count}")
except Exception as e:
    raise ValueError(f"Bronze table {bronze_table} does not exist or cannot be accessed: {e}")

# Determine incremental processing strategy
# Option 1: Process new batch_ids not yet in Silver
# Option 2: Process records since last processed_timestamp
try:
    silver_exists = spark.catalog.tableExists(silver_table)
    if silver_exists:
        # Get last processed timestamp from Silver
        last_processed = spark.table(silver_table).agg(
            spark_max("processed_timestamp").alias("max_processed")
        ).collect()[0]["max_processed"]
        
        if last_processed:
            print(f"[INFO] Silver table exists. Processing records since: {last_processed}")
            bronze_df = bronze_df.filter(col("ingestion_timestamp") > lit(last_processed))
        else:
            print(f"[INFO] Silver table exists but empty. Processing all bronze data.")
    else:
        print(f"[INFO] Silver table does not exist. Processing all bronze data.")
except Exception as e:
    print(f"[WARN] Could not determine incremental strategy: {e}")
    print("       Processing all bronze data")

bronze_count_after_filter = bronze_df.count()
print(f"     Rows to process: {bronze_count_after_filter}")

if bronze_count_after_filter == 0:
    print("\n=== No new data to process ===")
    print("Silver transformation complete (no new records)")
    dbutils.notebook.exit("SUCCESS: No new data to process")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Transform Bronze to Silver

# COMMAND ----------

print("\n=== Transforming Bronze to Silver ===")
print(f"Input rows: {bronze_count_after_filter}")

# Apply transformation
silver_df = clean_bronze_to_silver(bronze_df)

silver_count = silver_df.count()
print(f"Output rows: {silver_count}")

# Quality metrics
from pyspark.sql.functions import avg as spark_avg
quality_metrics = silver_df.agg(
    spark_max("quality_score").alias("max_quality"),
    spark_avg("quality_score").alias("avg_quality"),
    spark_max(col("is_valid")).alias("has_valid")
).collect()[0]

# Count valid vs invalid
valid_count = silver_df.filter(col("is_valid") == 1).count()
invalid_count = silver_df.filter(col("is_valid") == 0).count()

print(f"\n=== Quality Metrics ===")
print(f"Valid records: {valid_count}")
print(f"Invalid records: {invalid_count}")
print(f"Max quality score: {quality_metrics['max_quality']:.3f}")
print(f"Avg quality score: {quality_metrics['avg_quality']:.3f}")

silver_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Catalog and Schema

# COMMAND ----------

silver_catalog = DATABRICKS_PATHS["silver_catalog"]
silver_schema = DATABRICKS_PATHS["silver_schema"]

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {silver_catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {silver_catalog}.{silver_schema}")
    print(f"[OK] Created catalog/schema: {silver_catalog}.{silver_schema}")
except Exception as e:
    print(f"Note: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Write to Silver Delta Table (Idempotent MERGE)

# COMMAND ----------

try:
    table_exists = spark.catalog.tableExists(silver_table)
    
    if table_exists:
        # Use MERGE to upsert data (idempotent)
        print(f"Table {silver_table} exists. Using MERGE for idempotent upsert...")
        
        # Create temporary view for merge
        silver_df.createOrReplaceTempView("new_silver_data")
        
        # MERGE statement: update if exists, insert if not
        merge_sql = f"""
        MERGE INTO {silver_table} AS target
        USING new_silver_data AS source
        ON target.symbol = source.symbol 
           AND target.timestamp = source.timestamp
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
        """
        
        spark.sql(merge_sql)
        print(f"[OK] Merged data into {silver_table}")
        
    else:
        # First time: create table
        print(f"Table {silver_table} does not exist. Creating new table...")
        silver_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(silver_table)
        print(f"[OK] Created and wrote to {silver_table}")
    
    # Verify write
    final_count = spark.table(silver_table).count()
    final_valid = spark.table(silver_table).filter(col("is_valid") == 1).count()
    final_invalid = spark.table(silver_table).filter(col("is_valid") == 0).count()
    
    print(f"\n=== Transformation Complete ===")
    print(f"Total rows in {silver_table}: {final_count}")
    print(f"Valid rows: {final_valid}")
    print(f"Invalid rows: {final_invalid}")
    
except Exception as e:
    print(f"[ERROR] Failed to write to Delta table: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation Complete
# MAGIC
# MAGIC Data has been successfully transformed into the Silver Delta table.
# MAGIC The next pipeline step (`04_gold_analytics.py`) will read from Silver
# MAGIC and create Gold layer analytics.
