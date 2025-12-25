# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: Streaming Ingestion to Delta Tables
# MAGIC
# MAGIC **Production Pipeline - Step 2: Bronze Streaming Ingestion**
# MAGIC
# MAGIC **Note**: For production, consider using the Delta Live Tables (DLT) pipeline (`dlt_pipeline.py`) 
# MAGIC which provides automatic dependency management, data quality expectations, and built-in monitoring.
# MAGIC
# MAGIC This notebook provides an alternative implementation using Structured Streaming:
# MAGIC 1. Sets up a streaming job that monitors the landing zone for new JSON files
# MAGIC 2. Automatically ingests new files as they arrive using Auto Loader
# MAGIC 3. Validates and structures the data according to Bronze schema
# MAGIC 4. Writes to Bronze Delta table using streaming writes with exactly-once semantics
# MAGIC
# MAGIC **Streaming**: Uses Structured Streaming with Auto Loader for automatic file detection
# MAGIC **Exactly-Once**: Uses checkpointing to ensure no duplicates even if job restarts
# MAGIC **Production Mode**: Continuous streaming enabled for production-ready real-time ingestion
# MAGIC **Databricks Features**: Auto Loader, Unity Catalog, Delta Lake streaming

# COMMAND ----------

import sys
import os
import json
from pathlib import Path
from typing import Optional, List, Dict, Any
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
            if (notebook_dir / '02_ingest_bronze_bars.py').exists():
                project_root = notebook_dir.parent
    except (NameError, AttributeError):
        pass
    
    # Method 2: Try using dbutils (Databricks-specific)
    if project_root is None:
        try:
            import dbutils
            # Try multiple ways to get the notebook path
            try:
                workspace_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
            except:
                try:
                    workspace_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
                except:
                    workspace_path = None
            
            if workspace_path:
                # Handle both /Workspace/Repos/... and /Workspace/Users/... paths
                if workspace_path.startswith('/'):
                    # Remove leading slash and split
                    parts = workspace_path.lstrip('/').split('/')
                    if len(parts) >= 2:
                        # Try Repos path: /Workspace/Repos/user/repo/notebooks/...
                        if parts[0] == 'Workspace' and parts[1] == 'Repos':
                            if len(parts) >= 4:
                                repo_path = Path('/Workspace/Repos').joinpath(*parts[2:-1])
                                if (repo_path / "src").exists():
                                    project_root = repo_path
                        # Try Users path: /Workspace/Users/user/project/notebooks/...
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
    print(f"Verifying src directory exists: {(Path(project_root_str) / 'src').exists()}")
    print(f"Verifying notebooks directory exists: {(Path(project_root_str) / 'notebooks').exists()}")
    
    return project_root_str

project_root = add_project_root_to_path()
print(f"Added project root to path: {project_root}")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StructType
from src.config import DATABRICKS_PATHS
from src.schemas import BRONZE_BARS_SCHEMA

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Initialize Spark Session

# COMMAND ----------

spark = SparkSession.builder \
    .appName("Bronze Ingestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
    .config("spark.databricks.delta.autoCompact.enabled", "true") \
    .config("spark.databricks.io.cache.enabled", "true") \
    .getOrCreate()

print("Spark session initialized with Delta Lake and Databricks optimizations")
print("  - Delta Lake extensions enabled")
print("  - Auto-optimize write enabled")
print("  - Auto-compact enabled")
print("  - IO cache enabled")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Configure Streaming Source

# COMMAND ----------

raw_bars_path = DATABRICKS_PATHS["raw_bars_path"]
bronze_table = DATABRICKS_PATHS["bronze_bars_table"]
checkpoint_path = f"{raw_bars_path}/_checkpoints/bronze_ingestion"

print(f"Streaming source path: {raw_bars_path}")
print(f"Target table: {bronze_table}")
print(f"Checkpoint path: {checkpoint_path}")

# Create checkpoint directory if it doesn't exist
try:
    dbutils.fs.mkdirs(checkpoint_path)
    print(f"[OK] Checkpoint directory ready: {checkpoint_path}")
except Exception as e:
    print(f"Note: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Catalog and Schema

# COMMAND ----------

catalog = DATABRICKS_PATHS["bronze_catalog"]
schema = DATABRICKS_PATHS["bronze_schema"]

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    print(f"[OK] Created catalog/schema: {catalog}.{schema}")
except Exception as e:
    print(f"Note: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Define Streaming Query

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, current_timestamp, to_timestamp,
    input_file_name, regexp_extract
)

# Define the streaming source using Auto Loader
# Auto Loader automatically detects new files and handles schema evolution
streaming_df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema") \
    .option("cloudFiles.inferColumnTypes", "true") \
    .option("pathGlobFilter", "bars_*.json") \
    .load(raw_bars_path)

print("[OK] Streaming source configured with Auto Loader")
print("     - Automatically detects new JSON files")
print("     - Handles schema evolution")
print("     - Processes files matching bars_*.json pattern")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Transform and Add Metadata

# COMMAND ----------

# Extract batch_id from filename (bars_YYYYMMDD.json)
bronze_stream = streaming_df \
    .withColumn(
        "batch_id",
        regexp_extract(input_file_name(), r"bars_(\d{8})\.json", 1)
    ) \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .select(
        col("symbol").cast("string"),
        to_timestamp(col("timestamp")).alias("timestamp"),
        col("open").cast("double"),
        col("high").cast("double"),
        col("low").cast("double"),
        col("close").cast("double"),
        col("volume").cast("bigint"),
        col("ingestion_timestamp").cast("timestamp"),
        col("batch_id").cast("string")
    )

print("[OK] Streaming transformation configured")
print("     - Schema enforcement applied")
print("     - Metadata columns added (ingestion_timestamp, batch_id)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Write Stream to Bronze Delta Table

# COMMAND ----------

# Write stream to Delta table with exactly-once semantics
# Checkpointing ensures no duplicates even if the job restarts
try:
    print(f"[OK] Starting streaming query")
    print(f"     Writing to: {bronze_table}")
    print(f"     Checkpoint: {checkpoint_path}")
    
    # CONTINUOUS MODE: Production-ready streaming that processes files as they arrive
    print(f"\n=== Continuous Streaming Mode ===")
    print("Stream will keep running and automatically process new files as they arrive.")
    print("This is production-ready continuous ingestion.")
    print("To stop: query.stop() or interrupt the notebook")
    
    query = bronze_stream.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .option("mergeSchema", "true") \
        .table(bronze_table)
    
    query.awaitTermination()
    
except Exception as e:
    print(f"[ERROR] Failed to start streaming query: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Ingestion Complete
# MAGIC
# MAGIC **Batch Mode**: Processed all available files and stopped.
# MAGIC
# MAGIC **For Continuous Streaming** (process files as they arrive):
# MAGIC - Uncomment the continuous mode section in Step 6
# MAGIC - The stream will keep running and automatically process new files
# MAGIC - Use `query.stop()` to stop the stream
# MAGIC
# MAGIC **Checkpointing**: The checkpoint ensures exactly-once semantics - files are only processed once even if the job restarts.
# MAGIC
# MAGIC **Auto Loader Benefits**:
# MAGIC - Automatically detects new files
# MAGIC - Handles schema evolution
# MAGIC - Efficient incremental processing
# MAGIC - Exactly-once guarantees via checkpointing
# MAGIC
# MAGIC The next pipeline step (`03_transform_silver_bars.py`) will read from Bronze
# MAGIC and transform data into the Silver layer.

