# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

# Databricks notebook source
# MAGIC %md
# MAGIC # Setup and Configuration
# MAGIC
# MAGIC **Production Pipeline - Step 0: Environment Setup**
# MAGIC
# MAGIC This notebook sets up the environment for the market data pipeline:
# MAGIC 1. Initializes Spark session with Delta Lake support
# MAGIC 2. Creates storage directories (DBFS or Unity Catalog volumes)
# MAGIC 3. Creates Delta table schemas (Bronze, Silver, Gold)
# MAGIC 4. Verifies configuration
# MAGIC
# MAGIC **Run this notebook once before running the pipeline.**

# COMMAND ----------

import sys
import os
from pathlib import Path
from typing import Optional

# Add project root to Python path for imports
def add_project_root_to_path() -> str:
    """Add project root directory to sys.path for imports."""
    project_root: Optional[Path] = None
    
    try:
        if '__file__' in globals():
            notebook_dir = Path(__file__).parent
            if (notebook_dir / '00_setup.py').exists():
                project_root = notebook_dir.parent
    except (NameError, AttributeError):
        pass
    
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
        except Exception:
            pass
    
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
    
    if project_root is None:
        raise RuntimeError("Could not determine project root directory")
    
    project_root_str = str(project_root.resolve())
    if project_root_str not in sys.path:
        sys.path.insert(0, project_root_str)
    
    return project_root_str

project_root = add_project_root_to_path()
print(f"Added project root to path: {project_root}")

# COMMAND ----------

from pyspark.sql import SparkSession
from src.config import DATABRICKS_PATHS, STORAGE_MODE, DATABRICKS_PATHS_VOLUMES, DATABRICKS_PATHS_WORKSPACE
from src.schemas import BRONZE_BARS_SCHEMA, BRONZE_SYMBOLS_SCHEMA, SILVER_BARS_SCHEMA, GOLD_DAILY_OHLCV_SCHEMA, GOLD_ANALYTICS_SCHEMA

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Initialize Spark Session

# COMMAND ----------

spark = SparkSession.builder \
    .appName("Pipeline Setup") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

print("Spark session initialized with Delta Lake support")
print(f"Storage mode: {STORAGE_MODE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Storage Directories

# COMMAND ----------

# Create storage directories based on STORAGE_MODE
if STORAGE_MODE == "volumes":
    print("=== Setting up Unity Catalog Volumes ===")
    print("Note: This requires Unity Catalog to be enabled and appropriate permissions.")
    
    try:
        # Create catalog if it doesn't exist
        catalog = DATABRICKS_PATHS_VOLUMES["bronze_catalog"]
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
        print(f"[OK] Catalog '{catalog}' ready")
        
        # Create schemas
        for schema_name in ["default", "bronze", "silver", "gold"]:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")
            print(f"[OK] Schema '{catalog}.{schema_name}' ready")
        
        # Create volumes
        # Note: Volume creation requires specific SQL syntax and permissions
        # This is a simplified version - adjust based on your Unity Catalog setup
        try:
            spark.sql("CREATE VOLUME IF NOT EXISTS main.default.raw_market_data")
            print("[OK] Volume 'main.default.raw_market_data' created")
        except Exception as e:
            print(f"[WARN] Could not create volume (may require admin permissions): {e}")
            print("       You may need to create volumes manually in the Databricks UI")
        
    except Exception as e:
        print(f"[ERROR] Failed to set up Unity Catalog: {e}")
        print("       Consider switching to workspace mode by setting STORAGE_MODE='workspace' in src/config.py")
        print("       Note: DBFS root (/mnt/) is often disabled in modern Databricks environments")
        raise

elif STORAGE_MODE == "workspace":
    print("=== Setting up Workspace File Directories ===")
    print("Note: Workspace files are stored in the workspace and may have size limits.")
    
    try:
        # Create workspace directories for raw data
        dbutils.fs.mkdirs(DATABRICKS_PATHS_WORKSPACE["raw_bars_path"])
        dbutils.fs.mkdirs(DATABRICKS_PATHS_WORKSPACE["raw_symbols_path"])
        print(f"[OK] Created raw data directories in workspace")
        
        # Create workspace directories for medallion layers
        bronze_path = "/Workspace/bronze"
        silver_path = "/Workspace/silver"
        gold_path = "/Workspace/gold"
        
        dbutils.fs.mkdirs(f"{bronze_path}/bars")
        dbutils.fs.mkdirs(f"{bronze_path}/symbols")
        dbutils.fs.mkdirs(f"{silver_path}/bars")
        dbutils.fs.mkdirs(f"{gold_path}/daily_ohlcv")
        dbutils.fs.mkdirs(f"{gold_path}/analytics")
        
        print(f"[OK] Created medallion layer directories in workspace")
        print(f"     Bronze: {bronze_path}")
        print(f"     Silver: {silver_path}")
        print(f"     Gold: {gold_path}")
        
    except Exception as e:
        print(f"[ERROR] Failed to create workspace directories: {e}")
        raise
else:
    print(f"[ERROR] Unknown storage mode: {STORAGE_MODE}")
    print("       Valid modes: 'volumes' or 'workspace'")
    raise ValueError(f"Invalid STORAGE_MODE: {STORAGE_MODE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Delta Table Schemas (Optional)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Tables
# MAGIC
# MAGIC Note: Tables will be created automatically when data is first written.
# MAGIC This section can be used to pre-create tables if desired.

# COMMAND ----------

if STORAGE_MODE == "volumes":
    # Create tables in Unity Catalog
    try:
        catalog = DATABRICKS_PATHS_VOLUMES["bronze_catalog"]
        
        # Bronze bars table
        bronze_bars_table = DATABRICKS_PATHS_VOLUMES["bronze_bars_table"]
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {bronze_bars_table} (
                symbol STRING NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                ingestion_timestamp TIMESTAMP NOT NULL,
                batch_id STRING
            ) USING DELTA
            TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true'
            )
        """)
        print(f"[OK] Created table: {bronze_bars_table}")
        
    except Exception as e:
        print(f"[WARN] Could not pre-create Unity Catalog tables: {e}")
        print("       Tables will be created automatically on first write")
else:
    # For DBFS, tables are created on first write
    print("[INFO] DBFS Delta tables will be created automatically on first write")
    print("       No pre-creation needed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify Configuration

# COMMAND ----------

print("\n=== Configuration Summary ===")
print(f"Storage Mode: {STORAGE_MODE}")
print(f"\nPaths configured:")
for key, value in DATABRICKS_PATHS.items():
    print(f"  {key}: {value}")

print("\n=== Setup Complete ===")
print("You can now run the pipeline notebooks:")
print("  1. 01_collect_raw_data.py - Collect market data")
print("  2. 02_ingest_bronze_bars.py - Ingest to Bronze layer")
print("  3. 03_transform_silver_bars.py - Transform to Silver layer")
print("  4. 04_gold_analytics.py - Create Gold analytics")
print("  5. 05_data_quality_checks.py - Validate data quality")
