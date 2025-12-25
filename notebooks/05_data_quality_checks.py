# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Checks
# MAGIC
# MAGIC **Production Pipeline - Step 5: Data Quality Validation**
# MAGIC
# MAGIC This notebook validates data quality across all medallion layers:
# MAGIC - **Bronze Layer**: Raw data completeness and basic validation
# MAGIC - **Silver Layer**: Data quality scores, valid/invalid record counts
# MAGIC - **Gold Layer**: Completeness checks, consistency validation
# MAGIC
# MAGIC **Quality Checks**:
# MAGIC - Null value checks in key fields
# MAGIC - Range validation (prices, volumes, timestamps)
# MAGIC - Completeness checks (expected vs actual row counts)
# MAGIC - Consistency checks (price relationships, data lineage)
# MAGIC - Quality score distribution analysis

# COMMAND ----------

import sys
import os
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime, timedelta

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
            if (notebook_dir / '05_data_quality_checks.py').exists():
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
from pyspark.sql.functions import (
    col, count, sum as spark_sum, min as spark_min, max as spark_max,
    avg, stddev, when, isnan, isnull, countDistinct,
    date_trunc, to_date
)
from src.config import DATABRICKS_PATHS

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Initialize Spark Session

# COMMAND ----------

spark = SparkSession.builder \
    .appName("Data Quality Checks") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.io.cache.enabled", "true") \
    .getOrCreate()

print("Spark session initialized with Delta Lake and Databricks optimizations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Bronze Layer Quality Checks

# COMMAND ----------

bronze_table = DATABRICKS_PATHS["bronze_bars_table"]

print("=== Bronze Layer Quality Checks ===")

try:
    bronze_df = spark.table(bronze_table)
    bronze_total = bronze_df.count()
    
    if bronze_total == 0:
        print("[WARN] Bronze table is empty")
    else:
        print(f"[OK] Bronze table exists with {bronze_total:,} rows")
        
        # Null checks
        null_checks = bronze_df.agg(
            spark_sum(when(col("symbol").isNull() | (col("symbol") == ""), 1).otherwise(0)).alias("null_symbols"),
            spark_sum(when(col("timestamp").isNull(), 1).otherwise(0)).alias("null_timestamps"),
            spark_sum(when(col("close").isNull(), 1).otherwise(0)).alias("null_closes")
        ).collect()[0]
        
        print(f"\n--- Null Value Checks ---")
        print(f"Null symbols: {null_checks['null_symbols']}")
        print(f"Null timestamps: {null_checks['null_timestamps']}")
        print(f"Null closes: {null_checks['null_closes']}")
        
        # Range checks
        range_checks = bronze_df.agg(
            spark_min("timestamp").alias("min_timestamp"),
            spark_max("timestamp").alias("max_timestamp"),
            spark_min("close").alias("min_close"),
            spark_max("close").alias("max_close"),
            spark_min("volume").alias("min_volume"),
            spark_max("volume").alias("max_volume")
        ).collect()[0]
        
        print(f"\n--- Range Checks ---")
        print(f"Timestamp range: {range_checks['min_timestamp']} to {range_checks['max_timestamp']}")
        print(f"Close price range: ${range_checks['min_close']:.2f} to ${range_checks['max_close']:.2f}")
        print(f"Volume range: {range_checks['min_volume']:,} to {range_checks['max_volume']:,}")
        
        # Completeness checks
        completeness = bronze_df.agg(
            countDistinct("symbol").alias("unique_symbols"),
            countDistinct("batch_id").alias("unique_batches"),
            countDistinct(date_trunc("day", col("timestamp"))).alias("unique_dates")
        ).collect()[0]
        
        print(f"\n--- Completeness Checks ---")
        print(f"Unique symbols: {completeness['unique_symbols']}")
        print(f"Unique batches: {completeness['unique_batches']}")
        print(f"Unique dates: {completeness['unique_dates']}")
        
        
except Exception as e:
    print(f"[ERROR] Failed to check Bronze layer: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Silver Layer Quality Checks

# COMMAND ----------

silver_table = DATABRICKS_PATHS["silver_bars_table"]

print("\n=== Silver Layer Quality Checks ===")

try:
    silver_df = spark.table(silver_table)
    silver_total = silver_df.count()
    
    if silver_total == 0:
        print("[WARN] Silver table is empty")
    else:
        print(f"[OK] Silver table exists with {silver_total:,} rows")
        
        # Quality score distribution
        quality_stats = silver_df.agg(
            spark_min("quality_score").alias("min_quality"),
            spark_max("quality_score").alias("max_quality"),
            avg("quality_score").alias("avg_quality"),
            stddev("quality_score").alias("stddev_quality")
        ).collect()[0]
        
        print(f"\n--- Quality Score Statistics ---")
        print(f"Min quality score: {quality_stats['min_quality']:.3f}")
        print(f"Max quality score: {quality_stats['max_quality']:.3f}")
        print(f"Avg quality score: {quality_stats['avg_quality']:.3f}")
        print(f"Stddev quality score: {quality_stats['stddev_quality']:.3f}")
        
        # Valid vs Invalid counts
        validity_counts = silver_df.groupBy("is_valid").agg(
            count("*").alias("count")
        ).collect()
        
        valid_count = next((r["count"] for r in validity_counts if r["is_valid"] == 1), 0)
        invalid_count = next((r["count"] for r in validity_counts if r["is_valid"] == 0), 0)
        
        print(f"\n--- Validity Checks ---")
        print(f"Valid records (is_valid=1): {valid_count:,} ({100*valid_count/silver_total:.2f}%)")
        print(f"Invalid records (is_valid=0): {invalid_count:,} ({100*invalid_count/silver_total:.2f}%)")
        
        # Null checks (should be minimal after cleaning)
        null_checks = silver_df.agg(
            spark_sum(when(col("symbol").isNull() | (col("symbol") == ""), 1).otherwise(0)).alias("null_symbols"),
            spark_sum(when(col("timestamp").isNull(), 1).otherwise(0)).alias("null_timestamps"),
            spark_sum(when(col("close").isNull(), 1).otherwise(0)).alias("null_closes"),
            spark_sum(when(col("open").isNull(), 1).otherwise(0)).alias("null_opens"),
            spark_sum(when(col("high").isNull(), 1).otherwise(0)).alias("null_highs"),
            spark_sum(when(col("low").isNull(), 1).otherwise(0)).alias("null_lows"),
            spark_sum(when(col("volume").isNull(), 1).otherwise(0)).alias("null_volumes")
        ).collect()[0]
        
        print(f"\n--- Null Value Checks (should be 0) ---")
        print(f"Null symbols: {null_checks['null_symbols']}")
        print(f"Null timestamps: {null_checks['null_timestamps']}")
        print(f"Null closes: {null_checks['null_closes']}")
        print(f"Null opens: {null_checks['null_opens']}")
        print(f"Null highs: {null_checks['null_highs']}")
        print(f"Null lows: {null_checks['null_lows']}")
        print(f"Null volumes: {null_checks['null_volumes']}")
        
        # Consistency checks (price relationships)
        consistency_checks = silver_df.agg(
            spark_sum(when(col("high") < col("low"), 1).otherwise(0)).alias("high_lt_low"),
            spark_sum(when(col("high") < col("close"), 1).otherwise(0)).alias("high_lt_close"),
            spark_sum(when(col("high") < col("open"), 1).otherwise(0)).alias("high_lt_open"),
            spark_sum(when(col("low") > col("close"), 1).otherwise(0)).alias("low_gt_close"),
            spark_sum(when(col("low") > col("open"), 1).otherwise(0)).alias("low_gt_open"),
            spark_sum(when(col("close") <= 0, 1).otherwise(0)).alias("non_positive_close"),
            spark_sum(when(col("volume") < 0, 1).otherwise(0)).alias("negative_volume")
        ).collect()[0]
        
        print(f"\n--- Consistency Checks (should be 0) ---")
        print(f"High < Low violations: {consistency_checks['high_lt_low']}")
        print(f"High < Close violations: {consistency_checks['high_lt_close']}")
        print(f"High < Open violations: {consistency_checks['high_lt_open']}")
        print(f"Low > Close violations: {consistency_checks['low_gt_close']}")
        print(f"Low > Open violations: {consistency_checks['low_gt_open']}")
        print(f"Non-positive close prices: {consistency_checks['non_positive_close']}")
        print(f"Negative volumes: {consistency_checks['negative_volume']}")
        
        # Completeness
        completeness = silver_df.agg(
            countDistinct("symbol").alias("unique_symbols"),
            countDistinct(date_trunc("day", col("timestamp"))).alias("unique_dates")
        ).collect()[0]
        
        print(f"\n--- Completeness Checks ---")
        print(f"Unique symbols: {completeness['unique_symbols']}")
        print(f"Unique dates: {completeness['unique_dates']}")
        
except Exception as e:
    print(f"[ERROR] Failed to check Silver layer: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Gold Layer Quality Checks

# COMMAND ----------

daily_ohlcv_table = DATABRICKS_PATHS["gold_daily_ohlcv_table"]
analytics_table = DATABRICKS_PATHS["gold_analytics_table"]

print("\n=== Gold Layer Quality Checks ===")

# Daily OHLCV checks
print("\n--- Daily OHLCV Table ---")
try:
    daily_df = spark.table(daily_ohlcv_table)
    daily_total = daily_df.count()
    
    if daily_total == 0:
        print("[WARN] Daily OHLCV table is empty")
    else:
        print(f"[OK] Daily OHLCV table exists with {daily_total:,} rows")
        
        # Null checks
        null_checks = daily_df.agg(
            spark_sum(when(col("symbol").isNull() | (col("symbol") == ""), 1).otherwise(0)).alias("null_symbols"),
            spark_sum(when(col("trade_date").isNull(), 1).otherwise(0)).alias("null_dates"),
            spark_sum(when(col("close").isNull(), 1).otherwise(0)).alias("null_closes"),
            spark_sum(when(col("daily_return").isNull(), 1).otherwise(0)).alias("null_returns")
        ).collect()[0]
        
        print(f"\n--- Null Value Checks ---")
        print(f"Null symbols: {null_checks['null_symbols']}")
        print(f"Null dates: {null_checks['null_dates']}")
        print(f"Null closes: {null_checks['null_closes']}")
        print(f"Null daily returns: {null_checks['null_returns']}")
        
        # Consistency checks
        consistency_checks = daily_df.agg(
            spark_sum(when(col("high") < col("low"), 1).otherwise(0)).alias("high_lt_low"),
            spark_sum(when(col("high") < col("close"), 1).otherwise(0)).alias("high_lt_close"),
            spark_sum(when(col("high") < col("open"), 1).otherwise(0)).alias("high_lt_open"),
            spark_sum(when(col("low") > col("close"), 1).otherwise(0)).alias("low_gt_close"),
            spark_sum(when(col("num_bars") <= 0, 1).otherwise(0)).alias("invalid_num_bars")
        ).collect()[0]
        
        print(f"\n--- Consistency Checks (should be 0) ---")
        print(f"High < Low violations: {consistency_checks['high_lt_low']}")
        print(f"High < Close violations: {consistency_checks['high_lt_close']}")
        print(f"High < Open violations: {consistency_checks['high_lt_open']}")
        print(f"Low > Close violations: {consistency_checks['low_gt_close']}")
        print(f"Invalid num_bars: {consistency_checks['invalid_num_bars']}")
        
        # Completeness
        completeness = daily_df.agg(
            countDistinct("symbol").alias("unique_symbols"),
            countDistinct("trade_date").alias("unique_dates"),
            avg("num_bars").alias("avg_bars_per_day")
        ).collect()[0]
        
        print(f"\n--- Completeness Checks ---")
        print(f"Unique symbols: {completeness['unique_symbols']}")
        print(f"Unique dates: {completeness['unique_dates']}")
        print(f"Avg bars per day: {completeness['avg_bars_per_day']:.1f}")
        
except Exception as e:
    print(f"[ERROR] Failed to check Daily OHLCV table: {e}")

# Analytics table checks
print("\n--- Analytics Table ---")
try:
    analytics_df = spark.table(analytics_table)
    analytics_total = analytics_df.count()
    
    if analytics_total == 0:
        print("[WARN] Analytics table is empty")
    else:
        print(f"[OK] Analytics table exists with {analytics_total:,} rows")
        
        # Indicator completeness
        indicator_completeness = analytics_df.agg(
            spark_sum(when(col("sma_5").isNotNull(), 1).otherwise(0)).alias("has_sma_5"),
            spark_sum(when(col("sma_20").isNotNull(), 1).otherwise(0)).alias("has_sma_20"),
            spark_sum(when(col("sma_50").isNotNull(), 1).otherwise(0)).alias("has_sma_50"),
            spark_sum(when(col("volatility").isNotNull(), 1).otherwise(0)).alias("has_volatility")
        ).collect()[0]
        
        print(f"\n--- Indicator Completeness ---")
        print(f"Rows with SMA_5: {indicator_completeness['has_sma_5']:,} ({100*indicator_completeness['has_sma_5']/analytics_total:.1f}%)")
        print(f"Rows with SMA_20: {indicator_completeness['has_sma_20']:,} ({100*indicator_completeness['has_sma_20']/analytics_total:.1f}%)")
        print(f"Rows with SMA_50: {indicator_completeness['has_sma_50']:,} ({100*indicator_completeness['has_sma_50']/analytics_total:.1f}%)")
        print(f"Rows with Volatility: {indicator_completeness['has_volatility']:,} ({100*indicator_completeness['has_volatility']/analytics_total:.1f}%)")
        
        # Indicator statistics
        indicator_stats = analytics_df.agg(
            avg("sma_5").alias("avg_sma_5"),
            avg("sma_20").alias("avg_sma_20"),
            avg("sma_50").alias("avg_sma_50"),
            avg("volatility").alias("avg_volatility")
        ).collect()[0]
        
        print(f"\n--- Indicator Statistics ---")
        print(f"Avg SMA_5: {indicator_stats['avg_sma_5']:.2f}" if indicator_stats['avg_sma_5'] else "Avg SMA_5: N/A")
        print(f"Avg SMA_20: {indicator_stats['avg_sma_20']:.2f}" if indicator_stats['avg_sma_20'] else "Avg SMA_20: N/A")
        print(f"Avg SMA_50: {indicator_stats['avg_sma_50']:.2f}" if indicator_stats['avg_sma_50'] else "Avg SMA_50: N/A")
        print(f"Avg Volatility: {indicator_stats['avg_volatility']:.4f}" if indicator_stats['avg_volatility'] else "Avg Volatility: N/A")
        
        # Completeness
        completeness = analytics_df.agg(
            countDistinct("symbol").alias("unique_symbols"),
            countDistinct("trade_date").alias("unique_dates")
        ).collect()[0]
        
        print(f"\n--- Completeness Checks ---")
        print(f"Unique symbols: {completeness['unique_symbols']}")
        print(f"Unique dates: {completeness['unique_dates']}")
        
except Exception as e:
    print(f"[ERROR] Failed to check Analytics table: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Cross-Layer Consistency Checks

# COMMAND ----------

print("\n=== Cross-Layer Consistency Checks ===")

try:
    # Check data lineage: Bronze -> Silver -> Gold
    bronze_df = spark.table(bronze_table)
    silver_df = spark.table(silver_table)
    daily_df = spark.table(daily_ohlcv_table)
    
    # Symbol consistency
    bronze_symbols = set(bronze_df.select("symbol").distinct().rdd.map(lambda r: r[0]).collect())
    silver_symbols = set(silver_df.select("symbol").distinct().rdd.map(lambda r: r[0]).collect())
    gold_symbols = set(daily_df.select("symbol").distinct().rdd.map(lambda r: r[0]).collect())
    
    print(f"\n--- Symbol Consistency ---")
    print(f"Bronze symbols: {len(bronze_symbols)}")
    print(f"Silver symbols: {len(silver_symbols)}")
    print(f"Gold symbols: {len(gold_symbols)}")
    
    symbols_only_bronze = bronze_symbols - silver_symbols
    symbols_only_silver = silver_symbols - gold_symbols
    
    if symbols_only_bronze:
        print(f"[WARN] Symbols in Bronze but not in Silver: {len(symbols_only_bronze)}")
    if symbols_only_silver:
        print(f"[WARN] Symbols in Silver but not in Gold: {len(symbols_only_silver)}")
    
    # Date range consistency
    bronze_dates = bronze_df.agg(
        spark_min(date_trunc("day", col("timestamp"))).alias("min_date"),
        spark_max(date_trunc("day", col("timestamp"))).alias("max_date")
    ).collect()[0]
    
    silver_dates = silver_df.agg(
        spark_min(date_trunc("day", col("timestamp"))).alias("min_date"),
        spark_max(date_trunc("day", col("timestamp"))).alias("max_date")
    ).collect()[0]
    
    gold_dates = daily_df.agg(
        spark_min("trade_date").alias("min_date"),
        spark_max("trade_date").alias("max_date")
    ).collect()[0]
    
    print(f"\n--- Date Range Consistency ---")
    print(f"Bronze date range: {bronze_dates['min_date']} to {bronze_dates['max_date']}")
    print(f"Silver date range: {silver_dates['min_date']} to {silver_dates['max_date']}")
    print(f"Gold date range: {gold_dates['min_date']} to {gold_dates['max_date']}")
    
except Exception as e:
    print(f"[ERROR] Failed cross-layer consistency checks: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Check Summary

# COMMAND ----------

print("\n" + "="*60)
print("DATA QUALITY CHECK SUMMARY")
print("="*60)
print("\nAll quality checks have been completed.")
print("Review the output above for any warnings or errors.")
print("\nKey metrics to monitor:")
print("- Silver layer: Quality score distribution, valid/invalid ratio")
print("- Gold layer: Indicator completeness, consistency violations")
print("- Cross-layer: Symbol and date range consistency")
print("\n" + "="*60)
