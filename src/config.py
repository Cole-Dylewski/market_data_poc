"""
Configuration module for the market data pipeline.

Defines paths, settings, and configurations for the production pipeline.
"""

from typing import Dict, Any
import os

# Databricks paths for medallion architecture
# Supports both Unity Catalog volumes and DBFS paths

# Storage mode: "volumes" for Unity Catalog volumes, "workspace" for workspace files
# Note: DBFS root (/mnt/) is often disabled in modern Databricks environments
STORAGE_MODE: str = "volumes"  # Default to Unity Catalog volumes (recommended)

# Unity Catalog paths (requires catalog 'main' and schema 'default' to exist)
DATABRICKS_PATHS_VOLUMES: Dict[str, str] = {
    # Volume paths (raw landing zone)
    "raw_volume": "/Volumes/main/default/raw_market_data",
    "raw_bars_path": "/Volumes/main/default/raw_market_data/bars",
    "raw_symbols_path": "/Volumes/main/default/raw_market_data/symbols",
    
    # Delta table paths (medallion layers)
    "bronze_catalog": "main",
    "bronze_schema": "bronze",
    "bronze_bars_table": "main.bronze.bars",
    "bronze_symbols_table": "main.bronze.symbols",
    
    "silver_catalog": "main",
    "silver_schema": "silver",
    "silver_bars_table": "main.silver.bars",
    
    "gold_catalog": "main",
    "gold_schema": "gold",
    "gold_daily_ohlcv_table": "main.gold.daily_ohlcv",
    "gold_analytics_table": "main.gold.analytics",
}

# Workspace file paths (fallback if Unity Catalog not available)
# Uses workspace-local storage (not recommended for production, but works without setup)
DATABRICKS_PATHS_WORKSPACE: Dict[str, str] = {
    # Workspace file paths (raw landing zone)
    "raw_bars_path": "/Workspace/raw_market_data/bars",
    "raw_symbols_path": "/Workspace/raw_market_data/symbols",
    
    # Delta table paths using workspace files
    "bronze_bars_table": "delta.`/Workspace/bronze/bars`",
    "bronze_symbols_table": "delta.`/Workspace/bronze/symbols`",
    "silver_bars_table": "delta.`/Workspace/silver/bars`",
    "gold_daily_ohlcv_table": "delta.`/Workspace/gold/daily_ohlcv`",
    "gold_analytics_table": "delta.`/Workspace/gold/analytics`",
}

# Select paths based on storage mode
if STORAGE_MODE == "volumes":
    DATABRICKS_PATHS = DATABRICKS_PATHS_VOLUMES
elif STORAGE_MODE == "workspace":
    DATABRICKS_PATHS = DATABRICKS_PATHS_WORKSPACE
else:
    # Default to workspace if unknown mode
    DATABRICKS_PATHS = DATABRICKS_PATHS_WORKSPACE

# Data source configurations
DATA_SOURCES: Dict[str, Dict[str, Any]] = {
    "yahoo_finance": {
        "enabled": True,
        "rate_limit_delay_seconds": 3.0,
        "retry_attempts": 3,
        "retry_delay_seconds": 15.0,
    },
}

# Pipeline settings
PIPELINE_CONFIG: Dict[str, Any] = {
    "default_interval": "5m",
    "market_hours": {
        "open": "09:30",
        "close": "16:00",
        "timezone": "America/New_York",
    },
    "batch_size": 50,  # Process symbols in batches to avoid rate limits
    "enable_idempotency": True,  # Use Delta MERGE for idempotent processing
}

# Symbol list configuration
SYMBOL_CONFIG: Dict[str, Any] = {
    "sp500_url": "https://stockanalysis.com/list/sp-500-stocks/",
    "cache_symbols": True,  # Cache symbol list to avoid repeated scraping
    "symbol_cache_ttl_hours": 24,  # Refresh symbol list daily
}
