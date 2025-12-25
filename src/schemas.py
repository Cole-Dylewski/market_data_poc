"""
Spark schemas for market data tables.

Defines explicit schemas for Bronze, Silver, and Gold layers
following the medallion architecture pattern.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    TimestampType,
    IntegerType,
)

# Bronze Layer Schema (Raw ingested data)
# Contains raw data with minimal transformation, includes metadata
BRONZE_BARS_SCHEMA = StructType([
    StructField("symbol", StringType(), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("open", DoubleType(), nullable=True),
    StructField("high", DoubleType(), nullable=True),
    StructField("low", DoubleType(), nullable=True),
    StructField("close", DoubleType(), nullable=True),
    StructField("volume", LongType(), nullable=True),
    # Metadata fields
    StructField("ingestion_timestamp", TimestampType(), nullable=False),
    StructField("batch_id", StringType(), nullable=True),  # For tracking ingestion batches
])

BRONZE_SYMBOLS_SCHEMA = StructType([
    StructField("symbol", StringType(), nullable=False),
    StructField("source", StringType(), nullable=False),  # e.g., "sp500", "custom"
    StructField("ingestion_timestamp", TimestampType(), nullable=False),
    StructField("is_active", IntegerType(), nullable=True),  # 1 for active, 0 for inactive
])

# Silver Layer Schema (Cleaned and normalized data)
# Standardized schema with enforced types and deduplication
SILVER_BARS_SCHEMA = StructType([
    StructField("symbol", StringType(), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("open", DoubleType(), nullable=False),
    StructField("high", DoubleType(), nullable=False),
    StructField("low", DoubleType(), nullable=False),
    StructField("close", DoubleType(), nullable=False),
    StructField("volume", LongType(), nullable=False),
    # Metadata
    StructField("processed_timestamp", TimestampType(), nullable=False),
    # Quality flags
    StructField("is_valid", IntegerType(), nullable=True),  # 1 for valid, 0 for invalid
    StructField("quality_score", DoubleType(), nullable=True),  # 0.0 to 1.0
])

# Gold Layer Schema (Analytics and aggregates)
# Daily OHLCV aggregations
GOLD_DAILY_OHLCV_SCHEMA = StructType([
    StructField("symbol", StringType(), nullable=False),
    StructField("trade_date", TimestampType(), nullable=False),  # Date of trading
    StructField("open", DoubleType(), nullable=False),  # First price of the day
    StructField("high", DoubleType(), nullable=False),  # Highest price of the day
    StructField("low", DoubleType(), nullable=False),  # Lowest price of the day
    StructField("close", DoubleType(), nullable=False),  # Last price of the day
    StructField("volume", LongType(), nullable=False),  # Total volume for the day
    StructField("num_bars", IntegerType(), nullable=True),  # Number of intraday bars aggregated
    # Calculated metrics
    StructField("daily_return", DoubleType(), nullable=True),  # (close - open) / open
    StructField("price_range", DoubleType(), nullable=True),  # high - low
    StructField("avg_price", DoubleType(), nullable=True),  # (high + low) / 2
    StructField("updated_timestamp", TimestampType(), nullable=False),
])

# Gold Layer Schema (Analytics table)
# Technical indicators and analytics
GOLD_ANALYTICS_SCHEMA = StructType([
    StructField("symbol", StringType(), nullable=False),
    StructField("trade_date", TimestampType(), nullable=False),
    StructField("sma_5", DoubleType(), nullable=True),  # 5-day simple moving average
    StructField("sma_20", DoubleType(), nullable=True),  # 20-day simple moving average
    StructField("sma_50", DoubleType(), nullable=True),  # 50-day simple moving average
    StructField("volatility", DoubleType(), nullable=True),  # Standard deviation of returns
    StructField("updated_timestamp", TimestampType(), nullable=False),
])
