"""
Spark transformation functions for the medallion architecture.

Provides reusable transformation functions for:
- Bronze to Silver: Cleaning, deduplication, type casting, quality scoring
- Silver to Gold: Aggregations, technical indicators, analytics
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType
from datetime import datetime
from typing import Optional

from src.schemas import SILVER_BARS_SCHEMA, GOLD_DAILY_OHLCV_SCHEMA, GOLD_ANALYTICS_SCHEMA


def clean_bronze_to_silver(bronze_df: DataFrame) -> DataFrame:
    """
    Transform bronze data to silver with cleaning, deduplication, and quality scoring.
    
    This function:
    1. Applies schema enforcement and type casting
    2. Deduplicates records (keeps most recent by ingestion_timestamp)
    3. Validates data quality (price relationships, volume, nulls)
    4. Calculates quality scores
    5. Flags valid/invalid records
    
    Args:
        bronze_df: DataFrame from Bronze layer
        
    Returns:
        DataFrame conforming to SILVER_BARS_SCHEMA with quality metrics
    """
    from pyspark.sql.functions import col, when, lit, row_number, current_timestamp
    
    # Step 1: Filter out records with null critical fields
    bronze_df = bronze_df.filter(
        col("symbol").isNotNull() &
        col("timestamp").isNotNull() &
        col("close").isNotNull()
    )
    
    # Step 2: Deduplicate - keep most recent record by ingestion_timestamp
    window_spec = Window.partitionBy("symbol", "timestamp").orderBy(
        col("ingestion_timestamp").desc()
    )
    bronze_df = bronze_df.withColumn("row_num", row_number().over(window_spec))
    bronze_df = bronze_df.filter(col("row_num") == 1).drop("row_num")
    
    # Step 3: Handle null OHLC values - filter out records with null OHLC
    # (We preserve them in Bronze for audit, but filter for Silver)
    bronze_df = bronze_df.filter(
        col("open").isNotNull() &
        col("high").isNotNull() &
        col("low").isNotNull() &
        col("volume").isNotNull()
    )
    
    # Step 4: Apply type casting to match Silver schema
    silver_df = bronze_df.select(
        col("symbol").cast("string"),
        col("timestamp").cast("timestamp"),
        col("open").cast("double"),
        col("high").cast("double"),
        col("low").cast("double"),
        col("close").cast("double"),
        col("volume").cast("bigint"),
        current_timestamp().alias("processed_timestamp")
    )
    
    # Step 5: Data quality validation and scoring
    silver_df = _add_quality_checks(silver_df)
    
    return silver_df


def _add_quality_checks(df: DataFrame) -> DataFrame:
    """
    Add data quality checks and scoring to DataFrame.
    
    Quality score components:
    - Completeness (40%): All OHLCV fields present
    - Consistency (40%): Price relationships valid
    - Reasonableness (20%): Values within expected ranges
    
    Args:
        df: DataFrame with OHLCV data
        
    Returns:
        DataFrame with is_valid and quality_score columns
    """
    from pyspark.sql.functions import when, col, lit
    
    # Completeness check: All fields present (already filtered nulls, so = 1.0)
    completeness_score = lit(1.0)
    
    # Consistency checks: Price relationships
    price_checks = (
        (col("high") >= col("low")).cast("double") +
        (col("high") >= col("close")).cast("double") +
        (col("high") >= col("open")).cast("double") +
        (col("low") <= col("close")).cast("double") +
        (col("low") <= col("open")).cast("double")
    ) / 5.0
    
    # Reasonableness checks: Prices > 0, volume >= 0
    reasonableness_checks = (
        (col("open") > 0).cast("double") +
        (col("high") > 0).cast("double") +
        (col("low") > 0).cast("double") +
        (col("close") > 0).cast("double") +
        (col("volume") >= 0).cast("double")
    ) / 5.0
    
    # Calculate quality score: weighted average
    quality_score = (
        completeness_score * 0.4 +
        price_checks * 0.4 +
        reasonableness_checks * 0.2
    )
    
    # Add quality columns
    df = df.withColumn("quality_score", quality_score)
    df = df.withColumn(
        "is_valid",
        when(col("quality_score") >= 0.8, lit(1)).otherwise(lit(0))
    )
    
    return df


def aggregate_to_daily_ohlcv(silver_df: DataFrame) -> DataFrame:
    """
    Aggregate intraday bars to daily OHLCV per symbol.
    
    Aggregations:
    - Open: First open price of the day
    - High: Maximum high price of the day
    - Low: Minimum low price of the day
    - Close: Last close price of the day
    - Volume: Sum of all volume for the day
    - num_bars: Count of intraday bars
    
    Calculated metrics:
    - daily_return: (close - open) / open
    - price_range: high - low
    - avg_price: (high + low) / 2
    
    Args:
        silver_df: DataFrame from Silver layer (should filter is_valid = 1)
        
    Returns:
        DataFrame conforming to GOLD_DAILY_OHLCV_SCHEMA
    """
    from pyspark.sql.functions import (
        date_trunc, first, max, min, last, sum, count, 
        when, col, lit, current_timestamp
    )
    
    # Filter only valid records
    silver_df = silver_df.filter(col("is_valid") == 1)
    
    # Group by symbol and date
    daily_df = silver_df.groupBy(
        col("symbol"),
        date_trunc("day", col("timestamp")).alias("trade_date")
    ).agg(
        first("open", ignorenulls=True).alias("open"),
        max("high").alias("high"),
        min("low").alias("low"),
        last("close", ignorenulls=True).alias("close"),
        sum("volume").alias("volume"),
        count("*").alias("num_bars")
    )
    
    # Calculate metrics
    daily_df = daily_df.withColumn(
        "daily_return",
        when(col("open") != 0, (col("close") - col("open")) / col("open")).otherwise(lit(None))
    )
    daily_df = daily_df.withColumn("price_range", col("high") - col("low"))
    daily_df = daily_df.withColumn("avg_price", (col("high") + col("low")) / 2.0)
    
    # Add updated timestamp
    daily_df = daily_df.withColumn("updated_timestamp", current_timestamp())
    
    # Ensure schema compliance
    daily_df = daily_df.select(
        col("symbol").cast("string"),
        col("trade_date").cast("timestamp"),
        col("open").cast("double"),
        col("high").cast("double"),
        col("low").cast("double"),
        col("close").cast("double"),
        col("volume").cast("bigint"),
        col("num_bars").cast("integer"),
        col("daily_return").cast("double"),
        col("price_range").cast("double"),
        col("avg_price").cast("double"),
        col("updated_timestamp").cast("timestamp")
    )
    
    return daily_df


def calculate_technical_indicators(daily_df: DataFrame) -> DataFrame:
    """
    Calculate technical indicators from daily OHLCV data.
    
    Indicators:
    - SMA 5: 5-day simple moving average of close prices
    - SMA 20: 20-day simple moving average of close prices
    - SMA 50: 50-day simple moving average of close prices
    - Volatility: Standard deviation of daily returns over 20-day window
    
    Args:
        daily_df: DataFrame from gold.daily_ohlcv table
        
    Returns:
        DataFrame conforming to GOLD_ANALYTICS_SCHEMA
    """
    from pyspark.sql.functions import avg, stddev, col, current_timestamp
    
    # Define window specifications
    window_5d = Window.partitionBy("symbol").orderBy("trade_date").rowsBetween(-4, 0)
    window_20d = Window.partitionBy("symbol").orderBy("trade_date").rowsBetween(-19, 0)
    window_50d = Window.partitionBy("symbol").orderBy("trade_date").rowsBetween(-49, 0)
    window_20d_returns = Window.partitionBy("symbol").orderBy("trade_date").rowsBetween(-19, 0)
    
    # Calculate moving averages
    analytics_df = daily_df.withColumn("sma_5", avg("close").over(window_5d))
    analytics_df = analytics_df.withColumn("sma_20", avg("close").over(window_20d))
    analytics_df = analytics_df.withColumn("sma_50", avg("close").over(window_50d))
    
    # Calculate volatility (stddev of daily returns)
    analytics_df = analytics_df.withColumn(
        "volatility",
        stddev("daily_return").over(window_20d_returns)
    )
    
    # Add updated timestamp
    analytics_df = analytics_df.withColumn("updated_timestamp", current_timestamp())
    
    # Select only analytics columns
    analytics_df = analytics_df.select(
        col("symbol").cast("string"),
        col("trade_date").cast("timestamp"),
        col("sma_5").cast("double"),
        col("sma_20").cast("double"),
        col("sma_50").cast("double"),
        col("volatility").cast("double"),
        col("updated_timestamp").cast("timestamp")
    )
    
    return analytics_df


def get_incremental_bronze_data(
    spark,
    bronze_table: str,
    last_processed_timestamp: Optional[datetime] = None,
    batch_ids: Optional[list] = None
) -> DataFrame:
    """
    Get incremental bronze data for processing.
    
    Args:
        spark: SparkSession
        bronze_table: Name of bronze table
        last_processed_timestamp: Only get records ingested after this timestamp
        batch_ids: Only get records with these batch_ids
        
    Returns:
        DataFrame with incremental bronze data
    """
    bronze_df = spark.table(bronze_table)
    
    if last_processed_timestamp:
        bronze_df = bronze_df.filter(
            col("ingestion_timestamp") > lit(last_processed_timestamp)
        )
    
    if batch_ids:
        bronze_df = bronze_df.filter(col("batch_id").isin(batch_ids))
    
    return bronze_df


def get_incremental_silver_data(
    spark,
    silver_table: str,
    last_processed_timestamp: Optional[datetime] = None
) -> DataFrame:
    """
    Get incremental silver data for processing.
    
    Args:
        spark: SparkSession
        silver_table: Name of silver table
        last_processed_timestamp: Only get records processed after this timestamp
        
    Returns:
        DataFrame with incremental silver data
    """
    from pyspark.sql.functions import col, lit
    
    silver_df = spark.table(silver_table)
    
    if last_processed_timestamp:
        silver_df = silver_df.filter(
            col("processed_timestamp") > lit(last_processed_timestamp)
        )
    
    return silver_df


def get_new_dates_for_gold(
    spark,
    daily_ohlcv_table: str,
    silver_table: str
) -> DataFrame:
    """
    Get dates from Silver that are not yet in Gold daily_ohlcv table.
    
    Args:
        spark: SparkSession
        daily_ohlcv_table: Name of gold daily OHLCV table
        silver_table: Name of silver table
        
    Returns:
        DataFrame with new dates to process
    """
    from pyspark.sql.functions import date_trunc, col
    
    # Get dates from silver
    silver_df = spark.table(silver_table).filter(col("is_valid") == 1)
    silver_dates = silver_df.select(
        col("symbol"),
        date_trunc("day", col("timestamp")).alias("trade_date")
    ).distinct()
    
    # Get existing dates from gold
    try:
        gold_df = spark.table(daily_ohlcv_table)
        existing_dates = gold_df.select("symbol", "trade_date").distinct()
        
        # Find new dates (not in gold) using left_anti join
        new_dates = silver_dates.join(
            existing_dates,
            (silver_dates["symbol"] == existing_dates["symbol"]) &
            (silver_dates["trade_date"] == existing_dates["trade_date"]),
            how="left_anti"
        ).select(silver_dates["symbol"], silver_dates["trade_date"])
    except Exception:
        # Table doesn't exist yet, process all dates
        new_dates = silver_dates
    
    return new_dates
