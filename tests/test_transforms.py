"""
Tests for transformation functions in src/transforms.py.
"""

import pytest
from unittest.mock import Mock, MagicMock
from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, 
    DoubleType, LongType, IntegerType
)
from pyspark.sql import Row

from src.transforms import (
    clean_bronze_to_silver,
    aggregate_to_daily_ohlcv,
    calculate_technical_indicators,
    get_incremental_bronze_data,
    get_incremental_silver_data,
    get_new_dates_for_gold
)


@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for testing."""
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("test_transforms") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def bronze_schema():
    """Bronze layer schema."""
    return StructType([
        StructField("symbol", StringType(), nullable=False),
        StructField("timestamp", TimestampType(), nullable=False),
        StructField("open", DoubleType(), nullable=True),
        StructField("high", DoubleType(), nullable=True),
        StructField("low", DoubleType(), nullable=True),
        StructField("close", DoubleType(), nullable=True),
        StructField("volume", LongType(), nullable=True),
        StructField("ingestion_timestamp", TimestampType(), nullable=False),
        StructField("batch_id", StringType(), nullable=True),
    ])


@pytest.fixture
def sample_bronze_data(spark, bronze_schema):
    """Sample bronze data for testing."""
    data = [
        ("AAPL", datetime(2024, 1, 2, 9, 30), 150.0, 152.0, 149.0, 151.0, 1000000, datetime(2024, 1, 2, 10, 0), "20240102"),
        ("AAPL", datetime(2024, 1, 2, 9, 35), 151.0, 153.0, 150.0, 152.0, 1200000, datetime(2024, 1, 2, 10, 0), "20240102"),
        ("MSFT", datetime(2024, 1, 2, 9, 30), 380.0, 382.0, 379.0, 381.0, 500000, datetime(2024, 1, 2, 10, 0), "20240102"),
        # Duplicate with later ingestion (should be kept)
        ("AAPL", datetime(2024, 1, 2, 9, 30), 150.5, 152.5, 149.5, 151.5, 1100000, datetime(2024, 1, 2, 10, 5), "20240102"),
        # Invalid: null close (should be filtered)
        ("INVALID", datetime(2024, 1, 2, 9, 30), 10.0, 12.0, 9.0, None, 10000, datetime(2024, 1, 2, 10, 0), "20240102"),
        # Invalid: null OHLC (should be filtered)
        ("INVALID2", datetime(2024, 1, 2, 9, 30), None, None, None, 10.0, 10000, datetime(2024, 1, 2, 10, 0), "20240102"),
    ]
    return spark.createDataFrame(data, schema=bronze_schema)


class TestCleanBronzeToSilver:
    """Test suite for clean_bronze_to_silver function."""

    def test_clean_bronze_to_silver_basic(self, spark, sample_bronze_data):
        """Test basic cleaning and deduplication."""
        result = clean_bronze_to_silver(sample_bronze_data)
        
        # Should filter out nulls
        assert result.count() == 3  # AAPL (2), MSFT (1) - duplicates removed
        
        # Check schema
        assert "symbol" in result.columns
        assert "timestamp" in result.columns
        assert "open" in result.columns
        assert "high" in result.columns
        assert "low" in result.columns
        assert "close" in result.columns
        assert "volume" in result.columns
        assert "processed_timestamp" in result.columns
        assert "quality_score" in result.columns
        assert "is_valid" in result.columns

    def test_clean_bronze_to_silver_deduplication(self, spark, sample_bronze_data):
        """Test that deduplication keeps most recent record."""
        result = clean_bronze_to_silver(sample_bronze_data)
        
        # Check AAPL 9:30 record - should keep the one with later ingestion_timestamp
        aapl_records = result.filter("symbol = 'AAPL' AND timestamp = '2024-01-02 09:30:00'").collect()
        assert len(aapl_records) == 1
        assert aapl_records[0]["close"] == 151.5  # From the later ingestion

    def test_clean_bronze_to_silver_quality_scores(self, spark, sample_bronze_data):
        """Test that quality scores are calculated."""
        result = clean_bronze_to_silver(sample_bronze_data)
        
        # All valid records should have quality scores
        rows = result.collect()
        for row in rows:
            assert row["quality_score"] is not None
            assert 0.0 <= row["quality_score"] <= 1.0
            assert row["is_valid"] in [0, 1]

    def test_clean_bronze_to_silver_filters_nulls(self, spark, bronze_schema):
        """Test that records with null critical fields are filtered."""
        data = [
            ("SYM1", datetime(2024, 1, 2, 9, 30), 100.0, 102.0, 99.0, 101.0, 1000, datetime(2024, 1, 2, 10, 0), "20240102"),
            ("SYM2", None, 100.0, 102.0, 99.0, 101.0, 1000, datetime(2024, 1, 2, 10, 0), "20240102"),  # null timestamp
            ("SYM3", datetime(2024, 1, 2, 9, 30), 100.0, 102.0, 99.0, None, 1000, datetime(2024, 1, 2, 10, 0), "20240102"),  # null close
            (None, datetime(2024, 1, 2, 9, 30), 100.0, 102.0, 99.0, 101.0, 1000, datetime(2024, 1, 2, 10, 0), "20240102"),  # null symbol
        ]
        df = spark.createDataFrame(data, schema=bronze_schema)
        
        result = clean_bronze_to_silver(df)
        
        # Should only keep SYM1
        assert result.count() == 1
        assert result.collect()[0]["symbol"] == "SYM1"

    def test_clean_bronze_to_silver_price_validation(self, spark, bronze_schema):
        """Test that invalid price relationships are flagged."""
        data = [
            # Valid: high >= low, high >= close, etc.
            ("VALID", datetime(2024, 1, 2, 9, 30), 100.0, 102.0, 99.0, 101.0, 1000, datetime(2024, 1, 2, 10, 0), "20240102"),
            # Invalid: high < low
            ("INVALID1", datetime(2024, 1, 2, 9, 30), 100.0, 98.0, 99.0, 101.0, 1000, datetime(2024, 1, 2, 10, 0), "20240102"),
            # Invalid: negative prices
            ("INVALID2", datetime(2024, 1, 2, 9, 30), -10.0, 102.0, 99.0, 101.0, 1000, datetime(2024, 1, 2, 10, 0), "20240102"),
        ]
        df = spark.createDataFrame(data, schema=bronze_schema)
        
        result = clean_bronze_to_silver(df)
        
        rows = result.collect()
        valid_row = [r for r in rows if r["symbol"] == "VALID"][0]
        invalid1_row = [r for r in rows if r["symbol"] == "INVALID1"][0]
        invalid2_row = [r for r in rows if r["symbol"] == "INVALID2"][0]
        
        # Valid record should have higher quality score
        assert valid_row["quality_score"] > invalid1_row["quality_score"]
        assert valid_row["quality_score"] > invalid2_row["quality_score"]


class TestAggregateToDailyOHLCV:
    """Test suite for aggregate_to_daily_ohlcv function."""

    def test_aggregate_to_daily_ohlcv_basic(self, spark):
        """Test basic daily aggregation."""
        silver_schema = StructType([
            StructField("symbol", StringType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False),
            StructField("open", DoubleType(), nullable=False),
            StructField("high", DoubleType(), nullable=False),
            StructField("low", DoubleType(), nullable=False),
            StructField("close", DoubleType(), nullable=False),
            StructField("volume", LongType(), nullable=False),
            StructField("processed_timestamp", TimestampType(), nullable=False),
            StructField("is_valid", IntegerType(), nullable=True),
            StructField("quality_score", DoubleType(), nullable=True),
        ])
        
        # Multiple bars for same day
        data = [
            ("AAPL", datetime(2024, 1, 2, 9, 30), 150.0, 152.0, 149.0, 151.0, 1000000, datetime(2024, 1, 2, 10, 0), 1, 0.95),
            ("AAPL", datetime(2024, 1, 2, 9, 35), 151.0, 153.0, 150.0, 152.0, 1200000, datetime(2024, 1, 2, 10, 0), 1, 0.95),
            ("AAPL", datetime(2024, 1, 2, 9, 40), 152.0, 154.0, 151.0, 153.0, 1100000, datetime(2024, 1, 2, 10, 0), 1, 0.95),
            ("MSFT", datetime(2024, 1, 2, 9, 30), 380.0, 382.0, 379.0, 381.0, 500000, datetime(2024, 1, 2, 10, 0), 1, 0.95),
        ]
        df = spark.createDataFrame(data, schema=silver_schema)
        
        result = aggregate_to_daily_ohlcv(df)
        
        # Should have 2 rows (one per symbol)
        assert result.count() == 2
        
        # Check AAPL aggregation
        aapl = result.filter("symbol = 'AAPL'").collect()[0]
        assert aapl["open"] == 150.0  # First open
        assert aapl["high"] == 154.0  # Max high
        assert aapl["low"] == 149.0   # Min low
        assert aapl["close"] == 153.0  # Last close
        assert aapl["volume"] == 3300000  # Sum volume
        assert aapl["num_bars"] == 3
        assert aapl["daily_return"] is not None
        assert aapl["price_range"] == 5.0  # 154 - 149
        assert aapl["avg_price"] == 151.5  # (154 + 149) / 2

    def test_aggregate_to_daily_ohlcv_filters_invalid(self, spark):
        """Test that invalid records are filtered."""
        silver_schema = StructType([
            StructField("symbol", StringType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False),
            StructField("open", DoubleType(), nullable=False),
            StructField("high", DoubleType(), nullable=False),
            StructField("low", DoubleType(), nullable=False),
            StructField("close", DoubleType(), nullable=False),
            StructField("volume", LongType(), nullable=False),
            StructField("processed_timestamp", TimestampType(), nullable=False),
            StructField("is_valid", IntegerType(), nullable=True),
            StructField("quality_score", DoubleType(), nullable=True),
        ])
        
        data = [
            ("AAPL", datetime(2024, 1, 2, 9, 30), 150.0, 152.0, 149.0, 151.0, 1000000, datetime(2024, 1, 2, 10, 0), 1, 0.95),
            ("AAPL", datetime(2024, 1, 2, 9, 35), 151.0, 153.0, 150.0, 152.0, 1200000, datetime(2024, 1, 2, 10, 0), 0, 0.5),  # Invalid
        ]
        df = spark.createDataFrame(data, schema=silver_schema)
        
        result = aggregate_to_daily_ohlcv(df)
        
        # Should only aggregate valid records
        aapl = result.filter("symbol = 'AAPL'").collect()[0]
        assert aapl["num_bars"] == 1  # Only the valid one


class TestCalculateTechnicalIndicators:
    """Test suite for calculate_technical_indicators function."""

    def test_calculate_technical_indicators_basic(self, spark):
        """Test basic technical indicator calculation."""
        daily_schema = StructType([
            StructField("symbol", StringType(), nullable=False),
            StructField("trade_date", TimestampType(), nullable=False),
            StructField("open", DoubleType(), nullable=False),
            StructField("high", DoubleType(), nullable=False),
            StructField("low", DoubleType(), nullable=False),
            StructField("close", DoubleType(), nullable=False),
            StructField("volume", LongType(), nullable=False),
            StructField("num_bars", IntegerType(), nullable=True),
            StructField("daily_return", DoubleType(), nullable=True),
            StructField("price_range", DoubleType(), nullable=True),
            StructField("avg_price", DoubleType(), nullable=True),
            StructField("updated_timestamp", TimestampType(), nullable=False),
        ])
        
        # Create 10 days of data for AAPL
        data = []
        base_price = 150.0
        for i in range(10):
            price = base_price + i * 0.5
            data.append((
                "AAPL",
                datetime(2024, 1, 2 + i, 0, 0),
                price,
                price + 1.0,
                price - 1.0,
                price + 0.5,
                1000000,
                10,
                0.003,
                2.0,
                price,
                datetime(2024, 1, 2, 10, 0)
            ))
        
        df = spark.createDataFrame(data, schema=daily_schema)
        
        result = calculate_technical_indicators(df)
        
        # Should have all rows
        assert result.count() == 10
        
        # Check that indicators are calculated
        rows = result.collect()
        for i, row in enumerate(rows):
            if i >= 4:  # SMA 5 needs 5 days
                assert row["sma_5"] is not None
            if i >= 19:  # SMA 20 needs 20 days (won't have any)
                assert row["sma_20"] is not None
            # All should have updated_timestamp
            assert row["updated_timestamp"] is not None

    def test_calculate_technical_indicators_schema(self, spark):
        """Test that output schema is correct."""
        daily_schema = StructType([
            StructField("symbol", StringType(), nullable=False),
            StructField("trade_date", TimestampType(), nullable=False),
            StructField("open", DoubleType(), nullable=False),
            StructField("high", DoubleType(), nullable=False),
            StructField("low", DoubleType(), nullable=False),
            StructField("close", DoubleType(), nullable=False),
            StructField("volume", LongType(), nullable=False),
            StructField("num_bars", IntegerType(), nullable=True),
            StructField("daily_return", DoubleType(), nullable=True),
            StructField("price_range", DoubleType(), nullable=True),
            StructField("avg_price", DoubleType(), nullable=True),
            StructField("updated_timestamp", TimestampType(), nullable=False),
        ])
        
        data = [
            ("AAPL", datetime(2024, 1, 2), 150.0, 152.0, 149.0, 151.0, 1000000, 10, 0.01, 3.0, 150.5, datetime(2024, 1, 2, 10, 0)),
        ]
        df = spark.createDataFrame(data, schema=daily_schema)
        
        result = calculate_technical_indicators(df)
        
        # Check schema
        assert "symbol" in result.columns
        assert "trade_date" in result.columns
        assert "sma_5" in result.columns
        assert "sma_20" in result.columns
        assert "sma_50" in result.columns
        assert "volatility" in result.columns
        assert "updated_timestamp" in result.columns


class TestIncrementalFunctions:
    """Test suite for incremental data retrieval functions."""

    def test_get_incremental_bronze_data(self, spark, bronze_schema):
        """Test incremental bronze data retrieval."""
        # Create test table
        data = [
            ("AAPL", datetime(2024, 1, 2, 9, 30), 150.0, 152.0, 149.0, 151.0, 1000000, datetime(2024, 1, 2, 10, 0), "20240102"),
            ("AAPL", datetime(2024, 1, 3, 9, 30), 151.0, 153.0, 150.0, 152.0, 1200000, datetime(2024, 1, 3, 10, 0), "20240103"),
        ]
        df = spark.createDataFrame(data, schema=bronze_schema)
        df.createOrReplaceTempView("test_bronze")
        
        # Test with timestamp filter
        result = get_incremental_bronze_data(spark, "test_bronze", datetime(2024, 1, 2, 10, 0))
        assert result.count() == 1  # Only the later one
        
        # Test with batch_id filter
        result = get_incremental_bronze_data(spark, "test_bronze", None, ["20240103"])
        assert result.count() == 1
        assert result.collect()[0]["batch_id"] == "20240103"

    def test_get_incremental_silver_data(self, spark):
        """Test incremental silver data retrieval."""
        silver_schema = StructType([
            StructField("symbol", StringType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False),
            StructField("open", DoubleType(), nullable=False),
            StructField("high", DoubleType(), nullable=False),
            StructField("low", DoubleType(), nullable=False),
            StructField("close", DoubleType(), nullable=False),
            StructField("volume", LongType(), nullable=False),
            StructField("processed_timestamp", TimestampType(), nullable=False),
            StructField("is_valid", IntegerType(), nullable=True),
            StructField("quality_score", DoubleType(), nullable=True),
        ])
        
        data = [
            ("AAPL", datetime(2024, 1, 2, 9, 30), 150.0, 152.0, 149.0, 151.0, 1000000, datetime(2024, 1, 2, 10, 0), 1, 0.95),
            ("AAPL", datetime(2024, 1, 3, 9, 30), 151.0, 153.0, 150.0, 152.0, 1200000, datetime(2024, 1, 3, 10, 0), 1, 0.95),
        ]
        df = spark.createDataFrame(data, schema=silver_schema)
        df.createOrReplaceTempView("test_silver")
        
        result = get_incremental_silver_data(spark, "test_silver", datetime(2024, 1, 2, 10, 0))
        assert result.count() == 1  # Only the later one

    def test_get_new_dates_for_gold(self, spark):
        """Test new dates detection for gold processing."""
        silver_schema = StructType([
            StructField("symbol", StringType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False),
            StructField("open", DoubleType(), nullable=False),
            StructField("high", DoubleType(), nullable=False),
            StructField("low", DoubleType(), nullable=False),
            StructField("close", DoubleType(), nullable=False),
            StructField("volume", LongType(), nullable=False),
            StructField("processed_timestamp", TimestampType(), nullable=False),
            StructField("is_valid", IntegerType(), nullable=True),
            StructField("quality_score", DoubleType(), nullable=True),
        ])
        
        daily_schema = StructType([
            StructField("symbol", StringType(), nullable=False),
            StructField("trade_date", TimestampType(), nullable=False),
            StructField("open", DoubleType(), nullable=False),
            StructField("high", DoubleType(), nullable=False),
            StructField("low", DoubleType(), nullable=False),
            StructField("close", DoubleType(), nullable=False),
            StructField("volume", LongType(), nullable=False),
            StructField("num_bars", IntegerType(), nullable=True),
            StructField("daily_return", DoubleType(), nullable=True),
            StructField("price_range", DoubleType(), nullable=True),
            StructField("avg_price", DoubleType(), nullable=True),
            StructField("updated_timestamp", TimestampType(), nullable=False),
        ])
        
        # Silver has 2 dates
        silver_data = [
            ("AAPL", datetime(2024, 1, 2, 9, 30), 150.0, 152.0, 149.0, 151.0, 1000000, datetime(2024, 1, 2, 10, 0), 1, 0.95),
            ("AAPL", datetime(2024, 1, 3, 9, 30), 151.0, 153.0, 150.0, 152.0, 1200000, datetime(2024, 1, 3, 10, 0), 1, 0.95),
        ]
        silver_df = spark.createDataFrame(silver_data, schema=silver_schema)
        silver_df.createOrReplaceTempView("test_silver")
        
        # Gold has 1 date
        gold_data = [
            ("AAPL", datetime(2024, 1, 2, 0, 0), 150.0, 152.0, 149.0, 151.0, 1000000, 10, 0.01, 3.0, 150.5, datetime(2024, 1, 2, 10, 0)),
        ]
        gold_df = spark.createDataFrame(gold_data, schema=daily_schema)
        gold_df.createOrReplaceTempView("test_gold")
        
        result = get_new_dates_for_gold(spark, "test_gold", "test_silver")
        
        # Should find 1 new date (2024-01-03)
        assert result.count() == 1
        new_date = result.collect()[0]
        assert new_date["symbol"] == "AAPL"
        assert new_date["trade_date"].date() == date(2024, 1, 3)

