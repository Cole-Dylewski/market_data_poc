# Performance Optimization Guide

This guide covers performance optimization strategies for the market data pipeline, including partitioning, Z-ordering, query tuning, and Delta Lake optimization.

## Table of Contents

1. [Partitioning Strategies](#partitioning-strategies)
2. [Z-Ordering Optimization](#z-ordering-optimization)
3. [Delta Lake Optimization](#delta-lake-optimization)
4. [Query Performance Tuning](#query-performance-tuning)
5. [Cluster Configuration](#cluster-configuration)
6. [Best Practices](#best-practices)

---

## Partitioning Strategies

### Current Partitioning

The pipeline uses date-based partitioning for efficient time-range queries:

```sql
-- Bronze table partitioning (implicit via batch_id)
-- Silver table: Partitioned by date
-- Gold tables: Partitioned by trade_date
```

### Recommended Partitioning

For better performance with large datasets:

```sql
-- Option 1: Partition by date and symbol (for high-cardinality symbols)
ALTER TABLE main.silver.bars
PARTITIONED BY (trade_date, symbol);

-- Option 2: Partition by date only (recommended for most cases)
ALTER TABLE main.gold.daily_ohlcv
PARTITIONED BY (trade_date);
```

### Partitioning Best Practices

1. **Avoid Over-Partitioning**: Too many small partitions hurt performance
2. **Partition by Query Patterns**: Partition on columns frequently used in WHERE clauses
3. **Consider Partition Size**: Aim for 1-10GB per partition
4. **Use Date Partitioning**: For time-series data, partition by date

---

## Z-Ordering Optimization

### What is Z-Ordering?

Z-ordering (multi-dimensional clustering) optimizes data layout for queries that filter on multiple columns.

### Apply Z-Ordering

```sql
-- Optimize Bronze table for symbol + timestamp queries
OPTIMIZE main.bronze.bars
ZORDER BY (symbol, timestamp);

-- Optimize Silver table for symbol + timestamp queries
OPTIMIZE main.silver.bars
ZORDER BY (symbol, timestamp);

-- Optimize Gold daily OHLCV for symbol + trade_date queries
OPTIMIZE main.gold.daily_ohlcv
ZORDER BY (symbol, trade_date);

-- Optimize Gold analytics for symbol + trade_date queries
OPTIMIZE main.gold.analytics
ZORDER BY (symbol, trade_date);
```

### Automated Z-Ordering

Configure in DLT pipeline (`notebooks/dlt_pipeline.py`):

```python
@dlt.table(
    name="bronze_bars",
    table_properties={
        "pipelines.autoOptimize.zOrderCols": "symbol,timestamp"
    }
)
```

Or in `resources/dlt_pipeline.yml`:

```yaml
spark_conf:
  spark.databricks.delta.optimizeWrite.enabled: "true"
  spark.databricks.delta.autoCompact.enabled: "true"
```

---

## Delta Lake Optimization

### Optimize Command

Run regularly to compact small files and improve query performance:

```sql
-- Optimize all tables
OPTIMIZE main.bronze.bars;
OPTIMIZE main.silver.bars;
OPTIMIZE main.gold.daily_ohlcv;
OPTIMIZE main.gold.analytics;
```

### Auto-Optimization

Enable in DLT pipeline configuration:

```yaml
spark_conf:
  spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite: "true"
  spark.databricks.delta.properties.defaults.autoOptimize.autoCompact: "true"
```

### Vacuum Old Files

Remove old Delta files to save storage:

```sql
-- Remove files older than 7 days (retain for time travel)
VACUUM main.bronze.bars RETAIN 168 HOURS;

-- Remove files older than 30 days
VACUUM main.silver.bars RETAIN 720 HOURS;
VACUUM main.gold.daily_ohlcv RETAIN 720 HOURS;
```

**⚠️ Warning**: VACUUM removes time travel capability. Only run after ensuring you don't need historical versions.

### Table Statistics

Update table statistics for better query planning:

```sql
ANALYZE TABLE main.bronze.bars COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE main.silver.bars COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE main.gold.daily_ohlcv COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE main.gold.analytics COMPUTE STATISTICS FOR ALL COLUMNS;
```

---

## Query Performance Tuning

### Use Predicate Pushdown

Always filter early in queries:

```sql
-- ✅ Good: Filter early
SELECT * FROM main.gold.daily_ohlcv
WHERE trade_date >= CURRENT_DATE() - INTERVAL 7 DAYS
  AND symbol = 'AAPL';

-- ❌ Bad: Filter late
SELECT * FROM (
    SELECT * FROM main.gold.daily_ohlcv
) WHERE trade_date >= CURRENT_DATE() - INTERVAL 7 DAYS;
```

### Avoid Full Table Scans

Use partition pruning:

```sql
-- ✅ Good: Uses partition pruning
SELECT * FROM main.gold.daily_ohlcv
WHERE trade_date = '2024-01-15';

-- ❌ Bad: Full table scan
SELECT * FROM main.gold.daily_ohlcv
WHERE DATE(trade_date) = '2024-01-15';  -- Function on partition column
```

### Limit Result Sets

Always use LIMIT for exploratory queries:

```sql
-- ✅ Good
SELECT * FROM main.gold.daily_ohlcv
WHERE trade_date >= CURRENT_DATE() - INTERVAL 7 DAYS
LIMIT 100;

-- ❌ Bad: Returns all rows
SELECT * FROM main.gold.daily_ohlcv
WHERE trade_date >= CURRENT_DATE() - INTERVAL 7 DAYS;
```

### Use Appropriate Joins

Prefer broadcast joins for small tables:

```sql
-- For small dimension tables, use broadcast join hint
SELECT /*+ BROADCAST(small_table) */ 
    d.*, 
    s.attribute
FROM main.gold.daily_ohlcv d
INNER JOIN small_table s ON d.symbol = s.symbol;
```

### Window Functions Optimization

Use RANGE BETWEEN for time-based windows:

```sql
-- ✅ Good: Uses RANGE for time-based windows
SELECT 
    symbol,
    trade_date,
    AVG(close) OVER (
        PARTITION BY symbol 
        ORDER BY trade_date 
        RANGE BETWEEN INTERVAL 19 DAYS PRECEDING AND CURRENT ROW
    ) AS sma_20
FROM main.gold.daily_ohlcv;

-- Alternative: ROWS for fixed number of rows
SELECT 
    symbol,
    trade_date,
    AVG(close) OVER (
        PARTITION BY symbol 
        ORDER BY trade_date 
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS sma_20
FROM main.gold.daily_ohlcv;
```

---

## Cluster Configuration

### Recommended Cluster Settings

For DLT pipeline:

```yaml
clusters:
  - label: "default"
    num_workers: 2-4  # Adjust based on data volume
    node_type_id: "i3.xlarge"  # Memory-optimized for analytics
    spark_conf:
      spark.sql.adaptive.enabled: "true"
      spark.sql.adaptive.coalescePartitions.enabled: "true"
      spark.databricks.delta.optimizeWrite.enabled: "true"
      spark.databricks.delta.autoCompact.enabled: "true"
```

### Spark Configuration Tuning

```python
# In notebook or DLT pipeline
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")

# For large datasets
spark.conf.set("spark.sql.shuffle.partitions", "200")  # Default: 200
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB
```

### Photon Engine

Enable Photon for faster queries:

```yaml
photon: true
```

Or in Spark config:

```python
spark.conf.set("spark.databricks.photon.enabled", "true")
```

---

## Best Practices

### 1. Regular Optimization Schedule

Set up a scheduled job to optimize tables:

```python
# notebooks/07_optimize_tables.py
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TableOptimization").getOrCreate()

# Optimize all tables
tables = [
    "main.bronze.bars",
    "main.silver.bars",
    "main.gold.daily_ohlcv",
    "main.gold.analytics"
]

for table in tables:
    print(f"Optimizing {table}...")
    spark.sql(f"OPTIMIZE {table} ZORDER BY (symbol, timestamp)")
    print(f"Completed {table}")
```

### 2. Monitor Query Performance

Use Databricks SQL query history to identify slow queries:

```sql
-- View slow queries
SELECT 
    query_text,
    execution_time_ms,
    rows_read,
    bytes_read
FROM system.information_schema.query_history
WHERE execution_time_ms > 10000  -- Queries taking > 10 seconds
ORDER BY execution_time_ms DESC
LIMIT 10;
```

### 3. Cache Frequently Used Tables

For tables accessed repeatedly:

```python
# Cache Silver table if used in multiple queries
spark.table("main.silver.bars").cache()
```

### 4. Use Delta Lake Time Travel Sparingly

Time travel queries can be slow. Use for specific use cases only:

```sql
-- ✅ Good: Specific point-in-time query
SELECT * FROM main.silver.bars
VERSION AS OF 10
WHERE symbol = 'AAPL';

-- ❌ Avoid: Scanning all versions
SELECT * FROM main.silver.bars
TIMESTAMP AS OF '2024-01-01'
WHERE symbol = 'AAPL';
```

### 5. Optimize Write Operations

Use Delta Lake optimizations for writes:

```python
# Enable optimize write
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# Write with optimal partition size
df.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("main.silver.bars")
```

---

## Performance Monitoring

### Check Table Statistics

```sql
DESCRIBE DETAIL main.gold.daily_ohlcv;
DESCRIBE HISTORY main.gold.daily_ohlcv;
```

### Monitor File Sizes

```sql
SELECT 
    path,
    size,
    modificationTime
FROM delta.`/path/to/table`
ORDER BY modificationTime DESC
LIMIT 10;
```

### Query Execution Plans

Use EXPLAIN to understand query plans:

```sql
EXPLAIN SELECT * FROM main.gold.daily_ohlcv
WHERE trade_date >= CURRENT_DATE() - INTERVAL 7 DAYS;
```

---

## Troubleshooting Performance Issues

### Issue: Slow Queries

1. Check if tables are optimized: `DESCRIBE DETAIL table_name`
2. Verify partition pruning is working
3. Check for data skew
4. Review query execution plan

### Issue: Small Files Problem

1. Run `OPTIMIZE` command
2. Enable auto-compaction
3. Adjust write batch sizes

### Issue: High Memory Usage

1. Increase cluster size
2. Enable disk spill: `spark.sql.shuffle.spill.enabled = true`
3. Reduce partition count

---

## Summary

Key optimization strategies:

1. ✅ **Partition** by date for time-series queries
2. ✅ **Z-order** by (symbol, timestamp) for multi-column filters
3. ✅ **Optimize** tables regularly (daily/weekly)
4. ✅ **Enable** auto-optimization in DLT pipeline
5. ✅ **Use** predicate pushdown in queries
6. ✅ **Monitor** query performance and adjust

For production workloads, schedule regular optimization jobs and monitor performance metrics.

