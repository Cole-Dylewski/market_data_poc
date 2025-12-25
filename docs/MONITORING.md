# Monitoring and Alerting Guide

This guide covers monitoring strategies for the market data pipeline, including DLT expectations, job monitoring, and data quality alerts.

## Table of Contents

1. [DLT Pipeline Monitoring](#dlt-pipeline-monitoring)
2. [Job Monitoring](#job-monitoring)
3. [Data Quality Monitoring](#data-quality-monitoring)
4. [Alerting Configuration](#alerting-configuration)
5. [Dashboard Queries](#dashboard-queries)

---

## DLT Pipeline Monitoring

### Pipeline Health Dashboard

Monitor your DLT pipeline in the Databricks UI:

1. **Navigate to**: Workflows → Delta Live Tables → Your Pipeline
2. **Key Metrics to Monitor**:
   - Pipeline status (Running, Failed, Stopped)
   - Latest update timestamp
   - Records processed per table
   - Data quality metrics (expectations passed/failed)

### DLT Expectations Monitoring

The pipeline includes built-in data quality expectations. Monitor them via SQL:

```sql
-- Check expectation results for Bronze table
SELECT 
    expectation,
    dataset,
    passed_records,
    failed_records,
    input_records,
    (passed_records * 100.0 / input_records) AS pass_rate_pct
FROM system.information_schema.expectations
WHERE dataset = 'bronze_bars'
ORDER BY timestamp DESC
LIMIT 10;
```

### Pipeline Metrics Query

```sql
-- Get pipeline execution metrics
SELECT 
    pipeline_id,
    update_id,
    state,
    started_on,
    completed_on,
    DATEDIFF(second, started_on, completed_on) AS duration_seconds,
    metrics.num_output_rows AS output_rows
FROM system.information_schema.pipeline_state
WHERE pipeline_name = 'Market Data Medallion Pipeline'
ORDER BY started_on DESC
LIMIT 10;
```

---

## Job Monitoring

### Job Run Status

Monitor job execution in Databricks Workflows:

```python
# Example: Check job run status programmatically
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Get job runs
runs = w.jobs.list_runs(
    job_id=YOUR_JOB_ID,
    limit=10
)

for run in runs:
    print(f"Run {run.run_id}: {run.state.result_state}")
    print(f"  Started: {run.start_time}")
    print(f"  Duration: {run.end_time - run.start_time if run.end_time else 'Running'}")
```

### Job Failure Alerts

Configure email notifications in `resources/workflows.yml`:

```yaml
email_notifications:
  on_failure:
    - your-email@example.com
  on_success: []
  on_start: []
```

### Job Metrics Dashboard Query

```sql
-- Create a view for job monitoring (run in a monitoring notebook)
CREATE OR REPLACE TEMP VIEW job_metrics AS
SELECT 
    job_id,
    run_id,
    run_name,
    state.result_state AS status,
    start_time,
    end_time,
    DATEDIFF(second, start_time, end_time) AS duration_seconds,
    tasks[0].state.result_state AS first_task_status
FROM system.information_schema.job_runs
WHERE job_name LIKE '%Market Data%'
ORDER BY start_time DESC;
```

---

## Data Quality Monitoring

### Bronze Layer Quality Checks

```sql
-- Monitor Bronze layer data quality
SELECT 
    DATE(ingestion_timestamp) AS ingestion_date,
    COUNT(*) AS total_records,
    COUNT(DISTINCT symbol) AS unique_symbols,
    SUM(CASE WHEN symbol IS NULL THEN 1 ELSE 0 END) AS null_symbols,
    SUM(CASE WHEN timestamp IS NULL THEN 1 ELSE 0 END) AS null_timestamps,
    SUM(CASE WHEN close IS NULL THEN 1 ELSE 0 END) AS null_closes,
    MIN(ingestion_timestamp) AS first_ingestion,
    MAX(ingestion_timestamp) AS last_ingestion
FROM main.bronze.bars
WHERE ingestion_timestamp >= CURRENT_DATE() - INTERVAL 7 DAYS
GROUP BY DATE(ingestion_timestamp)
ORDER BY ingestion_date DESC;
```

### Silver Layer Quality Metrics

```sql
-- Monitor Silver layer quality scores
SELECT 
    DATE(processed_timestamp) AS process_date,
    COUNT(*) AS total_records,
    SUM(CASE WHEN is_valid = 1 THEN 1 ELSE 0 END) AS valid_records,
    SUM(CASE WHEN is_valid = 0 THEN 1 ELSE 0 END) AS invalid_records,
    AVG(quality_score) AS avg_quality_score,
    MIN(quality_score) AS min_quality_score,
    MAX(quality_score) AS max_quality_score,
    (SUM(CASE WHEN is_valid = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS validity_rate_pct
FROM main.silver.bars
WHERE processed_timestamp >= CURRENT_DATE() - INTERVAL 7 DAYS
GROUP BY DATE(processed_timestamp)
ORDER BY process_date DESC;
```

### Gold Layer Completeness Check

```sql
-- Check Gold layer data completeness
SELECT 
    symbol,
    COUNT(*) AS trading_days,
    MIN(trade_date) AS first_date,
    MAX(trade_date) AS last_date,
    DATEDIFF(day, MIN(trade_date), MAX(trade_date)) AS date_range_days,
    CASE 
        WHEN DATEDIFF(day, MIN(trade_date), MAX(trade_date)) > COUNT(*) * 1.5 
        THEN 'Missing Days Detected'
        ELSE 'Complete'
    END AS completeness_status
FROM main.gold.daily_ohlcv
GROUP BY symbol
HAVING COUNT(*) < 20  -- Flag symbols with less than 20 days
ORDER BY trading_days ASC;
```

### Cross-Layer Consistency Check

```sql
-- Verify data consistency across layers
WITH bronze_counts AS (
    SELECT 
        DATE(timestamp) AS trade_date,
        COUNT(DISTINCT symbol) AS bronze_symbols,
        COUNT(*) AS bronze_records
    FROM main.bronze.bars
    WHERE timestamp >= CURRENT_DATE() - INTERVAL 7 DAYS
    GROUP BY DATE(timestamp)
),
silver_counts AS (
    SELECT 
        DATE(timestamp) AS trade_date,
        COUNT(DISTINCT symbol) AS silver_symbols,
        COUNT(*) AS silver_records
    FROM main.silver.bars
    WHERE processed_timestamp >= CURRENT_DATE() - INTERVAL 7 DAYS
    GROUP BY DATE(timestamp)
),
gold_counts AS (
    SELECT 
        DATE(trade_date) AS trade_date,
        COUNT(DISTINCT symbol) AS gold_symbols,
        COUNT(*) AS gold_records
    FROM main.gold.daily_ohlcv
    WHERE trade_date >= CURRENT_DATE() - INTERVAL 7 DAYS
    GROUP BY DATE(trade_date)
)
SELECT 
    COALESCE(b.trade_date, s.trade_date, g.trade_date) AS trade_date,
    b.bronze_symbols,
    b.bronze_records,
    s.silver_symbols,
    s.silver_records,
    g.gold_symbols,
    g.gold_records,
    CASE 
        WHEN s.silver_records < b.bronze_records * 0.9 THEN 'Warning: High Silver Filter Rate'
        WHEN g.gold_records < s.silver_records * 0.8 THEN 'Warning: High Gold Filter Rate'
        ELSE 'OK'
    END AS consistency_status
FROM bronze_counts b
FULL OUTER JOIN silver_counts s ON b.trade_date = s.trade_date
FULL OUTER JOIN gold_counts g ON COALESCE(b.trade_date, s.trade_date) = g.trade_date
ORDER BY trade_date DESC;
```

---

## Alerting Configuration

### Email Alerts for Data Quality Issues

Create a monitoring notebook that runs daily:

```python
# notebooks/06_monitoring_alerts.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("DataQualityMonitoring").getOrCreate()

# Check for quality issues
silver_quality = spark.sql("""
    SELECT 
        DATE(processed_timestamp) AS process_date,
        AVG(quality_score) AS avg_quality,
        SUM(CASE WHEN is_valid = 0 THEN 1 ELSE 0 END) AS invalid_count
    FROM main.silver.bars
    WHERE processed_timestamp >= CURRENT_DATE() - INTERVAL 1 DAY
    GROUP BY DATE(processed_timestamp)
""").collect()

# Alert if quality drops below threshold
for row in silver_quality:
    if row.avg_quality < 0.8 or row.invalid_count > 100:
        # Send alert (implement your alerting mechanism)
        print(f"ALERT: Quality issue detected on {row.process_date}")
        print(f"  Average Quality Score: {row.avg_quality}")
        print(f"  Invalid Records: {row.invalid_count}")
```

### Slack/Teams Integration

Use Databricks webhooks for Slack/Teams notifications:

```python
import requests
import json

def send_slack_alert(message: str, webhook_url: str):
    """Send alert to Slack channel."""
    payload = {
        "text": f"🚨 Market Data Pipeline Alert\n{message}"
    }
    response = requests.post(webhook_url, json=payload)
    return response.status_code == 200
```

---

## Dashboard Queries

### Pipeline Health Summary

```sql
-- Create a comprehensive monitoring dashboard
CREATE OR REPLACE TEMP VIEW pipeline_health AS
SELECT 
    'Bronze' AS layer,
    COUNT(*) AS total_records,
    COUNT(DISTINCT symbol) AS unique_symbols,
    MAX(ingestion_timestamp) AS last_update
FROM main.bronze.bars
WHERE ingestion_timestamp >= CURRENT_DATE() - INTERVAL 1 DAY

UNION ALL

SELECT 
    'Silver' AS layer,
    COUNT(*) AS total_records,
    COUNT(DISTINCT symbol) AS unique_symbols,
    MAX(processed_timestamp) AS last_update
FROM main.silver.bars
WHERE processed_timestamp >= CURRENT_DATE() - INTERVAL 1 DAY

UNION ALL

SELECT 
    'Gold' AS layer,
    COUNT(*) AS total_records,
    COUNT(DISTINCT symbol) AS unique_symbols,
    MAX(updated_timestamp) AS last_update
FROM main.gold.daily_ohlcv
WHERE trade_date >= CURRENT_DATE() - INTERVAL 1 DAY;
```

### Data Freshness Monitoring

```sql
-- Check data freshness across all layers
SELECT 
    'Bronze' AS layer,
    MAX(ingestion_timestamp) AS last_update,
    DATEDIFF(hour, MAX(ingestion_timestamp), CURRENT_TIMESTAMP()) AS hours_since_update
FROM main.bronze.bars

UNION ALL

SELECT 
    'Silver' AS layer,
    MAX(processed_timestamp) AS last_update,
    DATEDIFF(hour, MAX(processed_timestamp), CURRENT_TIMESTAMP()) AS hours_since_update
FROM main.silver.bars

UNION ALL

SELECT 
    'Gold' AS layer,
    MAX(updated_timestamp) AS last_update,
    DATEDIFF(hour, MAX(updated_timestamp), CURRENT_TIMESTAMP()) AS hours_since_update
FROM main.gold.daily_ohlcv;
```

---

## Best Practices

1. **Set Up Daily Monitoring Jobs**: Schedule a monitoring notebook to run daily after pipeline execution
2. **Configure Alerts**: Set up email/Slack alerts for critical failures
3. **Track Metrics Over Time**: Store monitoring metrics in a separate table for trend analysis
4. **Review Quality Scores**: Regularly review quality scores to identify data source issues
5. **Monitor Pipeline Costs**: Track DLT pipeline compute costs and optimize cluster sizes

---

## Troubleshooting

### Pipeline Not Running

1. Check pipeline status in DLT UI
2. Verify cluster availability
3. Check for configuration errors in `resources/dlt_pipeline.yml`

### Data Quality Issues

1. Review expectation results in DLT UI
2. Check source data quality (Yahoo Finance API)
3. Review quality score breakdowns in Silver layer

### Performance Issues

1. Monitor cluster utilization
2. Check for data skew (uneven distribution)
3. Review query execution plans
4. Consider optimizing Z-ordering and partitioning

