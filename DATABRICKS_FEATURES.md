# Databricks Native Features

This document highlights the Databricks-specific technologies and features used in this POC.

## Delta Live Tables (DLT)

**File**: `notebooks/dlt_pipeline.py`

Delta Live Tables is Databricks' declarative ETL framework designed specifically for medallion architecture. The DLT pipeline provides:

- **Automatic Dependency Management**: DLT automatically manages dependencies between Bronze, Silver, and Gold tables
- **Data Quality Expectations**: Built-in data quality checks using `@dlt.expect` decorators
- **Automatic Retries**: Built-in error handling and retry logic
- **Monitoring**: Built-in observability and monitoring dashboards
- **Declarative Syntax**: Clean, declarative pipeline definitions

### Key Features:
- Streaming ingestion from landing zone using Auto Loader
- Data quality expectations at each layer
- Automatic table optimization (Z-ordering, compaction)
- Unity Catalog integration

## Auto Loader

**Used in**: `02_ingest_bronze_bars.py` and `dlt_pipeline.py`

Auto Loader is Databricks' native file ingestion technology that:
- Automatically detects new files in the landing zone
- Handles schema evolution automatically
- Provides exactly-once semantics via checkpointing
- Efficiently processes large numbers of files

## Unity Catalog

**Used throughout**: All table definitions

Unity Catalog provides:
- Centralized data governance
- Fine-grained access control
- Data lineage tracking
- Schema and table management

All tables are registered in Unity Catalog:
- `main.bronze.bars`
- `main.silver.bars`
- `main.gold.daily_ohlcv`
- `main.gold.analytics`

## Databricks Asset Bundles

**Files**: `databricks.yml`, `resources/dlt_pipeline.yml`, `resources/workflows.yml`

Infrastructure as Code for Databricks:
- Version-controlled pipeline definitions
- Environment-specific configurations (dev, prod)
- Automated deployments
- Resource management

## Databricks Workflows

**File**: `resources/workflows.yml`

Production-ready orchestration:
- Job dependencies and scheduling
- Retry policies and error handling
- Email notifications
- Cron-based scheduling (daily after market close)

## Auto-Optimization

**Enabled in**: All Spark sessions

Databricks auto-optimization features:
- **Optimize Write**: Automatically optimizes write operations
- **Auto Compact**: Automatically compacts small files
- **Z-Ordering**: Automatic Z-ordering on key columns for query performance

Configuration:
```python
.config("spark.databricks.delta.optimizeWrite.enabled", "true")
.config("spark.databricks.delta.autoCompact.enabled", "true")
```

## Structured Streaming

**Used in**: `02_ingest_bronze_bars.py`

Databricks Structured Streaming with:
- Exactly-once semantics via checkpointing
- Automatic file detection
- Schema evolution support
- Real-time processing capabilities

## Photon Engine (Optional)

**Configured in**: `resources/dlt_pipeline.yml`

Photon is Databricks' high-performance query engine that can accelerate:
- Aggregations
- Joins
- Window functions
- Filtering operations

Enable with: `photon: true` in DLT pipeline configuration

## Databricks Runtime Optimizations

All notebooks use Databricks Runtime optimizations:
- Delta Lake extensions pre-configured
- Optimized Spark configurations
- IO caching enabled
- Performance tuning defaults

## Deployment Options

### Option 1: Delta Live Tables (Recommended)
```bash
# Deploy using Databricks Asset Bundles
databricks bundle deploy
```

### Option 2: Traditional Notebooks
Run notebooks individually or via Databricks Workflows

### Option 3: Databricks Workflows
Use the provided `resources/workflows.yml` for automated orchestration

## Benefits of Using Databricks Native Technologies

1. **Simplified Operations**: DLT handles dependency management automatically
2. **Built-in Quality**: Data quality expectations are declarative and monitored
3. **Better Performance**: Auto-optimization and Photon provide better query performance
4. **Production Ready**: Workflows and Asset Bundles provide enterprise-grade orchestration
5. **Cost Optimization**: Auto-compaction and optimization reduce storage costs
6. **Observability**: Built-in monitoring and lineage tracking

## Next Steps

To fully leverage Databricks features:
1. Deploy the DLT pipeline using Asset Bundles
2. Configure Workflows for automated scheduling
3. Enable Photon for production workloads
4. Set up Unity Catalog governance policies
5. Configure monitoring and alerts

