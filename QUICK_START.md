# Quick Start: Get Pipelines Running in 10 Minutes

## Fastest Path to Running Pipelines

### Step 1: Manual DLT Pipeline Setup (5 minutes)

1. **Open Databricks UI**:
   - Navigate to: Workflows → Delta Live Tables

2. **Create Pipeline**:
   - Click "Create Pipeline"
   - Name: `Market Data Medallion Pipeline`
   - Source type: `Notebook`
   - Path: Browse to `notebooks/dlt_pipeline.py` in your workspace
   - Target schema: `main` (or your catalog)
   - Storage: `/Volumes/main/default/dlt_storage`

3. **Configure Cluster**:
   - Node type: `i3.xlarge` (or smaller for testing)
   - Workers: 1 (for testing) or 2 (for production)
   - Enable Photon: Yes (if available)

4. **Add Configuration**:
   - Click "Advanced" → "Configuration"
   - Add these key-value pairs:
     ```
     raw_bars_path = /Volumes/main/default/raw_market_data/bars
     bronze_catalog = main
     bronze_schema = bronze
     silver_catalog = main
     silver_schema = silver
     gold_catalog = main
     gold_schema = gold
     ```

5. **Start Pipeline**:
   - Click "Start" button
   - First run will create all tables (Bronze, Silver, Gold)

### Step 2: Create Data Collection Job (3 minutes)

1. **Open Databricks UI**:
   - Navigate to: Workflows → Jobs

2. **Create Job**:
   - Click "Create Job"
   - Name: `Market Data Collection`

3. **Add Task**:
   - Click "Add task" → "Notebook"
   - Task name: `collect_raw_data`
   - Source: Workspace
   - Path: `./notebooks/01_collect_raw_data.py`
   - Cluster: Single node (i3.xlarge or smaller)
   - Libraries: Add PyPI packages:
     - `yfinance>=0.2.0`
     - `beautifulsoup4>=4.12.0`

4. **Set Schedule** (optional):
   - Click "Schedule" tab
   - Schedule type: `Scheduled`
   - Cron: `0 0 17 * * ?` (5 PM daily)
   - Timezone: `America/New_York`

5. **Save and Run**:
   - Click "Save"
   - Click "Run Now" to test

### Step 3: Verify Everything Works (2 minutes)

1. **Check Data Collection**:
   - Run the data collection job
   - Verify files appear in: `/Volumes/main/default/raw_market_data/bars/`

2. **Check DLT Pipeline**:
   - Go to DLT pipeline UI
   - Click "Start" to process the new files
   - Watch the pipeline create/update tables

3. **Verify Tables**:
   ```python
   # Run in a Databricks notebook
   spark.sql("SHOW TABLES IN main.bronze").show()
   spark.sql("SHOW TABLES IN main.silver").show()
   spark.sql("SHOW TABLES IN main.gold").show()
   
   # Check data
   spark.table("main.bronze.bars").count()
   spark.table("main.silver.bars").count()
   spark.table("main.gold.daily_ohlcv").count()
   ```

## That's It!

You now have:
- ✅ DLT pipeline running (Bronze → Silver → Gold)
- ✅ Data collection job scheduled
- ✅ All tables created in Unity Catalog

## Next Steps

- **Monitor**: Check DLT pipeline runs and data quality metrics
- **Schedule DLT**: Create a workflow job to trigger DLT pipeline after data collection
- **Add Quality Checks**: Create a job for `05_data_quality_checks.py`

## Troubleshooting

**DLT Pipeline Errors**:
- Ensure Unity Catalog is enabled
- Check that catalog `main` exists
- Verify cluster has proper permissions

**No Data in Tables**:
- Run data collection job first
- Then run DLT pipeline to process the files
- Check landing zone has JSON files

**Import Errors**:
- Ensure `src/` directory is in workspace
- Check Python path in DLT pipeline notebook

