# Deployment Guide: Activating Pipelines in Databricks

The pipelines are **code-complete** but need to be **deployed and activated** in your Databricks workspace. This guide covers all deployment options.

## Current Status

✅ **Code Complete**: All pipeline code is written and ready
⏳ **Needs Deployment**: Pipelines must be deployed to Databricks to run

## Deployment Options

### Option 1: Deploy Using Databricks Asset Bundles (Recommended)

**Prerequisites:**
- Databricks CLI installed and configured
- Authenticated to your Databricks workspace

**Steps:**

1. **Install Databricks CLI** (if not already installed):
   ```bash
   pip install databricks-cli
   ```

2. **Authenticate**:
   ```bash
   databricks configure --token
   ```

3. **Set environment variable** (or update `databricks.yml`):
   ```bash
   export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
   ```

4. **Deploy the DLT Pipeline**:
   ```bash
   cd market_data_poc
   databricks bundle deploy
   ```

5. **Deploy Workflows** (optional):
   ```bash
   databricks bundle deploy -t resources/workflows.yml
   ```

6. **Start the DLT Pipeline**:
   - Go to Databricks UI → Workflows → Delta Live Tables
   - Find "Market Data Medallion Pipeline"
   - Click "Start" to run the pipeline

---

### Option 2: Manual Deployment via Databricks UI

#### A. Deploy DLT Pipeline Manually

1. **Create DLT Pipeline**:
   - Go to Databricks UI → Workflows → Delta Live Tables
   - Click "Create Pipeline"
   - Pipeline name: `Market Data Medallion Pipeline`
   - Pipeline type: `Python`
   - Source code: Upload or point to `notebooks/dlt_pipeline.py`
   - Target schema: `main` (or your catalog)
   - Storage location: `/Volumes/main/default/dlt_storage`
   - Cluster configuration:
     - Node type: `i3.xlarge`
     - Workers: 2
     - Enable Photon: Yes
   - Advanced settings:
     - Enable auto-optimization
     - Enable auto-compaction

2. **Configure Pipeline Settings**:
   - Development mode: `true` (for testing)
   - Continuous: `false` (triggered runs)
   - Add configuration:
     ```
     raw_bars_path: /Volumes/main/default/raw_market_data/bars
     bronze_catalog: main
     bronze_schema: bronze
     silver_catalog: main
     silver_schema: silver
     gold_catalog: main
     gold_schema: gold
     ```

3. **Start the Pipeline**:
   - Click "Start" to run the pipeline
   - Monitor progress in the pipeline UI

#### B. Deploy Workflows Manually

1. **Create Data Collection Job**:
   - Go to Databricks UI → Workflows → Jobs
   - Click "Create Job"
   - Name: `Market Data Collection`
   - Add task:
     - Type: Notebook
     - Path: `./notebooks/01_collect_raw_data.py`
     - Cluster: Single node (i3.xlarge)
     - Libraries: `yfinance>=0.2.0`, `beautifulsoup4>=4.12.0`
   - Schedule: Daily at 5 PM ET (after market close)
   - Save

2. **Create DLT Pipeline Job**:
   - Create new job: `Market Data Medallion Pipeline (DLT)`
   - Add task:
     - Type: Delta Live Tables pipeline
     - Pipeline: Select the DLT pipeline created above
   - Save

3. **Create Quality Checks Job**:
   - Create new job: `Market Data Quality Checks`
   - Add task:
     - Type: Notebook
     - Path: `./notebooks/05_data_quality_checks.py`
     - Depends on: DLT Pipeline job
   - Save

---

### Option 3: Use Traditional Notebooks (No Deployment Needed)

If you prefer not to use DLT, you can run the traditional notebooks directly:

1. **Run notebooks in order**:
   - `00_setup.py` - One-time setup
   - `01_collect_raw_data.py` - Collect data
   - `02_ingest_bronze_bars.py` - Bronze ingestion (streaming)
   - `03_transform_silver_bars.py` - Silver transformation
   - `04_gold_analytics.py` - Gold analytics
   - `05_data_quality_checks.py` - Quality checks

2. **Or create a simple workflow**:
   - Create a multi-task job in Databricks Workflows
   - Add each notebook as a task with dependencies

---

## Activating the Pipelines

### For DLT Pipeline:

**One-time Setup:**
1. Deploy the pipeline (using Option 1 or 2A above)
2. First run will create all tables in Unity Catalog

**Running the Pipeline:**
- **Manual Run**: Click "Start" in the DLT pipeline UI
- **Scheduled Run**: Create a workflow job that triggers the DLT pipeline
- **Continuous Mode**: Change `continuous: true` in config for real-time processing

**Monitoring:**
- View pipeline runs in the DLT UI
- Check data quality metrics
- Monitor table updates in Unity Catalog

### For Workflows:

**Activation:**
1. Deploy workflows (using Option 1 or 2B above)
2. Workflows will run automatically based on schedule
3. Or trigger manually by clicking "Run Now"

**Dependencies:**
- Data Collection → DLT Pipeline → Quality Checks
- Each job waits for the previous to complete

---

## Verification Steps

After deployment, verify everything is working:

1. **Check DLT Pipeline**:
   ```python
   # In a Databricks notebook
   spark.sql("SHOW TABLES IN main.bronze").show()
   spark.sql("SHOW TABLES IN main.silver").show()
   spark.sql("SHOW TABLES IN main.gold").show()
   ```

2. **Check Data Flow**:
   - Run data collection job
   - Verify files appear in landing zone
   - Run DLT pipeline
   - Verify data appears in Bronze → Silver → Gold tables

3. **Check Quality**:
   - Run quality checks job
   - Review quality metrics

---

## Troubleshooting

### DLT Pipeline Not Starting
- Check that Unity Catalog is enabled
- Verify catalog and schema exist
- Check cluster permissions
- Review pipeline logs for errors

### Tables Not Created
- Ensure DLT pipeline has run at least once
- Check Unity Catalog permissions
- Verify target schema is correct

### Workflows Not Running
- Check schedule configuration
- Verify job permissions
- Review job run history for errors

### Import Errors
- Ensure `src/` directory is accessible
- Check Python path configuration
- Verify all dependencies are installed

---

## Next Steps After Deployment

1. **Monitor First Run**: Watch the DLT pipeline create all tables
2. **Verify Data**: Check that data flows through all layers
3. **Review Quality Metrics**: Ensure data quality expectations pass
4. **Set Up Alerts**: Configure email notifications for failures
5. **Optimize**: Review query performance and adjust cluster sizes if needed

---

## Quick Start (Fastest Path)

For the quickest deployment:

1. **Manual DLT Pipeline** (5 minutes):
   - UI → Workflows → Delta Live Tables → Create Pipeline
   - Upload `notebooks/dlt_pipeline.py`
   - Configure settings
   - Click "Start"

2. **Manual Workflow** (3 minutes):
   - UI → Workflows → Jobs → Create Job
   - Add notebook task: `01_collect_raw_data.py`
   - Set schedule
   - Save and run

This gets you up and running quickly, then you can refine with Asset Bundles later.

