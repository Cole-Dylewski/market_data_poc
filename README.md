# Databricks Market Data POC

## Overview

This project is a proof-of-concept data engineering pipeline that demonstrates how financial market data can be ingested from multiple public and free data sources into Databricks using Apache Spark and Delta Lake. The primary goal is to showcase modern lakehouse design patterns, incremental ingestion techniques, and Spark-based analytical transformations in a Databricks environment.

The project supports multiple market data sources, including Alpaca (optional, requires API keys) and other public/free data sources that can be used without authentication. This makes the repository easily cloneable and usable by anyone without requiring API keys or secrets.

The project is intentionally scoped as a technical demonstration rather than a production system. It emphasizes clarity, correctness, and architectural best practices over enterprise-scale operational complexity.

---

## Objectives

* Demonstrate ingestion of external REST API data into Databricks
* Apply a medallion (bronze / silver / gold) data architecture
* Use Delta Lake for reliable, repeatable data processing
* Showcase Spark transformations, window functions, and incremental loads
* Present clean, interview-ready code and documentation suitable for a portfolio project

---

## Architecture

The pipeline follows a standard Databricks lakehouse pattern:

### Bronze Layer (Raw Ingestion)

* Data is pulled from multiple market data sources (e.g., Alpaca, Yahoo Finance, Alpha Vantage, etc.)
* Raw records are ingested with minimal transformation
* Metadata such as ingestion timestamp and data source are added
* Data is written append-only to Delta tables

### Silver Layer (Cleaned & Normalized)

* Schema enforcement and type casting
* Deduplication based on symbol and timestamp
* Standardized column names and data types
* Idempotent processing using Delta MERGE operations

### Gold Layer (Analytics & Aggregates)

* Daily OHLCV aggregations per symbol
* Simple technical indicators (e.g., moving averages, returns)
* Analytics-ready tables optimized for querying and visualization

---

## Technology Stack

* Python
* Apache Spark
* Delta Lake
* Databricks (Free Edition)
* Multiple Market Data Sources:
  * **Yahoo Finance** (✅ implemented, free, no API keys required)
  * Alpaca Market Data API (optional, requires API keys, TODO)
  * Alpha Vantage (free tier available, TODO)
  * Other public/free market data APIs
* REST-based ingestion using `requests`
* Web scraping for symbol lists using `beautifulsoup4` (S&P 500 symbols from stockanalysis.com)
* Testing framework: `pytest` with comprehensive test coverage

---

## Project Structure

```
databricks-market-data-poc/
├── README.md
├── base_environment.yml        # Databricks serverless environment config
├── databricks.yml             # Databricks asset bundle configuration
├── requirements.txt           # Python package dependencies
├── LICENSE
├── notebooks/
│   ├── 00_setup.py
│   ├── 01_ingest_bronze_bars.py  # Symbol scraping & data ingestion
│   ├── 02_transform_silver_bars.py
│   ├── 03_gold_analytics.py
│   └── 04_data_quality_checks.py
├── src/
│   ├── data_sources/          # Data source clients
│   │   ├── __init__.py
│   │   ├── base_client.py     # Abstract base class for data sources
│   │   ├── yahoo_finance.py   # Yahoo Finance REST API client (✅ implemented)
│   │   └── alpaca_client.py   # Alpaca API client (optional, TODO)
│   ├── config.py
│   ├── schemas.py
│   ├── transforms.py
│   └── utils.py
└── tests/
    ├── data_sources/
    │   └── test_yahoo_finance.py  # Unit tests for Yahoo Finance client
    └── test_utils.py              # Unit tests for utility functions (S&P 500 scraper)
```

### Notebooks

* `00_setup.py`
  Environment setup, configuration loading, and shared utilities

* `01_ingest_bronze_bars.py`
  Generates list of S&P 500 stock symbols (scraped from stockanalysis.com) and downloads market bar data from configured data sources. Features dynamic path resolution that works in local Python, Databricks Repos, and Workspace environments. Saves data to JSON files by symbol for bronze layer processing.

* `02_transform_silver_bars.py`
  Cleans, deduplicates, and normalizes bronze data into silver tables

* `03_gold_analytics.py`
  Builds aggregated and analytical datasets for downstream use

* `04_data_quality_checks.py`
  Basic data validation and sanity checks

### Source Code

* `data_sources/`
  Modular data source clients that can be easily extended:
  * `base_client.py`: Abstract base class defining the interface for all data sources (✅ implemented)
  * `yahoo_finance.py`: Yahoo Finance REST API client with retry logic and error handling (✅ implemented, fully tested)
  * `alpaca_client.py`: Alpaca API client (optional, requires API keys, TODO)
  * Additional data sources can be added by implementing the base client interface

* `schemas.py`
  Explicit Spark schemas used across ingestion and transformation layers

* `transforms.py`
  Reusable Spark transformation logic

* `utils.py`
  Utility functions including S&P 500 symbol scraper (`get_sp500_symbols()`), logging, date/time handling, and common operations

---

## Data Ingestion Approach

* **Symbol Discovery**: Scrapes S&P 500 stock symbols from stockanalysis.com to generate a curated list of 500+ major US stocks. The scraper (`get_sp500_symbols()` in `src/utils.py`) automatically extracts symbols from the HTML table and validates them.
* **Dynamic Path Resolution**: Notebooks automatically detect the project root regardless of where the repository is cloned (local Python, Databricks Repos, or Workspace environments), making the project easily shareable and cloneable.
* **Unified Interface**: All data sources implement `BaseMarketDataClient` for consistent API
* **Yahoo Finance Integration**: Fully implemented REST API client with:
  - Automatic retry logic with exponential backoff
  - Rate limiting handling
  - Standardized OHLCV data format
  - No API keys required
* **Flexible Configuration**: Supports parameterized symbols, timeframes, and time windows
* **Incremental Ingestion**: Designed for fetching last N days/hours of data
* **Error Handling**: Robust error handling and logging throughout
* **Output Format**: Saves data to JSON files organized by symbol for bronze layer processing

Streaming and WebSocket ingestion are intentionally out of scope for this POC.

---

## Data Quality & Reliability

Although this is a demonstration project, basic quality checks are included:

* Non-null constraints on key fields (symbol, timestamp, close)
* Deduplication guarantees at the silver layer
* Simple row count and timestamp range validation
* Idempotent re-runs using Delta Lake semantics

---

## Security & Configuration

* API keys are not hardcoded
* Credentials are expected to be provided via environment variables or notebook-scoped configuration
* No secrets are committed to the repository
* **The project can be used without any API keys** by defaulting to free data sources (e.g., Yahoo Finance)
* Alpaca and other premium data sources are optional and only required if explicitly configured

---

## How to Run

### Local Development

1. **Clone the repository:**
   ```bash
   git clone https://github.com/Cole-Dylewski/databricks-market-data-poc.git
   cd databricks-market-data-poc
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Run tests:**
   ```bash
   pytest tests/ -v
   ```

### Databricks Setup

1. **Import the project into a Databricks workspace:**
   - Use Databricks Repos to connect the repository (recommended), or
   - Manually upload the notebooks and source files

2. **Configure environment:**
   - For **serverless notebooks**: 
     - Upload `base_environment.yml` to Databricks workspace
     - Go to Settings → Workspace admin → Compute
     - Click "Manage" next to "Base environments for serverless compute"
     - Create new environment and upload `base_environment.yml`
     - Set as default if desired
   - For **cluster-based notebooks**: 
     - Install packages from `requirements.txt` in your Databricks cluster
     - Note: PySpark and Delta Lake are typically pre-installed in Databricks

3. **Configure data sources (optional):**
   - By default, the project uses Yahoo Finance which requires no API keys
   - To use Alpaca or other premium sources, set API credentials as environment variables or notebook parameters
   - See `src/config.py` for data source configuration options

4. **Run notebooks in order:**
   1. `00_setup.py` - Environment setup
   2. `01_ingest_bronze_bars.py` - Generate symbol list and download market data
   3. `02_transform_silver_bars.py` - Clean and normalize data
   4. `03_gold_analytics.py` - Build analytics tables
   5. `04_data_quality_checks.py` - Validate data quality

---

## Intended Audience

This project is designed for:

* Hiring managers evaluating data engineering candidates
* Interview discussions around Databricks, Spark, and lakehouse design
* Demonstrating architectural thinking and clean implementation practices

It is not intended for live trading, real-time analytics, or production deployment.

---

## Implementation Status

### ✅ Completed
* Yahoo Finance REST API client with full test coverage
* Base client abstract interface
* S&P 500 symbol scraper from stockanalysis.com with comprehensive test coverage
* Dynamic path resolution for notebooks (works in local Python, Databricks Repos, and Workspace)
* Databricks base environment configuration
* Testing framework setup with pytest

### 🚧 In Progress
* Bronze layer ingestion pipeline
* Symbol list generation and persistence

### 📋 TODO
* Alpaca API client implementation
* Silver layer transformations
* Gold layer analytics
* Delta Lake table schemas
* Data quality checks
* Databricks Jobs orchestration
* Structured Streaming ingestion
* Secret scopes / key vault integration
* CI/CD for notebooks and schemas
* Advanced data quality frameworks
* Visualization dashboards

---

## License

This project is licensed under the MIT License.
