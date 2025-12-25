# Troubleshooting Guide

Common issues and solutions for the market data pipeline.

## Table of Contents

1. [Pipeline Issues](#pipeline-issues)
2. [Data Quality Issues](#data-quality-issues)
3. [Performance Issues](#performance-issues)
4. [Connection Issues](#connection-issues)
5. [Schema Issues](#schema-issues)

---

## Pipeline Issues

### DLT Pipeline Not Starting

**Symptoms**: Pipeline shows "Stopped" or "Failed" status

**Solutions**:
1. Check cluster availability
2. Verify configuration in `resources/dlt_pipeline.yml`
3. Check for syntax errors in `notebooks/dlt_pipeline.py`
4. Review pipeline logs in Databricks UI

### Job Failing with Import Errors

**Symptoms**: `ModuleNotFoundError` or import failures

**Solutions**:
1. Verify all dependencies in `requirements.txt`
2. Check if libraries are installed on cluster
3. Ensure `src/` directory is in Python path
4. Use Databricks Repos for automatic path resolution

### Data Not Appearing in Tables

**Symptoms**: Tables exist but are empty

**Solutions**:
1. Check if data collection job ran successfully
2. Verify file paths in configuration
3. Check Unity Catalog permissions
4. Review DLT pipeline expectations (may be filtering data)

---

## Data Quality Issues

### High Invalid Record Rate

**Symptoms**: Many records with `is_valid = 0` in Silver layer

**Solutions**:
1. Check source data quality (Yahoo Finance API)
2. Review quality score breakdowns
3. Adjust quality thresholds if needed
4. Check for data source changes

### Missing Data for Symbols

**Symptoms**: Some symbols missing from Gold tables

**Solutions**:
1. Check if symbols exist in Bronze layer
2. Verify Silver layer filtering logic
3. Check for date range issues
4. Review deduplication logic

---

## Performance Issues

### Slow Query Performance

**Solutions**:
1. Run `OPTIMIZE` on tables
2. Check if Z-ordering is applied
3. Verify partition pruning is working
4. Review query execution plans

### High Memory Usage

**Solutions**:
1. Increase cluster size
2. Enable disk spill
3. Reduce partition count
4. Optimize window functions

---

## Connection Issues

### Cannot Connect to Databricks

**Solutions**:
1. Verify Databricks host URL
2. Check authentication token
3. Verify network connectivity
4. Check firewall rules

### Unity Catalog Access Denied

**Solutions**:
1. Verify catalog permissions
2. Check schema access
3. Verify table permissions
4. Review IAM roles

---

## Schema Issues

### Schema Evolution Errors

**Symptoms**: `SchemaMismatchError` or similar

**Solutions**:
1. Enable schema evolution: `mergeSchema = "true"`
2. Review schema changes in source data
3. Update schema definitions in `src/schemas.py`
4. Use `ALTER TABLE` to add new columns

### Type Casting Errors

**Solutions**:
1. Check source data types
2. Review transformation logic
3. Add explicit type casting
4. Handle null values appropriately

---

## Getting Help

1. Check logs in Databricks UI
2. Review error messages carefully
3. Check documentation in `docs/` folder
4. Review example queries in `examples/`

