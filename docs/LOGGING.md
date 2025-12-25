# Structured Logging Guide

This guide covers structured logging best practices for the market data pipeline.

## Overview

Structured logging provides machine-readable logs that are easier to parse, search, and analyze. This is essential for production systems.

## Basic Logging Setup

```python
import logging
import json
from datetime import datetime
from typing import Dict, Any

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)
```

## Structured Logging Pattern

```python
def log_structured(level: str, message: str, **kwargs):
    """Log structured data as JSON."""
    log_data = {
        "timestamp": datetime.utcnow().isoformat(),
        "level": level,
        "message": message,
        **kwargs
    }
    log_json = json.dumps(log_data)
    
    if level == "INFO":
        logger.info(log_json)
    elif level == "ERROR":
        logger.error(log_json)
    elif level == "WARNING":
        logger.warning(log_json)
    else:
        logger.debug(log_json)
```

## Usage Examples

### Data Collection Logging

```python
# In notebooks/01_collect_raw_data.py
log_structured(
    "INFO",
    "Starting data collection",
    job_id="collect_raw_data",
    date="2024-01-15",
    symbol_count=500
)

log_structured(
    "INFO",
    "Symbol data collected",
    symbol="AAPL",
    records=390,
    status="success"
)

log_structured(
    "ERROR",
    "Failed to collect symbol data",
    symbol="INVALID",
    error="API rate limit exceeded",
    retry_count=3
)
```

### Pipeline Processing Logging

```python
# In notebooks/03_transform_silver_bars.py
log_structured(
    "INFO",
    "Starting Silver transformation",
    batch_id="20240115",
    input_records=10000
)

log_structured(
    "INFO",
    "Silver transformation completed",
    batch_id="20240115",
    input_records=10000,
    output_records=9500,
    valid_records=9200,
    invalid_records=300,
    avg_quality_score=0.92,
    duration_seconds=45
)
```

## Best Practices

1. **Use Consistent Fields**: Always include timestamp, level, message
2. **Add Context**: Include relevant IDs (batch_id, symbol, etc.)
3. **Log Errors with Stack Traces**: Include full exception info
4. **Use Appropriate Levels**: INFO for normal operations, ERROR for failures
5. **Avoid Logging Sensitive Data**: Never log API keys or passwords

