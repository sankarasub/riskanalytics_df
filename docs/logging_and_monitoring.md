# Logging and Monitoring

> Quick links: [Overview](index.md) | [Architecture](architecture.md) | [Architecture Simplification](architecture_simplification.md) | [Runbooks](runbooks.md) | [Troubleshooting](troubleshooting.md)

This document describes the centralized logging system and how to integrate with Splunk for monitoring and analysis.

For execution mode-specific procedures, see the [Runbooks](runbooks.md).

## Overview

The Risk Analytics platform uses a centralized logging system that provides:

- **Structured logging** with timestamps, levels, and context
- **Performance metrics** (execution time, record counts) for each step
- **Connection details** (catalog, storage, Spark configuration)
- **Splunk-compatible JSON logs** for easy ingestion
- **Multi-output** (console, file, JSON for Splunk)

## Log Files

All logs are written to the `logs/` directory:

- **`logs/pipeline_execution.log`** - Detailed human-readable logs with timestamps
- **`logs/splunk_pipeline.log`** - JSON-formatted logs for Splunk ingestion
- **`logs/pipeline_metrics.json`** - Structured metrics JSON file with all pipeline statistics

## Log Format

### Console/File Format
```
2026-07-18 14:30:45 | INFO | pipeline.bootstrap | PIPELINE START: bootstrap
2026-07-18 14:30:45 | INFO | pipeline.bootstrap | Execution Mode: local
2026-07-18 14:30:45 | INFO | pipeline.bootstrap | Parameters: {"action": "all", "as_of_date": "2026-07-18"}
2026-07-18 14:30:45 | INFO | pipeline.bootstrap | CONNECTION DETAILS:
2026-07-18 14:30:45 | INFO | pipeline.bootstrap |   Catalog: local
2026-07-18 14:30:45 | INFO | pipeline.bootstrap |   Catalog Type: memory
2026-07-18 14:30:45 | INFO | pipeline.bootstrap |   Storage Type: local
2026-07-18 14:30:45 | INFO | pipeline.bootstrap |   Spark Mode: local
2026-07-18 14:30:45 | INFO | pipeline.bootstrap | ============================================================
2026-07-18 14:30:46 | INFO | pipeline.bootstrap | ------------------------------------------------------------
2026-07-18 14:30:46 | INFO | pipeline.bootstrap | STEP START: create_all_tables
2026-07-18 14:30:47 | INFO | pipeline.bootstrap | ------------------------------------------------------------
2026-07-18 14:30:47 | INFO | pipeline.bootstrap | STEP COMPLETE: create_all_tables
2026-07-18 14:30:47 | INFO | pipeline.bootstrap | Records Processed: 0
2026-07-18 14:30:47 | INFO | pipeline.bootstrap | Duration: 1234.56ms (1.23s)
2026-07-18 14:30:47 | INFO | pipeline.bootstrap | ------------------------------------------------------------
```

### Splunk JSON Format
```json
{"timestamp": "2026-07-18T14:30:45", "level": "INFO", "logger": "pipeline.bootstrap", "message": "PIPELINE START: bootstrap"}
{"timestamp": "2026-07-18T14:30:45", "level": "INFO", "logger": "pipeline.bootstrap", "message": "Execution Mode: local"}
```

### Structured Metrics JSON
```json
{
  "pipeline_name": "bootstrap",
  "execution_mode": "local",
  "start_time": "2026-07-18T14:30:45.123456",
  "steps": [
    {
      "step_name": "create_all_tables",
      "start_time": "2026-07-18T14:30:46.123456",
      "end_time": "2026-07-18T14:30:47.357890",
      "duration_ms": 1234.56,
      "records_processed": 0,
      "status": "completed"
    }
  ],
  "total_records_processed": 0,
  "total_duration_seconds": 2.34,
  "status": "completed",
  "errors": []
}
```

## Connection Details Logged

The logging system automatically captures and logs:

- **Catalog Information**
  - Catalog name (e.g., `nessie`, `local`)
  - Catalog type (e.g., `rest`, `memory`)
  - Catalog URI (e.g., `http://localhost:19120/api/v2`)

- **Storage Information**
  - Storage type (e.g., `s3`, `local`)
  - Storage endpoint (e.g., `http://localhost:8333`)

- **Spark Configuration**
  - Spark mode (e.g., `local`, `cluster`)
  - Spark remote URL (if set via `SPARK_REMOTE`)
  - Reference branch (for Nessie)

- **Orchestration**
  - Orchestration type (e.g., `airflow`, `none`)

## Performance Metrics

Each pipeline step logs:

- **Step Name**: Name of the pipeline step
- **Start Time**: When the step started
- **End Time**: When the step completed
- **Duration**: Execution time in milliseconds and seconds
- **Records Processed**: Number of records written/read
- **Status**: `completed`, `failed`, or `skipped`

## Splunk Integration

### Option 1: File-Based Ingestion

Configure Splunk to monitor the log files:

1. **Add Data Input** in Splunk
   - Go to Settings → Data Inputs → Files & Directories
   - Add `logs/splunk_pipeline.log`
   - Set sourcetype to `_json`

2. **Add Metrics Input**
   - Add `logs/pipeline_metrics.json`
   - Set sourcetype to `_json`

### Option 2: HTTP Event Collector (HEC)

Send logs directly to Splunk via HTTP:

```python
import requests
import json

def send_to_splunk(log_file: str, hec_url: str, hec_token: str):
    """Send logs to Splunk HEC."""
    with open(log_file, 'r') as f:
        for line in f:
            event = {
                "time": json.loads(line).get("timestamp"),
                "host": "risk-analytics-server",
                "source": "pipeline",
                "sourcetype": "_json",
                "event": json.loads(line)
            }
            response = requests.post(
                f"{hec_url}/services/collector/event",
                headers={"Authorization": f"Splunk {hec_token}"},
                json=event
            )
```

### Option 3: Splunk Forwarder

Install Splunk Universal Forwarder on the server:

1. Install Splunk Universal Forwarder
2. Configure inputs.conf:
   ```ini
   [monitor:///path/to/riskanalytics_df/logs/splunk_pipeline.log]
   sourcetype = _json
   index = risk_analytics
   
   [monitor:///path/to/riskanalytics_df/logs/pipeline_metrics.json]
   sourcetype = _json
   index = risk_analytics
   ```

## Splunk Queries

### Pipeline Execution Overview
```splunk
index=risk_analytics sourcetype=_json message="PIPELINE START"
| stats count by pipeline_name, execution_mode, status
```

### Performance Analysis
```splunk
index=risk_analytics sourcetype=_json
| eval duration_ms = coalesce(duration_ms, 0)
| stats avg(duration_ms) as avg_duration_ms, max(duration_ms) as max_duration_ms by step_name
| sort - avg_duration_ms
```

### Error Tracking
```splunk
index=risk_analytics sourcetype=_json level=ERROR
| table timestamp, logger, message, error_type, error_message
```

### Connection Issues
```splunk
index=risk_analytics sourcetype=_json message="CONNECTION DETAILS"
| table timestamp, catalog, catalog_uri, storage_type, storage_endpoint
```

### Record Throughput
```splunk
index=risk_analytics sourcetype=_json
| eval records = coalesce(records_processed, 0)
| stats sum(records) as total_records by pipeline_name, execution_mode
```

### Time Series Analysis
```splunk
index=risk_analytics sourcetype=_json
| timechart span=1h avg(duration_ms) by step_name
```

## Log Levels

- **DEBUG**: Detailed diagnostic information
- **INFO**: General informational messages (default)
- **WARNING**: Warning messages for potential issues
- **ERROR**: Error messages for failures

## Log Rotation

Logs are automatically rotated:
- **Max file size**: 10MB
- **Backup count**: 5 files
- **Total retention**: ~50MB per log file

## Custom Logging

To add logging to custom scripts:

```python
from risk_analytics.logging_config import PipelineLogger, setup_logging

# Initialize logging
setup_logging()
logger = PipelineLogger("my_custom_pipeline")

# Log pipeline start
logger.log_pipeline_start({"param1": "value1"})

# Log step execution
logger.log_step_start("my_step", {"details": "step details"})
# ... perform work ...
logger.log_step_complete("my_step", records_processed=100)

# Log errors
try:
    # ... work that might fail ...
    pass
except Exception as e:
    logger.log_step_error("my_step", e)

# Log pipeline completion
logger.log_pipeline_complete(success=True)
```

## Monitoring Dashboard (Splunk)

### Recommended Dashboard Panels

1. **Pipeline Status**
   - Pie chart: Completed vs Failed pipelines
   - Time chart: Pipeline executions over time

2. **Performance Metrics**
   - Bar chart: Average step duration by step name
   - Time chart: Total execution time trend

3. **Throughput**
   - Gauge: Total records processed today
   - Bar chart: Records per pipeline

4. **Error Analysis**
   - Table: Recent errors with details
   - Pie chart: Error types distribution

5. **Connection Health**
   - Table: Catalog and storage connection status
   - Status indicators for each service

## Alerts

### Configure Splunk Alerts

1. **Pipeline Failure Alert**
   ```splunk
   index=risk_analytics sourcetype=_json status="failed"
   | stats count by pipeline_name
   | where count > 0
   ```
   Trigger: Real-time, every 5 minutes

2. **Performance Degradation Alert**
   ```splunk
   index=risk_analytics sourcetype=_json
   | eval duration_ms = coalesce(duration_ms, 0)
   | stats avg(duration_ms) as avg_duration by step_name
   | where avg_duration > 30000  # 30 seconds
   ```
   Trigger: Real-time, every 10 minutes

3. **Connection Failure Alert**
   ```splunk
   index=risk_analytics sourcetype=_json level=ERROR message="CONNECTION DETAILS"
   ```
   Trigger: Real-time, every 5 minutes

## Troubleshooting

### Logs Not Appearing
- Check `logs/` directory exists and is writable
- Verify logging_config.py is imported in the script
- Check execution mode is set correctly

### Splunk Not Ingesting Logs
- Verify Splunk HEC token is valid
- Check firewall allows Splunk port (default: 8088)
- Ensure log file path is correct in Splunk inputs

### Missing Connection Details
- Verify config files are loaded correctly
- Check that load_config() is called before logging
- Ensure all required config keys are present

## Best Practices

1. **Always initialize logging** at the start of each script
2. **Use structured parameters** for step_start to enable better filtering
3. **Always log step completion** with record counts when available
4. **Log errors with exception objects** for full stack traces
5. **Log pipeline completion** even on failure for proper status tracking
6. **Review logs regularly** to identify performance bottlenecks
7. **Set up Splunk alerts** for proactive monitoring