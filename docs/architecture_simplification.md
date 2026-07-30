# Architecture Simplification for Local/Hybrid Modes

> Quick links: [Overview](index.md) | [Architecture](architecture.md) | [Runbooks](runbooks.md) | [Logging and Monitoring](logging_and_monitoring.md)

This document describes the simplified architecture for local and hybrid execution modes, which removes the unnecessary source tables layer while maintaining full production architecture for Docker mode.

For step-by-step execution procedures, see the [Runbooks](runbooks.md).

For information about logging and monitoring with Splunk, see [Logging and Monitoring](logging_and_monitoring.md).

## Overview

The Risk Analytics platform now supports **simplified architecture** for local and hybrid execution modes, removing the unnecessary source tables layer while maintaining full production architecture for Docker mode.

## Execution Mode Architectures

### Docker Mode (Full Architecture)
```
Files/Kafka → Source Tables (nessie.risk_analytics) → Stage Tables (nessie.risk_analytics_stage) → ODS Tables (nessie.risk_analytics_ods) → Risk Metrics
```

### Local/Hybrid Modes (Simplified Architecture)
```
Files → Stage Tables (nessie.risk_analytics_stage) → ODS Tables (nessie.risk_analytics_ods) → Risk Metrics
```

## Key Changes

### 1. Source Tables (nessie.risk_analytics)
- **Docker Mode**: Created and used for both batch and streaming ingestion
- **Local/Hybrid Modes**: Not created - data goes directly from files to stage tables

### 2. Stage YAML Files
- Updated to support **direct file reading** as primary source
- Table sources kept as fallback for Docker mode compatibility
- Automatic file path resolution based on execution mode

### 3. Bootstrap Process
- **Docker Mode**: Creates source tables and seeds them with data
- **Local/Hybrid Mode**: Skips source table creation and seeding
- Stage and ODS tables always created in all modes

### 4. Risk Metrics Pipeline
- Always reads from ODS tables: `nessie.risk_analytics_ods.deals`, `collateral`, `asset`
- Risk metrics table placement:
  - Docker: `nessie.risk_analytics.risk_metrics`
  - Local/Hybrid: `nessie.risk_analytics_ods.risk_metrics`

## Benefits

### For Local/Hybrid Modes
- **Faster Execution**: Eliminates unnecessary table creation and data movement
- **Simpler Debugging**: Direct file-to-stage pipeline is easier to trace
- **Lower Resource Usage**: Fewer tables mean less memory and storage
- **Faster Development**: Quick iteration without full stack overhead

### For Docker Mode
- **Production Parity**: Maintains full architecture with streaming support
- **Streaming Support**: Kafka can write to source tables alongside batch
- **Consistency**: Existing pipelines and DAGs work unchanged

## File Path Resolution

### SourceA (Local/Hybrid)
- Customer: `data/sourcea/customer.json`
- Asset: `data/sourcea/asset.json`
- Collateral: `data/sourcea/collateral.json`
- Deals: `data/sourcea/deals.json`

### SourceB (Local/Hybrid)
- Customer: `data/sourceb/customer/*.csv`
- Asset: `data/sourceb/asset/asset_sourceb_sample.json`
- Collateral: `data/sourceb/collateral/collateral_sourceb_sample.json`
- Deals: `data/sourceb/trans/*.csv`

## Migration Impact

### Breaking Changes
- **Local/Hybrid modes**: Direct source table access no longer works
- **Streaming**: Only available in Docker mode
- **API/Scripts**: Updated to handle mode-specific behavior

### Backward Compatibility
- **Docker mode**: Completely backward compatible
- **DAGs**: Work unchanged in Docker mode
- **Existing Data**: Source tables preserved in Docker mode

## Configuration

### Mode Selection
Set `EXECUTION_MODE` environment variable:
- `docker`: Full architecture with source tables
- `hybrid`: Simplified architecture, remote catalog/storage
- `local`: Simplified architecture, local catalog/storage

### Mode-Specific Behavior
```python
# In jobs/bootstrap.py
if execution_mode == "docker":
    create_source_tables()
    seed_source_tables()
create_stage_and_ods_tables()  # Always
```

## Testing

### Local Mode Test
```powershell
$env:EXECUTION_MODE = "local"
python jobs/bootstrap.py --action all --as-of-date 2026-07-18
python jobs/run_source_to_ods_step.py --layer stage --entity customer --source sourcea --as-of-date 2026-07-18
python jobs/run_source_to_ods_step.py --layer ods --entity customer --source sourcea --as-of-date 2026-07-18
python jobs/run_risk_pipeline.py --as-of-date 2026-07-18 --data-model source-to-ods
```

### Hybrid Mode Test
```powershell
# Start required services
docker compose up -d nessie seaweedfs

# Run pipeline
$env:EXECUTION_MODE = "hybrid"
python jobs/bootstrap.py --action all --as-of-date 2026-07-18
python jobs/run_source_to_ods_step.py --layer stage --entity customer --source sourcea --as-of-date 2026-07-18
python jobs/run_source_to_ods_step.py --layer ods --entity customer --source sourcea --as-of-date 2026-07-18
python jobs/run_risk_pipeline.py --as-of-date 2026-07-18 --data-model source-to-ods
```

### Docker Mode Test (Unchanged)
```powershell
docker compose up -d
# Existing DAGs and scripts work as before
```

## Conclusion

The simplified architecture provides significant performance and resource benefits for local/hybrid development while maintaining full production capabilities in Docker mode. This aligns with the project's goal of supporting flexible execution modes without compromising production readiness.