# Local Development Guide

This guide explains how to run the Risk Analytics platform locally without the full Docker stack, using different execution modes.

## Execution Modes

### 1. Docker Mode (Current Production Setup)
- Full Docker stack with all services
- Cluster Spark
- Remote Nessie catalog and SeaweedFS storage
- Airflow orchestration
- Kafka streaming

**Use when:** Running full production-like environment

```powershell
.\scripts\run_risk_analytics_pipeline.ps1 -AsOfDate 2026-07-18 -PlatformMode first-build
```

### 2. Hybrid Mode (Recommended for Development)
- Docker Spark services (Spark Connect)
- Remote Nessie catalog and SeaweedFS storage (Docker containers)
- Script-based orchestration (no Airflow)
- Optional Kafka streaming

**Use when:** Developing data pipelines with catalog consistency

```powershell
# Step 1: Start required services
.\scripts\start_hybrid_services.ps1

# Step 2: Run pipeline
.\scripts\run_local_pipeline.ps1 -Mode hybrid -AsOfDate 2026-07-18
```

### 3. Local Mode (Zero External Dependencies)
- Local Spark
- Local file-based catalog
- Local filesystem storage
- Script-based orchestration
- No streaming

**Use when:** Quick testing without external services

```powershell
# Step 1: Set up local environment
.\scripts\setup_local_env.ps1

# Step 2: Run pipeline
.\scripts\run_local_pipeline.ps1 -Mode local -AsOfDate 2026-07-18
```

## Setup Instructions

### Prerequisites

1. **Python 3.11+** installed
2. **Docker Desktop** (for hybrid/docker modes)
3. **PowerShell** (Windows) or bash (Linux/Mac)
4. **Java 17+** (required for local mode only, not for hybrid/docker modes)

### Local Mode Setup

```powershell
# Set up Python environment (requires Java)
.\scripts\setup_local_env.ps1

# Run pipeline
.\scripts\run_local_pipeline.ps1 -Mode local -AsOfDate 2026-07-18
```

### Hybrid Mode Setup

```powershell
# Set up Python environment (no Java required)
.\scripts\setup_local_env.ps1

# Start required Docker services (Nessie, SeaweedFS, Spark)
.\scripts\start_hybrid_services.ps1

# Run pipeline
.\scripts\run_local_pipeline.ps1 -Mode hybrid -AsOfDate 2026-07-18
```

To include Kafka:

```powershell
# Start with Kafka
.\scripts\start_hybrid_services.ps1

# Run pipeline (Kafka will be available if needed)
.\scripts\run_local_pipeline.ps1 -Mode hybrid -AsOfDate 2026-07-18
```

## Configuration

Execution modes are configured in `config/modes/`:

- `docker.yaml` - Full Docker stack configuration
- `hybrid.yaml` - Local Spark, remote catalog/storage
- `local.yaml` - Everything local

You can override configuration via environment variables:

```powershell
$env:EXECUTION_MODE = "hybrid"
$env:NESSIE_URI = "http://localhost:19120/api/v2"
$env:S3_ENDPOINT = "http://localhost:8333"
```

## Running Individual Pipeline Steps

```powershell
# Set execution mode
$env:EXECUTION_MODE = "hybrid"

# Bootstrap only
.\scripts\run_local_pipeline.ps1 -Mode hybrid -AsOfDate 2026-07-18 -SkipOrchestration -SkipRiskMetrics

# STAGE/ODS only
.\scripts\run_local_pipeline.ps1 -Mode hybrid -AsOfDate 2026-07-18 -SkipBootstrap -SkipRiskMetrics

# Risk metrics only
.\scripts\run_local_pipeline.ps1 -Mode hybrid -AsOfDate 2026-07-18 -SkipBootstrap -SkipOrchestration
```

## Development Workflow

### 1. Quick Development (Local Mode)
```powershell
# Fast iteration with no external dependencies
.\scripts\run_local_pipeline.ps1 -Mode local -AsOfDate 2026-07-18
```

### 2. Testing with Catalog (Hybrid Mode)
```powershell
# Test with real catalog/storage but local Spark
.\scripts\start_hybrid_services.ps1
.\scripts\run_local_pipeline.ps1 -Mode hybrid -AsOfDate 2026-07-18
```

### 3. Full Testing (Docker Mode)
```powershell
# Full production-like environment
.\scripts\run_risk_analytics_pipeline.ps1 -AsOfDate 2026-07-18 -PlatformMode offline
```

## Troubleshooting

### Python Dependencies
```powershell
# Reinstall dependencies
pip install --upgrade -r requirements/dev.txt
```

### Spark Connection Issues
```powershell
# Check Spark is accessible
python -c "from risk_analytics.spark import create_spark_session; spark = create_spark_session('test'); print(spark.version)"
```

### Catalog Connection Issues
```powershell
# Check Nessie (hybrid mode)
Invoke-RestMethod http://localhost:19120/api/v1/config

# Check local catalog (local mode)
# Should create tables in /tmp/iceberg
```

### Storage Issues
```powershell
# Check SeaweedFS (hybrid mode)
Invoke-RestMethod http://localhost:8333

# Check local storage (local mode)
# Check ./data/warehouse directory exists
```

## Performance Considerations

### Local Mode
- **Pros:** Fastest iteration, no external dependencies
- **Cons:** Requires Java, limited scalability, no catalog features
- **Use for:** Quick testing, development (requires Java)

### Hybrid Mode
- **Pros:** Catalog consistency, no Java required, reasonable performance
- **Cons:** Requires Docker services (Spark, Nessie, SeaweedFS)
- **Use for:** Development with catalog features (recommended)

### Docker Mode
- **Pros:** Full feature parity, production-like
- **Cons:** Slower iteration, resource intensive
- **Use for:** Full testing, production deployment

## Next Steps

After setting up local development:

1. Run your first pipeline in local mode
2. Test data transformations
3. Verify risk metrics calculations
4. Transition to hybrid mode for catalog testing
5. Use Docker mode for full system testing

## Migration Notes

- Existing Docker scripts continue to work unchanged
- New modes are additive, not replacements
- Configuration system maintains backward compatibility
- Gradual migration recommended