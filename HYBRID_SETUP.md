# Hybrid Execution Mode Setup Guide

This document describes the hybrid execution mode implementation for the Risk Analytics platform, which allows running the Python data processing code locally without Docker images while maintaining a unified React UI.

## Overview

The platform now supports three execution modes:

1. **Docker Mode** (default): Full Docker stack with all services containerized
2. **Hybrid Mode**: Local Spark + remote catalog/storage (Nessie + SeaweedFS)
3. **Local Mode**: Everything local, no external services required

## Architecture

### Mode-Specific Configurations

Each mode has its own configuration file in `config/modes/`:

- `docker.yaml`: Full Docker stack (production setup)
- `hybrid.yaml`: Local Spark, remote Nessie/SeaweedFS
- `local.yaml`: Local Spark, local filesystem storage

### Configuration System

The configuration system in `risk_analytics/config.py` supports:
- Base configuration from `config/platform.yaml`
- Mode-specific overrides from `config/modes/{mode}.yaml`
- Environment variable overrides for sensitive values

### Spark Session Creation

The `risk_analytics/spark.py` module creates different Spark sessions based on execution mode:

- **Local mode**: Embedded Spark with local Iceberg catalog
- **Hybrid mode**: Local Spark with remote Nessie catalog
- **Docker mode**: Cluster Spark with full Iceberg/Nessie/S3A configuration

## Unified React UI

### Frontend Technology Stack

- React 18 with TypeScript
- Vite for fast development
- Material-UI (MUI) for components
- React Query for data fetching
- Zustand for state management
- React Router for navigation

### UI Pages

1. **Dashboard**: Platform health and execution mode status
2. **Risk Metrics**: Business dashboard with risk metrics visualization
3. **Pipeline Control**: Developer tools for pipeline execution
4. **Data Explorer**: Browse and query catalog tables
5. **Configuration**: View and edit platform configuration

### Backend API

The unified FastAPI backend (`api/backend.py`) provides:

- `/api/platform/health`: Health check for all services
- `/api/platform/config`: Get/update platform configuration
- `/api/pipeline/execute`: Execute pipeline targets
- `/api/pipeline/status`: Get pipeline execution status
- `/api/data/tables`: List catalog tables
- `/api/data/table/{name}`: Get table data and schema
- `/api/metrics/summary`: Get risk metrics summary
- `/api/metrics/historical`: Get historical metrics

## Running the Platform

### Prerequisites

For all modes:
- Python 3.8+
- Node.js 16+ and npm for the React UI
- Required Python packages (see requirements files)

For hybrid mode:
- Running Nessie instance
- Running SeaweedFS or S3-compatible storage

For Docker mode:
- Docker and Docker Compose

### Local Mode (No External Services)

```powershell
# Start everything locally
.\scripts\start_local.ps1

# Or manually:
# Terminal 1: Backend
$env:EXECUTION_MODE = "local"
python -m uvicorn api.backend:app --reload --port 8000

# Terminal 2: Frontend
cd risk-analytics-ui
npm run dev:local
```

### Hybrid Mode (Local Spark + Remote Services)

```powershell
# Start required services first
docker compose up -d nessie seaweedfs

# Then start the platform
.\scripts\start_hybrid.ps1

# Or manually:
# Terminal 1: Backend
$env:EXECUTION_MODE = "hybrid"
python -m uvicorn api.backend:app --reload --port 8000

# Terminal 2: Frontend
cd risk-analytics-ui
npm run dev:hybrid
```

### Docker Mode (Full Stack)

```powershell
# Start all services with Docker
docker compose up -d

# The React UI will be accessible at http://localhost:3000
# It will connect to the Docker services automatically
```

## Development Workflow

### Starting Development

1. **Choose your execution mode** based on your needs:
   - Use **local mode** for fastest iteration without external dependencies
   - Use **hybrid mode** for development with production-like catalog/storage
   - Use **Docker mode** for full-system testing

2. **Start the services** for your chosen mode

3. **Access the UI** at http://localhost:3000 (or http://localhost:5173 for Vite dev server)

### Testing the System

1. **Health Check**: Navigate to Dashboard to verify all services are healthy
2. **Configuration**: Check Configuration page to verify execution mode
3. **Pipeline Execution**: Use Pipeline Control to run bootstrap and data pipelines
4. **Data Exploration**: Use Data Explorer to browse created tables
5. **Risk Metrics**: View computed risk metrics in the Risk Metrics page

### Switching Modes

To switch between execution modes:

1. Stop all running services
2. Set the `EXECUTION_MODE` environment variable
3. Restart services with the new mode
4. The UI will automatically connect to the appropriate backend

## Benefits

### Development Speed
- Faster iteration without Docker overhead in local mode
- Easy debugging with direct access to Python processes
- Quick restarts for frontend changes

### Resource Efficiency
- Lower memory/CPU requirements for local development
- Only run necessary services for your development task

### Flexibility
- Choose execution mode based on your current task
- Easy switching between modes for different development scenarios
- Maintain consistency with production environment in hybrid mode

### Testing
- Test individual components easily in local mode
- Integration testing with hybrid mode
- Full system testing with Docker mode

## Trade-offs

### Local Mode
- **Pros**: Fastest development, no external dependencies
- **Cons**: Different from production environment, limited scale

### Hybrid Mode
- **Pros**: Production-like catalog/storage, fast local Spark
- **Cons**: Requires some external services (Nessie, SeaweedFS)

### Docker Mode
- **Pros**: Production-identical environment, full feature parity
- **Cons**: Slower development cycle, higher resource usage

## Troubleshooting

### Common Issues

1. **Spark fails to start in local mode**
   - Ensure Java 8+ is installed
   - Check PySpark dependencies are installed
   - Verify sufficient memory available

2. **Cannot connect to Nessie in hybrid mode**
   - Ensure Nessie is running: `docker compose up nessie`
   - Check URI configuration in `config/modes/hybrid.yaml`
   - Verify network connectivity

3. **UI cannot connect to backend**
   - Ensure backend is running on port 8000
   - Check CORS configuration in backend
   - Verify proxy settings in Vite config

4. **Tables not showing in Data Explorer**
   - Run bootstrap pipeline first to create tables
   - Check catalog namespace configuration
   - Verify Spark session is using correct catalog

## Next Steps

1. **Install dependencies**: Run `npm install` in `risk-analytics-ui` directory
2. **Choose execution mode**: Decide which mode fits your development needs
3. **Start development**: Use the appropriate startup script
4. **Explore the UI**: Navigate through the different pages
5. **Run pipelines**: Execute bootstrap and data pipelines to populate data
6. **Develop features**: Add new features following the existing patterns

## File Structure

```
riskanalytics_df/
├── api/
│   ├── app.py              # Original API (kept for compatibility)
│   └── backend.py          # New unified backend
├── config/
│   ├── platform.yaml       # Base configuration
│   └── modes/
│       ├── docker.yaml     # Docker mode config
│       ├── hybrid.yaml     # Hybrid mode config
│       └── local.yaml      # Local mode config
├── risk-analytics-ui/
│   ├── src/
│   │   ├── components/     # React components
│   │   ├── pages/          # Page components
│   │   ├── services/       # API service layer
│   │   ├── store/          # State management
│   │   └── types/          # TypeScript types
│   ├── package.json
│   └── vite.config.ts
├── scripts/
│   ├── start_hybrid.ps1    # Hybrid mode startup
│   └── start_local.ps1     # Local mode startup
└── risk_analytics/
    ├── config.py           # Configuration loader
    └── spark.py            # Spark session creation
```

## Conclusion

The hybrid execution approach provides flexibility for different development scenarios while maintaining a unified, modern React UI. You can choose the execution mode that best fits your current task and easily switch between modes as needed.