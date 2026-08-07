# Windows Setup Guide

This guide covers Windows-specific setup for the Risk Analytics platform, including WSL configuration and troubleshooting Windows-specific issues.

## Windows Development Options

### Option 1: React UI/API (Recommended)

The React UI and FastAPI backend work directly on Windows without requiring Spark or Java.

**Prerequisites:**
- Python 3.11
- Node.js 16+ and npm
- PowerShell

**Setup Steps:**

```powershell
# Install Python 3.11
py -3.11 --version

# Navigate to project directory
cd D:\riskanalytics_df

# Set up virtual environment
py -3.11 setup_venv.py

# Start the platform
.\scripts\start_local.ps1
```

**Access Points:**
- Frontend: http://localhost:5173
- Backend API: http://localhost:8000
- API Documentation: http://localhost:8000/docs

**What Works:**
- ✅ Full React UI functionality
- ✅ FastAPI backend endpoints
- ✅ Platform health monitoring
- ✅ Configuration management
- ✅ Data exploration UI
- ✅ Pipeline control interface

**Limitations:**
- ❌ Cannot run Spark jobs directly
- ❌ No local data processing
- ❌ Limited to API/UI functionality

### Option 2: WSL (Windows Subsystem for Linux)

For full Spark functionality on Windows, WSL is recommended due to PySpark's known compatibility issues with Windows subprocess execution.

**Prerequisites:**
- Windows 10/11
- WSL enabled
- Ubuntu or other Linux distribution

**Setup Steps:**

```powershell
# Install WSL (if not already installed)
wsl --install

# Restart computer when prompted
# Complete WSL setup in the terminal that opens
```

```bash
# In WSL terminal
cd /mnt/d/riskanalytics_df

# Install Python (Ubuntu 26.04 uses Python 3.14)
sudo apt update
sudo apt install python3 python3-venv python3-pip

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements/dev.txt -r requirements/notebook.txt -r requirements/docs.txt -r requirements/airflow.txt -r requirements/spark.txt -r requirements/ui.txt

# Run Spark jobs
python jobs/bootstrap.py --action create-all-source-to-ods --as-of-date 2026-07-18
```

**Detailed WSL Commands:**
See [WSL Commands Guide](wsl_commands.md) for comprehensive WSL-specific commands for all execution modes (local, hybrid, Docker, and React UI/API).

**What Works:**
- ✅ Full Spark functionality
- ✅ All data processing jobs
- ✅ Complete platform features
- ✅ Better performance for Spark jobs

**Limitations:**
- ❌ Requires WSL setup
- ❌ File system differences (/mnt/d/ paths)
- ❌ Network configuration complexity

### Option 3: Docker (Full Production Stack)

Docker works directly on Windows and provides the complete platform experience.

**Prerequisites:**
- Docker Desktop for Windows
- Sufficient RAM (10GB+ recommended)

**Setup Steps:**

```powershell
# Start Docker Desktop
# Verify Docker is working
docker --version
docker compose version

# Start the platform
docker-compose up
```

**What Works:**
- ✅ Complete platform functionality
- ✅ All services included
- ✅ Production-like environment
- ✅ Cross-platform consistency

**Limitations:**
- ❌ Resource intensive
- ❌ Slower iteration cycle
- ❌ Requires Docker Desktop

## Windows-Specific Issues

### PySpark FileNotFoundError

**Problem:**
```
FileNotFoundError: [WinError 2] The system cannot find the file specified
```

**Cause:**
PySpark has known compatibility issues with Windows subprocess execution and Hadoop library dependencies.

**Solutions:**
1. Use WSL for Spark jobs (recommended)
2. Use Docker for full platform
3. Use React UI/API mode for Windows development

### Port Conflicts

**Problem:**
Frontend or backend won't start due to port conflicts.

**Solution:**
```powershell
# Find processes using port 5173 or 8000
Get-NetTCPConnection -LocalPort 5173 -ErrorAction SilentlyContinue | Select-Object OwningProcess

# Kill conflicting processes
Stop-Process -Id <process_id> -Force
```

### Module Import Errors

**Problem:**
```
ModuleNotFoundError: No module named 'risk_analytics'
```

**Solution:**
All job scripts now automatically add the project root to Python path. If you still encounter this:

```powershell
# Ensure you're in the project directory
cd D:\riskanalytics_df

# Use the virtual environment Python
.venv\Scripts\python.exe jobs\bootstrap.py --action create-all-source-to-ods --as-of-date 2026-07-18
```

### React UI Blank Page

**Problem:**
React UI loads but shows blank page.

**Solution:**
1. Check browser console for JavaScript errors (F12)
2. Clear browser cache and hard refresh (Ctrl+Shift+R)
3. Rebuild the frontend:

```powershell
cd risk-analytics-ui
npm run build
```

### JAVA_HOME Issues

**Problem:**
PySpark cannot find Java.

**Solution:**
```powershell
# Set JAVA_HOME permanently
[System.Environment]::SetEnvironmentVariable("JAVA_HOME", "C:\Program Files\Java\jdk-24", "User")

# Or set for current session
$env:JAVA_HOME = "C:\Program Files\Java\jdk-24"
$env:PATH = "C:\Program Files\Java\jdk-24\bin;$env:PATH"
```

## Recommended Windows Workflow

### For UI/API Development

1. Use Windows native PowerShell
2. Run `.\scripts\start_local.ps1`
3. Develop React UI and FastAPI backend
4. Test using browser and API documentation

### For Data Pipeline Development

1. Use WSL terminal
2. Navigate to project via `/mnt/d/riskanalytics_df`
3. Run Spark jobs in WSL environment
4. Test data transformations and risk calculations

### For Full Platform Testing

1. Use Docker Desktop
2. Run `docker-compose up`
3. Test complete platform functionality
4. Validate integration between all services

## Performance Comparison

| Environment | Spark Jobs | UI/API | Setup Complexity | Performance |
|-------------|------------|--------|------------------|-------------|
| Windows Native | ❌ | ✅ | Low | Good |
| WSL | ✅ | ✅ | Medium | Better |
| Docker | ✅ | ✅ | Low | Best |

## File System Considerations

### Windows Native
- Direct file access
- Windows path format (`D:\path\to\file`)
- No performance overhead

### WSL
- Linux file system (`/mnt/d/path/to/file`)
- Potential performance overhead for Windows files
- Linux path format

### Docker
- Container file system
- Volume mounts for persistence
- Best isolation

## Network Configuration

### Windows Native
- Direct localhost access
- No network complexity
- Standard Windows networking

### WSL
- Use `localhost` for Windows services
- Use `$(hostname).local` for WSL services from Windows
- Network translation layer

### Docker
- Container networking
- Service discovery via docker-compose
- Consistent network environment

## Conclusion

For Windows users, the recommended approach is:

1. **Primary Development:** Use Windows native for React UI/API development
2. **Spark Jobs:** Use WSL for data pipeline development
3. **Testing:** Use Docker for full platform validation

This hybrid approach provides the best development experience while working around Windows-specific PySpark limitations.