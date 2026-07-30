param(
    [switch]$Force
)

$ErrorActionPreference = "Stop"

Write-Host "Setting up local development environment..."
Write-Host ""

# Check Python
Write-Host "Checking Python installation..."
try {
    $pythonVersion = python --version
    Write-Host "✓ Python found: $pythonVersion"
} catch {
    Write-Host "✗ Python not found. Please install Python 3.11+"
    exit 1
}

# Check Java (required for local Spark)
Write-Host "Checking Java installation..."
try {
    $javaVersion = java -version 2>&1
    Write-Host "✓ Java found: $javaVersion"
    
    # Check JAVA_HOME
    if (-not $env:JAVA_HOME) {
        Write-Host "⚠ JAVA_HOME not set. Setting to common path..."
        $javaPath = "C:\Program Files\Java\jdk-24"
        if (Test-Path $javaPath) {
            $env:JAVA_HOME = $javaPath
            Write-Host "✓ JAVA_HOME set to: $javaPath"
        } else {
            Write-Host "⚠ Java not found at common path. Please set JAVA_HOME manually."
        }
    } else {
        Write-Host "✓ JAVA_HOME: $env:JAVA_HOME"
    }
} catch {
    Write-Host "⚠ Java not found. Local Spark requires Java."
    Write-Host "  You can still use hybrid mode with Docker Spark services."
}

# Check pip
Write-Host "Checking pip..."
try {
    pip --version | Out-Null
    Write-Host "✓ pip available"
} catch {
    Write-Host "✗ pip not found"
    exit 1
}

# Create virtual environment if it doesn't exist
if (-not (Test-Path ".venv")) {
    Write-Host "Creating virtual environment..."
    python -m venv .venv
    Write-Host "✓ Virtual environment created"
} else {
    Write-Host "Virtual environment already exists"
}

# Activate virtual environment
Write-Host "Activating virtual environment..."
& .\.venv\Scripts\Activate.ps1

# Upgrade pip
Write-Host "Upgrading pip..."
python -m pip install --upgrade pip

# Install dependencies
Write-Host "Installing Python dependencies..."

$requirements = @(
    "pyspark[connect]==4.1.3",
    "pyiceberg[s3fs,hive]==0.11.1",
    "pandas==2.2.3",
    "pyyaml==6.0.2",
    "requests==2.32.3",
    "protobuf==6.33.0",
    "grpcio==1.76.0",
    "grpcio-status==1.76.0",
    "pyarrow==18.1.0",
    "zstandard==0.25.0"
)

foreach ($req in $requirements) {
    Write-Host "  Installing $req..."
    pip install $req
    if ($LASTEXITCODE -ne 0) {
        Write-Host "✗ Failed to install $req"
        exit 1
    }
}

Write-Host "✓ All dependencies installed"

# Verify installation
Write-Host ""
Write-Host "Verifying installation..."
try {
    python -c "import pyspark; print('✓ PySpark:', pyspark.__version__)"
    python -c "import pyiceberg; print('✓ PyIceberg:', pyiceberg.__version__)"
    python -c "import pandas; print('✓ Pandas:', pandas.__version__)"
    python -c "import yaml; print('✓ PyYAML:', yaml.__version__)"
    python -c "import grpc; print('✓ gRPC:', grpc.__version__)"
    python -c "import protobuf; print('✓ Protobuf:', protobuf.__version__)"
} catch {
    Write-Host "✗ Verification failed"
    exit 1
}

Write-Host ""
Write-Host "=== Local environment setup completed ==="
Write-Host "Virtual environment: .venv"
Write-Host "Activate with: .\.venv\Scripts\Activate.ps1"
Write-Host ""
Write-Host "Next steps:"
Write-Host "1. Start hybrid services: .\scripts\start_hybrid_services.ps1"
Write-Host "2. Run pipeline: .\scripts\run_local_pipeline.ps1 -Mode hybrid -AsOfDate 2026-07-18"