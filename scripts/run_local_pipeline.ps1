param(
    [Parameter(Mandatory = $true)]
    [string]$AsOfDate,
    
    [ValidateSet("local", "hybrid", "docker")]
    [string]$Mode = "hybrid",
    
    [switch]$SkipBootstrap,
    [switch]$SkipOrchestration,
    [switch]$SkipRiskMetrics
)

$ErrorActionPreference = "Stop"

# Set execution mode environment variable
$env:EXECUTION_MODE = $Mode

Write-Host "Running Risk Analytics Pipeline in $Mode mode"
Write-Host "As-Of-Date: $AsOfDate"
Write-Host ""

# Check Python dependencies
Write-Host "Checking Python dependencies..."
try {
    python --version | Out-Null
    python -c "import pyspark" | Out-Null
    python -c "import pyiceberg" | Out-Null
    Write-Host "✓ Python dependencies OK"
} catch {
    Write-Host "✗ Python dependencies missing. Install with: pip install pyspark[connect]==4.1.3 pyiceberg[s3fs]==0.11.1"
    exit 1
}

# Check mode-specific requirements
if ($Mode -eq "hybrid") {
    Write-Host "Checking hybrid mode requirements..."
    try {
        # Check if Nessie is available
        $nessieResponse = Invoke-RestMethod -Uri "http://localhost:19120/api/v1/config" -TimeoutSec 5 -ErrorAction SilentlyContinue
        if ($nessieResponse) {
            Write-Host "✓ Nessie available at http://localhost:19120"
        } else {
            Write-Host "✗ Nessie not available. Start with: docker compose up nessie seaweedfs"
            exit 1
        }
    } catch {
        Write-Host "✗ Nessie not available. Start with: docker compose up nessie seaweedfs"
        exit 1
    }
}

# Set up Python path
$env:PYTHONPATH = (Get-Location).Path

# Run bootstrap
if (-not $SkipBootstrap) {
    Write-Host ""
    Write-Host "=== Step 1: Bootstrap - Create tables and load seed data ==="
    python jobs/bootstrap.py --as-of-date $AsOfDate
    if ($LASTEXITCODE -ne 0) {
        Write-Host "✗ Bootstrap failed"
        exit 1
    }
    Write-Host "✓ Bootstrap completed"
} else {
    Write-Host "Skipping bootstrap"
}

# Run STAGE and ODS transformations
if (-not $SkipOrchestration) {
    Write-Host ""
    Write-Host "=== Step 2: Run STAGE and ODS transformations ==="
    
    $entities = @("customer", "asset", "collateral", "deals")
    $sources = @("sourcea", "sourceb")
    
    foreach ($entity in $entities) {
        foreach ($source in $sources) {
            Write-Host "Processing $entity from $source..."
            
            # STAGE layer
            $stageArgs = @(
                "jobs/run_source_to_ods_step.py",
                "--layer", "stage",
                "--entity", $entity,
                "--source", $source,
                "--as-of-date", $AsOfDate
            )
            
            if ($source -eq "sourceb") {
                $stageArgs += @("--param", "${entity}_sourceb_path=data/sourceb/${entity}/*.csv")
            }
            
            python @stageArgs
            if ($LASTEXITCODE -ne 0) {
                Write-Host "✗ STAGE failed for $entity from $source"
                exit 1
            }
            
            # ODS layer
            $odsArgs = @(
                "jobs/run_source_to_ods_step.py",
                "--layer", "ods",
                "--entity", $entity,
                "--source", $source,
                "--as-of-date", $AsOfDate
            )
            
            if ($source -eq "sourceb") {
                $odsArgs += @("--param", "${entity}_sourceb_path=data/sourceb/${entity}/*.csv")
            }
            
            python @odsArgs
            if ($LASTEXITCODE -ne 0) {
                Write-Host "✗ ODS failed for $entity from $source"
                exit 1
            }
            
            Write-Host "✓ Completed $entity from $source"
        }
    }
    
    Write-Host "✓ STAGE and ODS transformations completed"
} else {
    Write-Host "Skipping STAGE and ODS transformations"
}

# Run risk metrics calculation
if (-not $SkipRiskMetrics) {
    Write-Host ""
    Write-Host "=== Step 3: Calculate risk metrics ==="
    python jobs/run_risk_pipeline.py --as-of-date $AsOfDate --data-model source-to-ods
    if ($LASTEXITCODE -ne 0) {
        Write-Host "✗ Risk metrics calculation failed"
        exit 1
    }
    Write-Host "✓ Risk metrics calculation completed"
} else {
    Write-Host "Skipping risk metrics calculation"
}

Write-Host ""
Write-Host "=== Pipeline completed successfully ==="
Write-Host "Mode: $Mode"
Write-Host "As-Of-Date: $AsOfDate"