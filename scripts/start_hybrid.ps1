# Start the Risk Analytics platform in hybrid mode
# Hybrid mode: Local Spark, remote catalog/storage (Nessie + SeaweedFS)

Write-Host "Starting Risk Analytics Platform in Hybrid Mode..." -ForegroundColor Green

# Set execution mode
$env:EXECUTION_MODE = "hybrid"

# Check if required services are running
Write-Host "Checking if Nessie and SeaweedFS are running..." -ForegroundColor Yellow
try {
    $nessieResponse = Invoke-WebRequest -Uri "http://localhost:19120/api/v2/config" -TimeoutSec 2
    Write-Host "✓ Nessie is running" -ForegroundColor Green
} catch {
    Write-Host "✗ Nessie is not running. Start it with: docker compose up nessie seaweedfs" -ForegroundColor Red
    exit 1
}

try {
    $seaweedResponse = Invoke-WebRequest -Uri "http://localhost:8333" -TimeoutSec 2
    Write-Host "✓ SeaweedFS is running" -ForegroundColor Green
} catch {
    Write-Host "✗ SeaweedFS is not running. Start it with: docker compose up nessie seaweedfs" -ForegroundColor Red
    exit 1
}

# Start the FastAPI backend
Write-Host "Starting FastAPI backend..." -ForegroundColor Yellow
$backendJob = Start-Job -ScriptBlock {
    Set-Location $args[0]
    $env:EXECUTION_MODE = "hybrid"
    python -m uvicorn api.backend:app --reload --port 8000
} -ArgumentList (Get-Location)

# Wait for backend to start
Start-Sleep -Seconds 5

# Start the React frontend
Write-Host "Starting React frontend..." -ForegroundColor Yellow
$frontendJob = Start-Job -ScriptBlock {
    Set-Location $args[0]
    Set-Location "risk-analytics-ui"
    npm run dev:hybrid
} -ArgumentList (Get-Location)

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Hybrid Mode Started Successfully!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Frontend: http://localhost:5173" -ForegroundColor White
Write-Host "Backend API: http://localhost:8000" -ForegroundColor White
Write-Host "API Docs: http://localhost:8000/docs" -ForegroundColor White
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Press Ctrl+C to stop all services" -ForegroundColor Yellow

# Keep script running
try {
    while ($true) {
        Start-Sleep -Seconds 1
        
        # Check if jobs are still running
        if ($backendJob.State -ne "Running") {
            Write-Host "Backend job stopped unexpectedly" -ForegroundColor Red
            break
        }
        if ($frontendJob.State -ne "Running") {
            Write-Host "Frontend job stopped unexpectedly" -ForegroundColor Red
            break
        }
    }
} finally {
    Write-Host "Stopping services..." -ForegroundColor Yellow
    Stop-Job $backendJob
    Stop-Job $frontendJob
    Remove-Job $backendJob
    Remove-Job $frontendJob
    Write-Host "Services stopped" -ForegroundColor Green
}