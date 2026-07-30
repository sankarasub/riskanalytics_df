# Start the Risk Analytics platform in local mode
# Local mode: Everything local, no external services required

Write-Host "Starting Risk Analytics Platform in Local Mode..." -ForegroundColor Green

# Set execution mode
$env:EXECUTION_MODE = "local"

# Create local data directory if it doesn't exist
$dataDir = "./data/warehouse"
if (-not (Test-Path $dataDir)) {
    Write-Host "Creating local data directory: $dataDir" -ForegroundColor Yellow
    New-Item -ItemType Directory -Path $dataDir -Force | Out-Null
}

# Start the FastAPI backend
Write-Host "Starting FastAPI backend..." -ForegroundColor Yellow
$backendJob = Start-Job -ScriptBlock {
    Set-Location $args[0]
    $env:EXECUTION_MODE = "local"
    python -m uvicorn api.backend:app --reload --port 8000
} -ArgumentList (Get-Location)

# Wait for backend to start
Start-Sleep -Seconds 5

# Start the React frontend
Write-Host "Starting React frontend..." -ForegroundColor Yellow
$frontendJob = Start-Job -ScriptBlock {
    Set-Location $args[0]
    Set-Location "risk-analytics-ui"
    npm run dev:local
} -ArgumentList (Get-Location)

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Local Mode Started Successfully!" -ForegroundColor Green
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