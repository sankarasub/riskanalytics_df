param(
    [switch]$SkipKafka
)

$ErrorActionPreference = "Stop"

Write-Host "Starting hybrid mode services..."
Write-Host ""

# Start Nessie, SeaweedFS, and Spark services (required for hybrid mode)
Write-Host "Starting Nessie, SeaweedFS, and Spark services..."
docker compose up -d nessie seaweedfs spark-master spark-worker spark-connect

# Wait for services to be ready
Write-Host "Waiting for services to be ready..."
Start-Sleep -Seconds 15

# Check Nessie health
try {
    $nessieResponse = Invoke-RestMethod -Uri "http://localhost:19120/api/v1/config" -TimeoutSec 5
    Write-Host "✓ Nessie is ready"
} catch {
    Write-Host "✗ Nessie failed to start"
    exit 1
}

# Check SeaweedFS health
try {
    $seaweedResponse = Invoke-RestMethod -Uri "http://localhost:8333" -TimeoutSec 5 -ErrorAction SilentlyContinue
    Write-Host "✓ SeaweedFS is ready"
} catch {
    Write-Host "✗ SeaweedFS failed to start"
    exit 1
}

# Start Kafka if not skipped
if (-not $SkipKafka) {
    Write-Host "Starting Kafka..."
    docker compose up -d kafka kafka-init kafka-ui
    Write-Host "✓ Kafka started"
}

Write-Host ""
Write-Host "=== Hybrid mode services started ==="
Write-Host "Nessie: http://localhost:19120"
Write-Host "SeaweedFS: http://localhost:8333"
if (-not $SkipKafka) {
    Write-Host "Kafka: localhost:29092"
    Write-Host "Kafka UI: http://localhost:8090"
}