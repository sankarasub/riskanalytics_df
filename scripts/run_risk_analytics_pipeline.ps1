param(
    [Parameter()]
    [string]$AsOfDate = (Get-Date -Format 'yyyy-MM-dd'),

    [ValidateSet('none', 'first-build', 'offline')]
    [string]$PlatformMode = 'offline',

    [switch]$OpenEndpoints
)

# Fail fast on undefined variables and command errors.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# Always run from repository root so relative paths behave consistently.
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$sparkSubmitBase = @(
    '/opt/spark/bin/spark-submit',
    '--master', 'spark://spark-master:7077'
)

function Assert-DockerDaemonAvailable {
    # Docker CLI can be installed while Docker Desktop is stopped. Probe the
    # daemon before Compose commands so users receive an actionable diagnosis.
    $previousErrorPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $version = & docker info --format '{{.ServerVersion}}' 2>&1
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorPreference
    }

    if ($exitCode -ne 0) {
        $details = ($version | Out-String).Trim()
        throw "Docker Desktop is not running or its daemon is unavailable. Start Docker Desktop, wait for it to finish starting, then retry. Details: $details"
    }
}

function Assert-OfflineImagesAvailable {
    # ``--pull never`` prevents Compose from silently reaching a registry, while
    # this preflight identifies the exact cached images that are missing.
    $images = & docker compose config --images 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to resolve Compose images for offline startup: $(($images | Out-String).Trim())"
    }

    $missing = @()
    foreach ($image in ($images | Where-Object { $_ -and $_.Trim() } | Sort-Object -Unique)) {
        $previousErrorPreference = $ErrorActionPreference
        $ErrorActionPreference = 'Continue'
        try {
            # Missing images are an expected preflight result, not a PowerShell
            # exception; retain only Docker's exit code for the aggregate report.
            & docker image inspect $image 2>&1 | Out-Null
            $inspectExitCode = $LASTEXITCODE
        }
        finally {
            $ErrorActionPreference = $previousErrorPreference
        }
        if ($inspectExitCode -ne 0) {
            $missing += $image
        }
    }

    if ($missing.Count -gt 0) {
        throw "Strict offline mode cannot start because these images are not cached locally: $($missing -join ', '). Connect to the network and run '.\scripts\run_risk_analytics_pipeline.ps1 -PlatformMode first-build' once to build/pull them."
    }
}

function Invoke-Compose {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Args,
        [Parameter(Mandatory = $true)]
        [string]$FailureMessage
    )

    $previousErrorPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $output = & docker @Args 2>&1
    }
    finally {
        $ErrorActionPreference = $previousErrorPreference
    }

    $exitCode = $LASTEXITCODE
    $output | ForEach-Object { $_ }

    if ($exitCode -ne 0) {
        $message = ($output | Out-String).Trim()
        if ($message) {
            throw "$FailureMessage`n$message"
        }
        throw $FailureMessage
    }
}

function Invoke-SparkSubmit {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Args,
        [Parameter(Mandatory = $true)]
        [string]$FailureMessage
    )

    $command = @('compose', 'exec', 'spark-master') + $sparkSubmitBase + $Args
    Invoke-Compose -Args $command -FailureMessage $FailureMessage
}

function Wait-ForSparkCluster {
    $maxAttempts = 20
    $delaySeconds = 5

    for ($attempt = 1; $attempt -le $maxAttempts; $attempt++) {
        $output = & docker compose exec -T spark-master /bin/sh -c 'echo ok' 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Host 'Spark cluster is ready for jobs.'
            return
        }

        if ($attempt -lt $maxAttempts) {
            Write-Host "Spark cluster not ready yet (attempt $attempt/$maxAttempts). Waiting $delaySeconds seconds..."
            Start-Sleep -Seconds $delaySeconds
        }
    }

    throw "Spark cluster did not become ready after $maxAttempts attempts."
}

function Start-PlatformIfNeeded {
    $services = & docker compose ps --services --filter status=running 2>$null
    if ($LASTEXITCODE -eq 0 -and ($services -match 'spark-master')) {
        Write-Host 'Spark services are already running; skipping platform startup.'
        return
    }

    switch ($PlatformMode) {
        'first-build' {
            Write-Host 'Starting platform in first-build mode (docker compose up --build -d)...'
            & docker compose up --build -d
            if ($LASTEXITCODE -ne 0) {
                throw 'docker compose up --build failed.'
            }
        }
        'offline' {
            Assert-OfflineImagesAvailable
            Write-Host 'Starting platform in strict offline mode (no build and no image pull)...'
            & docker compose up -d --no-build --pull never
            if ($LASTEXITCODE -ne 0) {
                throw "Strict offline startup failed. No build or pull was attempted. Review cached images, local volumes, and 'docker compose ps --all', then use -PlatformMode first-build only when network access is available."
            }
        }
        'none' {
            throw 'Platform mode is none, but the required services are not running. Start them with first-build or offline mode.'
        }
        default {
            throw "Unsupported platform mode '$PlatformMode'."
        }
    }
}

function Wait-ForRequiredServices {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$RequiredServices,

        [int]$MaxAttempts = 30,
        [int]$DelaySeconds = 5
    )

    for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
        $runningServices = & docker compose ps --services --filter status=running 2>$null
        $allReady = $true

        foreach ($service in $RequiredServices) {
            if ($runningServices -notcontains $service) {
                $allReady = $false
                break
            }
        }

        if ($allReady) {
            Write-Host "Required services are running: $($RequiredServices -join ', ')"
            return
        }

        if ($attempt -lt $MaxAttempts) {
            Write-Host "Waiting for services ($($RequiredServices -join ', ')) (attempt $attempt/$MaxAttempts)..."
            Start-Sleep -Seconds $DelaySeconds
        }
    }

    throw "The following services did not become ready in time: $($RequiredServices -join ', ')"
}

function Invoke-AirflowDagTrigger {
    param(
        [Parameter(Mandatory = $true)]
        [string]$DagId,

        [Parameter(Mandatory = $true)]
        [string]$AsOfDate
    )

    $body = @{ conf = @{ as_of_date = $AsOfDate } } | ConvertTo-Json -Compress
    $headers = @{ Authorization = 'Basic ' + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('admin:admin')) }
    $maxAttempts = 5
    $delaySeconds = 3

    for ($attempt = 1; $attempt -le $maxAttempts; $attempt++) {
        try {
            $response = Invoke-RestMethod -Method Post -Uri "http://localhost:8088/api/v1/dags/$DagId/dagRuns" -Headers $headers -ContentType 'application/json' -Body $body -ErrorAction Stop
            Write-Host "Airflow DAG $DagId triggered successfully."
            $response | ConvertTo-Json -Depth 5 | Write-Host
            return
        }
        catch {
            $message = $_.Exception.Message
            if ($attempt -lt $maxAttempts) {
                Write-Warning "Trigger attempt $attempt/$maxAttempts for Airflow DAG $DagId failed. Retrying in $delaySeconds seconds. $message"
                Start-Sleep -Seconds $delaySeconds
            }
            else {
                throw "Triggering Airflow DAG $DagId failed after $maxAttempts attempts.`n$message"
            }
        }
    }
}

function Unpause-AirflowDags {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$DagIds
    )

    foreach ($dagId in $DagIds) {
        Invoke-Compose -Args @('compose', 'exec', '-T', 'airflow-webserver', 'airflow', 'dags', 'unpause', $dagId) -FailureMessage "Unpausing Airflow DAG $dagId failed."
    }
}

function Invoke-ValidationQuery {
    $command = @(
        'compose', 'exec', 'business-ui',
        'python', '/opt/risk_analytics/scripts/health_check.py',
        '--host', 'host.docker.internal',
        '--postgres-host', 'postgres',
        '--check-iceberg'
    )

    $previousErrorPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $output = & docker @command 2>&1
        $output | ForEach-Object { $_ }
    }
    finally {
        $ErrorActionPreference = $previousErrorPreference
    }

    if ($LASTEXITCODE -ne 0) {
        Write-Warning 'Validation query reported issues, but the workflow trigger succeeded. Continuing with the startup flow.'
    }
}

function Open-PlatformEndpoints {
    $endpoints = @(
        'http://localhost:8000',
        'http://localhost:8501',
        'http://localhost:8502',
        'http://localhost:8088',
        'http://localhost:8888',
        'http://localhost:9047',
        'http://localhost:8080',
        'http://localhost:8090',
        'http://localhost:19120/api/v2'
    )

    foreach ($endpoint in $endpoints) {
        Start-Process $endpoint | Out-Null
    }
}

Assert-DockerDaemonAvailable
Start-PlatformIfNeeded

Write-Host 'Current service state:'
Invoke-Compose -Args @('compose', 'ps', '--all') -FailureMessage 'docker compose ps failed.'

Wait-ForRequiredServices -RequiredServices @('spark-master', 'airflow-webserver', 'airflow-scheduler', 'business-ui', 'postgres', 'nessie')

$dagIdsToUnpause = @(
    'ra_createtables_and_data',
    'ra_stage_to_ods_orchestration',
    'ra_riskmetrics_eval_ods'
)
foreach ($source in @('sourceA', 'sourceB')) {
    foreach ($entity in @('customer', 'asset', 'collateral', 'deals')) {
        $dagIdsToUnpause += "ra_${source}_${entity}_stage"
        $dagIdsToUnpause += "ra_${source}_${entity}_ods"
    }
}

Unpause-AirflowDags -DagIds $dagIdsToUnpause

Write-Host "Triggering Airflow workflow for $AsOfDate..."
Invoke-AirflowDagTrigger -DagId 'ra_createtables_and_data' -AsOfDate $AsOfDate

Write-Host 'Running loaded-table validation query...'
Invoke-ValidationQuery

if ($OpenEndpoints) {
    Write-Host 'Opening platform endpoints in the default browser...'
    Open-PlatformEndpoints
}

Write-Host 'Risk Analytics platform startup, Airflow workflow trigger, and validation completed successfully.'

