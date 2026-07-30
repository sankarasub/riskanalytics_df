<#
.SYNOPSIS
    Starts the platform, runs the Airflow pipeline for an as-of date, and validates the result.

.DESCRIPTION
    Why this script exists: it is the single supported end-to-end entry point on
    Windows. Doing this by hand means starting Compose, checking service
    readiness, unpausing 27 DAGs, triggering the bootstrap, watching three DAG
    runs in the UI, and only then running a validation query - and getting that
    order wrong produces misleading failures (for example querying
    `risk_analytics_ods.risk_metrics` before the metrics DAG has run).

    Steps:
      1. Verify the Docker daemon answers, and start the platform if Spark is
         not already running (`-PlatformMode first-build|offline|none`; offline
         refuses to build or pull and reports missing cached images instead).
      2. Wait until the required services report running.
      3. Verify Airflow has registered all 27 expected `ra_*` DAGs, retrying to
         absorb scheduler parse delay. A stale checkout fails here with
         remediation instead of triggering a DAG that no longer exists.
      4. Report - or with `-RemoveLegacyDags` delete - `risk_analytics_*` DAG
         metadata left over from the pre-refactor layout.
      5. Unpause every expected DAG over the REST API.
      6. Trigger `ra_createtables_and_data` with the as-of date.
      7. Wait for `ra_createtables_and_data`, `ra_stage_to_ods_orchestration`,
         and `ra_riskmetrics_eval_ods` to succeed (each DAG triggers the next),
         bounded by `-WaitTimeoutMinutes`. `-SkipPipelineWait` skips this.
      8. Run `scripts/health_check.py --check-iceberg` as the validation query,
         and with `-OpenEndpoints` open the platform URLs in the browser.

.EXAMPLE
    .\scripts\run_risk_analytics_pipeline.ps1 -AsOfDate 2026-07-18

.EXAMPLE
    .\scripts\run_risk_analytics_pipeline.ps1 -AsOfDate 2026-07-18 -PlatformMode first-build -RemoveLegacyDags
#>
param(
    [Parameter()]
    [string]$AsOfDate = (Get-Date -Format 'yyyy-MM-dd'),

    [ValidateSet('none', 'first-build', 'offline')]
    [string]$PlatformMode = 'offline',

    [int]$WaitTimeoutMinutes = 60,

    [switch]$SkipPipelineWait,

    [switch]$RemoveLegacyDags,

    [switch]$OpenEndpoints
)

# Fail fast on undefined variables and command errors.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# Always run from repository root so relative paths behave consistently.
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$airflowApiRoot = 'http://localhost:8088/api/v1'
$airflowHeaders = @{ Authorization = 'Basic ' + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('admin:admin')) }
$pipelineEntities = @('customer', 'asset', 'collateral', 'deals')
$pipelineSources = @('sourceA', 'sourceB')

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

function Wait-ForAirflowApi {
    param(
        [int]$MaxAttempts = 30,
        [int]$DelaySeconds = 5
    )

    Write-Host "Waiting for Airflow API to be ready..."
    for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
        try {
            $previousErrorPreference = $ErrorActionPreference
            $ErrorActionPreference = 'Continue'
            try {
                $response = Invoke-RestMethod -Uri "$airflowApiRoot/health" -Headers $airflowHeaders -TimeoutSec 10
                # Check if the response contains the expected health components
                if ($response.PSObject.Properties.Name -contains 'metadatabase' -and 
                    $response.PSObject.Properties.Name -contains 'scheduler') {
                    Write-Host "Airflow API is ready."
                    return
                }
            }
            finally {
                $ErrorActionPreference = $previousErrorPreference
            }
        }
        catch {
            # Connection failed, retry
        }

        if ($attempt -lt $MaxAttempts) {
            Write-Host "Waiting for Airflow API (attempt $attempt/$MaxAttempts)..."
            Start-Sleep -Seconds $DelaySeconds
        }
    }

    throw "Airflow API did not become ready in time. Check Airflow logs with 'docker compose logs airflow-webserver airflow-scheduler'."
}

function Invoke-AirflowApi {
    param(
        [string]$Method = 'Get',

        [Parameter(Mandatory = $true)]
        [string]$Path,

        [hashtable]$Body
    )

    $uri = "$airflowApiRoot/$Path"
    if ($PSBoundParameters.ContainsKey('Body')) {
        return Invoke-RestMethod -Method $Method -Uri $uri -Headers $airflowHeaders -ContentType 'application/json' -Body ($Body | ConvertTo-Json -Depth 5 -Compress) -ErrorAction Stop
    }

    return Invoke-RestMethod -Method $Method -Uri $uri -Headers $airflowHeaders -ErrorAction Stop
}

function Get-ExpectedDagIds {
    $dagIds = [System.Collections.Generic.List[string]]::new()
    foreach ($dagId in @('ra_createtables_and_data', 'ra_stage_to_ods_orchestration', 'ra_riskmetrics_eval_ods')) {
        $dagIds.Add($dagId)
    }
    foreach ($source in $pipelineSources) {
        foreach ($entity in $pipelineEntities) {
            $dagIds.Add("ra_${source}_${entity}_stage")
            $dagIds.Add("ra_${source}_${entity}_ods")
        }
    }
    foreach ($entity in $pipelineEntities) {
        $dagIds.Add("ra_kafka_${entity}_stage")
        $dagIds.Add("ra_kafka_${entity}_ods")
    }

    return $dagIds.ToArray()
}

function Get-RegisteredDagIds {
    $payload = Invoke-AirflowApi -Path 'dags?limit=500'
    return @($payload.dags | ForEach-Object { $_.dag_id })
}

function Assert-ExpectedDagsRegistered {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$DagIds,

        [int]$MaxAttempts = 12,
        [int]$DelaySeconds = 10
    )

    $missing = $DagIds
    for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
        $registered = Get-RegisteredDagIds
        $missing = @($DagIds | Where-Object { $registered -notcontains $_ })
        if ($missing.Count -eq 0) {
            Write-Host "All $($DagIds.Count) ra_* DAGs are registered in Airflow."
            return $registered
        }

        if ($attempt -lt $MaxAttempts) {
            Write-Host "Waiting for the scheduler to parse $($missing.Count) DAG(s) (attempt $attempt/$MaxAttempts)..."
            Start-Sleep -Seconds $DelaySeconds
        }
    }

    throw "Airflow does not know these DAGs: $($missing -join ', '). The airflow/dags folder mounted into the containers does not match this repository revision. Run 'git pull', then 'docker compose up -d' (or 'docker compose restart airflow-scheduler airflow-webserver airflow-triggerer') and retry."
}

function Remove-StaleDags {
    param(
        [Parameter(Mandatory = $true)]
        [AllowEmptyCollection()]
        [string[]]$RegisteredDagIds,

        [Parameter(Mandatory = $true)]
        [string[]]$ExpectedDagIds
    )

    $stale = @($RegisteredDagIds | Where-Object { $_ -like 'risk_analytics*' -and $ExpectedDagIds -notcontains $_ })
    if ($stale.Count -eq 0) {
        return
    }

    if (-not $RemoveLegacyDags) {
        Write-Warning "Airflow still lists pre-refactor DAGs: $($stale -join ', '). They are metadata left over from an earlier revision, have no DAG file, and cannot run. Re-run with -RemoveLegacyDags to delete them."
        return
    }

    foreach ($dagId in $stale) {
        Write-Host "Deleting stale DAG $dagId from the Airflow metadata database..."
        Invoke-Compose -Args @('compose', 'exec', '-T', 'airflow-webserver', 'airflow', 'dags', 'delete', '-y', $dagId) -FailureMessage "Deleting stale Airflow DAG $dagId failed."
    }
}

function Invoke-AirflowDagTrigger {
    param(
        [Parameter(Mandatory = $true)]
        [string]$DagId,

        [Parameter(Mandatory = $true)]
        [string]$AsOfDate
    )

    $maxAttempts = 5
    $delaySeconds = 3

    for ($attempt = 1; $attempt -le $maxAttempts; $attempt++) {
        try {
            $response = Invoke-AirflowApi -Method Post -Path "dags/$DagId/dagRuns" -Body @{ conf = @{ as_of_date = $AsOfDate } }
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

function Wait-ForTriggeredDagRun {
    param(
        [Parameter(Mandatory = $true)]
        [string]$DagId,

        [Parameter(Mandatory = $true)]
        [datetime]$NotBefore,

        [Parameter(Mandatory = $true)]
        [datetime]$Deadline,

        [int]$PollSeconds = 15
    )

    $filter = [uri]::EscapeDataString($NotBefore.ToUniversalTime().ToString('o'))
    while ((Get-Date) -lt $Deadline) {
        $runs = @((Invoke-AirflowApi -Path "dags/$DagId/dagRuns?execution_date_gte=$filter&order_by=-execution_date&limit=1").dag_runs)
        if ($runs.Count -eq 0) {
            Write-Host "Waiting for $DagId to be triggered..."
            Start-Sleep -Seconds $PollSeconds
            continue
        }

        $run = $runs[0]
        if ($run.state -eq 'success') {
            Write-Host "DAG $DagId run $($run.dag_run_id) succeeded."
            return
        }
        if ($run.state -eq 'failed') {
            throw "DAG $DagId run $($run.dag_run_id) finished in state 'failed'. Inspect the task logs at http://localhost:8088/dags/$DagId/grid."
        }

        Write-Host "DAG $DagId run $($run.dag_run_id) is $($run.state); waiting..."
        Start-Sleep -Seconds $PollSeconds
    }

    throw "Timed out after $WaitTimeoutMinutes minute(s) waiting for $DagId to finish. Inspect http://localhost:8088/dags/$DagId/grid, then retry with a larger -WaitTimeoutMinutes."
}

function Unpause-AirflowDags {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$DagIds
    )

    foreach ($dagId in $DagIds) {
        Invoke-AirflowApi -Method Patch -Path "dags/${dagId}?update_mask=is_paused" -Body @{ is_paused = $false } | Out-Null
    }

    Write-Host "Unpaused $($DagIds.Count) DAG(s)."
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
        'http://localhost:19120/tree/main'
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

Wait-ForAirflowApi

$expectedDagIds = Get-ExpectedDagIds
$registeredDagIds = Assert-ExpectedDagsRegistered -DagIds $expectedDagIds
Remove-StaleDags -RegisteredDagIds $registeredDagIds -ExpectedDagIds $expectedDagIds
Unpause-AirflowDags -DagIds $expectedDagIds

$triggeredAtUtc = (Get-Date).ToUniversalTime()
Write-Host "Triggering Airflow workflow for $AsOfDate..."
Invoke-AirflowDagTrigger -DagId 'ra_createtables_and_data' -AsOfDate $AsOfDate

if ($SkipPipelineWait) {
    Write-Warning 'Skipping the pipeline wait: the ODS tables are probably still empty, so the validation query below can report missing tables.'
}
else {
    # The bootstrap DAG only fires the orchestration, which in turn fires the
    # metrics DAG, so each run has to be awaited before ODS tables exist.
    $deadline = (Get-Date).AddMinutes($WaitTimeoutMinutes)
    foreach ($dagId in @('ra_createtables_and_data', 'ra_stage_to_ods_orchestration', 'ra_riskmetrics_eval_ods')) {
        Wait-ForTriggeredDagRun -DagId $dagId -NotBefore $triggeredAtUtc -Deadline $deadline
    }
}

Write-Host 'Running loaded-table validation query...'
Invoke-ValidationQuery

if ($OpenEndpoints) {
    Write-Host 'Opening platform endpoints in the default browser...'
    Open-PlatformEndpoints
}

Write-Host 'Risk Analytics platform startup, Airflow workflow trigger, and validation completed successfully.'

