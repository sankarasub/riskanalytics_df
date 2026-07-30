<#
.SYNOPSIS
    Runs the STAGE -> ODS -> risk-metrics job sequence directly, without Airflow.

.DESCRIPTION
    Why this script exists: it is the smallest possible reproduction of what the
    Airflow fan-out does. When a YAML transformation misbehaves you usually do
    not want Docker lifecycle handling, bootstrap, or run reports - you want the
    same job entrypoints the DAGs call, in order, with the output in your own
    terminal. That is all this script does.

    It assumes the platform is already running and the tables already exist. Use
    `scripts/run_local_python_no_airflow.py` instead when you also need Docker
    startup, bootstrap/seeding, previews, and a markdown run report, or
    `scripts/run_risk_analytics_pipeline.ps1` to drive the same flow through the
    Airflow DAGs.

    Steps:
      1. For each source (sourcea, sourceb) run the STAGE layer for all four
         entities in one `jobs/run_source_to_ods_step.py` process, then the ODS
         layer in a second process. One process per layer means one Spark
         session per layer instead of one per entity.
         Source B input paths are container paths, because the Spark Connect
         server resolves them - not this shell.
      2. Run `jobs/run_risk_pipeline.py --data-model source-to-ods` to publish
         `nessie.risk_analytics_ods.risk_metrics` for the as-of date.
    Any non-zero step exits immediately with that step's exit code.

.PARAMETER AsOfDate
    Business as-of date passed to every job.

.PARAMETER Sources
    Sources to process; defaults to both.

.PARAMETER PythonExecutable
    Python used for the jobs. Defaults to `python` on PATH; point it at
    `.venv\Scripts\python.exe` to use the project virtual environment.

.EXAMPLE
    .\scripts\run_manual_pipeline_sequence.ps1 -AsOfDate 2026-07-18

.EXAMPLE
    .\scripts\run_manual_pipeline_sequence.ps1 -AsOfDate 2026-07-18 -Sources sourcea -PythonExecutable .\.venv\Scripts\python.exe
#>
param(
    [string]$AsOfDate = "2026-07-18",

    [ValidateSet('sourcea', 'sourceb')]
    [string[]]$Sources = @('sourcea', 'sourceb'),

    [string]$PythonExecutable = 'python'
)

$repoRoot = Split-Path -Parent $PSScriptRoot
$stepRunner = Join-Path $repoRoot "jobs/run_source_to_ods_step.py"
$riskRunner = Join-Path $repoRoot "jobs/run_risk_pipeline.py"

# Source B ships as CSV/JSON extracts whose locations are YAML parameters. These
# are paths inside the Spark container, which is what resolves the globs.
$sourceBParams = @(
    "--param", "customer_sourceb_path=/opt/risk_analytics/data/sourceb/customer/*.csv",
    "--param", "asset_sourceb_path=/opt/risk_analytics/data/sourceb/asset/*.json",
    "--param", "product_sourceb_path=/opt/risk_analytics/data/sourceb/product/*.json",
    "--param", "trans_sourceb_path=/opt/risk_analytics/data/sourceb/trans/*.csv",
    "--param", "collateral_sourceb_path=/opt/risk_analytics/data/sourceb/collateral/*.json"
)

$entities = @("customer", "asset", "collateral", "deals")
# `--entity` is repeatable, so all entities of a layer share one Spark session.
$entityArgs = @()
foreach ($entity in $entities) {
    $entityArgs += @("--entity", $entity)
}

Write-Host "Starting manual pipeline sequence for $AsOfDate ($($Sources -join ', '))..." -ForegroundColor Cyan

foreach ($source in $Sources) {
    foreach ($layer in @("stage", "ods")) {
        Write-Host "`nRunning $layer for $($entities -join ', ') ($source)" -ForegroundColor Yellow
        $cmd = @(
            $stepRunner,
            "--layer", $layer
        ) + $entityArgs + @(
            "--source", $source,
            "--as-of-date", $AsOfDate
        )
        if ($source -eq "sourceb") {
            $cmd += $sourceBParams
        }

        & $PythonExecutable @cmd
        if ($LASTEXITCODE -ne 0) {
            Write-Host "Step failed for $layer/$source" -ForegroundColor Red
            exit $LASTEXITCODE
        }
    }
}

Write-Host "`nRunning final risk pipeline (source-to-ods)" -ForegroundColor Yellow
& $PythonExecutable $riskRunner --as-of-date $AsOfDate --data-model source-to-ods
if ($LASTEXITCODE -ne 0) {
    Write-Host "Final risk pipeline failed" -ForegroundColor Red
    exit $LASTEXITCODE
}

Write-Host "`nManual pipeline sequence completed successfully." -ForegroundColor Green
