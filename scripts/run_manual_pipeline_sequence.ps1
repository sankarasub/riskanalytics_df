param(
    [string]$AsOfDate = "2026-07-18"
)

$repoRoot = Split-Path -Parent $PSScriptRoot
$stepRunner = Join-Path $repoRoot "jobs/run_source_to_ods_step.py"
$riskRunner = Join-Path $repoRoot "jobs/run_risk_pipeline.py"

$sourceBParams = @(
    "--param", "customer_sourceb_path=/opt/risk_analytics/data/sourceb/customer/*.csv",
    "--param", "asset_sourceb_path=/opt/risk_analytics/data/sourceb/asset/*.json",
    "--param", "product_sourceb_path=/opt/risk_analytics/data/sourceb/product/*.json",
    "--param", "trans_sourceb_path=/opt/risk_analytics/data/sourceb/trans/*.csv",
    "--param", "collateral_sourceb_path=/opt/risk_analytics/data/sourceb/collateral/*.json"
)

$entities = @("customer", "asset", "collateral", "deals")
$sources = @("sourcea", "sourceb")

Write-Host "Starting manual pipeline sequence..." -ForegroundColor Cyan

foreach ($source in $sources) {
    foreach ($entity in $entities) {
        foreach ($layer in @("stage", "ods")) {
            Write-Host "\nRunning $layer for $entity ($source)" -ForegroundColor Yellow
            $cmd = @(
                $stepRunner,
                "--layer", $layer,
                "--entity", $entity,
                "--source", $source,
                "--as-of-date", $AsOfDate
            )
            if ($source -eq "sourceb") {
                $cmd += $sourceBParams
            }

            python @cmd
            if ($LASTEXITCODE -ne 0) {
                Write-Host "Step failed for $layer/$entity/$source" -ForegroundColor Red
                exit $LASTEXITCODE
            }
        }
    }
}

Write-Host "\nRunning final risk pipeline (source-to-ods)" -ForegroundColor Yellow
python $riskRunner --as-of-date $AsOfDate --data-model source-to-ods
if ($LASTEXITCODE -ne 0) {
    Write-Host "Final risk pipeline failed" -ForegroundColor Red
    exit $LASTEXITCODE
}

Write-Host "\nManual pipeline sequence completed successfully." -ForegroundColor Green
