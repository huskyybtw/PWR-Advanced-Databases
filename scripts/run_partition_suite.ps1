param(
    [string]$BaseTag = 'baseline_indexes',
    [int]$Populations = 3,
    [string]$CommitMessage = 'Run partition experiments and comparisons'
)

$ErrorActionPreference = 'Stop'

Set-Location (Split-Path -Parent $PSScriptRoot)

$partitionTags = @(
    'partition_cal_yearly',
    'partition_cal_6month',
    'partition_list_year',
    'partition_external_storage'
)

foreach ($tag in $partitionTags) {
    Write-Host "Running benchmark suite for $tag ..."
    python main.py --benchmark-all --populations $Populations --tag $tag

    Write-Host "Comparing $BaseTag vs $tag ..."
    python main.py --compare $BaseTag $tag
}

git add .
git commit -m $CommitMessage
git push