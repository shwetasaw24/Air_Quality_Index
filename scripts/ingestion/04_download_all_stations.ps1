# ============================================================
# OpenAQ Historical Data Downloader
# Downloads all selected stations in batches of 5
# Uses AWS S3 sync so already-downloaded files are skipped
# ============================================================

$stations = Import-Csv "metadata\final_stations.csv"

# Stations already downloaded/tested
$skipStations = @("17", "5657")

# Number of stations downloading simultaneously
$batchSize = 5

$totalStations = $stations.Count
$completed = 0

Write-Host ""
Write-Host "============================================================"
Write-Host " OpenAQ Historical Data Download"
Write-Host "============================================================"
Write-Host "Total selected stations : $totalStations"
Write-Host "Batch size              : $batchSize"
Write-Host "Years                   : Available archive data through 2025"
Write-Host "Already downloaded      : 17, 5657"
Write-Host "============================================================"
Write-Host ""

# Remove stations we already downloaded/tested
$stationsToDownload = $stations | Where-Object {
    $_.location_id -notin $skipStations
}

$totalRemaining = $stationsToDownload.Count

for ($i = 0; $i -lt $totalRemaining; $i += $batchSize) {

    $batch = $stationsToDownload | Select-Object -Skip $i -First $batchSize

    Write-Host ""
    Write-Host "------------------------------------------------------------"
    Write-Host "Starting batch: stations $($i + 1) to $([Math]::Min($i + $batchSize, $totalRemaining)) of $totalRemaining"
    Write-Host "------------------------------------------------------------"

    $jobs = @()

    foreach ($station in $batch) {

        $id = $station.location_id
        $name = $station.name

        $dest = Join-Path (Get-Location) "data\raw\openaq\historical\location_$id"

        Write-Host "Starting station $id - $name"

        $job = Start-Job -ScriptBlock {

            param(
                $id,
                $name,
                $dest
            )

            New-Item -ItemType Directory -Force -Path $dest | Out-Null

            Write-Output "[$id] Download started: $name"

            aws s3 sync `
                "s3://openaq-data-archive/records/csv.gz/locationid=$id/" `
                $dest `
                --no-sign-request `
                --exclude "year=2026/*"

            $exitCode = $LASTEXITCODE

            if ($exitCode -eq 0) {
                Write-Output "[$id] SUCCESS"
            }
            else {
                Write-Output "[$id] FAILED - AWS exit code $exitCode"
            }

        } -ArgumentList $id, $name, $dest

        $jobs += $job
    }

    Write-Host ""
    Write-Host "Waiting for this batch to finish..."

    $jobs | Wait-Job | Out-Null

    foreach ($job in $jobs) {
        Receive-Job $job
    }

    $jobs | Remove-Job

    $completed = [Math]::Min($i + $batchSize, $totalRemaining)

    Write-Host ""
    Write-Host "Batch completed."
    Write-Host "Stations processed: $completed / $totalRemaining"
}

Write-Host ""
Write-Host "============================================================"
Write-Host " ALL DOWNLOADS FINISHED"
Write-Host "============================================================"