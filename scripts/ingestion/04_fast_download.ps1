$stations = Import-Csv "metadata\final_stations.csv"

$maxConcurrent = 5
$running = @()

foreach ($station in $stations) {

    $id = $station.location_id
    $name = $station.name
    $dest = "data\raw\openaq\historical\location_$id"

    # Skip stations that are already being/fully downloaded manually
    if ($id -in @("17","5657")) {
        Write-Host "Skipping location $id - already downloaded/tested"
        continue
    }

    while (($running | Where-Object {$_.State -eq "Running"}).Count -ge $maxConcurrent) {
        Start-Sleep -Seconds 2
    }

    Write-Host "Starting location $id - $name"

    $job = Start-Job -ScriptBlock {
        param($id, $dest)

        aws s3 sync `
            "s3://openaq-data-archive/records/csv.gz/locationid=$id/" `
            $dest `
            --no-sign-request `
            --exclude "year=2026/*"

    } -ArgumentList $id, $dest

    $running += $job
}

Write-Host ""
Write-Host "Waiting for all downloads to finish..."

$running | Wait-Job | Out-Null

Write-Host ""
Write-Host "Download process completed."

$running | Receive-Job

$running | Remove-Job first one is the one that was used in the I I I I I I I The first one is the I I And I I I I I I I I I The first one is the one that was used in the movie.