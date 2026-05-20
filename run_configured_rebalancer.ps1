Set-Location "D:\python\nft_projects"

$logDir = "D:\python\nft_projects\latest_farms\logs"
$logFile = Join-Path $logDir "configured_rebalancer.log"

$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
"[$timestamp] Starting configured_pool_rebalancer" | Out-File -FilePath $logFile -Append

$env:PYTHONIOENCODING = "utf-8"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

python -m latest_farms.configured_pool_rebalancer.cli --config my_rebalance_config.json --migrate --execute >> $logFile 2>&1

$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
"[$timestamp] Done" | Out-File -FilePath $logFile -Append
