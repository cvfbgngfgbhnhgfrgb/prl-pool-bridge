# Download the krig miner (Kryptex's own Pearl/PRL miner, 0% fee) for Windows x64.
$ErrorActionPreference = "Stop"
$ver = "1.2.0"
$url = "https://github.com/kryptex-miners-org/kryptex-miners/releases/download/krig-1-2-0/krig-miner-$ver-win-x64.zip"

Set-Location -Path $PSScriptRoot
Write-Host "Downloading krig $ver ..."
Invoke-WebRequest -Uri $url -OutFile "krig.zip"

Expand-Archive -Path "krig.zip" -DestinationPath "krig-tmp" -Force
$bin = Get-ChildItem -Path "krig-tmp" -Recurse -Filter "krig-miner.exe" | Select-Object -First 1
if (-not $bin) { throw "krig-miner.exe not found inside the archive" }
Copy-Item $bin.FullName -Destination ".\krig-miner.exe" -Force

Remove-Item "krig.zip" -Force
Remove-Item "krig-tmp" -Recurse -Force

Write-Host "OK -> .\krig-miner.exe"
& .\krig-miner.exe --help
