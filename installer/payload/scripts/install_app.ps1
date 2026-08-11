# PowerShell installer for Kontur Markirovka (no Inno Setup required).
# Usage:
#   powershell -ExecutionPolicy Bypass -File Install-KonturMarkirovka.ps1
#   powershell -ExecutionPolicy Bypass -File Install-KonturMarkirovka.ps1 -SourceZip .\KonturMarkirovka-1.0.0-payload.zip
[CmdletBinding()]
param(
    [string]$SourceZip = "",
    [string]$SourceDir = "",
    [string]$TargetDir = "",
    [switch]$SkipShortcuts
)

Set-StrictMode -Version 3
$ErrorActionPreference = "Stop"

function Write-Step([string]$Message) { Write-Host "[*] $Message" -ForegroundColor Cyan }
function Write-Ok([string]$Message) { Write-Host "[+] $Message" -ForegroundColor Green }

if ([string]::IsNullOrWhiteSpace($TargetDir)) {
    $TargetDir = Join-Path $env:LOCALAPPDATA "Programs\KonturMarkirovka"
}

$scriptRoot = if ($PSScriptRoot) { $PSScriptRoot } else { (Get-Location).Path }

Write-Step "Installing Kontur Markirovka to $TargetDir"
New-Item -ItemType Directory -Force -Path $TargetDir | Out-Null

if (-not [string]::IsNullOrWhiteSpace($SourceZip) -and (Test-Path -LiteralPath $SourceZip)) {
    $tmp = Join-Path $env:TEMP ("kontur-install-" + [guid]::NewGuid().ToString("N"))
    New-Item -ItemType Directory -Path $tmp | Out-Null
    Expand-Archive -LiteralPath $SourceZip -DestinationPath $tmp -Force
    $payloadRoot = $tmp
    $inner = Get-ChildItem $tmp -Directory | Select-Object -First 1
    if ((Test-Path (Join-Path $tmp "main.py")) -eq $false -and $inner) {
        $payloadRoot = $inner.FullName
    }
    robocopy $payloadRoot $TargetDir /E /NFL /NDL /NJH /NJS /nc /ns /np | Out-Null
    Remove-Item $tmp -Recurse -Force -ErrorAction SilentlyContinue
} elseif (-not [string]::IsNullOrWhiteSpace($SourceDir) -and (Test-Path -LiteralPath $SourceDir)) {
    robocopy $SourceDir $TargetDir /E /XD .git .venv node_modules __pycache__ .pytest_cache dist installer .cursor /NFL /NDL /NJH /NJS /nc /ns /np | Out-Null
} elseif (Test-Path (Join-Path $scriptRoot "..\main.py")) {
    $repo = (Resolve-Path (Join-Path $scriptRoot "..")).Path
    robocopy $repo $TargetDir /E /XD .git .venv node_modules __pycache__ .pytest_cache dist installer .cursor .history_update_backup /NFL /NDL /NJH /NJS /nc /ns /np | Out-Null
} else {
    throw "Provide -SourceZip or -SourceDir, or run from the repo scripts folder."
}

$env:KONTUR_INSTALL_SINGLE_SHORTCUT = "1"
$post = Join-Path $TargetDir "scripts\post_install.ps1"
$full = Join-Path $TargetDir "scripts\install_windows.ps1"
if (Test-Path -LiteralPath $post) {
    & powershell -NoProfile -ExecutionPolicy Bypass -File $post -ProjectDir $TargetDir
} elseif (Test-Path -LiteralPath $full) {
    & powershell -NoProfile -ExecutionPolicy Bypass -File $full
} else {
    throw "No post-install script found in $TargetDir"
}

if (-not $SkipShortcuts) {
    Write-Ok "Desktop shortcut should be: Контур Маркировка"
}

Write-Ok "Installed: $TargetDir"
