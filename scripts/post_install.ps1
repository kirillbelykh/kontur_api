# Post-install setup for Kontur Markirovka (run from installed app dir).
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$ProjectDir
)

Set-StrictMode -Version 3
$ErrorActionPreference = "Stop"

function Write-Step([string]$Message) { Write-Host "[*] $Message" -ForegroundColor Cyan }
function Write-Ok([string]$Message) { Write-Host "[+] $Message" -ForegroundColor Green }
function Write-WarnMsg([string]$Message) { Write-Host "[!] $Message" -ForegroundColor Yellow }

$ProjectDir = (Resolve-Path -LiteralPath $ProjectDir).Path
Set-Location $ProjectDir

# Prefer full installer helpers when present.
$fullInstall = Join-Path $ProjectDir "scripts\install_windows.ps1"
$ensureDriver = Join-Path $ProjectDir "scripts\ensure_yandex_driver.ps1"

Write-Step "Post-install in $ProjectDir"

# Remove legacy desktop shortcuts; keep a single product icon.
$desktop = [Environment]::GetFolderPath("Desktop")
foreach ($name in @(
    "KonturAPI",
    "KonturTestAPI",
    "KonturMobile",
    "CRPT server",
    "KonturAccessProlongation"
)) {
    $path = Join-Path $desktop "$name.lnk"
    if (Test-Path -LiteralPath $path) {
        Remove-Item -LiteralPath $path -Force -ErrorAction SilentlyContinue
        Write-Ok "Removed legacy shortcut: $name"
    }
}

if (Test-Path -LiteralPath $fullInstall) {
    Write-Step "Running environment setup (uv, deps, driver)"
    # Reuse project installer but skip multi-shortcut creation via env flag.
    $env:KONTUR_INSTALL_SINGLE_SHORTCUT = "1"
    & powershell -NoProfile -ExecutionPolicy Bypass -File $fullInstall
    if ($LASTEXITCODE -ne 0) {
        throw "install_windows.ps1 failed with exit code $LASTEXITCODE"
    }
} else {
    Write-WarnMsg "scripts\install_windows.ps1 missing - skipping full setup"
    if (Test-Path -LiteralPath $ensureDriver) {
        & powershell -NoProfile -ExecutionPolicy Bypass -File $ensureDriver -ProjectDir $ProjectDir
    }
}

$icon = Join-Path $ProjectDir "assets\icons\kontur.ico"
$shortcutPath = Join-Path $desktop "Контур Маркировка.lnk"
$pythonw = Join-Path $ProjectDir ".venv\Scripts\pythonw.exe"
$mainPy = Join-Path $ProjectDir "main.py"
if (-not ((Test-Path -LiteralPath $pythonw) -and (Test-Path -LiteralPath $mainPy))) {
    throw "pythonw.exe not found after install: $pythonw"
}
$shell = New-Object -ComObject WScript.Shell
$shortcut = $shell.CreateShortcut($shortcutPath)
$shortcut.TargetPath = $pythonw
$shortcut.Arguments = "`"$mainPy`""
$shortcut.WorkingDirectory = $ProjectDir
$shortcut.Description = "Контур Маркировка"
if (Test-Path -LiteralPath $icon) {
    $shortcut.IconLocation = $icon
}
$shortcut.Save()
Write-Ok "Desktop shortcut: $shortcutPath"
Write-Ok "Post-install completed"
