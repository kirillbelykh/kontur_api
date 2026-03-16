[CmdletBinding()]
param()

Set-StrictMode -Version 3
$ErrorActionPreference = "Stop"

function Write-Ok {
    param([string]$Message)
    Write-Host "[+] $Message" -ForegroundColor Green
}

$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectDir = (Resolve-Path (Join-Path $scriptRoot "..")).Path
$launcher = Join-Path $projectDir "run_kontur.vbs"

if (-not (Test-Path $launcher)) {
    throw "Launcher not found: $launcher"
}

$wscript = Join-Path $env:WINDIR "System32\\wscript.exe"
if (-not (Test-Path $wscript)) {
    $wscript = "wscript.exe"
}

$desktop = [Environment]::GetFolderPath("Desktop")
$shortcutPath = Join-Path $desktop "KonturAPI.lnk"

if (Test-Path $shortcutPath) {
    Remove-Item -Path $shortcutPath -Force -ErrorAction SilentlyContinue
}

$shell = New-Object -ComObject WScript.Shell
$shortcut = $shell.CreateShortcut($shortcutPath)
$shortcut.TargetPath = $wscript
$shortcut.Arguments = "`"$launcher`""
$shortcut.WorkingDirectory = $projectDir

$iconPath = Join-Path $projectDir "icon.ico"
if (Test-Path $iconPath) {
    $shortcut.IconLocation = $iconPath
}

$shortcut.Save()
Write-Ok "Shortcut repaired: $shortcutPath"
