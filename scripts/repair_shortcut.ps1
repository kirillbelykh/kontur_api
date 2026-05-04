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

$wscript = Join-Path $env:WINDIR "System32\\wscript.exe"
if (-not (Test-Path $wscript)) {
    $wscript = "wscript.exe"
}

function New-KonturShortcut {
    param(
        [Parameter(Mandatory = $true)][string]$ShortcutName,
        [Parameter(Mandatory = $true)][string]$LauncherFile,
        [string]$Description = ""
    )

    $launcher = Join-Path $projectDir $LauncherFile
    if (-not (Test-Path $launcher)) {
        throw "Launcher not found: $launcher"
    }

    $desktop = [Environment]::GetFolderPath("Desktop")
    $shortcutPath = Join-Path $desktop "$ShortcutName.lnk"

    if (Test-Path $shortcutPath) {
        Remove-Item -Path $shortcutPath -Force -ErrorAction SilentlyContinue
    }

    $shell = New-Object -ComObject WScript.Shell
    $shortcut = $shell.CreateShortcut($shortcutPath)
    $shortcut.TargetPath = $wscript
    $shortcut.Arguments = "`"$launcher`""
    $shortcut.WorkingDirectory = $projectDir
    if (-not [string]::IsNullOrWhiteSpace($Description)) {
        $shortcut.Description = $Description
    }

    $iconPath = Join-Path $projectDir "icon.ico"
    if (Test-Path $iconPath) {
        $shortcut.IconLocation = $iconPath
    }

    $shortcut.Save()
    Write-Ok "Shortcut repaired: $shortcutPath"
}

New-KonturShortcut -ShortcutName "KonturAPI" -LauncherFile "run_kontur.vbs" -Description "Kontur API classic UI"
New-KonturShortcut -ShortcutName "KonturTestAPI" -LauncherFile "run_kontur_v2.vbs" -Description "Kontur API v2 UI"
