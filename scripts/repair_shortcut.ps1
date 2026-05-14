[CmdletBinding()]
param()

Set-StrictMode -Version 3
$ErrorActionPreference = "Stop"

function Write-Ok {
    param([string]$Message)
    Write-Host "[+] $Message" -ForegroundColor Green
}

function ConvertFrom-Utf8Base64 {
    param([Parameter(Mandatory = $true)][string]$Value)
    return [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($Value))
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
    $launcherExtension = [System.IO.Path]::GetExtension($launcher).ToLowerInvariant()
    $targetPath = $launcher
    $arguments = ""
    if ($launcherExtension -eq ".vbs") {
        $targetPath = $wscript
        $arguments = "`"$launcher`""
    } elseif ($launcherExtension -in @(".cmd", ".bat")) {
        $cmdExe = $env:ComSpec
        if ([string]::IsNullOrWhiteSpace($cmdExe)) {
            $cmdExe = "cmd.exe"
        }
        $targetPath = $cmdExe
        $arguments = "/c `"$launcher`""
    }

    $desktop = [Environment]::GetFolderPath("Desktop")
    $shortcutPath = Join-Path $desktop "$ShortcutName.lnk"

    if (Test-Path $shortcutPath) {
        Remove-Item -Path $shortcutPath -Force -ErrorAction SilentlyContinue
    }

    $shell = New-Object -ComObject WScript.Shell
    $shortcut = $shell.CreateShortcut($shortcutPath)
    $shortcut.TargetPath = $targetPath
    if (-not [string]::IsNullOrWhiteSpace($arguments)) {
        $shortcut.Arguments = $arguments
    }
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

function Remove-KonturShortcut {
    param(
        [Parameter(Mandatory = $true)][string]$ShortcutName
    )

    $desktop = [Environment]::GetFolderPath("Desktop")
    $shortcutPath = Join-Path $desktop "$ShortcutName.lnk"

    if (Test-Path $shortcutPath) {
        Remove-Item -Path $shortcutPath -Force -ErrorAction SilentlyContinue
        Write-Ok "Shortcut removed: $shortcutPath"
    }
}

New-KonturShortcut -ShortcutName "KonturAPI" -LauncherFile "run_kontur.vbs" -Description "Kontur API classic UI"
New-KonturShortcut -ShortcutName "KonturTestAPI" -LauncherFile "run_kontur_v2.vbs" -Description "Kontur API v2 UI"
New-KonturShortcut -ShortcutName "KonturMobile" -LauncherFile "run_kontur_mobile.vbs" -Description "Kontur API mobile UI"
Remove-KonturShortcut -ShortcutName "KonturAccessProlongation"
New-KonturShortcut -ShortcutName (ConvertFrom-Utf8Base64 "0J7QsdC90L7QstC70LXQvdC40LU=") -LauncherFile (ConvertFrom-Utf8Base64 "0J7QsdC90L7QstC70LXQvdC40LUuYmF0") -Description "Kontur API full update and rebuild"
