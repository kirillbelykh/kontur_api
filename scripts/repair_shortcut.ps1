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
        [string]$Description = "",
        [string]$TargetFolder = [Environment]::GetFolderPath("Desktop")
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

    if (-not (Test-Path $TargetFolder)) {
        New-Item -ItemType Directory -Path $TargetFolder -Force | Out-Null
    }
    $shortcutPath = Join-Path $TargetFolder "$ShortcutName.lnk"

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

    $iconPath = Join-Path $projectDir "assets\icons\icon.ico"
    if (-not (Test-Path $iconPath)) {
        $iconPath = Join-Path $projectDir "icon.ico"
    }
    if (Test-Path $iconPath) {
        $shortcut.IconLocation = $iconPath
    }

    $shortcut.Save()
    Write-Ok "Shortcut repaired: $shortcutPath"
}

function New-KonturStartupShortcut {
    param(
        [Parameter(Mandatory = $true)][string]$ShortcutName,
        [Parameter(Mandatory = $true)][string]$LauncherFile,
        [string]$Description = ""
    )

    $startupFolder = [Environment]::GetFolderPath("Startup")
    New-KonturShortcut -ShortcutName $ShortcutName -LauncherFile $LauncherFile -Description $Description -TargetFolder $startupFolder
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

# Single primary app shortcut + optional CRPT bridge + update helper
Remove-KonturShortcut -ShortcutName "KonturTestAPI"
Remove-KonturShortcut -ShortcutName "KonturMobile"
Remove-KonturShortcut -ShortcutName "KonturAccessProlongation"
New-KonturShortcut -ShortcutName "KonturAPI" -LauncherFile "run_kontur.vbs" -Description "Kontur Markirovka"
New-KonturShortcut -ShortcutName "CRPT server" -LauncherFile "scripts\launchers\run_crpt_server.vbs" -Description "Kontur API background bridge"
New-KonturStartupShortcut -ShortcutName "CRPT server" -LauncherFile "scripts\launchers\run_crpt_server.vbs" -Description "Kontur API background bridge"
New-KonturShortcut -ShortcutName (ConvertFrom-Utf8Base64 "0J7QsdC90L7QstC70LXQvdC40LU=") -LauncherFile "update.bat" -Description "Kontur API full update and rebuild"
