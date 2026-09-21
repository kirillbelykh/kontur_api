#Requires -Version 5.1
<#
.SYNOPSIS
  Ensure driver/yandexdriver.exe matches the installed Yandex Browser version.
.DESCRIPTION
  Reads the browser version, picks the matching YandexDriver release from GitHub,
  and downloads it only when the installed driver is missing or mismatched.
#>
[CmdletBinding()]
param(
    [Parameter()][string]$ProjectDir = "",
    [Parameter()][string]$BrowserPath = "",
    [switch]$Force
)

Set-StrictMode -Version 3
$ErrorActionPreference = "Stop"

function Write-Step([string]$Message) { Write-Host "[*] $Message" -ForegroundColor Cyan }
function Write-Ok([string]$Message) { Write-Host "[+] $Message" -ForegroundColor Green }
function Write-WarnMsg([string]$Message) { Write-Host "[!] $Message" -ForegroundColor Yellow }

function Extract-VersionParts {
    param([string]$Text)
    if ([string]::IsNullOrWhiteSpace($Text)) { return @() }
    $match = [regex]::Match($Text, "\d+(?:\.\d+){0,3}")
    if (-not $match.Success) { return @() }
    $parts = @($match.Value.Split('.') | ForEach-Object { [int]$_ })
    while ($parts.Count -lt 4) { $parts += 0 }
    return $parts
}

function Find-YandexBrowserPath {
    $candidates = @(
        (Join-Path $env:LOCALAPPDATA "Yandex\YandexBrowser\Application\browser.exe"),
        (Join-Path ${env:ProgramFiles} "Yandex\YandexBrowser\Application\browser.exe")
    )
    if (${env:ProgramFiles(x86)}) {
        $candidates += (Join-Path ${env:ProgramFiles(x86)} "Yandex\YandexBrowser\Application\browser.exe")
    }
    foreach ($path in $candidates) {
        if ($path -and (Test-Path -LiteralPath $path)) { return $path }
    }
    try {
        $cmd = Get-ItemProperty -Path "HKCU:\Software\Classes\YandexBrowserHTML\shell\open\command" -ErrorAction Stop
        $raw = [string]$cmd.'(default)'
        if ($raw -match '"([^"]+browser\.exe)"') { return $Matches[1] }
    } catch {}
    return $null
}

function Test-YandexMarketingMajor {
    param([int]$Major)
    return ($Major -ge 20 -and $Major -le 40)
}

function Test-ChromiumMajor {
    param([int]$Major)
    return ($Major -ge 80)
}

function Get-YandexBrowserVersionTexts {
    param([Parameter(Mandatory = $true)][string]$BrowserPath)

    $texts = New-Object System.Collections.Generic.List[string]
    $appDir = Split-Path -Parent $BrowserPath
    $versionInfo = $null
    try { $versionInfo = (Get-Item -LiteralPath $BrowserPath).VersionInfo } catch {}
    foreach ($value in @(
        $(if ($versionInfo) { $versionInfo.FileVersion } else { $null }),
        $(if ($versionInfo) { $versionInfo.ProductVersion } else { $null })
    )) {
        if (-not [string]::IsNullOrWhiteSpace($value)) { $texts.Add($value) }
    }

    $folders = @(Get-ChildItem -LiteralPath $appDir -Directory -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -match '^\d+\.\d+' } |
        Sort-Object {
            $p = Extract-VersionParts $_.Name
            "{0:D4}{1:D4}{2:D4}{3:D4}" -f $p[0], $p[1], $p[2], $p[3]
        } -Descending)
    foreach ($folder in $folders) { $texts.Add($folder.Name) }
    return @($texts)
}

function Get-YandexBrowserVersion {
    param([Parameter(Mandatory = $true)][string]$BrowserPath)

    $texts = Get-YandexBrowserVersionTexts -BrowserPath $BrowserPath
    foreach ($candidate in $texts) {
        $parts = Extract-VersionParts -Text $candidate
        if ($parts.Count -gt 0 -and (Test-YandexMarketingMajor -Major $parts[0])) {
            # GitHub YandexDriver tags are 25.x / 26.x, not Chromium 148/150.
            return ($parts -join '.')
        }
    }

    foreach ($candidate in $texts) {
        $parts = Extract-VersionParts -Text $candidate
        if ($parts.Count -gt 0) {
            return ($parts -join '.')
        }
    }
    throw "Unable to detect Yandex Browser version from $BrowserPath"
}

function Get-YandexBrowserChromiumMajor {
    param([Parameter(Mandatory = $true)][string]$BrowserPath)

    foreach ($candidate in (Get-YandexBrowserVersionTexts -BrowserPath $BrowserPath)) {
        $parts = Extract-VersionParts -Text $candidate
        if ($parts.Count -gt 0 -and (Test-ChromiumMajor -Major $parts[0])) {
            return [int]$parts[0]
        }
    }
    return $null
}

function Get-InstalledDriverInfo {
    param([Parameter(Mandatory = $true)][string]$DriverExe)

    $info = [pscustomobject]@{
        Exists = $false
        ChromeMajor = $null
        VersionLine = $null
        ReleaseTag = $null
    }
    if (-not (Test-Path -LiteralPath $DriverExe)) { return $info }
    $info.Exists = $true

    $marker = Join-Path (Split-Path -Parent $DriverExe) "yandexdriver.release"
    if (Test-Path -LiteralPath $marker) {
        $info.ReleaseTag = (Get-Content -LiteralPath $marker -Raw -ErrorAction SilentlyContinue).Trim()
    }

    try {
        $line = & $DriverExe --version 2>&1 | Out-String
        $info.VersionLine = $line.Trim()
        $m = [regex]::Match($line, "ChromeDriver\s+(\d+)")
        if ($m.Success) { $info.ChromeMajor = [int]$m.Groups[1].Value }
    } catch {}

    return $info
}

function Get-YandexDriverSelection {
    param([Parameter(Mandatory = $true)][string]$BrowserVersion)

    $browserParts = Extract-VersionParts -Text $BrowserVersion
    if ($browserParts.Count -eq 0) { throw "Invalid browser version: $BrowserVersion" }

    $headers = @{ "User-Agent" = "KonturAPI-Installer" }
    $releases = Invoke-RestMethod -Uri "https://api.github.com/repos/yandex/YandexDriver/releases?per_page=100" -Headers $headers
    $candidates = New-Object System.Collections.Generic.List[psobject]

    foreach ($release in $releases) {
        $releaseParts = Extract-VersionParts -Text ("$($release.tag_name) $($release.name)")
        if ($releaseParts.Count -eq 0) { continue }

        $assets = @($release.assets | Where-Object { $_.name -match "win" -and $_.name -match "\.zip$" })
        if ($assets.Count -eq 0) { continue }

        $asset = $assets |
            Sort-Object @{ Expression = { if ($_.name -match "win64") { 0 } else { 1 } } }, @{ Expression = { $_.name } } |
            Select-Object -First 1

        $score = 0
        if ($releaseParts[0] -eq $browserParts[0]) {
            $score = 100
            if ($releaseParts[1] -eq $browserParts[1]) {
                $score = 200
                if ($releaseParts[2] -eq $browserParts[2]) { $score = 300 }
            }
        }

        $versionKey = "{0:D4}{1:D4}{2:D4}{3:D4}" -f $releaseParts[0], $releaseParts[1], $releaseParts[2], $releaseParts[3]
        $candidates.Add([pscustomobject]@{
            ReleaseTag  = $release.tag_name
            Score       = $score
            VersionKey  = $versionKey
            DownloadUrl = $asset.browser_download_url
            AssetName   = $asset.name
        })
    }

    if ($candidates.Count -eq 0) {
        throw "No downloadable Windows YandexDriver assets found in GitHub releases."
    }

    $best = $candidates |
        Sort-Object @{ Expression = { $_.Score }; Descending = $true }, @{ Expression = { $_.VersionKey }; Descending = $true } |
        Select-Object -First 1

    if ($best.Score -lt 100) {
        # ProductVersion is often Chromium 150.x while releases are tagged 26.x.
        # Newest Windows asset is the driver that matches the current browser.
        Write-WarnMsg "No YandexDriver tag matches browser $BrowserVersion; using newest Windows release $($best.ReleaseTag)."
    }

    return $best
}

function Stop-YandexDriverProcesses {
    Get-Process -Name "yandexdriver" -ErrorAction SilentlyContinue | ForEach-Object {
        try {
            Stop-Process -Id $_.Id -Force -ErrorAction Stop
            Write-WarnMsg "Stopped yandexdriver PID $($_.Id)"
        } catch {
            Write-WarnMsg "Could not stop yandexdriver PID $($_.Id): $($_.Exception.Message)"
        }
    }
}

function Ensure-YandexDriver {
    param(
        [Parameter(Mandatory = $true)][string]$ProjectDir,
        [Parameter()][string]$BrowserPath = "",
        [switch]$Force
    )

    if ([string]::IsNullOrWhiteSpace($BrowserPath)) {
        $BrowserPath = Find-YandexBrowserPath
    }
    if (-not $BrowserPath -or -not (Test-Path -LiteralPath $BrowserPath)) {
        throw "Yandex Browser not found. Install it before configuring YandexDriver."
    }

    $browserVersion = Get-YandexBrowserVersion -BrowserPath $BrowserPath
    $chromiumMajor = Get-YandexBrowserChromiumMajor -BrowserPath $BrowserPath
    Write-Ok "Yandex Browser version: $browserVersion"
    if ($chromiumMajor) {
        Write-Ok "Yandex Browser Chromium major: $chromiumMajor"
    }

    $selection = Get-YandexDriverSelection -BrowserVersion $browserVersion
    Write-Step "Matching YandexDriver release: $($selection.ReleaseTag) ($($selection.AssetName))"

    $driverDir = Join-Path $ProjectDir "driver"
    New-Item -ItemType Directory -Path $driverDir -Force | Out-Null
    $targetExe = Join-Path $driverDir "yandexdriver.exe"
    $markerPath = Join-Path $driverDir "yandexdriver.release"
    $installed = Get-InstalledDriverInfo -DriverExe $targetExe

    $needsInstall = $Force -or (-not $installed.Exists) -or ($installed.ReleaseTag -ne $selection.ReleaseTag)
    if (-not $needsInstall -and $installed.ChromeMajor -and $chromiumMajor -and ($installed.ChromeMajor -ne $chromiumMajor)) {
        $needsInstall = $true
        Write-WarnMsg "YandexDriver Chrome $($installed.ChromeMajor) != browser Chromium $chromiumMajor. Reinstalling."
    }
    if (-not $needsInstall -and $installed.Exists -and [string]::IsNullOrWhiteSpace($installed.ReleaseTag)) {
        # Legacy install without marker: keep only if driver reports a matching Chrome major.
        if (-not $installed.ChromeMajor -or $installed.ChromeMajor -lt 140 -or ($chromiumMajor -and $installed.ChromeMajor -ne $chromiumMajor)) {
            $needsInstall = $true
            Write-WarnMsg "Installed YandexDriver looks outdated ($($installed.VersionLine)). Reinstalling."
        } else {
            Write-WarnMsg "Driver has no release marker; keeping current binary ($($installed.VersionLine))."
            Set-Content -LiteralPath $markerPath -Value $selection.ReleaseTag -Encoding ASCII
            Write-Ok "Wrote driver marker: $($selection.ReleaseTag)"
            return
        }
    }

    if (-not $needsInstall) {
        Write-Ok "YandexDriver already matches browser ($($selection.ReleaseTag), $($installed.VersionLine))"
        return
    }

    Write-Step "Downloading $($selection.AssetName)"
    $tmpRoot = Join-Path $env:TEMP ("konturapi-yandexdriver-" + [Guid]::NewGuid().ToString("N"))
    New-Item -ItemType Directory -Path $tmpRoot -Force | Out-Null
    try {
        $zipPath = Join-Path $tmpRoot $selection.AssetName
        Invoke-WebRequest -Uri $selection.DownloadUrl -OutFile $zipPath -Headers @{ "User-Agent" = "KonturAPI-Installer" }
        Expand-Archive -Path $zipPath -DestinationPath $tmpRoot -Force
        $driverExe = Get-ChildItem -Path $tmpRoot -Recurse -Filter "yandexdriver.exe" | Select-Object -First 1
        if (-not $driverExe) { throw "yandexdriver.exe was not found in the downloaded archive." }

        Stop-YandexDriverProcesses
        Start-Sleep -Milliseconds 400

        if (Test-Path -LiteralPath $targetExe) {
            try {
                $lockProbe = [System.IO.File]::Open($targetExe, "Open", "ReadWrite", "None")
                $lockProbe.Close()
            } catch {
                throw "Cannot replace YandexDriver because it is locked. Close Kontur API / Selenium and retry."
            }
        }

        Copy-Item -Path $driverExe.FullName -Destination $targetExe -Force
        Set-Content -LiteralPath $markerPath -Value $selection.ReleaseTag -Encoding ASCII
        $after = Get-InstalledDriverInfo -DriverExe $targetExe
        Write-Ok "YandexDriver installed: $targetExe"
        Write-Ok "Driver reports: $($after.VersionLine)"
        Write-Ok "Release marker: $($selection.ReleaseTag)"
    } finally {
        Remove-Item -Path $tmpRoot -Recurse -Force -ErrorAction SilentlyContinue
    }
}

if ([string]::IsNullOrWhiteSpace($ProjectDir)) {
    $ProjectDir = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}

Ensure-YandexDriver -ProjectDir $ProjectDir -BrowserPath $BrowserPath -Force:$Force
