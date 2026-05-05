[CmdletBinding()]
param(
    [string]$RepoUrl = "https://github.com/kirillbelykh/kontur_api.git"
)

Set-StrictMode -Version 3
$ErrorActionPreference = "Stop"

function Write-Step {
    param([string]$Message)
    Write-Host "[*] $Message" -ForegroundColor Cyan
}

function Write-Ok {
    param([string]$Message)
    Write-Host "[+] $Message" -ForegroundColor Green
}

function Write-WarnMsg {
    param([string]$Message)
    Write-Host "[!] $Message" -ForegroundColor Yellow
}

function ConvertFrom-Utf8Base64 {
    param([Parameter(Mandatory = $true)][string]$Value)
    return [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($Value))
}

function Refresh-Path {
    $machinePath = [Environment]::GetEnvironmentVariable("Path", "Machine")
    $userPath = [Environment]::GetEnvironmentVariable("Path", "User")
    $env:Path = "$machinePath;$userPath"
}

function Add-UserPathDirectory {
    param([Parameter(Mandatory = $true)][string]$DirectoryPath)

    if (-not (Test-Path $DirectoryPath)) {
        New-Item -ItemType Directory -Path $DirectoryPath -Force | Out-Null
    }

    $currentUserPath = [Environment]::GetEnvironmentVariable("Path", "User")
    $entries = @()
    if (-not [string]::IsNullOrWhiteSpace($currentUserPath)) {
        $entries = $currentUserPath.Split(';') | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    }

    if ($entries -notcontains $DirectoryPath) {
        $newPath = if ($currentUserPath) { "$currentUserPath;$DirectoryPath" } else { $DirectoryPath }
        [Environment]::SetEnvironmentVariable("Path", $newPath, "User")
    }

    Refresh-Path
}

function Ensure-Winget {
    if (-not (Get-Command winget -ErrorAction SilentlyContinue)) {
        throw "winget is not available. Install App Installer from Microsoft Store and re-run setup.bat."
    }
}

function Ensure-Command {
    param(
        [Parameter(Mandatory = $true)][string]$CommandName,
        [Parameter(Mandatory = $true)][string]$PackageId,
        [Parameter(Mandatory = $true)][string]$DisplayName
    )

    if (Get-Command $CommandName -ErrorAction SilentlyContinue) {
        Write-Ok "$DisplayName is already installed"
        return
    }

    Ensure-Winget
    Write-Step "Installing $DisplayName via winget"

    & winget install --id $PackageId -e --source winget --silent --accept-package-agreements --accept-source-agreements
    if ($LASTEXITCODE -ne 0) {
        throw "winget failed to install $DisplayName (package id: $PackageId)."
    }
    Refresh-Path

    if (-not (Get-Command $CommandName -ErrorAction SilentlyContinue)) {
        throw "$DisplayName installation failed (command '$CommandName' not found after winget install)."
    }

    Write-Ok "$DisplayName installed"
}

function Get-UvAssetName {
    $isArm64 = $false
    try {
        $arch = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString()
        $isArm64 = $arch -eq "Arm64"
    } catch {
    }

    if ($isArm64) {
        return "uv-aarch64-pc-windows-msvc.zip"
    }
    return "uv-x86_64-pc-windows-msvc.zip"
}

function Install-UvFallback {
    Write-Step "Trying fallback installation for uv"

    $tmpRoot = Join-Path $env:TEMP ("konturapi-uv-install-" + [Guid]::NewGuid().ToString("N"))
    New-Item -ItemType Directory -Path $tmpRoot -Force | Out-Null
    $installed = $false

    try {
        $assetName = Get-UvAssetName
        $downloadUrl = "https://github.com/astral-sh/uv/releases/latest/download/$assetName"
        $zipPath = Join-Path $tmpRoot $assetName

        Write-Step "Downloading uv directly from GitHub release"
        Invoke-WebRequest -Uri $downloadUrl -OutFile $zipPath -Headers @{ "User-Agent" = "KonturAPI-Installer" }

        $extractDir = Join-Path $tmpRoot "uv-extract"
        Expand-Archive -Path $zipPath -DestinationPath $extractDir -Force

        $uvExe = Get-ChildItem -Path $extractDir -Recurse -Filter "uv.exe" | Select-Object -First 1
        if (-not $uvExe) {
            throw "uv.exe was not found in downloaded archive."
        }

        $targetDir = Join-Path $env:LOCALAPPDATA "Programs\uv\bin"
        Add-UserPathDirectory -DirectoryPath $targetDir
        Copy-Item -Path $uvExe.FullName -Destination (Join-Path $targetDir "uv.exe") -Force

        $uvxExe = Get-ChildItem -Path $extractDir -Recurse -Filter "uvx.exe" | Select-Object -First 1
        if ($uvxExe) {
            Copy-Item -Path $uvxExe.FullName -Destination (Join-Path $targetDir "uvx.exe") -Force
        }

        if (Get-Command uv -ErrorAction SilentlyContinue) {
            Write-Ok "uv installed via direct download fallback"
            $installed = $true
        }
    } catch {
        Write-WarnMsg "Direct uv download fallback failed: $($_.Exception.Message)"
    }

    if (-not $installed) {
        try {
            Write-Step "Trying Astral install script fallback for uv"
            $installerPath = Join-Path $tmpRoot "install-uv.ps1"
            Invoke-WebRequest -Uri "https://astral.sh/uv/install.ps1" -OutFile $installerPath -Headers @{ "User-Agent" = "KonturAPI-Installer" }
            & powershell -NoProfile -ExecutionPolicy Bypass -File $installerPath
            Refresh-Path

            if (Get-Command uv -ErrorAction SilentlyContinue) {
                Write-Ok "uv installed via Astral install script fallback"
                $installed = $true
            }
        } catch {
            Write-WarnMsg "Astral install script fallback failed: $($_.Exception.Message)"
        }
    }

    Remove-Item -Path $tmpRoot -Recurse -Force -ErrorAction SilentlyContinue
    return $installed
}

function Ensure-Uv {
    if (Get-Command uv -ErrorAction SilentlyContinue) {
        Write-Ok "uv is already installed"
        return
    }

    try {
        Ensure-Command -CommandName "uv" -PackageId "astral-sh.uv" -DisplayName "uv"
    } catch {
        Write-WarnMsg "winget installation of uv failed: $($_.Exception.Message)"
        if (-not (Install-UvFallback)) {
            throw "Failed to install uv. Check internet/proxy/DNS and try again."
        }
    }

    if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
        throw "uv installation failed (command 'uv' still not found)."
    }
}

function Resolve-ProjectDir {
    param([string]$ScriptRoot)

    $localProjectDir = (Resolve-Path (Join-Path $ScriptRoot "..")).Path
    if (Test-Path (Join-Path $localProjectDir "main.pyw")) {
        return $localProjectDir
    }

    Ensure-Command -CommandName "git" -PackageId "Git.Git" -DisplayName "Git"
    $target = Join-Path $env:USERPROFILE "kontur_api"

    if (-not (Test-Path $target)) {
        Write-Step "Cloning repository into $target"
        & git clone $RepoUrl $target
        if ($LASTEXITCODE -ne 0) {
            throw "git clone failed."
        }
    }

    return $target
}

function Get-BrowserPathFromRegValue {
    param([string]$Value)

    if ([string]::IsNullOrWhiteSpace($Value)) {
        return $null
    }

    $quoted = [regex]::Match($Value, '"([^\"]+browser\.exe)"', [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
    if ($quoted.Success) {
        return $quoted.Groups[1].Value
    }

    $plain = [regex]::Match($Value, '([^\s]+browser\.exe)', [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
    if ($plain.Success) {
        return $plain.Groups[1].Value
    }

    return $null
}

function Find-YandexBrowserPath {
    $candidates = New-Object System.Collections.Generic.List[string]

    $regPaths = @(
        "Registry::HKEY_CURRENT_USER\\Software\\Classes\\YandexBrowserHTML\\shell\\open\\command",
        "Registry::HKEY_LOCAL_MACHINE\\Software\\Classes\\YandexBrowserHTML\\shell\\open\\command"
    )

    foreach ($regPath in $regPaths) {
        try {
            $regValue = (Get-ItemProperty -Path $regPath -Name "(default)" -ErrorAction Stop)."(default)"
            $fromReg = Get-BrowserPathFromRegValue -Value $regValue
            if ($fromReg) {
                $candidates.Add($fromReg)
            }
        } catch {
        }
    }

    $filesystemPaths = New-Object System.Collections.Generic.List[string]
    if ($env:LOCALAPPDATA) {
        $filesystemPaths.Add((Join-Path $env:LOCALAPPDATA "Yandex\\YandexBrowser\\Application\\browser.exe"))
    }
    if ($env:PROGRAMFILES) {
        $filesystemPaths.Add((Join-Path $env:PROGRAMFILES "Yandex\\YandexBrowser\\Application\\browser.exe"))
    }
    if (${env:PROGRAMFILES(X86)}) {
        $filesystemPaths.Add((Join-Path ${env:PROGRAMFILES(X86)} "Yandex\\YandexBrowser\\Application\\browser.exe"))
    }

    foreach ($path in $filesystemPaths) {
        $candidates.Add($path)
    }

    foreach ($candidate in $candidates) {
        if ($candidate -and (Test-Path $candidate)) {
            return (Resolve-Path $candidate).Path
        }
    }

    return $null
}

function Ensure-YandexBrowser {
    param([Parameter(Mandatory = $true)][string]$ProjectDir)

    $browserPath = Find-YandexBrowserPath
    if ($browserPath) {
        Write-Ok "Yandex Browser found: $browserPath"
        return $browserPath
    }

    Write-WarnMsg "Yandex Browser not found. Trying installation via winget (Yandex.Browser)."
    try {
        Ensure-Winget
        & winget install --id Yandex.Browser -e --source winget --silent --accept-package-agreements --accept-source-agreements
        if ($LASTEXITCODE -eq 0) {
            Start-Sleep -Seconds 2
        } else {
            Write-WarnMsg "winget returned exit code $LASTEXITCODE when installing Yandex Browser"
        }
    } catch {
        Write-WarnMsg "winget installation of Yandex Browser failed: $($_.Exception.Message)"
    }

    $browserPath = Find-YandexBrowserPath
    if ($browserPath) {
        Write-Ok "Yandex Browser installed: $browserPath"
        return $browserPath
    }

    $localInstaller = Join-Path $ProjectDir "Yandex.exe"
    if (Test-Path $localInstaller) {
        Write-WarnMsg "Trying bundled Yandex.exe installer"
        $silentArgSets = @(
            @("/silent", "/install"),
            @("/S"),
            @("/silent")
        )
        foreach ($argSet in $silentArgSets) {
            try {
                Start-Process -FilePath $localInstaller -ArgumentList $argSet -Wait -NoNewWindow
                Start-Sleep -Seconds 2
                $browserPath = Find-YandexBrowserPath
                if ($browserPath) {
                    Write-Ok "Yandex Browser installed with bundled installer"
                    return $browserPath
                }
            } catch {
                Write-WarnMsg "Yandex.exe with args '$($argSet -join ' ')' failed"
            }
        }
    }

    throw "Yandex Browser is required but was not found after installation attempts."
}

function Extract-VersionParts {
    param([string]$Text)

    if ([string]::IsNullOrWhiteSpace($Text)) {
        return @()
    }

    $match = [regex]::Match($Text, "\d+(?:\.\d+){0,3}")
    if (-not $match.Success) {
        return @()
    }

    $parts = @($match.Value.Split('.') | ForEach-Object { [int]$_ })
    while ($parts.Count -lt 4) {
        $parts += 0
    }

    return $parts
}

function Get-YandexBrowserVersion {
    param([Parameter(Mandatory = $true)][string]$BrowserPath)

    $version = (Get-Item $BrowserPath).VersionInfo.ProductVersion
    if ([string]::IsNullOrWhiteSpace($version)) {
        try {
            $output = & $BrowserPath --version 2>$null
            $match = [regex]::Match($output, "\d+(?:\.\d+){1,3}")
            if ($match.Success) {
                $version = $match.Value
            }
        } catch {
        }
    }

    $parts = Extract-VersionParts -Text $version
    if ($parts.Count -eq 0) {
        throw "Unable to detect Yandex Browser version from $BrowserPath"
    }

    return ($parts -join '.')
}

function Get-YandexDriverSelection {
    param([Parameter(Mandatory = $true)][string]$BrowserVersion)

    $browserParts = Extract-VersionParts -Text $BrowserVersion
    if ($browserParts.Count -eq 0) {
        throw "Invalid browser version: $BrowserVersion"
    }

    $headers = @{ "User-Agent" = "KonturAPI-Installer" }
    $releases = Invoke-RestMethod -Uri "https://api.github.com/repos/yandex/YandexDriver/releases?per_page=100" -Headers $headers

    $candidates = New-Object System.Collections.Generic.List[psobject]

    foreach ($release in $releases) {
        $releaseParts = Extract-VersionParts -Text ("$($release.tag_name) $($release.name)")
        if ($releaseParts.Count -eq 0) {
            continue
        }

        $assets = @($release.assets | Where-Object {
            $_.name -match "win" -and $_.name -match "\.zip$"
        })

        if ($assets.Count -eq 0) {
            continue
        }

        $asset = $assets |
            Sort-Object `
                @{ Expression = { if ($_.name -match "win64") { 0 } else { 1 } } },
                @{ Expression = { $_.name } } |
            Select-Object -First 1

        $score = 0
        if ($releaseParts[0] -eq $browserParts[0]) {
            $score = 100
            if ($releaseParts[1] -eq $browserParts[1]) {
                $score = 200
                if ($releaseParts[2] -eq $browserParts[2]) {
                    $score = 300
                }
            }
        }

        $versionKey = "{0:D4}{1:D4}{2:D4}{3:D4}" -f $releaseParts[0], $releaseParts[1], $releaseParts[2], $releaseParts[3]

        $candidates.Add([pscustomobject]@{
            ReleaseTag = $release.tag_name
            Score = $score
            VersionKey = $versionKey
            DownloadUrl = $asset.browser_download_url
            AssetName = $asset.name
        })
    }

    if ($candidates.Count -eq 0) {
        throw "No downloadable Windows YandexDriver assets found in GitHub releases."
    }

    return $candidates |
        Sort-Object `
            @{ Expression = { $_.Score }; Descending = $true },
            @{ Expression = { $_.VersionKey }; Descending = $true } |
        Select-Object -First 1
}

function Install-YandexDriver {
    param(
        [Parameter(Mandatory = $true)][string]$ProjectDir,
        [Parameter(Mandatory = $true)][string]$BrowserVersion
    )

    $selection = Get-YandexDriverSelection -BrowserVersion $BrowserVersion
    Write-Step "Selected YandexDriver release $($selection.ReleaseTag) asset $($selection.AssetName)"

    $driverDir = Join-Path $ProjectDir "driver"
    New-Item -ItemType Directory -Path $driverDir -Force | Out-Null

    $tmpRoot = Join-Path $env:TEMP ("konturapi-yandexdriver-" + [Guid]::NewGuid().ToString("N"))
    New-Item -ItemType Directory -Path $tmpRoot -Force | Out-Null

    try {
        $zipPath = Join-Path $tmpRoot $selection.AssetName
        Invoke-WebRequest -Uri $selection.DownloadUrl -OutFile $zipPath -Headers @{ "User-Agent" = "KonturAPI-Installer" }

        Expand-Archive -Path $zipPath -DestinationPath $tmpRoot -Force
        $driverExe = Get-ChildItem -Path $tmpRoot -Recurse -Filter "yandexdriver.exe" | Select-Object -First 1
        if (-not $driverExe) {
            throw "yandexdriver.exe was not found in the downloaded archive."
        }

        $targetExe = Join-Path $driverDir "yandexdriver.exe"
        if (Test-Path $targetExe) {
            try {
                $lockProbe = [System.IO.File]::Open($targetExe, "Open", "ReadWrite", "None")
                $lockProbe.Close()
            } catch {
                Write-WarnMsg "Existing YandexDriver is currently in use. Keeping current file: $targetExe"
                return
            }
        }

        Copy-Item -Path $driverExe.FullName -Destination $targetExe -Force
        Write-Ok "YandexDriver installed: $targetExe"
    } finally {
        Remove-Item -Path $tmpRoot -Recurse -Force -ErrorAction SilentlyContinue
    }
}

function Sync-ProjectDependencies {
    param([Parameter(Mandatory = $true)][string]$ProjectDir)

    Push-Location $ProjectDir
    try {
        Write-Step "Installing Python 3.12 via uv (if needed)"
        & uv python install 3.12
        if ($LASTEXITCODE -ne 0) {
            throw "uv python install failed."
        }

        $lockPath = Join-Path $ProjectDir "uv.lock"
        if (Test-Path $lockPath) {
            Write-Step "Running uv sync --frozen"
            & uv sync --python 3.12 --frozen
        } else {
            Write-Step "Running uv sync"
            & uv sync --python 3.12
        }

        if ($LASTEXITCODE -ne 0) {
            throw "uv sync failed."
        }

        Write-Ok "Python dependencies installed"
    } finally {
        Pop-Location
    }
}

function Ensure-EnvFile {
    param([Parameter(Mandatory = $true)][string]$ProjectDir)

    $envPath = Join-Path $ProjectDir ".env"
    if (Test-Path $envPath) {
        Write-Ok ".env already exists"
        return
    }

    $examplePath = Join-Path $ProjectDir ".env.example"
    if (-not (Test-Path $examplePath)) {
        Write-WarnMsg ".env.example was not found. Create .env manually before using Kontur API requests."
        return
    }

    Copy-Item -Path $examplePath -Destination $envPath -Force
    Write-Ok ".env created from .env.example"
}

function Ensure-DesktopDataDirectories {
    $desktop = [Environment]::GetFolderPath("Desktop")
    $dirnames = @(
        (ConvertFrom-Utf8Base64 "0JrQvtC00Ysg0LrQvA=="),
        (ConvertFrom-Utf8Base64 "0JDQs9GA0LXQsyDQutC+0LTRiyDQutC8"),
        (ConvertFrom-Utf8Base64 "0KPQtNCw0LvQtdC90L3Ri9C1")
    )
    foreach ($dirname in $dirnames) {
        $path = Join-Path $desktop $dirname
        if (-not (Test-Path $path)) {
            New-Item -ItemType Directory -Path $path -Force | Out-Null
            Write-Ok "Created data folder: $path"
        }
    }
}

function Test-PythonEnvironment {
    param([Parameter(Mandatory = $true)][string]$ProjectDir)

    $python = Join-Path $ProjectDir ".venv\Scripts\python.exe"
    if (-not (Test-Path $python)) {
        throw "Python executable was not found after dependency sync: $python"
    }

    Write-Step "Checking Python runtime imports"
    & $python -c "import customtkinter, openpyxl, pandas, requests, selenium, trustme, win32com.client, webview; print('ok')"
    if ($LASTEXITCODE -ne 0) {
        throw "Python runtime import check failed."
    }
    Write-Ok "Python runtime imports are available"
}

function Test-BarTenderInstallation {
    $sdkPath = "C:\Program Files\Seagull\BarTender 2022\SDK\Assemblies\Seagull.BarTender.Print.dll"
    if (Test-Path $sdkPath) {
        Write-Ok "BarTender SDK found: $sdkPath"
        return
    }

    Write-WarnMsg "BarTender SDK was not found. Label printing needs BarTender 2022 installed separately."
}

function Create-DesktopShortcut {
    param(
        [Parameter(Mandatory = $true)][string]$ProjectDir,
        [Parameter(Mandatory = $true)][string]$ShortcutName,
        [Parameter(Mandatory = $true)][string]$LauncherFile,
        [string]$Description = ""
    )

    $launcher = Join-Path $ProjectDir $LauncherFile
    if (-not (Test-Path $launcher)) {
        throw "$LauncherFile was not found in project directory."
    }
    $wscript = Join-Path $env:WINDIR "System32\\wscript.exe"
    if (-not (Test-Path $wscript)) {
        $wscript = "wscript.exe"
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
    $shortcut.WorkingDirectory = $ProjectDir
    if (-not [string]::IsNullOrWhiteSpace($Description)) {
        $shortcut.Description = $Description
    }

    $iconPath = Join-Path $ProjectDir "icon.ico"
    if (Test-Path $iconPath) {
        $shortcut.IconLocation = $iconPath
    }

    $shortcut.Save()
    Write-Ok "Shortcut created: $shortcutPath"
}

if ($env:OS -ne "Windows_NT") {
    throw "This installer supports Windows only."
}

Write-Step "Starting KonturAPI setup"

Ensure-Command -CommandName "git" -PackageId "Git.Git" -DisplayName "Git"
Ensure-Uv

$projectDir = Resolve-ProjectDir -ScriptRoot $PSScriptRoot
Write-Ok "Project directory: $projectDir"

Ensure-EnvFile -ProjectDir $projectDir
Ensure-DesktopDataDirectories
Sync-ProjectDependencies -ProjectDir $projectDir
Test-PythonEnvironment -ProjectDir $projectDir

$browserPath = Ensure-YandexBrowser -ProjectDir $projectDir
$browserVersion = Get-YandexBrowserVersion -BrowserPath $browserPath
Write-Ok "Yandex Browser version: $browserVersion"

Install-YandexDriver -ProjectDir $projectDir -BrowserVersion $browserVersion
Test-BarTenderInstallation
Create-DesktopShortcut -ProjectDir $projectDir -ShortcutName "KonturAPI" -LauncherFile "run_kontur.vbs" -Description "Kontur API classic UI"
Create-DesktopShortcut -ProjectDir $projectDir -ShortcutName "KonturTestAPI" -LauncherFile "run_kontur_v2.vbs" -Description "Kontur API v2 UI"
Create-DesktopShortcut -ProjectDir $projectDir -ShortcutName "KonturMobile" -LauncherFile "run_kontur_mobile.vbs" -Description "Kontur API mobile UI"

Write-Host ""
Write-Ok "Installation completed"
Write-Host "Run the app from desktop shortcut: KonturTestAPI"
