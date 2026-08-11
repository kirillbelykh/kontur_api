# Build KonturMarkirovka-Setup.exe (Inno Setup) and a portable payload zip.
[CmdletBinding()]
param(
    [string]$ProjectDir = "",
    [string]$Version = "1.0.0",
    [switch]$SkipInno
)

Set-StrictMode -Version 3
$ErrorActionPreference = "Stop"

function Write-Step([string]$Message) { Write-Host "[*] $Message" -ForegroundColor Cyan }
function Write-Ok([string]$Message) { Write-Host "[+] $Message" -ForegroundColor Green }
function Write-WarnMsg([string]$Message) { Write-Host "[!] $Message" -ForegroundColor Yellow }

if ([string]::IsNullOrWhiteSpace($ProjectDir)) {
    $ProjectDir = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}

$installerDir = Join-Path $ProjectDir "installer"
$payloadDir = Join-Path $installerDir "payload"
$distDir = Join-Path $ProjectDir "dist\installer"
$issPath = Join-Path $installerDir "KonturMarkirovka.iss"

New-Item -ItemType Directory -Force -Path $distDir | Out-Null
if (Test-Path -LiteralPath $payloadDir) {
    Remove-Item -LiteralPath $payloadDir -Recurse -Force
}
New-Item -ItemType Directory -Force -Path $payloadDir | Out-Null

$excludeDirNames = @(
    ".git", ".venv", ".pytest_cache", "__pycache__", "node_modules",
    ".history_update_backup", "dist", "installer", ".cursor", "runtime"
)
$excludeFileGlobs = @("*.pyc", "*.log", ".DS_Store", "uv.lock.bak")

Write-Step "Preparing installer payload from $ProjectDir"

function Should-SkipPath([string]$FullPath, [string]$Root) {
    $rel = $FullPath.Substring($Root.Length).TrimStart('\')
    $parts = $rel.Split('\')
    foreach ($part in $parts) {
        if ($excludeDirNames -contains $part) { return $true }
    }
    foreach ($glob in $excludeFileGlobs) {
        if ($parts[-1] -like $glob) { return $true }
    }
    # Keep frontend/dist, skip huge history backups
    if ($rel -like ".history_update_backup*") { return $true }
    return $false
}

Get-ChildItem -LiteralPath $ProjectDir -Force | ForEach-Object {
    if ($excludeDirNames -contains $_.Name) { return }
    if ($_.Name -eq "installer") { return }
    if ($_.Name -eq "dist") { return }
    # Skip non-ASCII operational folders at repo root (local dumps / soft-delete)
    if ($_.PSIsContainer -and ($_.Name -cnotmatch '^[\x20-\x7E]+$')) { return }
    $dest = Join-Path $payloadDir $_.Name
    if ($_.PSIsContainer) {
        Write-Step "Copy tree: $($_.Name)"
        robocopy $_.FullName $dest /E /XD $excludeDirNames /XF *.pyc *.log .DS_Store /NFL /NDL /NJH /NJS /nc /ns /np | Out-Null
        if ($LASTEXITCODE -ge 8) { throw "robocopy failed for $($_.Name) with code $LASTEXITCODE" }
    } else {
        if ($_.Name -match '\.log$') { return }
        if ($_.Name -eq 'full_orders_history.json') { return }
        Copy-Item -LiteralPath $_.FullName -Destination $dest -Force
    }
}

# Ensure launcher + icon present
foreach ($required in @(
    "main.py",
    "KonturMarkirovka.bat",
    "run_kontur.vbs",
    "assets\icons\icon.ico",
    "scripts\post_install.ps1",
    "scripts\ensure_yandex_driver.ps1",
    "scripts\install_windows.ps1"
)) {
    $path = Join-Path $payloadDir $required
    if (-not (Test-Path -LiteralPath $path)) {
        throw "Payload missing required file: $required"
    }
}

# Patch version in iss
$iss = Get-Content -LiteralPath $issPath -Raw -Encoding UTF8
$iss = [regex]::Replace($iss, '#define MyAppVersion ".*?"', "#define MyAppVersion `"$Version`"")
Set-Content -LiteralPath $issPath -Value $iss -Encoding UTF8

$zipPath = Join-Path $distDir "KonturMarkirovka-$Version-payload.zip"
if (Test-Path -LiteralPath $zipPath) { Remove-Item -LiteralPath $zipPath -Force }
Write-Step "Writing payload zip"
# Prefer tar.exe - Compress-Archive locks files held by editors/scanners.
$tar = Get-Command tar.exe -ErrorAction SilentlyContinue
if ($tar) {
    Push-Location $payloadDir
    try {
        & tar.exe -a -c -f $zipPath *
        if ($LASTEXITCODE -ne 0) { throw "tar failed with exit code $LASTEXITCODE" }
    } finally {
        Pop-Location
    }
} else {
    Compress-Archive -Path (Join-Path $payloadDir "*") -DestinationPath $zipPath -Force
}
Write-Ok "Payload zip: $zipPath"

# PowerShell-only installer next to Setup.exe
$psInstaller = Join-Path $distDir "Install-KonturMarkirovka.ps1"
Copy-Item -LiteralPath (Join-Path $PSScriptRoot "install_app.ps1") -Destination $psInstaller -Force
Write-Ok "PS installer: $psInstaller"

if ($SkipInno) {
    Write-WarnMsg "SkipInno set - Setup.exe not built"
    exit 0
}

$iscc = ""
$pf86 = [Environment]::GetFolderPath("ProgramFilesX86")
$pf = [Environment]::GetFolderPath("ProgramFiles")
$isccCandidates = @(
    (Join-Path $pf86 "Inno Setup 6\ISCC.exe"),
    (Join-Path $pf "Inno Setup 6\ISCC.exe"),
    (Join-Path $env:LOCALAPPDATA "Programs\Inno Setup 6\ISCC.exe")
)
foreach ($candidate in $isccCandidates) {
    if (-not [string]::IsNullOrWhiteSpace($candidate) -and (Test-Path -LiteralPath $candidate)) {
        $iscc = $candidate
        break
    }
}

if ([string]::IsNullOrWhiteSpace($iscc)) {
    Write-Step "Inno Setup not found - downloading installer"
    $tmp = Join-Path $env:TEMP ("innosetup-" + [guid]::NewGuid().ToString("N"))
    New-Item -ItemType Directory -Path $tmp | Out-Null
    $installerExe = Join-Path $tmp "innosetup.exe"
    $url = "https://jrsoftware.org/download.php/is.exe"
    try {
        Invoke-WebRequest -Uri $url -OutFile $installerExe -UseBasicParsing
        Start-Process -FilePath $installerExe -ArgumentList "/VERYSILENT", "/SUPPRESSMSGBOXES", "/NORESTART", "/SP-" -Wait
        foreach ($candidate in $isccCandidates) {
            if (-not [string]::IsNullOrWhiteSpace($candidate) -and (Test-Path -LiteralPath $candidate)) {
                $iscc = $candidate
                break
            }
        }
        # Re-scan common paths after silent install
        $isccCandidates2 = @(
            (Join-Path $pf86 "Inno Setup 6\ISCC.exe"),
            (Join-Path $pf "Inno Setup 6\ISCC.exe")
        )
        foreach ($candidate in $isccCandidates2) {
            if ((Test-Path -LiteralPath $candidate)) { $iscc = $candidate; break }
        }
    } catch {
        Write-WarnMsg "Could not download/install Inno Setup: $($_.Exception.Message)"
        Write-WarnMsg "Falling back to IExpress Setup.exe"
    }
}

if ([string]::IsNullOrWhiteSpace($iscc)) {
    Write-Step "Building Setup.exe with IExpress"
    $sedPath = Join-Path $distDir "KonturMarkirovka.sed"
    $setupBatSource = Join-Path $installerDir "Setup-KonturMarkirovka.bat"
    if (Test-Path -LiteralPath $setupBatSource) {
        Copy-Item -LiteralPath $setupBatSource -Destination (Join-Path $distDir "Setup-KonturMarkirovka.bat") -Force
    }
    $sed = @"
[Version]
Class=IEXPRESS
SEDVersion=3
[Options]
PackagePurpose=InstallApp
ShowInstallProgramWindow=0
HideExtractAnimation=1
UseLongFileName=1
InsideCompressed=0
CAB_FixedSize=0
CAB_ResvCodeSigning=0
RebootMode=N
InstallPrompt=
DisplayLicense=
FinishMessage=
TargetName=$distDir\KonturMarkirovka-Setup.exe
FriendlyName=Kontur Markirovka Setup
AppLaunched=cmd.exe /c Setup-KonturMarkirovka.bat
PostInstallCmd=<None>
AdminQuietInstCmd=
UserQuietInstCmd=
SourceFiles=SourceFiles
[Strings]
FILE0="Setup-KonturMarkirovka.bat"
FILE1="Install-KonturMarkirovka.ps1"
FILE2="KonturMarkirovka-$Version-payload.zip"
[SourceFiles]
SourceFiles0=$distDir\
[SourceFiles0]
%FILE0%=
%FILE1%=
%FILE2%=
"@
    Set-Content -LiteralPath $sedPath -Value $sed -Encoding ASCII
    $iexpress = Join-Path $env:WINDIR "System32\iexpress.exe"
    $proc = Start-Process -FilePath $iexpress -ArgumentList "/N", "/Q", $sedPath -Wait -PassThru -NoNewWindow
    if ($proc.ExitCode -ne 0) {
        Write-WarnMsg "IExpress failed with exit code $($proc.ExitCode). Use Setup-KonturMarkirovka.bat from dist\installer."
        exit 0
    }
    $setup = Get-Item -LiteralPath (Join-Path $distDir "KonturMarkirovka-Setup.exe") -ErrorAction SilentlyContinue
    if ($null -ne $setup) {
        Write-Ok "Installer ready: $($setup.FullName)"
        exit 0
    }
    Write-WarnMsg "Setup.exe was not created."
    exit 0
}

Write-Step "Compiling Setup.exe with $iscc"
Push-Location $installerDir
try {
    & $iscc $issPath
    if ($LASTEXITCODE -ne 0) { throw "ISCC failed with exit code $LASTEXITCODE" }
} finally {
    Pop-Location
}

$setup = Get-ChildItem -LiteralPath $distDir -Filter "KonturMarkirovka-Setup*.exe" | Sort-Object LastWriteTime -Descending | Select-Object -First 1
if ($null -ne $setup) {
    Write-Ok "Installer ready: $($setup.FullName)"
} else {
    Write-WarnMsg "Setup.exe not found in $distDir"
}
