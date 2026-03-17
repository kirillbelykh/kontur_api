param(
    [string]$Version = "0.1.0",
    [string]$AppName = "Kontur Go Workbench",
    [string]$OutputFilename = "KonturGoWorkbench",
    [string]$Publisher = "kirillbelykh",
    [string]$WailsCommand = "wails",
    [string]$MakensisCommand = "makensis",
    [switch]$SkipFrontendInstall,
    [switch]$SkipFrontendBuild,
    [switch]$SkipWailsBuild
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = (Resolve-Path (Join-Path $ScriptDir "..")).Path
$FrontendDir = Join-Path $ProjectRoot "frontend"
$BuildBinDir = Join-Path $ProjectRoot "build\bin"
$InstallerOutDir = Join-Path $ProjectRoot "build\installer"
$NsisScript = Join-Path $ProjectRoot "installer\windows\KonturGoWorkbench.nsi"
$ExecutablePath = Join-Path $BuildBinDir ($OutputFilename + ".exe")

function Require-Command([string]$CommandName) {
    if (-not (Get-Command $CommandName -ErrorAction SilentlyContinue)) {
        throw "Required command '$CommandName' was not found in PATH."
    }
}

Require-Command go
Require-Command npm
Require-Command $MakensisCommand
if (-not $SkipWailsBuild) {
    Require-Command $WailsCommand
}

Push-Location $FrontendDir
try {
    if (-not $SkipFrontendInstall -and -not (Test-Path (Join-Path $FrontendDir "node_modules"))) {
        Write-Host "Installing frontend dependencies..."
        npm install
    }

    if (-not $SkipFrontendBuild) {
        Write-Host "Building frontend..."
        npm run build
    }
} finally {
    Pop-Location
}

if (-not $SkipWailsBuild) {
    Push-Location $ProjectRoot
    try {
        Write-Host "Building Wails desktop binary for windows/amd64..."
        & $WailsCommand build -clean -platform windows/amd64
    } finally {
        Pop-Location
    }
}

if (-not (Test-Path $ExecutablePath)) {
    throw "Expected executable not found: $ExecutablePath"
}

New-Item -ItemType Directory -Force -Path $InstallerOutDir | Out-Null

$nsisArgs = @(
    "/DAPP_NAME=$AppName",
    "/DAPP_VERSION=$Version",
    "/DAPP_EXE_NAME=$($OutputFilename).exe",
    "/DAPP_PUBLISHER=$Publisher",
    "/DINSTALL_DIR_NAME=$OutputFilename",
    "/DSOURCE_DIR=$BuildBinDir",
    "/DOUTPUT_DIR=$InstallerOutDir",
    $NsisScript
)

Write-Host "Building NSIS installer..."
& $MakensisCommand @nsisArgs

$installerPath = Join-Path $InstallerOutDir ("{0}-Setup-{1}.exe" -f $OutputFilename, $Version)
if (-not (Test-Path $installerPath)) {
    throw "Installer was not produced: $installerPath"
}

Write-Host "Installer ready: $installerPath"
