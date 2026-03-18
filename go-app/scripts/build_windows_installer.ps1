param(
    [string]$Version = "0.1.0",
    [string]$AppName = "Kontur Go Workbench",
    [string]$OutputFilename = "KonturGoWorkbench",
    [string]$Publisher = "kirillbelykh",
    [string]$WailsCommand = "wails",
    [string]$MakensisCommand = "makensis",
    [string]$WebView2BootstrapperUrl = "https://go.microsoft.com/fwlink/p/?LinkId=2124703",
    [string]$CryptoProInstallerPath = "",
    [string]$CryptoProSilentArgs = "/quiet /norestart",
    [string]$CertificatePfxPath = "",
    [string]$CertificatePfxPassword = "",
    [switch]$SkipFrontendInstall,
    [switch]$SkipFrontendBuild,
    [switch]$SkipWailsBuild,
    [switch]$SkipWebView2BootstrapperDownload
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = (Resolve-Path (Join-Path $ScriptDir "..")).Path
$FrontendDir = Join-Path $ProjectRoot "frontend"
$BuildBinDir = Join-Path $ProjectRoot "build\bin"
$WindowsBuildDir = Join-Path $ProjectRoot "build\windows"
$PackageDir = Join-Path $ProjectRoot "build\package\windows"
$InstallerOutDir = Join-Path $ProjectRoot "build\installer"
$NsisScript = Join-Path $ProjectRoot "installer\windows\KonturGoWorkbench.nsi"
$ExecutablePath = Join-Path $BuildBinDir ($OutputFilename + ".exe")
$WebView2BootstrapperPath = Join-Path $WindowsBuildDir "MicrosoftEdgeWebView2Setup.exe"
$CryptoProInstallerCachedBase = Join-Path $WindowsBuildDir "CryptoProSetup"
$CertificatePfxCachedPath = Join-Path $WindowsBuildDir "SigningCertificate.pfx"
$EnvDefaultsPath = Join-Path $PackageDir ".env.defaults"

function Require-Command([string]$CommandName) {
    if (-not (Get-Command $CommandName -ErrorAction SilentlyContinue)) {
        throw "Required command '$CommandName' was not found in PATH."
    }
}

function Resolve-ConfigValue([string]$Name, [string]$Fallback) {
    $value = [Environment]::GetEnvironmentVariable($Name)
    if ([string]::IsNullOrWhiteSpace($value)) {
        return $Fallback
    }
    return $value
}

function Write-EnvDefaults([string]$DestinationPath) {
    $content = @"
BASE_URL=$(Resolve-ConfigValue "BASE_URL" "https://mk.kontur.ru")
ORGANIZATION_ID=$(Resolve-ConfigValue "ORGANIZATION_ID" "5cda50fa-523f-4bb5-85b6-66d7241b23cd")
WAREHOUSE_ID=$(Resolve-ConfigValue "WAREHOUSE_ID" "59739360-7d62-434b-ad13-4617c87a6d13")
PRODUCT_GROUP=$(Resolve-ConfigValue "PRODUCT_GROUP" "wheelChairs")
RELEASE_METHOD_TYPE=$(Resolve-ConfigValue "RELEASE_METHOD_TYPE" "production")
CIS_TYPE=$(Resolve-ConfigValue "CIS_TYPE" "unit")
FILLING_METHOD=$(Resolve-ConfigValue "FILLING_METHOD" "manual")
YANDEX_TARGET_URL=$(Resolve-ConfigValue "YANDEX_TARGET_URL" "https://mk.kontur.ru/organizations/5cda50fa-523f-4bb5-85b6-66d7241b23cd/warehouses")
HISTORY_SYNC_ENABLED=false
"@
    [System.IO.File]::WriteAllText($DestinationPath, $content, [System.Text.UTF8Encoding]::new($false))
}

function Ensure-WebView2Bootstrapper([string]$DestinationPath, [string]$DownloadUrl) {
    if (Test-Path $DestinationPath) {
        return $DestinationPath
    }

    if ($SkipWebView2BootstrapperDownload) {
        throw "Microsoft Edge WebView2 bootstrapper was not found at '$DestinationPath'. Remove -SkipWebView2BootstrapperDownload or place the bootstrapper there first."
    }

    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $DestinationPath) | Out-Null
    Write-Host "Downloading Microsoft Edge WebView2 bootstrapper..."
    Invoke-WebRequest -Uri $DownloadUrl -OutFile $DestinationPath
    return $DestinationPath
}

function Copy-Artifact([string]$SourcePath, [string]$DestinationPath, [string]$Label) {
    if ([string]::IsNullOrWhiteSpace($SourcePath)) {
        return $null
    }
    if (-not (Test-Path $SourcePath)) {
        throw "$Label was not found: $SourcePath"
    }
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $DestinationPath) | Out-Null
    Copy-Item $SourcePath $DestinationPath -Force
    return $DestinationPath
}

function Resolve-CachedArtifactPath([string]$BasePath, [string]$ProvidedPath, [string]$FallbackExtension) {
    $extension = $FallbackExtension
    if (-not [string]::IsNullOrWhiteSpace($ProvidedPath)) {
        $providedExtension = [System.IO.Path]::GetExtension($ProvidedPath)
        if (-not [string]::IsNullOrWhiteSpace($providedExtension)) {
            $extension = $providedExtension
        }
    }
    return "$BasePath$extension"
}

Require-Command $MakensisCommand
if (-not $SkipFrontendInstall -or -not $SkipFrontendBuild) {
    Require-Command npm
}
if (-not $SkipWailsBuild) {
    Require-Command go
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

if (Test-Path $PackageDir) {
    Remove-Item -Recurse -Force $PackageDir
}
New-Item -ItemType Directory -Force -Path $PackageDir | Out-Null
New-Item -ItemType Directory -Force -Path $InstallerOutDir | Out-Null

Copy-Item (Join-Path $BuildBinDir "*") -Destination $PackageDir -Recurse -Force
Write-EnvDefaults $EnvDefaultsPath

$cryptoProInstallerCachedPath = Resolve-CachedArtifactPath -BasePath $CryptoProInstallerCachedBase -ProvidedPath $CryptoProInstallerPath -FallbackExtension ".exe"

$bootstrapperPath = Ensure-WebView2Bootstrapper -DestinationPath $WebView2BootstrapperPath -DownloadUrl $WebView2BootstrapperUrl
$cryptoProInstaller = Copy-Artifact -SourcePath $CryptoProInstallerPath -DestinationPath $cryptoProInstallerCachedPath -Label "CryptoPro installer"
$certificatePfx = Copy-Artifact -SourcePath $CertificatePfxPath -DestinationPath $CertificatePfxCachedPath -Label "certificate PFX"

$nsisArgs = @(
    "/DAPP_NAME=$AppName",
    "/DAPP_VERSION=$Version",
    "/DAPP_EXE_NAME=$($OutputFilename).exe",
    "/DAPP_PUBLISHER=$Publisher",
    "/DINSTALL_DIR_NAME=$OutputFilename",
    "/DSOURCE_DIR=$PackageDir",
    "/DOUTPUT_DIR=$InstallerOutDir"
)

if ($bootstrapperPath) {
    $nsisArgs += "/DWEBVIEW2_BOOTSTRAPPER=$bootstrapperPath"
}
if ($cryptoProInstaller) {
    $nsisArgs += "/DCRYPTOPRO_INSTALLER=$cryptoProInstaller"
    $nsisArgs += "/DCRYPTOPRO_INSTALLER_NAME=$([System.IO.Path]::GetFileName($cryptoProInstaller))"
    $nsisArgs += "/DCRYPTOPRO_INSTALLER_KIND=$([System.IO.Path]::GetExtension($cryptoProInstaller).TrimStart('.').ToLowerInvariant())"
    $nsisArgs += "/DCRYPTOPRO_SILENT_ARGS=$CryptoProSilentArgs"
} else {
    Write-Warning "CryptoPro installer was not provided. The output installer will not be able to install CryptoPro automatically."
}
if ($certificatePfx) {
    $nsisArgs += "/DCERTIFICATE_PFX=$certificatePfx"
    if (-not [string]::IsNullOrWhiteSpace($CertificatePfxPassword)) {
        $nsisArgs += "/DCERTIFICATE_PFX_PASSWORD=$CertificatePfxPassword"
    } else {
        Write-Warning "Certificate PFX was bundled without a password. Automatic import will be skipped."
    }
}

$nsisArgs += $NsisScript

Write-Host "Building NSIS installer..."
& $MakensisCommand @nsisArgs

$installerPath = Join-Path $InstallerOutDir ("{0}-Setup-{1}.exe" -f $OutputFilename, $Version)
if (-not (Test-Path $installerPath)) {
    throw "Installer was not produced: $installerPath"
}

Write-Host "Installer ready: $installerPath"
