# Compile root Install.exe (in-place reinstall of the git clone).
[CmdletBinding()]
param(
    [string]$ProjectDir = ""
)

Set-StrictMode -Version 3
$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($ProjectDir)) {
    $ProjectDir = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}

$csc = Join-Path $env:WINDIR "Microsoft.NET\Framework64\v4.0.30319\csc.exe"
if (-not (Test-Path -LiteralPath $csc)) {
    $csc = Join-Path $env:WINDIR "Microsoft.NET\Framework\v4.0.30319\csc.exe"
}
if (-not (Test-Path -LiteralPath $csc)) {
    throw "csc.exe not found. Install .NET Framework 4.x developer tools."
}

$source = Join-Path $ProjectDir "scripts\install_launcher.cs"
$outExe = Join-Path $ProjectDir "Install.exe"
$icon = Join-Path $ProjectDir "assets\icons\kontur.ico"
if (-not (Test-Path -LiteralPath $source)) {
    throw "Missing $source"
}

$args = @(
    "/nologo",
    "/target:exe",
    "/optimize+",
    "/out:$outExe"
)
if (Test-Path -LiteralPath $icon) {
    $args += "/win32icon:$icon"
}
$args += $source

Write-Host "[*] Compiling Install.exe"
& $csc @args
if ($LASTEXITCODE -ne 0) {
    throw "csc failed with exit code $LASTEXITCODE"
}

Write-Host "[+] $outExe"
