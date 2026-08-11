[CmdletBinding(SupportsShouldProcess = $true)]
param()

$ProjectDir = Resolve-Path (Join-Path $PSScriptRoot "..")

function Test-GitTracked {
    param([Parameter(Mandatory = $true)][string]$Path)

    $fullPath = (Resolve-Path -LiteralPath $Path).Path
    $rootPath = $ProjectDir.Path.TrimEnd("\") + "\"
    if (-not $fullPath.StartsWith($rootPath, [System.StringComparison]::OrdinalIgnoreCase)) {
        return $false
    }

    $relative = $fullPath.Substring($rootPath.Length).Replace("\", "/")
    git -C $ProjectDir.Path ls-files --error-unmatch -- $relative *> $null
    return $LASTEXITCODE -eq 0
}

$patterns = @(
    "*.log",
    "*.log.*",
    "*.tmp",
    "*.bak",
    "*.old",
    "tmp_*",
    "bundle-legacy.*.js",
    "index.*.js",
    "polyfills.*.js",
    "kontur_cookies.json",
    "kontur_access_prolongation.json",
    "last_snapshot.json"
)

$cacheDirs = @(
    ".mypy_cache",
    ".pytest_cache",
    "__pycache__"
)

$seenFiles = @{}
foreach ($pattern in $patterns) {
    Get-ChildItem -Path $ProjectDir -Filter $pattern -File -Recurse -ErrorAction SilentlyContinue |
        Where-Object {
            $_.FullName -notmatch "\\.git\\" -and
            $_.FullName -notmatch "\\.venv\\" -and
            -not (Test-GitTracked -Path $_.FullName) -and
            -not $seenFiles.ContainsKey($_.FullName)
        } |
        ForEach-Object {
            $seenFiles[$_.FullName] = $true
            if ($PSCmdlet.ShouldProcess($_.FullName, "Remove runtime file")) {
                Remove-Item -LiteralPath $_.FullName -Force
            }
        }
}

foreach ($dirName in $cacheDirs) {
    Get-ChildItem -Path $ProjectDir -Directory -Recurse -Force -ErrorAction SilentlyContinue |
        Where-Object {
            $_.Name -eq $dirName -and
            $_.FullName -notmatch "\\.git\\" -and
            $_.FullName -notmatch "\\.venv\\"
        } |
        ForEach-Object {
            if ($PSCmdlet.ShouldProcess($_.FullName, "Remove cache directory")) {
                Remove-Item -LiteralPath $_.FullName -Recurse -Force
            }
        }
}
