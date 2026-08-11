[CmdletBinding()]
param(
    [string]$RepoUrl = "https://github.com/kirillbelykh/kontur_api.git",
    [string]$Branch = "main"
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

function Invoke-Git {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [switch]$IgnoreExitCode
    )

    & git @Arguments
    $exitCode = $LASTEXITCODE
    if (-not $IgnoreExitCode -and $exitCode -ne 0) {
        throw "git $($Arguments -join ' ') failed with exit code $exitCode."
    }
    return $exitCode
}

function Backup-OrderHistory {
    param([Parameter(Mandatory = $true)][string]$ProjectDir)

    $historyPath = Join-Path $ProjectDir "full_orders_history.json"
    if (-not (Test-Path $historyPath)) {
        return $null
    }

    $backupDir = Join-Path $ProjectDir ".history_update_backup"
    New-Item -ItemType Directory -Force -Path $backupDir | Out-Null
    $backupPath = Join-Path $backupDir ("full_orders_history-" + (Get-Date -Format "yyyyMMdd-HHmmss") + ".json")
    Copy-Item -LiteralPath $historyPath -Destination $backupPath -Force
    Write-Ok "Backed up local order history: $backupPath"
    return $backupPath
}

function Repair-UnmergedOrderHistory {
    param(
        [Parameter(Mandatory = $true)][string]$ProjectDir,
        [Parameter()][string]$BackupPath
    )

    $unmerged = @(& git diff --name-only --diff-filter=U)
    if ($LASTEXITCODE -ne 0) {
        return
    }
    if ($unmerged -notcontains "full_orders_history.json") {
        return
    }

    if (-not $BackupPath) {
        $BackupPath = Backup-OrderHistory -ProjectDir $ProjectDir
    }

    Write-WarnMsg "Detected unfinished merge for full_orders_history.json. Resetting it before update; backup is kept."
    & git merge --abort *> $null
    if ($LASTEXITCODE -ne 0) {
        & git reset --hard HEAD *> $null
        if ($LASTEXITCODE -ne 0) {
            throw "Could not clear unfinished merge before update."
        }
    }
}

function Restore-OrderHistoryBackup {
    param(
        [Parameter(Mandatory = $true)][string]$ProjectDir,
        [Parameter()][string]$BackupPath
    )

    if (-not $BackupPath -or -not (Test-Path $BackupPath)) {
        return
    }

    $historyPath = Join-Path $ProjectDir "full_orders_history.json"
    Copy-Item -LiteralPath $BackupPath -Destination $historyPath -Force
    Write-Ok "Restored local order history backup"

    $python = Join-Path $ProjectDir ".venv\Scripts\python.exe"
    if (-not (Test-Path $python)) {
        $python = "python"
    }

    $mergeScript = @"
from history_db import OrderHistoryDB
db = OrderHistoryDB(startup_sync='none')
db.sync_with_github(force=True, push=True, reason='update_restore_history')
print('Order history restored and synchronized')
"@

    $mergeScript | & $python -
    if ($LASTEXITCODE -ne 0) {
        Write-WarnMsg "Order history backup was restored locally, but synchronization did not complete."
    }
}

function Reset-OrderHistoryForCodeUpdate {
    param([Parameter(Mandatory = $true)][string]$ProjectDir)

    $historyPath = Join-Path $ProjectDir "full_orders_history.json"
    if (-not (Test-Path $historyPath)) {
        return
    }

    Invoke-Git -Arguments @("checkout", "--", "full_orders_history.json") -IgnoreExitCode | Out-Null
}

function Stop-KonturRuntimeProcesses {
    param([Parameter(Mandatory = $true)][string]$ProjectDir)

    $targets = @()
    $projectDirLower = $ProjectDir.ToLowerInvariant()
    try {
        $pythonProcesses = Get-CimInstance Win32_Process -Filter "Name='python.exe' OR Name='pythonw.exe'" -ErrorAction Stop
    } catch {
        Write-WarnMsg "Could not enumerate running Python processes: $($_.Exception.Message)"
        return
    }

    foreach ($process in $pythonProcesses) {
        $commandLine = [string]$process.CommandLine
        if ([string]::IsNullOrWhiteSpace($commandLine)) {
            continue
        }
        if ($commandLine.ToLowerInvariant().Contains($projectDirLower)) {
            $targets += $process
        }
    }

    foreach ($process in $targets) {
        try {
            Stop-Process -Id $process.ProcessId -Force -ErrorAction Stop
            Write-Ok "Stopped process $($process.Name) (PID $($process.ProcessId))"
        } catch {
            Write-WarnMsg "Could not stop process $($process.Name) (PID $($process.ProcessId)): $($_.Exception.Message)"
        }
    }

    if ($targets.Count -gt 0) {
        Start-Sleep -Seconds 2
    }
}

$projectDir = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$installScript = Join-Path $projectDir "scripts\install_windows.ps1"
$logPath = Join-Path $projectDir "kontur_update.log"
$stashCreated = $false
$stashRestored = $false
$stashRestoreFailed = $false
$historyBackupPath = $null
$exitCode = 0

try {
    try {
        Start-Transcript -Path $logPath -Force | Out-Null
    } catch {
    }

    if (-not (Test-Path $installScript)) {
        throw "Installer script was not found: $installScript"
    }

    if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
        throw "Git is not installed. Run setup.bat first."
    }

    Write-Step "Starting full update and rebuild"
    Stop-KonturRuntimeProcesses -ProjectDir $projectDir

    Push-Location $projectDir
    try {
        if (-not (Test-Path (Join-Path $projectDir ".git"))) {
            throw "Project directory is not a git repository: $projectDir"
        }

        $historyBackupPath = Backup-OrderHistory -ProjectDir $projectDir
        Repair-UnmergedOrderHistory -ProjectDir $projectDir -BackupPath $historyBackupPath
        Reset-OrderHistoryForCodeUpdate -ProjectDir $projectDir

        $statusLines = @(& git status --porcelain=v1 --untracked-files=all)
        if ($LASTEXITCODE -ne 0) {
            throw "git status failed."
        }

        $stashableStatusLines = @($statusLines | Where-Object {
            $_ -notmatch 'full_orders_history\.json$' -and $_ -notmatch '^(\?\?| M|M |MM|A |AM| D|D |R |RM|C |CM) \.history_update_backup/'
        })

        if ($stashableStatusLines.Count -gt 0) {
            $stashName = "autostash-before-full-update-" + (Get-Date -Format "yyyyMMdd-HHmmss")
            Write-Step "Saving local changes to temporary git stash"
            Invoke-Git -Arguments @("stash", "push", "-u", "-m", $stashName, "--", ":(exclude)full_orders_history.json", ":(exclude).history_update_backup")
            $stashCreated = $true
        }

        Write-Step "Fetching latest code from origin/$Branch"
        Invoke-Git -Arguments @("fetch", "origin", $Branch)

        $currentBranch = (& git branch --show-current).Trim()
        if ($LASTEXITCODE -ne 0) {
            throw "git branch --show-current failed."
        }

        if ($currentBranch -ne $Branch) {
            Write-Step "Switching repository to branch $Branch"
            Invoke-Git -Arguments @("checkout", $Branch)
        }

        try {
            Write-Step "Applying latest changes with fast-forward pull"
            Invoke-Git -Arguments @("pull", "--ff-only", "origin", $Branch)
        } catch {
            Write-WarnMsg "Fast-forward pull failed. Resetting branch to origin/$Branch."
            Invoke-Git -Arguments @("reset", "--hard", "origin/$Branch")
        }
    } finally {
        Pop-Location
    }

    Write-Step "Running installer to rebuild local environment"
    & powershell -NoProfile -ExecutionPolicy Bypass -File $installScript -RepoUrl $RepoUrl
    if ($LASTEXITCODE -ne 0) {
        throw "Installer rebuild failed with exit code $LASTEXITCODE."
    }

    if ($stashCreated) {
        Push-Location $projectDir
        try {
            Write-Step "Restoring local changes from temporary git stash"
            & git stash pop --index
            if ($LASTEXITCODE -ne 0) {
                $stashRestoreFailed = $true
                Write-WarnMsg "Local changes were not fully restored automatically. They remain in git stash."
            } else {
                $stashRestored = $true
            }
        } finally {
            Pop-Location
        }
    }

    Restore-OrderHistoryBackup -ProjectDir $projectDir -BackupPath $historyBackupPath

    if ($stashRestoreFailed) {
        throw "Update completed, but local changes need manual restore from git stash."
    }

    Write-Ok "Full update and rebuild completed"
} catch {
    $exitCode = 1
    Write-Host ""
    Write-Host "[ERROR] $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "[INFO] Update log: $logPath" -ForegroundColor Yellow
} finally {
    if ($stashCreated -and -not $stashRestored -and -not $stashRestoreFailed) {
        Push-Location $projectDir
        try {
            Write-Step "Restoring local changes from temporary git stash after failed update"
            & git stash pop --index
            if ($LASTEXITCODE -ne 0) {
                $stashRestoreFailed = $true
                Write-WarnMsg "Local changes were not fully restored automatically. They remain in git stash."
            }
        } catch {
            $stashRestoreFailed = $true
            Write-WarnMsg "Could not restore local changes automatically: $($_.Exception.Message)"
        } finally {
            Pop-Location
        }
    }

    try {
        Stop-Transcript | Out-Null
    } catch {
    }
}

exit $exitCode
