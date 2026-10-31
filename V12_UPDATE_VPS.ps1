$ErrorActionPreference = 'Stop'
$Project = 'C:\Sites\kick-live-project'
$TargetBranch = 'v12'
$BackupRoot = 'C:\Sites\kick-live-backups'

Set-Location $Project
$Python = Join-Path $Project 'venv\Scripts\python.exe'
$DbPath = Join-Path $Project 'data\kick_live.db'

if (-not (Test-Path $Python)) { throw "Python venv not found: $Python" }
if (-not (Test-Path (Join-Path $Project '.env'))) { throw 'Existing .env is missing; refusing to update.' }
if (-not (Test-Path $DbPath)) { throw 'Existing SQLite database is missing; refusing to update.' }

# Do not overwrite locally edited tracked files on the VPS.
git diff --quiet
if ($LASTEXITCODE -ne 0) { throw 'Tracked working-tree changes exist. Resolve them before updating.' }
git diff --cached --quiet
if ($LASTEXITCODE -ne 0) { throw 'Staged changes exist. Resolve them before updating.' }

Write-Host "Fetching origin/$TargetBranch ..."
git fetch origin $TargetBranch
if ($LASTEXITCODE -ne 0) { throw "Could not fetch origin/$TargetBranch. Push the branch from your PC first." }
git rev-parse --verify "refs/remotes/origin/$TargetBranch" *> $null
if ($LASTEXITCODE -ne 0) { throw "Remote branch origin/$TargetBranch does not exist." }

$OldBranch = (git branch --show-current).Trim()
$ChangedBranch = $false
$ServiceStopped = $false

try {
    # The current server starts KickRecorder inside KickLive. These old NSSM
    # services point to legacy installations and must not start alongside it.
    foreach ($LegacyName in @('KickRecorder', 'KickServer')) {
        $Legacy = Get-Service -Name $LegacyName -ErrorAction SilentlyContinue
        if ($null -ne $Legacy) {
            if ($Legacy.Status -eq 'Running') {
                Stop-Service -Name $LegacyName -Force
                (Get-Service -Name $LegacyName).WaitForStatus(
                    [System.ServiceProcess.ServiceControllerStatus]::Stopped,
                    [TimeSpan]::FromSeconds(20)
                )
            }
            Set-Service -Name $LegacyName -StartupType Disabled
            Write-Host "Disabled obsolete duplicate service: $LegacyName"
        }
    }

    if ((Get-Service -Name 'KickLive').Status -eq 'Running') {
        Stop-Service -Name 'KickLive'
        (Get-Service -Name 'KickLive').WaitForStatus(
            [System.ServiceProcess.ServiceControllerStatus]::Stopped,
            [TimeSpan]::FromSeconds(30)
        )
    }
    $ServiceStopped = $true

    # Create a transactionally consistent SQLite backup after KickLive stops.
    New-Item -ItemType Directory -Path $BackupRoot -Force | Out-Null
    $BackupDb = Join-Path $BackupRoot ("kick_live_before_v12_{0}.db" -f (Get-Date -Format 'yyyyMMdd_HHmmss'))
    $env:KICK_DB_SOURCE = $DbPath
    $env:KICK_DB_BACKUP = $BackupDb
    $BackupCode = "import os,sqlite3,sys; s=sqlite3.connect(os.environ['KICK_DB_SOURCE']); d=sqlite3.connect(os.environ['KICK_DB_BACKUP']); s.backup(d); d.close(); s.close(); c=sqlite3.connect(os.environ['KICK_DB_BACKUP']); r=c.execute('PRAGMA integrity_check').fetchone()[0]; c.close(); print('Backup integrity:',r); sys.exit(0 if r=='ok' else 1)"
    & $Python -c $BackupCode
    $BackupExit = $LASTEXITCODE
    Remove-Item Env:\KICK_DB_SOURCE -ErrorAction SilentlyContinue
    Remove-Item Env:\KICK_DB_BACKUP -ErrorAction SilentlyContinue
    if ($BackupExit -ne 0 -or -not (Test-Path $BackupDb)) { throw 'SQLite backup failed; code was not switched.' }
    Write-Host "Database backup created: $BackupDb"

    $ExistingBranch = git branch --list $TargetBranch
    if ([string]::IsNullOrWhiteSpace(($ExistingBranch -join '').Trim())) {
        git switch --track -c $TargetBranch "origin/$TargetBranch"
    } else {
        git switch $TargetBranch
    }
    if ($LASTEXITCODE -ne 0) { throw "Could not switch to $TargetBranch." }
    $ChangedBranch = ($OldBranch -ne $TargetBranch)

    git pull --ff-only origin $TargetBranch
    if ($LASTEXITCODE -ne 0) { throw 'Fast-forward update failed.' }

    $Version = (Get-Content (Join-Path $Project 'VERSION') -Raw).Trim()
    if ($Version -ne '12.0.0') { throw "Unexpected code version: $Version" }

    $PyFiles = Get-ChildItem -Path $Project -Filter '*.py' -File | ForEach-Object { $_.FullName }
    & $Python -m py_compile $PyFiles
    if ($LASTEXITCODE -ne 0) { throw 'Python compilation check failed.' }
    & $Python -c 'import server, kick_realtime; print("Python imports OK")'
    if ($LASTEXITCODE -ne 0) { throw 'Python import check failed.' }

    Start-Service -Name 'KickLive'
    (Get-Service -Name 'KickLive').WaitForStatus(
        [System.ServiceProcess.ServiceControllerStatus]::Running,
        [TimeSpan]::FromSeconds(20)
    )

    $ApiReady = $false
    for ($i = 0; $i -lt 12; $i++) {
        try {
            $null = Invoke-WebRequest 'http://127.0.0.1:8000/api/status' -UseBasicParsing -TimeoutSec 4
            $ApiReady = $true
            break
        } catch {
            Start-Sleep -Seconds 2
        }
    }
    if (-not $ApiReady) { throw 'KickLive started but /api/status did not return successfully.' }

    Write-Host "V$Version deployed. .env and data directory were preserved. Backup: $BackupDb"
    Write-Host 'Latest application log lines:'
    Get-Content (Join-Path $Project 'data\kicklive_stdout.log') -Tail 45

    Start-Sleep -Seconds 10
    $ConnectedLine = Select-String -Path (Join-Path $Project 'data\kicklive_stdout.log') -Pattern '\[KICK\] Realtime bağlı: provider=' | Select-Object -Last 1
    if ($ConnectedLine) {
        Write-Host 'Realtime transport reports connected:'
        Write-Host $ConnectedLine.Line
    } else {
        Write-Warning 'HTTP is up, but no successful realtime connection is logged yet. Review the new tail above; the site/API remains running.'
        Get-Content (Join-Path $Project 'data\kicklive_stdout.log') -Tail 20
    }
}
catch {
    Write-Host "Update failed: $_" -ForegroundColor Red
    if ($ServiceStopped) {
        try {
            if ((Get-Service -Name 'KickLive').Status -eq 'Running') {
                Stop-Service -Name 'KickLive' -Force
                Start-Sleep -Seconds 2
            }
            if ($ChangedBranch -and -not [string]::IsNullOrWhiteSpace($OldBranch)) {
                git switch $OldBranch
                if ($LASTEXITCODE -ne 0) { throw "Could not roll back to $OldBranch." }
            }
            Start-Service -Name 'KickLive'
            Write-Warning "Tried to restore the previous branch ($OldBranch) and restart KickLive. Check the service and backup before further changes."
        } catch {
            Write-Warning "Automatic recovery was not fully successful: $_"
        }
    }
    throw
}
