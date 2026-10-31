[CmdletBinding()]
param(
    [ValidateSet('preflight', 'config-migrate', 'migrate', 'dry-run', 'run', 'status', 'reviews-list', 'reviews-resolve', 'audit')]
    [string]$Action = 'status',
    [string]$Config = 'config.v5.json',
    [string]$Legacy,
    [string]$Output,
    [string]$Report,
    [int]$Id,
    [string]$Resolution,
    [int]$Generation,
    [switch]$CheckOnly,
    [switch]$PreviousDay,
    [switch]$ResumeOnly,
    [string]$Backfill
)

$ErrorActionPreference = 'Stop'
$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$scriptPath = Join-Path $scriptRoot 'run_pipeline.py'
$pythonExe = if ($env:ANIME_PIPELINE_PYTHON) {
    $env:ANIME_PIPELINE_PYTHON
} else {
    'python'
}

$arguments = @()
switch ($Action) {
    'preflight' { $arguments = @('preflight', '--config', $Config) }
    'config-migrate' {
        if (-not $Legacy -or -not $Output -or -not $Report) {
            throw 'config-migrate requires -Legacy, -Output, and -Report'
        }
        $arguments = @('config-migrate', '--legacy', $Legacy, '--output', $Output, '--report', $Report)
    }
    'migrate' {
        $arguments = @('migrate', '--config', $Config)
        if ($CheckOnly) { $arguments += '--check-only' }
    }
    'dry-run' {
        $arguments = @('dry-run', '--config', $Config)
        if ($PreviousDay) { $arguments += '--previous-day' }
        if ($Backfill) { $arguments += @('--backfill', $Backfill) }
    }
    'run' {
        $arguments = @('run', '--config', $Config)
        if ($ResumeOnly) { $arguments += '--resume-only' }
        if ($PreviousDay) { $arguments += '--previous-day' }
        if ($Backfill) { $arguments += @('--backfill', $Backfill) }
    }
    'status' { $arguments = @('status', '--config', $Config) }
    'reviews-list' { $arguments = @('reviews', 'list', '--config', $Config) }
    'reviews-resolve' {
        if (-not $Id -or -not $Resolution) { throw 'reviews-resolve requires -Id and -Resolution' }
        $arguments = @('reviews', 'resolve', '--config', $Config, '--id', $Id, '--resolution', $Resolution)
        if ($PSBoundParameters.ContainsKey('Generation')) { $arguments += @('--generation', $Generation) }
    }
    'audit' { $arguments = @('audit', '--config', $Config) }
}

& $pythonExe -B $scriptPath @arguments
exit $LASTEXITCODE
