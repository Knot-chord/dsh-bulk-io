/**
 * Pwsh PowerShell wrapper around the dsh-bulk CLI.
 *
 * The point of this file is the user's own idea: one short command in pwsh 7
 * that hands a whole batch to a warm runtime instead of looping Copy-Item /
 * Set-Content over thousands of files. It does not need PowerShell modules; it
 * only locates a Node runtime and forwards the arguments.
 *
 *   dsh-bulk write  -Root C:\repo -From .\files.json
 *   dsh-bulk read   -Root C:\repo -Include 'src/**\/*.ts'
 *   dsh-bulk copy   -Dest D:\out  -Sources C:\repo\src
 *   dsh-bulk delete -Root C:\repo -Include 'tmp/**'
 *   dsh-bulk scan   -Root C:\repo -Include '**\/*.js' -Json
 */

[CmdletBinding()]
param(
  [Parameter(Position = 0, Mandatory = $true)]
  [ValidateSet('write', 'read', 'copy', 'move', 'sync', 'delete', 'scan', 'doctor')]
  [string]$Command,

  [string]$Root,
  [string]$Dest,
  [string[]]$Sources,
  [string]$From,
  [string[]]$Include,
  [string[]]$Exclude,
  [string]$Grep,
  [int]$MaxFiles = 0,
  [int]$MaxBytes = 0,
  [switch]$Permanent,
  [switch]$DryRun,
  [switch]$Json,
  [string]$NodePath
)

$ErrorActionPreference = 'Stop'

function Resolve-Node {
  if ($NodePath) { return $NodePath }
  $cmd = Get-Command node -ErrorAction SilentlyContinue
  if ($cmd) { return $cmd.Source }
  foreach ($candidate in @(
      "$env:ProgramFiles\nodejs\node.exe",
      "$env:LOCALAPPDATA\Programs\nodejs\node.exe",
      "$env:USERPROFILE\scoop\apps\nodejs\current\node.exe"
    )) {
    if (Test-Path -LiteralPath $candidate) { return $candidate }
  }
  throw 'node.exe not found on PATH. Install Node.js 20+ or pass -NodePath.'
}

$cli = Join-Path $PSScriptRoot '..\lib\cli.mjs'
if (-not (Test-Path -LiteralPath $cli)) {
  throw "dsh-bulk CLI not found next to this script: $cli"
}

$argv = @($cli, $Command)
if ($Root) { $argv += @('--root', $Root) }
if ($Dest) { $argv += @('--dest', $Dest) }
if ($From) { $argv += @('--from', $From) }
foreach ($s in $Sources) { $argv += @('--source', $s) }
foreach ($p in $Include) { $argv += @('--include', $p) }
foreach ($p in $Exclude) { $argv += @('--exclude', $p) }
if ($Grep) { $argv += @('--grep', $Grep) }
if ($MaxFiles -gt 0) { $argv += @('--max-files', "$MaxFiles") }
if ($MaxBytes -gt 0) { $argv += @('--max-bytes', "$MaxBytes") }
if ($Permanent) { $argv += '--permanent' }
if ($DryRun) { $argv += '--dry-run' }
if ($Json) { $argv += '--json' }

& (Resolve-Node) @argv
exit $LASTEXITCODE
