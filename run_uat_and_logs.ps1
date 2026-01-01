param(
  [int]$Port = 8501,
  [string]$Url = "",
  [string]$LedgerPath = ".\qb_export.csv",
  [int]$StartupWaitSeconds = 60,
  [switch]$SkipPytest
)

$ErrorActionPreference = "Stop"

if ($Url -eq "") {
  $Url = "http://localhost:$Port/?debug=1"
}

# Create logs dir
if (-not (Test-Path "logs")) {
  New-Item -ItemType Directory -Force -Path "logs" | Out-Null
}

# Clear prior outputs (optional)
"".Trim() | Out-File "logs\streamlit_stdout.log"
"".Trim() | Out-File "logs\streamlit_stderr.log"
"".Trim() | Out-File "logs\pytest_out.log"
"".Trim() | Out-File "logs\uat_metrics_out.log"
"".Trim() | Out-File "logs\tabs_smoke_out.log"
"".Trim() | Out-File "logs\run_summary.json"

$runStartedAt = Get-Date

Write-Host "Starting Streamlit on $Url ..."
# Kill any existing process on this port (idempotent)
try {
  $existing = Get-NetTCPConnection -LocalPort $Port -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess -Unique
  if ($existing) {
    $existing | ForEach-Object { Stop-Process -Id $_ -Force -ErrorAction SilentlyContinue }
  }
} catch {
  # If Get-NetTCPConnection isn't available, continue; Start-Process will fail if port is busy.
}

# Prefer venv python if present
$pythonExe = $null
if (Test-Path ".\.venv\Scripts\python.exe") {
  $pythonExe = (Resolve-Path ".\.venv\Scripts\python.exe").Path
} else {
  $pythonExe = "python"
}

# Use python -m streamlit to avoid PATH issues
$streamlitArgs = @(
  "-m", "streamlit", "run", "app.py",
  "--server.port", "$Port",
  "--server.address", "0.0.0.0",
  "--server.headless", "true",
  "--logger.level", "debug"
)

$streamlitProcess = Start-Process `
  -FilePath $pythonExe `
  -ArgumentList $streamlitArgs `
  -PassThru `
  -NoNewWindow `
  -RedirectStandardOutput "logs\streamlit_stdout.log" `
  -RedirectStandardError "logs\streamlit_stderr.log"

function Wait-ForUrl([string]$TargetUrl, [int]$TimeoutSeconds) {
  $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
  while ((Get-Date) -lt $deadline) {
    try {
      $resp = Invoke-WebRequest -UseBasicParsing -Uri $TargetUrl -TimeoutSec 5
      if ($resp.StatusCode -ge 200 -and $resp.StatusCode -lt 500) {
        return $true
      }
    } catch {
      Start-Sleep -Seconds 2
    }
  }
  return $false
}

try {
  Write-Host "Waiting for app to become reachable..."
  # Reachability check doesn't need the query string
  $baseUrl = $Url.Split('?')[0]
  $ok = Wait-ForUrl -TargetUrl $baseUrl -TimeoutSeconds $StartupWaitSeconds
  if (-not $ok) {
    throw "Streamlit did not become reachable at $Url within $StartupWaitSeconds seconds."
  }

  if (-not (Test-Path $LedgerPath)) {
    throw "Ledger file not found: $LedgerPath"
  }

  # Run pytest
  $pytestExit = 0
  if (-not $SkipPytest) {
    Write-Host "Running pytest..."
    try {
      & $pythonExe -m pytest -q 2>&1 | Tee-Object -FilePath "logs\pytest_out.log"
      $pytestExit = $LASTEXITCODE
    } catch {
      "PYTEST_EXCEPTION: $($_.Exception.Message)" | Out-File -Append -Encoding utf8 -FilePath "logs\pytest_out.log"
      $pytestExit = 1
    }
  }

  # Run UAT metrics capture
  Write-Host "Running UAT metrics test..."
  # Ensure your test_ui_kpis.py points to $Url, or supports env var APP_URL.
  $env:APP_URL = $Url
  $env:LEDGER_PATH = (Resolve-Path $LedgerPath).Path

  $uatExit = 0
  try {
    & $pythonExe .\scripts\uat_playwright_kpis.py 2>&1 | Tee-Object -FilePath "logs\uat_metrics_out.log"
    $uatExit = $LASTEXITCODE
  } catch {
    "UAT_EXCEPTION: $($_.Exception.Message)" | Out-File -Append -Encoding utf8 -FilePath "logs\uat_metrics_out.log"
    $uatExit = 1
  }

  # Run tab smoke test
  Write-Host "Running tab smoke test..."
  $tabsExit = 0
  try {
    & $pythonExe .\scripts\uat_tabs_smoke.py 2>&1 | Tee-Object -FilePath "logs\tabs_smoke_out.log"
    $tabsExit = $LASTEXITCODE
  } catch {
    "TABS_EXCEPTION: $($_.Exception.Message)" | Out-File -Append -Encoding utf8 -FilePath "logs\tabs_smoke_out.log"
    $tabsExit = 1
  }

  # Build run summary (one-line JSON)
  $gitHash = ""
  try {
    $gitHash = (git rev-parse HEAD 2>$null).Trim()
  } catch {
    $gitHash = ""
  }

  function Parse-JsonFromLog([string]$path) {
    if (-not (Test-Path $path)) { return $null }
    $raw = Get-Content $path -Raw
    $start = $raw.IndexOf('{')
    $end = $raw.LastIndexOf('}')
    if ($start -lt 0 -or $end -le $start) { return $null }
    $json = $raw.Substring($start, $end - $start + 1)
    try {
      return $json | ConvertFrom-Json
    } catch {
      return $null
    }
  }

  $uatObj = Parse-JsonFromLog "logs\uat_metrics_out.log"
  $tabsObj = Parse-JsonFromLog "logs\tabs_smoke_out.log"

  $tabsStatus = $null
  if ($tabsObj -and $tabsObj.tabs) { $tabsStatus = $tabsObj.tabs }

  $summary = [ordered]@{
    git_commit = $gitHash
    pytest_status = $(if ($pytestExit -eq 0) { "PASS" } else { "FAIL" })
    tabs_status = $(if ($tabsExit -eq 0) { "PASS" } else { "FAIL" })
    uat_status = $(if ($uatExit -eq 0) { "PASS" } else { "FAIL" })
    exit_codes = [ordered]@{ pytest = $pytestExit; uat = $uatExit; tabs = $tabsExit }
    url = $Url
    ledger = (Resolve-Path $LedgerPath).Path
    tabs = $tabsStatus
    uat_payload = $(if ($uatObj -and $uatObj.uat_payload) { $uatObj.uat_payload } else { $null })
  }

  $summary.started_at = $runStartedAt.ToString("o")
  $summary.finished_at = (Get-Date).ToString("o")
  $summary.duration_seconds = [math]::Round(((Get-Date) - $runStartedAt).TotalSeconds, 2)

  ($summary | ConvertTo-Json -Compress) | Out-File -Encoding utf8 -FilePath "logs\run_summary.json"

  Write-Host ""
  Write-Host "==================== RUN SUMMARY JSON ===================="
  Write-Host (($summary | ConvertTo-Json -Compress))
  Write-Host "=========================================================="

  Write-Host ""
  Write-Host "Pytest exit code: $pytestExit"
  Write-Host "UAT exit code: $uatExit"
  Write-Host "Tabs exit code: $tabsExit"

  if ($pytestExit -ne 0 -or $uatExit -ne 0 -or $tabsExit -ne 0) {
    Write-Host "One or more tests failed."
    exit 1
  }

  Write-Host "All tests passed."
}
finally {
  Write-Host "Stopping Streamlit..."
  if ($streamlitProcess -and -not $streamlitProcess.HasExited) {
    Stop-Process -Id $streamlitProcess.Id -Force -ErrorAction SilentlyContinue
  }
}

Write-Host "`n==================== PASTE TO CHATGPT ===================="
Write-Host "URL: $Url"
Write-Host "Ledger: $LedgerPath"
Write-Host "`n--- UAT Metrics Output (logs\uat_metrics_out.log) ---"
Get-Content "logs\uat_metrics_out.log" -Tail 200
Write-Host "`n--- Tab Smoke Output (logs\tabs_smoke_out.log) ---"
Get-Content "logs\tabs_smoke_out.log" -Tail 200

Write-Host "`n--- Streamlit STDERR (last 200) ---"
if (Test-Path "logs\streamlit_stderr.log") {
  Get-Content "logs\streamlit_stderr.log" -Tail 200
}
Write-Host "`n--- Streamlit STDOUT (last 200) ---"
if (Test-Path "logs\streamlit_stdout.log") {
  Get-Content "logs\streamlit_stdout.log" -Tail 200
}
Write-Host "=========================================================="
