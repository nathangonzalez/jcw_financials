param(
    [int]$Port = 8501,
    [string]$LedgerPath = ".\qb_export.csv",
    [int]$StartupWaitSeconds = 60
)

$ErrorActionPreference = "Stop"

# Create logs directory if it doesn't exist
if (-not (Test-Path "logs")) {
    New-Item -ItemType Directory -Force -Path "logs" | Out-Null
}

# Get git commit hash
try {
    $gitHash = (git rev-parse HEAD 2>$null).Trim()
    if (-not $gitHash) { $gitHash = "unknown" }
} catch {
    $gitHash = "unknown"
}

Write-Host "Starting full pipeline (Git: $gitHash)..."
Write-Host "Port: $Port, Ledger: $LedgerPath"

# Kill any existing process on the port
try {
    $existingProcess = Get-NetTCPConnection -LocalPort $Port -ErrorAction SilentlyContinue
    if ($existingProcess) {
        $processId = $existingProcess.OwningProcess
        Write-Host "Killing existing process on port $Port (PID: $processId)"
        Stop-Process -Id $processId -Force -ErrorAction SilentlyContinue
        Start-Sleep -Seconds 2
    }
} catch {
    # Port not in use, continue
}

# Clear prior log files
@("streamlit_stdout.log", "streamlit_stderr.log", "streamlit_debug.log", "uat_metrics_out.log", "tabs_smoke_out.log") | ForEach-Object {
    "" | Out-File "logs\$_" -Encoding UTF8
}

try {
    # Start Streamlit with debug=1
    Write-Host "Starting Streamlit on port $Port with debug mode..."
    $url = "http://localhost:$Port/?debug=1"
    
    $streamlitArgs = @(
        "-m", "streamlit", "run", "app.py",
        "--server.port", "$Port",
        "--server.address", "0.0.0.0", 
        "--server.headless", "true",
        "--logger.level", "debug"
    )

    $streamlitProcess = Start-Process `
        -FilePath "python" `
        -ArgumentList $streamlitArgs `
        -PassThru `
        -NoNewWindow `
        -RedirectStandardOutput "logs\streamlit_stdout.log" `
        -RedirectStandardError "logs\streamlit_stderr.log"

    # Wait for Streamlit to be ready
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

    Write-Host "Waiting for Streamlit to become ready..."
    $streamlitReady = Wait-ForUrl -TargetUrl $url -TimeoutSeconds $StartupWaitSeconds
    if (-not $streamlitReady) {
        throw "Streamlit did not become ready at $url within $StartupWaitSeconds seconds"
    }

    if (-not (Test-Path $LedgerPath)) {
        throw "Ledger file not found: $LedgerPath"
    }

    # Run pytest
    Write-Host "Running pytest..."
    python -m pytest tests/ -v --tb=short 2>&1 | Tee-Object -FilePath "logs\pytest_out.log"
    $pytestExit = $LASTEXITCODE

    # Run UAT pipeline
    Write-Host "Running UAT pipeline..."
    $env:APP_URL = $url
    $env:LEDGER_PATH = (Resolve-Path $LedgerPath).Path

    # Run UAT metrics
    python .\scripts\uat_playwright_kpis.py 2>&1 | Tee-Object -FilePath "logs\uat_metrics_out.log"
    $uatExit = $LASTEXITCODE

    # Run tabs smoke test
    python .\scripts\uat_tabs_smoke.py 2>&1 | Tee-Object -FilePath "logs\tabs_smoke_out.log"  
    $tabsExit = $LASTEXITCODE

    # Extract UAT payload from logs
    $uatPayload = $null
    try {
        $uatContent = Get-Content "logs\uat_metrics_out.log" -Raw -ErrorAction SilentlyContinue
        if ($uatContent -match "UAT_METRICS_START\n(.*?)\nUAT_METRICS_END") {
            $uatPayload = $matches[1] | ConvertFrom-Json
        }
    } catch {
        Write-Warning "Failed to extract UAT payload: $_"
    }

    # Create summary JSON
    $summary = @{
        timestamp = (Get-Date -Format "yyyy-MM-ddTHH:mm:ssZ")
        git_hash = $gitHash
        pytest_exit_code = $pytestExit
        uat_exit_code = $uatExit  
        tabs_exit_code = $tabsExit
        streamlit_url = $url
        ledger_path = $LedgerPath
        uat_payload = $uatPayload
        overall_success = ($pytestExit -eq 0 -and $uatExit -eq 0 -and $tabsExit -eq 0)
    }

    $summaryJson = $summary | ConvertTo-Json -Depth 10 -Compress
    $summaryJson | Out-File "logs\run_summary.json" -Encoding UTF8

    Write-Host ""
    Write-Host "=== PIPELINE SUMMARY ==="
    Write-Host "Git Hash: $gitHash"
    Write-Host "Pytest Exit: $pytestExit"
    Write-Host "UAT Exit: $uatExit" 
    Write-Host "Tabs Exit: $tabsExit"
    Write-Host "Overall Success: $(($pytestExit -eq 0 -and $uatExit -eq 0 -and $tabsExit -eq 0))"
    Write-Host "Summary: logs\run_summary.json"

    # Check for Arrow serialization errors
    $arrowErrors = (Select-String -Path 'logs\streamlit_stderr.log' -Pattern 'Serialization of dataframe to Arrow table was unsuccessful' -SimpleMatch -ErrorAction SilentlyContinue | Measure-Object).Count
    if ($arrowErrors -gt 0) {
        Write-Warning "Found $arrowErrors Arrow serialization errors in stderr"
    }

    # Exit with failure if any step failed
    if ($pytestExit -ne 0 -or $uatExit -ne 0 -or $tabsExit -ne 0) {
        Write-Host "One or more pipeline steps failed!"
        exit 1
    }

    Write-Host "Pipeline completed successfully!"

} finally {
    # Always cleanup Streamlit process
    if ($streamlitProcess -and -not $streamlitProcess.HasExited) {
        Write-Host "Stopping Streamlit process..."
        Stop-Process -Id $streamlitProcess.Id -Force -ErrorAction SilentlyContinue
    }
}