import json
import sys
import time
import os
from playwright.sync_api import sync_playwright

def run_smoke_test():
    _raw_url = os.getenv("APP_URL", "http://localhost:8501")
    app_url = _raw_url if "debug=1" in _raw_url else (_raw_url + ("&" if "?" in _raw_url else "?") + "debug=1")
    ledger_path = os.getenv("LEDGER_PATH", "qb_export.csv")

    results = {"url": app_url, "tabs": {}}
    last_body_text = ""
    
    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            page.goto(app_url)
            
            # Allow time for initial load
            page.wait_for_timeout(2000)
            
            # Upload file
            try:
                # Find file input. Streamlit's file uploader is usually an input[type='file'] inside a dropzone
                # Direct upload to hidden input, do not wait for visibility
                page.locator("input[type='file']").first.set_input_files(ledger_path)
            except Exception as e:
                results["error"] = f"File upload failed: {str(e)}"
                print(json.dumps(results, indent=2))
                sys.exit(1)

            # Wait for processing (look for UAT_METRICS_START)
            try:
                page.wait_for_selector("text=UAT_METRICS_START", timeout=30000)
            except:
                results["error"] = "Timeout waiting for UAT metrics (ledger processing)"
                print(json.dumps(results, indent=2))
                # Print debug info
                try:
                    print("DEBUG_BODY_START")
                    print(page.inner_text("body")[-2000:])
                    print("DEBUG_BODY_END")
                except:
                    pass
                sys.exit(1)

            # Tabs to test
            tabs_map = {
                "📈 Forecast & Run Rates": "FORECAST",
                "🔍 Addbacks Analysis": "ADDBACKS",
                "📋 Data Inspection": "DATA_INSPECTION",
                "📑 Project Billing Digital Twin": "BILLING",
                "📚 Accounts & SDE Tuning": "ACCOUNTS_TUNING",
                "⚖️ Reconciliation": "RECONCILIATION",
                "📊 KPI Explorer": "KPI_EXPLORER"
            }
            
            for label, key in tabs_map.items():
                try:
                    # Click tab by role
                    # Sometimes labels have emojis which might be tricky with exact match if font rendering differs?
                    # But usually exact string match works.
                    
                    # Click
                    page.get_by_role("tab", name=label).click()
                    
                    # Wait for render
                    page.wait_for_timeout(1500)
                    
                    content = page.inner_text("body")
                    last_body_text = content

                    marker = f"TAB_OK::{key}"
                    error_marker = f"TAB_ERROR::{key}"

                    if "TAB_ERROR::" in content:
                        results["tabs"][key] = "FAIL: Found TAB_ERROR marker"
                    elif error_marker in content:
                        results["tabs"][key] = f"FAIL: Found {error_marker}"
                    elif marker in content:
                        results["tabs"][key] = "OK"
                    else:
                        results["tabs"][key] = f"FAIL: Marker {marker} not found"

                except Exception as e:
                    results["tabs"][key] = f"ERROR: {str(e)}"

        except Exception as e:
            results["fatal_error"] = str(e)
        finally:
            if 'browser' in locals():
                browser.close()
            
    print(json.dumps(results, indent=2))

    # Fail process if any tab is not OK, or if any error was recorded
    if results.get("error") or results.get("fatal_error"):
        sys.exit(1)

    bad_tabs = {k: v for k, v in results.get("tabs", {}).items() if v != "OK"}
    if bad_tabs:
        # Print last bit of body for debugging
        if last_body_text:
            print("DEBUG_BODY_TAIL_START")
            print(last_body_text[-1500:])
            print("DEBUG_BODY_TAIL_END")
        sys.exit(1)

    sys.exit(0)

if __name__ == "__main__":
    run_smoke_test()
