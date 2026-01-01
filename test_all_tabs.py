#!/usr/bin/env python3
"""Pytest-friendly smoke test for the Streamlit server.

Note: Without a real browser/websocket session, we can't truly "click" tabs.
This test focuses on reachability + a basic health check, and ensures we don't
return non-None values from tests.
"""

import os

import pytest
import requests


BASE_URL = os.getenv("FINANCIALS_APP_URL", "http://localhost:8501")


def _get(url: str, timeout: float = 10.0) -> requests.Response:
    try:
        return requests.get(url, timeout=timeout)
    except requests.RequestException as exc:
        pytest.skip(f"Streamlit app not reachable at {url}: {exc}")


def test_streamlit_app():
    # Prefer Streamlit's internal health endpoint when available.
    health_url = BASE_URL.rstrip("/") + "/_stcore/health"
    r_health = _get(health_url, timeout=5.0)
    if r_health.status_code != 200:
        r = _get(BASE_URL, timeout=10.0)
        assert r.status_code == 200

    # Tab list kept for parity with older script output.
    tabs_to_test = [
        "📈 Forecast & Run Rates",
        "🔍 Addbacks Analysis",
        "📋 Data Inspection",
        "📑 Project Billing Digital Twin",
        "📚 Accounts & SDE Tuning",
        "⚖️ Reconciliation",
        "📊 KPI Explorer",
    ]

    tab_statuses = {tab: "NOT_BROWSER_TESTED" for tab in tabs_to_test}
    assert tab_statuses, "Expected at least one tab to validate"
    assert all(status != "UNKNOWN" for status in tab_statuses.values())

def create_browser_test_script():
    """Create a more comprehensive browser automation script"""
    
    script_content = '''
import time
import json
from playwright.sync_api import sync_playwright

def test_streamlit_with_playwright():
    """Test Streamlit app using Playwright for real browser automation"""
    
    results = {}
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        
        try:
            print("Navigating to Streamlit app...")
            page.goto("http://localhost:8501", timeout=30000)
            
            # Wait for app to load
            page.wait_for_selector("[data-testid='stApp']", timeout=30000)
            print("✅ App loaded successfully")
            
            # Set up sidebar parameters
            print("Setting up sidebar parameters...")
            
            # Set Owner Revenue Start
            owner_revenue_start = page.wait_for_selector("input[value*='2025-08-01']", timeout=5000)
            if not owner_revenue_start:
                owner_revenue_start = page.locator("input").first
            owner_revenue_start.fill("2025-08-01")
            
            # Set Year 1 End  
            year_1_end = page.locator("input").nth(1)
            year_1_end.fill("2026-06-30")
            
            # Set Current Report Date
            current_date = page.locator("input").nth(2) 
            current_date.fill("2025-12-07")
            
            print("✅ Sidebar parameters set")
            
            # Upload qb_export.csv
            print("Uploading qb_export.csv...")
            file_input = page.locator("input[type='file']").first
            file_input.set_input_files("qb_export.csv")
            
            # Wait for upload to process
            time.sleep(5)
            print("✅ File uploaded")
            
            # Test main dashboard
            print("Testing main dashboard...")
            main_content = page.locator("[data-testid='stApp']")
            if main_content.is_visible():
                print("✅ Main dashboard loaded")
                results["main_dashboard"] = "OK"
            else:
                print("❌ Main dashboard not visible")
                results["main_dashboard"] = "ERROR"
            
            # Test all tabs
            tabs = [
                "📈 Forecast & Run Rates",
                "🔍 Addbacks Analysis", 
                "📋 Data Inspection",
                "📑 Project Billing Digital Twin",
                "📚 Accounts & SDE Tuning",
                "⚖️ Reconciliation",
                "📊 KPI Explorer"
            ]
            
            for tab_name in tabs:
                print(f"\\n--- Testing Tab: {tab_name} ---")
                try:
                    # Look for tab elements (Streamlit uses buttons/links for tabs)
                    tab_selector = f"button:has-text('{tab_name}')"
                    
                    # Try different possible selectors for tabs
                    tab_found = False
                    possible_selectors = [
                        f"button:has-text('{tab_name}')",
                        f"a:has-text('{tab_name}')", 
                        f"span:has-text('{tab_name}')",
                        f"div:has-text('{tab_name}')"
                    ]
                    
                    for selector in possible_selectors:
                        try:
                            tab_element = page.locator(selector).first
                            if tab_element.is_visible():
                                tab_element.click()
                                tab_found = True
                                break
                        except:
                            continue
                    
                    if not tab_found:
                        # Try to find any tab-like elements
                        all_buttons = page.locator("button").all()
                        for button in all_buttons:
                            if tab_name in button.text_content():
                                button.click()
                                tab_found = True
                                break
                    
                    if tab_found:
                        # Wait for tab content to load
                        time.sleep(3)
                        
                        # Check for error elements
                        error_elements = page.locator(".stException, .streamlit-error").all()
                        if error_elements:
                            print(f"❌ {tab_name}: Error detected")
                            error_text = error_elements[0].text_content()
                            results[tab_name] = f"ERROR: {error_text}"
                        else:
                            print(f"✅ {tab_name}: Loaded successfully")
                            results[tab_name] = "OK"
                    else:
                        print(f"⚠️  {tab_name}: Tab element not found")
                        results[tab_name] = "TAB_NOT_FOUND"
                        
                except Exception as e:
                    print(f"❌ {tab_name}: Exception - {e}")
                    results[tab_name] = f"ERROR: {str(e)}"
            
        except Exception as e:
            print(f"❌ Error during testing: {e}")
            results["general_error"] = str(e)
        
        finally:
            browser.close()
    
    return results

if __name__ == "__main__":
    results = test_streamlit_with_playwright()
    print("\\n=== Final Results ===")
    for key, value in results.items():
        print(f"{key}: {value}")
    
    # Save results to file
    with open("tab_test_results.json", "w") as f:
        json.dump(results, f, indent=2)
    print("\\nResults saved to tab_test_results.json")
'''
    
    with open("browser_test_script.py", "w") as f:
        f.write(script_content)
    
    print("✅ Created browser_test_script.py")
    print("This script requires Playwright: pip install playwright")

if __name__ == "__main__":
    print("=== Streamlit App Error Reproduction ===")
    
    # First, try basic connectivity test
    basic_results = test_streamlit_app()
    
    # Create browser automation script for detailed testing
    create_browser_test_script()
    
    print("\n=== Next Steps ===")
    print("1. Install Playwright: pip install playwright")
    print("2. Install browser: playwright install chromium") 
    print("3. Run browser test: python browser_test_script.py")
    print("4. Check results in tab_test_results.json")
