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
            
            # Set up sidebar parameters - be more flexible with selectors
            print("Setting up sidebar parameters...")
            
            # Wait for sidebar to be visible
            page.wait_for_selector("[data-testid='stSidebar']", timeout=10000)
            
            # Find all input fields in sidebar and set them one by one
            inputs = page.locator("[data-testid='stSidebar'] input").all()
            print(f"Found {len(inputs)} input fields in sidebar")
            
            if len(inputs) >= 3:
                # Set Owner Revenue Start (first input)
                inputs[0].fill("2025-08-01")
                print("✅ Set Owner Revenue Start")
                
                # Set Year 1 End (second input)
                inputs[1].fill("2026-06-30")
                print("✅ Set Year 1 End")
                
                # Set Current Report Date (third input)
                inputs[2].fill("2025-12-07")
                print("✅ Set Current Report Date")
            
            # Upload qb_export.csv
            print("Uploading qb_export.csv...")
            file_inputs = page.locator("input[type='file']").all()
            if file_inputs:
                # Use the first file input (should be the QB ledger upload)
                file_inputs[0].set_input_files("qb_export.csv")
                print("✅ File uploaded")
            else:
                print("❌ No file input found")
            
            # Wait for upload to process and data to load
            time.sleep(8)
            print("✅ Waiting for data processing...")
            
            # Test main dashboard
            print("Testing main dashboard...")
            main_content = page.locator("[data-testid='stApp']")
            if main_content.is_visible():
                print("✅ Main dashboard loaded")
                results["main_dashboard"] = "OK"
            else:
                print("❌ Main dashboard not visible")
                results["main_dashboard"] = "ERROR"
            
            # Test all tabs - look for tab buttons/links more broadly
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
                print(f"\n--- Testing Tab: {tab_name} ---")
                try:
                    # Look for tab elements - try multiple approaches
                    tab_found = False
                    
                    # Method 1: Look for buttons with tab text
                    tab_buttons = page.locator("button").all()
                    for button in tab_buttons:
                        if tab_name in button.text_content():
                            print(f"Found tab button: {button.text_content()}")
                            button.click()
                            tab_found = True
                            break
                    
                    # Method 2: Look for links with tab text
                    if not tab_found:
                        tab_links = page.locator("a").all()
                        for link in tab_links:
                            if tab_name in link.text_content():
                                print(f"Found tab link: {link.text_content()}")
                                link.click()
                                tab_found = True
                                break
                    
                    # Method 3: Look for spans/divs with tab text
                    if not tab_found:
                        tab_elements = page.locator("span, div").all()
                        for elem in tab_elements:
                            if tab_name in elem.text_content():
                                print(f"Found tab element: {elem.text_content()}")
                                elem.click()
                                tab_found = True
                                break
                    
                    if tab_found:
                        # Wait for tab content to load
                        time.sleep(5)
                        
                        # Check for error elements
                        error_elements = page.locator(".stException, .streamlit-error, [data-testid='stException']").all()
                        if error_elements:
                            print(f"❌ {tab_name}: Error detected")
                            error_text = error_elements[0].text_content()
                            results[tab_name] = f"ERROR: {error_text[:200]}..."  # Truncate long errors
                            print(f"Error preview: {error_text[:200]}...")
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
    print("\n=== Final Results ===")
    for key, value in results.items():
        print(f"{key}: {value}")
    
    # Save results to file
    with open("tab_test_results.json", "w") as f:
        json.dump(results, f, indent=2)
    print("\nResults saved to tab_test_results.json")
