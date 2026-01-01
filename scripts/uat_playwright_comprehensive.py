import asyncio
import json
from pathlib import Path

from playwright.async_api import async_playwright

APP_URL = "http://localhost:8502"
LEDGER_PATH = Path("qb_export.csv")

async def test_tab(page, tab_name, tab_selector):
    """Test a specific tab and capture any errors"""
    print(f"\n=== Testing {tab_name} ===")
    try:
        # Click the tab
        await page.click(tab_selector)
        await page.wait_for_timeout(3000)  # Wait for rendering
        
        # Check for error elements
        error_elements = await page.locator('[data-testid="stException"]').count()
        red_boxes = await page.locator('.stException').count()
        
        if error_elements > 0 or red_boxes > 0:
            print(f"❌ {tab_name}: ERROR DETECTED")
            # Try to get error details
            try:
                error_text = await page.locator('[data-testid="stException"]').inner_text()
                print(f"Error content: {error_text}")
            except:
                try:
                    error_text = await page.locator('.stException').inner_text()
                    print(f"Error content: {error_text}")
                except:
                    print("Could not extract error text")
            return "ERROR"
        else:
            print(f"✅ {tab_name}: OK")
            return "OK"
    except Exception as e:
        print(f"❌ {tab_name}: EXCEPTION - {e}")
        return f"EXCEPTION: {e}"

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # 1. Go to app
        print(f"Navigating to {APP_URL}")
        await page.goto(APP_URL, wait_until="networkidle")

        # 2. Upload ledger file
        print(f"Uploading {LEDGER_PATH}")
        file_input = page.locator('input[type="file"]').first
        if not LEDGER_PATH.exists():
            print(f"❌ Ledger file not found: {LEDGER_PATH}")
            await browser.close()
            return

        await file_input.set_input_files(str(LEDGER_PATH))

        # Wait for processing
        print("Waiting for ledger processing...")
        await page.wait_for_timeout(8000)

        # Check for processing errors
        error_elements = await page.locator('[data-testid="stException"]').count()
        if error_elements > 0:
            print("❌ Top-level processing error detected")
            try:
                error_text = await page.locator('[data-testid="stException"]').inner_text()
                print(f"Processing error: {error_text}")
            except:
                print("Could not extract processing error text")
            await browser.close()
            return

        print("✅ Ledger processed successfully")

        # 3. Test each tab
        tabs_to_test = [
            ("📈 Forecast & Run Rates", "text=📈 Forecast & Run Rates"),
            ("🔍 Addbacks Analysis", "text=🔍 Addbacks Analysis"),
            ("📋 Data Inspection", "text=📋 Data Inspection"),
            ("📑 Project Billing Digital Twin", "text=📑 Project Billing Digital Twin"),
            ("📚 Accounts & SDE Tuning", "text=📚 Accounts & SDE Tuning"),
            ("⚖️ Reconciliation", "text=⚖️ Reconciliation"),
            ("📊 KPI Explorer", "text=📊 KPI Explorer"),
        ]

        results = {}
        for tab_name, tab_selector in tabs_to_test:
            results[tab_name] = await test_tab(page, tab_name, tab_selector)

        # 4. Summary
        print("\n" + "="*50)
        print("TAB TESTING SUMMARY")
        print("="*50)
        for tab_name, status in results.items():
            status_icon = "✅" if status == "OK" else "❌"
            print(f"{status_icon} {tab_name}: {status}")

        # 5. Test special functionality for Billing tab
        print(f"\n=== Testing Billing Digital Twin Upload ===")
        try:
            await page.click("text=📑 Project Billing Digital Twin")
            await page.wait_for_timeout(2000)
            
            # Try to find file upload for green sheets
            green_sheet_upload = page.locator('input[type="file"]').nth(1)  # Second file input
            upload_count = await green_sheet_upload.count()
            
            if upload_count > 0:
                print("✅ Green sheet upload field found")
                # Note: We don't have a green sheet file to test with
            else:
                print("ℹ️  Green sheet upload field not found or not visible")
        except Exception as e:
            print(f"❌ Billing tab upload test failed: {e}")

        # 6. Test Reconciliation tab
        print(f"\n=== Testing Reconciliation Upload ===")
        try:
            await page.click("text=⚖️ Reconciliation")
            await page.wait_for_timeout(2000)
            
            # Try to find bank file upload
            bank_upload = page.locator('input[type="file"]').nth(1)  # Second file input
            upload_count = await bank_upload.count()
            
            if upload_count > 0:
                print("✅ Bank file upload field found")
            else:
                print("ℹ️  Bank file upload field not found or not visible")
        except Exception as e:
            print(f"❌ Reconciliation tab upload test failed: {e}")

        await browser.close()

        # Final result
        result = {
            "url": APP_URL,
            "ledger_file": str(LEDGER_PATH),
            "test_results": results,
            "summary": {
                "total_tabs": len(results),
                "successful": sum(1 for v in results.values() if v == "OK"),
                "failed": sum(1 for v in results.values() if v != "OK")
            }
        }
        
        print(f"\nFinal JSON Result:")
        print(json.dumps(result, indent=2))

if __name__ == "__main__":
    asyncio.run(main())
