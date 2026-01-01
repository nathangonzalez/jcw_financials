import asyncio
import json
import os
import sys
from pathlib import Path

from playwright.async_api import async_playwright


_raw_url = os.getenv("APP_URL", "http://localhost:8501")
APP_URL = _raw_url if "debug=1" in _raw_url else (_raw_url + ("&" if "?" in _raw_url else "?") + "debug=1")
LEDGER_PATH = Path(os.getenv("LEDGER_PATH", "qb_export.csv"))


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # 1. Go to app
        await page.goto(APP_URL, wait_until="networkidle")

        # 2. Upload ledger file
        # Find the first file input on the page (Streamlit's uploader)
        file_input = page.locator('input[type="file"]').first
        if not LEDGER_PATH.exists():
            print(json.dumps({"error": f"Ledger file not found: {LEDGER_PATH}"}))
            await browser.close()
            sys.exit(1)

        await file_input.set_input_files(str(LEDGER_PATH))

        # 3. Wait for our UAT block to appear
        # Give Streamlit time to process the file and render the code block
        await page.wait_for_timeout(8000)  # crude but robust

        body_text = await page.inner_text("body")

        start_idx = body_text.find("UAT_METRICS_START")
        end_idx = body_text.find("UAT_METRICS_END")

        if start_idx == -1 or end_idx == -1:
            result = {
                "url": APP_URL,
                "ledger_file": str(LEDGER_PATH),
                "error": "UAT_METRICS markers not found in page text",
                "raw_excerpt": body_text[-2000:],  # last part of page for debugging
            }
            print(json.dumps(result, indent=2))
            await browser.close()
            sys.exit(1)

        # Extract the JSON substring
        # Grab from the first '{' after UAT_METRICS_START to the matching '}'
        marker_block = body_text[start_idx:end_idx]
        json_start = marker_block.find("{")
        json_end = marker_block.rfind("}") + 1

        if json_start == -1 or json_end == -1:
            result = {
                "url": APP_URL,
                "ledger_file": str(LEDGER_PATH),
                "error": "Could not locate JSON braces inside UAT block",
                "raw_block": marker_block,
            }
            print(json.dumps(result, indent=2))
            await browser.close()
            sys.exit(1)

        json_str = marker_block[json_start:json_end]

        # Optionally validate JSON
        try:
            payload = json.loads(json_str)
        except Exception as e:
            result = {
                "url": APP_URL,
                "ledger_file": str(LEDGER_PATH),
                "error": f"JSON parse error: {e}",
                "json_str": json_str,
            }
            print(json.dumps(result, indent=2))
            await browser.close()
            sys.exit(1)

        # Final UAT output
        result = {
            "url": APP_URL,
            "ledger_file": str(LEDGER_PATH),
            "uat_payload": payload,
        }
        print(json.dumps(result, indent=2))

        await browser.close()
        sys.exit(0)


if __name__ == "__main__":
    asyncio.run(main())
