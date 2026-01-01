import asyncio
import json
import os
import sys
from pathlib import Path

from playwright.async_api import async_playwright


def append_debug(url: str, debug: bool) -> str:
    if not debug:
        return url
    return url if "debug=1" in url else (url + ("&" if "?" in url else "?") + "debug=1")


RAW_URL = os.getenv("APP_URL", "http://localhost:8501")
LEDGER_PATH = Path(os.getenv("LEDGER_PATH", "qb_export.csv"))


async def main():
    url_no_debug = append_debug(RAW_URL, debug=False)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        await page.goto(url_no_debug, wait_until="networkidle")

        # Upload ledger
        if not LEDGER_PATH.exists():
            print(json.dumps({"error": f"Ledger file not found: {LEDGER_PATH}"}, indent=2))
            await browser.close()
            sys.exit(1)

        await page.locator('input[type="file"]').first.set_input_files(str(LEDGER_PATH))
        await page.wait_for_timeout(8000)

        body = await page.inner_text("body")

        # Ensure debug harness is hidden
        forbidden = [
            "UAT_METRICS_START",
            "UAT_METRICS_END",
            "TAB_OK::",
            "Debug:",
        ]
        present = [x for x in forbidden if x in body]

        result = {
            "url": url_no_debug,
            "ledger_file": str(LEDGER_PATH),
            "forbidden_present": present,
        }
        print(json.dumps(result, indent=2))

        await browser.close()

        if present:
            sys.exit(1)
        sys.exit(0)


if __name__ == "__main__":
    asyncio.run(main())
