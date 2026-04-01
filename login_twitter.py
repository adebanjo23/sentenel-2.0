"""
Manual Twitter login helper.

Opens a visible Chromium browser → you login manually → cookies get saved.
Future API requests will reuse these cookies and skip login.

Usage:
    python login_twitter.py
"""

import asyncio
import json
from pathlib import Path
from playwright.async_api import async_playwright

COOKIES_FILE = "./data/twitter/cookies/playwright_cookies.json"


async def main():
    print("Opening browser — please login to Twitter manually...")
    print("Once you're on the home feed, press Enter here to save cookies.\n")

    pw = await async_playwright().start()
    browser = await pw.chromium.launch(headless=False)
    context = await browser.new_context(
        viewport={"width": 1280, "height": 900},
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
    )
    page = await context.new_page()
    await page.goto("https://twitter.com/i/flow/login")

    input("\n>>> Login in the browser, then press Enter here to save cookies... ")

    cookies = await context.cookies()
    Path(COOKIES_FILE).parent.mkdir(parents=True, exist_ok=True)
    Path(COOKIES_FILE).write_text(json.dumps(cookies, indent=2))
    print(f"\nSaved {len(cookies)} cookies to {COOKIES_FILE}")
    print("You can now use the Twitter API endpoints — they'll skip login.")

    await browser.close()
    await pw.stop()


if __name__ == "__main__":
    asyncio.run(main())
