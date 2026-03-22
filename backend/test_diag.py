#!/usr/bin/env python3
"""Diagnostic: test is_search_form_page on two sites to prove generality."""
import asyncio, sys, json
sys.stdout.reconfigure(line_buffering=True)

SITES = [
    ("MagicBricks (search form)", "https://www.magicbricks.com", True),
    ("Naukri (search form)", "https://www.naukri.com", True),
    ("Worldometers (data table)", "https://www.worldometers.info/world-population/population-by-country/", False),
]

async def main():
    from playwright.async_api import async_playwright
    from engine.agent_navigator import AgentNavigator
    from engine.stealth_browser import launch_stealth_browser, create_stealth_context, new_stealth_page

    nav = AgentNavigator()
    all_pass = True

    async with async_playwright() as p:
        browser = await launch_stealth_browser(p)
        ctx = await create_stealth_context(browser)

        for name, url, expected in SITES:
            print(f"\n{'='*50}")
            print(f"  {name}")
            print(f"{'='*50}")

            page = await new_stealth_page(ctx)
            print(f"Loading {url} ...")
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)

            snapshot = await nav._get_dom_snapshot(page)
            body = snapshot.get("bodyTextLength", 0)
            elements = snapshot.get("interactiveElements", [])
            tables = snapshot.get("tableDataRows", 0)
            meta = snapshot.get("_meta", {})

            print(f"  bodyTextLength: {body}")
            print(f"  interactive elements: {len(elements)}")
            print(f"  tableDataRows: {tables}")
            print(f"  snapshotPassesTaken: {meta.get('snapshotPassesTaken')}")
            print(f"  overlayDismissed: {meta.get('overlayDismissed')}")

            # Show first 5 interactive elements
            for i, el in enumerate(elements[:5]):
                print(f"    [{i}] {el.get('tag')} id={el.get('id')} "
                      f"placeholder={el.get('placeholder')!r} "
                      f"text={el.get('text')!r}")

            result = nav.is_search_form_page(snapshot, records_found=0)
            status = "PASS" if result == expected else "FAIL"
            if result != expected:
                all_pass = False

            print(f"\n  is_search_form_page: {result}  (expected {expected})  → {status}")
            await page.close()

        await browser.close()

    print(f"\n{'='*50}")
    print(f"  ALL TESTS: {'PASS' if all_pass else 'FAIL'}")
    print(f"{'='*50}")

asyncio.run(main())
