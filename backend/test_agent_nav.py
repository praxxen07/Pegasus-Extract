#!/usr/bin/env python3
"""Direct AgentNavigator test — bypasses analyze/extract to save all AI tokens for navigation."""
import asyncio, sys
sys.stdout.reconfigure(line_buffering=True)

DESCRIPTION = (
    "I want 2BHK and 3BHK flats for sale in Delhi. "
    "Select Flat as property type, choose Delhi as city, "
    "select 2BHK and 3BHK filters, then click Search."
)

async def main():
    from playwright.async_api import async_playwright
    from engine.agent_navigator import AgentNavigator
    from engine.stealth_browser import launch_stealth_browser, create_stealth_context, new_stealth_page

    nav = AgentNavigator()

    async with async_playwright() as p:
        browser = await launch_stealth_browser(p)
        ctx = await create_stealth_context(browser)
        page = await new_stealth_page(ctx)

        url = "https://www.magicbricks.com"
        print(f"Loading {url} ...")
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)

        # Step 1: Verify snapshot + heuristic
        snapshot = await nav._get_dom_snapshot(page)
        body = snapshot.get("bodyTextLength", 0)
        elems = snapshot.get("interactiveElements", [])
        tables = snapshot.get("tableDataRows", 0)
        is_form = nav.is_search_form_page(snapshot, records_found=0)

        print(f"\n  bodyTextLength: {body}")
        print(f"  interactive elements: {len(elems)}")
        print(f"  tableDataRows: {tables}")
        print(f"  is_search_form_page: {is_form}")

        for i, el in enumerate(elems[:10]):
            print(f"    [{i}] {el.get('tag')} id={el.get('id')!r} "
                  f"placeholder={el.get('placeholder')!r} "
                  f"text={el.get('text')!r}")

        if not is_form:
            print("\n  NOT a search form — aborting")
            await browser.close()
            return

        # Step 2: Run AgentNavigator
        print(f"\n{'='*50}")
        print("  Starting AgentNavigator...")
        print(f"{'='*50}")

        result = await nav.navigate_to_results(
            start_url=url,
            client_description=DESCRIPTION,
            page=page,
        )

        print(f"\n  success: {result['success']}")
        print(f"  message: {result.get('message', '')}")
        print(f"  results_url: {result.get('results_url', '')}")
        print(f"  steps: {result.get('steps_taken', 0)}")

        if result["success"]:
            final_page = result["page"]
            print(f"  final URL: {final_page.url}")
            title = await final_page.title()
            print(f"  final title: {title}")

        await browser.close()

    print(f"\n{'='*50}")
    print(f"  RESULT: {'PASS' if result['success'] else 'FAIL'}")
    print(f"{'='*50}")

asyncio.run(main())
