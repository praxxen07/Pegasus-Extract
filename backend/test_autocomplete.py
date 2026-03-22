#!/usr/bin/env python3
"""Diagnose autocomplete dropdown HTML after typing 'Delhi' on MagicBricks."""
import asyncio, sys
sys.stdout.reconfigure(line_buffering=True)

async def main():
    from playwright.async_api import async_playwright
    from engine.stealth_browser import launch_stealth_browser, create_stealth_context, new_stealth_page

    async with async_playwright() as p:
        browser = await launch_stealth_browser(p)
        ctx = await create_stealth_context(browser)
        page = await new_stealth_page(ctx)

        print("Loading MagicBricks...")
        await page.goto("https://www.magicbricks.com", wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(3000)

        el = await page.query_selector("#keyword")
        if not el:
            el = await page.query_selector("input[placeholder]")
        if not el:
            print("ERROR: no input found")
            await browser.close()
            return

        print("Typing Delhi...")
        await el.click()
        await page.wait_for_timeout(300)
        await el.fill("")
        await el.type("Delhi", delay=100)
        await page.wait_for_timeout(2000)

        # Find ALL visible elements containing "delhi"
        results = await page.evaluate("""() => {
            const found = [];
            const all = document.querySelectorAll('*');
            for (const el of all) {
                const text = (el.innerText || '').trim();
                if (text.length > 2 && text.length < 100 && text.toLowerCase().includes('delhi')) {
                    const rect = el.getBoundingClientRect();
                    if (rect.width > 0 && rect.height > 0) {
                        found.push({
                            tag: el.tagName.toLowerCase(),
                            id: el.id || '',
                            cls: (el.className ? String(el.className) : '').substring(0, 60),
                            role: el.getAttribute('role') || '',
                            text: text.substring(0, 60),
                            parent: el.parentElement ? el.parentElement.tagName.toLowerCase() + '.' + (el.parentElement.className ? String(el.parentElement.className) : '').substring(0, 40) : '',
                        });
                    }
                }
            }
            return found;
        }""")

        print(f"\nFound {len(results)} elements with 'delhi':")
        for i, r in enumerate(results[:30]):
            print(f"  [{i}] <{r['tag']}> cls={r['cls'][:40]!r} role={r['role']!r} text={r['text']!r}")
            print(f"       parent={r['parent'][:60]!r}")

        await browser.close()
        print("\nDONE")

asyncio.run(main())
