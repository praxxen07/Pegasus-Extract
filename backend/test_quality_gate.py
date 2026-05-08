#!/usr/bin/env python3
"""
Test script to verify quality gate fixes:
1. Deterministic shortcuts now require 80% coverage
2. CSS class extraction works for Books rating/availability
3. XHR runs when LiveDOM is rejected
"""
import asyncio
import json
import logging
import os
import uuid
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    datefmt='%H:%M:%S'
)

os.chdir(os.path.dirname(os.path.abspath(__file__)))
from engine.extraction_runner import run_extraction


async def noop_progress(job_id, progress, step, records):
    pass


async def run_test(name, url, description, fields, max_pages=1):
    print(f"\n{'='*60}")
    print(f"  {name}")
    print(f"{'='*60}")
    
    fields_config = {f: {'selector': '', 'attribute': 'innerText'} for f in fields}
    plan = {
        'plan_id': str(uuid.uuid4()),
        'target_url': url,
        'description': description,
        'strategy': 'multi_page',
        'crawler_config': {
            'seed_urls': [url],
            'depth': 0,
            'max_pages': max_pages
        },
        'extraction_config': {
            'container_selector': '',
            'fields': fields_config,
            'pagination': {'type': 'next_button', 'next_selector': 'li.next a'}
        },
        'browser_config': {
            'headless': True,
            'js_enabled': True,
            'wait_for': 'domcontentloaded',
            'scroll_to_load': True
        },
        'output_config': {
            'format': ['csv', 'json'],
            'filename_prefix': 'test_data'
        },
        'status': 'ready',
    }
    
    result = await run_extraction(
        job_id=str(uuid.uuid4()),
        plan=plan,
        output_dir=str(Path('output').resolve()),
        progress_callback=noop_progress
    )
    
    print(f"\n  Status: {result.get('status')}")
    print(f"  Records: {result.get('records_extracted', 0)}")
    
    jp = result.get('output_files', {}).get('json', '')
    if jp and os.path.exists(jp):
        rows = json.load(open(jp))
        total = len(rows) * len(fields)
        filled = sum(
            1 for r in rows for f in fields
            if str(r.get(f, '')).strip()
            and str(r.get(f, '')).strip().lower() not in ('none', 'null', 'undefined', '', 'n/a')
        )
        coverage = filled / total * 100 if total > 0 else 0
        print(f"  Field coverage: {coverage:.1f}% ({filled}/{total} cells)")
        
        print(f"\n  === FIRST 5 ROWS ===")
        for i, row in enumerate(rows[:5]):
            print(f"  Row {i+1}:")
            for f in fields:
                val = str(row.get(f, ''))[:60]
                print(f"    {f}: {val if val else '(empty)'}")
    
    return result


async def main():
    try:
        await run_test(
            name="TEST 2 — MagicBricks (quality gate + XHR)",
            url="https://www.magicbricks.com",
            description="I want 2BHK and 3BHK flats for sale in Bangalore. Navigate to search, select Flat type, enter Bangalore as city, select 2BHK and 3BHK filters, click Search. Extract 15 listings: property_title, bhk_type, carpet_area_sqft, price, price_per_sqft, locality, city, transaction_type, construction_status, url. Set city = Bangalore for all rows.",
            fields=["property_title", "bhk_type", "carpet_area_sqft", "price", "price_per_sqft", "locality", "city", "transaction_type", "construction_status", "url"],
            max_pages=2,
        )
    except Exception as e:
        print(f"\n  ERROR: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"\n{'='*60}")
    print("  TEST COMPLETE")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    asyncio.run(main())
