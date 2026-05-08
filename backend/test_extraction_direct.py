"""
Direct extraction pipeline test — bypasses analysis step.
Tests: Navigation → Page Load → Extraction → CSV output
"""
import asyncio
import json
import os
import uuid
from pathlib import Path

# Ensure we can import from the backend
os.chdir(os.path.dirname(os.path.abspath(__file__)))

from engine.extraction_runner import run_extraction


async def noop_progress(job_id, progress, step, records):
    print(f"  [{progress:3d}%] {step} | records={records}")


def build_plan(url, description, fields, max_pages=2):
    """Build a minimal extraction plan matching what the API would produce."""
    fields_config = {}
    for f in fields:
        fields_config[f] = {"selector": "", "attribute": "innerText"}
    return {
        "plan_id": str(uuid.uuid4()),
        "target_url": url,
        "description": description,
        "strategy": "multi_page",
        "crawler_config": {
            "seed_urls": [url],
            "depth": 0,
            "max_pages": max_pages,
        },
        "extraction_config": {
            "container_selector": "",
            "fields": fields_config,
            "pagination": {"type": "none"},
        },
        "browser_config": {
            "headless": True,
            "js_enabled": True,
            "wait_for": "domcontentloaded",
            "scroll_to_load": True,
        },
        "output_config": {
            "format": ["csv", "json"],
            "filename_prefix": "extracted_data",
        },
        "status": "ready",
    }


TESTS = [
    {
        "name": "TEST 1 — Worldometers (static data)",
        "url": "https://www.worldometers.info/world-population/population-by-country/",
        "description": (
            "Extract all countries with population, density, "
            "land area, yearly change, world share percentage"
        ),
        "fields": ["country", "population", "density", "land_area", "yearly_change", "world_share"],
        "max_pages": 1,
    },
    {
        "name": "TEST 2 — IMDB Top 250 (JS rendered)",
        "url": "https://www.imdb.com/chart/top",
        "description": (
            "Extract all 250 movies with rank, title, release year, and rating"
        ),
        "fields": ["rank", "title", "year", "rating"],
        "max_pages": 1,
    },
    {
        "name": "TEST 3 — Books to Scrape (paginated)",
        "url": "https://books.toscrape.com",
        "description": (
            "Extract all books with title, price, rating, and availability"
        ),
        "fields": ["title", "price", "rating", "availability"],
        "max_pages": 50,
    },
    {
        "name": "TEST 4 — MagicBricks Bangalore (search form)",
        "url": "https://www.magicbricks.com",
        "description": (
            "I want 2BHK and 3BHK flats for sale in Bangalore. "
            "Navigate to search, select Flat type, enter Bangalore as city, "
            "select 2BHK and 3BHK filters, click Search. "
            "Extract 15 listings with these fields: property_title, "
            "bhk_type, carpet_area_sqft, price, price_per_sqft, locality, "
            "city, transaction_type, construction_status, url. "
            "Set city = Bangalore for all rows."
        ),
        "fields": [
            "property_title", "bhk_type", "carpet_area_sqft",
            "price", "price_per_sqft", "locality", "city",
            "transaction_type", "construction_status", "url",
        ],
        "max_pages": 2,
    },
]


async def run_test(test_config: dict) -> dict:
    name = test_config["name"]
    print(f"\n{'='*60}")
    print(f"  {name}")
    print(f"{'='*60}")

    plan = build_plan(
        url=test_config["url"],
        description=test_config["description"],
        fields=test_config["fields"],
        max_pages=test_config["max_pages"],
    )

    job_id = str(uuid.uuid4())
    output_dir = str(Path("output").resolve())

    print(f"  Job: {job_id}")
    print(f"  URL: {test_config['url']}")
    print(f"  Fields: {test_config['fields']}")
    print()

    try:
        result = await run_extraction(
            job_id=job_id,
            plan=plan,
            output_dir=output_dir,
            progress_callback=noop_progress,
        )
    except Exception as e:
        print(f"\n  ❌ Exception: {e}")
        import traceback; traceback.print_exc()
        return {"name": name, "status": "FAIL", "error": str(e)}

    status = result.get("status", "unknown")
    records = result.get("records_extracted", 0)
    print(f"\n  Status: {status}")
    print(f"  Records: {records}")

    if status == "success" and records > 0:
        json_path = result.get("output_files", {}).get("json", "")
        csv_path = result.get("output_files", {}).get("csv", "")

        # Load JSON for field coverage analysis
        rows_data = []
        if json_path and os.path.exists(json_path):
            with open(json_path) as f:
                rows_data = json.load(f)

        # Field coverage
        fields = test_config["fields"]
        total_cells = len(rows_data) * len(fields) if rows_data else 0
        filled_cells = 0
        for row in rows_data:
            for fld in fields:
                val = str(row.get(fld, "")).strip()
                if val and val != "":
                    filled_cells += 1
        coverage = (filled_cells / total_cells * 100) if total_cells else 0

        print(f"  CSV file: {csv_path}")
        print(f"  Records: {len(rows_data)}")
        print(f"  Field coverage: {coverage:.1f}% ({filled_cells}/{total_cells} cells)")

        # First 3 rows with all fields
        print(f"\n  === FIRST 3 ROWS ===")
        for i, row in enumerate(rows_data[:3]):
            print(f"\n  Row {i+1}:")
            for fld in fields:
                val = str(row.get(fld, "")).strip()
                display = val[:100] if val else "(empty)"
                print(f"    {fld}: {display}")

        return {"name": name, "status": "PASS", "records": len(rows_data), "coverage": coverage}
    else:
        error = result.get("error", "No records extracted")
        print(f"  ❌ {error}")
        return {"name": name, "status": "FAIL", "error": error}


async def main():
    results = []
    for test in TESTS:
        result = await run_test(test)
        results.append(result)
        print(f"\n  → {result['name']}: {result['status']}")

        if result["status"] != "PASS":
            print(f"\n  ⚠ Test failed — continuing with remaining tests.")

    print(f"\n{'='*60}")
    print(f"  FINAL SUMMARY")
    print(f"{'='*60}")
    for r in results:
        icon = "✓" if r["status"] == "PASS" else "✗"
        if r["status"] == "PASS":
            extra = f"records={r.get('records', 0)}, coverage={r.get('coverage', 0):.1f}%"
        else:
            extra = r.get("error", "")
        print(f"  {icon} {r['name']}: {r['status']} ({extra})")


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )
    asyncio.run(main())
