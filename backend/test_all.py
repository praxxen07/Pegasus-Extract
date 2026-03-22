#!/usr/bin/env python3
"""Run all 3 tests sequentially with unbuffered output."""
import requests, time, sys

BASE = "http://localhost:8001"
p = lambda *a, **k: print(*a, **k, flush=True)


def run_test(name, url, description, schema_fields, max_pages, expected):
    p(f"\n{'='*60}")
    p(f"  {name}")
    p(f"{'='*60}")

    # Analyze
    r = requests.post(f"{BASE}/analyze", json={
        "url": url,
        "description": description,
        "schema_fields": schema_fields,
        "max_pages": max_pages,
    })
    job_id = r.json().get("job_id")
    p(f"  analyze job: {job_id}")

    for i in range(60):
        time.sleep(5)
        s = requests.get(f"{BASE}/analyze/{job_id}").json()
        if s.get("status") in ("success", "failed"):
            p(f"  analyze: {s.get('status')}")
            break

    # Extract
    r2 = requests.post(f"{BASE}/extract", json={"job_id": job_id, "confirm": True})
    ext_id = r2.json().get("extraction_job_id")
    p(f"  extract job: {ext_id}")

    for i in range(180):
        time.sleep(5)
        s = requests.get(f"{BASE}/extract/{ext_id}").json()
        status = s.get("status")
        records = s.get("records_extracted", 0)
        step = s.get("current_step", "")
        p(f"  poll {i+1}: status={status} records={records} step={step}")
        if status in ("success", "failed"):
            break

    p(f"\n  RESULT: status={status} records={records}")
    p(f"  expected: {expected}")

    # Print first 5 rows if CSV exists
    csv_path = (s.get("output_files") or {}).get("csv", "")
    if csv_path:
        p(f"\n  FIRST 5 ROWS:")
        try:
            with open(csv_path) as f:
                for j, line in enumerate(f):
                    if j <= 5:
                        p(f"    {line.rstrip()}")
        except Exception as e:
            p(f"  Could not read CSV: {e}")
    p("")
    return status, records


# ── TEST 1: Worldometers ──
s1, r1 = run_test(
    "TEST 1 — Worldometers Regression",
    "https://www.worldometers.info/world-population/population-by-country/",
    "Extract all countries with population, density, land area, world share percentage",
    ["country", "population", "yearly_change", "net_change", "density",
     "land_area", "migrants", "fert_rate", "med_age", "urban_pop", "world_share"],
    1,
    "234 rows, Tier 1 deterministic, no AgentNavigator"
)

# ── TEST 2: IMDB ──
s2, r2 = run_test(
    "TEST 2 — IMDB Top 250 Regression",
    "https://www.imdb.com/chart/top",
    "Extract all 250 movies — rank, title, year, rating",
    ["rank", "title", "release_year", "rating"],
    1,
    "250 rows, Tier 1 or Tier 2, no AgentNavigator"
)

# ── TEST 3: MagicBricks ──
s3, r3 = run_test(
    "TEST 3 — MagicBricks (AgentNavigator)",
    "https://www.magicbricks.com",
    "I want 2BHK and 3BHK flats for sale in Delhi. Select Flat as property type, "
    "choose Delhi as city, select 2BHK and 3BHK filters, then click Search. "
    "Extract 50 listings: property_title, bhk_type, carpet_area_sqft, price, "
    "price_per_sqft, locality, city, transaction_type, construction_status, url. "
    "City = Delhi for all rows. Leave blank if field missing, never skip a listing.",
    ["property_title", "bhk_type", "carpet_area_sqft", "price", "price_per_sqft",
     "locality", "city", "transaction_type", "construction_status", "url"],
    3,
    "AgentNavigator triggers, navigates search form, 50 listings"
)

p(f"\n{'='*60}")
p(f"  FINAL SUMMARY")
p(f"{'='*60}")
p(f"  TEST 1 (Worldometers): status={s1} records={r1}")
p(f"  TEST 2 (IMDB):         status={s2} records={r2}")
p(f"  TEST 3 (MagicBricks):  status={s3} records={r3}")
