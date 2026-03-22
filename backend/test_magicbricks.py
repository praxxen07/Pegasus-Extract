#!/usr/bin/env python3
"""TEST 3 — MagicBricks homepage via API, with unbuffered output."""
import requests, time, sys, csv, io

BASE = "http://localhost:8001"
p = lambda *a, **k: print(*a, **k, flush=True)

# Step 1: Analyze
p("=== STEP 1: ANALYZE ===")
r = requests.post(f"{BASE}/analyze", json={
    "url": "https://www.magicbricks.com",
    "description": (
        "Extract 50 property listings — 2BHK and 3BHK flats for sale in Delhi. "
        "Fields: property_title, bhk_type, carpet_area_sqft, price, price_per_sqft, "
        "locality, city, transaction_type, construction_status, url. "
        "City = Delhi for all rows. Leave blank if field missing, never skip a listing."
    ),
    "schema_fields": [
        "property_title", "bhk_type", "carpet_area_sqft", "price",
        "price_per_sqft", "locality", "city", "transaction_type",
        "construction_status", "url"
    ],
    "max_pages": 3,
})
job_id = r.json().get("job_id")
p(f"job_id: {job_id}")

for i in range(60):
    time.sleep(5)
    s = requests.get(f"{BASE}/analyze/{job_id}").json()
    status = s.get("status")
    p(f"  analyze poll {i+1}: {status}")
    if status in ("success", "failed"):
        break

# Step 2: Extract
p("\n=== STEP 2: EXTRACT ===")
r2 = requests.post(f"{BASE}/extract", json={"job_id": job_id, "confirm": True})
ext_id = r2.json().get("extraction_job_id")
p(f"extraction_job_id: {ext_id}")

for i in range(120):
    time.sleep(5)
    s = requests.get(f"{BASE}/extract/{ext_id}").json()
    status = s.get("status")
    records = s.get("records_extracted")
    step = s.get("current_step")
    p(f"  extract poll {i+1}: status={status} records={records} step={step}")
    if status in ("success", "failed"):
        p(f"\nFINAL: status={status} records={records}")
        csv_path = s.get("output_files", {}).get("csv", "")
        if csv_path:
            p(f"\n=== FIRST 5 ROWS ===")
            with open(csv_path) as f:
                for j, line in enumerate(f):
                    if j <= 5:
                        p(line.rstrip())
        break
