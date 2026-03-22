#!/usr/bin/env python3
"""TEST 3: MagicBricks with AgentNavigator."""
import requests, time, sys
sys.stdout.reconfigure(line_buffering=True)
BASE = "http://localhost:8001"

print("=== TEST 3: MagicBricks (AgentNavigator) ===")
r = requests.post(f"{BASE}/analyze", json={
    "url": "https://www.magicbricks.com",
    "description": "I want 2BHK and 3BHK flats for sale in Delhi. Select Flat as property type, choose Delhi as city, select 2BHK and 3BHK filters, then click Search. Extract 50 listings: property_title, bhk_type, carpet_area_sqft, price, price_per_sqft, locality, city, transaction_type, construction_status, url. City = Delhi for all rows. Leave blank if field missing, never skip a listing.",
    "schema_fields": ["property_title","bhk_type","carpet_area_sqft","price","price_per_sqft","locality","city","transaction_type","construction_status","url"],
    "max_pages": 3,
})
job_id = r.json().get("job_id")
print(f"analyze job: {job_id}")

for i in range(30):
    time.sleep(5)
    s = requests.get(f"{BASE}/analyze/{job_id}").json()
    if s.get("status") in ("success", "failed"):
        print(f"analyze: {s.get('status')}")
        break

r2 = requests.post(f"{BASE}/extract", json={"job_id": job_id, "confirm": True})
ext_id = r2.json().get("extraction_job_id")
print(f"extract job: {ext_id}")

for i in range(180):
    time.sleep(5)
    s = requests.get(f"{BASE}/extract/{ext_id}").json()
    status = s.get("status")
    records = s.get("records_extracted", 0)
    step = s.get("current_step", "")
    print(f"  poll {i+1}: {status} rec={records} step={step}")
    if status in ("success", "failed"):
        break

print(f"\nRESULT: status={status} records={records}")
csv_path = (s.get("output_files") or {}).get("csv", "")
if csv_path:
    print("FIRST 5 ROWS:")
    with open(csv_path) as f:
        for j, line in enumerate(f):
            if j <= 5:
                print(f"  {line.rstrip()}")
