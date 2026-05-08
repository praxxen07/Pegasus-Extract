"""
Full pipeline end-to-end test: Navigation → Extraction → CSV
Tests MagicBricks (Bangalore, Delhi) and Naukri.
"""
import json
import time
import requests

BASE = "http://localhost:8001"

TESTS = [
    {
        "name": "TEST A — MagicBricks Bangalore",
        "url": "https://www.magicbricks.com",
        "description": (
            "I want 2BHK and 3BHK flats for sale in Bangalore. "
            "Select Flat as property type, choose Bangalore as city, "
            "select 2BHK and 3BHK filters, then click Search. "
            "Extract 15 listings: property_title, bhk_type, carpet_area_sqft, "
            "price, price_per_sqft, locality, city, transaction_type, "
            "construction_status, url. City = Bangalore for all rows. "
            "Leave blank if field missing, never skip a listing."
        ),
        "schema_fields": [
            "property_title", "bhk_type", "carpet_area_sqft",
            "price", "price_per_sqft", "locality", "city",
            "transaction_type", "construction_status", "url",
        ],
        "max_pages": 2,
    },
    {
        "name": "TEST B — MagicBricks Delhi",
        "url": "https://www.magicbricks.com",
        "description": (
            "I want 2BHK and 3BHK flats for sale in Delhi. "
            "Select Flat as property type, choose Delhi as city, "
            "select 2BHK and 3BHK filters, then click Search. "
            "Extract 15 listings: property_title, bhk_type, carpet_area_sqft, "
            "price, price_per_sqft, locality, city, transaction_type, "
            "construction_status, url. City = Delhi for all rows. "
            "Leave blank if field missing, never skip a listing."
        ),
        "schema_fields": [
            "property_title", "bhk_type", "carpet_area_sqft",
            "price", "price_per_sqft", "locality", "city",
            "transaction_type", "construction_status", "url",
        ],
        "max_pages": 2,
    },
    {
        "name": "TEST C — Naukri Python Developer",
        "url": "https://www.naukri.com",
        "description": (
            "Search for Python developer jobs in Bangalore. "
            "Extract 15 job listings with these fields: job_title, "
            "company_name, experience, salary, location, posted_date, url."
        ),
        "schema_fields": [
            "job_title", "company_name", "experience",
            "salary", "location", "posted_date", "url",
        ],
        "max_pages": 2,
    },
]


def run_test(test_config: dict) -> dict:
    name = test_config["name"]
    print(f"\n{'='*60}")
    print(f"  {name}")
    print(f"{'='*60}")

    # ── Step 1: Analyze ──
    print(f"\n[1/4] Starting analysis...")
    r = requests.post(f"{BASE}/analyze", json={
        "url": test_config["url"],
        "description": test_config["description"],
        "schema_fields": test_config["schema_fields"],
        "max_pages": test_config["max_pages"],
    })
    if r.status_code != 202:
        print(f"  ❌ Analysis failed to start: {r.status_code} {r.text[:200]}")
        return {"name": name, "status": "FAIL", "error": f"Analyze start failed: {r.status_code}"}

    job_id = r.json()["job_id"]
    print(f"  Analysis job: {job_id}")

    # ── Step 2: Poll analysis ──
    print(f"[2/4] Polling analysis...")
    for i in range(120):  # 4 min max
        time.sleep(2)
        r = requests.get(f"{BASE}/analyze/{job_id}")
        data = r.json()
        status = data.get("status", "unknown")
        if status == "success":
            plan = data.get("extraction_plan", {})
            print(f"  ✓ Analysis complete. Strategy: {plan.get('strategy', '?')}")
            print(f"  Provider: {data.get('provider_used', '?')}")
            print(f"  Fields in plan: {list(plan.get('extraction_config', {}).get('fields', {}).keys())[:5]}...")
            break
        elif status == "error":
            print(f"  ❌ Analysis error: {data.get('error', '?')}")
            return {"name": name, "status": "FAIL", "error": data.get("error")}
        if i % 10 == 0 and i > 0:
            print(f"  ... still analyzing ({i*2}s)")
    else:
        print(f"  ❌ Analysis timed out after 240s")
        return {"name": name, "status": "FAIL", "error": "Analysis timeout"}

    # ── Step 3: Start extraction ──
    print(f"[3/4] Starting extraction...")
    r = requests.post(f"{BASE}/extract", json={
        "job_id": job_id,
        "confirm": True,
    })
    if r.status_code != 202:
        print(f"  ❌ Extraction failed to start: {r.status_code} {r.text[:200]}")
        return {"name": name, "status": "FAIL", "error": f"Extract start failed: {r.status_code}"}

    ext_id = r.json()["extraction_job_id"]
    print(f"  Extraction job: {ext_id}")

    # ── Step 4: Poll extraction ──
    print(f"[4/4] Polling extraction...")
    for i in range(180):  # 6 min max
        time.sleep(2)
        r = requests.get(f"{BASE}/extract/{ext_id}")
        data = r.json()
        status = data.get("status", "unknown")
        step = data.get("current_step", "")
        records = data.get("records_extracted", 0)
        progress = data.get("progress", 0)

        if i % 5 == 0:
            print(f"  [{progress}%] {step} | records: {records}")

        if status == "success":
            print(f"\n  ✓ Extraction complete!")
            print(f"  Records extracted: {records}")
            # Try downloading CSV
            try:
                csv_r = requests.get(f"{BASE}/extract/{ext_id}/download/csv")
                if csv_r.status_code == 200:
                    csv_text = csv_r.text
                    lines = csv_text.strip().split("\n")
                    print(f"  CSV rows (incl header): {len(lines)}")
                    print(f"\n  === FIRST 4 ROWS ===")
                    for line in lines[:4]:
                        print(f"  {line[:150]}")
                else:
                    print(f"  CSV download status: {csv_r.status_code}")
                    csv_text = ""
            except Exception as e:
                print(f"  CSV download error: {e}")
                csv_text = ""

            # Debug info
            try:
                dbg = requests.get(f"{BASE}/extract/{ext_id}/debug").json()
                print(f"\n  === DEBUG ===")
                print(f"  Files: {json.dumps(dbg.get('output_files', {}), indent=2)}")
            except Exception:
                pass

            return {
                "name": name,
                "status": "PASS",
                "records": records,
                "csv_rows": len(csv_text.strip().split("\n")) if csv_text else 0,
            }

        elif status == "failed":
            error = data.get("error", "unknown")
            print(f"\n  ❌ Extraction failed: {error}")
            return {"name": name, "status": "FAIL", "error": error}

    print(f"  ❌ Extraction timed out after 360s")
    return {"name": name, "status": "FAIL", "error": "Extraction timeout"}


if __name__ == "__main__":
    results = []
    for test in TESTS:
        result = run_test(test)
        results.append(result)
        print(f"\n  → {result['name']}: {result['status']}")

        # Stop running further tests if current test fails
        if result["status"] != "PASS":
            print(f"\n  ⚠ Stopping — {result['name']} failed. Fix before proceeding.")
            break

    print(f"\n{'='*60}")
    print(f"  FINAL SUMMARY")
    print(f"{'='*60}")
    for r in results:
        status_icon = "✓" if r["status"] == "PASS" else "✗"
        extra = f"records={r.get('records', 0)}" if r["status"] == "PASS" else r.get("error", "")
        print(f"  {status_icon} {r['name']}: {r['status']} ({extra})")
