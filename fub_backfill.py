import os
import requests
import json
from datetime import datetime

FUB_API_KEY = os.getenv("FUB_API_KEY")
if not FUB_API_KEY:
    raise ValueError("❌ Missing FUB_API_KEY environment variable")

BASE_URL = "https://api.followupboss.com/v1/people?limit=5"

def log(msg):
    print(f"{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} [INFO] {msg}")

def main():
    log("🚀 Starting FUB pagination debug test...")
    session = requests.Session()
    session.auth = (FUB_API_KEY, "")
    session.headers.update({
        "Accept": "application/json",
        "X-System": "x-system",
    })

    url = BASE_URL
    resp = session.get(url)
    print(f"Status: {resp.status_code}")
    if resp.status_code != 200:
        print(resp.text)
        return

    data = resp.json()
    print("\n=== RAW JSON KEYS ===")
    print(list(data.keys()))

    # Pretty print a sample of the JSON for inspection
    print("\n=== FULL JSON (first 20 lines) ===")
    print(json.dumps(data, indent=2)[:2000])  # only first 2000 chars to avoid overflow

    # Check for possible pagination fields
    next_candidates = [
        data.get("nextLink"),
        data.get("next"),
        data.get("links", {}).get("next"),
        data.get("pagination", {}).get("next"),
    ]
    print("\n=== Pagination Fields Found ===")
    for c in next_candidates:
        print("→", c)

    people = data.get("people", [])
    print(f"\nFetched {len(people)} people.")
    if people:
        first_id = people[0].get("id")
        last_id = people[-1].get("id")
        print(f"first_id={first_id}, last_id={last_id}")

    log("🎯 Completed pagination debug test.")

if __name__ == "__main__":
    main()
