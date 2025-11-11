import requests
import os
import time

FUB_API_KEY = os.getenv("FUB_API_KEY")
if not FUB_API_KEY:
    raise ValueError("❌ Missing FUB_API_KEY environment variable")

BASE_URL = "https://api.followupboss.com/v1/people?limit=100"

def fetch_all_people():
    total_processed = 0
    page = 1
    next_link = BASE_URL

    print(f"🚀 Starting FUB pagination test...")
    session = requests.Session()
    session.auth = (FUB_API_KEY, "")
    session.headers.update({"Accept": "application/json"})

    while next_link:
        print(f"\n📦 Fetching Page {page}: {next_link}")
        resp = session.get(next_link)
        if resp.status_code != 200:
            print(f"❌ Error {resp.status_code}: {resp.text}")
            break

        data = resp.json()
        people = data.get("people", [])
        count = len(people)
        total_processed += count

        if count == 0:
            print("⚠️ No people found, stopping.")
            break

        first_id = people[0].get("id")
        last_id = people[-1].get("id")
        print(f"✅ Page {page} → fetched {count} people (first_id={first_id}, last_id={last_id})")

        # Get the next link for pagination
        next_link = data.get("nextLink")
        if not next_link:
            print("🏁 No nextLink found — reached end of records.")
            break

        page += 1
        time.sleep(0.2)  # small delay for safety

    print(f"\n🎯 Completed pagination test: {total_processed} total records fetched.")

if __name__ == "__main__":
    fetch_all_people()


