import os
import time
import requests
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime

# =========================
# CONFIGURATION
# =========================
DATABASE_URL = os.getenv("DATABASE_URL")
FUB_API_KEY = os.getenv("FUB_API_KEY")
if not DATABASE_URL or not FUB_API_KEY:
    raise ValueError("❌ Missing DATABASE_URL or FUB_API_KEY environment variables")

BASE_URL = "https://api.followupboss.com/v1/people?limit=100"

# =========================
# UTILITIES
# =========================
def log(msg):
    print(f"{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} [INFO] {msg}")

def connect_db():
    conn = psycopg2.connect(DATABASE_URL)
    log("🔗 Connected to database.")
    return conn

# =========================
# DB UPSERT
# =========================
def upsert_contacts(conn, contacts):
    if not contacts:
        return
    with conn.cursor() as cur:
        execute_batch(cur, """
            INSERT INTO contacts_master (fub_id, full_name, email, phone, source, stage)
            VALUES (%(id)s, %(name)s, %(email)s, %(phone)s, %(source)s, %(stage)s)
            ON CONFLICT (fub_id) DO UPDATE
            SET full_name = EXCLUDED.full_name,
                email = EXCLUDED.email,
                phone = EXCLUDED.phone,
                source = EXCLUDED.source,
                stage = EXCLUDED.stage;
        """, contacts)
        conn.commit()

# =========================
# FETCH + PAGINATION
# =========================
def fetch_all_people():
    session = requests.Session()
    session.auth = (FUB_API_KEY, "")
    session.headers.update({
        "Accept": "application/json",
        "X-System": "x-system",
    })

    all_contacts = []
    url = BASE_URL
    page = 1

    while url:
        log(f"📦 Fetching Page {page}: {url}")
        resp = session.get(url)
        if resp.status_code != 200:
            log(f"❌ Error {resp.status_code}: {resp.text}")
            break

        data = resp.json()
        people = data.get("people", [])
        if not people:
            log("⚠️ No 'people' field in response — stopping.")
            break

        meta = data.get("_metadata", {})
        next_token = meta.get("next")
        next_link = meta.get("nextLink")

        all_contacts.extend(people)
        first_id = people[0].get("id")
        last_id = people[-1].get("id")

        log(f"✅ Page {page} → fetched {len(people)} people "
            f"(first_id={first_id}, last_id={last_id})")
        log(f"🔄 next={next_token}, nextLink={next_link}")

        if not next_link:
            log("🏁 No nextLink found in _metadata — reached end of records.")
            break

        url = next_link
        page += 1
        time.sleep(0.3)  # avoid hitting FUB rate limits

    log(f"🎯 Completed pagination test: {len(all_contacts)} total records fetched.")
    return all_contacts

# =========================
# MAIN EXECUTION
# =========================
def main():
    log("🚀 Starting FUB → Render backfill")

    conn = connect_db()
    try:
        contacts = fetch_all_people()
        log(f"🧾 Preparing to write {len(contacts)} contacts to DB...")

        formatted = []
        for p in contacts:
            formatted.append({
                "id": p.get("id"),
                "name": p.get("name"),
                "email": (p.get("emails")[0]["value"] if p.get("emails") else None),
                "phone": (p.get("phones")[0]["value"] if p.get("phones") else None),
                "source": p.get("source"),
                "stage": p.get("stage"),
            })

        upsert_contacts(conn, formatted)
        log(f"✅ Wrote {len(formatted)} contacts to contacts_master.")
    finally:
        conn.close()
        log("🔒 Database connection closed.")
        log(f"✅ Backfill complete: total_processed={len(formatted)}")

# =========================
# ENTRY POINT
# =========================
if __name__ == "__main__":
    main()
