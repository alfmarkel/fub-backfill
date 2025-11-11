import os
import psycopg2
import requests
import hashlib
import json
from datetime import datetime, timezone

DATABASE_URL = os.getenv("DATABASE_URL")
FUB_API_KEY = os.getenv("FUB_API_KEY")
PAGE_LIMIT = 100
STATE_FILE = "fub_pagination_state.json"

def log(msg):
    print(f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} [INFO] {msg}")

def hash_contact(contact):
    key_fields = [
        contact.get("firstName", ""),
        contact.get("lastName", ""),
        contact.get("email", ""),
        contact.get("phone", ""),
        contact.get("stage", ""),
        contact.get("source", "")
    ]
    return hashlib.sha256("|".join(map(str, key_fields)).encode()).hexdigest()

def save_state(next_link):
    with open(STATE_FILE, "w") as f:
        json.dump({"next_link": next_link}, f)

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f).get("next_link")
    return None

def bulk_upsert(conn, contacts, hashes, logs):
    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO contacts_master (
                fub_id, first_name, last_name, email, phone,
                stage, source, full_name, updated_at, last_sync
            )
            VALUES (
                %(fub_id)s, %(first_name)s, %(last_name)s, %(email)s, %(phone)s,
                %(stage)s, %(source)s, %(full_name)s, %(updated_at)s, %(last_sync)s
            )
            ON CONFLICT (fub_id) DO UPDATE
            SET first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                email = EXCLUDED.email,
                phone = EXCLUDED.phone,
                stage = EXCLUDED.stage,
                source = EXCLUDED.source,
                updated_at = EXCLUDED.updated_at;
        """, contacts)

        cur.executemany("""
            INSERT INTO contact_hashes (fub_id, full_hash, data_hash, last_update)
            VALUES (%(fub_id)s, %(full_hash)s, %(data_hash)s, %(last_update)s)
            ON CONFLICT (fub_id) DO UPDATE
            SET full_hash = EXCLUDED.full_hash,
                data_hash = EXCLUDED.data_hash,
                last_update = EXCLUDED.last_update;
        """, hashes)

        cur.executemany("""
            INSERT INTO sync_logs (fub_id, action, origin, timestamp, notes)
            VALUES (%(fub_id)s, %(action)s, %(origin)s, %(timestamp)s, %(notes)s);
        """, logs)

    conn.commit()

def fetch_all_people(api_key, limit=100, resume_from=None):
    base_url = "https://api.followupboss.com/v1/people"
    next_url = resume_from or f"{base_url}?limit={limit}"
    total = 0
    page = 0

    while next_url:
        resp = requests.get(next_url, auth=(api_key, ""))
        if resp.status_code != 200:
            raise Exception(f"FUB API Error {resp.status_code}: {resp.text}")

        data = resp.json()
        people = data.get("people", [])
        if not people:
            break

        page += 1
        total += len(people)
        first_id, last_id = people[0]["id"], people[-1]["id"]
        log(f"📦 Page {page}: first_id={first_id}, last_id={last_id}, fetched={len(people)} (total={total})")

        yield people, page

        # --- Pagination fix ---
        next_url = None
        if "nextLink" in data and data["nextLink"]:
            next_url = data["nextLink"]
        elif "next" in data and data["next"]:
            next_url = f"{base_url}?limit={limit}&next={data['next']}"
        elif "links" in data and isinstance(data["links"], dict) and data["links"].get("next"):
            next_url = data["links"]["next"]

        if next_url:
            save_state(next_url)
            log(f"➡️ Next cursor detected: {next_url}")
        else:
            save_state("COMPLETED")
            log("🏁 No nextLink found — reached end of records.")

    log(f"✅ Completed fetch. Total people fetched: {total}")

def run_backfill():
    log("🚀 Starting FUB → Render backfill")
    conn = psycopg2.connect(DATABASE_URL)
    log("🔗 Connected to database.")

    total_processed = 0
    resume_from = load_state()
    if resume_from and resume_from != "COMPLETED":
        log(f"⏩ Resuming from {resume_from}")

    try:
        for people, page in fetch_all_people(FUB_API_KEY, PAGE_LIMIT, resume_from):
            contact_batch, hash_batch, log_batch = [], [], []
            for p in people:
                fub_id = p["id"]
                first_name = p.get("firstName")
                last_name = p.get("lastName")
                email = (p.get("emails") or [{}])[0].get("value")
                phone = (p.get("phones") or [{}])[0].get("value")
                stage = p.get("stage")
                source = p.get("source")
                full_name = f"{first_name or ''} {last_name or ''}".strip()
                data_hash = hash_contact(p)
                now = datetime.now(timezone.utc)

                contact_batch.append({
                    "fub_id": fub_id, "first_name": first_name, "last_name": last_name,
                    "email": email, "phone": phone, "stage": stage, "source": source,
                    "full_name": full_name, "updated_at": now, "last_sync": now
                })
                hash_batch.append({
                    "fub_id": fub_id, "full_hash": data_hash,
                    "data_hash": data_hash, "last_update": now
                })
                log_batch.append({
                    "fub_id": fub_id, "action": "UPSERT", "origin": "fub_backfill",
                    "timestamp": now, "notes": f"Upserted page {page}"
                })

            bulk_upsert(conn, contact_batch, hash_batch, log_batch)
            total_processed += len(contact_batch)
            log(f"✅ Wrote {len(contact_batch)} contacts → total_processed={total_processed}")

    except Exception as e:
        log(f"❌ Error during backfill: {e}")
    finally:
        conn.close()
        log("🔒 Database connection closed.")
        log(f"✅ Backfill complete: total_processed={total_processed}")

if __name__ == "__main__":
    run_backfill()
