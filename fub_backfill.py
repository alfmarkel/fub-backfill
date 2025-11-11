import os
import psycopg2
import requests
import hashlib
import json
from datetime import datetime, timezone

# ---------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL")
FUB_API_KEY = os.getenv("FUB_API_KEY")
PAGE_LIMIT = 100
STATE_FILE = "fub_pagination_state.json"  # remembers where it left off

# ---------------------------------------------------
# HELPERS
# ---------------------------------------------------
def log(msg):
    """Simple structured logging"""
    print(f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} [INFO] {msg}")

def hash_contact(contact):
    """Generate deterministic SHA256 hash for data integrity checks"""
    relevant_fields = [
        contact.get("firstName", ""),
        contact.get("lastName", ""),
        contact.get("email", ""),
        contact.get("phone", ""),
        contact.get("stage", ""),
        contact.get("source", "")
    ]
    data_string = "|".join(map(str, relevant_fields))
    return hashlib.sha256(data_string.encode("utf-8")).hexdigest()

def save_state(next_link):
    """Persist pagination state so backfill can resume"""
    state = {"next_link": next_link, "timestamp": datetime.now(timezone.utc).isoformat()}
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)
    log(f"💾 Saved state with next_link: {next_link}")

def load_state():
    """Restore pagination state if file exists"""
    if not os.path.exists(STATE_FILE):
        return None
    with open(STATE_FILE, "r") as f:
        state = json.load(f)
    return state.get("next_link")

# ---------------------------------------------------
# DATABASE
# ---------------------------------------------------
def bulk_upsert(conn, contacts, hashes, logs):
    """Efficient bulk UPSERT into all three tables"""
    with conn.cursor() as cur:
        # contacts_master
        cur.executemany("""
            INSERT INTO contacts_master (
                fub_id, first_name, last_name, email, phone,
                street, city, state, zip, fub_url, ghl_url,
                stage, source, last_sync, full_name, updated_at
            )
            VALUES (
                %(fub_id)s, %(first_name)s, %(last_name)s, %(email)s, %(phone)s,
                %(street)s, %(city)s, %(state)s, %(zip)s, %(fub_url)s, %(ghl_url)s,
                %(stage)s, %(source)s, %(last_sync)s, %(full_name)s, %(updated_at)s
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

        # contact_hashes
        cur.executemany("""
            INSERT INTO contact_hashes (fub_id, full_hash, partial_fub, data_hash, last_update)
            VALUES (%(fub_id)s, %(full_hash)s, %(partial_fub)s, %(data_hash)s, %(last_update)s)
            ON CONFLICT (fub_id) DO UPDATE
            SET full_hash = EXCLUDED.full_hash,
                partial_fub = EXCLUDED.partial_fub,
                data_hash = EXCLUDED.data_hash,
                last_update = EXCLUDED.last_update;
        """, hashes)

        # sync_logs
        cur.executemany("""
            INSERT INTO sync_logs (fub_id, action, origin, timestamp, notes)
            VALUES (%(fub_id)s, %(action)s, %(origin)s, %(timestamp)s, %(notes)s);
        """, logs)

    conn.commit()

# ---------------------------------------------------
# FUB API
# ---------------------------------------------------
def fetch_all_people(api_key, limit=100, resume_from=None):
    """Paginate through all people using nextLink (preferred) or next token."""
    base_url = "https://api.followupboss.com/v1/people"
    next_url = resume_from or f"{base_url}?limit={limit}"
    total_fetched = 0
    page = 0

    while next_url:
        resp = requests.get(next_url, auth=(api_key, ""))
        if resp.status_code != 200:
            raise Exception(f"Error fetching people: {resp.status_code} - {resp.text}")

        data = resp.json()
        people = data.get("people", [])
        if not people:
            break

        page += 1
        total_fetched += len(people)
        first_id, last_id = people[0]["id"], people[-1]["id"]
        log(f"📦 Page {page}: first_id={first_id}, last_id={last_id}, fetched={len(people)} (total={total_fetched})")

        yield people, page

        # Choose next URL
        next_url = data.get("nextLink") or (
            f"{base_url}?limit={limit}&next={data.get('next')}" if data.get("next") else None
        )

        save_state(next_url if next_url else "COMPLETED")

    log(f"✅ Completed fetch. Total people fetched: {total_fetched}")

# ---------------------------------------------------
# MAIN
# ---------------------------------------------------
def run_backfill():
    log("🚀 Starting FUB → Render backfill")

    conn = psycopg2.connect(DATABASE_URL)
    log("🔗 Connected to database.")

    total_processed = 0
    total_changed = 0

    try:
        resume_from = load_state()
        if resume_from:
            log(f"⏩ Resuming from saved cursor: {resume_from}")

        for people, page in fetch_all_people(FUB_API_KEY, PAGE_LIMIT, resume_from):
            contact_batch = []
            hash_batch = []
            log_batch = []

            for person in people:
                fub_id = person.get("id")
                first_name = person.get("firstName")
                last_name = person.get("lastName")
                email = person.get("emails", [{}])[0].get("value") if person.get("emails") else None
                phone = person.get("phones", [{}])[0].get("value") if person.get("phones") else None
                stage = person.get("stage")
                source = person.get("source")
                fub_url = f"https://allenmarkel.followupboss.com/2/people/view/{fub_id}"

                full_name = f"{first_name or ''} {last_name or ''}".strip()
                data_hash = hash_contact(person)
                timestamp = datetime.now(timezone.utc)

                contact_batch.append({
                    "fub_id": fub_id,
                    "first_name": first_name,
                    "last_name": last_name,
                    "email": email,
                    "phone": phone,
                    "street": None,
                    "city": None,
                    "state": None,
                    "zip": None,
                    "fub_url": fub_url,
                    "ghl_url": None,
                    "stage": stage,
                    "source": source,
                    "last_sync": timestamp,
                    "full_name": full_name,
                    "updated_at": timestamp
                })

                hash_batch.append({
                    "fub_id": fub_id,
                    "full_hash": data_hash,
                    "partial_fub": str(fub_id)[:4],
                    "data_hash": data_hash,
                    "last_update": timestamp
                })

                log_batch.append({
                    "fub_id": fub_id,
                    "action": "UPSERT",
                    "origin": "fub_backfill",
                    "timestamp": timestamp,
                    "notes": f"Upserted on page {page}"
                })

            bulk_upsert(conn, contact_batch, hash_batch, log_batch)
            total_processed += len(contact_batch)
            total_changed += len(contact_batch)
            log(f"✅ Wrote {len(contact_batch)} contacts, total_processed={total_processed}")

        log("🏁 Backfill run complete.")

    except Exception as e:
        log(f"❌ Error during backfill: {e}")
    finally:
        conn.close()
        log("🔒 Database connection closed.")
        log(f"✅ Backfill complete: total_processed={total_processed}, total_changed={total_changed}")

# ---------------------------------------------------
# ENTRY
# ---------------------------------------------------
if __name__ == "__main__":
    run_backfill()
