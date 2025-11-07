import os
import requests
import psycopg2
import hashlib
import json
import logging
from datetime import datetime

# -------------------------------------------------
# CONFIG
# -------------------------------------------------
FUB_API_KEY = os.getenv("FUB_API_KEY")
DB_CONN = os.getenv("DB_CONN")
PAGE_LIMIT = 100

# -------------------------------------------------
# LOGGING SETUP
# -------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# -------------------------------------------------
# HELPERS
# -------------------------------------------------
def get_db_connection():
    """Connect to Postgres database."""
    return psycopg2.connect(DB_CONN)

def hash_contact(contact):
    """Generate a hash for change detection."""
    return hashlib.sha256(json.dumps(contact, sort_keys=True).encode()).hexdigest()

def verify_schema(conn):
    """Ensure required tables exist."""
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS contacts_master (
            fub_id BIGINT PRIMARY KEY,
            full_name TEXT,
            email TEXT,
            phone TEXT,
            source TEXT,
            stage TEXT,
            updated_at TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS contact_hashes (
            fub_id BIGINT PRIMARY KEY,
            data_hash TEXT
        );
        CREATE TABLE IF NOT EXISTS sync_logs (
            id SERIAL PRIMARY KEY,
            fub_id BIGINT,
            action TEXT,
            log_time TIMESTAMP DEFAULT NOW()
        );
        """)
        conn.commit()
    logging.info("✅ Verified schema exists.")

# -------------------------------------------------
# FUB API FETCH
# -------------------------------------------------
def fetch_contacts(cursor=None):
    """Fetch one page of contacts from FUB API using cursor pagination."""
    url = "https://api.followupboss.com/v1/people"
    params = {"limit": PAGE_LIMIT}
    if cursor:
        params["cursor"] = cursor

    logging.info(f"📡 Fetching page from {url} (cursor={cursor})")
    resp = requests.get(url, auth=(FUB_API_KEY, ""), params=params)

    if resp.status_code != 200:
        logging.error(f"❌ FUB API Error {resp.status_code}: {resp.text}")
        return None, None

    data = resp.json()
    people = data.get("people", [])
    next_cursor = data.get("nextCursor")

    logging.info(f"📥 Fetched {len(people)} contacts (next_cursor={next_cursor})")
    return people, next_cursor

# -------------------------------------------------
# BULK UPSERT
# -------------------------------------------------
def bulk_upsert(conn, contacts, hashes, logs):
    """Insert or update contacts, hashes, and logs in one batch."""
    if not contacts:
        return

    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO contacts_master (fub_id, full_name, email, phone, source, stage, updated_at)
            VALUES (%(fub_id)s, %(full_name)s, %(email)s, %(phone)s, %(source)s, %(stage)s, %(updated_at)s)
            ON CONFLICT (fub_id) DO UPDATE SET
                full_name = EXCLUDED.full_name,
                email = EXCLUDED.email,
                phone = EXCLUDED.phone,
                source = EXCLUDED.source,
                stage = EXCLUDED.stage,
                updated_at = EXCLUDED.updated_at;
        """, contacts)

        cur.executemany("""
            INSERT INTO contact_hashes (fub_id, data_hash)
            VALUES (%(fub_id)s, %(data_hash)s)
            ON CONFLICT (fub_id) DO UPDATE SET
                data_hash = EXCLUDED.data_hash;
        """, hashes)

        cur.executemany("""
            INSERT INTO sync_logs (fub_id, action)
            VALUES (%(fub_id)s, %(action)s);
        """, logs)

        conn.commit()

    logging.info(f"✅ Wrote {len(contacts)} contacts, {len(hashes)} hashes, {len(logs)} logs.")

# -------------------------------------------------
# MAIN RUNNER
# -------------------------------------------------
def run_backfill():
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S")
    logging.info(f"🚀 Starting FUB → Render backfill [RUN {run_id}]")

    conn = get_db_connection()
    verify_schema(conn)

    total_processed = 0
    total_changed = 0
    cursor = None
    page = 0

    try:
        while True:
            page += 1
            people, next_cursor = fetch_contacts(cursor)
            if not people:
                logging.warning("⚠️ No contacts returned from FUB API.")
                break

            contact_batch = []
            hash_batch = []
            log_batch = []

            for p in people:
                fub_id = p.get("id")
                full_name = p.get("name")
                email = p.get("emails", [{}])[0].get("value") if p.get("emails") else None
                phone = p.get("phones", [{}])[0].get("value") if p.get("phones") else None
                source = p.get("source")
                stage = p.get("stage")
                updated_at = p.get("updated")

                h = hash_contact(p)
                contact_batch.append({
                    "fub_id": fub_id,
                    "full_name": full_name,
                    "email": email,
                    "phone": phone,
                    "source": source,
                    "stage": stage,
                    "updated_at": updated_at
                })
                hash_batch.append({"fub_id": fub_id, "data_hash": h})
                log_batch.append({"fub_id": fub_id, "action": "UPSERT"})

            # Perform the DB upsert
            bulk_upsert(conn, contact_batch, hash_batch, log_batch)

            # Spot-check first and last FUB IDs
            first_id = people[0].get("id") if people else None
            last_id = people[-1].get("id") if people else None

            total_processed += len(people)
            total_changed += len(contact_batch)

            logging.info(
                f"[RUN {run_id}] Page {page}: first_id={first_id}, last_id={last_id}, "
                f"processed={len(people)} → total_processed={total_processed}, total_changed={total_changed}"
            )

            if not next_cursor:
                logging.info(f"[RUN {run_id}] ✅ Reached end (no next cursor).")
                break

            cursor = next_cursor

    except Exception as e:
        logging.exception(f"❌ Error during backfill: {e}")

    finally:
        conn.close()
        logging.info(f"🔒 Database connection closed.")
        logging.info(f"✅ Backfill complete: total_processed={total_processed}, total_changed={total_changed}")

# -------------------------------------------------
# ENTRY POINT
# -------------------------------------------------
if __name__ == "__main__":
    run_backfill()
