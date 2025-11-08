import os
import json
import hashlib
import logging
import requests
import psycopg2
from datetime import datetime
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

# -------------------------------------------------
# CONFIGURATION
# -------------------------------------------------
FUB_API_KEY = os.getenv("FUB_API_KEY", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL")

if not FUB_API_KEY:
    raise SystemExit("❌ Missing FUB_API_KEY environment variable.")

if not DATABASE_URL:
    raise SystemExit("❌ Missing DATABASE_URL environment variable.")


# -------------------------------------------------
# LOGGING
# -------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# -------------------------------------------------
# DATABASE CONNECTION
# -------------------------------------------------
def get_db_connection():
    try:
        parsed = urlparse(DATABASE_URL)
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        if "sslmode" not in query:
            query["sslmode"] = "require"
        final_url = urlunparse(
            (parsed.scheme, parsed.netloc, parsed.path, parsed.params, urlencode(query), parsed.fragment)
        )

        logging.info(f"🔗 Connecting to database host: {parsed.hostname}, database: {parsed.path.lstrip('/')}")
        return psycopg2.connect(final_url)
    except Exception as e:
        logging.critical(f"❌ Database connection failed: {e}")
        raise


# -------------------------------------------------
# DATABASE SETUP
# -------------------------------------------------
def verify_schema(conn):
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
# HELPERS
# -------------------------------------------------
def hash_contact(contact):
    """Generate stable SHA256 hash for contact record."""
    return hashlib.sha256(json.dumps(contact, sort_keys=True).encode()).hexdigest()


def bulk_upsert(conn, contacts, hashes, logs):
    """Write contacts and hashes to the database."""
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

    logging.info(f"✅ Wrote {len(contacts)} contacts, {len(logs)} logs.")


# -------------------------------------------------
# FUB API FETCHING (PAGE-BASED)
# -------------------------------------------------
PAGE_LIMIT = 100

def fetch_contacts(page):
    """Fetch one page of contacts from FUB."""
    url = "https://api.followupboss.com/v1/people"
    params = {"limit": PAGE_LIMIT, "page": page}

    resp = requests.get(url, auth=(FUB_API_KEY, ""), params=params)
    if resp.status_code != 200:
        logging.error(f"❌ FUB API Error {resp.status_code}: {resp.text}")
        return []

    data = resp.json()
    return data.get("people", [])


# -------------------------------------------------
# MAIN BACKFILL PROCESS
# -------------------------------------------------
def run_backfill():
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S")
    logging.info(f"🚀 Starting FUB → Render backfill [RUN {run_id}]")

    conn = get_db_connection()
    verify_schema(conn)

    total_processed = 0
    total_changed = 0
    page = 1

    try:
        while True:
            people = fetch_contacts(page)
            if not people:
                logging.info(f"✅ Completed — no contacts on page {page}.")
                break

            contact_batch = []
            hash_batch = []
            log_batch = []

            for p in people:
                fub_id = p.get("id")
                full_name = p.get("name")
                email = (p.get("emails") or [{}])[0].get("value")
                phone = (p.get("phones") or [{}])[0].get("value")
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

            bulk_upsert(conn, contact_batch, hash_batch, log_batch)

            first_id = people[0].get("id")
            last_id = people[-1].get("id")
            total_processed += len(people)
            total_changed += len(contact_batch)

            logging.info(
                f"[RUN {run_id}] Page {page}: first_id={first_id}, last_id={last_id}, "
                f"processed={len(people)} → total_processed={total_processed}, total_changed={total_changed}"
            )

            # stop when fewer than PAGE_LIMIT results
            if len(people) < PAGE_LIMIT:
                logging.info(f"✅ Reached last page ({page}).")
                break

            page += 1

    except Exception as e:
        logging.exception(f"❌ Error during backfill: {e}")

    finally:
        conn.close()
        logging.info("🔒 Database connection closed.")
        logging.info(f"✅ Backfill complete: total_processed={total_processed}, total_changed={total_changed}")


# -------------------------------------------------
if __name__ == "__main__":
    run_backfill()
