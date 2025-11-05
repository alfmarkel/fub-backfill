# fub_backfill.py
import os
import sys
import json
import time
import base64
import logging
import hashlib
import requests
import psycopg2
from datetime import datetime, timezone
from psycopg2.extras import execute_batch

# ------------------------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------------------------

FUB_API_KEY = os.getenv("FUB_API_KEY") or os.getenv("FUB_KEY") or os.getenv("FUB_TOKEN")
FUB_BASE_URL = os.getenv("FUB_BASE_URL", "https://api.followupboss.com")
DB_URL = os.getenv("DATABASE_URL") or os.getenv("DB_URL")

if not FUB_API_KEY:
    print("❌ Missing FUB_API_KEY in environment!", file=sys.stderr)
    sys.exit(1)
if not DB_URL:
    print("❌ Missing DATABASE_URL in environment!", file=sys.stderr)
    sys.exit(1)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# ------------------------------------------------------------------------------
# DATABASE
# ------------------------------------------------------------------------------

def get_db_connection():
    conn = psycopg2.connect(DB_URL, sslmode="require")
    conn.autocommit = False
    return conn


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS contacts_master (
                fub_id BIGINT PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                email TEXT,
                phone TEXT,
                street TEXT,
                city TEXT,
                state TEXT,
                zip TEXT,
                stage TEXT,
                source TEXT,
                fub_url TEXT,
                ghl_url TEXT,
                last_sync TIMESTAMP
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS contact_hashes (
                fub_id BIGINT PRIMARY KEY,
                full_hash TEXT,
                partial_fub TEXT,
                last_update TIMESTAMP
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sync_logs (
                id BIGSERIAL PRIMARY KEY,
                fub_id BIGINT,
                action TEXT,
                origin TEXT,
                timestamp TIMESTAMP,
                notes TEXT
            );
        """)
    conn.commit()
    logging.info("✅ Verified schema exists.")


# ------------------------------------------------------------------------------
# FOLLOW UP BOSS API (LEGACY PAGINATION)
# ------------------------------------------------------------------------------

def fub_headers() -> dict:
    token = base64.b64encode(f"{FUB_API_KEY}:".encode("utf-8")).decode("ascii")
    return {
        "Authorization": f"Basic {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def fetch_all_contacts(limit: int = 100) -> list:
    """
    Paginate using ?limit=100&page=n until no more results are returned.
    Works for standard FUB API keys (no export/cursor access needed).
    """
    url = f"{FUB_BASE_URL.rstrip('/')}/v1/people"
    all_contacts = []
    page = 1

    while True:
        params = {"limit": limit, "page": page}
        logging.info(f"📡 Fetching page {page} from {url}")
        resp = requests.get(url, headers=fub_headers(), params=params, timeout=60)

        if resp.status_code != 200:
            logging.error(f"❌ FUB API Error {resp.status_code}: {resp.text}")
            break

        data = resp.json()
        people = data.get("people") or data.get("items") or data.get("contacts") or []

        if not people:
            logging.info(f"✅ Completed: no more data after page {page}.")
            break

        all_contacts.extend(people)
        logging.info(f"📄 Page {page}: fetched {len(people)} contacts (total={len(all_contacts)})")

        if len(people) < limit:
            logging.info(f"✅ Final page {page} ({len(people)} records).")
            break

        page += 1
        time.sleep(0.2)  # API rate-limit protection

    logging.info(f"✅ Received total {len(all_contacts)} contacts.")
    return all_contacts


# ------------------------------------------------------------------------------
# HASHING + CHANGE DETECTION
# ------------------------------------------------------------------------------

def compute_contact_hash(c: dict) -> str:
    body = {
        "id": c.get("id"),
        "first": (c.get("firstName") or "").strip(),
        "last": (c.get("lastName") or "").strip(),
        "email": (c.get("primaryEmail") or "").strip(),
        "phone": (c.get("primaryPhoneNumber") or "").strip(),
        "address": {
            "street": (c.get("primaryAddress") or {}).get("street", ""),
            "city": (c.get("primaryAddress") or {}).get("city", ""),
            "state": (c.get("primaryAddress") or {}).get("state", ""),
            "zip": (c.get("primaryAddress") or {}).get("postalCode", ""),
        },
        "stage": (c.get("stage") or "").strip(),
        "source": (c.get("source") or "").strip(),
        "url": (c.get("url") or "").strip(),
    }
    return hashlib.sha256(
        json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def compute_partial_hash(c: dict) -> str:
    body = {
        "first": (c.get("firstName") or "").strip(),
        "last": (c.get("lastName") or "").strip(),
        "email": (c.get("primaryEmail") or "").strip(),
        "phone": (c.get("primaryPhoneNumber") or "").strip(),
    }
    return hashlib.md5(
        json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def get_existing_hashes(conn, fub_ids: list) -> dict:
    if not fub_ids:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            "SELECT fub_id, full_hash FROM contact_hashes WHERE fub_id = ANY(%s)",
            (fub_ids,),
        )
        rows = cur.fetchall()
    return {int(r[0]): r[1] for r in rows}


# ------------------------------------------------------------------------------
# WRITE OPERATIONS
# ------------------------------------------------------------------------------

INSERT_CONTACT_SQL = """
INSERT INTO contacts_master (
    fub_id, first_name, last_name, email, phone,
    street, city, state, zip, stage, source, fub_url, ghl_url, last_sync
) VALUES (
    %(fub_id)s, %(first_name)s, %(last_name)s, %(email)s, %(phone)s,
    %(street)s, %(city)s, %(state)s, %(zip)s, %(stage)s, %(source)s, %(fub_url)s, %(ghl_url)s, %(last_sync)s
)
ON CONFLICT (fub_id) DO UPDATE SET
    first_name = EXCLUDED.first_name,
    last_name  = EXCLUDED.last_name,
    email      = EXCLUDED.email,
    phone      = EXCLUDED.phone,
    street     = EXCLUDED.street,
    city       = EXCLUDED.city,
    state      = EXCLUDED.state,
    zip        = EXCLUDED.zip,
    stage      = EXCLUDED.stage,
    source     = EXCLUDED.source,
    fub_url    = EXCLUDED.fub_url,
    ghl_url    = COALESCE(contacts_master.ghl_url, EXCLUDED.ghl_url),
    last_sync  = EXCLUDED.last_sync;
"""

INSERT_HASH_SQL = """
INSERT INTO contact_hashes (fub_id, full_hash, partial_fub, last_update)
VALUES (%(fub_id)s, %(full_hash)s, %(partial_fub)s, %(last_update)s)
ON CONFLICT (fub_id) DO UPDATE SET
    full_hash   = EXCLUDED.full_hash,
    partial_fub = EXCLUDED.partial_fub,
    last_update = EXCLUDED.last_update;
"""

INSERT_LOG_SQL = """
INSERT INTO sync_logs (fub_id, action, origin, timestamp, notes)
VALUES (%(fub_id)s, %(action)s, %(origin)s, %(timestamp)s, %(notes)s);
"""


def bulk_upsert(conn, contacts, hashes, logs):
    with conn.cursor() as cur:
        if contacts:
            execute_batch(cur, INSERT_CONTACT_SQL, contacts, page_size=500)
        if hashes:
            execute_batch(cur, INSERT_HASH_SQL, hashes, page_size=500)
        if logs:
            execute_batch(cur, INSERT_LOG_SQL, logs, page_size=500)
    conn.commit()
    logging.info(f"✅ Wrote {len(contacts)} contacts, {len(hashes)} hashes, {len(logs)} logs.")


# ------------------------------------------------------------------------------
# MAIN RUN
# ------------------------------------------------------------------------------

def run_backfill():
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    logging.info(f"🚀 Starting FUB → Render backfill [RUN {run_id}]")

    conn = get_db_connection()
    ensure_schema(conn)

    contacts = fetch_all_contacts()
    if not contacts:
        logging.warning("⚠️ No contacts returned from FUB API.")
        return

    total_processed = 0
    total_changed = 0

    now = datetime.now(timezone.utc)
    fub_ids = [c.get("id") for c in contacts if c.get("id") is not None]
    existing_hashes = get_existing_hashes(conn, fub_ids)

    contact_batch = []
    hash_batch = []
    log_batch = []

    for c in contacts:
        fub_id = c.get("id")
        if not fub_id:
            continue

        full_hash = compute_contact_hash(c)
        if existing_hashes.get(int(fub_id)) != full_hash:
            total_changed += 1
            contact_batch.append({
                "fub_id": fub_id,
                "first_name": (c.get("firstName") or "").strip(),
                "last_name": (c.get("lastName") or "").strip(),
                "email": (c.get("primaryEmail") or "").strip(),
                "phone": (c.get("primaryPhoneNumber") or "").strip(),
                "street": (c.get("primaryAddress") or {}).get("street", ""),
                "city": (c.get("primaryAddress") or {}).get("city", ""),
                "state": (c.get("primaryAddress") or {}).get("state", ""),
                "zip": (c.get("primaryAddress") or {}).get("postalCode", ""),
                "stage": (c.get("stage") or "").strip(),
                "source": (c.get("source") or "").strip(),
                "fub_url": (c.get("url") or "").strip(),
                "ghl_url": None,
                "last_sync": now,
            })
            hash_batch.append({
                "fub_id": fub_id,
                "full_hash": full_hash,
                "partial_fub": compute_partial_hash(c),
                "last_update": now,
            })
            log_batch.append({
                "fub_id": fub_id,
                "action": "UPSERT",
                "origin": f"backfill:{run_id}",
                "timestamp": now,
                "notes": f"Updated contact {fub_id}",
            })

        total_processed += 1

        if len(contact_batch) >= 500:
            bulk_upsert(conn, contact_batch, hash_batch, log_batch)
            contact_batch.clear()
            hash_batch.clear()
            log_batch.clear()

    bulk_upsert(conn, contact_batch, hash_batch, log_batch)

    logging.info(
        f"✅ [RUN {run_id}] Backfill complete: total_processed={total_processed}, total_changed={total_changed}"
    )

    conn.close()
    logging.info(f"[RUN {run_id}] Database connection closed.")


if __name__ == "__main__":
    run_backfill()
