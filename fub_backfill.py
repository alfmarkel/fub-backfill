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
# CONFIG
# ------------------------------------------------------------------------------

FUB_API_KEY = os.getenv("FUB_API_KEY")
DB_URL = os.getenv("DATABASE_URL")
FUB_BASE_URL = "https://api.followupboss.com"

if not FUB_API_KEY or not DB_URL:
    print("❌ Missing required environment variables FUB_API_KEY or DATABASE_URL", file=sys.stderr)
    sys.exit(1)

logging.basicConfig(
    level="INFO",
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
# FOLLOW UP BOSS API
# ------------------------------------------------------------------------------

def fub_headers():
    token = base64.b64encode(f"{FUB_API_KEY}:".encode("utf-8")).decode("ascii")
    return {
        "Authorization": f"Basic {token}",
        "Accept": "application/json",
    }


# ------------------------------------------------------------------------------
# HASHING HELPERS
# ------------------------------------------------------------------------------

def compute_contact_hash(c: dict) -> str:
    body = {
        "id": c.get("id"),
        "first": (c.get("firstName") or "").strip(),
        "last": (c.get("lastName") or "").strip(),
        "email": (c.get("primaryEmail") or "").strip(),
        "phone": (c.get("primaryPhoneNumber") or "").strip(),
        "address": (c.get("primaryAddress") or {}),
        "stage": (c.get("stage") or "").strip(),
        "source": (c.get("source") or "").strip(),
        "url": (c.get("url") or "").strip(),
    }
    return hashlib.sha256(json.dumps(body, sort_keys=True).encode("utf-8")).hexdigest()


def compute_partial_hash(c: dict) -> str:
    body = {
        "first": (c.get("firstName") or "").strip(),
        "last": (c.get("lastName") or "").strip(),
        "email": (c.get("primaryEmail") or "").strip(),
        "phone": (c.get("primaryPhoneNumber") or "").strip(),
    }
    return hashlib.md5(json.dumps(body, sort_keys=True).encode("utf-8")).hexdigest()


# ------------------------------------------------------------------------------
# WRITE OPERATIONS
# ------------------------------------------------------------------------------

INSERT_CONTACT_SQL = """
INSERT INTO contacts_master (
    fub_id, first_name, last_name, email, phone, street, city, state, zip,
    stage, source, fub_url, ghl_url, last_sync
) VALUES (
    %(fub_id)s, %(first_name)s, %(last_name)s, %(email)s, %(phone)s,
    %(street)s, %(city)s, %(state)s, %(zip)s, %(stage)s, %(source)s,
    %(fub_url)s, %(ghl_url)s, %(last_sync)s
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
    last_sync  = EXCLUDED.last_sync;
"""

INSERT_HASH_SQL = """
INSERT INTO contact_hashes (fub_id, full_hash, partial_fub, last_update)
VALUES (%(fub_id)s, %(full_hash)s, %(partial_fub)s, %(last_update)s)
ON CONFLICT (fub_id) DO UPDATE SET
    full_hash = EXCLUDED.full_hash,
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
    logging.info(f"🗃️ Wrote {len(contacts)} contacts, {len(hashes)} hashes, {len(logs)} logs.")


# ------------------------------------------------------------------------------
# MAIN RUN LOOP
# ------------------------------------------------------------------------------

def run_backfill():
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    logging.info(f"🚀 Starting FUB → Render backfill [RUN {run_id}]")

    conn = get_db_connection()
    ensure_schema(conn)

    total_processed = 0
    total_changed = 0
    page = 1
    limit = 100

    while True:
        url = f"{FUB_BASE_URL}/v1/people"
        params = {"limit": limit, "page": page}
        resp = requests.get(url, headers=fub_headers(), params=params, timeout=60)

        if resp.status_code != 200:
            logging.error(f"❌ API error {resp.status_code}: {resp.text}")
            break

        data = resp.json()
        people = data.get("people") or []
        if not people:
            logging.info(f"✅ No more contacts at page {page}.")
            break

        now = datetime.now(timezone.utc)
        contact_batch, hash_batch, log_batch = [], [], []

        for c in people:
            fub_id = c.get("id")
            if not fub_id:
                continue

            full_hash = compute_contact_hash(c)
            contact_batch.append({
                "fub_id": fub_id,
                "first_name": c.get("firstName", ""),
                "last_name": c.get("lastName", ""),
                "email": c.get("primaryEmail", ""),
                "phone": c.get("primaryPhoneNumber", ""),
                "street": (c.get("primaryAddress") or {}).get("street", ""),
                "city": (c.get("primaryAddress") or {}).get("city", ""),
                "state": (c.get("primaryAddress") or {}).get("state", ""),
                "zip": (c.get("primaryAddress") or {}).get("postalCode", ""),
                "stage": c.get("stage", ""),
                "source": c.get("source", ""),
                "fub_url": c.get("url", ""),
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
                "notes": f"Synced contact {fub_id}",
            })
            total_processed += 1
            total_changed += 1

        bulk_upsert(conn, contact_batch, hash_batch, log_batch)
        logging.info(f"[RUN {run_id}] Page {page}: processed={len(people)} → total_processed={total_processed}")

        if len(people) < limit:
            logging.info(f"✅ Final page {page}. Total contacts processed: {total_processed}")
            break

        page += 1
        time.sleep(0.2)

    conn.close()
    logging.info(f"✅ Backfill complete: processed={total_processed}, changed={total_changed}")
    logging.info(f"[RUN {run_id}] Database connection closed.")


if __name__ == "__main__":
    run_backfill()
