import os
import logging
import psycopg2
from psycopg2.extras import execute_batch
import requests
from datetime import datetime, timezone
import hashlib

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

DB_URL = os.getenv("DATABASE_URL")
FUB_API_KEY = os.getenv("FUB_API_KEY")

if not DB_URL:
    raise RuntimeError("❌ DATABASE_URL is missing from environment variables.")
if not FUB_API_KEY:
    raise RuntimeError("❌ FUB_API_KEY is missing from environment variables.")

PAGE_SIZE = 100


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------

def get_db_connection():
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = True
    return conn


def ensure_schema(conn):
    schema_sql = """
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
        fub_url TEXT,
        ghl_url TEXT,
        stage TEXT,
        source TEXT,
        last_sync TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS contact_hashes (
        fub_id BIGINT PRIMARY KEY,
        full_hash TEXT,
        partial_fub TEXT,
        last_update TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS sync_logs (
        id BIGSERIAL PRIMARY KEY,
        fub_id BIGINT,
        action TEXT,
        origin TEXT,
        timestamp TIMESTAMP,
        notes TEXT
    );
    """
    with conn.cursor() as cur:
        cur.execute(schema_sql)
    logging.info("✅ Verified schema exists.")


def get_existing_hashes(conn, fub_ids):
    if not fub_ids:
        return {}
    sql = "SELECT fub_id, full_hash FROM contact_hashes WHERE fub_id = ANY(%s)"
    with conn.cursor() as cur:
        cur.execute(sql, (fub_ids,))
        return {r[0]: r[1] for r in cur.fetchall()}


def get_existing_master_ids(conn, fub_ids):
    if not fub_ids:
        return set()
    sql = "SELECT fub_id FROM contacts_master WHERE fub_id = ANY(%s)"
    with conn.cursor() as cur:
        cur.execute(sql, (fub_ids,))
        return {r[0] for r in cur.fetchall()}


def bulk_upsert(conn, contacts_batch, hashes_batch, logs_batch):
    with conn.cursor() as cur:
        if contacts_batch:
            execute_batch(
                cur,
                """
                INSERT INTO contacts_master (
                    fub_id, first_name, last_name, email, phone, street, city, state, zip,
                    fub_url, ghl_url, stage, source, last_sync
                )
                VALUES (
                    %(fub_id)s, %(first_name)s, %(last_name)s, %(email)s, %(phone)s,
                    %(street)s, %(city)s, %(state)s, %(zip)s,
                    %(fub_url)s, %(ghl_url)s, %(stage)s, %(source)s, %(last_sync)s
                )
                ON CONFLICT (fub_id)
                DO UPDATE SET
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    email = EXCLUDED.email,
                    phone = EXCLUDED.phone,
                    street = EXCLUDED.street,
                    city = EXCLUDED.city,
                    state = EXCLUDED.state,
                    zip = EXCLUDED.zip,
                    fub_url = EXCLUDED.fub_url,
                    ghl_url = EXCLUDED.ghl_url,
                    stage = EXCLUDED.stage,
                    source = EXCLUDED.source,
                    last_sync = EXCLUDED.last_sync;
                """,
                contacts_batch,
                page_size=500,
            )

        if hashes_batch:
            execute_batch(
                cur,
                """
                INSERT INTO contact_hashes (fub_id, full_hash, partial_fub, last_update)
                VALUES (%(fub_id)s, %(full_hash)s, %(partial_fub)s, %(last_update)s)
                ON CONFLICT (fub_id)
                DO UPDATE SET
                    full_hash = EXCLUDED.full_hash,
                    partial_fub = EXCLUDED.partial_fub,
                    last_update = EXCLUDED.last_update;
                """,
                hashes_batch,
                page_size=500,
            )

        if logs_batch:
            execute_batch(
                cur,
                """
                INSERT INTO sync_logs (fub_id, action, origin, timestamp, notes)
                VALUES (%(fub_id)s, %(action)s, %(origin)s, %(timestamp)s, %(notes)s);
                """,
                logs_batch,
                page_size=500,
            )

    logging.info(
        f"✅ Wrote {len(contacts_batch)} contacts, {len(hashes_batch)} hashes, {len(logs_batch)} logs."
    )


# ---------------------------------------------------------------------------
# HASH
# ---------------------------------------------------------------------------

def compute_contact_hash(contact):
    key_fields = [
        contact.get("firstName", ""),
        contact.get("lastName", ""),
        contact.get("primaryEmail", ""),
        contact.get("primaryPhoneNumber", ""),
        contact.get("stage", ""),
        contact.get("source", ""),
        str(contact.get("primaryAddress", {}).get("street", "")),
        str(contact.get("primaryAddress", {}).get("city", "")),
        str(contact.get("primaryAddress", {}).get("state", "")),
        str(contact.get("primaryAddress", {}).get("postalCode", "")),
    ]
    return hashlib.sha256("|".join(key_fields).encode("utf-8")).hexdigest()


def compute_partial_hash(contact):
    key_fields = [
        contact.get("firstName", ""),
        contact.get("lastName", ""),
        contact.get("primaryEmail", ""),
    ]
    return hashlib.md5("|".join(key_fields).encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# FUB API with cursor pagination
# ---------------------------------------------------------------------------

def get_fub_contacts(cursor=None):
    """Fetch contacts from Follow Up Boss API using cursor-based pagination."""
    url = "https://api.followupboss.com/v1/people"
    params = {"limit": PAGE_SIZE}
    if cursor:
        params["cursor"] = cursor

    try:
        resp = requests.get(
            url,
            params=params,
            auth=(FUB_API_KEY, ""),
            timeout=30,
        )
        if resp.status_code != 200:
            logging.error(f"❌ FUB API {resp.status_code}: {resp.text[:300]}")
            return [], None

        data = resp.json()
        contacts = data.get("people") or data.get("contacts") or []
        next_cursor = data.get("next") or data.get("cursor") or None
        logging.info(f"Fetched → {len(contacts)} contacts (next_cursor={next_cursor})")
        return contacts, next_cursor

    except Exception as e:
        logging.exception(f"Error fetching contacts: {e}")
        return [], None


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def run_backfill():
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    logging.info(f"🚀 Starting FUB → Render backfill [RUN {run_id}]")

    conn = get_db_connection()
    ensure_schema(conn)

    total_processed = 0
    total_changed = 0
    cursor = None

    try:
        while True:
            contacts, cursor = get_fub_contacts(cursor)
            if not contacts:
                break

            fub_ids = [c.get("id") for c in contacts if c.get("id")]
            if not fub_ids:
                continue

            existing_hashes = get_existing_hashes(conn, fub_ids)
            existing_master = get_existing_master_ids(conn, fub_ids)
            contacts_batch, hashes_batch, logs_batch = [], [], []
            changed_count = 0

            for c in contacts:
                fub_id = c["id"]
                full_hash = compute_contact_hash(c)
                old_hash = existing_hashes.get(fub_id)
                missing_master = fub_id not in existing_master

                if missing_master or full_hash != old_hash:
                    changed_count += 1
                    contact_row = {
                        "fub_id": fub_id,
                        "first_name": c.get("firstName", ""),
                        "last_name": c.get("lastName", ""),
                        "email": c.get("primaryEmail", ""),
                        "phone": c.get("primaryPhoneNumber", ""),
                        "street": c.get("primaryAddress", {}).get("street", "") if c.get("primaryAddress") else "",
                        "city": c.get("primaryAddress", {}).get("city", "") if c.get("primaryAddress") else "",
                        "state": c.get("primaryAddress", {}).get("state", "") if c.get("primaryAddress") else "",
                        "zip": c.get("primaryAddress", {}).get("postalCode", "") if c.get("primaryAddress") else "",
                        "stage": c.get("stage", ""),
                        "source": c.get("source", ""),
                        "fub_url": c.get("url", ""),
                        "ghl_url": None,
                        "last_sync": datetime.now(timezone.utc),
                    }

                    hash_row = {
                        "fub_id": fub_id,
                        "full_hash": full_hash,
                        "partial_fub": compute_partial_hash(c),
                        "last_update": datetime.now(timezone.utc),
                    }

                    log_row = {
                        "fub_id": fub_id,
                        "action": "UPSERT" if missing_master else "UPDATE",
                        "origin": "backfill",
                        "timestamp": datetime.now(timezone.utc),
                        "notes": "Inserted new master row" if missing_master else "Hash changed",
                    }

                    contacts_batch.append(contact_row)
                    hashes_batch.append(hash_row)
                    logs_batch.append(log_row)

            if changed_count > 0:
                bulk_upsert(conn, contacts_batch, hashes_batch, logs_batch)
                total_changed += changed_count

            total_processed += len(contacts)
            logging.info(
                f"[RUN {run_id}] processed={len(contacts):<4} changed={changed_count:<4} "
                f"⇢ total_processed={total_processed:<6} total_changed={total_changed:<6}"
            )

            if not cursor:
                logging.info(f"[RUN {run_id}] Reached end of available contacts.")
                break

        logging.info(f"✅ [RUN {run_id}] Backfill complete: total_processed={total_processed}, total_changed={total_changed}")

    except Exception as e:
        logging.exception(f"❌ [RUN {run_id}] Backfill failed: {e}")

    finally:
        conn.close()
        logging.info(f"[RUN {run_id}] Database connection closed.")


if __name__ == "__main__":
    run_backfill()

