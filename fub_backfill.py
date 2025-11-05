import os
import logging
import psycopg2
from psycopg2.extras import execute_batch
import requests
from datetime import datetime
import hashlib

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# Environment variables (Render → Environment)
DB_URL = os.getenv("DATABASE_URL")
FUB_API_KEY = os.getenv("FUB_API_KEY")

if not DB_URL:
    raise RuntimeError("❌ DATABASE_URL is missing from environment variables.")
if not FUB_API_KEY:
    raise RuntimeError("❌ FUB_API_KEY is missing from environment variables.")

PAGE_SIZE = 100
MAX_PAGES = 500


# ---------------------------------------------------------------------------
# DATABASE UTILITIES
# ---------------------------------------------------------------------------

def get_db_connection():
    """Connect to PostgreSQL using psycopg2"""
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = True
    return conn


def ensure_schema(conn):
    """Ensure tables exist with correct structure"""
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


# ---------------------------------------------------------------------------
# HASH FUNCTIONS
# ---------------------------------------------------------------------------

def compute_contact_hash(contact):
    """Compute a stable hash of the main fields for change tracking."""
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
    joined = "|".join(key_fields)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def compute_partial_hash(contact):
    """Compute a smaller hash for FUB-only data."""
    key_fields = [
        contact.get("firstName", ""),
        contact.get("lastName", ""),
        contact.get("primaryEmail", ""),
    ]
    joined = "|".join(key_fields)
    return hashlib.md5(joined.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# API FUNCTIONS
# ---------------------------------------------------------------------------

def get_fub_contacts(page: int):
    """Fetch one page of contacts from Follow Up Boss"""
    url = "https://api.followupboss.com/v1/people"
    params = {"page": page, "limit": PAGE_SIZE}
    headers = {
        "Authorization": f"Basic {FUB_API_KEY}",
        "Accept": "application/json",
    }

    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        if response.status_code != 200:
            logging.error(f"❌ FUB API {response.status_code}: {response.text[:300]}")
            return []

        data = response.json()
        contacts = data.get("people") or data.get("contacts") or []
        logging.info(
            f"Fetched page {page} → {len(contacts)} contacts "
            f"(hasMore={data.get('hasMore', 'n/a')})"
        )
        return contacts
    except Exception as e:
        logging.exception(f"Error fetching page {page}: {e}")
        return []


# ---------------------------------------------------------------------------
# DATABASE OPS
# ---------------------------------------------------------------------------

def get_existing_hashes(conn, fub_ids):
    """Get existing hashes for given FUB IDs"""
    if not fub_ids:
        return {}
    sql = "SELECT fub_id, full_hash FROM contact_hashes WHERE fub_id = ANY(%s)"
    with conn.cursor() as cur:
        cur.execute(sql, (fub_ids,))
        return {r[0]: r[1] for r in cur.fetchall()}


def bulk_upsert(conn, contacts_batch, hashes_batch, logs_batch):
    """Perform bulk upserts for contacts, hashes, and logs."""
    with conn.cursor() as cur:
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
# MAIN PROCESS
# ---------------------------------------------------------------------------

def run_backfill():
    """Full backfill process with accumulative counters and run ID."""

    run_id = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    logging.info(f"🚀 Starting FUB → Render backfill [RUN {run_id}]")

    conn = get_db_connection()
    ensure_schema(conn)

    total_processed = 0
    total_changed = 0

    try:
        for page in range(1, MAX_PAGES + 1):
            contacts = get_fub_contacts(page)
            if not contacts:
                logging.info(f"[RUN {run_id}] No more contacts at page {page}. Ending.")
                break

            fub_ids = [c.get("id") for c in contacts if c.get("id")]
            if not fub_ids:
                continue

            existing_hashes = get_existing_hashes(conn, fub_ids)
            contacts_batch, hashes_batch, logs_batch = [], [], []
            changed_count = 0

            for c in contacts:
                fub_id = c["id"]
                full_hash = compute_contact_hash(c)
                old_hash = existing_hashes.get(fub_id)

                if full_hash != old_hash:
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
                        "last_sync": datetime.utcnow(),
                    }

                    hash_row = {
                        "fub_id": fub_id,
                        "full_hash": full_hash,
                        "partial_fub": compute_partial_hash(c),
                        "last_update": datetime.utcnow(),
                    }

                    log_row = {
                        "fub_id": fub_id,
                        "action": "UPSERT",
                        "origin": "backfill",
                        "timestamp": datetime.utcnow(),
                        "notes": f"Updated contact {fub_id}",
                    }

                    contacts_batch.append(contact_row)
                    hashes_batch.append(hash_row)
                    logs_batch.append(log_row)

            if changed_count > 0:
                bulk_upsert(conn, contacts_batch, hashes_batch, logs_batch)
                total_changed += changed_count

            total_processed += len(contacts)
            logging.info(
                f"[RUN {run_id}] Page {page:>3}: processed={len(contacts):<4} changed={changed_count:<4} "
                f"⇢ total_processed={total_processed:<6} total_changed={total_changed:<6}"
            )

            if len(contacts) < PAGE_SIZE:
                logging.info(f"[RUN {run_id}] Reached end of available contacts.")
                break

        logging.info(f"✅ [RUN {run_id}] Backfill complete: total_processed={total_processed}, total_changed={total_changed}")

    except Exception as e:
        logging.exception(f"❌ [RUN {run_id}] Backfill failed: {e}")

    finally:
        conn.close()
        logging.info(f"[RUN {run_id}] Database connection closed.")


# ---------------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run_backfill()
