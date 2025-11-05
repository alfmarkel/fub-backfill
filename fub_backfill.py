import os
import requests
import psycopg2
import hashlib
import logging
from datetime import datetime, timezone
import urllib.parse as up
 
# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------
FUB_API_KEY = os.getenv("FUB_API_KEY")
FUB_BASE_URL = "https://api.followupboss.com/v1/people"

# ✅ Render-proof DB connection
def get_db_connection():
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        up.uses_netloc.append("postgres")
        url = up.urlparse(db_url)
        return psycopg2.connect(
            dbname=url.path[1:],
            user=url.username,
            password=url.password,
            host=url.hostname,
            port=url.port or 5432
        )
    # Fallback to individual vars (legacy)
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT", 5432)
    )

# ---------------------------------------------------------
# LOGGING
# ---------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ---------------------------------------------------------
# DATABASE HELPERS
# ---------------------------------------------------------
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
            last_sync TIMESTAMPTZ
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS contact_hashes (
            fub_id BIGINT PRIMARY KEY,
            full_hash TEXT,
            partial_fub TEXT,
            last_update TIMESTAMPTZ
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS sync_logs (
            id SERIAL PRIMARY KEY,
            fub_id BIGINT,
            action TEXT,
            origin TEXT,
            timestamp TIMESTAMPTZ,
            notes TEXT
        );
        """)
        conn.commit()
        logging.info("✅ Verified schema exists.")


def compute_contact_hash(contact):
    key_fields = [
        contact.get("firstName", ""),
        contact.get("lastName", ""),
        contact.get("primaryEmail", ""),
        contact.get("primaryPhoneNumber", "")
    ]
    return hashlib.sha256("|".join(key_fields).encode()).hexdigest()


def compute_partial_hash(contact):
    return hashlib.md5(
        (contact.get("primaryEmail", "") + contact.get("primaryPhoneNumber", "")).encode()
    ).hexdigest()


def get_existing_hashes(conn, fub_ids):
    if not fub_ids:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            "SELECT fub_id, full_hash FROM contact_hashes WHERE fub_id = ANY(%s);",
            (fub_ids,)
        )
        return {row[0]: row[1] for row in cur.fetchall()}


def bulk_upsert(conn, contacts, hashes, logs):
    with conn.cursor() as cur:
        for c in contacts:
            cur.execute("""
                INSERT INTO contacts_master (
                    fub_id, first_name, last_name, email, phone, street, city, state, zip,
                    stage, source, fub_url, ghl_url, last_sync
                )
                VALUES (%(fub_id)s, %(first_name)s, %(last_name)s, %(email)s, %(phone)s,
                        %(street)s, %(city)s, %(state)s, %(zip)s, %(stage)s, %(source)s,
                        %(fub_url)s, %(ghl_url)s, %(last_sync)s)
                ON CONFLICT (fub_id) DO UPDATE SET
                    first_name=EXCLUDED.first_name,
                    last_name=EXCLUDED.last_name,
                    email=EXCLUDED.email,
                    phone=EXCLUDED.phone,
                    street=EXCLUDED.street,
                    city=EXCLUDED.city,
                    state=EXCLUDED.state,
                    zip=EXCLUDED.zip,
                    stage=EXCLUDED.stage,
                    source=EXCLUDED.source,
                    fub_url=EXCLUDED.fub_url,
                    last_sync=EXCLUDED.last_sync;
            """, c)

        for h in hashes:
            cur.execute("""
                INSERT INTO contact_hashes (fub_id, full_hash, partial_fub, last_update)
                VALUES (%(fub_id)s, %(full_hash)s, %(partial_fub)s, %(last_update)s)
                ON CONFLICT (fub_id) DO UPDATE SET
                    full_hash=EXCLUDED.full_hash,
                    partial_fub=EXCLUDED.partial_fub,
                    last_update=EXCLUDED.last_update;
            """, h)

        for l in logs:
            cur.execute("""
                INSERT INTO sync_logs (fub_id, action, origin, timestamp, notes)
                VALUES (%(fub_id)s, %(action)s, %(origin)s, %(timestamp)s, %(notes)s);
            """, l)

        conn.commit()
        logging.info(f"✅ Wrote {len(contacts)} contacts, {len(hashes)} hashes, {len(logs)} logs.")


# ---------------------------------------------------------
# FUB API CALLS
# ---------------------------------------------------------
def get_fub_contacts(page: int):
    headers = {"Accept": "application/json"}
    response = requests.get(
        FUB_BASE_URL,
        auth=(FUB_API_KEY, ""),
        params={"page": page, "limit": 100}
    )
    if response.status_code != 200:
        logging.error(f"❌ FUB API {response.status_code}: {response.text}")
        return None
    data = response.json()
    return data.get("people", [])


# ---------------------------------------------------------
# MAIN BACKFILL
# ---------------------------------------------------------
def run_backfill():
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    logging.info(f"🚀 Starting FUB → Render backfill [RUN {run_id}]")
    conn = get_db_connection()
    ensure_schema(conn)

    total_processed = 0
    total_changed = 0
    page = 1
    max_pages = 200  # enough for ~20,000 records

    try:
        while page <= max_pages:
            contacts = get_fub_contacts(page)
            if not contacts:
                logging.info(f"[RUN {run_id}] No more contacts at page {page}. Ending.")
                break

            fub_ids = [c["id"] for c in contacts if "id" in c]
            existing_hashes = get_existing_hashes(conn, fub_ids)
            contacts_batch, hashes_batch, logs_batch = [], [], []
            changed_count = 0

            for c in contacts:
                fub_id = c["id"]
                full_hash = compute_contact_hash(c)
                old_hash = existing_hashes.get(fub_id)

                if old_hash != full_hash:
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
                        "last_sync": datetime.now(timezone.utc)
                    }
                    hash_row = {
                        "fub_id": fub_id,
                        "full_hash": full_hash,
                        "partial_fub": compute_partial_hash(c),
                        "last_update": datetime.now(timezone.utc)
                    }
                    log_row = {
                        "fub_id": fub_id,
                        "action": "UPSERT",
                        "origin": "backfill",
                        "timestamp": datetime.now(timezone.utc),
                        "notes": f"Updated contact {fub_id}"
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
                f"→ total_processed={total_processed:<6} total_changed={total_changed:<6}"
            )

            if len(contacts) < 100:
                logging.info(f"[RUN {run_id}] Reached end of available contacts.")
                break

            page += 1

        logging.info(f"✅ [RUN {run_id}] Backfill complete: total_processed={total_processed}, total_changed={total_changed}")

    except Exception as e:
        logging.exception(f"Backfill failed: {e}")
    finally:
        conn.close()
        logging.info(f"[RUN {run_id}] Database connection closed.")


if __name__ == "__main__":
    run_backfill()

