import os
import requests
import psycopg2
import hashlib
from datetime import datetime, timezone

# ------------------------------------------------------------
# ENVIRONMENT VARIABLES
# ------------------------------------------------------------
FUB_API_KEY = os.getenv("FUB_API_KEY")
FUB_SYSTEM_KEY = os.getenv("FUB_SYSTEM_KEY", "x-system")
DATABASE_URL = os.getenv("DATABASE_URL")

# ------------------------------------------------------------
# LOGGING UTILITY
# ------------------------------------------------------------
def log(msg):
    print(f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} [INFO] {msg}")

# ------------------------------------------------------------
# DATABASE CONNECTION
# ------------------------------------------------------------
def get_connection():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    return conn

# ------------------------------------------------------------
# TABLE CREATION / VALIDATION
# ------------------------------------------------------------
def ensure_tables():
    with get_connection() as conn, conn.cursor() as cur:
        # Force correct schema and heal missing columns
        cur.execute("SET search_path TO public;")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS public.contacts_master (
            fub_id BIGINT PRIMARY KEY,
            full_name TEXT,
            email TEXT,
            phone TEXT,
            stage TEXT,
            source TEXT,
            updated_at TIMESTAMP
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS public.contact_hashes (
            fub_id BIGINT PRIMARY KEY,
            data_hash TEXT,
            updated_at TIMESTAMP
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS public.sync_logs (
            id SERIAL PRIMARY KEY,
            fub_id BIGINT,
            action TEXT,
            run_id TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # Auto-heal schema if somehow desynced
        cur.execute("ALTER TABLE public.contact_hashes ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP;")
        conn.commit()
        log("✅ Tables verified or created in public schema.")

# ------------------------------------------------------------
# FETCH DATA FROM FOLLOWUPBOSS
# ------------------------------------------------------------
def fetch_page(url):
    headers = {
        "Authorization": f"Basic {FUB_API_KEY}:",
        "X-System": FUB_SYSTEM_KEY
    }
    res = requests.get(url, headers=headers)
    if res.status_code != 200:
        log(f"❌ Error {res.status_code}: {res.text}")
        return None
    return res.json()

# ------------------------------------------------------------
# HASHING UTILITY
# ------------------------------------------------------------
def compute_hash(person):
    m = hashlib.sha256()
    fields = [
        str(person.get("id", "")),
        person.get("name", ""),
        str(person.get("emails", [])),
        str(person.get("phones", [])),
        person.get("stage", ""),
        person.get("source", ""),
    ]
    m.update("|".join(fields).encode("utf-8"))
    return m.hexdigest()

# ------------------------------------------------------------
# WRITE TO DATABASE
# ------------------------------------------------------------
def write_batch_to_db(people, conn, run_id):
    with conn.cursor() as cur:
        for person in people:
            fub_id = person["id"]
            full_name = person.get("name")
            email = person.get("emails", [{}])[0].get("value") if person.get("emails") else None
            phone = person.get("phones", [{}])[0].get("value") if person.get("phones") else None
            stage = person.get("stage")
            source = person.get("source")
            h = compute_hash(person)

            # Upsert contact
            cur.execute("""
                INSERT INTO public.contacts_master (fub_id, full_name, email, phone, stage, source, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (fub_id)
                DO UPDATE SET full_name = EXCLUDED.full_name,
                              email = EXCLUDED.email,
                              phone = EXCLUDED.phone,
                              stage = EXCLUDED.stage,
                              source = EXCLUDED.source,
                              updated_at = EXCLUDED.updated_at;
            """, (fub_id, full_name, email, phone, stage, source, datetime.now(timezone.utc)))

            # Upsert hash
            cur.execute("""
                INSERT INTO public.contact_hashes (fub_id, data_hash, updated_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (fub_id)
                DO UPDATE SET data_hash = EXCLUDED.data_hash,
                              updated_at = EXCLUDED.updated_at;
            """, (fub_id, h, datetime.now(timezone.utc)))

            # Log sync action
            cur.execute("""
                INSERT INTO public.sync_logs (fub_id, action, run_id)
                VALUES (%s, %s, %s);
            """, (fub_id, "FUB_BACKFILL", run_id))

        conn.commit()
        log(f"✅ Wrote {len(people)} contacts to database.")

# ------------------------------------------------------------
# FULL BACKFILL EXECUTION
# ------------------------------------------------------------
def run_full_backfill():
    log("🚀 Starting FUB → Render full backfill")
    ensure_tables()

    conn = get_connection()
    base_url = "https://api.followupboss.com/v1/people?limit=100"
    next_url = base_url
    total_processed = 0
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

    while next_url:
        data = fetch_page(next_url)
        if not data or "people" not in data:
            break

        people = data["people"]
        if not people:
            break

        total_processed += len(people)
        write_batch_to_db(people, conn, run_id)

        meta = data.get("_metadata", {})
        next_url = meta.get("nextLink")
        if next_url:
            log(f"🔄 Next page → {next_url}")
        else:
            break

    conn.close()
    log(f"✅ Completed backfill. Total records processed: {total_processed}")
    log("🔒 Database connection closed.")

# ------------------------------------------------------------
# ENTRY POINT
# ------------------------------------------------------------
if __name__ == "__main__":
    run_full_backfill()
