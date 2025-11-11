import os
import psycopg2
import requests
import hashlib
from datetime import datetime
from urllib.parse import urljoin

# === ENVIRONMENT CONFIG ===
FUB_API_KEY = os.getenv("FUB_API_KEY")
DB_URL = os.getenv("DATABASE_URL")  # full postgres:// URI from Render
X_SYSTEM_NAME = os.getenv("FUB_SYSTEM", "x-system")
X_SYSTEM_KEY = os.getenv("FUB_SYSTEM_KEY", "test-key")

# === CONSTANTS ===
BASE_URL = "https://api.followupboss.com/v1/people"
PAGE_SIZE = 100
BATCH_LIMIT = 10000  # safety cap for full runs

# === LOGGING ===
def log(msg: str):
    print(f"{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} [INFO] {msg}")

# === HASHING ===
def compute_hash(record: dict) -> str:
    """Compute a hash for change detection."""
    raw = "|".join([str(record.get(k, "")) for k in sorted(record.keys())])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

# === DATABASE ===
def get_connection():
    return psycopg2.connect(DB_URL)

def ensure_tables():
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS contacts_master (
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
        CREATE TABLE IF NOT EXISTS contact_hashes (
            fub_id BIGINT PRIMARY KEY,
            data_hash TEXT,
            updated_at TIMESTAMP
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS sync_logs (
            id SERIAL PRIMARY KEY,
            fub_id BIGINT,
            action TEXT,
            run_id TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        conn.commit()
        log("✅ Tables verified or created.")

# === FUB API ===
def fetch_people(url: str):
    """Fetch a single page of people from FUB."""
    headers = {
        "Authorization": "Basic " + requests.utils.quote(f"{FUB_API_KEY}:"),
        "X-System": X_SYSTEM_NAME,
        "X-System-Key": X_SYSTEM_KEY,
    }
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        log(f"❌ Error {resp.status_code}: {resp.text}")
        return None
    return resp.json()

# === WRITE ===
def write_batch_to_db(records, conn, run_id):
    """Write contacts and logs to DB."""
    if not records:
        return
    with conn.cursor() as cur:
        for p in records:
            fub_id = p.get("id")
            name = p.get("name")
            email = p.get("emails", [{}])[0].get("value")
            phone = p.get("phones", [{}])[0].get("value")
            stage = p.get("stage")
            source = p.get("source")

            # Insert or update contact
            cur.execute("""
                INSERT INTO contacts_master (fub_id, full_name, email, phone, stage, source, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (fub_id) DO UPDATE
                SET full_name = EXCLUDED.full_name,
                    email = EXCLUDED.email,
                    phone = EXCLUDED.phone,
                    stage = EXCLUDED.stage,
                    source = EXCLUDED.source,
                    updated_at = EXCLUDED.updated_at;
            """, (fub_id, name, email, phone, stage, source, datetime.utcnow()))

            # Hash tracking
            h = compute_hash(p)
            cur.execute("""
                INSERT INTO contact_hashes (fub_id, data_hash, updated_at)
                VALUES (%s,%s,%s)
                ON CONFLICT (fub_id) DO UPDATE
                SET data_hash = EXCLUDED.data_hash, updated_at = EXCLUDED.updated_at;
            """, (fub_id, h, datetime.utcnow()))

            # Log entry
            cur.execute("""
                INSERT INTO sync_logs (fub_id, action, run_id, created_at)
                VALUES (%s,%s,%s,%s)
            """, (fub_id, "backfill", run_id, datetime.utcnow()))
    conn.commit()

# === MAIN ===
def run_full_backfill():
    log("🚀 Starting FUB → Render full backfill")
    ensure_tables()
    conn = get_connection()
    total_processed = 0
    page = 1
    next_link = f"{BASE_URL}?limit={PAGE_SIZE}"
    run_id = datetime.utcnow().strftime("%Y%m%d%H%M%S")

    while next_link and total_processed < BATCH_LIMIT:
        log(f"📦 Fetching Page {page}: {next_link}")
        data = fetch_people(next_link)
        if not data:
            break

        people = data.get("people", [])
        meta = data.get("_metadata", {})
        write_batch_to_db(people, conn, run_id)
        total_processed += len(people)

        next_link = meta.get("nextLink")
        page += 1
        if not next_link:
            log("🏁 No nextLink found — reached end of records.")
            break

    log(f"✅ Completed backfill. Total records processed: {total_processed}")
    conn.close()
    log("🔒 Database connection closed.")

# === ENTRY POINT ===
if __name__ == "__main__":
    run_full_backfill()
