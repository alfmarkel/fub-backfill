import os, logging, psycopg2, requests, hashlib
from psycopg2.extras import execute_batch
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

DB_URL = os.getenv("DATABASE_URL")
FUB_API_KEY = os.getenv("FUB_API_KEY")
PAGE_SIZE, MAX_PAGES, BATCH_LOOKUP = 100, 2000, 5000

if not DB_URL:
    raise RuntimeError("❌ DATABASE_URL not set")
if not FUB_API_KEY:
    raise RuntimeError("❌ FUB_API_KEY not set")

# ---------------------------------------------------------------------------
# DB HELPERS
# ---------------------------------------------------------------------------
def get_db():
    c = psycopg2.connect(DB_URL)
    c.autocommit = True
    return c

def ensure_schema(conn):
    sql = """
    CREATE TABLE IF NOT EXISTS contacts_master(
      fub_id BIGINT PRIMARY KEY,
      first_name TEXT, last_name TEXT, email TEXT, phone TEXT,
      street TEXT, city TEXT, state TEXT, zip TEXT,
      fub_url TEXT, ghl_url TEXT,
      stage TEXT, source TEXT,
      last_sync TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS contact_hashes(
      fub_id BIGINT PRIMARY KEY,
      full_hash TEXT, partial_fub TEXT, last_update TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS sync_logs(
      id BIGSERIAL PRIMARY KEY,
      fub_id BIGINT,
      action TEXT,
      origin TEXT,
      timestamp TIMESTAMP,
      notes TEXT
    );
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    logging.info("✅ Verified schema exists.")

def batched_query(conn, sql, ids):
    out = set()
    for i in range(0, len(ids), BATCH_LOOKUP):
        with conn.cursor() as cur:
            cur.execute(sql, (ids[i:i+BATCH_LOOKUP],))
            out |= {r[0] for r in cur.fetchall()}
    return out

def get_hashes(conn, ids):
    out = {}
    for i in range(0, len(ids), BATCH_LOOKUP):
        with conn.cursor() as cur:
            cur.execute("SELECT fub_id, full_hash FROM contact_hashes WHERE fub_id = ANY(%s)",
                        (ids[i:i+BATCH_LOOKUP],))
            for fid, fh in cur.fetchall():
                out[fid] = fh
    return out

def upsert_all(conn, contacts, hashes, logs):
    with conn.cursor() as cur:
        if contacts:
            execute_batch(cur, """
                INSERT INTO contacts_master(
                  fub_id, first_name, last_name, email, phone,
                  street, city, state, zip, fub_url, ghl_url,
                  stage, source, last_sync
                )
                VALUES(
                  %(fub_id)s, %(first_name)s, %(last_name)s, %(email)s, %(phone)s,
                  %(street)s, %(city)s, %(state)s, %(zip)s,
                  %(fub_url)s, %(ghl_url)s,
                  %(stage)s, %(source)s, %(last_sync)s
                )
                ON CONFLICT(fub_id) DO UPDATE SET
                  first_name=EXCLUDED.first_name,
                  last_name=EXCLUDED.last_name,
                  email=EXCLUDED.email,
                  phone=EXCLUDED.phone,
                  street=EXCLUDED.street,
                  city=EXCLUDED.city,
                  state=EXCLUDED.state,
                  zip=EXCLUDED.zip,
                  fub_url=EXCLUDED.fub_url,
                  ghl_url=EXCLUDED.ghl_url,
                  stage=EXCLUDED.stage,
                  source=EXCLUDED.source,
                  last_sync=EXCLUDED.last_sync;
            """, contacts, page_size=500)
        if hashes:
            execute_batch(cur, """
                INSERT INTO contact_hashes(fub_id, full_hash, partial_fub, last_update)
                VALUES(%(fub_id)s, %(full_hash)s, %(partial_fub)s, %(last_update)s)
                ON CONFLICT(fub_id) DO UPDATE SET
                  full_hash=EXCLUDED.full_hash,
                  partial_fub=EXCLUDED.partial_fub,
                  last_update=EXCLUDED.last_update;
            """, hashes, page_size=500)
        if logs:
            execute_batch(cur, """
                INSERT INTO sync_logs(fub_id, action, origin, timestamp, notes)
                VALUES(%(fub_id)s, %(action)s, %(origin)s, %(timestamp)s, %(notes)s);
            """, logs, page_size=500)
    logging.info(f"✅ wrote {len(contacts)} contacts, {len(hashes)} hashes, {len(logs)} logs.")

# ---------------------------------------------------------------------------
# HASHING
# ---------------------------------------------------------------------------
def hash_full(c):
    addr = c.get("primaryAddress") or {}
    fields = [
        c.get("firstName", ""), c.get("lastName", ""), c.get("primaryEmail", ""),
        c.get("primaryPhoneNumber", ""), c.get("stage", ""), c.get("source", ""),
        addr.get("street", ""), addr.get("city", ""), addr.get("state", ""), addr.get("postalCode", "")
    ]
    return hashlib.sha256("|".join(fields).encode()).hexdigest()

def hash_partial(c):
    f = [c.get("firstName",""), c.get("lastName",""), c.get("primaryEmail","")]
    return hashlib.md5("|".join(f).encode()).hexdigest()

# ---------------------------------------------------------------------------
# FUB API
# ---------------------------------------------------------------------------
def get_page(page):
    url = "https://api.followupboss.com/v1/people"
    params = {"limit": PAGE_SIZE, "page": page}
    r = requests.get(url, params=params, auth=(FUB_API_KEY, ""), timeout=30)
    if r.status_code != 200:
        logging.error(f"❌ FUB API {r.status_code}: {r.text[:200]}")
        return []
    data = r.json()
    return data.get("people") or data.get("contacts") or []

# ---------------------------------------------------------------------------
# MAIN BACKFILL
# ---------------------------------------------------------------------------
def run_backfill():
    run = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    logging.info(f"🚀 Starting FUB → Render backfill [RUN {run}]")

    conn = get_db()
    ensure_schema(conn)

    total_proc, total_chg = 0, 0

    try:
        for page in range(1, MAX_PAGES + 1):
            people = get_page(page)
            if not people:
                logging.info(f"[RUN {run}] No contacts on page {page}. Ending cleanly.")
                break

            ids = [p["id"] for p in people if p.get("id")]
            hashes = get_hashes(conn, ids)
            masters = batched_query(conn, "SELECT fub_id FROM contacts_master WHERE fub_id = ANY(%s)", ids)

            contacts, new_hashes, logs = [], [], []
            changed = 0

            for c in people:
                fid = c["id"]
                fh = hash_full(c)
                missing = fid not in masters
                changed_now = missing or fh != hashes.get(fid)
                if changed_now:
                    changed += 1
                    contacts.append({
                        "fub_id": fid,
                        "first_name": c.get("firstName",""),
                        "last_name": c.get("lastName",""),
                        "email": c.get("primaryEmail",""),
                        "phone": c.get("primaryPhoneNumber",""),
                        "street": c.get("primaryAddress",{}).get("street",""),
                        "city": c.get("primaryAddress",{}).get("city",""),
                        "state": c.get("primaryAddress",{}).get("state",""),
                        "zip": c.get("primaryAddress",{}).get("postalCode",""),
                        "fub_url": c.get("url",""),
                        "ghl_url": None,
                        "stage": c.get("stage",""),
                        "source": c.get("source",""),
                        "last_sync": datetime.now(timezone.utc)
                    })
                    new_hashes.append({
                        "fub_id": fid,
                        "full_hash": fh,
                        "partial_fub": hash_partial(c),
                        "last_update": datetime.now(timezone.utc)
                    })
                    logs.append({
                        "fub_id": fid,
                        "action": "UPSERT" if missing else "UPDATE",
                        "origin": "backfill",
                        "timestamp": datetime.now(timezone.utc),
                        "notes": "Inserted new row" if missing else "Hash changed"
                    })

            if changed:
                upsert_all(conn, contacts, new_hashes, logs)
                total_chg += changed

            total_proc += len(people)
            logging.info(f"[RUN {run}] Page {page}: processed={len(people)} changed={changed} "
                         f"→ total_processed={total_proc} total_changed={total_chg}")

            if len(people) < PAGE_SIZE:
                logging.info(f"[RUN {run}] End of data reached.")
                break

        logging.info(f"✅ [RUN {run}] Finished. Total processed={total_proc}, changed={total_chg}")

    except Exception as e:
        logging.exception(f"❌ [RUN {run}] Failed: {e}")
    finally:
        conn.close()
        logging.info(f"[RUN {run}] Database connection closed.")
        logging.info("🏁 Backfill run complete. Exiting normally.")

# ---------------------------------------------------------------------------
if __name__ == "__main__":
    run_backfill()
