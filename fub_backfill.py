import os, sys, time, json, hashlib, logging
from typing import Dict, Any, List, Tuple, Optional

import requests
import psycopg2
from psycopg2.extras import execute_batch, DictCursor

# -----------------------------
# Config (env-based)
# -----------------------------
FUB_API_KEY = os.getenv("FUB_API_KEY", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
BASE_URL = "https://api.followupboss.com/v1"
PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "100"))          # FUB limit per page
SLEEP_BETWEEN_PAGES = float(os.getenv("SLEEP_BETWEEN_PAGES", "0.15"))  # be nice to API
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

if not FUB_API_KEY or not DATABASE_URL:
    print("ERROR: FUB_API_KEY and DATABASE_URL must be set.")
    sys.exit(1)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("fub_backfill")

# -----------------------------
# Helpers
# -----------------------------
def sha256_obj(obj: Any) -> str:
    """Stable hash of any JSON-serializable object."""
    return hashlib.sha256(json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()

def select_primary_email(p: Dict[str, Any]) -> Optional[str]:
    # Prefer p['primaryEmail'] if present, else first from emails list
    if p.get("primaryEmail"):
        return p.get("primaryEmail")
    emails = p.get("emails") or []
    for e in emails:
        if e.get("value"):
            return e["value"]
    return None

def select_primary_phone(p: Dict[str, Any]) -> Optional[str]:
    # Prefer p['primaryPhone'] if present, else first from phones list
    if p.get("primaryPhone"):
        return p.get("primaryPhone")
    phones = p.get("phones") or []
    for ph in phones:
        if ph.get("value"):
            return ph["value"]
    return None

def get_address(p: Dict[str, Any]) -> Dict[str, Optional[str]]:
    addr = p.get("address") or {}
    return {
        "street": addr.get("street"),
        "city": addr.get("city"),
        "state": addr.get("state"),
        "zip": addr.get("zip"),
    }

def extract_custom_field(p: Dict[str, Any], name_contains: str) -> Optional[str]:
    """Find first custom field whose name contains <name_contains> (case-insensitive) and return value."""
    cfs = p.get("customFields") or []
    for cf in cfs:
        n = (cf.get("name") or "").lower()
        if name_contains.lower() in n:
            val = cf.get("value")
            if val:
                return val
    return None

def normalize_person(p: Dict[str, Any]) -> Dict[str, Any]:
    fub_id = p.get("id")
    first = p.get("firstName")
    last = p.get("lastName")
    email = select_primary_email(p)
    phone = select_primary_phone(p)
    addr = get_address(p)
    stage = p.get("stage")
    source = p.get("source")

    # URLs
    fub_url = f"https://allenmarkel.followupboss.com/2/people/{fub_id}" if fub_id else None
    ghl_url = extract_custom_field(p, "ghl url") or extract_custom_field(p, "gohighlevel")

    normalized = {
        "fub_id": fub_id,
        "first_name": first,
        "last_name": last,
        "email": email,
        "phone": phone,
        "street": addr["street"],
        "city": addr["city"],
        "state": addr["state"],
        "zip": addr["zip"],
        "fub_url": fub_url,
        "ghl_url": ghl_url,
        "stage": stage,
        "source": source
    }
    return normalized

def compute_hashes(n: Dict[str, Any]) -> Tuple[str, str]:
    """
    full_hash includes all normalized fields.
    partial_fub excludes 'stage' and 'source' (since these may be volatile for workflows).
    """
    full_hash = sha256_obj(n)
    partial_base = {k: v for k, v in n.items() if k not in ("stage", "source")}
    partial_fub = sha256_obj(partial_base)
    return full_hash, partial_fub

# -----------------------------
# Database
# -----------------------------
SCHEMA_SQL = """
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
  last_sync TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS contact_hashes (
  fub_id BIGINT PRIMARY KEY,
  full_hash TEXT,
  partial_fub TEXT,
  last_update TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sync_logs (
  id BIGSERIAL PRIMARY KEY,
  fub_id BIGINT,
  action TEXT,
  origin TEXT,
  timestamp TIMESTAMP DEFAULT now(),
  notes TEXT
);

-- helpful index for quick verification checks
CREATE INDEX IF NOT EXISTS idx_contacts_master_email ON contacts_master (email);
CREATE INDEX IF NOT EXISTS idx_contacts_master_ghlurl ON contacts_master (ghl_url);
"""

UPSERT_CONTACT_SQL = """
INSERT INTO contacts_master (
  fub_id, first_name, last_name, email, phone, street, city, state, zip, fub_url, ghl_url, stage, source, last_sync
)
VALUES (
  %(fub_id)s, %(first_name)s, %(last_name)s, %(email)s, %(phone)s, %(street)s, %(city)s, %(state)s, %(zip)s, %(fub_url)s, %(ghl_url)s, %(stage)s, %(source)s, now()
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
  fub_url    = EXCLUDED.fub_url,
  ghl_url    = EXCLUDED.ghl_url,
  stage      = EXCLUDED.stage,
  source     = EXCLUDED.source,
  last_sync  = now()
"""

UPSERT_HASH_SQL = """
INSERT INTO contact_hashes (fub_id, full_hash, partial_fub, last_update)
VALUES (%(fub_id)s, %(full_hash)s, %(partial_fub)s, now())
ON CONFLICT (fub_id) DO UPDATE SET
  full_hash  = EXCLUDED.full_hash,
  partial_fub = EXCLUDED.partial_fub,
  last_update = now()
"""

INSERT_LOG_SQL = """
INSERT INTO sync_logs (fub_id, action, origin, notes) VALUES (%s, %s, %s, %s)
"""

def db_connect():
    return psycopg2.connect(DATABASE_URL)

def ensure_schema(conn):
    with conn, conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)

def get_existing_hashes(conn, fub_ids: List[int]) -> Dict[int, str]:
    """Return map fub_id -> full_hash for existing rows to minimize writes & logs."""
    if not fub_ids:
        return {}
    sql = "SELECT fub_id, full_hash FROM contact_hashes WHERE fub_id = ANY(%s)"
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(sql, (fub_ids,))
        rows = cur.fetchall()
    return {int(r["fub_id"]): r["full_hash"] for r in rows}

def bulk_upsert(conn, contacts: List[Dict[str, Any]], hashes: List[Dict[str, Any]], changed_flags: List[Tuple[int, bool]]):
    """Execute bulk upserts, and log only changed ones."""
    with conn:
        with conn.cursor() as cur:
            execute_batch(cur, UPSERT_CONTACT_SQL, contacts, page_size=200)
            execute_batch(cur, UPSERT_HASH_SQL, hashes, page_size=200)

            # Only log updates for changed rows to keep sync_logs lean
            logs = [(c["fub_id"], "update" if changed else "noop", "fub_step2_auth", "contact upsert + hash")
                    for (fub_id, changed), c in zip(changed_flags, contacts)]
            # We can skip "noop" logs to save storage; toggle below as you prefer:
            logs = [l for l in logs if l[1] != "noop"]
            if logs:
                execute_batch(cur, INSERT_LOG_SQL, logs, page_size=500)

# -----------------------------
# FUB API
# -----------------------------
def fub_get_people_page(page: int, limit: int = PAGE_LIMIT) -> Dict[str, Any]:
    """
    FUB supports pagination via 'page' and 'limit' on /people.
    Using Basic Auth: (API_KEY, '')
    """
    url = f"{BASE_URL}/people"
    params = {
        "fields": "allFields",
        "limit": limit,
        "page": page
    }
    resp = requests.get(url, params=params, auth=(FUB_API_KEY, ""))
    resp.raise_for_status()
    return resp.json()

# -----------------------------
# Main routine
# -----------------------------
def run_backfill():
    log.info("Starting FUB → Render backfill…")

    conn = db_connect()
    ensure_schema(conn)

    total_processed = 0
    total_changed = 0
    page = 1

    while True:
        try:
            data = fub_get_people_page(page=page)
        except requests.HTTPError as e:
            log.error(f"HTTP error on page {page}: {e}")
            # backoff + retry basic (cheap and simple)
            time.sleep(2.0)
            continue

        people = data.get("people") or data.get("items") or data.get("data") or []
        if not people:
            log.info("No more people. Done.")
            break

        # Normalize + compute hashes
        contacts_batch: List[Dict[str, Any]] = []
        hashes_batch: List[Dict[str, Any]] = []
        fub_ids: List[int] = []

        for p in people:
            n = normalize_person(p)
            if not n.get("fub_id"):
                continue  # skip corrupt rows defensively

            full_hash, partial_fub = compute_hashes(n)
            n_with_hash = n.copy()

            contacts_batch.append(n_with_hash)
            hashes_batch.append({
                "fub_id": n["fub_id"],
                "full_hash": full_hash,
                "partial_fub": partial_fub
            })
            fub_ids.append(n["fub_id"])

        # Check which actually changed (compare full_hash to existing)
        existing = get_existing_hashes(conn, fub_ids)
        changed_flags: List[Tuple[int, bool]] = []
        # Filter contacts/hashes to only changed for DB write savings, or keep all but log only changed.
        # Here we UPSERT all (safe+simple) but only "log" changed.
        for h in hashes_batch:
            old = existing.get(int(h["fub_id"]))
            changed = (old != h["full_hash"])
            changed_flags.append((h["fub_id"], changed))
        changed_on_page = sum(1 for _, ch in changed_flags if ch)

        # Perform bulk upserts
        bulk_upsert(conn, contacts_batch, hashes_batch, changed_flags)

        total_processed += len(contacts_batch)
        total_changed += changed_on_page
        log.info(f"Page {page}: processed={len(contacts_batch)} changed={changed_on_page} total_processed={total_processed} total_changed={total_changed}")

        page += 1
        time.sleep(SLEEP_BETWEEN_PAGES)

    conn.close()
    log.info(f"Backfill complete. total_processed={total_processed} total_changed={total_changed}")

if __name__ == "__main__":
    run_backfill()
