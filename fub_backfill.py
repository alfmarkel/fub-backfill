# fub_backfill.py
import os
import sys
import json
import time
import base64
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests
import psycopg2
from psycopg2.extras import execute_batch

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------

FUB_API_KEY = os.getenv("FUB_API_KEY") or os.getenv("FUB_KEY") or os.getenv("FUB_TOKEN")
FUB_BASE = os.getenv("FUB_BASE_URL", "https://api.followupboss.com")
FUB_LIMIT = int(os.getenv("FUB_PAGE_SIZE", "100"))

DB_URL = (
    os.getenv("DATABASE_URL")
    or os.getenv("POSTGRES_URL")
    or os.getenv("DB_URL")
    or os.getenv("DATABASE_CONNECTION")
)

# Quick sanity checks (we fail fast & clear in logs)
if not FUB_API_KEY:
    print("ERROR: FUB_API_KEY is not set in environment.", file=sys.stderr)
    sys.exit(1)
if not DB_URL:
    print("ERROR: DATABASE_URL/DB_URL is not set in environment.", file=sys.stderr)
    sys.exit(1)

# Logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# ------------------------------------------------------------------------------
# DB helpers
# ------------------------------------------------------------------------------

def get_db_connection():
    """
    Always connect via TCP using the provided URL, with SSL required (Render/managed PG).
    """
    conn = psycopg2.connect(DB_URL, sslmode="require")
    conn.autocommit = False
    return conn


def ensure_schema(conn):
    """
    You already created tables. We just sanity-check they exist and have the key columns.
    (No destructive DDL here.)
    """
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
# FUB API
# ------------------------------------------------------------------------------

def fub_headers() -> Dict[str, str]:
    # Use Basic auth (key as username / blank password). requests can do auth=, but we keep
    # a header here so we’re explicit and easy to debug.
    token = base64.b64encode(f"{FUB_API_KEY}:".encode("utf-8")).decode("ascii")
    return {
        "Authorization": f"Basic {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def fetch_contacts_cursor(cursor: Optional[str], limit: int) -> Tuple[List[dict], Optional[str]]:
    """
    Cursor-first fetch. If FUB returns a `nextCursor`, we’ll use it. If not, we’ll
    return None for the next cursor and the caller can decide what to do.
    """
    url = f"{FUB_BASE.rstrip('/')}/v1/people"
    params = {"limit": limit}
    if cursor:
        params["cursor"] = cursor

    resp = requests.get(url, headers=fub_headers(), params=params, timeout=30)
    if resp.status_code != 200:
        logging.error(f"❌ FUB API {resp.status_code}: {resp.text}")
        return [], None

    data = resp.json() if resp.text else {}
    people = data.get("people") or data.get("items") or data.get("contacts") or []
    next_cursor = data.get("nextCursor") or data.get("next_cursor") or None

    logging.info(f"Fetched → {len(people)} contacts (next_cursor={'set' if next_cursor else 'None'})")
    return people, next_cursor


def fetch_contacts_page(page: int, limit: int) -> List[dict]:
    """
    Page fallback (if the API doesn’t give a cursor). Some deployments of FUB still
    honor page+limit; others do not. We use this ONLY if cursor isn’t present.
    """
    url = f"{FUB_BASE.rstrip('/')}/v1/people"
    params = {"limit": limit, "page": page}

    resp = requests.get(url, headers=fub_headers(), params=params, timeout=30)
    if resp.status_code != 200:
        logging.error(f"❌ FUB API {resp.status_code}: {resp.text}")
        return []
    data = resp.json() if resp.text else {}
    people = data.get("people") or data.get("items") or data.get("contacts") or []
    logging.info(f"Fetched page {page} → {len(people)} contacts (hasMore=n/a)")
    return people


# ------------------------------------------------------------------------------
# Hashing & change detection
# ------------------------------------------------------------------------------

def compute_contact_hash(c: dict) -> str:
    """
    Full content hash for change detection (stable ordering / trimmed fields).
    """
    body = {
        "id": c.get("id"),
        "first": (c.get("firstName") or "").strip(),
        "last": (c.get("lastName") or "").strip(),
        "email": (c.get("primaryEmail") or "").strip(),
        "phone": (c.get("primaryPhoneNumber") or "").strip(),
        "address": {
            "street": (c.get("primaryAddress", {}) or {}).get("street", "") if c.get("primaryAddress") else "",
            "city": (c.get("primaryAddress", {}) or {}).get("city", "") if c.get("primaryAddress") else "",
            "state": (c.get("primaryAddress", {}) or {}).get("state", "") if c.get("primaryAddress") else "",
            "zip": (c.get("primaryAddress", {}) or {}).get("postalCode", "") if c.get("primaryAddress") else "",
        },
        "stage": (c.get("stage") or "").strip(),
        "source": (c.get("source") or "").strip(),
        "url": (c.get("url") or "").strip(),
    }
    as_bytes = json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
    import hashlib
    return hashlib.sha256(as_bytes).hexdigest()


def compute_partial_hash(c: dict) -> str:
    """
    Smaller fingerprint we can also store (e.g., name+email+phone).
    """
    body = {
        "first": (c.get("firstName") or "").strip(),
        "last": (c.get("lastName") or "").strip(),
        "email": (c.get("primaryEmail") or "").strip(),
        "phone": (c.get("primaryPhoneNumber") or "").strip(),
    }
    as_bytes = json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
    import hashlib
    return hashlib.md5(as_bytes).hexdigest()


def get_existing_hashes(conn, fub_ids: List[int]) -> Dict[int, str]:
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
# Writes
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


def bulk_upsert(conn, contacts: List[dict], hashes: List[dict], logs: List[dict]):
    with conn.cursor() as cur:
        if contacts:
            execute_batch(cur, INSERT_CONTACT_SQL, contacts, page_size=500)
        if hashes:
            execute_batch(cur, INSERT_HASH_SQL, hashes, page_size=500)
        if logs:
            execute_batch(cur, INSERT_LOG_SQL, logs, page_size=500)
    conn.commit()
    if contacts or hashes or logs:
        logging.info(f"✅ Wrote {len(contacts)} contacts, {len(hashes)} hashes, {len(logs)} logs.")


# ------------------------------------------------------------------------------
# Runner
# ------------------------------------------------------------------------------

def run_backfill():
    """
    Full backfill with cursor-first pagination (falls back to page if cursor is absent).
    Per-run counters are included in logs (reset each execution).
    """
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    logging.info(f"🚀 Starting FUB → Render backfill [RUN {run_id}]")

    conn = get_db_connection()
    ensure_schema(conn)

    total_processed = 0
    total_changed = 0

    page = 1
    cursor = None
    used_cursor = False
    safety_max = int(os.getenv("MAX_PAGES", "2000"))  # hard cap so we never loop forever

    try:
        for _ in range(safety_max):
            # 1) Prefer cursor paging
            people, next_cursor = fetch_contacts_cursor(cursor, FUB_LIMIT)

            if people:
                used_cursor = True
            else:
                # If cursor gave nothing AND we're on the first loop, try page fallback
                if cursor is None and page == 1:
                    people = fetch_contacts_page(page, FUB_LIMIT)
                else:
                    # Cursor pagination says we’re done
                    break

            if not people:
                logging.info(f"[RUN {run_id}] No more contacts to process. Ending.")
                break

            fub_ids = [p.get("id") for p in people if p.get("id") is not None]
            existing = get_existing_hashes(conn, fub_ids)

            contacts_batch: List[dict] = []
            hashes_batch: List[dict] = []
            logs_batch: List[dict] = []

            changed = 0
            now = datetime.now(timezone.utc)

            for c in people:
                fub_id = c.get("id")
                if fub_id is None:
                    continue

                full_hash = compute_contact_hash(c)
                if existing.get(int(fub_id)) != full_hash:
                    changed += 1

                    contact_row = {
                        "fub_id": fub_id,
                        "first_name": (c.get("firstName") or "").strip(),
                        "last_name": (c.get("lastName") or "").strip(),
                        "email": (c.get("primaryEmail") or "").strip(),
                        "phone": (c.get("primaryPhoneNumber") or "").strip(),
                        "street": (c.get("primaryAddress") or {}).get("street", "") if c.get("primaryAddress") else "",
                        "city": (c.get("primaryAddress") or {}).get("city", "") if c.get("primaryAddress") else "",
                        "state": (c.get("primaryAddress") or {}).get("state", "") if c.get("primaryAddress") else "",
                        "zip": (c.get("primaryAddress") or {}).get("postalCode", "") if c.get("primaryAddress") else "",
                        "stage": (c.get("stage") or "").strip(),
                        "source": (c.get("source") or "").strip(),
                        "fub_url": (c.get("url") or "").strip(),
                        "ghl_url": None,
                        "last_sync": now,
                    }
                    hash_row = {
                        "fub_id": fub_id,
                        "full_hash": full_hash,
                        "partial_fub": compute_partial_hash(c),
                        "last_update": now,
                    }
                    log_row = {
                        "fub_id": fub_id,
                        "action": "UPSERT",
                        "origin": f"backfill:{run_id}",
                        "timestamp": now,
                        "notes": f"Updated contact {fub_id}",
                    }

                    contacts_batch.append(contact_row)
                    hashes_batch.append(hash_row)
                    logs_batch.append(log_row)

            if changed > 0:
                bulk_upsert(conn, contacts_batch, hashes_batch, logs_batch)
                total_changed += changed

            total_processed += len(people)
            where = f"Page {page:>3}" if not used_cursor else f"Cursor {('START' if cursor is None else cursor[:8]+'…')}"
            logging.info(
                f"[RUN {run_id}] {where}: processed={len(people):<4} changed={changed:<4} "
                f"→ total_processed={total_processed:<6} total_changed={total_changed:<6}"
            )

            # Pagination advance
            if used_cursor:
                cursor = next_cursor
                if not cursor:
                    logging.info(f"[RUN {run_id}] Reached end (no next cursor).")
                    break
            else:
                # Page fallback
                page += 1
                if len(people) < FUB_LIMIT:
                    logging.info(f"[RUN {run_id}] Reached end (short page).")
                    break

        logging.info(f"✅ [RUN {run_id}] Backfill complete: total_processed={total_processed}, total_changed={total_changed}")

    except Exception as e:
        logging.exception(f"Backfill failed: {e}")
        conn.rollback()
    finally:
        try:
            conn.close()
            logging.info(f"[RUN {run_id}] Database connection closed.")
        except Exception:
            pass


if __name__ == "__main__":
    # When Render runs `python fub_backfill.py && sleep 5`, we just run once and exit.
    run_backfill()
