import logging
import requests
from datetime import datetime
from psycopg2.extras import execute_batch

# --- assumes your existing helper functions still exist elsewhere ---
# get_db_connection(), ensure_schema(), compute_contact_hash(),
# compute_partial_hash(), get_existing_hashes(), bulk_upsert(), etc.

# Replace YOUR actual API key variable if named differently
FUB_API_KEY = FUB_API_KEY  # assuming it’s already defined above


# ---------------------------------------------------------------------------
# GET FUB CONTACTS (fixed with proper pagination + debug logs)
# ---------------------------------------------------------------------------
def get_fub_contacts(page: int):
    """
    Retrieves one page of contacts from the Follow Up Boss API.
    Returns a list of contact dicts, or an empty list if no more data.
    """

    base_url = "https://api.followupboss.com/v1/people"
    params = {"page": page, "limit": 100}
    headers = {
        "Authorization": f"Basic {FUB_API_KEY}",
        "Accept": "application/json",
    }

    try:
        logging.info(f"Fetching FUB contacts page {page}...")
        response = requests.get(base_url, headers=headers, params=params, timeout=30)

        if response.status_code != 200:
            logging.error(f"FUB API error {response.status_code}: {response.text[:300]}")
            return []

        data = response.json()
        contacts = data.get("people") or data.get("contacts") or []

        logging.info(
            f"Received {len(contacts)} contacts from FUB page {page}. "
            f"hasMore={data.get('hasMore', 'unknown')} nextPage={data.get('nextPage', 'n/a')}"
        )

        return contacts

    except Exception as e:
        logging.exception(f"Failed to fetch FUB contacts page {page}: {e}")
        return []


# ---------------------------------------------------------------------------
# RUN BACKFILL (main driver for sync)
# ---------------------------------------------------------------------------
def run_backfill():
    """
    Full backfill from Follow Up Boss → PostgreSQL.
    Paginates through FUB contacts, hashes each, compares to DB,
    and inserts/updates only changed contacts.
    """

    logging.info("🚀 Starting FUB → Render backfill")
    conn = get_db_connection()
    ensure_schema(conn)

    page = 1
    total_processed = 0
    total_changed = 0
    page_size = 100
    max_pages = 500  # safety cutoff to avoid runaway

    try:
        while page <= max_pages:
            contacts = get_fub_contacts(page)

            if not contacts:
                logging.info(f"No contacts returned on page {page}. Ending backfill.")
                break

            fub_ids = [c["id"] for c in contacts if "id" in c]
            if not fub_ids:
                logging.warning(f"Page {page} had no valid FUB IDs, skipping.")
                page += 1
                continue

            existing_hashes = get_existing_hashes(conn, fub_ids)
            contacts_batch, hashes_batch, logs_batch = [], [], []
            changed_count = 0

            for c in contacts:
                fub_id = c.get("id")
                if not fub_id:
                    continue

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
                        "street": (
                            c.get("primaryAddress", {}).get("street", "")
                            if c.get("primaryAddress")
                            else ""
                        ),
                        "city": (
                            c.get("primaryAddress", {}).get("city", "")
                            if c.get("primaryAddress")
                            else ""
                        ),
                        "state": (
                            c.get("primaryAddress", {}).get("state", "")
                            if c.get("primaryAddress")
                            else ""
                        ),
                        "zip": (
                            c.get("primaryAddress", {}).get("postalCode", "")
                            if c.get("primaryAddress")
                            else ""
                        ),
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

            # Write to DB if changed
            if changed_count > 0:
                bulk_upsert(conn, contacts_batch, hashes_batch, logs_batch)
                total_changed += changed_count

            total_processed += len(contacts)
            logging.info(
                f"Page {page}: processed={len(contacts)} changed={changed_count} "
                f"total_processed={total_processed} total_changed={total_changed}"
            )

            # stop if fewer than 100 (last page)
            if len(contacts) < page_size:
                logging.info("Reached last page — ending backfill.")
                break

            page += 1

        logging.info(
            f"✅ Backfill complete: total_processed={total_processed}, total_changed={total_changed}"
        )

    except Exception as e:
        logging.exception(f"❌ Backfill failed: {e}")

    finally:
        conn.close()
        logging.info("Database connection closed.")
