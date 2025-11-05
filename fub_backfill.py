def run_backfill():
    """
    Runs the full backfill from FUB → PostgreSQL database.
    Pulls contacts in pages, compares hashes, updates changed ones,
    and writes logs for each action.
    """

    logging.info("Starting FUB → Render backfill…")
    conn = get_db_connection()
    ensure_schema(conn)

    page = 1
    total_processed = 0
    total_changed = 0
    page_size = 100  # how many contacts per FUB API call
    max_pages = 200  # safety cap — avoid infinite loops

    try:
        while page <= max_pages:
            contacts = get_fub_contacts(page)
            if not contacts:
                logging.info(f"No contacts returned on page {page}. Ending backfill.")
                break

            fub_ids = [c["id"] for c in contacts if "id" in c]
            if not fub_ids:
                logging.warning(f"Page {page} had contacts without IDs — skipping.")
                page += 1
                continue

            # --- get current hashes from DB ---
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

            # --- Write updates if needed ---
            if changed_count > 0:
                bulk_upsert(conn, contacts_batch, hashes_batch, logs_batch)
                total_changed += changed_count

            total_processed += len(contacts)
            logging.info(
                f"Page {page}: processed={len(contacts)} changed={changed_count} "
                f"total_processed={total_processed} total_changed={total_changed}"
            )

            # Stop if less than full page was returned (end of data)
            if len(contacts) < page_size:
                logging.info("Reached end of contact list — stopping backfill.")
                break

            page += 1

        logging.info(
            f"✅ Backfill complete: total_processed={total_processed}, total_changed={total_changed}"
        )

    except Exception as e:
        logging.exception(f"Backfill failed: {e}")
    finally:
        conn.close()
